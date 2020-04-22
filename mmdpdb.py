"""
This module deals with mmdpdb related actions.

mmdpdb is the database of MMDPS, containing 3 databased.
	1. SQLite stores meta-info like
	   patient information, scan date, group relationships, 
	   research study cases and so on. 
	2. MongoDB keeps all extracted features like networks
	   and attributes. 
	3. Redis is a high-speed cache that starts up upon request. 

"""
import os

from sqlalchemy import create_engine, exists, and_
from sqlalchemy.orm import sessionmaker

from sqlalchemy.orm.exc import MultipleResultsFound

from mmdps.proc import atlas
from mmdps.dms import tables
from mmdps.util import loadsave, clock
from mmdps import rootconfig

import mongodb_database, redis_database

class MMDBDatabase:
	def __init__(self):
		self.rdb = redis_database.RedisDatabase()
		self.mdb = mongodb_database.MongoDBDatabase()
		self.sdb = SQLiteDB()

	def get_feature(self, scan_list, atlasobj, feature_name, data_source = 'Changgung'):
		#wrong input check
		return_single = False
		if type(scan_list) is str:
			scan_list = [scan_list]
			return_single = True
		if type(atlasobj) is atlas.Atlas:
			atlasobj = atlasobj.name
		ret_list = []
		for scan in scan_list:
			res = self.rdb.get_static_value(scan, atlasobj, feature_name)
			if res != None:
				ret_list.append(res)
			else:
				doc = self.mdb.query_static(scan, atlasobj, feature_name)
				if doc.count() != 0:
					ret_list.append(self.rdb.set_value(doc[0]))
				else:
					e = Exception('No such item in redis and mongodb: ' + scan +' '+ atlasobj +' '+ feature_name)
					print(e)
		if return_single:
			return ret_list[0]
		else:
			return ret_list

	def get_dynamic_feature(self, scan_list, atlasobj, feature_name, window_length, step_size, data_source = 'Changgung'):
		return_single = False
		if type(scan_list) is str:
			scan_list = [scan_list]
			return_single = True
		if type(atlasobj) is atlas.Atlas:
			atlasobj = atlasobj.name
		ret_list = []
		for scan in scan_list:
			res = self.rdb.get_dynamic_value(scan, atlasobj, feature_name, window_length, step_size)
			if type(res) is Exception:
				raise res
			if res != None:
				ret_list.append(res)
			else:
				doc = self.mdb.query_dynamic(scan, atlasobj, feature_name, window_length, step_size)
				if doc.count() != 0:
					mat = self.rdb.set_value(doc)
					if type(mat) is Exception:
						raise mat
					ret_list.append(mat)
				else:
					e = Exception('No such item in redis and mongodb: ' + scan +' '+ atlasobj +' '+ feature_name +' '+
								 ' '+ str(window_length) +' '+ str(step_size))
					print(e)
		if return_single:
			return ret_list[0]
		else:
			return ret_list

	def get_temp_feature(self, feature_collection, feature_name):
		pass

	def save_temp_feature(self, feature_collection, feature_name, value):
		pass

	def append_cache_list(self, cache_key, value):
		pass

	def get_cache_list(self, cache_key):
		pass

	def save_cache_list(self, cache_key):
		"""
		save list from Redis to MongoDB
		"""
		pass


class SQLiteDB:
	"""
	SQLite stores meta-info like patient information, scan date, group 
	relationships, research study cases and so on. 
	"""
	def __init__(self, dbFilePath = rootconfig.dms.mmdpdb_filepath):
		self.engine = create_engine('sqlite:///' + dbFilePath)
		self.Session = sessionmaker(bind = self.engine)
		self.session = self.Session()

	def new_session(self):
		return self.Session()

	def insert_mrirow(self, scan, hasT1, hasT2, hasBOLD, hasDWI):
		"""Insert one mriscan record."""
		# check if scan already exist
		try:
			ret = self.session.query(exists().where(tables.MRIScan.filename == scan)).scalar()
			if ret:
				# record exists
				return 0
		except MultipleResultsFound:
			print('Error when importing: multiple scan records found for %s' % scan)
			return 1
		mrifolder = rootconfig.dms.folder_mridata
		scan_info = loadsave.load_json(os.path.join(mrifolder, scan, 'scan_info.json'))

		machine = tables.MRIMachine(institution = scan_info['Machine']['Institution'],
							 manufacturer = scan_info['Machine']['Manufacturer'],
							 modelname = scan_info['Machine']['ManufacturerModelName'])

		name, date = scan.split('_')
		dateobj = clock.simple_to_time(date)
		db_mriscan = tables.MRIScan(date = dateobj, hasT1 = hasT1, hasT2 = hasT2, hasBOLD = hasBOLD, hasDWI = hasDWI, filename = scan)
		machine.mriscans.append(db_mriscan)
		try:
			ret = self.session.query(exists().where(and_(tables.Person.name == name, tables.Person.patientid == scan_info['Patient']['ID']))).scalar()
			if ret:
				self.session.add(db_mriscan)
				person = self.session.query(tables.Person).filter_by(name = name).one()
				person.mriscans.append(db_mriscan)
				self.session.commit()
				print('Old patient new scan %s inserted' % scan)
				return 0
		except MultipleResultsFound:
			print('Error when importing: multiple person records found for %s' % name)
			return 2
		db_person = tables.Person.build_person(name, scan_info)
		db_person.mriscans.append(db_mriscan)
		self.session.add(db_person)
		self.session.commit()
		print('New patient new scan %s inserted' % scan)
		return 0

	def getScansInGroup(self, groupName):
		group = self.session.query(tables.Group).filter_by(name = groupName).one()
		return group.scans

	def getNamesInGroup(self, groupName):
		group = self.session.query(tables.Group).filter_by(name = groupName).one()
		return group.people

	def getAllGroups(self):
		"""
		Return a list of all groups in this database
		"""
		return self.session.query(tables.Group).all()

	def getResearchStudy(self, alias):
		return self.session.query(tables.ResearchStudy).filter_by(alias = alias).one()

	def getHealthyGroup(self):
		"""
		"""
		return self.session.query(tables.Group).filter_by(name = 'Changgung HC').one()

	def newGroupByScans(self, groupName, scanList, desc = None):
		"""
		Initialize a group by a list of scans
		"""
		group = tables.Group(name = groupName, description = desc)
		# check if group already exist
		try:
			self.session.query(tables.Group).filter_by(name = groupName).one()
		except sqlalchemy.orm.exc.NoResultFound:
			# alright
			for scan in scanList:
				db_scan = self.session.query(tables.MRIScan).filter_by(filename = scan).one()
				group.scans.append(db_scan)
				group.people.append(db_scan.person)
			self.session.add(group)
			self.session.commit()
			return
		except sqlalchemy.orm.exc.MultipleResultsFound:
			# more than one record found
			raise Exception("More than one %s group found!" % groupName)
		# found one existing record
		raise Exception("%s group already exist" % groupName)

	def newGroupByNames(self, groupName, nameList, scanNum, desc = None, accumulateScan = False):
		"""
		Initialize a group by a list of names. The scans are generated automatically.
		scanNum - which scan (first/second/etc)
		accumulateScan - whether keep former scans in this group
		"""
		group = tables.Group(name = groupName, description = desc)
		try:
			self.session.query(tables.Group).filter_by(name = groupName).one()
		except sqlalchemy.orm.exc.NoResultFound:
			for name in nameList:
				db_person = self.session.query(tables.Person).filter_by(name = name).one()
				group.people.append(db_person)
				if accumulateScan:
					group.scans += sorted(db_person.mriscans, key = lambda x: x.filename)[:scanNum]
				else:
					group.scans.append(sorted(db_person.mriscans, key = lambda x: x.filename)[scanNum - 1])
			self.session.add(group)
			self.session.commit()
			return
		except sqlalchemy.orm.exc.MultipleResultsFound:
			# more than one record found
			raise Exception("More than one %s group found!" % groupName)
		# found one existing record
		raise Exception("%s group already exist" % groupName)

	def newGroupByNamesAndScans(self, groupName, nameList, scanList, desc = None):
		"""
		Initialize a group by giving both name and scans
		"""
		group = tables.Group(name = groupName, description = desc)
		for name in nameList:
			db_person = self.session.query(tables.Person).filter_by(name = name).one()
			group.people.append(db_person)
		for scan in scanList:
			db_scan = self.session.query(tables.MRIScan).filter_by(filename = scan).one()
			group.scans.append(db_scan)
		self.session.add(group)
		self.commit()

	def deleteGroupByName(self, groupName):
		groupList = self.session.query(tables.Group).filter_by(name = groupName).all()
		# if group is None:
		# 	raise Exception("%s group does not exist!" % groupName)
		for group in groupList:
			self.session.delete(group)
		self.session.commit()

	def personname_to_id(self, personnames):
		session = self.new_session()
		personIDs = []
		for personname in personnames:
			person = session.query(tables.Person).filter_by(name = personname).one()
			personIDs.append(person.id)
		return personIDs

	def get_all_scans_of_person(self, person_name):
		session = self.new_session()
		one_person = session.query(tables.Person).filter_by(name = person_name).one()
		return session.query(tables.MRIScan).filter_by(person_id = one_person.id)

	def deleteScan(self, session, mriscanFilename):
		db_scan = session.query(tables.MRIScan).filter_by(filename = mriscanFilename).one()
		session.delete(db_scan)
		session.commit()
