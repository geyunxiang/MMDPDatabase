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
import os , time

from sqlalchemy import create_engine, exists, and_
from sqlalchemy.orm import sessionmaker

from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from mmdps.proc import atlas
from mmdps.dms import tables
from mmdps.util import loadsave, clock
from mmdps import rootconfig

# from . import mongodb_database, redis_database
import mongodb_database, redis_database
from Cryptodome.Cipher import AES
from Cryptodome import Random

static_rdb_get = 0
static_rdb_set = 0
static_mdb_get = 0
dynamic_rdb_get = 0
dynamic_rdb_set = 0
dynamic_mdb_get = 0

class AESCoding:
	def __init__(self, tkey = b'this is a 16 key'):
		#you can change the mode here, there are five mode for you to choose,
		#CBC ECB CTR OCF CFB, CTR is not suggested
		#If you change into a different mode, you need to rewrite AESCoding,
		#because for different coding mode, the operation is not the same.
		if (type(tkey) is not bytes):
			tkey= tkey.encode()
		self.key = tkey
		self.iv = Random.new().read(AES.block_size)
		self.mycipher = AES.new(self.key, AES.MODE_CFB, self.iv)
	def encode(self, data):
		return self.iv + self.mycipher.encrypt(data.encode())
	def decode(self, data, tkey):
		mydecrypt = AES.new(tkey, AES.MODE_CFB, data[:16])
		return mydecrypt.decrypt(data[16:]).decode()

class MMDPDatabase:
	def __init__(self, data_source = 'Changgung', username = None, password = None):
		self.rdb = redis_database.RedisDatabase()
		if username is None:
			self.mdb = mongodb_database.MongoDBDatabase(data_source = data_source)
		else:
			self.mdb = mongodb_database.MongoDBDatabase(data_source = data_source, username = username, password = password)
		self.sdb = SQLiteDB()
		self.data_source = data_source

	def get_feature(self, scan_list, atlasobj, feature_name, comment = {}):
		"""
		Designed for static networks and attributes query.
		Using scan name , altasobj/altasobj name, feature name and data source(the default is Changgung) to query data from Redis.
		If the data is not in Redis, try to query data from Mongodb and store the data in Redis.
		If the query succeeds, return a Net or Attr class, if not, rasie an arror.
		"""
		#wrong input check
		global static_rdb_get
		global static_rdb_set
		global static_mdb_get
		return_single = False
		if type(scan_list) is str:
			scan_list = [scan_list]
			return_single = True
		if type(atlasobj) is atlas.Atlas:
			atlasobj = atlasobj.name

		if (not (type(scan_list) is list or type(scan_list) is str) or type(atlasobj) is not str or type(feature_name) is not str):
			raise Exception("Please input in the format as follows : scan must be str or a list of str, atlas and feature must be str")
		ret_list = []
		for scan in scan_list:
			static_rdb_get -= time.time()
			res = self.rdb.get_static_value(self.data_source, scan, atlasobj, feature_name, comment)
			static_rdb_get += time.time()
			if res is not None:
				ret_list.append(res)
			else:
				static_mdb_get -= time.time()
				doc = self.mdb.total_query('static', scan, atlasobj, feature_name, comment)
				static_rdb_get += time.time()
				doc = list(doc)
				if len(doc) != 0:
					static_rdb_set -= time.time()
					ret_list.append(self.rdb.set_value(doc[0],self.data_source))
					static_rdb_set += time.time()
				else:
					raise mongodb_database.NoRecordFoundException('No such item in redis and mongodb: ' + scan + ' ' + atlasobj + ' ' + feature_name)
					# raise Exception('No such item in redis and mongodb: ' + scan +' '+ atlasobj +' '+ feature_name)
		if return_single:
			return ret_list[0]
		else:
			return ret_list

	def get_dynamic_feature(self, scan_list, atlasobj, feature_name, window_length, step_size, comment = {}):
		"""
		Designed for dynamic networks and attributes query.
		Using scan name , altasobj/altasobj name, feature name, window length, step size and data source(the default is Changgung)
			to query data from Redis.
		If the data is not in Redis, try to query data from Mongodb and store the data in Redis.
		If the query succeeds, return a DynamicNet or DynamicAttr class, if not, rasie an arror.
		"""
		global dynamic_rdb_get
		global dynamic_rdb_set
		global dynamic_mdb_get
		return_single = False
		if type(scan_list) is str:
			scan_list = [scan_list]
			return_single = True
		if type(atlasobj) is atlas.Atlas:
			atlasobj = atlasobj.name
		if (not (type(scan_list) is list or type(scan_list) is str) or type(atlasobj) is not str or type(feature_name) is not str or type(window_length) is not int or type(step_size) is not int):
			raise Exception("Please input in the format as follows : scan must be str or a list of str, atlas and feature must be str, window length and step size must be int")
		ret_list = []
		for scan in scan_list:
			dynamic_rdb_get -= time.time()
			res = self.rdb.get_dynamic_value(self.data_source, scan, atlasobj, feature_name, window_length, step_size, comment)
			dynamic_rdb_get += time.time()
			if res is not None:
				ret_list.append(res)
			else:
				if feature_name.find('BOLD.net') != -1:
					dynamic_mdb_get -= time.time()
					doc = self.mdb.total_query('dynamic2', scan, atlasobj, feature_name, comment, window_length, step_size)
					dynamic_mdb_get += time.time()
				else:
					dynamic_mdb_get -= time.time()
					doc = self.mdb.total_query('dynamic1', scan, atlasobj, feature_name, comment, window_length, step_size)
					dynamic_mdb_get += time.time()
				doc = list(doc)
				if len(doc) != 0:
					dynamic_rdb_set -= time.time()
					mat = self.rdb.set_value(doc,self.data_source)
					dynamic_rdb_set += time.time()
					ret_list.append(mat)
				else:
					raise mongodb_database.NoRecordFoundException('No such item in redis or mongodb: ' + scan + ' ' + atlasobj + ' ' + feature_name + ' ' + str(window_length) + ' ' + str(step_size))
					# raise Exception('No such item in both redis and mongodb: ' + scan +' '+ atlasobj +' '+ feature_name +' '+
					# 			 ' '+ str(window_length) +' '+ str(step_size))
		if return_single:
			return ret_list[0]
		else:
			return ret_list

	def get_temp_feature(self, feature_collection, feature_name):
		pass

	def save_temp_feature(self, feature_collection, feature_name, value):
		pass

	def set_cache_list(self, cache_key, value):
		"""
		Store a list to redis as cache with cache_key
		"""
		if (type(cache_key) is not str or not all((type(x) is int or type(x) is float) for x in value)):
			raise Exception("Please input in the format as follows : key must be str, value must be a list of float or int")
		self.rdb.set_list_all_cache(cache_key, value)

	def append_cache_list(self, cache_key, value):
		"""
		Append value to a list in redis with cache_key.
		If the given key is empty in redis, a new list will be created.
		"""
		if (type(cache_key) is not str or not (type(value) is int or type(value) is float)):
			raise Exception("Please input in the format as follows : key mast be str, value must be int or float")
		self.rdb.set_list_cache(cache_key, value)

	def get_cache_list(self, cache_key):
		"""
		Return a list with given cache_key in redis
		"""
		return self.rdb.get_list_cache(cache_key)

	def save_cache_list(self, cache_key):
		"""
		Save list from redis to MongoDB
		"""
		a = self.get_cache_list(cache_key)
		#self.rdb.delete_key_cache(cache_key)
		self.mdb.put_temp_data(a,cache_key)


	def delete_cache_list(self, cache_key):
		"""
		Remove list from redis and mongo
		"""
		self.rdb.delete_key_cache(cache_key)
		# TODO: delete the list from mongo

	def get_study(self, alias):
		# TODO: input part of alias and search automatically
		return self.sdb.getResearchStudy(alias)

	def get_group(self, group_name):
		# TODO: input part of group_name and search automatically
		session = self.sdb.new_session()
		return session.query(tables.Group).filter_by(name = group_name).one()


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
	def init(self):
		tables.Base.metadata.create_all(self.engine)

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
			ret = self.session.query(exists().where(and_(tables.Person.name_pinyin == name, tables.Person.patientid == scan_info['Patient']['ID']))).scalar()
			if ret:
				self.session.add(db_mriscan)
				person = self.session.query(tables.Person).filter_by(name_pinyin = name).one()
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

	def insert_eegrow(self, eegjson):
		def to_gender(gen):
			if gen == 0:
				return 'F'
			else:
				return 'M'
		try:
			ret = self.session.query(tables.EEGScan).filter(tables.EEGScan.examid == eegjson['ExamID']).scalar()
			if ret:
				net = self.session.query(tables.EEGScan).filter(tables.EEGScan.examid == eegjson['ExamID']).one()
				print(net.person.name)
				print('scan %s has already existed in database' % eegjson['ExamID'])
				return 0
		except MultipleResultsFound:
			print('Error when importing: multiple scan records found for %s' % eegjson['ExamID'])
			return 1
		try:
			machine = self.session.query(tables.EEGMachine).filter(tables.EEGMachine.devicename == eegjson["DeviceName"]).one()
		except MultipleResultsFound:
			print('Error when importing: multiple machine records found for %s' % eegjson["DeviceName"])
			return 1
		except NoResultFound:
			machine = tables.EEGMachine(devicename=eegjson["DeviceName"],
										devicemode=eegjson["DeviceMode"],
										recordchannelsettinggroup=eegjson["RecordChannelSettingGroup"],
										recordmontagename=eegjson["RecordMontageName"],
										recordprotocolname=eegjson["RecordProtocolName"],
										recordeegcapname=eegjson["RecordEEGCapName"])
			self.session.add(machine)
		scan = tables.EEGScan(examid=eegjson["ExamID"],
							  date=clock.eeg_time(eegjson["ExamTime"]),
							  examitem=eegjson["ExamItem"],
							  impedancepos=','.join(eegjson["ImpedanceData"]["Item1"]),
							  impedancedata=','.join([str(i) for i in eegjson["ImpedanceData"]["Item2"]]),
							  impedanceonline=eegjson["ImpedanceOnline"],
							  begintimestamp=','.join([str(i["BeginTimeStamp"]) for i in eegjson["DataFileInformations"]]),
							  digitalmin=eegjson["DigitalMinimum"],
							  digitalmax=eegjson["DigitalMaximum"],
							  physicalmin=eegjson["PhysicalMinimum"],
							  physicalmax=eegjson["PhysicalMaximum"],
							  samplerate=eegjson["SampleRate"]
							  )
		self.session.add(scan)
		machine.eegscans.append(scan)
		try:
			person = self.session.query(tables.Person).filter(tables.Person.name == eegjson["PatientName"], tables.Person.eegid == eegjson["PatientID"]).one()
			if person.gender != to_gender(eegjson["Gender"]) or person.birth != clock.eeg_time(eegjson["BirthDate"]):
				raise Exception('Information about %s is not consistent' % eegjson["PatientName"])
			person.eegscans.append(scan)
			self.session.commit()
			print('Old patient new scan %s inserted' % eegjson["PatientName"])
			return 0
		except MultipleResultsFound:
			print('Error when importing: multiple person records found for %s' % eegjson["PatientName"])
			return 2
		except NoResultFound:
			person = tables.Person(name=eegjson["PatientName"],
								   eegid=eegjson["PatientID"],
								   gender=to_gender(eegjson["Gender"]),
								   birth=clock.eeg_time(eegjson["BirthDate"])
			)
			self.session.add(person)
			person.eegscans.append(scan)
			self.session.commit()
			print('New patient new scan %s inserted' % eegjson["PatientName"])
			return 0

	def getMRIScansInGroup(self, groupName):
		group = self.session.query(tables.Group).filter_by(name = groupName).one()
		return group.mriscans

	def getEEGScansInGroup(self, groupName):
		group = self.session.query(tables.Group).filter_by(name = groupName).one()
		return group.eegscans

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

	def newGroupByScans_forMRI(self, groupName, scanList, desc = None):
		"""
		Initialize a group by a list of mriscans
		"""
		group = tables.Group(name = groupName, description = desc)
		# check if group already exist
		try:
			self.session.query(tables.Group).filter_by(name = groupName).one()
		except NoResultFound:
			# alright
			for scan in scanList:
				db_scan = self.session.query(tables.MRIScan).filter_by(filename = scan).one()
				group.mriscans.append(db_scan)
				group.people.append(db_scan.person)
			self.session.add(group)
			self.session.commit()
			return
		except MultipleResultsFound:
			# more than one record found
			raise Exception("More than one %s group found!" % groupName)
		# found one existing record
		raise Exception("%s group already exist" % groupName)

	def newGroupByNames_forMRI(self, groupName, nameList, scanNum, desc = None, accumulateScan = False):
		"""
		Initialize a group by a list of names. The mriscans are generated automatically.
		scanNum - which scan (first/second/etc)
		accumulateScan - whether keep former mriscans in this group
		"""
		group = tables.Group(name = groupName, description = desc)
		try:
			self.session.query(tables.Group).filter_by(name = groupName).one()
		except NoResultFound:
			for name in nameList:
				db_person = self.session.query(tables.Person).filter_by(name = name).one()
				group.people.append(db_person)
				if accumulateScan:
					group.mriscans += sorted(db_person.mriscans, key = lambda x: x.filename)[:scanNum]
				else:
					group.mriscans.append(sorted(db_person.mriscans, key = lambda x: x.filename)[scanNum - 1])
			self.session.add(group)
			self.session.commit()
			return
		except MultipleResultsFound:
			# more than one record found
			raise Exception("More than one %s group found!" % groupName)
		# found one existing record
		raise Exception("%s group already exist" % groupName)

	def newGroupByNamesAndScans_forMRI(self, groupName, nameList, scanList, desc = None):
		"""
		Initialize a group by giving both name and scans
		"""
		group = tables.Group(name = groupName, description = desc)
		for name in nameList:
			db_person = self.session.query(tables.Person).filter_by(name = name).one()
			group.people.append(db_person)
		for scan in scanList:
			db_scan = self.session.query(tables.MRIScan).filter_by(filename = scan).one()
			group.mriscans.append(db_scan)
		self.session.add(group)
		self.session.commit()

	def newGroupByID_forEEG(self, groupName, scanList, desc = None):
		"""
		Initialize a group by a list of mriscans
		"""
		group = tables.Group(name = groupName, description = desc)
		# check if group already exist
		try:
			self.session.query(tables.Group).filter_by(name = groupName).one()
		except NoResultFound:
			# alright
			for scan in scanList:
				db_scan = self.session.query(tables.EEGScan).filter_by(examid = scan).one()
				group.eegscans.append(db_scan)
			self.session.add(group)
			self.session.commit()
			return
		except MultipleResultsFound:
			# more than one record found
			raise Exception("More than one %s group found!" % groupName)
		# found one existing record
		raise Exception("%s group already exist" % groupName)

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

	def get_all_mriscans_of_person(self, person_name):
		session = self.new_session()
		one_person = session.query(tables.Person).filter_by(name = person_name).one()
		return session.query(tables.MRIScan).filter_by(person_id = one_person.id)

	def deleteScan(self, session, mriscanFilename):
		db_scan = session.query(tables.MRIScan).filter_by(filename = mriscanFilename).one()
		session.delete(db_scan)
		session.commit()
