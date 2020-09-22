import requests
from mmdpdb import SQLiteDB
from sqlalchemy import not_
from mmdps.dms import tables
api_url_root = 'https://brainpercep.com/api'

def authenticate():
	json_dict = {'username': 'yaojh18', 'password': 'yjh20000524'}
	resp = requests.post(api_url_root + '/token-auth/', json = json_dict)
	if resp.status_code != 200:
		print(resp.json())
		raise Exception('POST /token-auth/ {}'.format(resp.status_code))
	token = resp.json()['token']
	return token

def response_ok(resp):
	if resp.status_code == 200 or resp.status_code == 201:
		return True
	return False

def load_treatment_data():
	# 1. authenticate
	token = authenticate()

	# 2. search patient
	s = SQLiteDB('mmdpdb.db')
	people = s.session.query(tables.Person).filter(tables.Person.name_chinese.isnot(None))
	for person in people:
		json_dict = {"name": person.name_chinese}
		resp = requests.post(api_url_root + '/patients/search/', json = json_dict, headers = {'Authorization': 'JWT ' + token})
		if not response_ok(resp):
			print(resp.headers)
			print(resp.status_code)
			print(resp.json())
			raise Exception('POST /patients/search/ {}'.format(resp.status_code))
		else:
			if (resp.json() != []):
				id = resp.json()[0]['id']
				print(person.mriscans)
				for mriscan in person.mriscans:
					return_json = {
						"rec_date": str(mriscan.date).replace(' ','T') + '+08:00',
						"visibility": True,
						"type": "MRI",
						"patient": id,
					}
					resp = requests.post(api_url_root + '/exams/treatment/', json = return_json, headers = {'Authorization': 'JWT ' + token})
					if not response_ok(resp):
						print(resp.headers)
						print(resp.status_code)
						print(resp.json())
				for eegscan in person.eegscans:
					return_json = {
						"rec_date": str(eegscan.date).replace(' ','T') + '+08:00',
						"visibility": True,
						"type": "EEG",
						"patient": id,
					}
					resp = requests.post(api_url_root + '/exams/treatment/', json = return_json, headers = {'Authorization': 'JWT ' + token})
					if not response_ok(resp):
						print(resp.headers)
						print(resp.status_code)
						print(resp.json())

if __name__ == '__main__':
	load_treatment_data()
