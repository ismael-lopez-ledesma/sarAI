#This library contains tools to get data from edgar website and handle different
#edgar related data structures.

from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import logging
import datetime
import os
import shutil
import configuration as config
import yahoo


def create_training_database():
	"""Creates a csv file containing the financial concepts extracted from EDGAR database for all the companies defined in the INDEX file.
	Returns the number of companies added to the table
	The csv table will be used afterwards for training"""
	if  not os.path.isfile(config.EDGAR_INDEX_FILE_PATH):
		logging.error(f"edgar.create_training_database: INDEX file does not exist in the expected location: {config.EDGAR_INDEX_FILE_PATH}")
		return 0
	logging.info(f"edgar.create_training_database: Reading list of companies from INDEX file {config.EDGAR_INDEX_FILE_PATH}")
	try:
		companies_list = pd.read_csv(config.EDGAR_INDEX_FILE_PATH).iloc[:, 0]
	except Exception as err:
		logging.error(f"edgar.create_training_database: INDEX file is not well formatted - {err}")
		return 0
	else:
		logging.info(f"edgar.create_training_database: List of companies obtained from INDEX file")
	companies_table = pd.DataFrame()
	companies_in_table = 0
	for company in companies_list:
		logging.info(f"edgar.create_training_database: Creating dataframe for company {company}")
		company_df = create_table_for_company(company)
		if company_df.empty:
			logging.warning(f"edgar.create_training_database: Dataframe for company {company} cannot be created")
		else:
			logging.info(f"edgar.create_training_database: Dataframe for company {company} created. Attaching it to the main table")
			try:
				companies_table = pd.concat([companies_table, company_df])
			except Exception as err:
				logging.warning(f"edgar.create_training_database: Dataframe for company {company} cannot be attached to the main table - {err}")
			else:
				logging.info(f"edgar.create_training_database: Dataframe for company {company} attached to the main table")
				companies_in_table +=1
	logging.info(f"edgar.create_training_database: Main table created with {companies_in_table} companies")
	if os.path.isfile(config.EDGAR_TRAINING_FILE):
		logging.info(f"edgar.create_training_database: File {config.EDGAR_TRAINING_FILE} already exists")
		backup_filename = config.BACKUP_PATH + "01_EDGAR_TRAINING_TABLE_" + datetime.datetime.now().strftime("%Y%m%d") + ".csv"
		logging.info(f"edgar.create_training_database: Creating backup copy {backup_filename}.")
		try:
			shutil.copyfile(config.EDGAR_TRAINING_FILE, backup_filename)
		except Exception as err:
			logging.warning(f"edgar.create_training_database: Backup copy {backup_filename} cannot be created.")
	logging.info(f"edgar.create_training_database: Storing main table into {config.EDGAR_TRAINING_FILE}")
	try:
		companies_table.to_csv(config.EDGAR_TRAINING_FILE)
	except Exception as err:
		logging.error(f"edgar.create_training_database: The training database cannot be stored - {err}")
		return 0
	else:
		logging.info(f"edgar.create_training_database: Training database with {companies_in_table} companies stored into {config.EDGAR_TRAINING_FILE}")
		return companies_in_table
	

def download_company_raw_json(ticker=""):
	"""Downloads from EDGAR database the json with RAW data for the received company ticker and stores it into a json file.
	Returns the downloaded json or an empty one in case of errors"""
	logging.info(f"edgar.download_company_raw_json: Downloading company facts for ticker {ticker}")
	json_empty = {}
	out_filename = config.DATABASE_PATH + ticker + ".json"
	cik = get_cik(ticker)
	if cik == "ERROR":
		logging.error(f"edgar.download_company_raw_json: CIK for ticker {ticker} cannot be obtained")
		return json_empty
	url = config.companyFactsURL.format(cik)
	logging.info(f"edgar.download_company_raw_json: Downloading from {url}")
	try:
		response = get(url, headers=config.EDGAR_HEADERS)
	except Exception as err:
		logging.error(f"edgar.download_company_raw_json: Download not possible - {err}")
		return json_empty
	else:
		logging.info(f"edgar.download_company_raw_json: Json downloaded.")
		if (response.status_code != 204 and response.headers["content-type"].strip().startswith("application/json")):
			try:
				json_data = response.json()
			except Exception as err:
				logging.error(f"edgar.download_company_raw_table: Response format is not valid - {err}")
				return json_empty
			else:
				if os.path.isfile(out_filename):
					logging.info(f"edgar.download_company_raw_table: File {out_filename} already exists.")
					backup_filename = config.BACKUP_PATH + ticker + "_" + datetime.datetime.now().strftime("%Y%m%d") + ".json"
					logging.info(f"edgar.download_company_raw_table: Creating backup copy {backup_filename}.")
					try:
						shutil.copyfile(out_filename, backup_filename)
					except Exception as err:
						logging.warning(f"edgar.download_company_raw_table: Backup copy {backup_filename} cannot be created.")
				logging.info(f"edgar.download_company_raw_table: Storing json into {out_filename}.")
				with open(out_filename, 'w') as f:
					try:
						json.dump(json_data, f)
					except Exception as err:
						logging.error(f"edgar.download_company_raw_table: The data cannot be stored - {err}")
						return json_empty
					else:
						logging.info(f"edgar.download_company_raw_table: Raw json file stored.")
						return json_data
		else:
			logging.error(f"edgar.download_company_raw_table: Downloaded json is not valid - Code: {response.status_code} / Type: {response.headers['content-type']}")
			return json_empty


def create_table_for_company(ticker=""):
	"""Obtains from EDGAR database a table formatted for training for the provided ticker.
	Returns the table as a pandas dataframe or an emptz dataframe in case of error"""
	logging.info(f"edgar.create_table_for_company: Creating company table for ticker {ticker}")
	company_table = pd.DataFrame()
	if not ticker:
		logging.error(f"edgar.create_table_for_company: Ticker empty.")
		return company_table
	company_json = download_company_raw_json(ticker)
	if not company_json:
		logging.error(f"edgar.create_table_for_company: Json empty. The table cannot be created for ticker {ticker}")
		return company_table
	else:
		if "us-gaap" not in company_json['facts']:
			logging.error(f"edgar.create_table_for_company: Json not formatted as expected, us-gaap key missing. The table cannot be created for ticker {ticker}")
			return company_table
		for financial_concept in company_json['facts']['us-gaap'].keys():
			logging.info(f"edgar.create_table_for_company: Getting financial concept {financial_concept}")
			financial_concept_unit = list(company_json['facts']['us-gaap'][financial_concept]['units'])[0]
			logging.info(f"edgar.create_table_for_company: Unit for financial concept {financial_concept}: {financial_concept_unit}")
			financial_concept_json_array = []
			try:
				financial_concept_json_array = company_json['facts']['us-gaap'][financial_concept]['units'][financial_concept_unit]
			except Exception as err:
				logging.warning(f"edgar.create_table_for_company: Not possible to obtain financial concept {financial_concept} - {err}")
			else:
				logging.info(f"edgar.create_table_for_company: Array found for financial concept {financial_concept}")
			if not financial_concept_json_array:
				logging.warning(f"edgar.create_table_for_company: Array for financial concept {financial_concept} is empty")
			else:
				logging.info(f"edgar.create_table_for_company: Creating dataframe from financial concept array")
				try:
					financial_concept_df = pd.json_normalize(financial_concept_json_array)[['val', 'fy', 'form', 'frame']]
				except Exception as err:
					logging.warning(f"edgar.create_table_for_company: Dataframe for financial concept {financial_concept} cannot be created - {err}")
				else:
					financial_concept_df['unit'] = financial_concept_unit
					financial_concept_df['concept'] = financial_concept
					logging.info(f"edgar.create_table_for_company: Appending financial concept dataframe to company table dataframe")
					company_table = pd.concat([company_table, financial_concept_df])
		logging.info(f"edgar.create_table_for_company: Adding company information columns")
		company_table['ticker'] = ticker
		company_table['sector'] = yahoo.get_company_sector(ticker)
		company_table['industry'] = yahoo.get_company_industry(ticker)
		company_table['activity'] = get_activity(ticker)
		company_table['sic'] = get_sic(ticker)
		logging.info(f"edgar.create_table_for_company: Table for ticker {ticker} created")
		return company_table


def get_cik(ticker=""):
	"""provides EDGAR CIK number for the provided ticker"""
	url = config.BROWSE_URL.format(ticker)
	logging.info(f"edgar.get_cik: Getting CIK from {url}")
	try:
		f = get(url, stream = True, headers=config.EDGAR_HEADERS)
	except Exception as err:
		logging.warning(f"edgar.get_cik: Edgar CIK cannot be obtained - {err}")
		return "ERROR"
	else:
		if config.ERROR_MESSAGE1 in str(f.content) or config.ERROR_MESSAGE1 in str(f.content):
			logging.warning(f"edgar.get_cik: Edgar CIK cannot be obtained.")
			return "ERROR"
		else:
			soup = BeautifulSoup(f.text, "html.parser")
			cik = soup.find_all("span", class_="companyName")[0].text.split("CIK#:")[1].split()[0]
			if check_cik(cik):
				logging.info(f"edgar.get_cik: CIK for {ticker}: {cik}")
				return cik
			else:
				logging.warning(f"edgar.get_cik: Retrieved CIK is not well formatted - {err}")
				return "ERROR"
			

def check_cik(cik=""):
	"""Checks if the provided cik is a valid CIK number in EDGAR database"""
	url = config.DOWNLOAD_URL.format(cik) + "/index.json"
	if len(cik) != 10 or not cik.isdigit():
		logging.warning(f"edgar.check_cik: CIK {cik} not valid: must have 10 numbers")
		return False
	logging.info(f"edgar.check_cik: Checking connection to {url}")
	try:
		f = get(url, stream = True, headers=config.EDGAR_HEADERS)
	except Exception as err:
		logging.warning(f"edgar.check_cik: Cannot connect to edgar with CIK {cik}")
		return False
	else:
		if f.status_code != 200:
			logging.warning(f"edgar.check_cik: Connection to EDGAR database not possible with CIK {cik} - {f.status_code}")
			return False
		else:
			logging.info(f"edgar.check_cik: CIK {cik} is valid")
			return True

def get_activity(ticker=""):
	"""gets company activity from EDGAR database for the provided ticker"""
	url = config.BROWSE_URL.format(ticker)
	logging.info(f"edgar.get_activity: Getting company activity from {url}")
	try:
		f = get(url, stream = True, headers=config.EDGAR_HEADERS)
	except Exception as err:
		logging.warning(f"edgar.get_activity: Company activity cannot be obtained - {err}")
		return "N/A"
	else:
		if config.ERROR_MESSAGE1 in str(f.content) or config.ERROR_MESSAGE1 in str(f.content):
			logging.warning(f"edgar.get_activity: Company activity cannot be obtained.")
			return "N/A"
		else:
			soup = BeautifulSoup(f.text, "html.parser")
			try:
				activity = str(soup.find("p", class_="identInfo").find_all("a")[0].next_element.next_element).split("- ")[1].replace(",", " &")
			except AttributeError as err:
				logging.warning(f"edgar.get_activity: Activity not found for ticker {ticker} - {err}")
				return "N/A"
			except Exception as err:
				logging.warning(f"edgar.get_activity: Activity not found for ticker {ticker} - {err}")
				return "N/A"
			else:
				logging.info(f"edgar.get_activity: Activity found for {ticker}: {activity}")
				return activity


def get_sic(ticker=""):
	"""gets company SIC (Standard Industrial Code) from EDGAR database for the provided ticker"""
	url = config.BROWSE_URL.format(ticker)
	logging.info(f"edgar.get_sic: Getting company SIC from {url}")
	try:
		f = get(url, stream = True, headers=config.EDGAR_HEADERS)
	except Exception as err:
		logging.warning(f"edgar.get_sic: Company SIC cannot be obtained - {err}")
		return "N/A"
	else:
		if config.ERROR_MESSAGE1 in str(f.content) or config.ERROR_MESSAGE1 in str(f.content):
			logging.warning(f"edgar.get_sic: Company SIC cannot be obtained.")
			return "N/A"
		else:
			soup = BeautifulSoup(f.text, "html.parser")
			try:
				sic = str(soup.find("p", class_="identInfo").find_all("a")[0].next_element)
			except AttributeError as err:
				logging.warning(f"edgar.get_sic: SIC not found for ticker {ticker} - {err}")
				return "N/A"
			except Exception as err:
				logging.warning(f"edgar.get_sic: SIC not found for ticker {ticker} - {err}")
				return "N/A"
			else:
				if not check_sic(sic):
					logging.warning(f"edgar.get_sic: Retrieved SIC {sic} is not valid")
					return "N/A"
				else:
					logging.info(f"edgar.get_sic: SIC found for {ticker}: {sic}")
					return sic


def check_sic(sic=""):
	"""Checks if the provided sic has a valid format"""
	if len(sic) != 4 or not sic.isdigit():
		logging.warning(f"edgar.check_sic: SIC {sic} not valid: must have 4 numbers")
		return False
	else:
		logging.info(f"edgar.check_sic: SIC {sic} valid")
		return True