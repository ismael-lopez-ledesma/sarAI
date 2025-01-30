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
		response = get(url, headers=config.HEADERS)
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
					financial_concept_df['ticker'] = ticker
					logging.info(f"edgar.create_table_for_company: Appending financial concept dataframe to company table dataframe")
					company_table = pd.concat([company_table, financial_concept_df])
		return company_table


def get_cik(ticker=""):
	"""provides EDGAR CIK number for the provided ticker"""
	url = config.BROWSE_URL.format(ticker)
	logging.info(f"edgar.get_cik: Getting CIK from {url}")
	try:
		f = get(url, stream = True, headers=config.HEADERS)
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
		f = get(url, stream = True, headers=config.HEADERS)
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


def translate_frame_to_fiscal_year(frame=""):
	"""Translates a frame in instant format (CY2014 or CY2013Q4I) into the corresponding fiscal year"""
	logging.info(f"edgar.translate_frame_to_fiscal_year: Translating frame {frame}")
	fiscal_year = frame.replace("CY", "")
	if fiscal_year.endswith("Q4I"):
		logging.info(f"edgar.translate_frame_to_fiscal_year: Instant frame, removing sufix and adding one")
		fiscal_year = fiscal_year.replace("Q4I", "")
		fiscal_year = str(int(fiscal_year) + 1)
	logging.info(f"edgar.translate_frame_to_fiscal_year: Returning fiscal year {fiscal_year}")
	return fiscal_year
	