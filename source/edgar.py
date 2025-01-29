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
	"""Downloads the table with RAW data for the received company ticker and stores it into a json file.
	Returns the downloaded json or an empty one in case of errors"""
	logging.info(f"edgar.download_company_raw_json: Downloading company facts for ticker {ticker}")
	out_filename = config.DATABASE_PATH + ticker + ".json"
	cik = get_cik(ticker)
	url = config.companyFactsURL.format(cik)
	json_empty = {}
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
	