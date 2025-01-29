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


def create_table(financial_concept=""):
    """Creates a table for the financial concept received as parameter and stores it into a csv file with the same name"""
    out_filename = config.DATABASE_RAW_PATH + financial_concept[0] + ".csv"
    table = pd.DataFrame()
    logging.info(f"edgar.create_table: Creting table {financial_concept[0]}")
    for c in financial_concept:
        for y in config.years:
            url = config.endPoint.format(c, y)
            logging.info(f"edgar.create_table: Downloading from {url}")
            try:
                response = get(url, headers=config.HEADERS)
            except Exception as err:
                logging.warning(f"edgar.create_table: Download not possible - {err}")
            else:
                logging.info(f"edgar.create_table: Json downloaded.")
                if (response.status_code != 204 and response.headers["content-type"].strip().startswith("application/json")):
                    logging.info(f"edgar.create_table: Decoding json.")
                    try:
                        frame_data = response.json()
                    except ValueError:
                        logging.warning(f"edgar.create_table: Response format is not valid - {ValueError}")
                    else:
                        logging.info(f"edgar.create_table: Translating to DataFrame.")
                        df = pd.json_normalize(frame_data, record_path='data', meta=['taxonomy', 'tag', 'ccp', 'uom', 'label', 'description', 'pts'])
                        logging.info(f"edgar.create_table: Appending DataFrame.")
                        table = pd.concat([table, df])
                else:
                    logging.warning(f"edgar.create_table: Downloaded json is not valid or it is empty - {response.status_code} - {response.headers['content-type']}")
                time.sleep(0.1)
    logging.info(f"edgar.create_table: Storing into {out_filename}")
    try:
        table.to_csv(out_filename)
    except Exception as err:
        logging.error(f"edgar.create_table: File {out_filename} cannot be stored - {err}")
    else:
        logging.info(f"edgar.create_table: File {out_filename} saved")


def add_company_to_database(ticker=""):
	"""Creates the database files for a company in the database"""
	cik = get_cik(ticker)
	if check_cik(cik) == False:
		logging.error(f"edgar.add_company_to_database: CIK cannot be determined")
		return False
	json_raw_data = download_company_raw_json(ticker, cik)
	if not json_raw_data:
		logging.error(f"edgar.add_company_to_database: JSON data cannot be downloaded.")
		return False
	df_basic_financials_table = build_basic_financials_table(config.financial_statements, json_raw_data)
	print(df_basic_financials_table)


def download_company_raw_json(ticker="", cik=""):
	"""Downloads the table with RAW data for the received company ticker and stores it into a json file.
	Returns the downloaded json or an empty one in case of errors"""
	logging.info(f"edgar.download_company_raw_json: Downloading company facts for ticker {ticker} with CIK  {cik}")
	out_filename = config.DATABASE_RAW_PATH + ticker + ".json"
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


def get_financial_concept_from_json(array_concept, json_raw_data):
	"""This function returns a dataframe with the financial concept received as a synonims array, extracted from the received json raw data"""
	df_concept = pd.DataFrame(columns=['start', 'end', 'val', 'accn', 'fy', 'fp', 'form', 'filed', 'frame'])
	df_year = pd.DataFrame()
	df_concept_year = pd.DataFrame(columns=['frame', 'val'])
	for concept_synonim in array_concept:
		logging.info(f"edgar.get_financial_concept_from_json: Gettind dataframe for {concept_synonim}")
		try:
			if array_concept[0] == "EarningsPerShareDiluted":
				df_synonim = pd.json_normalize(json_raw_data['facts']['us-gaap'][concept_synonim]['units']['USD/shares'])
			elif array_concept[0] == "WeightedAverageNumberOfDilutedSharesOutstanding":
				df_synonim = pd.json_normalize(json_raw_data['facts']['us-gaap'][concept_synonim]['units']['shares'])
			else:
				df_synonim = pd.json_normalize(json_raw_data['facts']['us-gaap'][concept_synonim]['units']['USD'])
		except Exception as err:
			logging.warning(f"edgar.get_financial_concept_from_json: Dataframe for {concept_synonim} cannot be obtained")
		else:
			df_concept = pd.concat([df_concept, df_synonim.loc[df_synonim['form'] == "10-K"]])
	for year in config.years:
		logging.info(f"edgar.get_financial_concept_from_json: Filtering data for year {year}")
		frame_year = ["CY" + str(year), "CY" + str(year - 1) + "Q4I"]
		df_year = df_concept.loc[df_concept['frame'].isin(frame_year)]
		if df_year.empty:
			logging.warning(f"edgar.get_financial_concept_from_json: No data for frame year {frame_year[0]}.")
			df_concept_year.loc[len(df_concept_year.index)] = ["CY" + str(year), "0"]
		elif len(df_year) > 1:
			logging.info(f"edgar.get_financial_concept_from_json: More than one entry for year {year}, analyzing")
			if df_year['val'].duplicated(keep=False).all():
				logging.info(f"edgar.get_financial_concept_from_json: All duplicated entries have the same value {df_year['val'].iloc[0]}, taking first value")
				df_concept_year = pd.concat([df_concept_year, df_year[['frame', 'val']].head(1)])
			else:
				logging.warning(f"edgar.get_financial_concept_from_json: Duplicated entries have different values, taking last filed entry: {df_year.sort_values(by=['filed'])['val'].iloc[-1]}")
				df_concept_year = pd.concat([df_concept_year, df_year.sort_values(by=['filed'])[['frame', 'val']].tail(1)])
		else:
			logging.info(f"edgar.get_financial_concept_from_json: Value for year {year}: {df_year['val'].iloc[0]}")
			df_concept_year = pd.concat([df_concept_year, df_year[['frame', 'val']]])
	df_concept_year['frame'] = df_concept_year['frame'].map(translate_frame_to_fiscal_year)
	df_concept_year.rename(columns={'frame': 'FY', 'val': array_concept[0]}, inplace=True)
	logging.info(f"edgar.get_financial_concept_from_json: Returned table\n{df_concept_year}")
	return df_concept_year


def build_basic_financials_table(array_financial_concepts, json_raw_data):
	"""This function returns a dataframe with the basic financials table, extracted from the received json raw data"""
	df_empty = pd.DataFrame()
	df_table = pd.DataFrame()
	df_column = pd.DataFrame()
	for concept in array_financial_concepts:
		df_column = get_financial_concept_from_json(concept, json_raw_data)
		df_table = pd.concat([df_table.reset_index(drop=True), df_column.reset_index(drop=True)], axis=1)
		df_table = df_table.loc[:,~df_table.columns.duplicated()]
	logging.info(f"edgar.build_basic_financials_table: Updating columns headers")
	try:
		df_table.rename(columns=config.columns_translation, inplace=True)
	except Exception as err:
		logging.warning(f"edgar.build_basic_financials_table: Error updating columns headers: {err}")
	logging.info(f"edgar.build_basic_financials_table: Returned table\n{df_table}")
	return df_table


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
	