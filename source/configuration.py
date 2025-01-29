import os
import logging
import re
from datetime import datetime

############## EDGAR CONFIGURATION ##########

#URLs for the API
endPoint = "https://data.sec.gov/api/xbrl/frames/us-gaap/{}/USD/CY{}.json"
companyFactsURL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json"

#URL to browse in EDGAR site and scrap CIK
BROWSE_URL = "https://www.sec.gov/cgi-bin/browse-edgar?CIK={}&owner=exclude&action=getcompany&Find=Search"

#URL to download SEC data
DOWNLOAD_URL = "https://www.sec.gov/Archives/edgar/data/{}"

#Headers to be included in connections to EDGAR, otherwise the requests are rejected
HEADERS = {'User-agent' : 'myAnalizer@stocks.com'}

#Error messages reported by EDGAR site
ERROR_MESSAGE1 = "No matching Ticker Symbol"
ERROR_MESSAGE2 = "The value you submitted is not valid"




############## GLOBAL CONFIGURATION ##########

DATABASE_PATH = os.path.dirname(os.path.dirname(__file__)) + "\\database\\"
BACKUP_PATH = DATABASE_PATH + "\\backup\\"
LOG_PATH = os.path.dirname(os.path.dirname(__file__)) + "\\log\\"
years = range(2009, datetime.now().year)


def check_folders():
    """Function that checks if all the needed folders are present and creates them otherwise"""
    if _check_folder(DATABASE_PATH) == False:
        logging.info(f"configuration.check_folders: Folder {DATABASE_PATH} does not exist.")
        if _create_folder(DATABASE_PATH) == 1:
            logging.info(f"configuration.check_folders: Folder {DATABASE_PATH} created.")
        else:
            logging.error(f"configuration.check_folders: Folder {DATABASE_PATH} cannot be created.")
    else:
        logging.info(f"configuration.check_folders: Folder {DATABASE_PATH} exists.")
	
    if _check_folder(DATABASE_RAW_PATH) == False:
        logging.info(f"configuration.check_folders: Folder {DATABASE_RAW_PATH} does not exist.")
        if _create_folder(DATABASE_RAW_PATH) == 1:
            logging.info(f"configuration.check_folders: Folder {DATABASE_RAW_PATH} created.")
        else:
            logging.error(f"configuration.check_folders: Folder {DATABASE_RAW_PATH} cannot be created.")
    else:
        logging.info(f"configuration.check_folders: Folder {DATABASE_RAW_PATH} exists.")
	
    if _check_folder(LOG_PATH) == False:
        logging.info(f"configuration.check_folders: Folder {LOG_PATH} does not exist.")
        if _create_folder(LOG_PATH) == 1:
            logging.info(f"configuration.check_folders: Folder {LOG_PATH} created.")
        else:
            logging.error(f"configuration.check_folders: Folder {LOG_PATH} cannot be created.")
    else:
        logging.info(f"configuration.check_folders: Folder {LOG_PATH} exists.")
        
    if _check_folder(BACKUP_PATH) == False:
        logging.info(f"configuration.check_folders: Folder {BACKUP_PATH} does not exist.")
        if _create_folder(BACKUP_PATH) == 1:
            logging.info(f"configuration.check_folders: Folder {BACKUP_PATH} created.")
        else:
            logging.error(f"configuration.check_folders: Folder {BACKUP_PATH} cannot be created.")
    else:
        logging.info(f"configuration.check_folders: Folder {BACKUP_PATH} exists.")


def _check_folder(folder=""):
	"""Checks if the provided folder exists"""
	"""Returns True if the folder exists, False otherwise"""
	if folder == "":
		logging.warning("configuration.check_folder: Folder not provided")
		return False
	if os.path.isdir(folder):
		logging.info(f"configuration.check_folder: Folder {folder} exists")
		return True
	else:
		logging.info(f"configuration.check_folder: Folder {folder} doesn't exist")
		return False


def _create_folder(folder=""):
	"""Creates a new folder"""
	"""Returns 1 if the folder is successfully created, 0 if the folder already
	exists, or -1 in case it cannot be created"""
	if folder == "":
		logging.warning("configuration.create_folder: Folder not provided")
		return -1
	try:
		os.mkdir(folder)
	except PermissionError as err:
		logging.warning(f"configuration.create_folder: Directory {folder} cannot be created - {err}")
		return -1
	except OSError as err:
		logging.warning(f"configuration.create_folder: Directory {folder} cannot be created - {err}")
		return -1
	except Exception as err:
		logging.warning(f"configuration.create_folder: Directory {folder} cannot be created - {err}.")
		return -1
	else:
		logging.info(f"configuration.create_folder: Directory {folder} created")
		return 1
	

