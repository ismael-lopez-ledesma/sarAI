import os
import logging
import re
from datetime import datetime

############## GLOBAL CONFIGURATION ##########

DATABASE_PATH = os.path.dirname(os.path.dirname(__file__)) + "\\database\\"
BACKUP_PATH = DATABASE_PATH + "\\backup\\"
LOG_PATH = os.path.dirname(os.path.dirname(__file__)) + "\\log\\"

############## EDGAR CONFIGURATION ##########

#URLs for the API
companyFactsURL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json"

#URL to browse in EDGAR site and scrap CIK and company activity
BROWSE_URL = "https://www.sec.gov/cgi-bin/browse-edgar?CIK={}&owner=exclude&action=getcompany&Find=Search"

#URL to download SEC data
DOWNLOAD_URL = "https://www.sec.gov/Archives/edgar/data/{}"

#Headers to be included in connections to EDGAR, otherwise the requests are rejected
EDGAR_HEADERS = {'User-agent' : 'myAnalizer@stocks.com'}

#Error messages reported by EDGAR site
ERROR_MESSAGE1 = "No matching Ticker Symbol"
ERROR_MESSAGE2 = "The value you submitted is not valid"

#File with a list of companies to be included in the EDGAR training database
EDGAR_INDEX_FILE_PATH = DATABASE_PATH + "00_INDEX_USA.csv"

#Path to the EDGAR training csv file
EDGAR_TRAINING_FILE = DATABASE_PATH + "01_EDGAR_TRAINING_TABLE.csv"

############## YAHOO CONFIGURATION ##########

PROFILE_URL = "https://finance.yahoo.com/quote/{}/profile/"
YAHOO_HEADERS = {'User-agent' : 'Mozilla/5.0'}
ERROR_MESSAGE3 = "No results for"
