#This library contains tools to get data from Yahoo website and handle different
#yahoo related data structures

import logging
#import os
from requests import get
#from datetime import datetime
from bs4 import BeautifulSoup
import configuration as config


def check_connection(yTicker=""):
	"""Checks if the connection to yahoo can be performed for the provided 
	ticker"""
	url = config.PROFILE_URL.format(yTicker)
	logging.info(f"yahoo.check_connection: Trying connection to {url}")
	try:
		r = get(url, headers=config.YAHOO_HEADERS)
	except Exception as err:
		logging.warning(f"yahoo.check_connection: Connection to {url} not possible - {err}")
		return False
	except:
		logging.warning(f"yahoo.check_connection: Unexpected error.")
		return False
	else:
		if r.status_code != 200:
			logging.warning(f"yahoo.check_connection: Connection to {url} not possible - {r.status_code}")
			return False
		else:
			if config.ERROR_MESSAGE3 in r.text:
				logging.warning(f"yahoo.check_connection: Ticker {yTicker} not found")
				return False
			else:
				logging.info(f"yahoo.check_connection: Connection to {url} working - {r.status_code}")
				return True


def get_company_sector(yTicker=""):
	"""Obtains the company sector for the provided ticker scraping from Yahoo website"""
	logging.info(f"yahoo.get_company_sector: Getting sector for ticker {yTicker}")
	if not check_connection(yTicker):
		logging.warning(f"yahoo.get_company_sector: Connection to Yahoo not possible")
		return "N/A"
	else:
		url = config.PROFILE_URL.format(yTicker)
		r = get(url, headers=config.YAHOO_HEADERS)
		soup = BeautifulSoup(r.content, "html.parser")
		try:
			sector = str(soup.find_all("dt")[0].find_next_sibling()).split('>')[2].split('<')[0].rstrip()
		except AttributeError as err:
			logging.warning(f"yahoo.get_company_sector: Sector not found for ticker {yTicker} - {err}")
			return "N/A"
		except Exception as err:
			logging.warning(f"yahoo.get_company_sector: Sector not found for ticker {yTicker} - {err}")
			return "N/A"
		else:
			logging.info(f"yahoo.get_company_sector: Sector for ticker {yTicker}: {sector}")
			return sector


def get_company_industry(yTicker=""):
	"""Obtains the company industry for the provided ticker scraping from Yahoo website"""
	logging.info(f"yahoo.get_company_industry: Getting sector for ticker {yTicker}")
	if not check_connection(yTicker):
		logging.warning(f"yahoo.get_company_industry: Connection to Yahoo not possible")
		return "N/A"
	else:
		url = config.PROFILE_URL.format(yTicker)
		r = get(url, headers=config.YAHOO_HEADERS)
		soup = BeautifulSoup(r.content, "html.parser")
		try:
			industry = str(soup.find_all("dt")[1].find_next_sibling()).split('>')[1].split('<')[0].rstrip().replace("amp;", "")
		except AttributeError as err:
			logging.warning(f"yahoo.get_company_industry: Industry not found for ticker {yTicker} - {err}")
			return "N/A"
		except Exception as err:
			logging.warning(f"yahoo.get_company_industry: Industry not found for ticker {yTicker} - {err}")
			return "N/A"
		else:
			logging.info(f"yahoo.get_company_industry: Industry for ticker {yTicker}: {industry}")
			return industry