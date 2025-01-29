import os
import logging
import re
from datetime import datetime

############## EDGAR CONFIGURATION ##########

#Define the potential synonims for the different financial concepts in EDGAR taxonomies
revenues = ["Revenues", "SalesRevenueNet", "SalesRevenueServicesNet", "RevenuesNetOfInterestExpense", "RealEstateRevenueNet", "RevenueFromContractWithCustomerIncludingAssessedTax", \
            "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueGoodsNet", "RevenuesExcludingInterestAndDividends", "RegulatedAndUnregulatedOperatingRevenue"]
costOfRevenues = ["CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold", "CostOfServices", "CostOfOtherPropertyOperatingExpense"]
grossProfit =  ["GrossProfit"]
researchDevelopmentExpense =  ["ResearchAndDevelopmentExpense", "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"]
operatingExpenses = ["OperatingExpenses", "CostsAndExpenses", "OperatingCostsAndExpenses", "OperatingExpensesCogs", "BenefitsLossesAndExpenses"]
operatingIncome = ["OperatingIncomeLoss"]
interestExpenseNet = ["InterestIncomeExpenseNonoperatingNet", "InterestExpenseOperating"]
interestExpense = ["InterestExpense"]
interestIncome = ["InvestmentIncomeInterest", "InterestIncomeOther"]
incomeContinuingOperations = ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"]
taxesExpense = ["IncomeTaxExpenseBenefit"]
netIncome = ["NetIncomeLoss", "NetIncomeLossAvailableToCommonStockholdersBasic", "ProfitLoss"]
dilutedEps = ["EarningsPerShareDiluted"]
dilutedSharesOutstanding = ["WeightedAverageNumberOfDilutedSharesOutstanding"]

cashAndEquivalents = ["CashAndCashEquivalentsAtCarryingValue"]
inventories = ["InventoryNet"]
currentAssets = ["AssetsCurrent", "InvestmentsAndCash"]
propertyAndEquipment = ["PropertyPlantAndEquipmentNet", "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization"]
goodwill =  ["Goodwill"]
intangibleAssets = ["IntangibleAssetsNetExcludingGoodwill", "FiniteLivedIntangibleAssetsNet"]
assets = ["Assets"]
shortTermDebt = ["DebtCurrent", "LongTermDebtCurrent", "ShortTermBorrowings"]
currentLiabilities = ["LiabilitiesCurrent"]
longTermDebt = ["LongTermDebtNoncurrent", "LongTermDebtAndCapitalLeaseObligations", "OtherLongTermDebtNoncurrent"]
liabilities = ["Liabilities"]
retainedEarnings = ["RetainedEarningsAccumulatedDeficit"]
equity = ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]

amortization = ["AmortizationOfIntangibleAssets"]
depreciation = ["Depreciation"]
depreciationAndAmortization = ["DepreciationDepletionAndAmortization", "DepreciationAmortizationAndAccretionNet"]
operatingCashFlow = ["NetCashProvidedByUsedInOperatingActivities", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]
capex = ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets", "PaymentsToAcquireOtherPropertyPlantAndEquipment", "PaymentsForCapitalImprovements"]
investingCashFlow = ["NetCashProvidedByUsedInInvestingActivities", "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations"]
financingCashFlow = ["NetCashProvidedByUsedInFinancingActivities", "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations"]

#Define the financial concepts needed to build the raw database
financial_statements = [revenues, costOfRevenues, grossProfit, researchDevelopmentExpense, operatingExpenses, operatingIncome, interestExpenseNet, interestExpense, interestIncome, incomeContinuingOperations, \
        taxesExpense, netIncome, dilutedEps, dilutedSharesOutstanding, cashAndEquivalents, inventories, currentAssets, propertyAndEquipment, goodwill, intangibleAssets, assets, shortTermDebt, currentLiabilities, \
        longTermDebt, liabilities, retainedEarnings, equity, amortization, depreciation, depreciationAndAmortization, operatingCashFlow, capex, investingCashFlow, financingCashFlow]

#Define a dictionary with the translations for the column names in the basic csv file generated transforming the raw json data
columns_translation = {'Revenues': 'REVENUES', 'CostOfRevenue': 'COGS', 'GrossProfit': 'GROSSPROFIT', 'ResearchAndDevelopmentExpense': 'R&D', 'OperatingExpenses': 'OPEXPENSES', \
                       'OperatingIncomeLoss': 'OPINCOME', 'InterestIncomeExpenseNonoperatingNet': 'INTERESTNET', 'InterestExpense': 'INTERESTEXP', 'InvestmentIncomeInterest': 'INTERESTINC', \
                        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest': 'NETINC', 'EarningsPerShareDiluted': 'EPS', \
                        'WeightedAverageNumberOfDilutedSharesOutstanding': 'SHARES', 'CashAndCashEquivalentsAtCarryingValue': 'CASH', 'InventoryNet': 'INVENTORY', \
                        'AssetsCurrent': 'CASSETS', 'PropertyPlantAndEquipmentNet': 'PROPERTY', 'Goodwill': 'GOODWILL', 'IntangibleAssetsNetExcludingGoodwill': 'INTASSETS', \
                        'Assets': 'ASSETS', 'DebtCurrent': 'STDEBT', 'LiabilitiesCurrent': 'CLIABILITIES', 'LongTermDebtNoncurrent': 'LTDEBT', 'Liabilities': 'LIABILITIES', \
                        'RetainedEarningsAccumulatedDeficit': 'RETEARNINGS', 'StockholdersEquity': 'EQUITY', 'AmortizationOfIntangibleAssets': 'AMORTIZATION', 'Depreciation': 'DEPRECIATION', \
                        'DepreciationDepletionAndAmortization': 'DEP&AMORT', 'NetCashProvidedByUsedInOperatingActivities': 'OPCF', 'PaymentsToAcquirePropertyPlantAndEquipment': 'CAPEX', \
                        'NetCashProvidedByUsedInInvestingActivities': 'INCF', 'NetCashProvidedByUsedInFinancingActivities': 'FICF'}

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

DATABASE_PATH = os.path.dirname(__file__) + "\\DATABASE\\"
DATABASE_RAW_PATH = DATABASE_PATH + "\\RAW\\"
LOG_PATH = os.path.dirname(__file__) + "\\LOG\\"
BACKUP_PATH = os.path.dirname(__file__) + "\\BACKUP\\"
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
	

