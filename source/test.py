import edgar
import configuration as config
import pandas as pd
from datetime import datetime
import logging
import json

TICKERS_FILE = config.DATABASE_PATH + "00_INDEX_USA.csv"
RESULT_FILE = config.DATABASE_PATH + "analysis_result.txt"

def add_ciks_to_tickers_file(file=""):
    """Adds the corresponding CIK for each ticker defined in a tickers file"""
    ciks = []
    df = pd.read_csv(TICKERS_FILE)
    df['CIK'] = df['TICKER'].map(edgar.get_cik)
    #for ticker in df.iterrows():
    #    ciks.append(edgar.get_cik(ticker))
    #df['CIK'] = ciks
    df.to_csv(TICKERS_FILE, index=False)

def check_file(file=""):
    """Checks for each ticker in in the tickers file, if it is present in the proided CSV file with a financial concept"""
    df_financials = pd.read_csv(file)
    tickers = open(TICKERS_FILE)
    for line in tickers.readlines():
        ticker = line.split(",")[0]
        if ticker != "TICKER":
            try:
                c = int(line.split(",")[1].lstrip("0"))
            except Exception as err:
                print(f"CIK cannot be retrieved for {ticker}")
            else:
                df_result = df_financials.query('cik == @c')[['cik', 'entityName', 'ccp', 'val', 'tag', 'label', 'end']].sort_values(by=['ccp'])
                with open(RESULT_FILE, "a") as f:
                    f.write(f"\n\n{ticker} - {c}")
                    f.write(df_result.to_string())




############## TESTS ##########
#Set up logging. SWITCH LEVEL TO INFO if you want to depurate
LOG_FILE = config.LOG_PATH + datetime.today().strftime('%Y-%m-%d') + ".log"
logging.basicConfig(filename=LOG_FILE,
	format="%(asctime)s - [%(levelname)s] - %(message)s", datefmt="%H:%M:%S", 
	level=logging.INFO)

logging.info("****************************************************************")
edgar.download_company_raw_json("AAPL")
#config.check_folders()
#edgar.create_raw_database()
#edgar.create_table(config.operatingExpenses)
#check_file(OPERATING_EXPENSES_FILE)
#edgar.download_company_raw_json("ABBV")
#with open(ABBV_JSON_RAW_FILE, 'r') as file:
#    json_data = json.load(file)
#df2 = edgar.get_financial_concept_from_json(["Dummy"], json_data)
#df2 = edgar.get_financial_concept_from_json(config.interestIncome, json_data)
#df3 = edgar.build_basic_financials_table(config.financial_statements, json_data)
#print(df2)

#edgar.add_company_to_database("PFE")
