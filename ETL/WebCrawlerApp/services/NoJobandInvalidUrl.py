__author__ = 'Santhosh Emmadi'
import sys 
import os
import re
import traceback
from datetime import date
import os, sys
import logging
from logging import handlers
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select
import time

LOG_ROTATE = 'd'
BASE_DIR = os.getcwd()
LOG_FORMATTER = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
LOG_LEVEL = logging.DEBUG

cur_timestamp = str(int(time.time()))

try:
    os.makedirs(os.path.join(BASE_DIR, "logs"))
except FileExistsError:
    pass

try:
    os.makedirs(os.path.join(BASE_DIR, cur_timestamp))
except FileExistsError:
    pass

def getAppLogger(name):
    LOG_FILE = os.path.join(BASE_DIR, 'logs', '{}_{}.log'.format(name, date.today().strftime("%Y-%B-%d")))
    handler = handlers.TimedRotatingFileHandler(LOG_FILE, when=LOG_ROTATE)
    handler.setFormatter(LOG_FORMATTER)
    logger = logging.getLogger(str(name))
    logger.addHandler(handler)
    logger.setLevel(LOG_LEVEL)
    return logger

chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument("start-maximized")


logger = getAppLogger("nolatestjobs")

ip_cmp_df = pd.read_excel("media/URLStocheck_15thMar2023.xlsx")
logger.info("No Latest Jobs Process start No URL'S : %s", ip_cmp_df.shape[0])

success_csv = open("media/success_new.csv", "w", encoding="utf-8")
success_csv.write("ID, CDMSID, PatternName,  Status, Comments, URL\n")

failed_csv = open("media/failed_new.csv", "w", encoding="utf-8")
failed_csv.write("ID, CDMSID, PatternName, Status,Comments, URL\n")
driver = webdriver.Chrome(f"media/drivers/chromedriver.exe",chrome_options=chrome_options)
i = 1
# exist_cmp = pd.read_csv("media/final.csv")
# exist_ids = exist_cmp["ID"].tolist()
ip_cmp_df = ip_cmp_df.loc[ip_cmp_df['PatternName'].isin(['greenhouse', 'lever', 'bamboohr'])]
for idx, each_cmp in ip_cmp_df.iterrows():
    try:
        status = None
        driver.get(each_cmp['careerURL'])
        time.sleep(10)
        cleantext = BeautifulSoup(driver.page_source, "lxml").text
        cleantext = cleantext.replace("\n", "")
        cleantext = cleantext.replace("  ", " ")
        html_file = open(os.path.join(BASE_DIR, str(cur_timestamp),"{}_{}.html".format(each_cmp['cdmsID'],each_cmp['id'])), 'w', encoding="utf-8")
        # check NOT_WORKING_URL_KEYWORDS
        row_data = None
        for each_keyword in NOT_WORKING_URL_KEYWORDS:
            if each_keyword.lower().strip() in cleantext.lower():
                row_data = f"{each_cmp['id']},{each_cmp['cdmsID']},{each_cmp['PatternName']},URLNotWorking,  {each_keyword.replace(',', '')}, {each_cmp['careerURL']}"
                #print (row_data)
                logger.info(row_data)
                status = "URLNotWorking"
                break
        if row_data is None:
            # check NO_JOBS_KEYWORDS
            for each_keyword in NO_JOBS_KEYWORDS:
                if each_keyword.lower().strip() in cleantext.lower():
                    row_data = f"{each_cmp['id']},{each_cmp['cdmsID']},{each_cmp['PatternName']},NoJobs,   {each_keyword.replace(',', '')}, {each_cmp['careerURL']}"
                    logger.info(row_data)
                    #print(row_data)
                    status = "NoJobs"
                    break
            else:
                row_data = f"{each_cmp['id']},{each_cmp['cdmsID']},{each_cmp['PatternName']},URLWorking,  NoJobsKeywordsNotmatched, {each_cmp['careerURL']}"
                #print(row_data)
                logger.info(row_data)
                status = "NoJobsKeywordsNotmatched"

        #print (row_data)
        success_csv.write(f"{row_data}\n")
        #logger.info(cleantext)
        html_file.write(driver.page_source)
        html_file.close()
        driver.get("data:,")
        time.sleep(1)
    except Exception as e:
        traceback.print_exc()
        row_data = f"{each_cmp['id']},{each_cmp['cdmsID']},{each_cmp['PatternName']},  {e},{each_cmp['careerURL']}"
        #print(row_data)
        status = f"Exception:{e}"
        failed_csv.write(f"{row_data}\n")
    print (i, each_cmp['id'], each_cmp['careerURL'], status)
    i = i+1

success_csv.close()
failed_csv.close()
logger.info("No Latest Jobs Process End")
driver.quit()