__author__ = 'Santhosh Emmadi'
"""
This file contains application main settings like Application version, project & media directory path  
Database table mapping, environment.
"""
APP_VERSION = 1.0
import os, sys, logging
from logging import handlers
from datetime import date
import traceback
import os, sys, logging
from logging import handlers
from datetime import date


PROJECT_DIR = os.path.join(os.getcwd())
BASE_DIR = os.getcwd()
DEFAULT_DB_ENV = 'DEV'
LOG_ROTATE = 'D'
IS_LOGGER_DISABLED = False

DIR_LIST = ["logs", "media", "media/drivers", "media/sql"]
for each_dir in DIR_LIST:
    os.makedirs(os.path.join(BASE_DIR, each_dir), exist_ok=True)
MEDIA_DIR = os.path.join(BASE_DIR, 'media')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
DRIVERS_DIR = os.path.join(BASE_DIR, 'media', "drivers")

CHROME_DRIVER_FILE_NAME = "chromedriver.exe" 
CHROME_DRIVER_EXE_FILE_PATH = os.path.join(PROJECT_DIR, DRIVERS_DIR, CHROME_DRIVER_FILE_NAME)

DB_CONFIG = {
    "DEV": {
        'driver': '{SQL Server}',
        'server': 'jobsdbsql.jp.pdm.local',
        'database': 'CareerPages',
        'uid': 'pythonuser',
        'pwd': 'JHsUJ<97$jgn',
        'timeout' : 5
    },
    "PROD": {
        'driver': '{SQL Server}',
        'server': 'jobsdbsql.jp.pdm.local',
        'database': 'CareerPages',
        'uid': 'pythonuser',
        'pwd': 'JHsUJ<97$jgn',
        'timeout' : 30
    },
}
DAILY_SPIDER_CONFIG = {3 : "NonTier1", 4: "Tier1"}
JA_CRAWL_STATUS_MASTER = { "YetToRun" : 0, "Success" : 1, "Failed" : 2, "NoJobs" : 3, "WIP":4, "URLNotWorking": 5 }
COMPONENT_CONFIG = {1: "BSoupPatternComponentClass", 2: "ScrapyPatternComponentClass", 4 : "APIPatternComponentClass"}
JOB_EXLUDE_KEYWORDS =["dont see your dream job"]
CLEAN_TEXT_LIST = ["\n", "\r", "\xa0", "'", '"', "“", "%", "’"]
MAX_PAGE_SOURCE_LENGTH = 1000
MAX_ERROR_LOG_LENGTH = 1000
REQUEST_TIMEOUT = 10
HTML_TAG_SEPERATOR  = "#GDRC#"

JA_TBL_CONFIG = {
    "Company_Registries": "[CareerPages].[dbo].[CP_Companies]",
    "PatternMaster" : "[CareerPages].[dbo].[CP_Patterns_Master]",
    "CompaniesTags" : "[CareerPages].[dbo].[CP_CompaniesTags]",
    "URL_Scenarios_Master" : "[CareerPages].[dbo].[CP_URLScenariosMaster]",
    "Tier1_CRAWL_STATUS": "[CareerPages].[dbo].[CP_Tier1_CrawlStatus]",
    "NonTier1_CRAWL_STATUS": "[CareerPages].[dbo].[CP_NonTier1_CrawlStatus]",
    
    "Tier1_Phase": "[CareerPages].[dbo].CP_Tier1_Phase",
    "NonTier1_Phase": "[CareerPages].[dbo].CP_NonTier1_Phase",    
    "Tier1_Jobs": "[CareerPages].[dbo].[CP_Tier1_CrawlingNeptune]",
    "NonTier1_Jobs": "[CareerPages].[dbo].[CP_NonTier1_CrawlingNeptune]",
    "Tier1_ConsolidatedData" : "[CareerPages].[dbo].[CP_Tier1_ConsolidatedData]",
    "NonTier1_ConsolidatedData" : "[CareerPages].[dbo].[CP_NonTier1_ConsolidatedData]",
}


JA_TBL_COLUMN_SIZE_CONFIG = {
    "JobTitle" : 500,
    "JobURL" : 500,
    "JobPostDate" : 20,
    "JobLocation" : 100,
    "ShortCode" : 50,
}

# 1   BeautifulSoup
# 2   Scrapy
# 2   Selenium

# PyResource  Crawling Table Names
# Srikanth    Mercury
# Pravallika  Earth
# Jagadeesh   Mars
# Rupesh  Neptune
# Prasad  Uranus
# Ragavendra  Venus
# Santhosh    Venus
# Juniors   Earth_LUTemp