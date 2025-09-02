"""
This is Application Web crawling Repository Class And it containts All WebScraiping related modules.
Common interface for a group of subclasses.
"""
import sys
import os
from urllib.request import Request, urlopen
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from math import ceil
import requests
import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import traceback
import time
from random import randint
from abc import ABC, abstractmethod
from dateutil.parser import (parse as dateparser)
from datetime import date, datetime
from scrapy.selector import Selector
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from Settings import BASE_DIR, DAILY_SPIDER_CONFIG, CLEAN_TEXT_LIST, JA_CRAWL_STATUS_MASTER as CRAWL_STATUS
from utils.CommonFunctions import getHash, getAppLogger, cleanText
from main.GenericDBRepository import GenericDBRepositoryClass


warnings.filterwarnings("ignore", category=DeprecationWarning)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class GenericWebCrawlerBaseAdapterClass(ABC, GenericDBRepositoryClass):
    
    def __init__(self, DBEnv, DailySpiderID, PatternID, **kwargs):
        super().__init__(DBEnv)
        self.DailySpiderID = int(DailySpiderID)
        self.PatternID = int(PatternID)
        self.PhaseDetails = {}
        self.PatternDetails = {}
        self.URLScenariosKeywordDetails = {}
        self.logger = getAppLogger(self.LoggerName, Stream=True)
        self.dblogger = getAppLogger(f"{self.LoggerName}_DB")

    def __repr__(self): 
        return self.__class__.__name__

    @property
    def LoggerName(self):
        LogDir = os.path.join(BASE_DIR, "logs", self.DailySpiderName, str(self.PhaseNumber))
        os.makedirs(LogDir,exist_ok=True)
        try:
            LogNameStr = f"{self.DailySpiderName}/{self.PhaseNumber}/PatternID-{self.PatternID}"
        except Exception:
            LogNameStr = f"{self.DailySpiderName}"
        return LogNameStr    

    @staticmethod
    def parseDate(date_str, date_format="%Y-%m-%d"):
        return dateparser(date_str).strftime(date_format)

    @staticmethod
    def parseURL(url):
        return urlparse(url)

    @staticmethod
    def getTime():
        return time.time()

    @staticmethod
    def sleep(sec):
        time.sleep(sec)

    @staticmethod
    def regex(pattern):
        return re.compile(pattern)
    # can be extended with re.pattern later

    @staticmethod
    def ceil(float):
        return ceil(float)

    @staticmethod
    def randint(min, max):
        return randint(min, max)

    @property
    def CurrentDateTime(self):
        return datetime.now()

    @staticmethod
    def getElapsedTitme(StartTime):
        return round(float((__class__.getTime() - StartTime))/float(60), 2)
    
    @property
    def CurrentDate(self):
        return self.CurrentDateTime.today()

    @property
    def CurrentDateStr(self):
        return self.CurrentDate.strftime("%Y-%m-%d")

    @property
    def CurrentDateTimeStr(self):
        return self.CurrentDateTime.strftime("%Y-%m-%d %H:%M-%S")

    @property
    def DailySpiderName(self):
        return DAILY_SPIDER_CONFIG[self.DailySpiderID]

    def sendRequest(self, Method, Url, **kwargs):
        try:
            Response = requests.request(Method, Url, **kwargs)
            return True, Response
        except Exception as ex:
            self.logger.error("sendRequest: %s", traceback.format_exc())
            return False, traceback.format_exc()

    def sendUrlopenRequest(self, Url, **kwargs):
        try:
            req = Request(url=Url, **kwargs)
            Response=urlopen(req).read()
            return True, Response
        except:
            try:
                Response = requests.get(Url, **kwargs)
                return True, Response.text
            except Exception as ex:
                self.logger.error("sendUrlopenRequest: %s", traceback.format_exc())
                return False, traceback.format_exc()

    def sendSessionRequest(self, Method, Url, Retries=5, BackoffFactor=0.5, StatusForcelist=None, **kwargs):
        """
        Sends an HTTP request using a session with a retry mechanism.
        """
        headers = kwargs.get('headers', None)
        session = requests.Session()
        try:
            retry = Retry(
                total=Retries,
                backoff_factor=BackoffFactor,
                status_forcelist=StatusForcelist or [500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            if headers:
                session.headers.update(headers)
            response = session.request(Method, Url, **kwargs)
            return True, response.text
        except Exception as ex:
            return False, traceback.format_exc()
        finally:
            session.close()


    def getBSoupResult(self, Response,**kwargs):
        try:
            Result = BeautifulSoup(Response, kwargs["Name"])
            return True, Result
        except KeyError:
            Result = BeautifulSoup(Response)
            return True, Result
        except Exception as ex:
            self.logger.error("getBSoupResult: %s", traceback.format_exc())
            return False, traceback.format_exc()

    def getScrapyResult(self, Response,  **kwargs):
        try:
            if kwargs.get('text'):
                Result = Selector(text=Response)
            else:
                Result = Selector(Response)
            return True, Result
        except Exception as ex:
            self.logger.error("getScrapyResult: %s", traceback.format_exc())
            return False, traceback.format_exc()

    @property
    def PhaseNumber(self):
        if self.PhaseDetails == {}:
            DBStatus, self.PhaseDetails = self.getMaxPhaseNumber(self.DailySpiderName)
        return self.PhaseDetails['PhaseNumber']

    @property
    def URLScenariosKeywords(self):
        if self.URLScenariosKeywordDetails == {}:
            DBStatus, DBResult = self.getURLScenariosKeywordDetails()
            for eachRow in DBResult:
                try:
                    self.URLScenariosKeywordDetails[eachRow['CrawlStatusID']].append(eachRow['KeywordText'])
                except KeyError:
                    self.URLScenariosKeywordDetails[eachRow['CrawlStatusID']] = [eachRow['KeywordText']]
        return self.URLScenariosKeywordDetails

    def getURLSNoCrawlStatus(self, URLSNo):
        DBStatus, Result = self.getURLCrawlStatusByPhaseNumberAndURLSNo(self.DailySpiderName, self.PhaseNumber, URLSNo)
        if not DBStatus:
            self.insertCrawlStatus(URLSNo)
            DBStatus, Result = self.getURLCrawlStatusByPhaseNumberAndURLSNo(self.DailySpiderName, self.PhaseNumber, URLSNo)
        return Result

    def insertCrawlStatus(self, URLSno, **kwargs):
        kwargs['FilePath']=self.CurrentFilePath
        return self.insertPhaseCrawlStatus(self.DailySpiderID, self.PhaseNumber, URLSno, **kwargs)

    def insertJob(self, URLSNo,  **kwargs):
        return self.saveJob(self.DailySpiderName, self.PhaseNumber, URLSNo, **kwargs)

    def updateCrawlStatusByURLSNo(self, URLSNo, **kwargs):        
        kwargs['FilePath']=self.CurrentFilePath
        return self.updateURLPhaseCrawlStatusByURLSNo(self.DailySpiderName, self.PhaseNumber, URLSNo,  **kwargs)

    def updateJobsDescByURLSNo(self, URLSNo,OutputTableName,  **kwargs):
        return self.updateJobsDescStatusByURLSNo(self.DailySpiderName, self.PhaseNumber, URLSNo, OutputTableName,  **kwargs)

    def getNewJobsByURLSNo(self, URLSNo,OutputTableName, **kwargs):
        return self.getNewJobsOnNoDescByURLSNo(self.DailySpiderName, self.PhaseNumber, URLSNo, OutputTableName, **kwargs)

    def updateJobDesc(self, JobSNo, JobDesc, OutputTableName):
        return self.updateJobsDescByJobSNo(self.DailySpiderName, JobSNo, JobDesc, OutputTableName)

    def updateJobDescPostDate(self, JobSNo, JobDesc,JobPostDate, OutputTableName):
        return self.updateJobDescPostDateByJobSNo(self.DailySpiderName, JobSNo, JobDesc,JobPostDate, OutputTableName)

    def updateJobDescJobLocation(self, JobSNo, JobDesc, JobLocation, OutputTableName):
        return self.updateJobDescJobLocationByJobSNo(self.DailySpiderName, JobSNo, JobDesc, JobLocation, OutputTableName)

    def updateJobDescJobLocationPostDate(self, JobSNo, JobDesc, JobLocation, JobPostDate, OutputTableName):
        return self.updateJobDescJobLocationPostDateByJobSNo(self.DailySpiderName, JobSNo, JobDesc, JobLocation, JobPostDate, OutputTableName)
    
    @staticmethod
    def formatErrorSet(messageSet, length=1000):
        messageStr = ''
        for eachMsg in messageSet:
            messageStr = messageStr + eachMsg.strip().replace("\n", "")
        return messageStr[:length]

    @property
    def PatternInfo(self):
        if self.PatternDetails == {}:
            DBStatus, self.PatternDetails = self.getPatternDetailsByPatterID(self.PatternID)
        return self.PatternDetails
    
    def getHTMLTAGDetails(self, PatternID, URLSNo=0):
        if URLSNo > 0:
            return self.getHTMLTagsByURLSNo(URLSNo)
        else:
            return self.getHTMLTagsByPatternID(PatternID)

    def getcleanText(self, InputTxt, Items=CLEAN_TEXT_LIST):
        return cleanText(InputTxt, Items)

    def validate(function):
        def wrapperFunc(self, CDMSID, URLSNoList, PyResourceName, **kwargs):
            ErrorList = []
            if not isinstance(CDMSID, int):
                ErrorList.append("CDMSID must be number")
            elif not isinstance(URLSNoList, list):
                ErrorList.append("URLSNo must be list")
            else:
                pass
            if len(ErrorList) > 0:
                self.logger.error("\n".join(ErrorList))
                sys.exit()
            return function(self, CDMSID, URLSNoList, PyResourceName, **kwargs)
        return wrapperFunc
            
    @validate
    def getCompanies(self, CDMSID, URLSNoList, PyResourceName, **kwargs):
        if self.PatternID >= 0 and CDMSID == 0 and len(URLSNoList) == 0  and PyResourceName == '':
            self.CompaniesDetails = self.getCompaniesByDailySpiderAndPatternID(self.DailySpiderID, self.PatternID, self.PhaseNumber)
        elif self.PatternID >= 0 and CDMSID > 0 and len(URLSNoList) == 0 and PyResourceName == '':
            self.CompaniesDetails = self.getCompaniesByDailySpiderAndPatternIDAndCDMSID(self.DailySpiderID, self.PatternID, CDMSID, self.PhaseNumber)
        elif self.PatternID >= 0 and CDMSID > 0 and len(URLSNoList) == 0 and PyResourceName != '':
            self.CompaniesDetails = self.getCompaniesByDailySpiderAndPatternIDAndCDMSIDAndPyResource(self.DailySpiderID, self.PatternID, self.CDMSID, PyResourceName, self.PhaseNumber)
        elif self.PatternID >=0 and CDMSID > 0 and len(URLSNoList) > 0 and PyResourceName == '':
            self.CompaniesDetails = self.getCompaniesByDailySpiderAndCDMSIDAndPatternIDAndURLSNo(self.DailySpiderID, self.PatternID, CDMSID, URLSNoList, self.PhaseNumber)        
        elif self.PatternID >= 0 and CDMSID == 0 and len(URLSNoList) ==0 and PyResourceName != '':
            self.CompaniesDetails = self.getCompaniesByDailySpiderAndPatternIDAndPyResource(self.DailySpider, self.PatternID, PyResourceName,self.PhaseNumber)
        return self.CompaniesDetails

    def checkURLStatusandNoJobs(self, HTMLPageTxt):
        NOT_WORKING_URL_KEYWORDS =  self.URLScenariosKeywords[CRAWL_STATUS['URLNotWorking']]
        NO_JOBS_KEYWORDS =  self.URLScenariosKeywords[CRAWL_STATUS['NoJobs']]
        for Item in NOT_WORKING_URL_KEYWORDS:
            if Item.lower() in HTMLPageTxt.lower():
                return "URLNotWorking", Item
        for Item in NO_JOBS_KEYWORDS:
            if Item.lower() in HTMLPageTxt.lower():
                return "NoJobs", Item
        return "Failed", HTMLPageTxt

    @abstractmethod
    def getCompanyList(self):
        pass
    
    def run(self):
        StartTime = self.getTime()
        self.logger.info("** %s Process Start DateTime: %s.**", self, self.CurrentDateTimeStr)
        try:
            CMPStatus, CmpResult = self.getCompanyList()
            self.logger.info("**CMPStatus: %s  CmpResult%s.**", CMPStatus, CmpResult)
            if not CMPStatus or len(CmpResult) == 0:
                self.logger.error("CmpResult: No Records Found on CDMSID: %s, URLSNoList: %s, PatternID: %s, DailySpiderID %s, ErrorLog:%s", self.CDMSID, self.URLSNoList, self.PatternID, self.DailySpiderID, CmpResult)

            for CmpRecord in CmpResult:
                self.logger.info(CmpRecord)
                self.logger.info("*#"*30)
                self.logger.info("# Step-1. Get URLSno,HTMLTags, PatternInfo & CrawlStatus Details")
                CrawlStatusRecord =  self.getURLSNoCrawlStatus(CmpRecord['Sno'])
                HTMLTagStatus, HTMLTagRecord = self.getHTMLTAGDetails(self.PatternID)
                CrawlStatusData = {"CrawlStatus" : CRAWL_STATUS["WIP"], "SuccessTotal" : 0, "FailedTotal" : 0}
                self.logger.info(CrawlStatusData)
                self.logger.info("# Step-2. Update URLSno CrawlStatus to Running")
                self.updateCrawlStatusByURLSNo(CmpRecord['Sno'], **CrawlStatusData)
                PatternFuncName = f"run{self.PatternInfo['FunctionName']}"
                self.logger.info("# Step-3. Call Job Function: %s", PatternFuncName)
                CrawlStatusData = self.startCrawler(CmpRecord, HTMLTagRecord, CrawlStatusRecord, **self.PatternInfo)
                self.logger.info(CrawlStatusData)
                self.logger.info("# Step-7. Update URLSno CrawlStatus to Success/Failed once process Completed")
                self.updateCrawlStatusByURLSNo(CmpRecord['Sno'], **CrawlStatusData)
                self.logger.info("*#"*30)
        except Exception as ex:
            self.logger.info("** %s Process failed. Exception: %s, ErrorLog: %s **",self, ex, traceback.format_exc())
            self.logger.info("*#"*30)
        self.logger.info("** %s Process End, DateTime:%s. elapsedTime %s(in Minutes)**",self, self.CurrentDateTimeStr,  self.getElapsedTitme(StartTime))
