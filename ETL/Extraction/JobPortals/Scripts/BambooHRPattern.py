__author__ = 'Santhosh Emmadi'


"""
This is API Patten Component and handles API related webcrawling pattern companies
"""
import sys, os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import traceback,re
import json
from random import randint
import time
from Settings import JA_CRAWL_STATUS_MASTER as CRAWL_STATUS, MAX_PAGE_SOURCE_LENGTH, MAX_ERROR_LOG_LENGTH, JOB_EXLUDE_KEYWORDS, HTML_TAG_SEPERATOR
from main.GenericWebCrawlerAdapter import GenericWebCrawlerBaseAdapterClass

class BambooHRPatternClass(GenericWebCrawlerBaseAdapterClass):
    def __init__(self, DailySpiderID, PatternID, CallDescOnly=False, **kwargs):
        super().__init__(kwargs.get('DBEnv'), DailySpiderID, PatternID)
        self.CDMSID = kwargs.get('CDMSID', 0)
        self.URLSNoList = kwargs.get('URLSNoList', [])
        self.PyResourceName =  kwargs.get('PyResourceName', '').strip()
        self.CallDescOnly = CallDescOnly

    def __repr__(self): 
        return self.__class__.__name__

    @property
    def CurrentFilePath(self):
        return str(os.path.abspath(__file__))
    
    def getCompanyList(self, **kwargs):
        return self.getCompanies(self.CDMSID, self.URLSNoList, self.PyResourceName, **kwargs)

    def startCrawl(self,CompanyData, HTMLTagData, CrawlStatusRecord, **kwargs):
        PatternFuncName = kwargs['FunctionName']
        StartTime = self.getTime()
        self.logger.info("***%s: Start. CompanyData: %s, HTMLTagData: %s, CrawlStatusRecord: %s, Kwargs: %s***",PatternFuncName, CompanyData, HTMLTagData, CrawlStatusRecord, kwargs)
        print (PatternFuncName, CompanyData, HTMLTagData, CrawlStatusRecord, kwargs)
        ErrorLogSet = set()
        SuccessTotal, FailedTotal = (0,0)
        PageNo = 1
        PageHeaders = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        }
        try:
            self.logger.info("# Step-4. Crawling all links for a particular URL Sno")
            Domain = CompanyData['jobUrl'].split('.bamboohr')[0].split('/')[-1]
            APIURL ='https://%s.bamboohr.com/careers/list' % (Domain)
            if not self.CallDescOnly:
                self.logger.info("# Step-4.1 Check Crawl Description url's only or not?")
                ReqStatus, APIResponse = self.sendRequest("GET", APIURL, verify=False, headers=PageHeaders)
                try:
                    APIResult = APIResponse.json()
                except Exception as e:
                    ErrorLogSet.add(str(traceback.format_exc()))
                    self.logger.error("%s: API JSON Response  Error %s",PatternFuncName, traceback.format_exc())
                    ErrorLog = self.formatErrorSet(ErrorLogSet)
                    self.logger.info("Pagination completed %s" % PageNo)
                    CrawlStatusData = {"CrawlStatus" : CRAWL_STATUS["Failed"] , "SuccessTotal" : SuccessTotal, "FailedTotal" : FailedTotal, "ErrorLog" : ErrorLog[:MAX_ERROR_LOG_LENGTH], "HTMLText" : APIResponse.text}

                #self.sleep(self.randint(5,15))
                JobsListTag = APIResult.get('result', [])
                if len(JobsListTag) == 0:
                    try:
                        CleanTxt =  self.getcleanText(APIResponse.text)
                    except AttributeError:
                        CleanTxt =  self.getcleanText(APIResponse)
                    PageStatus, PageSourceData = self.checkURLStatusandNoJobs(CleanTxt)
                    CrawlStatusData = {"CrawlStatus" : CRAWL_STATUS['NoJobs'], "SuccessTotal" : SuccessTotal, "FailedTotal" : FailedTotal, "HTMLText" : str(CleanTxt)[:MAX_PAGE_SOURCE_LENGTH], "ErrorLog" : str(PageSourceData)[:MAX_ERROR_LOG_LENGTH] }
                    self.logger.info(CrawlStatusData)
                    self.updateCrawlStatusByURLSNo(CompanyData['Sno'], **CrawlStatusData)
                    self.logger.info("*** %s Process End, elapsedTime %s (in Minutes)***", PatternFuncName, self.getElapsedTitme(StartTime))
                    return CrawlStatusData
                for JobItem in JobsListTag:
                    JobData = {}
                    JobData['JobTitle'] =  JobItem.get('jobOpeningName', '').strip().replace("'", "''")
                    JobData['JobURL'] = 'https://%s.bamboohr.com/careers/%s' % (Domain, str(JobItem.get('id', '')))
                    JobLocation = ''
                    try:
                        if JobItem.get('location', {}).get('city', ''):
                            JobLocation = JobItem.get('location', {}).get('city', '')
                        if JobItem.get('location', {}).get('state', ''):
                            JobLocation = JobLocation + ', ' + JobItem.get('location', {}).get('state', '')
                        if JobItem.get('location', {}).get('country', ''):
                            JobLocation = JobLocation + ', ' + JobItem.get('location', {}).get('country', '')
                    except Exception as e:
                        JobData['JobLocation'] = JobLocation
                    try:
                        JobData['OutputTableName'] = kwargs['OutputTableName']
                        JobData['JobPostDate'] = ''
                        JobData['JobLocation'] = ''
                        DBStatus, ErrorLog = self.insertJob(CompanyData['Sno'], **JobData)                            
                        assert DBStatus == True
                        SuccessTotal +=1
                    except AssertionError:
                        ErrorLogSet.add(ErrorLog)
                        self.logger.error("%s: JobInsertionError %s",PatternFuncName ,ErrorLog)
                        FailedTotal +=1 
                    except Exception as e:
                        ErrorLogSet.add(str(e))
                        self.logger.error("%s: JobExtraction Error %s",PatternFuncName, traceback.format_exc())
                        FailedTotal +=1
                    PageNo +=1
                    #self.sleep(self.randint(5,15))
                #self.sleep(self.randint(5,15))
            if self.CallDescOnly:
                self.logger.info("# Step-4.4 get Jobs SuccessTotal and FailedTotal from CrawlStatus Table When Call Description only ")
                SuccessTotal, FailedTotal = (CrawlStatusRecord.get('SuccessTotal', 0), CrawlStatusRecord.get('FailedTotal', 0))
            
            self.logger.info("# Step-5. Identifying new jobs with help of update query from cosolidated table")
            DBStatus, ErrorLog = self.updateJobsDescByURLSNo(CompanyData['Sno'], kwargs['OutputTableName'])
            self.logger.error("*** %s: updateJobsDescByURLSNo %s ***",PatternFuncName,  ErrorLog)

            self.logger.info("# Step-6.  Getting description for new jobs only")
            DBStatus, JobResult = self.getNewJobsByURLSNo(CompanyData['Sno'], kwargs['OutputTableName'])
            for JobRecord in JobResult:
                # self.logger.info(JobRecord)
                try:
                    self.sleep(30)
                    DescURL = JobRecord['JobTitle'] + '/detail'
                    DescReqStatus, DescResponse = self.sendRequest("GET", DescURL, verify=False)
                    try:
                        DescResult = DescResponse.json()
                        try:
                            JobPostDate = DescResult.get('result', {}).get('jobOpening', {}).get('datePosted', '')
                        except Exception as e:
                            JobPostDate = self.CurrentDateStr
                        JobDescHTMLTxt = DescResult.get('result', {}).get('jobOpening', {}).get('description', '')
                        ScrapyStatus, ScrapyResult = self.getScrapyResult(JobDescHTMLTxt, text=True)
                        if ScrapyStatus:
                            JobDescTxt = ''.join(ScrapyResult.xpath('//text()').extract()).replace('\r\n\t', '').replace('\r', '').replace('\n', ' ').replace('\t', '').replace('  ', ' ').replace('\xa0', '').strip()
                        else:
                            JobDescTxt = str(DescResult)
                    except Exception as e:
                        print(e)
                        JobDescTxt = ''
                    self.updateJobDescPostDate(JobRecord['Sno'], JobDescTxt.replace("'", "''"), JobPostDate   ,kwargs['OutputTableName'])
                except Exception as e:
                    print(e)
                    ErrorLogSet.add(str(traceback.format_exc()))
                self.logger.error("*** %s: getJobDesc Extraction Failed. ErrorLog: %s***", PatternFuncName, traceback.format_exc())
            ErrorLog = self.formatErrorSet(ErrorLogSet)
            CrawlStatusData = {"CrawlStatus" : CRAWL_STATUS["Failed"] if FailedTotal > 0 else CRAWL_STATUS["Success"], "SuccessTotal" : SuccessTotal, "FailedTotal" : FailedTotal,"ErrorLog" : ErrorLog[:MAX_ERROR_LOG_LENGTH] }
        except Exception as ex:
            ErrorLogSet.add(str(traceback.format_exc()))
            ErrorLog = self.formatErrorSet(ErrorLogSet)
            CrawlStatusData = { "CrawlStatus" : CRAWL_STATUS['Failed'], "ErrorLog": ErrorLog[:MAX_ERROR_LOG_LENGTH], "SuccessTotal" : SuccessTotal,"FailedTotal":FailedTotal, "HTMLText":str(ex)}
            self.logger.error("%s: Crawling ErrorLog %s",PatternFuncName, traceback.format_exc())
        self.logger.info("*** %s Process End, elapsedTime %s (in Minutes) ***", PatternFuncName, self.getElapsedTitme(StartTime))
        return CrawlStatusData

if __name__ == '__main__':
    BambooHRPatternClass(DailySpiderID=3, PatternID=5, CDMSID=3396689, URLSNoList=[113315]).run()