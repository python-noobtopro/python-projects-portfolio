"""
This is API Pattern Component and handles API related webcrawling pattern companies
"""
import sys, os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import traceback
from Settings import JA_CRAWL_STATUS_MASTER as CRAWL_STATUS, MAX_PAGE_SOURCE_LENGTH, MAX_ERROR_LOG_LENGTH
from main.GenericWebCrawlerAdapter import GenericWebCrawlerBaseAdapterClass

class TedK12PatternClass(GenericWebCrawlerBaseAdapterClass):
    def __init__(self, DailySpiderID, PatternID, CallDescOnly=False, **kwargs):
        super().__init__(kwargs.get('DBEnv'), DailySpiderID, PatternID)
        self.CDMSID = kwargs.get('CDMSID', 0)
        self.URLSNoList = kwargs.get('URLSNoList', [])
        self.PyResourceName = kwargs.get('PyResourceName', '').strip()
        self.CallDescOnly = CallDescOnly
        self.ProcessedJobs = set()  

    def __repr__(self): 
        return self.__class__.__name__

    @property
    def CurrentFilePath(self):
        return str(os.path.abspath(__file__))
    
    def getCompanyList(self, **kwargs):
        return self.getCompanies(self.CDMSID, self.URLSNoList, self.PyResourceName, **kwargs)

    def getJobDescription(self, CompanyID, JobID):
        """Helper method to fetch job description"""
        DescURL = f"https://{CompanyID}.tedk12.com/hire/ViewJob.aspx?JobID={JobID}"
        self.logger.info("Fetching description for JobID: %s from %s", JobID, DescURL)
        
        try:
            self.sleep(self.randint(2,4))
            DescReqStatus, DescResponse = self.sendRequest("GET", DescURL, verify=False)
            if not DescReqStatus:
                return '', '', ''

            ScrapyStatus, ScrapyResult = self.getScrapyResult(DescResponse.text, text=True)
            if not ScrapyStatus:
                return '', '', ''

            JobDesc = ScrapyResult.xpath('//meta[@property="og:description"]/@content').extract_first('')
            JobLocation = ScrapyResult.xpath('//span[@id="lblLocationName"]/text()').extract_first('')
            JobPostDate = ScrapyResult.xpath('//span[@id="lblApplicationStartDate"]/text()').extract_first('') or self.CurrentDateStr

            return self.getcleanText(JobDesc), self.getcleanText(JobLocation), self.getcleanText(JobPostDate)

        except Exception as e:
            self.logger.error("Failed to get job description. Error: %s", str(e))
            return '', '', ''

    def startCrawl(self, CompanyData, HTMLTagData, CrawlStatusRecord, **kwargs):
        PatternFuncName = kwargs['FunctionName']
        StartTime = self.getTime()
        self.logger.info("***%s: Start. CompanyData: %s, HTMLTagData: %s, CrawlStatusRecord: %s, Kwargs: %s***",
                        PatternFuncName, CompanyData, HTMLTagData, CrawlStatusRecord, kwargs)
        
        ErrorLogSet = set()
        SuccessTotal, FailedTotal = (0,0)
        PageHeaders = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        }
        
        try:
            self.logger.info("# Step-4. Crawling all links for a particular URL Sno")
            CompanyID = CompanyData['jobUrl'].split('.tedk12.com')[0].split('//')[-1]
            StartIndex = 0
            HasMoreJobs = True

            if not self.CallDescOnly:
                self.logger.info("# Step-4.1 Check Crawl Description url's only or not?")
                
                while HasMoreJobs:
                    APIURL = f'https://{CompanyID}.tedk12.com/hire/Index.aspx?JobListAJAX=Paging&StartIndex={StartIndex}&ListID=JobList&SearchString='
                    self.logger.info("Fetching jobs page. URL: %s", APIURL)
                    
                    ReqStatus, APIResponse = self.sendRequest("GET", APIURL, verify=False, headers=PageHeaders)
                    if not ReqStatus:
                        break

                    try:
                        ScrapyStatus, ScrapyResult = self.getScrapyResult(APIResponse.text, text=True)
                        JobsListTag = ScrapyResult.xpath('//tr[contains(@class, "row")]')
                        
                        if len(JobsListTag) == 0:
                            if StartIndex == 0:
                                try:
                                    CleanTxt = self.getcleanText(APIResponse.text)
                                except AttributeError:
                                    CleanTxt = self.getcleanText(APIResponse)
                                PageStatus, PageSourceData = self.checkURLStatusandNoJobs(CleanTxt)
                                CrawlStatusData = {
                                    "CrawlStatus": CRAWL_STATUS['NoJobs'],
                                    "SuccessTotal": SuccessTotal,
                                    "FailedTotal": FailedTotal,
                                    "HTMLText": str(CleanTxt)[:MAX_PAGE_SOURCE_LENGTH],
                                    "ErrorLog": str(PageSourceData)[:MAX_ERROR_LOG_LENGTH]
                                }
                                self.logger.info("No jobs found on first page")
                                self.updateCrawlStatusByURLSNo(CompanyData['Sno'], **CrawlStatusData)
                                return CrawlStatusData
                            HasMoreJobs = False
                            continue

                        BeforeCount = SuccessTotal
                        for JobItem in JobsListTag:
                            try:
                                JobURL = JobItem.xpath('.//strong/a/@href').extract_first('')
                                if not JobURL or JobURL in self.ProcessedJobs:
                                    continue
                                
                                JobID = JobURL.split('JobID=')[-1]
                                self.ProcessedJobs.add(JobURL)
                                JobDesc, JobLoc, JobDate = self.getJobDescription(CompanyID, JobID)
                                
                                JobData = {
                                    'JobTitle': self.getcleanText(JobItem.xpath('.//strong/a/text()').extract_first('')),
                                    'JobURL': f"https://{CompanyID}.tedk12.com/hire/{JobURL}",
                                    'JobLocation': JobLoc or self.getcleanText(JobItem.xpath('./td[4]/text()').extract_first('')),
                                    'JobPostDate': JobDate or self.getcleanText(JobItem.xpath('./td[2]/text()').extract_first('')) or self.CurrentDateStr,
                                    'JobDesc': JobDesc,
                                    'OutputTableName': kwargs['OutputTableName']
                                }
                                
                                self.logger.debug("Inserting job: %s", JobData['JobTitle'])
                                DBStatus, ErrorLog = self.insertJob(CompanyData['Sno'], **JobData)
                                assert DBStatus == True
                                SuccessTotal += 1

                            except AssertionError:
                                ErrorLogSet.add(ErrorLog)
                                self.logger.error("%s: JobInsertionError %s", PatternFuncName, ErrorLog)
                                FailedTotal += 1
                            except Exception as e:
                                ErrorLogSet.add(str(e))
                                self.logger.error("%s: JobExtraction Error %s", PatternFuncName, traceback.format_exc())
                                FailedTotal += 1

                        if SuccessTotal == BeforeCount:
                            self.logger.info("No new jobs found on page %d, ending pagination", StartIndex//50 + 1)
                            HasMoreJobs = False
                        else:
                            StartIndex += 50
                            self.sleep(self.randint(3,7))

                    except Exception as e:
                        ErrorLogSet.add(str(traceback.format_exc()))
                        self.logger.error("%s: Page processing error: %s", PatternFuncName, traceback.format_exc())
                        HasMoreJobs = False

            if self.CallDescOnly:
                self.logger.info("# Step-4.4 get Jobs SuccessTotal and FailedTotal from CrawlStatus Table When Call Description only")
                SuccessTotal, FailedTotal = (CrawlStatusRecord.get('SuccessTotal', 0), CrawlStatusRecord.get('FailedTotal', 0))
            
            self.logger.info("# Step-5. Identifying new jobs with help of update query from consolidated table")
            DBStatus, ErrorLog = self.updateJobsDescByURLSNo(CompanyData['Sno'], kwargs['OutputTableName'])
            self.logger.info("Jobs description status updated. Status: %s, Error: %s", DBStatus, ErrorLog)

            ErrorLog = self.formatErrorSet(ErrorLogSet)
            CrawlStatusData = {
                "CrawlStatus": CRAWL_STATUS["Success"] if SuccessTotal > 0 else CRAWL_STATUS["Failed"],
                "SuccessTotal": SuccessTotal,
                "FailedTotal": FailedTotal,
                "ErrorLog": ErrorLog[:MAX_ERROR_LOG_LENGTH]
            }

        except Exception as ex:
            ErrorLogSet.add(str(traceback.format_exc()))
            ErrorLog = self.formatErrorSet(ErrorLogSet)
            CrawlStatusData = {
                "CrawlStatus": CRAWL_STATUS['Failed'],
                "ErrorLog": ErrorLog[:MAX_ERROR_LOG_LENGTH],
                "SuccessTotal": SuccessTotal,
                "FailedTotal": FailedTotal,
                "HTMLText": str(ex)
            }
            self.logger.error("%s: Crawling ErrorLog %s", PatternFuncName, traceback.format_exc())

        self.logger.info("*** %s Process End, elapsedTime %s (in Minutes) ***", 
                        PatternFuncName, self.getElapsedTitme(StartTime))
        return CrawlStatusData

if __name__ == '__main__':
    TedK12PatternClass(DailySpiderID=3, PatternID=191, CDMSID=2372197).run()