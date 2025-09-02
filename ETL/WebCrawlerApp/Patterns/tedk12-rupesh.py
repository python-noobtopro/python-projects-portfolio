"""
This is API Pattern Component and handles API related webcrawling pattern companies
"""
import sys, os, re
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

    def getJobDescription(self, JobURL):
        """Helper method to fetch job description"""
        self.logger.info("Fetching description for JobURL: %s", JobURL)
        PartURL = JobURL.split('hire')[0]
        JobDescAPIURL = f'{PartURL}hire/ViewJob_Description.aspx'
        try:
            if not JobURL or "JobID=" not in JobURL:
                self.logger.error("Invalid JobURL: %s", JobURL)
                return ""

            JobID = re.search(r"JobID=(\d+)", JobURL).group(1)
            Headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'priority': 'u=0, i',
                'referer': f'{PartURL}hire/ViewJob.aspx?JobID={JobID}',
                'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'iframe',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            }
            Params = {
                'JobID': JobID,
            }
            ReqStatus, HTMLResponse = self.sendSessionRequest("GET", JobDescAPIURL, params=Params, headers=Headers, verify=False)
            if ReqStatus:
                try:
                    BSoupStatus, BSoupResult = self.getBSoupResult(HTMLResponse.text, Name='lxml')
                except AttributeError:
                    BSoupStatus, BSoupResult = self.getBSoupResult(HTMLResponse, Name='lxml')
                if BSoupStatus:
                    JobDescTxt = self.getcleanText(' '.join(BSoupResult.find('div', id='divJobDetails').text.split()))
                return JobDescTxt

            else:
                return ""

        except Exception as e:
            self.logger.error("Failed to get job description. Error: %s", str(e))
            return ""

    def startCrawl(self, CompanyData, HTMLTagData, CrawlStatusRecord, **kwargs):
        PatternFuncName = kwargs['FunctionName']
        StartTime = self.getTime()
        self.logger.info("***%s: Start. CompanyData: %s, HTMLTagData: %s, CrawlStatusRecord: %s, Kwargs: %s***",
                        PatternFuncName, CompanyData, HTMLTagData, CrawlStatusRecord, kwargs)
        
        ErrorLogSet = set()
        SuccessTotal, FailedTotal = (0,0)
        try:
            self.logger.info("# Step-4. Crawling all links for a particular URL Sno")
            CompanyURL = CompanyData['jobUrl']
            CompanyName = CompanyData['jobUrl'].split('.tedk12.com')[0].split('//')[-1]
            BaseURL = re.sub(r'index\.aspx', '', CompanyURL, flags=re.IGNORECASE)
            StartIndex = 0
            HasMoreJobs = True
            if not self.CallDescOnly:
                self.logger.info("# Step-4.1 Check Crawl Description url's only or not?")
                while HasMoreJobs:
                    PageHeaders = {
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9',
                        'priority': 'u=1, i',
                        'referer': CompanyURL,
                        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                    }

                    APIURL = f'https://{CompanyName}.tedk12.com/hire/Index.aspx?JobListAJAX=Paging&StartIndex={StartIndex}&ListID=JobList&SearchString='
                    self.logger.info("Fetching jobs page: %s URL: %s", StartIndex//50 + 1, APIURL)
                    
                    ReqStatus, APIResponse = self.sendSessionRequest("GET", APIURL, verify=False, headers=PageHeaders)
                    if not ReqStatus:
                        break

                    try:
                        try:
                            BSoupStatus, BSoupResult = self.getBSoupResult(APIResponse.text, Name='lxml')
                        except AttributeError:
                            BSoupStatus, BSoupResult = self.getBSoupResult(APIResponse, Name='lxml')

                        JobsListTag = BSoupResult.findAll('tr')[1:]
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
                                JobURL = f"{BaseURL}{JobItem.find('td').find('a')['href']}"
                                if JobURL in self.ProcessedJobs:
                                    break
                                self.ProcessedJobs.add(JobURL)
                                JobDesc = self.getJobDescription(JobURL)
                                if JobDesc:                             
                                    JobData = {
                                        'JobTitle': self.getcleanText(JobItem.find('a').text.strip()),
                                        'JobURL': JobURL,
                                        'JobLocation': self.getcleanText(JobItem.findAll('td')[-2].text.strip()),
                                        'JobPostDate': self.getcleanText(JobItem.findAll('td')[1].text.strip()) or self.CurrentDateStr,
                                        'JobDesc': JobDesc,
                                        'OutputTableName': kwargs['OutputTableName']
                                    }
                                    self.logger.debug("Inserting job: %s", JobData['JobTitle'])
                                    DBStatus, ErrorLog = self.insertJob(CompanyData['Sno'], **JobData)
                                    assert DBStatus == True
                                    SuccessTotal += 1
                                else:
                                    self.logger.error(f"NULL job description for JobURL {JobURL}")

                            except AssertionError:
                                ErrorLogSet.add(ErrorLog)
                                self.logger.error("%s: JobInsertionError %s", PatternFuncName, ErrorLog)
                                FailedTotal += 1
                            except Exception as e:
                                ErrorLogSet.add(str(e))
                                self.logger.error("%s: JobExtraction Error %s", PatternFuncName, traceback.format_exc())
                                FailedTotal += 1

                        if SuccessTotal == BeforeCount:
                            self.logger.info(f"No new jobs found on page {StartIndex//50 + 1}, ending pagination")
                            HasMoreJobs = False
                        else:
                            StartIndex += 50

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