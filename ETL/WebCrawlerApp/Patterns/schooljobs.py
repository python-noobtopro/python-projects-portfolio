__author__ = 'Jagadeesh Derangula'


"""
This is Beautifulsoup Pattern  Component and handles Bsoup related webcrawling pattern companies
"""
import sys, os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import traceback, re
import requests, datetime
from bs4 import BeautifulSoup as bs
import time
from Settings import JA_CRAWL_STATUS_MASTER as CRAWL_STATUS, MAX_PAGE_SOURCE_LENGTH, MAX_ERROR_LOG_LENGTH, \
    JOB_EXLUDE_KEYWORDS, HTML_TAG_SEPERATOR
from main.GenericWebCrawlerAdapter import GenericWebCrawlerBaseAdapterClass
class SchooljobsClass(GenericWebCrawlerBaseAdapterClass):
    def __init__(self, DailySpiderID, PatternID, CallDescOnly=False, **kwargs):
        super().__init__(kwargs.get('DBEnv'), DailySpiderID, PatternID)
        self.CDMSID = kwargs.get('CDMSID', 0)
        self.URLSNoList = kwargs.get('URLSNoList', [])
        self.PyResourceName = kwargs.get('PyResourceName', '').strip()
        self.CallDescOnly = CallDescOnly

    def __repr__(self):
        return self.__class__.__name__

    @property
    def CurrentFilePath(self):
        return str(os.path.abspath(__file__))

    def getCompanyList(self, **kwargs):
        return self.getCompanies(self.CDMSID, self.URLSNoList, self.PyResourceName, **kwargs)

    def startCrawler(self, CompanyData, HTMLTagData, CrawlStatusRecord, **kwargs):

        PatternFuncName = kwargs['FunctionName']
        StartTime = self.getTime()
        self.logger.info("***%s: Start. CompanyData: %s, HTMLTagData: %s, CrawlStatusRecord: %s, Kwargs: %s***",
                         PatternFuncName, CompanyData, HTMLTagData, CrawlStatusRecord, kwargs)
        print(PatternFuncName, CompanyData, HTMLTagData, CrawlStatusRecord, kwargs)
        ErrorLogSet = set()
        SuccessTotal, FailedTotal = (0, 0)
        page = 1
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'text/html',
            # 'cookie': '_ga=GA1.1.1301886123.1732693046; fpestid=UNHkrU5E-2vh6hNTtqsjxpo3_0GXC7c7c-axsdjHlsCKOULLHjHQ9H3g5BvdQyY_pXvGsw; _cc_id=a60b9d5f671121f0d6e28ff9611bd00; _RCRTX03=d5edd1f7ac9211ef9d8cdf1410a923b5836de982a7764748ab114559417d6cb2; _RCRTX03-samesite=d5edd1f7ac9211ef9d8cdf1410a923b5836de982a7764748ab114559417d6cb2; ASP.NET_SessionId=11crgsucoumhsr4tqgwes0op; __RequestVerificationToken=zh-x-eLIZG18JirKOxQILlhA-Tu7paKpLaON5nsNacm7r5WDxvTjbDHJEV0YHEiCBnSsRWkx4zwU3GrIyf_wEgJqgIk1; chatbase_anon_id=7028cefa-fbf2-4262-aee9-94b27d094424; _ga_7NPH7PCHPY=GS1.1.1732773847.2.1.1732775327.2.0.0; visitor_Id=97d8b7d5-bf79-42a9-b916-0169da47b89c; rx_jobid_a40f2b4c-d9f2-11e7-8bfd-cf0059c1cdfc=4723016; ADRUM=s=1732776158768&r=https%3A%2F%2Fwww.schooljobs.com%2Fcareers%2Flosriosccd%2Findex; panoramaId_expiry=1732871863566; panoramaId=c4cf5acf4d70b716425e1d9b3d6aa9fb927ac1e7bfda6d3c22b965fc00b0964d; panoramaIdType=panoDevice; SameSite=None; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Nov+28+2024+14%3A52%3A21+GMT%2B0530+(India+Standard+Time)&version=202408.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2COSSTA_BG%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false; _ga_N2Q33143XL=GS1.1.1732785462.6.1.1732785741.0.0.0; ADRUM_BTa=R:0|g:4b8d7650-5d5d-4747-9934-58aedfcef33e|n:neogov_698146b0-2502-4182-8f0e-5f1fccb51173; ADRUM_BT1=R:0|i:1195429|e:1',
            'priority': 'u=1, i',
            'referer': 'https://www.schooljobs.com/careers/hawaiipublicschools/index?page=1',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }
        try:
            self.logger.info("# Step-4. Crawling all links for a particular URL Sno")
            # print("company url:", CompanyData['jobUrl'])
            Page = 1
            if not self.CallDescOnly:
                self.logger.info("# Step-4.1 Check Crawl Description url's only or not?")
                JobCount=0
                while True:
                    if 'agency' in CompanyData['jobUrl']:
                        Comp_name = CompanyData['jobUrl'].split('agency=')[1].split('&')[0]
                        Main_Url='https://www.schooljobs.com/careers/' + Comp_name + f"?page={Page}"
                    else:
                        # Domain = CompanyData['jobUrl'].split('careers')[0]
                        Comp_name = CompanyData['jobUrl'].split('careers/')[1].split('&')[0].split('?')[0].split('/')[0]
                        Main_Url='https://www.schooljobs.com/careers/' + Comp_name + f"?page={Page}"
                    print("main url:", Main_Url)
                    ReqStatus, HTMLResponse = self.sendRequest("GET",Main_Url, verify=False, headers=headers)
                    # print(HTMLResponse6)
                    try:
                        BsoupStatus, BSoupResult = self.getBSoupResult(HTMLResponse.text, Name='lxml')

                    except Exception as e:
                        ErrorLogSet.add(str(traceback.format_exc()))
                        self.logger.error("%s: Bsoup Response  Error %s", PatternFuncName, traceback.format_exc())
                        ErrorLog = self.formatErrorSet(ErrorLogSet)
                        self.logger.info("Pagination completed %s" % page)
                        CrawlStatusData = {"CrawlStatus": CRAWL_STATUS["Failed"], "SuccessTotal": SuccessTotal,
                                           "FailedTotal": FailedTotal, "ErrorLog": ErrorLog[:MAX_ERROR_LOG_LENGTH],
                                           "HTMLText": HTMLResponse.text}

                    JobsListTag = BSoupResult.find_all('li',class_='list-item')

                    if JobsListTag == []:
                        break
                    if len(JobsListTag) == 0:
                        try:
                            CleanTxt = self.getcleanText(HTMLResponse.text)
                        except AttributeError:
                            CleanTxt = self.getcleanText(HTMLResponse)
                        PageStatus, PageSourceData = self.checkURLStatusandNoJobs(CleanTxt)
                        CrawlStatusData = {"CrawlStatus": CRAWL_STATUS['NoJobs'], "SuccessTotal": SuccessTotal,
                                           "FailedTotal": FailedTotal,
                                           "HTMLText": str(CleanTxt)[:MAX_PAGE_SOURCE_LENGTH],
                                           "ErrorLog": str(PageSourceData)[:MAX_ERROR_LOG_LENGTH]}
                        self.logger.info(CrawlStatusData)
                        self.updateCrawlStatusByURLSNo(CompanyData['Sno'], **CrawlStatusData)
                        self.logger.info("*** %s Process End, elapsedTime %s (in Minutes)***", PatternFuncName,
                                         self.getElapsedTitme(StartTime))
                        return CrawlStatusData

                    for JobItem in JobsListTag:
                        JobCount+=1
                        # print(JobCount)
                        JobData = {}
                        JobData['JobTitle'] = JobItem.find('h3').text.strip()
                        # print(JobData['JobTitle'])
                        JobData['JobURL'] = 'https://www.schooljobs.com'+JobItem.find('a', class_='item-details-link')['href']
                        # print(JobData['JobURL'])

                        try:
                            JobData['JobLocation'] = JobItem.find('li').text.strip().replace("'","''")
                        except Exception as e:
                            JobData['JobLocation'] = 'Global'
                            # print(JobData['JobLocation'])
                        JobData['JobPostDate'] = self.CurrentDateStr
                        # print(JobData['JobPostDate'])
                        a = requests.get(JobData['JobURL'])
                        soup = bs(a.text, 'html.parser',)
                        JobData['JobDesc'] =soup.find('div',class_='container entity-details-content tab-content').text.strip().replace("'","''")
                        # print(JobData['JobDesc'])
                        try:
                            JobData['Salary'] = soup.find('div', text=re.compile('Salary')).findNext('p').text.replace("'","''")

                        except:
                            JobData['Salary'] = ''
                        # print(JobData['Salary'])
                        try:

                            JobData['SourceJobID']  =soup.find('div',text = re.compile('Job Number')).findNext('p').text.replace("'","''")
                        except :
                            JobData['SourceJobID'] = ''
                        try:

                            JobData['JobDivision']  =soup.find('div',text = re.compile('Division')).findNext('p').text.replace("'","''")
                        except :
                            JobData['JobDivision'] = ''
                        try:
                            JobData['EmploymentType'] = soup.find('div', text=re.compile('Job Type')).findNext('p').text.replace("'","''")
                        except:
                            JobData['EmploymentType'] = ''
                        try:
                            JobData['OutputTableName'] = kwargs['OutputTableName']
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
                        print(JobCount,JobData)
                    Page += 1
            if self.CallDescOnly:
                self.logger.info(
                    "# Step-4.4 get Jobs SuccessTotal and FailedTotal from CrawlStatus Table When Call Description only ")
                SuccessTotal, FailedTotal = (
                    CrawlStatusRecord.get('SuccessTotal', 0), CrawlStatusRecord.get('FailedTotal', 0))

            self.logger.info("# Step-5. Identifying new jobs with help of update query from cosolidated table")
            DBStatus, ErrorLog = self.updateJobsDescByURLSNo(CompanyData['Sno'], kwargs['OutputTableName'])
            self.logger.error("*** %s: updateJobsDescByURLSNo %s ***", PatternFuncName, ErrorLog)

            self.logger.info("# Step-6.  Getting description for new jobs only")
            ErrorLog = self.formatErrorSet(ErrorLogSet)
            CrawlStatusData = {"CrawlStatus": CRAWL_STATUS["Failed"] if FailedTotal > 0 else CRAWL_STATUS["Success"],
                             "SuccessTotal": SuccessTotal, "FailedTotal": FailedTotal,
                               "ErrorLog": ErrorLog[:MAX_ERROR_LOG_LENGTH]}

        except Exception as ex:
            ErrorLogSet.add(str(traceback.format_exc()))
            ErrorLog = self.formatErrorSet(ErrorLogSet)
            CrawlStatusData = {"CrawlStatus": CRAWL_STATUS['Failed'], "ErrorLog": ErrorLog[:MAX_ERROR_LOG_LENGTH],
                               "SuccessTotal": SuccessTotal, "FailedTotal": FailedTotal, "HTMLText": str(ex)}
            self.logger.error("%s: Crawling ErrorLog %s", PatternFuncName, traceback.format_exc())
        self.logger.info("*** %s Process End, elapsedTime %s (in Minutes) ***", PatternFuncName,
                         self.getElapsedTitme(StartTime))
        return CrawlStatusData
if __name__ == '__main__':
    SchooljobsClass(DailySpiderID=3, CDMSID=1422579, PatternID=155, URLSNoList=[206358]).run()









