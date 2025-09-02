import re
import requests
from selenium.webdriver.chrome.service import Service
import warnings
import time
import json
from bs4 import BeautifulSoup as bs
from phantomjs import Phantom
from selenium import webdriver
import sql_server_career as db
import config
import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
chrome_options = webdriver.ChromeOptions()
# # chrome_# options.add_argument('headless')
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
# chrome_options.add_argument("start-minimized")
# chrome_options.add_argument("--incognito")
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys



check = "select MAX(TPhase),CStatus from [CP_NonTier1_Phase] with(nolock) group by CStatus"
snoRows = db.retrieveTabledata(check)
print(snoRows[0][1])
if snoRows[0][1] == None:
    print("here")
TPhase = int(snoRows[0][0])
print(TPhase)


url = 'https://www.secretcv.com/firma/ak-nisasta-is-ilanlari'
cdmsid = 5168121

getData = f"select jobUrl,CDMSID,CompanyName,Sno from [CP_Companies] with(nolock) where DailySpider = 3 and PyResource = 'Rupesh' and CDMSID = {cdmsid} and jobUrl = '{url}' order by Sno"
queryData = db.retrieveTabledata(getData)
for data in queryData:
    Sno = data[3]
    sUrl = data[0]
    cdmsid = data[1]
    compName = data[2]
    print("*************************")
    print(data[3])
    print(sUrl)
    print("******************")
    count = 0
    now = datetime.datetime.now()
    dtm = now.strftime("%Y/%m/%d")
    try:
        service = Service()
        options = webdriver.ChromeOptions()
        # options.add_argument('headless')
        driver = webdriver.Chrome(service=service, options=options)
        # driver.maximize_window()
        driver.get(url)
        time.sleep(2)
        soup = bs(driver.page_source, 'lxml')
        jobs = soup.findAll('div', {'class': 'content company-detail-content'})
        print(len(jobs))
        if count == 0:
            db.updateTableColumn('[CP_NonTier1_CrawlStatus]', 'CrawlStatus', config.JA_CRAWL_COMPLETED_NO_JOBS_STATUS, 'URLSNo', Sno, 'Phase', TPhase)
        for job in jobs:
            job_url = job.find('a')['href']
            job_title = job.find('h2').text.strip()
            job_loc = job.find('span', class_='city').text.strip()
            driver.get(job_url)
            time.sleep(4)
            soup = bs(driver.page_source, 'lxml')
            try:
                job_desc = ' '.join(soup.find('div', class_='cv-card content-job').text.split())
            except:
                job_desc = ''
            #########

            # print(job_title, job_loc, job_url)
            # print(job_desc)
            # print('*************************')
            # #########

            if job_desc != "":
                    try:
                        insqry = "insert into [CP_NonTier1_CrawlData](jobTitle,jobDesc,jobUrl,SiteName,publishDate,location,compName,CDMSID,TPhase,id, ArticleExtractor) values(N'{}',N'{}',N'{}',N'{}','{}',N'{}','{}','{}','{}','{}', N'{}')".format(
                            str(job_title).replace("'", "''"), str(job_desc).replace("'", "''"), job_url,
                            sUrl, dtm, job_loc,
                            str(compName).replace("'", "''"), cdmsid, TPhase, Sno,
                            str(job_desc).replace("'", "''"))
                        count += 1
                        print(count, job_title)
                        # # print(count, insqry)
                        db.insertRows(insqry)
                        if count == 1:
                            db.updateTableColumn('[CP_NonTier1_CrawlStatus]', 'CrawlStatus', config.JA_CRAWL_COMPLETED_SUCCESS_STATUS, 'URLSNo', Sno, 'Phase', TPhase)

                    except Exception as e:
                        print(e)
            else:
                print('==================================')
                print(count, job_title)


    except Exception as e:
        print(e)
        db.updateTableColumn('[CP_NonTier1_CrawlStatus]', 'CrawlStatus', config.JA_CRAWL_COMPLETED_FAILED_STATUS, 'URLSNo', Sno, 'Phase', TPhase)