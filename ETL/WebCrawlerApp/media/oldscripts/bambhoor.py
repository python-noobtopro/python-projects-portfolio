import requests, pyodbc
import json
import re
import ast
import urllib
import datetime
import time
import pandas as pd
import sql_server_career as db
import config
from scrapy.selector import Selector
from bs4 import BeautifulSoup

check = "select MAX(TPhase) from [CP_Tier1_Phase] with(nolock)"
snoRows = db.retrieveTabledata(check)
print(snoRows[0][0])
if snoRows[0][0] == None:
    print("here")
TPhase = int(snoRows[0][0])
print(TPhase)


def bamboohr_api():
    getData = "select jobUrl,CDMSID,CompanyName,Sno from [CareerPages].[dbo].[CP_Companies] with(nolock) where  DailySpider = 4  and PatternID=5  and Active>=1 and cdmsid in (1571204,1556404) order by Sno"
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
        now = datetime.datetime.now()
        dtm = now.strftime("%Y/%m/%d")
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        }
        try:
            count = 0
            domain = sUrl.split('.bamboohr')[0].split('/')[-1]
            url ='https://%s.bamboohr.com/careers/list' % (domain)
            response = requests.get(url, headers=headers).json()
            js_data = response.get('result', '')
            if js_data:
                db.updateTableColumn('[CP_Tier1_CrawlStatus]', 'CrawlStatus',
                                         config.JA_CRAWL_COMPLETED_SUCCESS_STATUS, 'URLSNo', Sno,
                                         'Phase', TPhase)
            if not js_data:
                db.updateTableColumn('[CP_Tier1_CrawlStatus]', 'CrawlStatus',config.JA_CRAWL_COMPLETED_NO_JOBS_STATUS, 'URLSNo', Sno, 'Phase', TPhase)
                print("pagination completed")
            for job in js_data:
                job_title = job.get('jobOpeningName', '')
                job_url = 'https://%s.bamboohr.com/careers/%s' % (domain, str(job.get('id', '')))
                try:
                    if job.get('location', {}).get('city', ''):
                        location = job.get('location', {}).get('city', '')
                    if job.get('location', {}).get('state', ''):
                        location = location + ', ' + job.get('location', {}).get('state', '')
                    if job.get('location', {}).get('country', ''):
                        location = location + ', ' + job.get('location', {}).get('country', '')
                except:
                    location =''
                desc_url = job_url + '/detail'
                if desc_url:
                    response1 = requests.get(desc_url).json()
                    try:
                        posted_date = response1.get('result', '').get('jobOpening', '').get('datePosted', '')
                    except:
                        date = dtm
                    desc_json = response1.get('result', '').get('jobOpening', '').get('description', '')
                    sel = Selector(text=desc_json)
                    job_desc = ''.join(sel.xpath('//text()').extract())\
                        .replace('\r\n\t', '').replace('\r', '').replace('\n', ' ')\
                        .replace('\t', '').replace('  ', ' ').replace('\xa0', '').strip()
                count += 1
                insqry = "insert into [CP_Tier1_CrawlData](jobTitle,jobDesc,jobUrl,SiteName,publishDate,location,compName,CDMSID,TPhase,id, ArticleExtractor) values(N'{}',N'{}','{}','{}','{}',N'{}','{}','{}','{}','{}', N'{}')".format(
                    str(job_title).replace("'", "''"), job_desc.replace("'", "''"), job_url, sUrl, posted_date, location.replace("'", "''"),
                    str(compName).replace("'", "''"), cdmsid, TPhase, "", job_desc.replace("'", "''"))
                print(count, insqry)
                print(count, job_title, job_url, location)
                db.insertRows(insqry)
        except Exception as error:
            db.updateTableColumn('[CP_Tier1_CrawlStatus]', 'CrawlStatus', config.JA_CRAWL_COMPLETED_FAILED_STATUS,'URLSNo', Sno,'Phase',TPhase)

            print("<<<<<<<<<<<<<<<<<<< Got issue this CDMSID %s >>>>>>>>>>>>>>>>>>>>>>>>" % cdmsid)



bamboohr_api()