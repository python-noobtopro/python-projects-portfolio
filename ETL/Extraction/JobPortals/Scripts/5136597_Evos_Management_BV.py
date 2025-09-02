import requests
from selenium.webdriver.chrome.service import Service
import re
import pyodbc
import time
from bs4 import BeautifulSoup
import pyodbc
import pdb
import re
import requests
from selenium.webdriver.chrome.service import Service
import datetime
import time
import pandas as pd
import requests
from selenium.webdriver.chrome.service import Service
import re, json
from selenium import webdriver
import sql_server_career as db
import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',}

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

check = "select MAX(TPhase),CStatus from [CP_NonTier1_Phase] with(nolock) group by CStatus"

snoRows = db.retrieveTabledata(check)
print(snoRows[0][1])
if snoRows[0][1] == None:
    print("here")
TPhase = int(snoRows[0][0])
print("TPhase:--------->",TPhase)

url = 'https://evos.eu/vacancies'
cdmsid = 5136597
company_name = 'Evos Management BV'
lancode = 'en'
# Doubt why active = 1 and where it is inserted
active = 1


sql_command = """
                    IF NOT EXISTS(Select * From CareerPages.[dbo].[CP_Companies] Where cdmsid= '{}' AND PyResource='Rupesh' AND joburl='{}')
                    BEGIN
                    INSERT INTO CareerPages.[dbo].[CP_Companies] (cdmsid,companyname,active,lancode,DailySpider,PyResource,joburl) values ('{}', '{}', '{}', '{}', '3', 'Rupesh', '{}')
                    END
                """.format(cdmsid, url, cdmsid, company_name, active, lancode, url)
# print(sql_command)
db.insertRows(sql_command)
getData = f"select jobUrl,CDMSID,CompanyName,Sno from [CP_Companies] with(nolock) where DailySpider = 3 and PyResource = 'Rupesh' and CDMSID = {cdmsid} and jobUrl = '{url}' order by Sno"
queryData = db.retrieveTabledata(getData)
for data in queryData:
    Sno = data[3]
    sUrl = data[0]
    cdmsid = data[1]
    compName = data[2]
    print("*************************")
    print("DATA:--------->",data[3])
    # print("sURL:----------->",sUrl)
    print("******************")
    count = 0
    now = datetime.datetime.now()
    dtm = now.strftime("%Y/%m/%d")

    try:

        url = 'https://evos.eu/vacancies'

        headers = {
            'authority': 'evoseu.wpengine.com',
            'accept': 'application/graphql+json, application/json',
            'accept-language': 'en-US,en;q=0.9',
            # Already added when you pass json=
            # 'content-type': 'application/json',
            'dnt': '1',
            'origin': 'https://evos.eu',
            'referer': 'https://evos.eu/',
            'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        }

        json_data = {
            'query': 'fragment StatisticFields on Statistic {\n  id\n  statisticFields {\n    label\n    value\n    valueSuffix\n    __typename\n  }\n}\n\nfragment ContactLinkFields on ContactLink {\n  id\n  contactLinkFields {\n    text\n    linkAddress\n    description\n    __typename\n  }\n}\n\nfragment TerminalProductFields on TerminalProduct {\n  id\n  terminalProductFields {\n    name\n    __typename\n  }\n}\n\nfragment TerminalOfferingFields on TerminalOffering {\n  id\n  terminalOfferingFields {\n    name\n    icon {\n      mediaItemUrl\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment PersonFields on Person {\n  id\n  personFields {\n    name\n    email\n    phone\n    title\n    image {\n      mediaItemUrl\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment TerminalContactFields on TerminalContact {\n  id\n  terminalContactFields {\n    name\n    person {\n      ...PersonFields\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment TerminalFields on Terminal {\n  terminalFields {\n    name\n    storageDescription\n    productionDescription\n    productionContent\n    contactAddress\n    contactPhone\n    contactLinks {\n      ...ContactLinkFields\n      __typename\n    }\n    storageContacts {\n      ...TerminalContactFields\n      __typename\n    }\n    productionContacts {\n      ...TerminalContactFields\n      __typename\n    }\n    keyStats {\n      ...StatisticFields\n      __typename\n    }\n    valueProp\n    thumbnailImage {\n      mediaItemUrl\n      __typename\n    }\n    portraitImage {\n      mediaItemUrl\n      __typename\n    }\n    totalCapacityStat {\n      ...StatisticFields\n      __typename\n    }\n    operationalSinceStat {\n      ...StatisticFields\n      __typename\n    }\n    specialtyStat {\n      ...StatisticFields\n      __typename\n    }\n    numberOfTanksStat {\n      ...StatisticFields\n      __typename\n    }\n    tankSizeFromStat {\n      ...StatisticFields\n      __typename\n    }\n    tankSizeToStat {\n      ...StatisticFields\n      __typename\n    }\n    tankTypesStat {\n      ...StatisticFields\n      __typename\n    }\n    draughtStat {\n      ...StatisticFields\n      __typename\n    }\n    bargeBerthsStat {\n      ...StatisticFields\n      __typename\n    }\n    vesselBerthsStat {\n      ...StatisticFields\n      __typename\n    }\n    operationalServicesStat {\n      ...StatisticFields\n      __typename\n    }\n    logisticServicesStat {\n      ...StatisticFields\n      __typename\n    }\n    administrativeServicesStat {\n      ...StatisticFields\n      __typename\n    }\n    products {\n      ...TerminalProductFields\n      __typename\n    }\n    offerings {\n      ...TerminalOfferingFields\n      __typename\n    }\n    pinLocationFromTop\n    pinLocationFromRight\n    mapData\n    __typename\n  }\n}\n\nfragment VacancyFields on Vacancy {\n  id\n  vacancyFields {\n    jobTitle\n    country {\n      ... on VacancyCountry {\n        id\n        title\n        __typename\n      }\n      __typename\n    }\n    site {\n      ... on Terminal {\n        id\n        title\n        __typename\n      }\n      __typename\n    }\n    departments {\n      ... on VacancyDepartment {\n        id\n        title\n        __typename\n      }\n      __typename\n    }\n    content\n    __typename\n  }\n}\n\nquery NewsPageQuery {\n  terminals(first: 100) {\n    edges {\n      node {\n        id\n        ...TerminalFields\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  vacancies(first: 10000) {\n    edges {\n      node {\n        id\n        date\n        slug\n        ...VacancyFields\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  vacancyCountries(first: 10000) {\n    edges {\n      node {\n        id\n        title\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  vacancyDepartments(first: 10000) {\n    edges {\n      node {\n        id\n        title\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}',
            'operationName': 'NewsPageQuery',
            'variables': {},
        }

        response = requests.post('https://evoseu.wpengine.com/graphql', headers=headers, json=json_data)
        # jobs_data = response.text

        jobs_data = response.json()
        # print("response:----------->", jobs_data)

        counter = 1
        for jobs in jobs_data['data']['vacancies']['edges']:

            print("Counter:------------>", counter)
            print("Jobs:--------->", jobs)

            job_title = job_loc = job_url = job_date = job_desc = ''

            try:
                job_title = jobs['node']['vacancyFields']['jobTitle']
                print("job_title:--------->", job_title)
            except:
                job_title = ''
                pass

            try:
                url = jobs['node']['slug']
                job_url = "https://evos.eu/vacancy/" + url
                print("job_url:--------->", job_url)
            except:
                job_url = ''
                pass

            try:
                job_loc = jobs['node']['vacancyFields']['country'][0].get('title', '')
                print("job_loc:--------->", job_loc)
            except:
                job_loc = ''
                pass

            try:
                job_date = jobs['node']['date']
                job_date = job_date.split('T')[0]
                print("job_date:--------->", job_date)
            except:
                job_date = ''
                pass

            try:
                job_desc_data = jobs['node']['vacancyFields']['content']
                desc = BeautifulSoup(job_desc_data, 'lxml')
                try:
                    job_desc = ' '.join(desc.text.split())
                except:
                    job_desc = ' '

                print("job_desc:--------->", job_desc)
            except:
                pass

            if job_title != "":
                try:

                    # select * from CP_NonTier1_CrawlData with(nolock)
                    # select *   from CP_NonTier1_CrawlData_RupeshTemp  with(nolock)

                    insqry = "insert into [CP_NonTier1_CrawlData](jobTitle,jobDesc,jobUrl,SiteName,publishDate,location,compName,CDMSID,TPhase,id, ArticleExtractor) values(N'{}',N'{}',N'{}',N'{}','{}',N'{}','{}','{}','{}','{}', N'{}')".format(
                        str(job_title).replace("'", "''"),
                        str(job_desc).replace("'", "''"),
                        job_url,
                        sUrl,
                        job_date,
                        str(job_loc).replace("'", "''"),
                        str(compName).replace("'", "''"),
                        cdmsid,
                        TPhase,
                        Sno,
                        str(job_desc).replace("'", "''"))
                    count += 1
                    print(count, job_title)
                    print("Insert Query:---------->", count, insqry)

                    db.insertRows(insqry)

                except Exception as e:
                    print("Exception:=========>", e)
            else:
                print('==================================')
                print(count, job_title)

                # Job_data.append()
            counter += 1
            print("=" * 120)
    except:
        pass

