# Breaking Crawling code into small chunks (module) to reduce copy paste for developers
# To begin with I am taking example for pure BeautifulSoup crawling code

# Part-1
# Getting data from database corresponding to jobUrl, CDMSID, PatternID, etc.

# imports
import requests, re, json
from bs4 import BeautifulSoup as bs
from ipdb import set_trace
from data_dict import *
import sql_server_career as db
import config
import datetime
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# set PyResource
PyResource = 'Rupesh'


# Normal_Case
def db_operations(func):
    def wrapper_func(url, cdmsid, dailyspider):
        check = "select MAX(TPhase),CStatus from [CompanyCareerPages_Tier1TPhase] with(nolock) group by CStatus"
        snoRows = db.retrieveTabledata(check)
        TPhase = int(snoRows[0][0])
        print(TPhase)
        getData = f"select jobUrl,CDMSID,CompanyName,Sno from [GenericSuccessCompanies2.5k] with(nolock) where DailySpider = {dailyspider} and PyResource = '{PyResource}' and CDMSID = {cdmsid} and jobUrl = '{url}' order by Sno"
        queryData = db.retrieveTabledata(getData)
        for data in queryData:
            Sno = data[3]
            sUrl = data[0]
            cdmsid = data[1]
            compName = data[2]
            now = datetime.datetime.now()
            dtm = now.strftime("%Y/%m/%d")
            count = 0
            result = func(url, cdmsid, dailyspider)
            for i in result:
                job_title = i['job_title']
                job_url = i['job_url']
                job_loc = i['job_loc']
                job_desc = i['job_desc']
                if job_title != "":
                    try:
                        insqry = "insert into [CompanyCareerPagesGeneric_Tier1](jobTitle,jobDesc,jobUrl,SiteName,publishDate,location,compName,CDMSID,TPhase,id, ArticleExtractor) values(N'{}',N'{}',N'{}',N'{}','{}',N'{}','{}','{}','{}','{}', N'{}')".format(
                            str(job_title).replace("'", "''"), str(job_desc).replace("'", "''"), job_url,
                            sUrl, dtm, job_loc,
                            str(compName).replace("'", "''"), cdmsid, TPhase, Sno,
                            str(job_desc).replace("'", "''"))
                        count += 1
                        print(count, job_title)
                        print(count, insqry)
                        # db.insertRows(insqry)
                        # db.updateTableColumn('[CP_Tier1_CrawlStatus]', 'CrawlStatus', config.JA_CRAWL_COMPLETED_SUCCESS_STATUS, 'URLSNo', Sno)

                    except Exception as e:
                        print(e)
                else:
                    print('==================================')
                    print(count, job_title)

    return wrapper_func


@db_operations
def your_crawling_func(url, cdmsid, dailyspider):
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
    soup = bs(response.content, 'lxml')
    jobs = soup.findAll('div', class_='single-job col-sm-12 col-md-6 col-lg-4')
    print(len(jobs))
    ##
    if len(jobs) == 0:
        return None
    else:
        data = []
        for job in jobs:
            job_title = job.find('h4').text.strip()
            job_url = job.find('a')['href']
            job_loc = job.find('div', class_='location').text.strip()
            response = requests.get(job_url, headers={'User-Agent': 'Mozilla/5.0'},
                                    verify=False)
            soup = bs(response.content, 'lxml')
            try:
                try:
                    job_desc = ' '.join(soup.find('div', {'style': 'padding:10px 10px 30px 0;'}).text.split())
                except:
                    job_desc = ' '.join(soup.find('table', {'id': 'JobDescription'}).text.split())
            except:
                job_desc = ''

            crawled_data = {'job_title': job_title,
                            'job_url': job_url,
                            'job_loc': job_loc,
                            'job_desc': job_desc}
            data.append(crawled_data)
        return data


# your_crawling_func(url='https://www.airborneglobal.com/careers', cdmsid=1003806, dailyspider=4)


# Special_Case
data_json.update({
    "url": 'https://www.airborneglobal.com/careers',
    "cdmsid": 1003806,
    "dailyspider": 4,
    "tags_and_attrs": {
        "jobs": [{"tag": 'div', "attr": 'class', "value": 'single-job col-sm-12 col-md-6 col-lg-4'}],
        # check multi options of tags, attrs
        "job_title": {"tag": 'h4', "attr": None, "value": None},
        "job_url": {"tag": 'a', "attr": None, "value": None},
        "job_loc": {"tag": 'div', "attr": 'class', "value": 'location'},
        "job_desc": {"tag": 'div', "attr": 'style', "value": 'padding:10px 10px 30px 0;'}
    }
})


def db_operations_special(func):
    def wrapper_func(data_dict):
        url, cdmsid, dailyspider = data_dict['url'], data_dict['cdmsid'], data_dict['dailyspider']
        check = "select MAX(TPhase),CStatus from [CompanyCareerPages_Tier1TPhase] with(nolock) group by CStatus"
        snoRows = db.retrieveTabledata(check)
        TPhase = int(snoRows[0][0])
        print(TPhase)
        getData = f"select jobUrl,CDMSID,CompanyName,Sno from [GenericSuccessCompanies2.5k] with(nolock) where DailySpider = {dailyspider} and PyResource = '{PyResource}' and CDMSID = {cdmsid} and jobUrl = '{url}' order by Sno"
        queryData = db.retrieveTabledata(getData)
        for data in queryData:
            Sno = data[3]
            sUrl = data[0]
            cdmsid = data[1]
            compName = data[2]
            now = datetime.datetime.now()
            dtm = now.strftime("%Y/%m/%d")
            count = 0
            result = func(data_dict)
            for i in result:
                job_title = i['job_title']
                job_url = i['job_url']
                job_loc = i['job_loc']
                job_desc = i['job_desc']
                if job_title != "":
                    try:
                        insqry = "insert into [CompanyCareerPagesGeneric_Tier1](jobTitle,jobDesc,jobUrl,SiteName,publishDate,location,compName,CDMSID,TPhase,id, ArticleExtractor) values(N'{}',N'{}',N'{}',N'{}','{}',N'{}','{}','{}','{}','{}', N'{}')".format(
                            str(job_title).replace("'", "''"), str(job_desc).replace("'", "''"), job_url,
                            sUrl, dtm, job_loc,
                            str(compName).replace("'", "''"), cdmsid, TPhase, Sno,
                            str(job_desc).replace("'", "''"))
                        count += 1
                        print(count, job_title)
                        print(count, insqry)
                        # db.insertRows(insqry)
                        # db.updateTableColumn('[CP_Tier1_CrawlStatus]', 'CrawlStatus', config.JA_CRAWL_COMPLETED_SUCCESS_STATUS, 'URLSNo', Sno)

                    except Exception as e:
                        print(e)
                else:
                    print('==================================')
                    print(count, job_title)

    return wrapper_func


@db_operations_special
def modular_crawling_func(data_dict):
    response = requests.get(data_dict['url'], headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
    if response.status_code == 200:
        soup = bs(response.content, 'lxml')
        tag = data_dict['tags_and_attrs']['jobs']['tag']
        attr = data_dict['tags_and_attrs']['jobs']['attr']
        value = data_dict['tags_and_attrs']['jobs']['value']
        jobs = soup.findAll(tag, {attr: value})
        print(len(jobs))
        if len(jobs) > 0:
            data = []
            for job in jobs:
                tag, attr, value = data_dict['tags_and_attrs']['job_title']['tag'], \
                                   data_dict['tags_and_attrs']['job_title']['attr'], \
                                   data_dict['tags_and_attrs']['job_title']['value']
                if attr is None:
                    attr = 'class'
                job_title = job.find(tag, {attr: value}).text.strip()

                tag = data_dict['tags_and_attrs']['job_loc']['tag']
                attr = data_dict['tags_and_attrs']['job_loc']['attr']
                value = data_dict['tags_and_attrs']['job_loc']['value']
                if attr is None:
                    attr = 'class'
                job_loc = job.find(tag, {attr: value}).text.strip()

                tag = data_dict['tags_and_attrs']['job_url']['tag']
                attr = data_dict['tags_and_attrs']['job_url']['attr']
                value = data_dict['tags_and_attrs']['job_url']['value']
                if attr is None:
                    attr = 'class'
                job_url = job.find(tag, {attr: value})['href']
                response = requests.get(job_url, headers={'User-Agent': 'Mozilla/5.0'},
                                        verify=False)
                soup = bs(response.content, 'lxml')

                tag = data_dict['tags_and_attrs']['job_desc']['tag']
                attr = data_dict['tags_and_attrs']['job_desc']['attr']
                value = data_dict['tags_and_attrs']['job_desc']['value']
                if attr is None:
                    attr = 'class'
                try:
                    job_desc = ' '.join(soup.find(tag, {attr: value}).text.split())
                except:
                    job_desc = ''

                crawled_data = {'job_title': job_title,
                                'job_url': job_url,
                                'job_loc': job_loc,
                                'job_desc': job_desc}
                data.append(crawled_data)
            print(data)
            return data
        print(f"No Jobs Found. Please check {data_dict['tags_and_attrs']['jobs']}")
    print(f"Cannot crawl {data_dict['url']}, response code {response.status_code}")


modular_crawling_func(data_dict=data_json)

# Pagination_handling

# pagination = y/n
# last_page y/n
#
# for range(last_page)
#
#     while True:
#         if len(jobs)< 1:
#             break
# last_page, total_jobs (no of jobs/page)
