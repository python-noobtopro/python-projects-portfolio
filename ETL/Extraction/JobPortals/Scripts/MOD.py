import pprint
from db_operations import *
from dateutil.parser import parse
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import warnings
import time
from selenium.webdriver.support.ui import Select
import re
import json
import codecs
from ipdb import set_trace
import pprint

warnings.filterwarnings("ignore", category=DeprecationWarning)
chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('headless')
chrome_options.add_argument('--incognito')
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
# global driver
import pandas as pd
from ipdb import set_trace
from flashtext.keyword import KeywordProcessor

service = Service()
options = webdriver.ChromeOptions()
# options.add_argument('headless')
# driver = webdriver.Chrome(service=service, options=options)
master_data = []
check = "select MAX(TPhase),CStatus from [CP_NonTier1_Phase] with(nolock) group by CStatus"
snoRows = db.retrieveTabledata(check)
print(snoRows[0][1])
if snoRows[0][1] == None:
    print("here")
TPhase = int(snoRows[0][0])
print(TPhase)


def extraction():
    driver = webdriver.Chrome(service=service, options=options)
    global Sno
    try:
        cdmsid = 1679949
        url = 'https://www.civilservicejobs.service.gov.uk/csr/index.cgi?SID=Y29udGV4dGlkPTU1ODM4MjQ5JnBhZ2VhY3Rpb249c2VhcmNoY29udGV4dCZvd25lcnR5cGU9ZmFpciZwYWdlPTEmc29ydD1jbG9zaW5nJnBhZ2VjbGFzcz1TZWFyY2gmb3duZXI9NTA3MDAwMCZyZXFzaWc9MTY5ODY2MjI5Mi0xZWE4Y2JjODFhNWEzOGFkOWYyZDM3OWI3ZDFiODBhODJhMzRlYmM3'
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
                filters_query = "select * from CP_Companies t1 with(nolock) where patternid = 1679949 and active>0"
                filters = db.retrieveTabledata(filters_query)
                cdmsid_filter_dict = {i[1]: i[2] for i in filters}
                page = 1
                driver.maximize_window()
                driver.get(url)
                driver.implicitly_wait(3)
                try:
                    accept_cookies = driver.find_element(By.XPATH, '//*[@id="accept_all_cookies_button"]')
                    driver.execute_script('arguments[0].click()', accept_cookies)
                    print('Clicked Cookies')
                    time.sleep(1)
                except:
                    pass
                try:
                    srch_btn = driver.find_element(By.XPATH, '//*[@id="submitSearch"]')
                    driver.execute_script('arguments[0].click()', srch_btn)
                    print('Clicked SEARCH')
                    time.sleep(2)
                except:
                    pass

                soup = bs(driver.page_source, 'lxml')
                total_pages = int(soup.find('div', class_='search-results-paging-menu').findAll('li')[-2].text.strip())
                next_page_url = soup.find('a', {'title': 'Go to next search results page'})['href']
                print(f"Total Pages ----------> {total_pages}")
                jobs = soup.findAll('li', 'search-results-job-box')
                print(f"Page -----> {page}")
                print(len(jobs))
                for job in jobs:
                    temp_dict = {}
                    job_title = job.find('a')['title']
                    job_url = job.find('a')['href']
                    filter = job.find('div', class_='search-results-job-box-department').text.strip()
                    # print(job_url)
                    try:
                        job_loc = job.find('div', class_='search-results-job-box-location').text.strip()
                    except:
                        job_loc = ''
                    response = requests.get(job_url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
                    soup = bs(response.content, 'lxml')
                    try:
                        job_desc = soup.find('main', {'id': 'main-content'}).text
                    except:
                        job_desc = ''
                    job_date = dtm
                    temp_dict['job_title'] = job_title
                    temp_dict['job_loc'] = job_loc
                    temp_dict['job_url'] = job_url
                    temp_dict['job_desc'] = job_desc
                    temp_dict['job_date'] = job_date
                    temp_dict['filter'] = filter
                    master_data.append(temp_dict)

                for page in range(2, total_pages + 1):
                    driver.get(next_page_url)
                    time.sleep(2)
                    print(f"Page -----> {page}")
                    print('Next Page URL  ', next_page_url)
                    soup = bs(driver.page_source, 'lxml')
                    if page < total_pages:
                        next_page_url = soup.find('a', {'title': 'Go to next search results page'})['href']
                    else:
                        pass
                    jobs = soup.findAll('li', 'search-results-job-box')
                    print(len(jobs))
                    for job in jobs:
                        temp_dict = {}
                        job_title = job.find('a')['title']
                        job_url = job.find('a')['href']
                        filter = job.find('div', class_='search-results-job-box-department').text.strip()
                        # print(job_url)
                        try:
                            job_loc = job.find('div', class_='search-results-job-box-location').text.strip()
                        except:
                            job_loc = ''
                        response = requests.get(job_url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
                        soup = bs(response.content, 'lxml')
                        try:
                            job_desc = soup.find('main', {'id': 'main-content'}).text
                        except:
                            job_desc = ''
                        job_date = dtm
                        temp_dict['job_title'] = job_title
                        temp_dict['job_loc'] = job_loc
                        temp_dict['job_url'] = job_url
                        temp_dict['job_desc'] = job_desc
                        temp_dict['job_date'] = job_date
                        temp_dict['filter'] = filter
                        master_data.append(temp_dict)

            except Exception as e:
                print(e)

    except Exception as e:
        print(e)
        db.updateTableColumn('[CP_NonTier1_CrawlStatus]', 'CrawlStatus', config.JA_CRAWL_COMPLETED_FAILED_STATUS,
                             'URLSNo', Sno, 'Phase', TPhase)

    df = pd.DataFrame.from_records(master_data)
    df.to_excel('Ministry_of_defence.xlsx', index=False)
    return df


def main():
    global Sno
    df = pd.read_excel('Ministry_of_defence.xlsx')
    # getting keywords to match
    kwquery = "select Sno, jobUrl from [CP_Companies] t1 with(nolock) where patternid = 1679949 and active>0"
    kwrows = db.retrieveTabledata(kwquery)
    Word_Dict = {}
    for row in kwrows:
        key = row[0]
        Word_Dict[key] = [row[1]]
    Wkeyword_processor = KeywordProcessor(case_sensitive=False)
    Wkeyword_processor.add_keywords_from_dict(Word_Dict)
    try:
        count = 0
        for index, row in df.iterrows():
            job_title = row['job_title']
            job_loc = row['job_loc']
            job_date = row['job_date']
            job_url = row['job_url']
            job_desc = row['job_desc']
            filter = row['filter']
            URLSno = (Wkeyword_processor.extract_keywords(filter))

            if not URLSno:
                print(filter)
                try:
                    insqry = "insert into [CP_NonTier1_CrawlingNeptune](jobTitle,jobDesc,jobUrl,postDate,location,Phase,URLSno,ShortCode) values(N'{}',N'{}',N'{}','{}',N'{}','{}','{}', '{}')".format(
                        str(job_title).replace("'", "''"), str(job_desc).replace("'", "''"), job_url,
                        job_date, job_loc.replace("'", "''"), TPhase, 0, filter)
                    count += 1
                    print(count, insqry)
                    db.insertRows(insqry)
                    db.updateTableColumn('[CP_NonTier1_CrawlStatus]', 'CrawlStatus',
                                         config.JA_CRAWL_COMPLETED_SUCCESS_STATUS, 'URLSNo', 0, 'Phase',
                                         TPhase)

                except Exception as e:
                    print(e)
                    db.updateTableColumn('[CP_NonTier1_CrawlStatus]', 'CrawlStatus',
                                         config.JA_CRAWL_COMPLETED_FAILED_STATUS,
                                         'URLSNo', 0, 'Phase', TPhase)
            elif URLSno:
                try:
                    insqry = "insert into [CP_NonTier1_CrawlingNeptune](jobTitle,jobDesc,jobUrl,postDate,location,Phase,URLSno, ShortCode) values(N'{}',N'{}',N'{}','{}',N'{}','{}','{}', '{}')".format(
                        str(job_title).replace("'", "''"), str(job_desc).replace("'", "''"), job_url,
                        job_date, job_loc.replace("'", "''"), TPhase, URLSno[0], filter)
                    count += 1
                    print(count, insqry)
                    db.insertRows(insqry)
                    db.updateTableColumn('[CP_NonTier1_CrawlStatus]', 'CrawlStatus',
                                         config.JA_CRAWL_COMPLETED_SUCCESS_STATUS, 'URLSNo', URLSno[0], 'Phase',
                                         TPhase)

                except Exception as e:
                    print(e)
                    db.updateTableColumn('[CP_NonTier1_CrawlStatus]', 'CrawlStatus',
                                         config.JA_CRAWL_COMPLETED_FAILED_STATUS,
                                         'URLSNo', URLSno[0], 'Phase', TPhase)



    except Exception as e:
        print(e)


if __name__ == '__main__':
    extraction()
    main()
