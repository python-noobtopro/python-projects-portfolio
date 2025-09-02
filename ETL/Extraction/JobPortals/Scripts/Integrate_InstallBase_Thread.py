import os, re
from urllib.request import Request, urlopen
from urllib.parse import urljoin
from boilerpy3 import extractors
from urllib.parse import quote
import requests
import pyodbc
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import os, re
import requests
import sql_server_landscape as db
from bs4 import BeautifulSoup as bs
import datetime
import time
import traceback
from urllib import parse
import json
import unicodedata
import html as HTMLParser
from threading import *



################################################################## Common ###############################################################################
def getData(sourceID, installBase):
    getData = f"Select  t1.Alias1,t1.CompanyID,t1.CDMSID,t1.phase from InstallBaseCompanies t1 with(nolocK) inner join IB_ProcessStatus t2 on t1.CompanyID=t2.CompanyID where t2.Source{sourceID} = 0"
    print(getData)
    queryData = db.retrieveTabledata(getData)
    # print(queryData)
    for comp_details in queryData:
        print(comp_details)
        cno = comp_details[1]
        cdmsid = comp_details[2]
        company_name = comp_details[0]
        tphase = comp_details[3]
        installBase(company_name, cno, cdmsid, tphase)


########################################################## JOB BOARD 1 ################################################################################################################

main_url = "http://www.jobvertise.com"


class JobsVertise():
    def __init__(self, alias, CNo, CDMSID, Phase):
        self.main_url = main_url
        self.compName = str(alias).replace("+", " ")
        self.cno = int(CNo)
        self.cdmsid = int(CDMSID)
        self.phase = int(Phase)
        self.get_data()  # Calling Method

    def get_data(self):
        phase = self.phase
        compName = str(self.compName).replace(" ", "+")
        # print compName
        print("site here")
        compNy_url = ''.join(
            self.main_url + '/jobs/search?query=' + compName + '&city=&radius=200&state=&button=Search+Jobs'.strip())
        print(compNy_url)
        counter = 0
        self.count = 0
        while True:
            try:
                cookies = {
                    '_ga': 'GA1.2.1441944816.1637303177',
                    '__qca': 'P0-44055322-1637303177247',
                    '__gads': 'ID=4b034ebf35f498b7-2248b73627cf00d0:T=1637303196:RT=1637303196:S=ALNI_Mbo9SEZGANWAm5VI9IU72B63C3RQA',
                    '_gid': 'GA1.2.1415268513.1637572854',
                    'jsearch': 'wipro%5E%5E%5E&wipro%5E%5E%5E30&HUGHES%2C%20WHILDEN%20R%5E%5E%5E&capgemini%5E%5E%5E30',
                }
                headers = {
                    'Connection': 'keep-alive',
                    'Accept': 'application/json, text/plain, */*',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept-Language': 'en-US,en;q=0.9,te;q=0.8,zh-CN;q=0.7,zh;q=0.6,hy;q=0.5',
                }
                params = (
                    ('query', self.compName), ('city', ''), ('radius', '200'), ('state', ''),
                    ('button', 'Search Jobs'),)
                data = {'offset': counter, 'bfcount': '0', 'query': self.compName, 'city': '', 'radius': '30',
                        'state': '', 'button': 'Next 10'}
                response = requests.post('https://www.jobvertise.com/jobs/search', headers=headers, params=params,
                                         cookies=cookies, data=data, verify=False)
                soup = bs(response.content, 'lxml')
                jobs = soup.find(text=re.compile('Showing jobs')).find_next('table').find_all('tr')[1:]
                if not jobs:
                    break
                for job in jobs:
                    try:
                        job_title = ' '.join(job.find('a').text.split())
                    except:
                        job_title = ''
                    job_url = job.find('a')['href']
                    try:
                        job_loc = ' '.join(job.find_all('td')[1].text.split())
                    except:
                        job_loc = ''
                    try:
                        job_company = job.find_all('td')[-1].text
                    except:
                        job_company = ''
                    response = requests.get(job_url)
                    response = response.text.replace('-->', '').replace('<!--', '')
                    soup = bs(response, 'lxml')
                    try:
                        job_desc = ' '.join(soup.find('body').text.split())
                    except:
                        job_desc = ''
                    date = datetime.datetime.now().strftime("%Y/%m/%d")
                    self.count += 1
                    insqry = "insert into JobVertise(Cno,CDMSID,JobTitle,CompName,URL,Text,PublishedDate,Location,Phase) values('{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(
                        self.cno, self.cdmsid, job_title.replace("'", "''"), self.compName.replace("'", "''"), job_url,
                        job_desc, date,
                        job_loc.replace("'", "''"), self.phase)
                    print(insqry)
                    print(job_title, job_url, job_loc, job_desc)
                    print('inserted ========================')
                    db.insertRows(insqry)
                counter += 10

            except Exception as error:
                print(error)
                break
        self.IB_Process_status(self.cno, 12, self.count)  # Calling Method

    def IB_Process_status(self, Companyid, sourceid, jobcount):

        sourcename = "Source" + str(sourceid)
        today = str(datetime.datetime.now()).split('.')[0]
        # dtm = today.strftime("%Y/%m/%d/")
        ins_job_qry = "insert into IB_Job_ProcessStatus(CompanyID,SourceID,JobsCount, Updated) values({},{},{},'{}')".format(
            Companyid, sourceid, jobcount, today)
        print(ins_job_qry)
        upqry = "update IB_ProcessStatus set {}=1 where CompanyID={}".format(sourcename, Companyid)
        db.insertRows(ins_job_qry)
        db.insertRows(upqry)
        return 1


########################################################## JOB BOARD 2 ################################################################################################################

hdr = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
    'Accept-Encoding': 'none',
    'Accept-Language': 'en-US,en;q=0.8',
    'Content-Type': 'application/json; charset=utf-8',
    'Connection': 'keep-alive'}

base_url = "https://www.glassdoor.com"


class GlassDoor():

    def __init__(self, comp_name, CNo, CDMSID, Phase):
        self.compName = str(comp_name).replace("+", " ")
        self.cno = int(CNo)
        self.cdmsid = int(CDMSID)
        self.phase = int(Phase)
        self.get_data()

    def IB_Process_status(self, Companyid, sourceid, jobcount):
        sourcename = "Source" + str(sourceid)

        today = str(datetime.datetime.now()).split('.')[0]
        # dtm = today.strftime("%Y/%m/%d/")
        ins_job_qry = "insert into IB_Job_ProcessStatus(CompanyID,SourceID,JobsCount, Updated) values({},{},{},'{}')".format(
            Companyid, sourceid, jobcount, today)
        print(ins_job_qry)
        upqry = "update IB_ProcessStatus set {}=1 where CompanyID={}".format(sourcename, Companyid)
        db.insertRows(ins_job_qry)
        db.insertRows(upqry)

        # print(upqry)
        return 1

    def get_data(self):
        global count
        print('Company ---------------->', self.compName)
        company_search_term = self.compName
        search_term = company_search_term and parse.quote_plus(company_search_term.strip()) or False
        status = '2'
        try:
            if search_term:
                company_url = base_url + """/Job/jobs.htm?suggestCount=0&suggestChosen=false&clickSource=searchBtn&typedKeyword={}&sc.keyword={}&locT=&locId=&jobType=""".format(
                    search_term, search_term)
                # print(company_url)
                response = requests.get(company_url)
                soup = bs(response.content, 'lxml')
                if soup:
                    pagination = soup.find("div", id="FooterPageNav")
                    pagination = pagination and pagination.findAll("li", class_="page") or False
                    pagination = pagination and pagination[1:] or False
                    pagination = pagination and [tag.find("a")['href'] for tag in pagination if tag.find("a")] or False
                    pagination = pagination and [base_url + p for p in pagination] or False
                    total_pages = pagination and [company_url] + pagination or [company_url]
                    # print(total_pages)
                    count = 0
                    for page in total_pages:
                        article = soup.find("article", id="MainCol")
                        ul_tags = article.find("ul", {'data-test': "jlGrid"})
                        # print(page)
                        response = requests.get(page)
                        soup = bs(response.content, 'lxml')
                        for li in ul_tags:
                            job_url = li.find("a", class_="jobLink")
                            job_url = job_url and base_url + job_url['href'] or False
                            # print(job_url)
                            if job_url:
                                response = requests.get(job_url, headers=hdr)
                                new_soup = bs(response.content, 'lxml')
                                description = new_soup.find("script", type="application/ld+json")
                                description = description and description.get_text()
                                if description is not None:
                                    extracted_content = unicodedata.normalize("NFKD", description)
                                    extracted_content = extracted_content.encode('ascii', 'ignore').decode('ascii')
                                    parsed_text = HTMLParser.unescape(extracted_content)
                                try:
                                    result = json.loads(r'{}'.format(parsed_text), strict=False)
                                except:
                                    extracted_content = extracted_content.replace('&#034;', '\\"')
                                    parsed_text = HTMLParser.unescape(extracted_content)
                                    try:
                                        result = json.loads(r'{}'.format(parsed_text), strict=False)
                                    except:
                                        result = None
                                if result is not None:
                                    job_description = result['description']
                                    job_title = result['title']
                                    posted_date = result['datePosted']
                                    company_name = result.get('hiringOrganization') and result[
                                        'hiringOrganization'].get(
                                        "name") and result['hiringOrganization']['name'] or ''
                                    location = result.get('jobLocation') and result['jobLocation'].get("address") and \
                                               result['jobLocation']['address'].get("addressLocality") and \
                                               result['jobLocation']['address']["addressLocality"] or ''
                                    job_soup = bs(job_description, "lxml")
                                    job_description = job_soup.get_text()

                                    print('Job Title ---------->', job_title)
                                    # print(job_title, posted_date, company_name, location)
                                    # print(job_description)

                                    if job_title != '':
                                        try:
                                            insqry = "insert into Glassdoor_US(Cno, CDMSID, JobTitle, CompName, URL, Text, Location, Phase) values('{}','{}','{}','{}','{}','{}','{}','{}')".format(
                                                self.cno, self.cdmsid, job_title.replace("'", "''"),
                                                self.compName.replace("'", "''").strip(),
                                                job_url, job_description.replace("'", "''"),
                                                location.replace("'", "''"),
                                                self.phase)
                                            irows = db.insertRows(insqry)
                                            print(irows, "+++++++++++++++++insert")
                                            count += 1
                                            # print("Job Count", count)
                                            status = '1'
                                        except Exception as ie:
                                            status = str(ie)

                        if status != '1':
                            break


        except Exception as e:
            print(traceback.print_exc())
            print(e, "_______________________exception")
            status = str(e)
            pass

        print(self.cno, count, "<<<<<<<<<<<<<<<Count>>>>>>>>>>>>>>>>>>")
        # self.IB_Process_status(self.cno, 20, count)


################################################## Integrated Thread Run ######################################################################################
t1 = Thread(target=getData, args=(20, GlassDoor,))
t2 = Thread(target=getData, args=(12, JobsVertise,))
t1.start()

# To avoid SQL interference
time.sleep(2)
t2.start()

