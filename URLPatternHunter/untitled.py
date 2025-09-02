#!/usr/bin/env python
# coding: utf-8

import time
import pyodbc
import asyncio
import aiohttp
import random
import numpy as np
import pandas as pd
from analysis_config import make_domain_list
from analysis_config import *
from nltk.probability import FreqDist
from urllib.parse import urlparse
from bs4 import BeautifulSoup as bs
from aiohttp import ClientSession
from flashtext import KeywordProcessor

# Input Data
excel_file_name = 'C:\\Users\\Rupesh.Ranjan\\Downloads\\PatternFind_050324.xlsx'
sheet_name = 'Sheet1'
url_column_name = 'careerURL'
print(excel_file_name)

# Load Data
start = time.time()
data = pd.read_excel(f'{excel_file_name}', sheet_name=f'{sheet_name}')

# Load Known Patterns in FlashText
# Loading pattern table from db
con = pyodbc.connect('Driver={SQL Server};Server=10.8.196.65;Database=CareerPages;uid=;pwd=')
df_keys = pd.read_sql("select ID, PatternName, PyResource, Keyword from cp_patterns_master", con=con)

pattern_dict = {str(k): [v] for k, v in zip(df_keys['ID'], df_keys['Keyword'])}

match = KeywordProcessor()
match.add_keywords_from_dict(pattern_dict)

# Cleaning of URLs
data[url_column_name] = data[url_column_name].str.strip()
data.loc[~data[url_column_name].str.startswith('http'), url_column_name] = 'https://' + data[url_column_name]

data.fillna('', inplace=True)

random.seed(1)


# Greenhouse can be API and not necessarily in page source
async def fetch_url(url, session):
    async with session.get(url) as response:
        if response.status == 200:
            if match.extract_keywords(url):
                pattern_id = int(match.extract_keywords(url)[0])
                pattern_name = df_keys.loc[df_keys['ID'] == pattern_id, 'PatternName'].values[0]
                py_resource = df_keys.loc[df_keys['ID'] == pattern_id, 'PyResource'].values[0]
                data.loc[data[url_column_name] == url, ['PatternSource', 'PatternIdentified', 'PatternName', 'PyResource']] = (
                    'Domain', pattern_id, pattern_name, py_resource)
            else:
                delay = random.randint(0, 5)
                await asyncio.sleep(delay)
                return await response.read()
        else:
            if match.extract_keywords(url):
                pattern_id = int(match.extract_keywords(url)[0])
                pattern_name = df_keys.loc[df_keys['ID'] == pattern_id, 'PatternName'].values[0]
                py_resource = df_keys.loc[df_keys['ID'] == pattern_id, 'PyResource'].values[0]
                data.loc[data[url_column_name] == url, ['PatternSource', 'PatternIdentified', 'PatternName', 'PyResource']] = (
                    'Domain', pattern_id, pattern_name, py_resource)
            print(response.status, url)
            data.loc[data[url_column_name] == url, 'URL Status Code'] = response.status
            if response.status == 404:
                data.loc[data[url_column_name] == url, 'PatternIdentified'] = '404 Error Detected'


async def get_page_source_tags(url, session, match):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                soup = bs(await response.read(), 'lxml')
                tags = soup.findAll(['a', 'script'])
                for tag in tags:
                    hook = tag.get('src') or tag.get('href')
                    if hook:
                        if match.extract_keywords(str(hook)):
                            pattern_id = int(match.extract_keywords(str(tag))[0])
                            pattern_name = df_keys.loc[df_keys['ID'] == pattern_id, 'PatternName'].values[0]
                            py_resource = df_keys.loc[df_keys['ID'] == pattern_id, 'PyResource'].values[0]
                            data.loc[data[url_column_name] == url, ['PatternSource', 'PatternIdentified', 'PatternName', 'PyResource']] = (
                                'Page Source', pattern_id, pattern_name, py_resource)
                            break
    except Exception as e:
        pass


async def run(url_list):
    tasks = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36"
    }

    connector = aiohttp.TCPConnector(ssl=False, limit=100, limit_per_host=10)
    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=25)
    async with aiohttp.ClientSession(connector=connector, timeout=session_timeout, headers=headers) as session:
        for url in url_list:
            task = asyncio.ensure_future(get_page_source_tags(url, session, match))
            tasks.append(task)

        await asyncio.gather(*tasks)


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

try:
    loop.run_until_complete(run(data[url_column_name].tolist()))
finally:
    loop.close()

print("\n\n\nData after Part-2 (Extracting patterns from page source) .....")
print(data)

print("\nPress Enter to continue...")
input('Continuing Part 3 (Finding new totally unknown pattern)')

# Part 3
# Analysis (Tokenization of domain, removing stopwords, Frequency Distribution ----> Finding new totally unknown pattern)

part3_data = data.fillna('')
url_list_3 = part3_data.loc[part3_data['PatternIdentified'] == '', url_column_name]

all_known_pattern_keyword = df_keys['Keyword'].tolist()

# Doing Analysis
freq_matcher = KeywordProcessor()
analysis = FreqDist(make_domain_list(url_list_3))

most_common_keys = [key for key, count in analysis.most_common(5) if key not in all_known_pattern_keyword + stopwords and count > 1]

print("Following URL look like a new pattern...")
for url in url_list_3:
    netloc = urlparse(url).netloc
    if freq_matcher.extract_keywords(netloc):
        data.loc[data[url_column_name] == url, 'PatternIdentified'] = f"New Pattern / {freq_matcher.extract_keywords(netloc)[0]}"

print("Final Data after marking each pattern....")
print(data)

data.to_excel(f'{excel_file_name[:-5]}_final_result.xlsx', sheet_name='Sheet1', index=False)
end = time.time()
print('Total Time:', end - start)
