#!/usr/bin/env python
# coding: utf-8
import re

import nltk
from nltk.tokenize import RegexpTokenizer
from flashtext import KeywordProcessor
from nltk.probability import FreqDist
import pandas as pd
from urllib.parse import urlparse

# nltk.download()

# Reading Data
# df = pd.read_excel(
#     r'C://Users//Rupesh.Ranjan//OneDrive - GlobalData PLC//Desktop//self_made_modules//Jupyter_Notebooks//all_non_pattern_urls.xlsx')
# df.to_csv('all_non_pattern_urls.csv', index=False, header=['URL'])

# df = pd.read_csv('all_non_pattern_urls.csv')['URL']
# url_list = df.tolist()

countries = ['ac', 'ad', 'ae', 'af', 'ag', 'ai', 'al', 'am', 'an', 'ao', 'aq', 'ar', 'as', 'at', 'au', 'aw', 'ax', 'az',
             'ba', 'bb', 'bd', 'be', 'bf', 'bg', 'bh', 'bi', 'bj', 'bm', 'bn', 'bo', 'br', 'bs', 'bt', 'bv', 'bw', 'by',
             'bz', 'ca', 'cc', 'cd', 'cf', 'cg', 'ch', 'ci', 'ck', 'cl', 'cm', 'cn', 'co', 'cr', 'cs', 'cu', 'cv', 'cx',
             'cy', 'cz', 'dd', 'de', 'dj', 'dk', 'dm', 'do', 'dz', 'ec', 'ee', 'eg', 'eh', 'er', 'es', 'et', 'eu', 'fi',
             'fj', 'fk', 'fm', 'fo', 'fr', 'ga', 'gb', 'gd', 'ge', 'gf', 'gg', 'gh', 'gi', 'gl', 'gm', 'gn', 'gp', 'gq',
             'gr', 'gs', 'gt', 'gu', 'gw', 'gy', 'hk', 'hm', 'hn', 'hr', 'ht', 'hu', 'id', 'ie', '', '', 'il', 'im',
             'in', 'io', 'iq', 'ir', 'is', 'it', 'je', 'jm', 'jo', 'jp', 'ke', 'kg', 'kh', 'ki', 'km', 'kn', 'kp', 'kr',
             'kw', 'ky', 'kz', 'la', 'lb', 'lc', 'li', 'lk', 'lr', 'ls', 'lt', 'lu', 'lv', 'ly', 'ma', 'mc', 'md', 'me',
             'mg', 'mh', 'mk', 'ml', 'mm', 'mn', 'mo', 'mp', 'mq', 'mr', 'ms', 'mt', 'mu', 'mv', 'mw', 'mx', 'my', 'mz',
             'na', 'nc', 'ne', 'nf', 'ng', 'ni', 'nl', 'no', 'np', 'nr', 'nu', 'nz', 'om', 'pa', 'pe', 'pf', 'pg', 'ph',
             'pk', 'pl', 'pm', 'pn', 'pr', 'ps', 'pt', 'pw', 'py', 'qa', 're', 'ro', 'rs', 'ru', 'rw', 'sa', 'sb', 'sc',
             'sd', 'se', 'sg', 'sh', 'si', 'sj', 'sk', 'sl', 'sm', 'sn', 'so', 'sr', 'st', 'su', 'sv', 'sy', 'sz', 'tc',
             'td', 'tf', 'tg', 'th', 'tj', 'tk', 'tl', 'tm', 'tn', 'to', 'tp', 'tr', 'tt', 'tv', 'tw', 'tz', 'ua', 'ug',
             'uk', 'us', 'uy', 'uz', 'va', 'vc', 've', 'vg', 'vi', 'vn', 'vu', 'wf', 'ws', 'ye', 'yt', 'za', 'zm', 'zw']
all_extnsn = ['com', 'org', 'net', 'int', 'edu', 'gov', 'mil', 'biz']
pattern_1 = re.compile("career[1-9]")
extras = ['www', 'www2', 'govt', 'career', 'careers', 'jobs', 'jobs', 'apply', 'recruit', pattern_1, 'job']

stopwords = countries + all_extnsn + extras


def tokenize_domain_remove_stopwords(domain):
    tokenizer = RegexpTokenizer(r'\w+')
    bow = tokenizer.tokenize(domain)
    filter_bow = [i for i in bow if i not in stopwords]
    return filter_bow


# To be used for analysis
master_doc_list = []


def make_domain_list(data):
    global master_doc_list
    for url in data:
        try:
            master_doc_list = master_doc_list + tokenize_domain_remove_stopwords(urlparse(url).netloc)
        except Exception as e:
            print(f"Cannot parse {url} ----->  {e}")
            pass
    return master_doc_list


