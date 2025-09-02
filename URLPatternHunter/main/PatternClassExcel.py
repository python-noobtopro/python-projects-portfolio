from database.DataBase import DBCon
import time
import requests
import pyodbc
import json
from queue import Queue
from ipdb import set_trace
import asyncio
import aiohttp
import random
import pprint
from scrapy import Selector
import numpy as np
import keyboard
import pandas as pd
from urllib.parse import urlparse
from bs4 import BeautifulSoup as bs
from aiohttp import ClientSession
from flashtext import KeywordProcessor
import gevent
import warnings
warnings.filterwarnings("ignore")


class PatternHunter:
	" Pattern Hunter on a Single URL, URL list or URL tuple"

	def __init__(self, urls):
		self.master_output = []
		self.patterndbtable = DBCon().query_table()
		self.matcher = KeywordProcessor()
		"Create keyword matcher instantiated"
		self.create_keyword_matcher()
		self.initial_urls = Queue()
		self.after_domain_urls = Queue()
		self.headers = {
						"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 \
						(KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36"
		   			   }
		if urls:
			# Multi URLs
			if isinstance(urls, (list, tuple)):
				[self.initial_urls.put(url) for url in urls]
			# Single URL
			elif isinstance(urls, str):
				self.initial_urls.put(urls)
			else:
				raise ValueError("Invalid URL format. URL can either be single 'URL' or [URL_LIST] or (URL_TUPLE).")
		else:
			raise Exception("Please provide at least one URL")


	def send_req(self, url):
		"Takes URL ---> Returns response, status_code / None, Error"

		session = requests.Session()
		with session as s:
			try:
				res = s.get(url, headers=self.headers, verify=False, timeout=8)
				return res, res.status_code
			except Exception as e:
				return None, str(e)


	def update_url_status(self, url, patternid, patternname, pyresource, patternsource, responsecode, error):
		"Takes URL status parameters and adds to master_output"

		output = 	{	
						"URL": url,
						"PatternID": patternid,
						"PatternName": patternname,
						"PyResource": pyresource,
						"PatternSource": patternsource,
						"Response Code": responsecode,
						"Error": error

					}
		self.master_output.append(output)


	def create_keyword_matcher(self):
		try:
			patterndict = {str(k): [v] for k, v in zip(self.patterndbtable['ID'], self.patterndbtable['Keyword'])}
		except Exception as e:
			patterndict = {}
			print(f"Error in self.patterndbtable: {e}")
		if patterndict:
			assert len(patterndict) > 0, "Cannot create KeywordMatcher. Please check PatternDBTable in DBCon().query_table()"
			self.matcher.add_keywords_from_dict(patterndict)


	def domain_matcher(self):
		"Takes a URL. Tries to find pattern directly in the URL."

		while not self.initial_urls.empty():
			url = self.initial_urls.get()
			match = self.matcher.extract_keywords(url)
			if match:
				print(f"{url} ---> Pattern in Domain")
				pattern_id = int(self.matcher.extract_keywords(url)[0])
				pattern_name = self.patterndbtable.loc[self.patterndbtable['ID'] == pattern_id, 'PatternName'].values[0]
				py_resource = self.patterndbtable.loc[self.patterndbtable['ID'] == pattern_id, 'PyResource'].values[0]


				response, status = self.send_req(url)
				if response is None:
					self.update_url_status(url, pattern_id, pattern_name, py_resource, "Domain", "", status)
				else:
					self.update_url_status(url, pattern_id, pattern_name, py_resource, "Domain", status, "")
			else:
				self.after_domain_urls.put(url)


	def page_source_matcher(self):
		"Takes a URL. Tries to find pattern in the page source."

		while not self.after_domain_urls.empty():
			url = self.after_domain_urls.get()
			response, status = self.send_req(url)
			if response is None:
				self.update_url_status(url, "", "", "", "", "", status)
			else:
				sel = Selector(response)
				tags = sel.css('a::attr(href), script::attr(src)').getall()
				match = self.matcher.extract_keywords(tags[0]) if tags else None
				if match:
					print(f"{url} ---> Pattern in Page Source")
					pattern_id = int(self.matcher.extract_keywords(url)[0])
					pattern_name = self.patterndbtable.loc[self.patterndbtable['ID'] == pattern_id, 'PatternName'].values[0]
					py_resource = self.patterndbtable.loc[self.patterndbtable['ID'] == pattern_id, 'PyResource'].values[0]
					
					self.update_url_status(url, pattern_id, pattern_name, py_resource, "Page Source", status, "")

				else:
					print(f"{url} ---> No Pattern in Page Source")
					self.update_url_status(url, "", "", "", "", status, "")


	def run(self):
		"Running functions"

		self.domain_matcher()
		self.page_source_matcher()
		pprint.pprint(self.master_output)
		json_return = json.dumps(self.master_output, indent=2)
		return json_return





















