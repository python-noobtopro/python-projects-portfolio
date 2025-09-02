#!/usr/bin/env python
# coding: utf-8

import sys
import time
import asyncio
import aiohttp
import pandas as pd
from urllib.parse import urlparse
from aiohttp import ClientSession

start_time = time.time()

class YTBS:
	def __init__(self, excel_file_name, sheet_name, url_column_name):
		self.excel_file_name = excel_file_name
		self.sheet_name = sheet_name
		self.url_column_name = url_column_name
		try:
			self.data = pd.read_excel(f'{self.excel_file_name}', sheet_name=f'{self.sheet_name}')
		except:
			print(f"Cannot Read Excel. Retry with correct inputs.")
			sys.exit()

	async def fetch_url(self, url, session):
		try:
			async with session.get(url) as response:
				if response.status == 200:
					print(url, "-------> 200")
					self.data.loc[self.data[f'{self.url_column_name}'] == url, 'Active URL'] = url
				else:
					print(url, f"-------> {response.status}")
					self.data.loc[self.data[f'{self.url_column_name}'] == url, 'Inactive URL'] = url
		except Exception as e:
			print(url, "---------> Error:", str(e))

			# Remove later
			self.data.loc[self.data[f'{self.url_column_name}'] == url, 'Error'] = str(e)
			self.data.loc[self.data[f'{self.url_column_name}'] == url, 'Inactive URL'] = url

	async def run_async_webspider(self, url_list):
		tasks = []
		headers = {
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
		}
		connector = aiohttp.TCPConnector(ssl=False, limit=100, limit_per_host=30)
		# session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=30)
		async with aiohttp.ClientSession(connector=connector, headers=headers) as session: # timeout=session_timeout
			for url in url_list:
				task = asyncio.ensure_future(self.fetch_url(url, session))
				tasks.append(task)

			await asyncio.gather(*tasks)

	def run(self):
		url_list = self.data[self.url_column_name].to_list()
		loop = asyncio.get_event_loop()
		loop.run_until_complete(self.run_async_webspider(url_list))
		self.data.to_excel('Result_YTBulkSpider.xlsx', columns=['Active URL', 'Inactive URL'], index=False)

if __name__ == '__main__':
	excel_file_name = 'youtube_urls.xlsx'
	sheet_name = 'Sheet2'
	url_column_name = 'url'
	MySpider = YTBS(excel_file_name, sheet_name, url_column_name)
	MySpider.run()

end_time = time.time()
total_time = round((end_time - start_time)/60, 2)
print(f"Total Time: {total_time} MINS")