import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

# Ignore (Deprecated)
class TagCollector(scrapy.Spider):
	name = 'ScrapyTagCollector'

	def __init__(self, urls=None, *args, **kwargs):
		super().__init__(*args, **kwargs)
		if urls:
			if isinstance(urls, str):
				self.start_urls = [urls]
			elif isinstance(urls, (list, tuple)):
				self.start_urls = list(urls)
			else:
				raise ValueError("Invalid URL format. URL can either be single 'URL' or [URL_LIST] or (URL_TUPLE).")
		else:
			self.start_urls = []

	def start_requests(self):
		for url in self.start_urls:
			yield scrapy.Request(url=url, callback=self.parse)

	def parse(self, response):
		tags = response.css('a::attr(href), script::attr(src)').getall()
		yield tags


# process = CrawlerProcess()
# process.crawl(TagCollector, urls=['https://docs.scrapy.org/en/latest/topics/practices.html'])
# process.start()
