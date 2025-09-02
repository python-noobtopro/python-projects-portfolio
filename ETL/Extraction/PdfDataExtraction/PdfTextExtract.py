__author__ = 'Rupesh Ranjan'

import time

"""
This class helps in text extraction from 'pdf url' directly
"""

import io

import os
from urllib.request import Request, urlopen
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

chrome_options = webdriver.ChromeOptions()
# Automatically download PyPDF2 package if not found
try:
    __import__("PyPDF2")
except ImportError:
    os.system("pip install " + "PyPDF2")

from PyPDF2 import PdfReader


class TextExtract:
    def __init__(self, url):
        self.url = url
        # print("Downloading webdriver in cache...Please wait...")
        # self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # make temporary file
    def make_temp_data(self):
        req = Request(url=self.url, headers={'User-Agent': 'Mozilla/5.0'})
        response = urlopen(req).read()
        # driver.get(self.url)
        # remote_file=driver.page_source

        memory_file = io.BytesIO(response)
        pdf_file = PdfReader(memory_file)
        return pdf_file

    # Extract specific page
    def extract_page(self, page_no):
        page = self.make_temp_data().pages[page_no - 1]
        text = page.extract_text().replace('\n', '').strip()
        return text

    # Extract all pages at once
    def extract_all(self):
        page_text = []
        for page in self.make_temp_data().pages:
            page_text.append(page.extract_text().replace('\n', '').strip())
        complete_text = '\n'.join(page_text)
        return complete_text


print(TextExtract('https://www.solinftec.com/wp-content/uploads/2022/09/CAN-Sales-Specialist-Solinftec.pdf').extract_all())
