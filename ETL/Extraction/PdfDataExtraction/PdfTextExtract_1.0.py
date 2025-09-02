__author__ = 'Rupesh Ranjan'

import struct

import time

"""
This class helps in text extraction from 'pdf url' directly
"""

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams
from pdfminer.converter import TextConverter
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfdocument import PDFDocument
import io
from io import StringIO
from io import BytesIO
import os
from urllib.request import Request, urlopen
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('headless')

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
        # Added drive
        remote_file = urlopen(Request(self.url)).read()
        memory_file = io.BytesIO(remote_file)
        pdf_file = PdfReader(memory_file)
        return pdf_file

    # Extract specific page
    # def extract_page(self, page_no):
    #     page = self.make_temp_data().pages[page_no - 1]
    #     text = page.extract_text().replace('\n', '').strip()
    #     return text

    # Extract all pages at once
    def extract_all(self):
        output_string = StringIO()
        parser = PDFParser(self.make_temp_data())
        doc = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        device = TextConverter(rsrcmgr=rsrcmgr, outfp=output_string, laparams=LAParams())
        interpreter = PDFPageInterpreter(rsrcmgr=rsrcmgr, device=device)
        for page in PDFPage.create_pages(doc):
            interpreter.process_page(page)
        return output_string.getvalue()


TextExtract('https://oncoresponse.com/careers/Director-Sr-Director-of-Translational-Medicine.pdf').extract_all()


