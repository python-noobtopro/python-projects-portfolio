# Necessary imports
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import time
from urllib.parse import urljoin
import warnings
import pprint
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
warnings.filterwarnings("ignore")
chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('headless')
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--incognito")
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service

