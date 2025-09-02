# import requests
# from selenium.webdriver.chrome.service import Service
# import warnings
# import time
# import json
# import pandas as pd
# import re
# from bs4 import BeautifulSoup as bs
# from selenium import webdriver
# import datetime
# from dateutil.parser import parse
# from requests.packages.urllib3.exceptions import InsecureRequestWarning
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.common.keys import Keys
# requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
# warnings.filterwarnings("ignore", category=DeprecationWarning)
# chrome_options = webdriver.ChromeOptions()
# # chrome_# options.add_argument('headless')
# chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
# # chrome_options.add_argument("start-minimized")
# # chrome_options.add_argument("--incognito")
# from selenium.webdriver.common.by import By
#
# service = Service()
# options = webdriver.ChromeOptions()
# # options.add_argument('headless')
# driver = webdriver.Chrome(service=service, options=options)
# driver.get("https://www.google.co.in/maps/search/pubs+in+hyderabad/@19.0932125,72.6466303,10.6z/data=!3m1!4b1?entry=ttu")
# # elem = driver.find_element(By.XPATH, '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[1]/div[1]/div[3]/div/a').get_attribute('href')
# # print(elem)
# # driver.get("https://www.google.co.in/maps/place/The+Irish+House,+Lower+Parel/@18.9945017,72.8248275,17z/data=!3m1!4b1!4m6!3m5!1s0x3be7ce8c8c13dbfb:0x8f47c316d4ff4bee!8m2!3d18.9945017!4d72.8248275!16s%2Fg%2F12640jmd8?authuser=0&hl=en&entry=ttu")
# # time.sleep(2)
# # txt = driver.find_element(By.XPATH, '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[11]/div[3]/button/div/div[2]/div[1]').get_attribute('innerHTML')
# # print(txt)
# divSideBar = driver.find_element(By.XPATH, '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[1]/div[1]')
# keepScrolling=True
#
# i = 1
# while(keepScrolling):
#     divSideBar.send_keys(Keys.PAGE_DOWN)
#     time.sleep(0.5)
#     divSideBar.send_keys(Keys.PAGE_DOWN)
#     time.sleep(0.5)
#     html =driver.find_element(By.TAG_NAME, "html").get_attribute('outerHTML')
#     # i += 1
#     # if i == 100:
#     #     keepScrolling = False
#     if (html.find("You've reached the end of the list.") != -1):
#         keepScrolling = False
#
# hrefs = [elem.get_attribute('href') for elem in driver.find_elements(By.XPATH, '//a[@class="hfpxzc"]')]
# print(len(hrefs))
# master_data = []
# for href in hrefs:
#     data_dict = {}
#     data_dict['Link'] = href
#     print(href)
#     driver.get(href)
#     time.sleep(2)
#     name = driver.find_element(By.XPATH, '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[1]/h1').get_attribute('innerHTML').replace('<span class="a5H0ec"></span>', '').replace('<span class="G0bp3e"></span>', '')
#     print(name)
#     data_dict['Name'] = name
#     try:
#         address = driver.find_element(By.XPATH, '//button[@data-item-id="address"]/div/div[2]/div[1]').get_attribute('innerHTML')
#     except:
#         address = ''
#     data_dict['Address'] = address
#     print(address)
#     try:
#         phone = driver.find_element(By.XPATH, '//button[@data-tooltip="Copy phone number"]/div/div[2]/div[1]').get_attribute('innerHTML')
#     except:
#         phone = ''
#     data_dict['Phone'] = address
#     print(phone)
#     master_data.append(data_dict)
#
# df = pd.DataFrame(master_data)
# df.to_excel('Test.xlsx', index=False)
#
#
#
from serpapi import GoogleSearch

params = {
  "engine": "google_maps",
  "q": "pizza",
  "ll": "@40.7455096,-74.0083012,15.1z",
  "type": "search",
  "api_key": "secret_api_key"
}

search = GoogleSearch(params)
results = search.get_dict()
local_results = results["local_results"]