import warnings
import requests
import base64
import pyperclip as pc
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
warnings.filterwarnings("ignore", category=DeprecationWarning)
chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('headless')
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
# chrome_options.add_argument("start-minimized")
# chrome_options.add_argument("--incognito")
from headless_captcha_cracker import *

driver = webdriver.Chrome(ChromeDriverManager().install())
driver.get('https://www.joinindianarmy.nic.in/Authentication.aspx')
captcha_elem = driver.find_element(By.XPATH, '//*[@id="divLogin"]/ul/li[1]/div/div[2]/div/img')
# captcha_elem.screenshot('test.png')
img_str = captcha_elem.screenshot_as_base64

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

json_data = {
    'captcha_img_base64_str': img_str
}
response = requests.post('http://127.0.0.1:8000/img_str', headers=headers, json=json_data)
print(response.json())
