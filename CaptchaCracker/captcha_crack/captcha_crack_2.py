import time
from selenium import webdriver
import datetime
from webdriver_manager.chrome import ChromeDriverManager
import warnings
import base64
import pyperclip as pc
import requests
import keyboard
from pdb import set_trace as st
warnings.filterwarnings("ignore", category=DeprecationWarning)
chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('headless')
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
# chrome_options.add_argument("start-minimized")
# chrome_options.add_argument("--incognito")
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import io
import os
from PIL import Image

global driver
global port
port = 1234


def google_lens(port=None, image=None):
    port = port
    driver1 = webdriver.Chrome(
        r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe",
        chrome_options=chrome_options, port=port)

    sleep_time = 1.25
    driver1.get('https://images.google.com/')
    try:
        accpt_btn = WebDriverWait(driver1, 1).until(EC.presence_of_element_located((By.XPATH, '//*[@id="L2AGLb"]/div')))
        driver1.execute_script('arguments[0].click()', accpt_btn)
    except Exception as e:
        # print(e)
        pass
    time.sleep(sleep_time)
    lens = driver1.find_element(By.XPATH, '//div[@class="nDcEnd"]')
    driver1.execute_script('arguments[0].click();', lens)
    time.sleep(sleep_time)
    pc.copy(image)
    # st()
    class_name = 'cB9M7'
    element = WebDriverWait(driver1, 5).until(EC.presence_of_element_located((By.CLASS_NAME, class_name)))
    # element.send_keys(pc.paste())
    element.send_keys(image)
    a = f'document.getElementsByClassName("cB9M7").value="{image}"'
    print(a)
    # driver.execute_script(str(a))
    search_btn = WebDriverWait(driver1, 10).until(EC.presence_of_element_located((By.XPATH, '//div[text()="Search"]')))
    driver1.execute_script('arguments[0].click()', search_btn)
    # time.sleep(2)
    driver1.get_screenshot_as_file("headless.png")
    translate_btn = WebDriverWait(driver1, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="ucj-5"]/span[4]')))
    driver1.execute_script('arguments[0].click()', translate_btn)
    time.sleep(2)
    try:
        answer = driver1.find_element(By.XPATH, '//div[@class="QeOavc"]/div').get_attribute('innerHTML').strip()
        print(answer)
        driver1.quit()
        return answer
    except Exception as e:
        driver1.quit()
        return None


global driver
driver = webdriver.Chrome(
        r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe")
url = 'http://ctri.nic.in/Clinicaltrials/advancesearchmain.php'
driver.maximize_window()
driver.get(url)
time.sleep(2)
def take_screenshot_captcha(path, port):
    # driver = webdriver.Chrome(
    #     r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe",
    #     chrome_options=chrome_options)
    # url = 'https://enquiry.indianrail.gov.in/'
    # driver.maximize_window()
    # driver.get(url)
    # time.sleep(2)
    captcha_elem = driver.find_element(By.XPATH, path)
    screenshot = captcha_elem.screenshot_as_base64
    base64_converted_url_stream = f"data:image/png;base64,{screenshot}"
    # imageStream = io.BytesIO(screenshot)
    # image = Image.open(imageStream)
    # image.save('captcha_ss.png')
    answer = google_lens(port, image=base64_converted_url_stream)
    if answer is not None:
        driver.find_element(By.XPATH, '//*[@id="T4"]').send_keys(answer)
        submit = driver.find_element(By.XPATH, '/html/body/div/center/div[3]/form/div/center/table[1]/tbody/tr[13]/td/input')
        driver.execute_script('arguments[0].click();', submit)
        time.sleep(1)
        if len(driver.current_url) == len(url):
            # driver.close()
            print('--------Invalid CAPTCHA, Retrying---------')
            take_screenshot_captcha('/html/body/b/img', port)
        else:
            print('<<<<<  CRACK SUCCESSFUL  >>>>>>')
            print(f'We are inside --------> {driver.current_url}')
            driver.implicitly_wait(10)
            driver.quit()
    else:
        print('Did not Crack, Rebuilding loop with different port number')
        # driver.quit()
        driver.refresh()
        port = 8080
        take_screenshot_captcha('/html/body/b/img', port)


take_screenshot_captcha('/html/body/div/center/div[3]/form/div/center/table[1]/tbody/tr[12]/td[2]/img', port)
