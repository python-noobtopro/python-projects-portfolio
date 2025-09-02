import time
from selenium import webdriver
import datetime
from webdriver_manager.chrome import ChromeDriverManager
import warnings
import base64
from pdb import set_trace as st

warnings.filterwarnings("ignore", category=DeprecationWarning)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('headless')
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

start_time = time.time()


def solve_captcha(img_base64_str=None):
    google_lens_driver = webdriver.Chrome(
        r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe",
        chrome_options=chrome_options)
    sleep_time = 1
    google_lens_driver.get('https://images.google.com/')
    try:
        accpt_btn = WebDriverWait(google_lens_driver, 1).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="L2AGLb"]/div')))
        google_lens_driver.execute_script('arguments[0].click()', accpt_btn)
    except Exception as e:
        # print(e)
        pass
    time.sleep(sleep_time)
    lens = google_lens_driver.find_element(By.XPATH, '//div[@class="nDcEnd"]')
    google_lens_driver.execute_script('arguments[0].click();', lens)
    time.sleep(sleep_time)
    ###
    base64_converted_img_stream = f"data:image/png;base64,{img_base64_str}"
    class_name = 'cB9M7'
    element = WebDriverWait(google_lens_driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, class_name)))
    # element.send_keys(pc.paste())
    script = f'arguments[0].value="{base64_converted_img_stream}";'
    google_lens_driver.execute_script(script, element)
    search_btn = WebDriverWait(google_lens_driver, 10).until(
        EC.presence_of_element_located((By.XPATH, '//div[text()="Search"]')))
    google_lens_driver.execute_script('arguments[0].click()', search_btn)
    translate_btn = WebDriverWait(google_lens_driver, 10).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="ucj-5"]/span[4]')))
    google_lens_driver.execute_script('arguments[0].click()', translate_btn)
    time.sleep(2)
    try:
        answer = google_lens_driver.find_element(By.XPATH, '//div[@class="QeOavc"]/div').get_attribute(
            'innerHTML').strip()
        print(answer)
        google_lens_driver.quit()
        total_time = time.time() - start_time
        print(f"Total Time Elapsed {round(time.time() - start_time, 2)} seconds")
        return answer, round(total_time, 2)
    except Exception as e:
        google_lens_driver.quit()
        total_time = time.time() - start_time
        return None, round(total_time, 2)
