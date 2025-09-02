import time
from selenium import webdriver
import datetime
from webdriver_manager.chrome import ChromeDriverManager
import warnings
import pyperclip as pc
from pdb import set_trace as st

warnings.filterwarnings("ignore", category=DeprecationWarning)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('headless')
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

start_time = time.time()
def google_lens(image=None):
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
    pc.copy(image)
    class_name = 'cB9M7'
    element = WebDriverWait(google_lens_driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, class_name)))
    element.send_keys(pc.paste())
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
        # print(answer)
        google_lens_driver.quit()
        return answer
    except Exception as e:
        google_lens_driver.quit()
        return None


def solve_captcha(url, xpath):
    url_driver = webdriver.Chrome(
        r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe")
    url_driver.maximize_window()
    url_driver.get(url)
    time.sleep(1)
    captcha_elem = url_driver.find_element(By.XPATH, xpath)
    screenshot = captcha_elem.screenshot_as_base64
    base64_converted_img_stream = f"data:image/png;base64,{screenshot}"
    answer = google_lens(image=base64_converted_img_stream)
    if answer is not None:
        url_driver.quit()
        total_time = time.time() - start_time
        return total_time
    else:
        # retry google lens
        url_driver.refresh()
        solve_captcha(url, xpath)


if __name__ == '__main__':
    solve_captcha(
        url='http://ctri.nic.in/Clinicaltrials/advancesearchmain.php',
        xpath='/html/body/div/center/div[3]/form/div/center/table[1]/tbody/tr[12]/td[2]/img'
    )
