import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import warnings
import keyboard
warnings.filterwarnings("ignore", category=DeprecationWarning)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import io
import os
from PIL import Image


def _get_driver(incognito=False):
    chrome_options = webdriver.ChromeOptions()
    if incognito is True:
        chrome_options.add_argument("--incognito")
    driver = webdriver.Chrome(
        r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe",
        chrome_options=chrome_options)
    return driver


def _google_lens():
    driver = _get_driver(incognito=True)
    sleep_time = 1.25
    driver.get('https://images.google.com/')
    try:
        accpt = driver.find_element(By.XPATH, '//*[@id="L2AGLb"]/div')
        driver.execute_script('arguments[0].click()', accpt)
    except Exception:
        pass
    time.sleep(sleep_time)
    lens = driver.find_element(By.XPATH, '//div[@class="nDcEnd"]')
    driver.execute_script('arguments[0].click();', lens)
    time.sleep(sleep_time)

    # Upload button
    driver.find_element(By.XPATH, '//span[@class="DV7the"]').click()
    time.sleep(0.5)
    keyboard.write(
        os.getcwd() + "\captcha_ss.png")
    keyboard.press_and_release('return')
    time.sleep(4)

    # Text button
    driver.find_element(By.XPATH, '//*[@id="ucj-4"]/span[1]').click()
    time.sleep(1)
    try:
        driver.find_element(By.XPATH,
                            '//*[@id="yDmH0d"]/div[3]/c-wiz/div/c-wiz/div/div[2]/div/div/div/div/div[1]/div/div[2]/div/button/span').click()
        answer = driver.find_element(By.XPATH,
                                     '//*[@id="yDmH0d"]/div[3]/c-wiz/div/c-wiz/div/div[2]/div/div/span/div/div[2]').get_attribute(
            'innerHTML').replace('"', '').strip()
        return answer
    except Exception:
        driver.quit()
        return None


class CaptchaCracker:
    def __init__(self):
        self.start_time = time.time()
        pass

    def solve(self, url, captcha_xpath):
        driver = _get_driver(incognito=True)
        driver.maximize_window()
        driver.get(url)
        time.sleep(2)
        captcha_elem = driver.find_element(By.XPATH, captcha_xpath)
        screenshot = captcha_elem.screenshot_as_png
        imageStream = io.BytesIO(screenshot)
        image = Image.open(imageStream)
        image.save('captcha_ss.png')
        answer = _google_lens()

        # Your code starts here
        if answer is not None:
            print(f"Captcha Answer from Google Lens ----> {answer}")
            print("Attempting Captcha in the webpage")

            # Entering keyword anywhere in the webpage (comment if not required)
            keyword_xpath = '//*[@id="searchword"]'
            driver.find_element(By.XPATH, keyword_xpath).send_keys('cancer')

            # box of captcha answer
            answer_box_xpath = '//*[@id="T4"]'
            driver.find_element(By.XPATH, answer_box_xpath).send_keys(answer)

            # driver.implicitly_wait(2)

            # captcha submit button
            submit_btn_xpath = '/html/body/div/center/div[3]/div[1]/form/div/div[3]/input[2]'
            submit = driver.find_element(By.XPATH, submit_btn_xpath)
            driver.execute_script('arguments[0].click();', submit)

            time.sleep(1)

            # Clinical Trial popup handling
            try:
                WebDriverWait(driver, 5).until(EC.alert_is_present())
                print(driver.switch_to.alert.text)
                driver.switch_to.alert.accept()
                time.sleep(1)
                driver.close()
                print('-------- Invalid CAPTCHA, Retrying ---------')
                self.solve(url, captcha_xpath)
            except Exception:
                print('<<<<<<<<<  CRACK SUCCESSFUL  >>>>>>>>>')
                print(f'We are inside --------> {driver.current_url}')
                total_time = round(time.time() - self.start_time, 2)
                print(f"Total Time Elapsed ---> {total_time} secs")

                # Write scraping code here (as a function)
                print("Start Scraping from here")
                # driver.implicitly_wait(10)
                driver.quit()
        else:
            print('Google Lens did not give answer, Rerunning the loop')
            driver.quit()
            self.solve(url, captcha_xpath)


if __name__ == '__main__':
    crack = CaptchaCracker()
    crack.solve(
        url='http://ctri.nic.in/Clinicaltrials/login.php',
        captcha_xpath='/html/body/div/center/div[3]/div[1]/form/div/div[3]/img'
    )
