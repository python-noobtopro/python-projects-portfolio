import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import warnings
import keyboard
import shutil
import random
import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import io
import os
from PIL import Image


class CaptchaCracker:
    def __init__(self):
        self.start_time = time.time()
        self.driver = self.__get_driver()
        self.attempts = 1

    @staticmethod
    def __get_driver(incognito=False, port_rotate=False):
        chrome_options = webdriver.ChromeOptions()
        if port_rotate is True:
            port_no = random.randint(8081, 9999)
        else:
            port_no = 8080
        if incognito is True:
            chrome_options.add_argument("--incognito")
        driver = webdriver.Chrome(
            r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe",
            chrome_options=chrome_options, port=port_no)
        return driver

    def __google_lens(self):
        driver = self.__get_driver(incognito=True, port_rotate=True)
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
        time.sleep(sleep_time)
        keyboard.write(
            os.path.join(os.getcwd() + "\captcha_ss.png"))
        keyboard.press_and_release('return')
        time.sleep(2)

        # Text button
        driver.find_element(By.XPATH, '//*[@id="ucj-4"]/span[1]').click()
        time.sleep(1)
        try:
            driver.find_element(By.XPATH,
                                '//*[@id="yDmH0d"]/div[3]/c-wiz/div/c-wiz/div/div[2]/div/div/div/div/div[1]/div/div[2]/div/button/span').click()
            answer = ''.join(driver.find_element(By.XPATH,
                                                 '//*[@id="yDmH0d"]/div[3]/c-wiz/div/c-wiz/div/div[2]/div/div/span/div/div[2]').get_attribute(
                'innerHTML').replace('"', '').split())
            return answer
        except Exception:
            driver.quit()
            return None

    def solve(self, url, captcha_xpath):
        self.driver.maximize_window()
        self.driver.get(url)
        time.sleep(5)

        # captcha screenshot and save
        captcha_elem = self.driver.find_element(By.XPATH, captcha_xpath)
        # Scroll to that element
        self.driver.execute_script("arguments[0].scrollIntoView();", captcha_elem)
        time.sleep(0.25)
        screenshot = captcha_elem.screenshot_as_png
        imageStream = io.BytesIO(screenshot)
        image = Image.open(imageStream)
        image.save('captcha_ss.png')
        answer = self.__google_lens()

        # Your code starts here
        if answer is not None:
            print(f"Captcha Answer from Google Lens ----> {answer}")
            user_answer = input("Please enter you answer (Y/N) ----> ")
            if user_answer == 'Y':
                print("Google lens gave correct answer. Quit Program.")

                # Create a folder to save the cracked captcha image
                if not os.path.exists(os.path.join(os.getcwd() + "\captcha_cracked")):
                    os.mkdir(os.path.join(os.getcwd() + "\captcha_cracked"))
                shutil.copy("captcha_ss.png", os.path.join(os.getcwd() + "\captcha_cracked\{}.png".format(answer)))
                total_time = round(time.time() - self.start_time, 2)
                info_data = [
                    {
                        'URL': url,
                        'ATTEMPTS': self.attempts,
                        'TIME ELAPSED (SECONDS)': total_time,
                        'CAPTCHA IMAGE PATH': os.path.join(os.getcwd() + "\captcha_cracked\{}.png".format(answer))
                    }
                ]
                if not os.path.exists(os.path.join(os.getcwd() + "\Testing_info.xlsx")):
                    with pd.ExcelWriter('Testing_info.xlsx') as writer:
                        pd.DataFrame.from_records(info_data).to_excel(writer, sheet_name='Testing Results', index=False)
                else:
                    with pd.ExcelWriter('Testing_info.xlsx', engine='openpyxl', mode='a',
                                        if_sheet_exists='overlay') as writer:
                        pd.DataFrame.from_records(info_data).to_excel(writer, sheet_name='Testing Results',
                                                                      startrow=writer.sheets['Testing Results'].max_row,
                                                                      header=False, index=False)
                print("Please check Testing_info.xlsx for Testing Info")
                self.driver.quit()
            else:
                print('Google Lens did not give correct answer, Retrying')
                self.attempts += 1
                self.solve(url, captcha_xpath)

        else:
            print('Google Lens could not get answer, Retrying')
            self.attempts += 1

            self.solve(url, captcha_xpath)


# Read Test Url Excel file
df = pd.read_excel("urls_testing.xlsx")
for url in df['URL (Text captcha)'][19:]:
    print("URL ---->", url)
    captcha_xpath = input("Please enter captcha xpath and press Enter ----> ")
    if captcha_xpath == 'Pass':
        continue
    crack = CaptchaCracker()
    crack.solve(
        url=url,
        captcha_xpath=captcha_xpath
    )
