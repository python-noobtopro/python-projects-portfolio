import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import warnings
import keyboard
warnings.filterwarnings("ignore", category=DeprecationWarning)
chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('headless')
# chrome_options.add_argument("--incognito")
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import io
import os
from PIL import Image


def google_lens():
    driver1 = webdriver.Chrome(
        r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe",
        chrome_options=chrome_options)
    sleep_time = 1.25
    driver1.get('https://images.google.com/')
    try:
        accpt = driver1.find_element(By.XPATH, '//*[@id="L2AGLb"]/div')
        driver1.execute_script('arguments[0].click()', accpt)
    except Exception as e:
        pass
    time.sleep(sleep_time)
    lens = driver1.find_element(By.XPATH, '//div[@class="nDcEnd"]')
    driver1.execute_script('arguments[0].click();', lens)
    time.sleep(sleep_time)

    # Upload button
    driver1.find_element(By.XPATH, '//span[@class="DV7the"]').click()
    time.sleep(sleep_time)
    keyboard.write(
        os.getcwd() + "\captcha_ss.png")
    keyboard.press_and_release('return')
    time.sleep(2)

    # Translate button
    driver1.find_element(By.XPATH, '//*[@id="ucj-5"]/span[1]').click()
    time.sleep(2)
    try:
        answer = driver1.find_element(By.XPATH, '//div[@class="QeOavc"]/div').get_attribute('innerHTML').strip()
        print(answer)
        driver1.quit()
        return answer
    except Exception as e:
        driver1.quit()
        return None


def take_screenshot_captcha(captcha_path, url):
    driver = webdriver.Chrome(
        r"C:\Users\Rupesh.Ranjan\OneDrive - GlobalData PLC\Desktop\chromedriver_win32\chromedriver.exe",
        chrome_options=chrome_options)
    driver.maximize_window()
    time.sleep(0.5)
    driver.get(url)
    time.sleep(2)
    captcha_elem = driver.find_element(By.XPATH, captcha_path)
    screenshot = captcha_elem.screenshot_as_png
    imageStream = io.BytesIO(screenshot)
    image = Image.open(imageStream)
    image.save('captcha_ss.png')
    answer = google_lens()
    if answer is not None:
        # Entering keyword
        driver.find_element(By.XPATH, '//*[@id="searchword"]').send_keys('cancer')
        # box of captcha answer
        driver.find_element(By.XPATH, '//*[@id="T4"]').send_keys(answer)
        driver.implicitly_wait(4)
        # captcha submit button
        submit = driver.find_element(By.XPATH,
                                     '/html/body/div/center/div[3]/form/div/center/table[1]/tbody/tr[13]/td/input')
        driver.execute_script('arguments[0].click();', submit)
        time.sleep(1)
        try:
            WebDriverWait(driver, 5).until(EC.alert_is_present())
            print(driver.switch_to.alert.text)
            driver.switch_to.alert.accept()
            time.sleep(1)
            driver.close()
            print('--------Invalid CAPTCHA, Retrying---------')
            take_screenshot_captcha(captcha_path, url)
        except Exception:
            print('<<<<<  CRACK SUCCESSFUL  >>>>>>')
            print(f'We are inside --------> {driver.current_url}')
            driver.implicitly_wait(10)
            driver.quit()
    else:
        print('Did not Crack, Rebuilding loop with different port number')
        driver.quit()
        take_screenshot_captcha(captcha_path, url)


if __name__ == '__main__':
    take_screenshot_captcha(
        captcha_path='/html/body/div/center/div[3]/form/div/center/table[1]/tbody/tr[12]/td[2]/img',
        url='http://ctri.nic.in/Clinicaltrials/advancesearchmain.php'
    )

