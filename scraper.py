# Module Imports
import os
b=os.path.dirname(os.path.abspath(__file__))
os.chdir(b)

import mariadb
import sys
import logging
import requests
import base64
logging.basicConfig(filename = "snd.log", level=logging.INFO, format= '%(levelname)s %(asctime)s: %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

import selenium
import urllib.request
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys

QUERY = 'INSERT INTO unprocessed_data_no_full(district,town,name,address,rooms,price,link) VALUES(%s, %s, %s, %s, %s, %s, %s)'

def get_judete(path):

    with open(path, "r") as f:
        file_jud = f.read()
        judete = file_jud.split("\n")

    print(judete)
    return judete

def prepare_driver(url):
    '''Returns a Firefox Webdriver.'''
    options = Options()
    options.add_argument('-headless')
    options.add_argument('-no-sandbox')
    logging.info('Driver is in preparation.')
    driver = Chrome(executable_path='chromedriver.exe', options=options)
    driver.get(url)
    wait = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
        (By.XPATH, '/html/body')))
    return driver

def scrape_results_for_district(driver, district, conn):
    global QUERY
    # Get Cursor
    cursor = conn.cursor()

    bacth_data = []
    try:
        town_container = driver.find_element_by_xpath('.//div[@class=" liste"]')
    except Exception:
        logging.error("No towns in " + district, exc_info=True)
        return None   

    towns = town_container.find_elements_by_tag_name("a")
    for town in towns:
        try:
            town_url = town.get_attribute("href")
            town_name = town.get_attribute("title").split(" ")[1]
            logging.info(town_name)
            town_driver = prepare_driver(town_url)
            logging.info(town_url)
            current_results = scrape_results_for_town(town_driver, district, town_name)
            if current_results != None:
                bacth_data.extend(current_results)
        except Exception:
            logging.exception("Exception while iterating towns in district " + district, exc_info=True)

        if len(bacth_data) > 200:
            logging.info("Batch_data length: " + str(len(bacth_data)))
            logging.info(bacth_data)
            cursor.executemany(QUERY, bacth_data)
            conn.commit()
            logging.warning("EXECUTED")
            bacth_data *= 0
    
    if len(bacth_data) > 0:
        logging.info("Batch_data length: " + str(len(bacth_data)))
        cursor.executemany(QUERY, bacth_data)
        conn.commit()
        logging.warning("EXECUTED")
        bacth_data *= 0           

def scrape_results_for_town(driver, district, town_name):
    '''Returns the data from n_results amount of results.'''
    logging.info(driver.current_url)

    current_data = []
    elements = []
    try:
        elements = driver.find_elements_by_xpath('.//div[@class="ucdetalii grey-text text-darken-1 inlinetc vtop"]')
    except Exception:
        logging.exception("No elements for town: " + town_name, exc_info=True)
        return None

    for element in elements:
        link = element.find_element_by_xpath('..').get_attribute('href')
        name = element.find_element_by_css_selector('.blue-text.text-darken-1').text
        name = name.strip()
        try:
            address = element.find_element_by_xpath('.//em/span[@itemprop="address"]').text
        except Exception:
            address = None
        logging.info("Current: " + name)
        try:
            rooms = element.find_element_by_xpath('.//div[@class="uclocuri valign-wrapper"]').text
            rooms = rooms.split('\n')[1].strip()
            capacity = rooms.split(' ')[0]
        except Exception:
            rooms = None
            capacity = None

        try:
            price = element.find_element_by_xpath('.//div[@class="preturilista ucsarbatori teal-text text-lighten-1"]').text
        except Exception:
            price = None

        name = name.replace("grade", "").strip()

        if name != None and address != None and capacity != None and price != None:
            continue

        # parent = element.find_element_by_xpath('.//parent::a')
        # image_loc = parent.find_element_by_xpath('.//div[@class="img-unitate inlinetc"]')
        # image_loc = image_loc.find_element_by_xpath('.//img')
        # image_src = "https://www.turistinfo.ro" + image_loc.get_attribute('data-original')
        
        # r = requests.get(image_src, stream=True)
        # image_str = ""
        # if r.status_code == 200:
        #     image_str = ("data:" + 
        #     r.headers['Content-Type'] + ";" +
        # "base64," + base64.b64encode(r.content).decode("utf-8"))

        row = tuple([district, town_name, name, address, rooms, price, link])
        logging.info(row)
        current_data.append(row)
        
    logging.info("Current length: " + str(len(current_data)))
    driver.close()
    return current_data

if __name__ == "__main__": 
    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user="admin",
            password="admin",
            host="127.0.0.1",
            port=3306,
            database="masterisi"

        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    

    judete = get_judete("judete.txt")
    driver = None
    try:
        for jud in judete:
            url = "https://www.turistinfo.ro/judet-" + jud +".html"
            print(url)
            driver = prepare_driver(url)
            scrape_results_for_district(driver, jud, conn)
            driver.close()
    except KeyboardInterrupt:
        print("exiting")
        if driver != None:
            driver.close()
        sys.exit(0)
