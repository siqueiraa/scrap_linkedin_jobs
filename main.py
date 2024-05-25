from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse
from datetime import timedelta

import sqlite3
import yaml
import time
import requests
import math
import logging
import random
import datetime
import json
import os
import re
import pytz
import langid
import sys
import getopt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_yaml(file_path="config.yml"):
    logging.info("Reading config file")
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


class ScrapLinkedin:
    """
    Class to scrap job details from LinkedIn.

    Args:
        keywords (str): Keywords to search for in job titles.
        location (str): Location to search for job postings.
        only_remote (bool, optional): Whether to include only remote jobs. Default is True.
        more_recents (bool, optional): Whether to prioritize more recent job postings. Default is True.
    """

    def __init__(self, keywords, location, only_remote=True, more_recents=True):
        """
        Initialize the ScrapLinkedin object.

        Args:
            keywords (str): Keywords to search for in job titles.
            location (str): Location to search for job postings.
            only_remote (bool, optional): Whether to include only remote jobs. Default is True.
            more_recents (bool, optional): Whether to prioritize more recent job postings. Default is True.
        """
        self.conn = sqlite3.connect('jobs.db')
        self.config = read_yaml()
        self.check_db()
        self.driver = self.connect_selenium()
        self.keywords = keywords
        self.location = location
        self.only_remote = only_remote
        self.more_recents = more_recents

    def check_db(self):
        logging.info("Checking db...")
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE if not exists jobs (
            Job_ID INTEGER PRIMARY KEY,
            type_work TEXT,
            time_work TEXT,
            Job_txt TEXT,
            company TEXT,
            job_title TEXT,
            level TEXT,
            location TEXT,
            posted_time_ago TEXT,
            nb_candidats TEXT,
            fit TEXT,
            employes TEXT,
            sector TEXT,
            scraping_date DATE,
            date_post TEXT,
            language TEXT,
            applied INTEGER
        )
        ''')

        self.conn.commit()

    def connect_selenium(self):
        logging.info("Connect selenium")
        service = Service()

        # 2. Instanciate the webdriver
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        driver = webdriver.Chrome(options=options, service=service)

        cookies_file = "cookies.json"
        if os.path.exists(cookies_file):
            driver.get("https://www.linkedin.com")
            with open(cookies_file, "r") as f:
                cookies = json.load(f)
                current_domain = urlparse(driver.current_url).netloc
                for cookie in cookies:
                    cookie['domain'] = cookie['domain'].lstrip(".")
                    if current_domain in cookie.get("domain", ""):
                        driver.add_cookie(cookie)
            driver.get("https://www.linkedin.com")
            with open(cookies_file, "w") as f:
                json.dump(driver.get_cookies(), f)
            return driver

        # 3. Open the LinkedIn login page
        driver.get("https://www.linkedin.com/login")
        time.sleep(5)  # waiting for the page to load

        # 4. Enter our email@ & pwd
        email_input = driver.find_element(By.ID, "username")
        password_input = driver.find_element(By.ID, "password")
        email_input.send_keys(self.config['credentials']['user'])
        password_input.send_keys(self.config['credentials']['password'])

        # 5. Click the login button
        password_input.send_keys(Keys.ENTER)

        # Save cookies after successful login
        with open(cookies_file, "w") as f:
            json.dump(driver.get_cookies(), f)

        return driver

    def scroll_to_bottom(self, sleep_time=10):
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        time.sleep(int(sleep_time))

    def parse_relative_time(self, relative_time):
        now = datetime.datetime.now(pytz.utc)
        relative_time = relative_time.replace("Reposted", "").strip()

        time_units = {
            'minute': 'minutes',
            'hour': 'hours',
            'day': 'days',
            'week': 'weeks',
            'month': 'months',
            'year': 'years'
        }

        for pattern, unit in time_units.items():
            match = re.match(rf'(\d+) {pattern}s? ago', relative_time)
            if match:
                value = int(match.group(1))
                if unit in {'months', 'years'}:
                    # Approximate months and years by converting to days
                    days = value * (30 if unit == 'months' else 365)
                    return (now - datetime.timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%S%z')
                else:
                    return (now - datetime.timedelta(**{unit: value})).strftime('%Y-%m-%dT%H:%M:%S%z')

        return None

    def find_job_ids(self, soup):
        """Parse the HTML content of the page (using BeautifulSoup) and find Job Ids"""
        Job_Ids_on_the_page = []

        job_postings = soup.find_all("li", {"class": "jobs-search-results__list-item"})
        for job_posting in job_postings:
            Job_ID = job_posting.get("data-occludable-job-id")
            Job_Ids_on_the_page.append(Job_ID)

        return Job_Ids_on_the_page

    def request_job_codes(self, page):
        url = f"https://www.linkedin.com/jobs/search/?keywords={self.keywords}&location={self.location}&start={page}&sortBy=DD{'&f_WT=2' if self.only_remote else ''}{'&f_TPR=r2592000' if self.more_recents else ''}"
        url = requests.utils.requote_uri(url)
        self.driver.get(url)
        self.scroll_to_bottom()

    def insert_job_ids(self, job_ids):
        cursor = self.conn.cursor()
        insert_query = '''
                    INSERT OR IGNORE INTO jobs (Job_ID) VALUES (?)
                    '''
        for job_id in job_ids:
            cursor.execute(insert_query, (job_id,))

        self.conn.commit()

    def insert_job_details(self, job_data):
        cursor = self.conn.cursor()
        job_data['scraping_date'] = datetime.datetime.now().date()  # Convert datetime to date

        try:
            cursor.execute('''
                INSERT OR REPLACE INTO jobs (Job_ID, type_work, time_work, level, Job_txt, language, company, job_title, location, posted_time_ago, date_post, nb_candidats, fit, employes, sector, scraping_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_data['Job_ID'],
                job_data['type_work'],
                job_data['time_work'],
                job_data['level'],
                job_data['Job_txt'],
                job_data['language'],
                job_data['company'],
                job_data['job_title'],
                job_data['location'],
                job_data['posted_time_ago'],
                job_data['date_post'],
                job_data['nb_candidats'],
                job_data['fit'],
                job_data['employes'],
                job_data['sector'],
                job_data['scraping_date']
            ))

        except Exception as e:
            logging.error(repr(e))
        finally:
            self.conn.commit()

    def list_ids_details(self):
        select_query = '''
            SELECT Job_ID
            FROM jobs
            WHERE company IS NULL OR company = ''
            '''

        # Executar a consulta
        cursor = self.conn.cursor()
        cursor.execute(select_query)

        results = cursor.fetchall()

        ids = [row[0] for row in results]
        return ids

    def update_posted_time_ago(self):
        cursor = self.conn.cursor()

        cursor.execute('''
            SELECT Job_ID, Job_txt FROM jobs WHERE Job_txt IS NOT NULL
        ''')

        rows = cursor.fetchall()

        for row in rows:
            job_id, posted_time_ago = row
            logging.info(f"Processing job id {job_id}")
            parsed_time, confidence = self.detect_language(posted_time_ago)
            formatted_time = parsed_time

            cursor.execute('''
                        UPDATE jobs
                        SET language = ?
                        WHERE Job_ID = ?
                    ''', (formatted_time, job_id))

        self.conn.commit()

    def scrap_ids(self):
        self.request_job_codes(0)

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        try:
            div_number_of_jobs = soup.find("div", {"class": "jobs-search-results-list__subtitle"})
            number_of_jobs = int(div_number_of_jobs.find("span").get_text().strip().split()[0].replace(",", ""))
        except:
            number_of_jobs = 0

        number_of_pages = math.ceil(number_of_jobs / 25)
        logging.info(f"Number Jobs: {number_of_jobs}")
        logging.info(f"Number of pages: {number_of_pages}")

        Jobs_on_this_page = self.find_job_ids(soup)
        self.insert_job_ids(Jobs_on_this_page)

        if number_of_pages > 1:
            for page_num in range(1, number_of_pages):
                logging.info(f"Scraping page: {page_num}")

                self.request_job_codes(25 * page_num)
                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                # Get Job Ids present on the first page.
                Jobs_on_this_page = self.find_job_ids(soup)
                if len(Jobs_on_this_page) <= 0:
                    logging.info("Finish scrap ids, no more jobs found")
                    break
                self.insert_job_ids(Jobs_on_this_page)
                logging.info(f"Jobs found:{len(Jobs_on_this_page)}")

    def detect_language(self, text):
        try:
            language, confidence = langid.classify(text)
            return language, confidence
        except Exception as e:
            return None, str(e)

    def scrap_details(self):
        list_job_IDs = self.list_ids_details()
        logging.info(f"Read details from {len(list_job_IDs)} jobs")
        job_url = "https://www.linkedin.com/jobs/view/{}"

        for j in range(0, len(list_job_IDs)):
            job = {}
            logging.info(f"{j + 1} - reading jobId: {list_job_IDs[j]}")

            self.driver.get(job_url.format(list_job_IDs[j]))
            try:
                random_sleep_duration = random.uniform(0.1, 2.0)
                time.sleep(random_sleep_duration)

                if "Too Many Requests" in self.driver.page_source:
                    logging.error("Got Too Many requests")
                    time.sleep(30)
                    self.driver.get(job_url.format(list_job_IDs[j]))

                element_present = EC.presence_of_element_located((By.CLASS_NAME, 'ui-label--accent-3'))
                WebDriverWait(self.driver, 10).until(element_present)
            except:
                time.sleep(10)
                self.driver.get(job_url.format(list_job_IDs[j]))

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            job["Job_ID"] = list_job_IDs[j]

            try:
                elements = self.driver.find_elements(By.XPATH,
                                                     "//span[@class='job-details-jobs-unified-top-card__job-insight-view-model-secondary']")
                if len(elements) <= 2:
                    remote_element = self.driver.find_element(By.XPATH,
                                                              "//span[contains(@class, 'ui-label--accent-3')]")
                    if remote_element:
                        job["type_work"] = remote_element.text.split('\n')[0] if len(
                            remote_element.text.split('\n')) > 0 else remote_element.text
                    else:
                        job["type_work"] = ''

                    contract_element = self.driver.find_elements(By.XPATH,
                                                                 "//span[@class='job-details-jobs-unified-top-card__job-insight-view-model-secondary']")[
                        0]
                    if contract_element:
                        job["time_work"] = contract_element.text.split('\n')[0] if len(
                            contract_element.text.split('\n')) > 0 else contract_element.text
                    else:
                        job["time_work"] = ''

                    level_element = self.driver.find_elements(By.XPATH,
                                                              "//span[@class='job-details-jobs-unified-top-card__job-insight-view-model-secondary']")[
                        1] if len(elements) > 1 else []
                    if level_element:
                        job["level"] = level_element.text.split('\n')[0] if len(
                            level_element.text.split('\n')) > 0 else level_element.text
                    else:
                        job["level"] = ''
                else:
                    job["type_work"] = elements[0].text.split('\n')[0] if len(elements[0].text.split('\n')) > 0 else \
                    elements[0].text
                    job["time_work"] = elements[1].text.split('\n')[0] if len(elements[1].text.split('\n')) > 0 else \
                    elements[1].text
                    job["level"] = elements[2].text.split('\n')[0] if len(elements[2].text.split('\n')) > 0 else \
                    elements[2].text
            except:
                job["type_work"] = ""
                job["time_work"] = ""
                job["level"] = ""

            try:  # remove tags
                description_tag = soup.find("div",
                                            class_="jobs-box__html-content jobs-description-content__text t-14 t-normal jobs-description-content__text--stretch").text
                description = description_tag

                job["Job_txt"] = description
                language, confidence = self.detect_language(description)
                job["language"] = language
            except:
                job["Job_txt"] = None
                job["language"] = None

            try:
                job["company"] = soup.find("div",
                                           class_="job-details-jobs-unified-top-card__company-name").text.replace("\n",
                                                                                                                  "")
            except:
                job["company"] = None

            try:
                job["job_title"] = soup.find("div",
                                             class_="t-24 job-details-jobs-unified-top-card__job-title").text.replace(
                    "\n", "")
            except:
                job["job_title"] = None

            location_details = soup.find("div", class_="job-details-jobs-unified-top-card__tertiary-description")

            try:
                job["location"] = location_details.contents[1].text
            except:
                job["location"] = None

            try:
                job["posted_time_ago"] = location_details.contents[3].text
                job["date_post"] = self.parse_relative_time(job["posted_time_ago"])
            except:
                job["posted_time_ago"] = None
                job["date_post"] = None

            try:
                job["nb_candidats"] = location_details.contents[5].text
            except:
                job["nb_candidats"] = None

            try:
                job["fit"] = soup.find("div", class_="display-flex flex-row align-items-center mt4").text.replace("\n",
                                                                                                                  "").strip()
            except:
                job["fit"] = None

            li_tags = soup.find_all('li', class_='job-details-jobs-unified-top-card__job-insight')
            try:
                if len(li_tags[1].get_text(strip=True).split("·")) == 2:
                    job["employes"] = li_tags[1].get_text(strip=True).split("·")[0]
                    job["sector"] = li_tags[1].get_text(strip=True).split("·")[1].strip()
                else:
                    job["employes"] = li_tags[1].get_text(strip=True)
                    job["sector"] = None
            except:
                job["employes"] = None
                job["sector"] = None

            self.insert_job_details(job_data=job)

    def fetch_jobs(self):
        one_month_ago = datetime.datetime.now() - datetime.timedelta(days=30)
        formatted_date = one_month_ago.strftime('%Y-%m-%dT%H:%M:%S%z')

        sql = """
                       SELECT *,
                            "https://www.linkedin.com/jobs/view/"||Job_ID AS link
                            FROM jobs j 
                            WHERE "language" IN ('en', 'br') 
                            AND date_post >= ?
                            AND type_work IN ('Remote', '')
                            AND "level" NOT IN ('Director', 'Entry level')
                            AND sector NOT IN ('Staffing and Recruiting')
                            AND job_title  NOT LIKE '%fullstack%'
                            AND fit NOT LIKE '%Stand out%'
                            AND time_work IS NOT NULL
                            AND applied IS null
                            ORDER BY date_post DESC   
                       """
        cursor = self.conn.cursor()
        cursor.execute(sql, (formatted_date,))
        jobs = cursor.fetchall()
        return jobs

    def update_job_status(self, job_id):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE jobs SET applied = 1 WHERE Job_ID = ?", (job_id,))
        self.conn.commit()

    def navigate_jobs(self):
        jobs = self.fetch_jobs()
        for job in jobs:
            job_id, link = job[0], job[-1]
            self.driver.get(link)
            logging.info(f"Opened job link: {link}")
            user_input = input("Press Enter to open the next job link or type 'exit' to quit: \n")
            if user_input.lower() == 'exit':
                break
            self.update_job_status(job_id)

    def parse_arguments(self, argv):
        try:
            opts, args = getopt.getopt(argv, "h", ["keywords=", "location=", "only_remote=", "more_recents="])
        except getopt.GetoptError:
            print("Usage: python script.py --keywords <keywords> --location <location> [--only_remote <True/False>] [--more_recents <True/False>]")
            sys.exit(2)

        for opt, arg in opts:
            if opt == '-h':
                print("Usage: python script.py --keywords <keywords> --location <location> [--only_remote <True/False>] [--more_recents <True/False>]")
                sys.exit()
            elif opt == "--keywords":
                self.keywords = arg
            elif opt == "--location":
                self.location = arg
            elif opt == "--only_remote":
                self.only_remote = arg.lower() == "true"
            elif opt == "--more_recents":
                self.more_recents = arg.lower() == "true"

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.close()


if __name__ == '__main__':
    scrap = ScrapLinkedin("", "")
    scrap.parse_arguments(sys.argv[1:])
    scrap.scrap_ids()
    scrap.scrap_details()
    scrap.navigate_jobs()


