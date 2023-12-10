import os
import pandas as pd
from scrapfly import ScrapflyClient, ScrapeConfig
from bs4 import BeautifulSoup
import tqdm
from datetime import date

from dotenv import load_dotenv
load_dotenv()

SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

import logging

logging.basicConfig(filename='logs/log_totaljobs.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

def scrapfly_request(link):
    
    scrapfly = ScrapflyClient(key=SCRAPFLY_API_KEY)
    result = scrapfly.scrape(ScrapeConfig(
        url = link,
        country  = "gb",
    ))
    
    return result.content
    


def get_pages_nums(link):
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    pages_num = int(soup.find('span',{'data-at':"search-jobs-count"}).text.split(' Advanced Practitioner jobs')[0].replace(',',''))//25

    return pages_num

def get_job_data(link):
    jobs_data = []
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    jobs = soup.find_all('article')
    for job in jobs:
        try:
            job_link = 'https://www.totaljobs.com'+job.find('h2').find('a')['href']
            title =  job.find('h2').text.strip()
            company = job.find('span',{'data-at':"job-item-company-name"}).text
            date = job.find('span',{'data-at':"job-item-timeago"}).find('time')['datetime']
            location = job.find('span',{'data-at':"job-item-location"}).text
            jobs_data.append({'date':date,'job title':title,'company working':company,'location working':location,'link':job_link})
        except:
            pass
    return jobs_data

def totaljobs_scrap(totaljobs_link):
    if not os.path.exists("output/totaljobs/data_by_location"):
        os.makedirs("output/totaljobs/data_by_location")
    if not os.path.exists("output/totaljobs/full_data"):
        os.makedirs("output/totaljobs/full_data")
    pages_num = get_pages_nums(totaljobs_link['links'])
    job_data = []
    for page_num in tqdm.tqdm(range(1,pages_num+1)):
        try:
            url = totaljobs_link['links'] + f'&page={page_num}'

            job_data.extend(get_job_data(url))
        except Exception as e:
            logging.info(f"details :: {str(e)}")

    df = pd.DataFrame(job_data)
    df.to_csv(f'output/totaljobs/data_by_location/totaljobs_output_{totaljobs_link["locations"]}_{date.today()}.csv',index=False)

def merge_data():
    folder_path = 'output/totaljobs/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith(f'{date.today()}.csv')]

    new_folder_path = 'output\\totaljobs\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, f'totaljobs_full_data_{date.today()}.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

