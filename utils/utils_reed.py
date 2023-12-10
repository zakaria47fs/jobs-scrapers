import os
import pandas as pd
from scrapfly import ScrapflyClient, ScrapeConfig
from bs4 import BeautifulSoup
import tqdm
import re
from datetime import date

from dotenv import load_dotenv
load_dotenv()

SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

def scrapfly_request(link):
    
    scrapfly = ScrapflyClient(key=SCRAPFLY_API_KEY)
    result = scrapfly.scrape(ScrapeConfig(
        url = link,
        country  = "gb",
    ))
    
    return result.content

def get_job_data(link):
    jobs_data = []
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    jobs = soup.find_all('article')
    for job in jobs:
        title_regex_pattern = re.compile(r'job-card_jobResultHeading__title__\w+')
        title = job.find('h2', class_=title_regex_pattern).text.strip()

        job_link = 'https://www.reed.co.uk'+ job.find('a')['href']

        date_company_regex_pattern = re.compile(r'job-card_jobResultHeading__postedBy\w+')
        job_date = job.find('div', class_=date_company_regex_pattern).text.split('by')[0].strip()
        company = job.find('div', class_=date_company_regex_pattern).text.split('by')[1].replace('\t','').strip()

        location_regex_pattern = re.compile(r'job-card_jobMetadata\w+')
        location = job.find('ul', class_=location_regex_pattern).find_all('li')[1].text.strip()
        
        jobs_data.append({'date':job_date, 'job title':title,'company working':company,'location working':location,'link':job_link})
    return jobs_data

def get_pages_nums(link):
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    pages_num = int(soup.find_all(class_="page-item")[-2].text.replace(',',''))
    return pages_num

def reed_scrap(reed_link):
    if not os.path.exists("output/reed/data_by_location"):
        os.makedirs("output/reed/data_by_location")
    if not os.path.exists("output/reed/full_data"):
        os.makedirs("output/reed/full_data")
    pages_num = get_pages_nums(reed_link['links'])
    job_data = []
    for page_num in tqdm.tqdm(range(1,pages_num+1)):
        url = reed_link['links'] + f'&pageno={page_num}'
        job_data.extend(get_job_data(url))

    df = pd.DataFrame(job_data)
    df.to_csv(f'output/reed/data_by_location/reed_output_{reed_link["locations"]}_{date.today()}.csv',index=False)

def merge_data():
    folder_path = 'output/reed/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith(f'_{date.today()}.csv')]

    new_folder_path = 'output\\reed\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, f'reed_full_data_{date.today()}.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

