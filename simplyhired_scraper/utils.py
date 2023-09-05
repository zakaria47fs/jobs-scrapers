import os
import pandas as pd
from scrapfly import ScrapflyClient, ScrapeConfig
from bs4 import BeautifulSoup
import tqdm
import re

from dotenv import load_dotenv
load_dotenv()

SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

def scrapfly_request(link):
    
    scrapfly = ScrapflyClient(key=SCRAPFLY_API_KEY)
    result = scrapfly.scrape(ScrapeConfig(
        url = link,
        asp = True,
        country  = "gb",
    ))
    
    return result.content

def get_pages_nums(link):
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    pages_num = int(soup.find(class_="pagination Pagination Pagination--withTotalJobCounts").find_all('li')[-2].text)
    return pages_num

def get_job_data(link):
    jobs_data = []
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    jobs = soup.find_all('article')
    for job in jobs:
        try:
            job_link = 'https://www.simplyhired.co.uk'+job.find(class_="jobposting-title").find('a')['href']
            title =  job.find(class_="jobposting-title").text
            company = job.find(class_="jobposting-subtitle").text.split('-')[0]
            date = job.find(class_="SerpJob-timestamp").find('time')['datetime']
            location = job.find(class_="jobposting-subtitle").text.split('-')[1]
            jobs_data.append({'date':date,'job title':title,'company working':company,'location working':location,'link':job_link})
        except:
            print('passed')
    return jobs_data

def simplyhired_scrap(simplyhired_link):
    if not os.path.exists("output/simplyhired/data_by_location"):
        os.makedirs("output/simplyhired/data_by_location")
    if not os.path.exists("output/simplyhired/full_data"):
        os.makedirs("output/simplyhired/full_data")
    pages_num = get_pages_nums(simplyhired_link['links'])
    job_data = []
    for page_num in tqdm.tqdm(range(1,pages_num+1)):
        url = simplyhired_link['links'] + f'&pageno={page_num}'
        job_data.extend(get_job_data(url))

    df = pd.DataFrame(job_data)
    df.to_csv(f'output/simplyhired/data_by_location/simplyhired_output_{simplyhired_link["locations"]}.csv',index=False)

def merge_data():
    folder_path = 'output/simplyhired/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    new_folder_path = 'output\\simplyhired\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, 'simplyhired_full_data.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

