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
    pages_num = int(soup.find(class_="p-3 text-sm text-center").text.split('of')[1].replace(',',''))
    return pages_num

def get_job_data(link):
    jobs_data = []
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    jobs = soup.find(class_="ui-search-results w-full md:w-3/4").find_all(class_="w-full")
    for job in jobs:
        try:
            job_link = job.find('a',{'data-js':"jobLink"})['href']
            title =  job.find('h2').text.replace('\n','').strip()
            company = job.find(class_="ui-company").text
            location = job.find(class_="ui-location text-adzuna-gray-900").text
            jobs_data.append({'job title':title,'company working':company,'location working':location,'link':job_link})
        except:
            pass
    return jobs_data

def adzuna_scrap(adzuna_link):
    if not os.path.exists("output/adzuna/data_by_location"):
        os.makedirs("output/adzuna/data_by_location")
    if not os.path.exists("output/adzuna/full_data"):
        os.makedirs("output/adzuna/full_data")
    pages_num = get_pages_nums(adzuna_link['links'])
    job_data = []
    for page_num in tqdm.tqdm(range(1,pages_num+1)):
        url = adzuna_link['links'] + f'&pageno={page_num}'
        job_data.extend(get_job_data(url))

    df = pd.DataFrame(job_data)
    df.to_csv(f'output/adzuna/data_by_location/adzuna_output_{adzuna_link["locations"]}.csv',index=False)

def merge_data():
    folder_path = 'output/adzuna/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    new_folder_path = 'output\\adzuna\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, 'adzuna_full_data.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

