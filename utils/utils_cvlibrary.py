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
    pages_num = int(soup.find(class_='search-header__results').text.split('of')[1].split('jobs')[0].replace(',',''))//25
    if pages_num==0:
        pages_num = 1
    return pages_num

def get_job_data(link):
    jobs_data = []
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    jobs = soup.find_all('article')
    for job in jobs:
        try:
            job_link = 'https://www.cv-library.co.uk'+job.find(class_="job__title").find('a')['href']
            title = job.find(class_="job__title").text.replace('\n','').strip()
            company = job.find(class_="job__company-link").text
            location = job.find(class_="job__details-location").text.replace('\n','').strip()
            date = job.find(class_="job__posted-by").find('span').text
            jobs_data.append({'date':date,'job title':title,'company working':company,'location working':location,'link':job_link})
        except:
            pass
    return jobs_data

def cvlibrary_scrap(cvlibrary_link):
    if not os.path.exists("output/cvlibrary/data_by_location"):
        os.makedirs("output/cvlibrary/data_by_location")
    if not os.path.exists("output/cvlibrary/full_data"):
        os.makedirs("output/cvlibrary/full_data")
    pages_num = get_pages_nums(cvlibrary_link['links'])
    job_data = []
    for page_num in tqdm.tqdm(range(1,pages_num+1)):
        url = cvlibrary_link['links'] + f'&page={page_num}'
        job_data.extend(get_job_data(url))

    df = pd.DataFrame(job_data)
    df.to_csv(f'output/cvlibrary/data_by_location/cvlibrary_output_{cvlibrary_link["locations"]}.csv',index=False)

def merge_data():
    folder_path = 'output/cvlibrary/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    new_folder_path = 'output\\cvlibrary\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, 'cvlibrary_full_data.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

