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
        asp = True,
        country  = "gb",
    ))
    
    return result.content

def get_pages_nums(link):
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    pages_num = int(soup.find('nav',{'aria-label':"pagination"}).find_all('a')[-2].text.replace(',',''))
    return pages_num

def get_job_data(link):
    jobs_data = []
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    jobs = soup.find(id="job-list").find_all('li')
    for job in jobs:
        try:
            job_link = 'https://www.simplyhired.co.uk'+job.find('a')['href']
            title =  job.find("h2").text
            company = job.find('span',{"data-testid":"companyName"}).text
            try:
                posted = job.find('p',{'data-testid':"searchSerpJobDateStamp"}).text
            except:
                posted = ''
            salary = job.find('p',{"data-testid":"searchSerpJobSalaryEst"}).text.replace('Estimated: ','')
            location = job.find('span',{'data-testid':"searchSerpJobLocation"}).text
            jobs_data.append({'posted':posted,'job title':title,'company working':company,'location working':location,'salary':salary,'link':job_link})
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
    df.to_csv(f'output/simplyhired/data_by_location/simplyhired_output_{simplyhired_link["locations"]}_{date.today()}.csv',index=False)

def merge_data():
    folder_path = 'output/simplyhired/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith(f'{date.today()}.csv')]

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
            merged_file_path = os.path.join(new_folder_path, f'simplyhired_full_data_{date.today()}.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

