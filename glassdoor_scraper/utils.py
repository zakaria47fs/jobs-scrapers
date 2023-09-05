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
    pages_num = int(soup.find(class_="paginationFooter").text.split('of')[1])
    return pages_num

def get_job_data(link):
    jobs_data = []
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    jobs = soup.find(id="MainCol").find_all('li')
    for job in jobs:
        try:
            job_link = 'https://www.glassdoor.co.uk' + job.find('a')['href']
            
            title_regex_pattern = re.compile(r'job-title \w+')
            title = job.find(class_=title_regex_pattern).text.strip()
            
            company_regex_pattern = re.compile(r'job-search-\w+')
            company = job.find('div',class_=company_regex_pattern).find('div',class_=company_regex_pattern).find('div',class_=company_regex_pattern).text
            
            location_regex_pattern = re.compile(r'location \w+')
            location = job.find(class_=location_regex_pattern).text.strip()
            
            date = job.find('div',{'data-test':"job-age"}).text
            jobs_data.append({'date':date,'job title':title,'company working':company,'location working':location,'link':job_link})
        except:
            pass
    return jobs_data

def glassdoor_scrap(glassdoor_link):
    if not os.path.exists("output/glassdoor/data_by_location"):
        os.makedirs("output/glassdoor/data_by_location")
    if not os.path.exists("output/glassdoor/full_data"):
        os.makedirs("output/glassdoor/full_data")
    pages_num = get_pages_nums(glassdoor_link['links'])
    job_data = []
    for page_num in tqdm.tqdm(range(1,pages_num+1)):
        url = glassdoor_link['links'].split('.htm')[0] + f'_IP{page_num}'+'.htm'+glassdoor_link['links'].split('.htm')[1]
        job_data.extend(get_job_data(url))

    df = pd.DataFrame(job_data)
    df.to_csv(f'output/glassdoor/data_by_location/glassdoor_output_{glassdoor_link["locations"]}.csv',index=False)

def merge_data():
    folder_path = 'output/glassdoor/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    new_folder_path = 'output\\glassdoor\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, 'glassdoor_full_data.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

