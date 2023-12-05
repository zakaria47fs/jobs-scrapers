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
    pages_regex_pattern = re.compile(r'SearchResultsHeader_jobCount\w+')

    pages_num = int(soup.find(class_=pages_regex_pattern).text.split()[0].replace(',',''))//30
    return pages_num

def get_job_data(link):
    jobs_data = []
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    jobs = soup.find_all('li',{'data-test':"jobListing"})
    for job in jobs:
        # try:
            if job.find('a')['href'].startswith('https://www.glassdoor.co.uk'):
                job_link = job.find('a')['href']
            else:
                job_link = 'https://www.glassdoor.co.uk' + job.find('a')['href']
            
            title_regex_pattern = re.compile(r'JobCard_seoLink__\w+')
            title = job.find('a',class_=title_regex_pattern).text.strip()
            
            company_regex_pattern = re.compile(r'EmployerProfile_profileContainer__\w+')
            company = job.find('div',class_=company_regex_pattern).text
            
            location = job.find('div',{'data-test':"emp-location"}).text.strip()
            
            date = job.find('div',{'data-test':"job-age"}).text
            jobs_data.append({'date':date,'job title':title,'company working':company,'location working':location,'link':job_link})
        # except:
        #     pass
    return jobs_data

def glassdoor_scrap(glassdoor_link):
    if not os.path.exists("output/glassdoor/data_by_location"):
        os.makedirs("output/glassdoor/data_by_location")
    if not os.path.exists("output/glassdoor/full_data"):
        os.makedirs("output/glassdoor/full_data")
    pages_num = get_pages_nums(glassdoor_link['links'])
    job_data = []
    for page_num in tqdm.tqdm(range(1,pages_num+1)[:10]):
        url = glassdoor_link['links'].split('.htm')[0] + f'_IP{page_num}'+'.htm'+glassdoor_link['links'].split('.htm')[1]
        job_data.extend(get_job_data(url))

    df = pd.DataFrame(job_data)
    df.to_csv(f'output/glassdoor/data_by_location/glassdoor_output_{glassdoor_link["locations"]}_{date.today()}.csv',index=False)

def merge_data():
    folder_path = 'output/glassdoor/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith(f'_{date.today()}.csv')]

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
            merged_file_path = os.path.join(new_folder_path, f'glassdoor_full_data_{date.today()}.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

