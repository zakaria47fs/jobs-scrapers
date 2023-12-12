import os
import pandas as pd
from scrapfly import ScrapflyClient, ScrapeConfig
from bs4 import BeautifulSoup
from tqdm import tqdm
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

def indeed_jobs_num(link):
    soup = BeautifulSoup(scrapfly_request(link),features="lxml")
    try:
        jobs_num = int(soup.find(class_="jobsearch-JobCountAndSortPane-jobCount css-1af0d6o eu4oa1w0").find('span').getText().replace('jobs','').replace(',', '').strip())
    except:
        jobs_num = 1
    jobs_data = scrap_indeed_jobs_data(soup)
    return jobs_num,jobs_data

def scrap_indeed_jobs_data(soup):   
    jobs_data = []
    jobs = soup.find(id="mosaic-jobResults").find_all('li')
    for job in jobs:
        try:
            job_title = job.find(class_="jobTitle").getText()
            try:
                job_link = 'https://uk.indeed.com/viewjob?'+job.find(class_="jobTitle").find('a')['href'].split('clk?')[1]
            except:
                job_link = 'https://uk.indeed.com'+job.find(class_="jobTitle").find('a')['href']
            company = job.find('span',{'data-testid':"company-name"}).getText()
            location = job.find('div',{'data-testid':"text-location"}).getText()
            posted = job.find(class_="date").getText().replace('Posted','')
            salary = job.find(class_="salary-snippet-container").text
            job_data = {'posted':posted, 'job title':job_title,'company working':company,'location working':location,'salary':salary,'link':job_link}
            jobs_data.append(job_data)
        except:
            pass
    return jobs_data

def indeed_scrap(indeed_link):
    if not os.path.exists("output/indeed/data_by_location"):
        os.makedirs("output/indeed/data_by_location")
    if not os.path.exists("output/indeed/full_data"):
        os.makedirs("output/indeed/full_data")
    jobs_num,job_data = indeed_jobs_num(indeed_link['links'])
    for page in tqdm(range(1,jobs_num//15)):
        try:
            soup = BeautifulSoup(scrapfly_request(indeed_link['links']+f'&start={10*page}'),features="lxml")
            job_data.extend(scrap_indeed_jobs_data(soup))
        except:
           break
    
    df = pd.DataFrame(job_data)
    df.to_csv(f'output/indeed/data_by_location/indeed_output_{indeed_link["locations"]}_{date.today()}.csv',index=False)

def merge_data():
    folder_path = 'output/indeed/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith(f'_{date.today()}.csv')]

    new_folder_path = 'output\\indeed\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, f'indeed_full_data_{date.today()}.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

