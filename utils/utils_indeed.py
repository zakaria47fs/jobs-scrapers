import os
import pandas as pd
from scrapfly import ScrapflyClient, ScrapeConfig
from bs4 import BeautifulSoup
import tqdm

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
            company = job.find(class_="companyName").getText()
            location = job.find(class_="companyLocation").getText()
            date_posted = job.find(class_="date").getText().replace('Posted','')
            job_data = {'date':date_posted, 'job title':job_title,'company working':company,'location working':location,'link':job_link}
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
    for page in range(1,jobs_num//15):
        try:
            soup = BeautifulSoup(scrapfly_request(indeed_link['links']+f'&start={10*page}'),features="lxml")
            job_data.extend(scrap_indeed_jobs_data(soup))
        except:
           break
    
    df = pd.DataFrame(job_data)
    df.to_csv(f'output/indeed/data_by_location/indeed_output_{indeed_link["locations"]}.csv',index=False)

def merge_data():
    folder_path = 'output/indeed/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

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
            merged_file_path = os.path.join(new_folder_path, 'indeed_full_data.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

