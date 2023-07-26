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
    jobs_num = soup.find(class_="jobsearch-JobCountAndSortPane-jobCount css-1af0d6o eu4oa1w0").find('span').getText().replace('jobs','').strip()
    jobs_data = scrap_indeed_jobs_data(soup)
    jobs_num = int(jobs_num.replace(',', ''))
    return jobs_num,jobs_data

def scrap_indeed_jobs_data(soup):   
    jobs_data = []
    jobs = soup.find(class_="jobsearch-ResultsList css-0").find_all('li')
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
            job_data = {'job_title':job_title,'job_link':job_link,'company':company,'location':location,'date_posted':date_posted}
            jobs_data.append(job_data)
        except:
            pass
    return jobs_data

def indeed_scrap(indeed_link):
    if not os.path.exists("output/indeed"):
        os.makedirs("output/indeed")
    jobs_num,job_data = indeed_jobs_num(indeed_link['links'])
    for page in tqdm.tqdm(range(1,jobs_num)//15):
        for i in range(3):
            try:
                soup = BeautifulSoup(scrapfly_request(indeed_link['links']+f'&start={10*page}'),features="lxml")
                job_data.extend(scrap_indeed_jobs_data(soup))
                break
            except:
                pass
    
    df = pd.DataFrame(job_data)
    df.to_csv(f'output/indeed/indeed_output_{indeed_link["locations"]}.csv')
