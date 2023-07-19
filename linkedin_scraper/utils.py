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
        country  = "gb" 
    ))
    
    return result.content

def linkedin_jobs_num(link):
    soup = BeautifulSoup(scrapfly_request(link),features="lxml")
    jobs_num = soup.find(class_="results-context-header__job-count").getText().replace('+','').replace(',','')
    jobs_data = scrap_linkedin_jobs_data(soup)
    return jobs_num,jobs_data

def scrap_linkedin_jobs_data(soup):
    jobs = soup.find_all('li')
    jobs_data = []
    for job in jobs:
        try:
            title = job.find('a').getText().replace('\n','').strip()
            url = job.find('a')['href']
            location = job.find(class_="job-search-card__location").getText().replace('\n','').strip()
            try:
                company = job.find(class_="hidden-nested-link").getText().replace('\n','').strip()
            except:
                company = job.find(class_="base-search-card__subtitle").getText().replace('\n','').strip()
            job_data = {'title':title,'url':url,'location':location,'company':company}
            jobs_data.append(job_data)
        except:
            pass
    return jobs_data

def scrap_linkedin_api(link,page_num):

    location = link.split('&position')[0].split('location=')[1].split('geoId=')[0]
    geoId = link.split('&position')[0].split('location=')[1].split('geoId=')[1]

    url =  f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=&location={location}locationId=&geoId={geoId}&position=1&pageNum=1&start={page_num*25}'

    headers = {
      'authority': 'www.linkedin.com',
      'referer': link
    }
    
    scrapfly = ScrapflyClient(key=SCRAPFLY_API_KEY)
    result = scrapfly.scrape(ScrapeConfig(
    url = url,
    headers = headers,
    asp = True,
    country  = "gb" ))
    
    return result.content

def linkedin_scrap(linkedin_link):
    if not os.path.exists("output/linkedin"):
        os.makedirs("output/linkedin")
    jobs_num,job_data = linkedin_jobs_num(linkedin_link['links'])
    for page in tqdm.tqdm(range(1,int(jobs_num)//25)):
        soup = BeautifulSoup(scrap_linkedin_api(linkedin_link['links'],page),features="lxml")
        job_data.extend(scrap_linkedin_jobs_data(soup))
    
    df = pd.DataFrame(job_data)
    df.to_csv(f'output/linkedin/linkedin_output_{linkedin_link["locations"]}.csv')
