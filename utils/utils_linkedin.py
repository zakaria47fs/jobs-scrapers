import os
import pandas as pd
from scrapfly import ScrapflyClient, ScrapeConfig
from bs4 import BeautifulSoup
from datetime import date, datetime
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import date

load_dotenv()

SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

def scrapfly_request(link):
    
    scrapfly = ScrapflyClient(key=SCRAPFLY_API_KEY)
    result = scrapfly.scrape(ScrapeConfig(
        url = link,
        country  = "gb" 
    ))
    
    return result.content

def linkedin_jobs_num(link):
    soup = BeautifulSoup(scrapfly_request(link),features="lxml")
    try:
        jobs_num = int(soup.find(class_="results-context-header__job-count").getText().replace('+','').replace(',',''))
    except:
        jobs_num = 25
    jobs_data = scrap_linkedin_jobs_data(soup)
    return jobs_num,jobs_data

def scrap_linkedin_jobs_data(soup):
    jobs = soup.find_all('li')
    jobs_data = []
    for job in jobs:
        try:
            job_title = job.find('a').getText().replace('\n','').strip()
            job_link = job.find('a')['href']
            location = job.find(class_="job-search-card__location").getText().replace('\n','').strip()
            try:
                company = job.find(class_="hidden-nested-link").getText().replace('\n','').strip()
            except:
                company = job.find(class_="base-search-card__subtitle").getText().replace('\n','').strip()
            posted = job.find('time')['datetime']
            days = (datetime.strptime(posted, "%Y-%m-%d").date()-date.today()).days
            if days<-2:
                continue
            job_data = {'posted':posted,'job title':job_title,'company working':company,'location working':location,'link':job_link}
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
    country  = "gb",
    retry=True))
    
    return result.content

def linkedin_scrap(linkedin_link):
    if not os.path.exists("output/linkedin/data_by_location"):
        os.makedirs("output/linkedin/data_by_location")

    if not os.path.exists("output/linkedin/full_data"):
        os.makedirs("output/linkedin/full_data")
    jobs_num,job_data = linkedin_jobs_num(linkedin_link['links'])
    for page in tqdm(range(1,jobs_num//25)):
        try:
            soup = BeautifulSoup(scrap_linkedin_api(linkedin_link['links'],page),features="lxml")
            job_data.extend(scrap_linkedin_jobs_data(soup))
        except:
            if page==20:
                break
    df = pd.DataFrame(job_data)
    df.to_csv(f'output/linkedin/data_by_location/linkedin_output_{linkedin_link["locations"]}_{date.today()}.csv',index=False)

def merge_data():
    folder_path = 'output/linkedin/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith(f'_{date.today()}.csv')]

    new_folder_path = 'output\\linkedin\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, f'linkedin_full_data_{date.today()}.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

