import os
import pandas as pd
from scrapfly import ScrapflyClient, ScrapeConfig
from bs4 import BeautifulSoup
from tqdm import tqdm 
import re
import requests
import json
from datetime import date

from dotenv import load_dotenv
load_dotenv()

SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

def scrapfly_request(link):
    
    scrapfly = ScrapflyClient(key=SCRAPFLY_API_KEY)
    result = scrapfly.scrape(ScrapeConfig(
        url = link,
        country  = "gb",
    ))
    
    return result.content

def get_apikey_fingerprint(link):
    content = scrapfly_request(link)
    soup = BeautifulSoup(content,features="lxml")
    
    data = str(soup)
    pattern = r'"key":"([^"]+)"'

    match = re.search(pattern, data)

    if match:
        key_value_pair = match.group(1)
    
    pattern = r'fingerprint":"([^"]+)"'

    match = re.search(pattern, data)

    if match:
        fingerprint_value = match.group(1)
    
    return fingerprint_value,key_value_pair

def get_searchId(fingerprint_value,key_value_pair,offset,query):
    url = f"https://appsapi.monster.io/jobs-svx-service/v2/monster/search-jobs/samsearch/en-GB?apikey={key_value_pair}"
    
    payload = '''{"jobQuery":{
"query":"'''+str(query)+'''","locations":[{"country":"gb","address":"","radius":{"unit":"mi","value":20}}],"activationRecency":"today"},"jobAdsRequest":{"position":[1,2,3,4,5,6,7,8,9],"placement":{"channel":"WEB","location":"JobSearchPage","property":"monster.co.uk","type":"JOB_SEARCH","view":"SPLIT"}},
 "fingerprintId":"'''+str(fingerprint_value)+'''","offset":"'''+str(offset)+'''","pageSize":9,"histogramQueries":["count(company_display_name)","count(employment_type)"],"includeJobs":[]}'''

    
    headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
      'content-type': 'application/json; charset=UTF-8'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return json.loads(response.text)

def scrap_job_data(fingerprint_value,key_value_pair,offset,searchId,location):
    url = f"https://appsapi.monster.io/jobs-svx-service/v2/monster/search-jobs/samsearch/en-GB?apikey={key_value_pair}"

    payload = '{\"jobQuery\":{\"query\":\"\",\"locations\":[{\"country\":\"gb\",\"address\":\"Bedfordshire\",\"radius\":{\"unit\":\"mi\",\"value\":5}}],\"activationRecency\":\"today\"},\"jobAdsRequest\":{\"position\":[1,2,3,4,5,6,7,8,9],\"placement\":{\"channel\":\"WEB\",\"location\":\"JobSearchPage\",\"property\":\"monster.co.uk\",\"type\":\"JOB_SEARCH\",\"view\":\"SPLIT\"}},\"fingerprintId\":\"z50446280c2b01c552e1556a3d58d1e28\",\"offset\":9,\"pageSize\":9,\"histogramQueries\":[\"count(company_display_name)\",\"count(employment_type)\"],\"searchId\":}'

    payload = re.sub(r'"fingerprintId":"[^"]+"', f'"fingerprintId":"{fingerprint_value}"', payload)
    payload = re.sub(r'"offset":\d+', f'"offset":{offset}', payload)
    payload = re.sub(r'"searchId":', f'"searchId":"{searchId}"', payload)
    payload = re.sub(r'"address":"([^"]+)"', f'"address":"{location}"', payload)
    headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
      'content-type': 'application/json; charset=UTF-8'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return json.loads(response.text)

def monster_scrap(monster_link):
    if not os.path.exists("output/monster/data_by_location"):
        os.makedirs("output/monster/data_by_location")
    if not os.path.exists("output/monster/full_data"):
        os.makedirs("output/monster/full_data")
    pages_data = []
    job_data = []
    offset = 0  
    fingerprint_value,key_value_pair = get_apikey_fingerprint(monster_link['links'])  
    for i in tqdm(range(0,1000)):
        # try:
            data = get_searchId(fingerprint_value,key_value_pair,offset,monster_link['locations'])
            if 'message' in data.keys():
                break
            if data['totalSize']==0:
                break
            offset+=9
            pages_data.append(data)
        # except:
        #     pass
    for jobs in pages_data:
        for job in jobs['jobResults']:
            try:
                title = job['jobPosting']['title']
                posted = job['jobPosting']['datePosted']
                location = job['jobPosting']['jobLocation'][0]['address']['addressLocality']+','+job['jobPosting']['jobLocation'][0]['address']['addressRegion']
                company = job['jobPosting']['hiringOrganization']['name']
                url = job['jobPosting']['url']
                job_data.append({'posted':posted,'job title':title,'company working':company,'location working':location,'link':url})  
            except:
                pass
            df = pd.DataFrame(job_data)
            df = df.drop_duplicates()

    df.to_csv(f'output/monster/data_by_location/monster_output_{monster_link["locations"]}_{date.today()}.csv',index=False)

def merge_data():
    folder_path = 'output/monster/data_by_location'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith(f'_{date.today()}.csv')]

    new_folder_path = 'output\\monster\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, f'monster_full_data_{date.today()}.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass

