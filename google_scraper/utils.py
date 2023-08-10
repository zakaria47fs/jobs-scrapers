from serpapi import GoogleSearch
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("API_KEY")

def google_jobs_scrape(search_query):
    if not os.path.exists("output/google"):
        os.makedirs("output/google")
    jobs_data = []
    for page_num in range(100):
        params = {
        "engine": "google_jobs",
        "q": search_query,
        "hl": "en",
        "location":"united kingdom",
        "google_domain":"google.co.uk",
        "gl":"uk",
        "start" : 10*page_num,
        "api_key": api_key
        }
        search = GoogleSearch(params)
        results = search.get_dict()
        if 'error' in results.keys():
            break
        jobs_results = results["jobs_results"]
    
        
        for job in jobs_results:
            try:
                job_title = job['title']
                job_id = job['job_id']
                company = job['company_name']
                location = job['location']
                date_posted = job['detected_extensions']['posted_at']
                job_link = jobs_link_by_id(job_id)
                job_data = {'job title':job_title,'company working':company,'location working':location,'link':job_link,'date_posted':date_posted}
                jobs_data.append(job_data)
            except:
                pass
    
    df = pd.DataFrame(jobs_data)
    df.to_csv(f'output/google/google_output_{search_query}.csv')

def jobs_link_by_id(job_id):
    params = {
      "engine": "google_jobs_listing",
      "q": job_id,
    "api_key": api_key
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    apply_options = results["apply_options"]
    
    return apply_options[0]['link']

    
    
    
