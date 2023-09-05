from serpapi import GoogleSearch
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("API_KEY")

def google_jobs_scrape(search_query):
    if not os.path.exists("output/google/data_by_search_query"):
        os.makedirs("output/google/data_by_search_query")

    if not os.path.exists("output/google/full_data"):
        os.makedirs("output/google/full_data")

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
                job_data = {'date':date_posted,'job title':job_title,'company working':company,'location working':location,'link':job_link}
                jobs_data.append(job_data)
            except:
                pass
    
    df = pd.DataFrame(jobs_data)
    df.to_csv(f'output/google/data_by_search_query/google_output_{search_query}.csv',index=False)

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

def merge_data():
    folder_path = 'output/google/data_by_search_query'

    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    new_folder_path = 'output\\google\\full_data'

    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
    dataframes = []

    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes.append(df)

            merged_data = pd.concat(dataframes, ignore_index=True)
            merged_file_path = os.path.join(new_folder_path, 'google_full_data.csv')
            merged_data.to_csv(merged_file_path, index=False)
        except:
            pass



    
    
    
