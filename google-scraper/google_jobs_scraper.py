from utils import google_jobs_scrape
from constants import SEARCH_QUERIES

if __name__=='__main__':
    for search_query in SEARCH_QUERIES:
        jobs_data = google_jobs_scrape(search_query)
