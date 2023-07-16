from google_jobs_scraper.utils import google_jobs_scrape
import pandas as pd

if __name__=='__main__':

    PHRASES = []
    google_df = pd.read_excel('input.xlsx',sheet_name=2)

    for index in google_df.index:
        PHRASES.append(google_df['phrases'][index])
        
    for phrase in PHRASES[:1]:
        google_jobs_scrape(phrase)

    