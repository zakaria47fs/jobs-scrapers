from google_scraper.utils import google_jobs_scrape, merge_data
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(filename='google_scraper/log_google.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)
if __name__=='__main__':
    logging.info(f"google scraper starts.")

    PHRASES = []
    google_df = pd.read_excel('input.xlsx',sheet_name='google')
    google_df.insert(1, "details", 'unprocessed')

    for index in google_df.index:
        PHRASES.append(google_df['phrases'][index])
    logging.info(f"{len(PHRASES)} phrases to scrap.")   
    for phrase in tqdm(PHRASES):
        try:
            google_jobs_scrape(phrase)
            google_df.loc[google_df['phrases'] == google_df['phrases'], 'details'] = 'success'
            google_df.to_csv('output/google_output_details.csv',index=False)
            logging.info(f"{phrase} :: scrapped successfully.")
        except Exception as e:
            google_df.loc[google_df['phrases'] == google_df['phrases'], 'details']='failed'
            google_df.to_csv('output/google_output_details.csv',index=False)
            logging.info(f"{phrase} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()