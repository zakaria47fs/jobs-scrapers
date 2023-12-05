from utils.utils_adzuna import adzuna_scrap,merge_data
import pandas as pd
from tqdm import tqdm
import logging
import shutil
import os
logging.basicConfig(filename='logs/log_adzuna.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"adzuna scraper starts.")
    ADZUNA_LINKS = []
    details = []
    shutil.rmtree("output/adzuna/data_by_location",ignore_errors=True)
    shutil.rmtree("output/adzuna/full_data",ignore_errors=True)
    os.makedirs("output/adzuna/data_by_location")
    os.makedirs("output/adzuna/full_data")
    adzuna_df = pd.read_excel('input.xlsx',sheet_name='adzuna')
    adzuna_df.insert(2, "details", 'unprocessed')

    for index in adzuna_df.index:
        ADZUNA_LINKS.append({'links':adzuna_df['links'][index],'locations':adzuna_df['locations'][index]})

    logging.info(f"{len(ADZUNA_LINKS)} links to scrap.")
    for adzuna_link in tqdm(ADZUNA_LINKS):
        try:
            adzuna_scrap(adzuna_link)
            adzuna_df.loc[adzuna_df['links'] == adzuna_link['links'], 'details'] ='success'
            adzuna_df.to_csv('output/adzuna/adzuna_output_details.csv',index=False)
            logging.info(f"{adzuna_link['links']} :: scrapped successfully.")
        except Exception as e:
            adzuna_df.loc[adzuna_df['links'] == adzuna_link['links'], 'details'] ='failed'
            adzuna_df.to_csv('output/adzuna/adzuna_output_details.csv',index=False)
            logging.info(f"{adzuna_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()
    
    
                
