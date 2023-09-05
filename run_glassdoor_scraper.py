from glassdoor_scraper.utils import glassdoor_scrap,merge_data
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(filename='glassdoor_scraper/log_glassdoor.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"glassdoor scraper starts.")
    GLASSDOOR_LINKS = []
    details = []
    glassdoor_df = pd.read_excel('input.xlsx',sheet_name='glassdoor')
    glassdoor_df.insert(2, "details", 'unprocessed')

    for index in glassdoor_df.index:
        GLASSDOOR_LINKS.append({'links':glassdoor_df['links'][index],'locations':glassdoor_df['locations'][index]})

    logging.info(f"{len(GLASSDOOR_LINKS)} links to scrap.")
    for glassdoor_link in tqdm(GLASSDOOR_LINKS):
        try:
            glassdoor_scrap(glassdoor_link)
            glassdoor_df.loc[glassdoor_df['links'] == glassdoor_link['links'], 'details'] ='success'
            glassdoor_df.to_csv('output/glassdoor_output_details.csv',index=False)
            logging.info(f"{glassdoor_link['links']} :: scrapped successfully.")
        except Exception as e:
            glassdoor_df.loc[glassdoor_df['links'] == glassdoor_link['links'], 'details'] ='failed'
            glassdoor_df.to_csv('output/glassdoor_output_details.csv',index=False)
            logging.info(f"{glassdoor_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()
    
    
                
