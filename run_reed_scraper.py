from reed_scraper.utils import reed_scrap,merge_data
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(filename='reed_scraper/log_reed.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"reed scraper starts.")
    REED_LINKS = []
    details = []
    reed_df = pd.read_excel('input.xlsx',sheet_name='reed')
    reed_df.insert(2, "details", 'unprocessed')

    for index in reed_df.index:
        REED_LINKS.append({'links':reed_df['links'][index],'locations':reed_df['locations'][index]})

    logging.info(f"{len(REED_LINKS)} links to scrap.")
    for reed_link in tqdm(REED_LINKS):
        try:
            reed_scrap(reed_link)
            reed_df.loc[reed_df['links'] == reed_link['links'], 'details'] ='success'
            reed_df.to_csv('output/reed_output_details.csv',index=False)
            logging.info(f"{reed_link['links']} :: scrapped successfully.")
        except Exception as e:
            reed_df.loc[reed_df['links'] == reed_link['links'], 'details'] ='failed'
            reed_df.to_csv('output/reed_output_details.csv',index=False)
            logging.info(f"{reed_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()
    
    
                
