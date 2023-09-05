from simplyhired_scraper.utils import simplyhired_scrap,merge_data
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(filename='simplyhired_scraper/log_simplyhired.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"simplyhired scraper starts.")
    SIMPLYHIRED_LINKS = []
    details = []
    simplyhired_df = pd.read_excel('input.xlsx',sheet_name='simplyhired')
    simplyhired_df.insert(2, "details", 'unprocessed')

    for index in simplyhired_df.index:
        SIMPLYHIRED_LINKS.append({'links':simplyhired_df['links'][index],'locations':simplyhired_df['locations'][index]})

    logging.info(f"{len(SIMPLYHIRED_LINKS)} links to scrap.")
    for simplyhired_link in tqdm(SIMPLYHIRED_LINKS):
        try:
            simplyhired_scrap(simplyhired_link)
            simplyhired_df.loc[simplyhired_df['links'] == simplyhired_link['links'], 'details'] ='success'
            simplyhired_df.to_csv('output/simplyhired_output_details.csv',index=False)
            logging.info(f"{simplyhired_link['links']} :: scrapped successfully.")
        except Exception as e:
            simplyhired_df.loc[simplyhired_df['links'] == simplyhired_link['links'], 'details']='failed'
            simplyhired_df.to_csv('output/simplyhired_output_details.csv',index=False)
            logging.info(f"{simplyhired_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()
    
    
                
