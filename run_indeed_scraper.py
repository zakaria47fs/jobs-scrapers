from indeed_scraper.utils import indeed_scrap,merge_data
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(filename='indeed_scraper/log_indeed.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"indeed scraper starts.")
    INDEED_LINKS = []
    details = []
    indeed_df = pd.read_excel('input.xlsx',sheet_name='indeed')
    indeed_df.insert(2, "details", 'unprocessed')

    for index in indeed_df.index:
        INDEED_LINKS.append({'links':indeed_df['links'][index],'locations':indeed_df['locations'][index]})

    logging.info(f"{len(INDEED_LINKS)} links to scrap.")
    for indeed_link in tqdm(INDEED_LINKS):
        try:
            indeed_scrap(indeed_link)
            indeed_df.loc[indeed_df['links'] == indeed_link['links'], 'details']='success'
            indeed_df.to_csv('output/indeed_output_details.csv',index=False)
            logging.info(f"{indeed_link['links']} :: scrapped successfully.")
        except Exception as e:
            indeed_df.loc[indeed_df['links'] == indeed_link['links'], 'details']='failed'
            indeed_df.to_csv('output/indeed_output_details.csv',index=False)
            logging.info(f"{indeed_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()
    
    
                
