from linkedin_scraper.utils import linkedin_scrap,merge_data
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(filename='linkedin_scraper/log_linkedin.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"linkedin scraper starts.")
    LINKEDIN_LINKS = []
    linkedin_df = pd.read_excel('input.xlsx',sheet_name='linkedin')
    linkedin_df.insert(2, "details", 'unprocessed')

    for index in linkedin_df.index:
        LINKEDIN_LINKS.append({'links':linkedin_df['links'][index],'locations':linkedin_df['locations'][index]})

    logging.info(f"{len(LINKEDIN_LINKS)} links to scrap.")

    for linkedin_link in tqdm(LINKEDIN_LINKS):
        try:
            linkedin_scrap(linkedin_link)
            linkedin_df.loc[linkedin_df['links'] == linkedin_link['links'], 'details'] ='success'
            linkedin_df.to_csv('output/linkedin_output_details.csv',index=False)
            logging.info(f"{linkedin_link['links']} :: scrapped successfully.")
        except Exception as e:
            linkedin_df.loc[linkedin_df['links'] == linkedin_link['links'], 'details'] ='failed'
            linkedin_df.to_csv('output/linkedin_output_details.csv',index=False)
            logging.info(f"{linkedin_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()