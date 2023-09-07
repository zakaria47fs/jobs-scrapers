from utils.utils_cvlibrary import cvlibrary_scrap,merge_data
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(filename='logs/log_cvlibrary.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"cvlibrary scraper starts.")
    CVLIBRARY_LINKS = []
    details = []
    cvlibrary_df = pd.read_excel('input.xlsx',sheet_name='cvlibrary')
    cvlibrary_df.insert(2, "details", 'unprocessed')

    for index in cvlibrary_df.index:
        CVLIBRARY_LINKS.append({'links':cvlibrary_df['links'][index],'locations':cvlibrary_df['locations'][index]})

    logging.info(f"{len(CVLIBRARY_LINKS)} links to scrap.")
    for cvlibrary_link in tqdm(CVLIBRARY_LINKS):
        try:
            cvlibrary_scrap(cvlibrary_link)
            cvlibrary_df.loc[cvlibrary_df['links'] == cvlibrary_link['links'], 'details'] ='success'
            cvlibrary_df.to_csv('output/cvlibrary/cvlibrary_output_details.csv',index=False)
            logging.info(f"{cvlibrary_link['links']} :: scrapped successfully.")
        except Exception as e:
            cvlibrary_df.loc[cvlibrary_df['links'] == cvlibrary_link['links'], 'details'] ='failed'
            cvlibrary_df.to_csv('output/cvlibrary/cvlibrary_output_details.csv',index=False)
            logging.info(f"{cvlibrary_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()
    
    
                
