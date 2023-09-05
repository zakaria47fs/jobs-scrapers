from totaljobs_scraper.utils import totaljobs_scrap,merge_data
import pandas as pd
import tqdm
import logging

logging.basicConfig(filename='totaljobs_scraper/log_totaljobs.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"totaljobs scraper starts.")
    TOTALJOBS_LINKS = []
    details = []
    totaljobs_df = pd.read_excel('input.xlsx',sheet_name='totaljobs')
    totaljobs_df.insert(2, "details", 'unprocessed')

    for index in totaljobs_df.index:
        TOTALJOBS_LINKS.append({'links':totaljobs_df['links'][index],'locations':totaljobs_df['locations'][index]})

    logging.info(f"{len(TOTALJOBS_LINKS)} links to scrap.")
    for totaljobs_link in tqdm.tqdm(TOTALJOBS_LINKS):
        try:
            totaljobs_scrap(totaljobs_link)
            totaljobs_df.loc[totaljobs_df['links'] == totaljobs_link['links'], 'details'] = 'success'
            totaljobs_df.to_csv('output/totaljobs_output_details.csv',index=False)
            logging.info(f"{totaljobs_link['links']} :: scrapped successfully.")
        except Exception as e:
            totaljobs_df.loc[totaljobs_df['links'] == totaljobs_link['links'], 'details'] ='failed'
            totaljobs_df.to_csv('output/totaljobs_output_details.csv',index=False)
            logging.info(f"{totaljobs_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()
    
    
                
