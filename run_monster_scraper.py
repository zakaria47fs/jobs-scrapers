from utils.utils_monster import monster_scrap,merge_data
import pandas as pd
from tqdm import tqdm
import logging

logging.basicConfig(filename='logs/log_monster.log',
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

if __name__=='__main__':
    logging.info(f"monster scraper starts.")
    MONSTER_LINKS = []
    details = []
    monster_df = pd.read_excel('input.xlsx',sheet_name='monster')
    monster_df.insert(2, "details", 'unprocessed')

    for index in monster_df.index:
        MONSTER_LINKS.append({'links':monster_df['links'][index],'locations':monster_df['locations'][index]})

    logging.info(f"{len(MONSTER_LINKS)} links to scrap.")
    for monster_link in tqdm(MONSTER_LINKS):
        try:
            monster_scrap(monster_link)
            monster_df.loc[monster_df['links'] == monster_link['links'], 'details'] ='success'
            monster_df.to_csv('output/monster/monster_output_details.csv',index=False)
            logging.info(f"{monster_link['links']} :: scrapped successfully.")
        except Exception as e:
            monster_df.loc[monster_df['links'] == monster_link['links'], 'details'] ='failed'
            monster_df.to_csv('output/monster/monster_output_details.csv',index=False)
            logging.info(f"{monster_link['links']} :: scrape failed.")
            logging.info(f"details :: {str(e)}")
            print('scraper failed')
    merge_data()
    