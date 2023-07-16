from indeed_scraper.utils import indeed_scrap
import pandas as pd


if __name__=='__main__':

    INDEED_LINKS = []
    indeed_df = pd.read_excel('input.xlsx',sheet_name=0)

    for index in indeed_df.index:
        INDEED_LINKS.append({'links':indeed_df['links'][index],'locations':indeed_df['locations'][index]})
        
    for indeed_link in INDEED_LINKS[:1]:
        indeed_scrap(indeed_link)
