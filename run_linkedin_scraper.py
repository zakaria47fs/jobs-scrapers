from linkedin_scraper.utils import linkedin_scrap
import pandas as pd


if __name__=='__main__':

    LINKEDIN_LINKS = []
    linkedin_df = pd.read_excel('input.xlsx',sheet_name=1)

    for index in linkedin_df.index:
        LINKEDIN_LINKS.append({'links':linkedin_df['links'][index],'locations':linkedin_df['locations'][index]})
        
    for linkedin_link in LINKEDIN_LINKS:
        linkedin_scrap(linkedin_link)
