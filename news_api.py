# import libraries
import pandas as pd
import requests
import time
from tqdm import tqdm
import psycopg2
from newsapi import NewsApiClient
from datetime import datetime, timedelta
from config import api_key, host, username, password

# continue indefinitely
while True:

    # instantiate newsapi object
    newsapi = NewsApiClient(api_key=api_key)

    # instantiate dataframe
    df = pd.DataFrame()

    # instantitate list with tickers
    search_terms = ['TSLA', 'Tesla', 'Elon Musk']

    # loop through all tickers
    for search_term in search_terms:

        # get articles from first 5 pages - News API limit for Free Tier
        for page in tqdm(range(1, 6)):

            # request information
            all_articles = newsapi.get_everything(q=search_term, # search_term
                                                  from_param=str(datetime.today()-timedelta(1))[:10], # yesterday
                                                  to=str(datetime.today())[:10], # today
                                                  language='en', # english
                                                  page=page) # for 5 pages - Limit

            # create dataframe with information
            i_df = pd.DataFrame(all_articles['articles'])

            # add search_term column
            i_df['search_term'] = search_term

            # concatenate dataframes for all pages
            df = pd.concat([df, i_df], axis=0)

    # reset dataframe's index
    df = df.reset_index(drop=True)

    # format source column to string
    df['source'] = df['source'].map(lambda x: x['name'])

    # connect to database
    conn = psycopg2.connect(host=host,
                            user=username,
                            password=password,
                            dbname='news')

    # loop through all rows of the dataframe
    for i in tqdm(df.index):

        # connect cursor
        with conn.cursor() as cur:

            # SQL query
            query = f'''

             INSERT INTO news_api
             (source, author, title, description, url, urltoimage, published_at, content, search_term)
             VALUES
             ($${df['source'][i]}$$, $${df['author'][i]}$$,
             $${df['title'][i]}$$, $${df['description'][i]}$$,
             $${df['url'][i]}$$, $${df['urlToImage'][i]}$$,
             $${df['publishedAt'][i]}$$, $${df['content'][i]}$$,
             $${df['search_term'][i]}$$)

            '''

            # execute query
            cur.execute(query)

            # commit to modify database
            conn.commit()

    # return info to user - Rows Written to Database
    print(f'{df.shape[0]} Rows Written to the Database on {datetime.now()}!')

    # wait 24h to run again
    time.sleep(60*60*24)
