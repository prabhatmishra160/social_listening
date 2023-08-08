import twint
from .extractor import Extractor
import time
import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta
import logging

class TwitterExtractor(Extractor):
    def __init__(self, args, keywords):
        self.t = twint.Config()
        self.t.Lang = "en"
        self.t.Pandas = True
        self.output_table_name = args.output_table
        self.since = args.since
        self.until = args.until
        self.scraped_posts = pd.DataFrame()
        self.keywords = keywords

    def run(self):
        """
        function is responsible to loop over all relevant keywords and extract posts. eventually it creates the
        final dataframe containing all extracted posts.
        """
        final_data = pd.DataFrame()
        keyword_df = self.keywords
        logging.info("Keywords/hashtags loaded successfully")
        for query in keyword_df.values:
            posts = self.scrape_posts(query=query)
            final_data = final_data.append(posts)
            logging.info("shape of the data is retrieved for query{}:".format(query), posts.shape)
        if len(final_data) != 0:
            final_data['processing_date'] = datetime.now()
        self.scraped_posts = final_data
        self.clean_data()

    def scrape_posts(self, query):
        """
        function is responsible for extracting relevant data from twitter.
        Args:
            :param1 query: keyword that is needed to be searched
        Returns:
            final scraped data after each search
        """
        data = pd.DataFrame()
        try:
            if '#' not in query:
                # we need to add \\ in keywords for the exact match of keywords in tweets.
                query = '\\"'+query+'\\"'
            self.t.limit = 50000
            self.t.Since = self.since
            self.t.Until = self.until
            self.t.Search = query
            twint.run.Search(self.t)
            data = twint.storage.panda.Tweets_df
        except Exception as e:
            logging.info("Exception Came: ", e)
            time.sleep(5)
        return data

    def clean_data(self):
        """
        function is responsible for cleaning the scraped data from twitter. Cleaning involves filtering out english
        posts only, dropping out any tweet with null body
        """
        if len(self.scraped_posts) != 0:
            self.scraped_posts = self.scraped_posts.drop_duplicates(subset=['id'], keep="first")
            # convert string date in datetime format
            self.scraped_posts['date'] = self.scraped_posts['date'].apply(lambda x: parser.parse(x))
            # filter posts which are in english language
            self.filter_english_posts(source='twitter')
            # filter only those posts which contain needed keywords
            self.filter_relevant_posts(self.keywords, 'body')
