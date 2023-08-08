import pandas as pd
from .extractor import Extractor
import instaloader
from dateutil import parser
from datetime import datetime
import logging

class InstagramExtractor(Extractor):
    def __init__(self, args, keywords):
        self.L = instaloader.Instaloader()
        self.output_table_name = args.output_table
        self.since = args.since
        self.until = args.until
        self.scraped_posts = pd.DataFrame()
        self.keywords = keywords


    def run(self):
        """
        function is responsible to loop over all relevant keywords and extract posts. eventually it creates the
        final dataframe containing all posts
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
        function is responsible for extracting relevant data from instagram.
        Args:
            :param1 query: keyword that is needed to be searched
        Returns:
            final scraped data after each search
        """

        topics_dict = {
            "comms_num": [],
            "likes": [],
            "reply_to": [],
            "body": [],
            "shortcode": [],
            "created": [],
            "hashtag": [],
            "url": [],
            "is_video": [],
            "caption_hashtags": [],
        }
        i=0
        query = query.replace('#', '')
        try:
            hashtags = instaloader.Hashtag.from_name(self.L.context, query).get_posts()
            for post in hashtags:
                if i==100:
                    break
                topics_dict["comms_num"].append(post.comments)
                topics_dict["likes"].append(post.likes)
                topics_dict["reply_to"].append(None)
                topics_dict["body"].append(post.caption)
                topics_dict["shortcode"].append(post.shortcode)
                topics_dict["created"].append(str(post.date))
                topics_dict["hashtag"].append(query)
                posturl = "https://www.instagram.com/p/"+post.shortcode
                topics_dict["url"].append(posturl)
                topics_dict["is_video"].append(post.is_video)
                topics_dict["caption_hashtags"].append(post.caption_hashtags)
                i += 1
        except Exception as e:
            logging.info("Exception came : ",e)

        post_df = pd.DataFrame(topics_dict)
        return post_df

    def clean_data(self):
        """
        function is responsible for cleaning the scraped data from instagram. Cleaning involves filtering out english
        posts only, dropping out any posts with null body
        """
        if len(self.scraped_posts) != 0:
            self.scraped_posts = self.scraped_posts.drop_duplicates(subset=['shortcode'], keep="first")
            # convert string date in datetime format
            self.scraped_posts['created'] = self.scraped_posts['created'].apply(lambda x: parser.parse(x))
            # filter posts which are in english language
            self.filter_english_posts(source='instagram')