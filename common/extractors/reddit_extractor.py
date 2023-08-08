import pandas as pd
import praw
from .extractor import Extractor
import datetime as dt
import logging
from ..google_utils.secrets_utils import access_secret_version

PROJECT_ID = 'brightfield-dev'
CLIENT_ID = access_secret_version(PROJECT_ID, 'reddit_client_id')
CLIENT_SECRET = access_secret_version(PROJECT_ID, 'reddit_client_secret')

class RedditExtractor(Extractor):
    def __init__(self, args, keywords):
        user = args.usename
        client_id = CLIENT_ID
        client_secret = CLIENT_SECRET
        user_agent = 'Comment Extraction (by /u/{})'.format(user)
        self.time_filter = args.time_filter
        self.limit = args.limit
        self.output_table_name = args.output_table
        self.reddit = praw.Reddit(user_agent=user_agent, client_id=client_id, client_secret=client_secret)
        self.scraped_posts = None
        self.keywords = keywords

    def run(self):
        """
        function is responsible to loop over all relevant keywords and extract posts. eventually it creates the
        final dataframe containing all posts
        """
        final_data = pd.DataFrame()
        print("going to read keywords/hashtags")
        keyword_df = self.keywords
        logging.info("Keywords/hashtags loaded successfully")
        for query in keyword_df.values:
            posts = self.scrape_posts(query=query)
            final_data = final_data.append(posts)
            print("shape of the data is retrieved for query{}:".format(query), posts.shape)
        if len(final_data) != 0:
            final_data['processing_date'] = dt.datetime.now()
        self.scraped_posts = final_data
        self.clean_data()

    def scrape_posts(self, query):
        """
        function is responsible for extracting relevant data from reddit.
        Args:
            :param1 query: keyword that is needed to be searched
        Returns:
            final scraped data after each search
        """
        topics_dict = {
            "title": [],
            "score": [],
            "id": [],
            "url": [],
            "comms_num": [],
            "created": [],
            "body": [],
            "subreddit": [],
            "reply_to": [],
            "query": [],
            "author": [],
            "type": [],
            "upvote_ratio": [],
        }
        print(query)
        all_reddits = self.reddit.subreddit("all")
        submissions = all_reddits.search(query, limit=self.limit, time_filter=self.time_filter)
        # time_filter – Can be one of: all, day, hour, month, week, year(default: all).
        for submission in submissions:
            topics_dict["title"].append(submission.title)
            topics_dict["score"].append(submission.score)
            topics_dict["id"].append(submission.id)
            topics_dict["url"].append(submission.url)
            topics_dict["comms_num"].append(submission.num_comments)
            topics_dict["created"].append(dt.datetime.fromtimestamp(submission.created))
            topics_dict["body"].append(submission.selftext)
            topics_dict["reply_to"].append(None)
            topics_dict["subreddit"].append(submission.subreddit.display_name)
            topics_dict["query"].append(query)
            topics_dict["type"].append("post")
            topics_dict["author"].append(submission.author)
            topics_dict["upvote_ratio"].append(submission.upvote_ratio)
            # I have commented out below line because scraping comment was taking very long. We can uncomment it
            # later on
            #self.comment_scrapper(submission)

        return pd.DataFrame(topics_dict)

    def comment_scrapper(self, submission):
        """
        function is responsible for scrapping reddit comments
        Args:
            param1 submission: submission of which comments needed to be extracted
        """
        topics_dict = {
            "title": [],
            "score": [],
            "id": [],
            "url": [],
            "comms_num": [],
            "created": [],
            "body": [],
            "subreddit": [],
            "reply_to": [],
            "query": [],
            "author": [],
            "type": [],
            "upvote_ratio": [],
        }

        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            cbody = comment.body
            if any(keyword in cbody for keyword in self.keywords):
                topics_dict["title"].append(submission.title)
                topics_dict['score'].append(comment.score)
                topics_dict['id'].append(comment.id)
                topics_dict["url"].append(None)
                topics_dict["comms_num"].append(len(comment.replies))
                topics_dict["created"].append(dt.datetime.fromtimestamp(comment.created))
                topics_dict['body'].append(comment.body)
                topics_dict['reply_to'].append(submission.id)
                topics_dict["subreddit"].append(submission.subreddit.display_name)
                topics_dict["query"].append(None)
                topics_dict["type"].append("comment")
                topics_dict["author"].append(comment.author)
                topics_dict["upvote_ratio"].append(None)

        post_df = pd.DataFrame(topics_dict)
        self.scraped_posts = self.scraped_posts.append(post_df)

    def clean_data(self):
        """
        function is responsible for cleaning the scraped data from reddit. Cleaning involves filtering out english
        posts only, dropping out any posts with null body and filtering out only those posts which have any of the
        specified keywords in the title.
        """
        if len(self.scraped_posts) != 0:
            self.scraped_posts = self.scraped_posts.drop_duplicates(subset=['id'], keep="first")
            # filter posts which are in english language
            self.filter_english_posts(source='reddit')
            # filter only those posts which contain needed keywords
            self.filter_relevant_posts(self.keywords, 'title')










