import os
import argparse
import logging
from brightfield_common.extractors import reddit_extractor, twitter_extractor, instagram_extractor
from brightfield_common.extractors import tables as tb
from dotenv import load_dotenv

load_dotenv()
KEYWORDS_SHEET_PATH = os.getenv('KEYWORDS_SHEET_PATH')

def create_extractor_object(source, args, keywords):
    """
    Function is responsible for creating the extractor object depending upon the source that we have provided
    Args:
        :param1 source: it could be either instagram/ twitter/ reddit
        :param2 args: other command line parameter required for the object to be created
    Returns:
        object of the extractor class
    """
    if source == 'reddit':
        obj = reddit_extractor.RedditExtractor(args=args, keywords=keywords)
    elif source == 'twitter':
        obj = twitter_extractor.TwitterExtractor(args=args, keywords=keywords)
    else:
        obj = instagram_extractor.InstagramExtractor(args=args, keywords=keywords)
    return obj

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="Output table where reddit data will be stored",
                        required=True, type=str)
    parser.add_argument("--output_table", help="Output table where reddit data will be stored",
                        required=True, type=str)
    parser.add_argument("--since", help="start date since when data needed to be scraped",
                        type=str, default=None)
    parser.add_argument("--until", help="end date until when data needed to be extracted",
                        type=str, default=None)
    parser.add_argument("--time_filter", help="end date until when data needed to be extracted",
                        type=str, default='all')
    parser.set_defaults(feature=False)

    args = parser.parse_args()
    source = args.source

    # load keywords needed to search
    keywords = tb.load_query_keywords(source=source, keywords_sheet_path=KEYWORDS_SHEET_PATH)
    # create extractor object
    extractor = create_extractor_object(source=source, args=args, keywords=keywords)
    # run extractor
    extractor.run()
    logging.info("shape of the final ", source, " data is :", extractor.scraped_posts.shape)
    extractor.save_data_to_big_query()


