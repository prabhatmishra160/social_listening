from src import extractor
import pytest
import pandas as pd

@pytest.fixture(name='api_mocker')
def fixture_api_mocker(mocker):
    """
    this fixture mocks the API calls to reddit and instagram
    """
    mocker.patch(
        'praw.Reddit',
        return_value='reddit_object'
    )

    mocker.patch(
        'instaloader.Instaloader',
        return_value='instagram_object'
    )

@pytest.fixture(name='twitter_data')
def fixture_twitter_data():
    """
    this fixture returns twitter test data
    """
    data = pd.DataFrame()
    data['id'] = ['1', '2']
    data['tweet'] = ["this is test 420 data", "ye test data CBD hai"]
    data['source'] = "twitter"
    data['date'] = '2022-02-12'
    return data

@pytest.fixture(name='reddit_data')
def fixture_reddit_data():
    """
    this fixture returns reddit test data
    """
    data = pd.DataFrame()
    data['id'] = ['1', '2']
    data['title'] = ["title 420", "title CBD"]
    data['body'] = ["this is test data", "ye test data hai"]
    data['source'] = "reddit"
    return data

@pytest.fixture(name='instagram_data')
def fixture_instagram_data():
    """
    this fixture returns instagram test data
    """
    data = pd.DataFrame()
    data['shortcode'] = ['1', '2']
    data['body'] = ["this is test 420 data", "ye test data CBD hai"]
    data['source'] = "instagram"
    data['created'] = '2022-02-12'
    return data


@pytest.fixture(name='args')
def fixture_args():
    """
    this fixture returns command line arguments
    """
    class Args:
        def __init__(self):
            self.user = 'user'
            self.client_id = 'client_id'
            self.client_secret = 'client_secret'
            self.output_table = 'output_table'
            self.source = 'source'
            self.time_filter = 'month'
            self.since = '2022-1-1'
            self.until = '2022-1-10'
            self.limit = 100

    args = Args()
    return args




def test_create_extractor_object(api_mocker, args):
    """
    function tests if the correct extractor object is returned by the function depending on source
    Args:
        :param1 api_mocker: mock the actual API calls to reddit, instagram
        :param2 args: args contain all the command line parameters
    """
    args.source = 'reddit'

    keywords = pd.DataFrame(["420", "CBD"])

    extractor_obj = extractor.create_extractor_object(source=args.source, args=args, keywords=keywords)
    assert str(type(extractor_obj)) == "<class 'brightfield_common.extractors.reddit_extractor.RedditExtractor'>"

    args.source = 'twitter'
    extractor_obj = extractor.create_extractor_object(source=args.source, args=args, keywords=keywords)
    assert str(type(extractor_obj)) == "<class 'brightfield_common.extractors.twitter_extractor.TwitterExtractor'>"

    args.source = 'instagram'
    extractor_obj = extractor.create_extractor_object(source=args.source, args=args, keywords=keywords)
    assert str(type(extractor_obj)) == "<class 'brightfield_common.extractors.instagram_extractor.InstagramExtractor'>"


def test_run_extractor(mocker, api_mocker, args, twitter_data, reddit_data, instagram_data):
    """
    function tests if the scrapper runs as expected.
    Args:
        :param api_mocker: mock the actual API calls to reddit, instagram
        :param args: args contain all the command line parameters
        :param twitter_data: test data for twitter api call
        :param reddit_data: test data for reddit api call
        :param instagram_data: test data for instagram api call
    """
    def scrape_data(query='query'):
        if str(type(extractor_obj)) == "<class 'brightfield_common.extractors.reddit_extractor.RedditExtractor'>":
            return reddit_data

        if str(type(extractor_obj)) == "<class 'brightfield_common.extractors.twitter_extractor.TwitterExtractor'>":
            return twitter_data

        if str(type(extractor_obj)) == "<class 'brightfield_common.extractors.instagram_extractor.InstagramExtractor'>":
            return instagram_data

    # create test keywords data
    keywords = pd.DataFrame()
    keywords['keywords'] = ["420", "CBD"]

    # test reedit extractor
    args.source = 'reddit'
    extractor_obj = extractor.create_extractor_object(source=args.source, args=args, keywords=keywords)
    mocker.patch.object(extractor_obj, 'scrape_posts', scrape_data)

    extractor_obj.run()
    assert len(extractor_obj.scraped_posts) != 0
    assert extractor_obj.scraped_posts['source'][0] == 'reddit'

    # test twitter extractor
    args.source = 'twitter'
    extractor_obj = extractor.create_extractor_object(source=args.source, args=args, keywords=keywords)
    mocker.patch.object(extractor_obj, 'scrape_posts', scrape_data)

    extractor_obj.run()
    assert len(extractor_obj.scraped_posts) != 0
    assert extractor_obj.scraped_posts['source'][0] == 'twitter'

    # test instagram extractor
    args.source = 'instagram'
    extractor_obj = extractor.create_extractor_object(source=args.source, args=args, keywords=keywords)
    mocker.patch.object(extractor_obj, 'scrape_posts', scrape_data)

    extractor_obj.run()
    assert len(extractor_obj.scraped_posts) != 0
    assert extractor_obj.scraped_posts['source'][0] == 'instagram'
