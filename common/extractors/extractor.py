import spacy
from spacy_langdetect import LanguageDetector
from spacy.matcher import Matcher
from spacy.language import Language
import abc
import tables as tb

def create_lang_detector(nlp, name):
    return LanguageDetector()

Language.factory("language_detector", func=create_lang_detector)
nlp = spacy.load('en_core_web_sm', disable=['ner'])
nlp.add_pipe('language_detector')
elglish_threshhold = .90


def create_matcher_pattern(keywords):
    """
    function is responsible for creating s list of patterns which we want to match in reddit posts title
    Args:
        :param1 keywords_sheet_path: path of keyword's file needed to create matcher patterns
    Returns:
        list of pattern dictionary
    """
    patterns = [[{'LOWER': str(key).lower()}] for key in keywords.values]
    return patterns

def find_relevant_posts(data, matcher, target_column):
    """
    function is responsible for matching created patterns(keywords) in reddit post's title by enumerating each post
    one by one
    Args:
        :param1 data: dataframe containing reddit posts
        :param2 matcher: list of patterns needed to mach in reddit post's title
    Returns:
        a list containing either Yes or No. Yes if any pattern is found in the title else No
    """
    match_list = list()
    for i, doc in enumerate(nlp.pipe(data[target_column], batch_size=1000)):
        matches = matcher(doc)
        print(len(matches), end=" ")
        if len(matches) > 0:
            match_list.append('Yes')
        else:
            match_list.append('No')
    return match_list

def english_score(row):
    """
    function is responsible for filtering out only posts which are in english
    Args:
        :param1 row: single row from whole extracted data
    Returns:
        A bool value. True if the data is in english. False otherwise
    """
    doc = nlp(row)
    if (doc._.language['language']=='en' and doc._.language['score'] >= elglish_threshhold):
        return True
    else:
        return False

class Extractor:

    @abc.abstractmethod
    def scrape_post(self, query):
        pass
    def save_data_to_big_query(self):
        """
        function is responsible for saving the data into bigquery table
        """
        if len(self.scraped_posts) != 0:
            tb.save_data_to_big_query(self.scraped_posts, self.output_table_name)

    def filter_english_posts(self, source):
        """
        function is responsible for filtering out english posts only
        Args:
            :param1 source: either reddit, twitter or instagram
        """
        if source == 'twitter':
            self.scraped_posts.rename(columns = {'tweet':'body'}, inplace = True)
        self.scraped_posts = self.scraped_posts[~self.scraped_posts['body'].isnull()]
        self.scraped_posts['is_english'] = self.scraped_posts['body'].apply(english_score)
        self.scraped_posts = self.scraped_posts[self.scraped_posts['is_english']==True]

    def filter_relevant_posts(self, keywords, target_column):
        """
        function is responsible for matching created patterns(keywords) in reddit post's title by enumerating each post
        one by one
        Args:
            :param1 keywords_sheet_path: path of keyword's file needed to create matcher patterns
        """
        matcher = Matcher(nlp.vocab)
        patterns = create_matcher_pattern(keywords)
        matcher.add("RELEVANT_KEYWORDS", patterns)
        match_list = find_relevant_posts(self.scraped_posts, matcher, target_column)
        self.scraped_posts['is_valid'] = match_list
