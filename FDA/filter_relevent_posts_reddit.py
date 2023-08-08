import spacy
from spacy.matcher import Matcher
import argparse
from brightfield_common.extractors import tables as tb
nlp = spacy.load("en_core_web_sm", disable=['parser', 'ner'])

def create_matcher_pattern():
    """
    function is responsible for creating s list of patterns which we want to match in reddit posts title
    :return: list of pattern dictionaryexit
    """
    source = 'reddit'
    keyword_df = tb.load_query_keywords(source, keywords_sheet_path = "../data/keywords.csv")
    patterns = [[{'LOWER': key.lower()}] for key in keyword_df.values]
    return patterns

def find_relevelt_posts(data, matcher):
    """
    function is responsible for matching created patterns(keywords) in reddit post's title by enumerating each post
    one by one
    :param data: dataframe containing reddit posts
    :param matcher: list of patterns needed to mach in reddit post's title
    :return: a list containing either Yes or No. Yes if any pattern is found in the title else No
    """
    match_list = list()
    for i, doc in enumerate(nlp.pipe(data['mention_title']+" "+data['channel'].str.lower(), batch_size=1000)):
        matches = matcher(doc)
        print(len(matches), end=" ")
        if len(matches) > 0:
            match_list.append('Yes')
        else:
            match_list.append('No')

    return match_list

def save_data_to_big_query(data, output_table):
    """
    function is responsible fot saving the final dataframe into bigquery table
    :param data: dataframe containing data needed to be saved in bigquery
    :param output_table: big query table in which data will e pushed
    """
    tb.save_data_to_big_query(data, output_table)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--output_table", help="Output table where reddit data will be stored",
                        required=True, type=str)

    parser.set_defaults(feature=False)
    args = parser.parse_args()

    output_table = args.output_table
    matcher = Matcher(nlp.vocab)
    patterns = create_matcher_pattern()
    matcher.add("RELEVANT_KEYWORDS", patterns)

    sql = """SELECT * FROM `brightfield-dev.fda_social_raw.social_data_raw` where data_source='reddit'"""
    data = tb.read_data_from_big_query(sql)
    #data = data.drop(['is_valid'], axis=1)
    match_list = find_relevelt_posts(data, matcher)
    data['is_valid'] = match_list
    save_data_to_big_query(data, output_table)

