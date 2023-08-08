from transformers import pipeline
import urllib.request
import logging
import argparse
import csv
from brightfield_common.extractors import tables as tb

def get_model():
    """
    prepares the sentiment model and return it
    Returns:
        returns the sentiment model
    """
    model = "cardiffnlp/twitter-roberta-base-sentiment"
    sentiment_model = pipeline(model=model, tokenizer=model, max_length=300, truncation=True)
    return sentiment_model

def preprocess(text):
    """
    function is responsible for preprocessing text containing emails and http so that vocabulary size can be reduced
    Args:
        :param1 text: text needed to be preprocessed
    Returns:
        preprocessed text
    """
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)


def predict_data(data, sentiment_model):
    """
    function is responsible for predicting the sentiment for each row in data. it first combines title and body together
    and then feed the combined data inside model to get the sentiment.
    Args:
        :param1 data: dataframe containing data needed to predict sentiments
        :param2 sentiment_model: it is hugging face model which will be used to predict sentiments
    Returns:
        output of the model
    """
    raw_group = data
    results = list()
    raw_group['mention_title'][raw_group['mention_title'].isna()] = ""
    raw_group['mention_body'][raw_group['mention_body'].isna()] = ""
    raw_group['concatenated_body_title'] = raw_group['mention_title'] +" "+raw_group['mention_body']
    raw_group['concatenated_body_title'] = raw_group['concatenated_body_title'].apply(preprocess)
    temp_data = list(raw_group['concatenated_body_title'].values)
    for text in temp_data:
        try:
            result = sentiment_model(text)
        except Exception as e:
            logging.info("Exception ", e, "occurred")
            result = ['error']
        results.extend(result)
    return results


def get_label_dict():
    """
    function is responsible for preparing dictionary which maps model encoded output label to either positive, negative
    or neutral
    Returns:
        return the dictionary containing encoded output label mapped to either positive, negative
        or neutral
    """
    # mapping can be found in below link
    mapping_link = f"https://raw.githubusercontent.com/cardiffnlp/tweeteval/main/datasets/sentiment/mapping.txt"
    with urllib.request.urlopen(mapping_link) as f:
        html = f.read().decode('utf-8').split("\n")
        csvreader = csv.reader(html, delimiter='\t')
    labels = [row[1] for row in csvreader if len(row) > 1]

    raw_labels = ["LABEL_"+str(i) for i in range(len(labels))]
    label_dict = dict(zip(raw_labels, labels))
    return label_dict

def extract_prediction_result(results, label_dict):
    """
    function is responsible for preparing 2 separate lists of sentiment labels and sentiment scores from the output
    produces by hugging face model
    Args:
        :param1 results: output of hugging face model
        :param2 label_dict: label_dict is a dictionary which maps model encoded output label to either positive,
        negative or neutral
    Returns:
        returns 2 separate lists: sentiment labels and sentiment scores
    """
    sentiments = []
    scores = []
    for result in results:
        if result != 'error':
            sentiments.append(label_dict[result['label']])
            scores.append(result['score'])
        else:
            sentiments.append('error')
            scores.append(0.0)
    return (sentiments,scores)

def read_data_from_big_query(sql):
    """
    function is responsible for reading data from big query table.
    Args:
       :param1 sql: select sql statement for reading data from big query table
    Returns:
        it returns the dataframe containing the data
    """
    data = tb.read_data_from_big_query(sql)
    return data

def save_data_to_big_query(data, output_table):
    """
    function is responsible fot saving the final dataframe into bigquery table
    Args:
        :param1 data: dataframe containing data needed to be saved in bigquery
        :param2 output_table: big query table in which data will e pushed
    """
    tb.save_data_to_big_query(data, output_table)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="SQL query needed to pull relevant data",
                        type=str)
    parser.add_argument("--output_table", help="Output table where reddit data will be stored",
                        required=True, type=str)

    parser.set_defaults(feature=False)

    args = parser.parse_args()
    output_table = args.output_table

    sql = args.query

    data = read_data_from_big_query(sql)

    # get sentiment model
    sentiment_model = get_model()
    # predict sentiment
    results = predict_data(data, sentiment_model)
    # model result three labels LABEL_0, LABEL_1, LABEL_2. LABEL_0=negative LABEL_1=neutral LABEL_1=positive
    # below code maps these model predicted labels to either positive, neutral or negative
    label_dict = get_label_dict()

    sentiments, sentiment_scores = extract_prediction_result(results, label_dict)
    # create new column sentiment to store related sentiment labels
    data['sentiments'] = sentiments
    # create new column sentiment_scores to store related sentiment labels
    data['sentiment_scores'] = sentiment_scores
    save_data_to_big_query(data, output_table)