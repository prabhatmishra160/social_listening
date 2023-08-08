from src import sentiment_analyzer
import pandas as pd

def test_predict_data():
    """
    function tests if the hugging face sentiment model predicts expected results
    """
    data = pd.DataFrame()
    data['mention_title'] = ['CBD cause stress', "CBD is making me happy"]
    data['mention_body'] = ['CBD is very bad for health', 'CBD is awesome']

    sentiment_model = sentiment_analyzer.get_model()
    results = sentiment_analyzer.predict_data(data, sentiment_model)
    label_dict = sentiment_analyzer.get_label_dict()
    sentiments, sentiment_scores = sentiment_analyzer.extract_prediction_result(results, label_dict)

    assert sentiments[0] == 'negative'
    assert sentiments[1] == 'positive'
