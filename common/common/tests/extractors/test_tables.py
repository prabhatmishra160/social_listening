import pandas as pd
from src import tables as tb
import pytest

def test_get_credentials():
    credentials = None
    assert not credentials
    credentials = tb.get_credentials()
    assert credentials

@pytest.mark.parametrize("source", [("reddit")])
def test_read_keywords(source):
    keyword_df = pd.DataFrame()
    assert not len(keyword_df)
    keyword_df = tb.load_query_keywords(source)
    assert len(keyword_df)