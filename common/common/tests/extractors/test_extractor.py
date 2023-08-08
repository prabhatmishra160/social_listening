import pandas as pd
from src import tables as tb
import pytest
from extractor import Extractor

def test_filter_english_posts():
    obj = Extractor()

    data = pd.DataFrame()
    data['id'] = ['1', '2']
    data['body'] = ["this is test data", "ye test data hai"]
    data['source'] = "reddit"

    obj.scraped_posts = data

    assert len(obj.scraped_posts) == 2

    obj.filter_english_posts(source='reddit')

    assert len(obj.scraped_posts) == 1
