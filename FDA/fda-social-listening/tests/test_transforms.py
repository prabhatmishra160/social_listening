import pytest

from src.configuration import *
from src.transforms import main
from unittest.mock import patch


@pytest.mark.unit
@patch("etl.ETL.build_fact_table_from_definitions")
@patch("etl.encoders.get_bridge_table_encoders")
@patch("etl.ETL.to_gbq")
@patch("etl.builders.WebAppDimensionBuilder.fetch_dimensions")
def test_tranforms_unit(fetch_dimensions, to_gbq, get_bridge_table_encoders,
                        build_fact_table_from_definitions):
    q = f"""
        SELECT * , source_id as id, created_datetime as whoosh_index_date, mention_body as text
        FROM `{settings.PROJECT}.fda_social_raw.social_data_raw`
        WHERE DATE(created_datetime) = "2022-05-07"
        LIMIT 10000
    """
    main(settings.PROJECT, 'fda_social_reporting', q)
    assert True, "successful social sentiments"


@pytest.mark.integration
def test_tranforms():
    q = f"""SELECT * , source_id as id, created_datetime as whoosh_index_date, mention_body as text
                FROM `{settings.PROJECT}.fda_social_raw.social_data_processed`
                WHERE DATE(created_datetime) = "2022-05-07"
                LIMIT 10000
        """
    main(settings.PROJECT, 'fda_social_reporting', q)
    assert True, "successful social sentiments"
