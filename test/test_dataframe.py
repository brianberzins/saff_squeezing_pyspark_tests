import functools

# Balazs idea
from datetime import datetime

import pytest

from pandemic_recovery_batch import count_interactions_from_reviews


class TestDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.data = [{}]
        self.schema = None

    def __enter__(self):
        return self.create_df()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def with_schema_from(self, reference_df):
        self.schema = reference_df.schema
        return self

    def create_df(self):
        return self.spark.createDataFrame(schema=self.schema, data=self.data)

    def with_data(self, rows):
        self.data = rows
        return self


class EmptyDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.schema = []

    def __enter__(self):
        return self.spark.createDataFrame(schema=self.schema, data=[{}])

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def with_schema_from(self, reference_df):
        self.schema = reference_df.schema
        return self

def test_context(spark):
    mobile_review_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    with TestDataFrame(spark).with_schema_from(mobile_review_df) as __:
        reviews_df = count_interactions_from_reviews(__, mobile_review_df, __, datetime(2022, 4, 14))
        assert reviews_df.count() == 1

def test_composition(spark):
    mobile_review_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    __ = TestDataFrame(spark).with_schema_from(mobile_review_df).create_df()
    reviews_df = count_interactions_from_reviews(__, mobile_review_df, __, datetime(2022, 4, 14))
    assert reviews_df.count() == 1