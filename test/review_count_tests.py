from datetime import datetime

from chispa import assert_df_equality
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

import pandas as pd



#There's a review without a checkin at the same time
# This thing is counted as a review
DEFAULT_STRING = "default"
DEFAULT_NUM = -1

class ReviewDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.schema = StructType([
            StructField("review_id", StringType()),
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("stars", FloatType()),
            StructField("useful", IntegerType()),
            StructField("funny", IntegerType()),
            StructField("cool", IntegerType()),
            StructField("text", StringType()),
            StructField("date", StringType()),
        ])

    def of(self, review_id=DEFAULT_STRING,
           user_id=DEFAULT_STRING,
           business_id=DEFAULT_STRING,
           stars=DEFAULT_NUM,
           useful=DEFAULT_NUM,
           funny=DEFAULT_NUM,
           cool=DEFAULT_NUM,
           text=DEFAULT_STRING,
           date=DEFAULT_STRING):
        data = [(
            review_id,
            user_id,
            business_id,
            stars,
            useful,
            funny,
            cool,
            text,
            date,
        )]
        return self.spark.createDataFrame(self.schema, data)

def test_foo(spark):
    review_dataframe = ReviewDataFrame(spark)
    # A PERSON Makes a new review at A TIME at THE BUSINESS
    df = review_dataframe.of(user_id="Scooby-Doo", date="2000-01-02 03:04:05", business_id="Crusty Crab")
    # This(=df?) should count as one review at THE BUSINESS
    # assert count_reviews(business) == 1

    # JACQUELINE Makes a new review at NOON_FRIDAY at INGLEWOOD PIZZA
    # This should count as one review at INGLEWOOD PIZZA










############################# SAFF SQUEEZE #################################
def test_keeps_mobile_reviews_without_checkins(spark: SparkSession) -> None:
    def __(map):
        return as_dataframe(map, spark)
    business_id = "my_business_id"
    mobile_reviews = [{
        "user_id": "my_user_id_2",
        "business_id": business_id,
        "date": "2022-04-14 00:01:03"
    }]
    date = datetime(2022, 4, 14)

    reviews_df = count_interactions_from_reviews(__(empty()), __(mobile_reviews), __(empty()),
                                                 date)  # <- This is what we care about

    expected_reviews = [{
        "business_id": business_id,
        "num_reviews": 1
    }]
    assert_dataframes_are_equal(reviews_df, __(expected_reviews))

def empty():
    return [{
        "user_id": "",
        "business_id": "",
        "date": ""
    }]

def as_dataframe(map, spark) -> DataFrame:
    return spark.createDataFrame(pd.DataFrame(map))

def assert_dataframes_are_equal(actual, expected):
    assert_df_equality(actual, expected, ignore_nullable=True)

############################# SAFF SQUEEZE #################################
