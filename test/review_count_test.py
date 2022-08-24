from datetime import datetime
from chispa import assert_df_equality

from test.test_dataframe import TestDataFrame
from pandemic_recovery_batch import count_interactions_from_reviews, create_checkin_df_with_one_date_per_row


def test_multiple_row_df_creation(spark):
    input_df = spark.createDataFrame(
        [
            {'user_id': 'uid', 'date': '2000-01-02 03:04:05, 2000-01-01 04:05:06', 'business_id': 'bid'}
        ]
    )
    df_actual = create_checkin_df_with_one_date_per_row(input_df)
    df_expected = spark.createDataFrame(
        [
            {'user_id': 'uid', 'date': '2000-01-02 03:04:05', 'business_id': 'bid'},
            {'user_id': 'uid', 'date': '2000-01-01 04:05:06', 'business_id': 'bid'}
        ]
    )
    assert_df_equality(df_expected, df_actual, ignore_nullable=True, ignore_column_order=True)


def test_keeps_mobile_reviews_without_checkins(spark):
    input_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14 00:01:03'}])
    __ = TestDataFrame(spark).with_schema_from(input_df).create_df()

    actual_df = count_interactions_from_reviews(__, input_df, __, datetime(2022, 4, 14))

    expected_df = spark.createDataFrame([{'business_id': 'bid', 'num_reviews': 1}])
    assert_df_equality(expected_df, actual_df, ignore_nullable=True, ignore_column_order=True)


def test_foo(spark):
    assert True
    # review_dataframe = ReviewDataFrame(spark)
    # # A PERSON Makes a new review at A TIME at THE BUSINESS
    # business = "Crusty Crab"
    # date = "2000-01-02 03:04:05"
    # df = review_dataframe.of(user_id="Scooby-Doo", date=date, business_id=business)
    # # This(=df?) should count as one review at THE BUSINESS
    # assert count_reviews(df=df, business_id=business, spark=spark, date=date) == 1
    #
    # # JACQUELINE Makes a new review at NOON_FRIDAY at INGLEWOOD PIZZA
    # # This should count as one review at INGLEWOOD PIZZA
