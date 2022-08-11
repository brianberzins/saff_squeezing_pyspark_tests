from datetime import datetime


from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

from pandemic_recovery_batch import count_interactions_from_reviews


def create_df(spark, schema, data):
    return spark.createDataFrame(schema=schema, data=data)


SCHEMA = StructType([
            StructField('user_id', StringType()),
            StructField('business_id', StringType()),
            StructField('date', StringType()),
            StructField('useful', IntegerType())
])

SCHEMA2 = StructType([
            StructField('business_id', StringType()),
            StructField('num_reviews', LongType(), False)])

def test_count_reviews_schema(spark):
    required_schema = StructType([
        StructField('user_id', StringType()),
        StructField('business_id', StringType()),
        StructField('date', StringType())
    ])
    required_schema_df = spark.createDataFrame(schema=required_schema, data=[])

    reviews_df = count_interactions_from_reviews(required_schema_df, required_schema_df, required_schema_df, datetime(2022, 4, 14))

    expected_output_schema = StructType([
        StructField('business_id', StringType()),
        StructField('num_reviews', LongType(), False)])
    expected_df = spark.createDataFrame(schema=expected_output_schema, data=[])
    assert_df_equality(reviews_df, expected_df)

# allTrue = all(somePredicate(elem) for elem in someIterable)
# anyTrue = any(somePredicate(elem) for elem in someIterable)

def df_has_rows(df, rows):
    return all(any(row.items() <= df_row.asDict().items() for df_row in df.collect()) for row in rows)

def test_keeps_mobile_reviews_without_checkins(spark):
    mobile_review_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    __ = spark.createDataFrame(schema=mobile_review_df.schema, data=[])

    reviews_df = count_interactions_from_reviews(__, mobile_review_df, __, datetime(2022, 4, 14))

    assert reviews_df.count() == 1
    assert df_has_rows(reviews_df, [{'business_id': 'bid', 'num_reviews': 1}])


def test_does_not_count_mobile_reviews_with_checkins(spark):
    mobile_review_df = spark.createDataFrame(data=[{ 'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    checkin_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    __ = spark.createDataFrame(schema=mobile_review_df.schema, data=[])

    reviews_df = count_interactions_from_reviews(checkin_df, mobile_review_df, __, datetime(2022, 4, 14))

    assert reviews_df.count() == 0
