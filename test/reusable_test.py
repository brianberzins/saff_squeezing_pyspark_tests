from datetime import datetime

import approvaltests
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from app.pandemic_recovery_batch import count_reviews


def create_df(spark, schema, data):
    return spark.createDataFrame(schema=schema, data=data)

def verify_df(df):
    # use count() to ensure we display every row
    # truncate as 0 means do not truncate at all
    # false to not show data vertically
    approvaltests.verify(df._jdf.showString(df.count(), 0, False))


SCHEMA = StructType([
            StructField('user_id', StringType()),
            StructField('business_id', StringType()),
            StructField('date', StringType())])

def test_keeps_mobile_reviews_without_checkins(spark):
    mobile_df = create_df(spark, SCHEMA, [{'user_id': 'uid', 'business_id': 'bid', 'date': '2022-04-14'}])
    __ = create_df(spark, SCHEMA, [])

    reviews_df = count_reviews(__, mobile_df, __, datetime(2022, 4, 14))

    verify_df(reviews_df)

def test_does_not_count_mobile_reviews_with_checkins(spark):
    mobile_df = create_df(spark, SCHEMA, [{ 'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    checkin_df = create_df(spark, SCHEMA, [{'user_id': 'uid', 'business_id': 'bid', 'date': '2022-04-14'}])
    __ = create_df(spark, SCHEMA, [])

    reviews_df = count_reviews(checkin_df, mobile_df, __, datetime(2022, 4, 14))

    verify_df(reviews_df)
