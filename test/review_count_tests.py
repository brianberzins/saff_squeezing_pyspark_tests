from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from pandemic_recovery_batch import create_checkin_df_with_one_date_per_row, count_reviews, count_checkins, count_tips
from test.saff_squeeze_start_point import create_df_from_json, data_frame_to_json, save_results_to_expected


############################# SAFF SQUEEZE #################################
def test_keeps_mobile_reviews_without_checkins(spark: SparkSession) -> None:
    b_reviews_df = create_df_from_json("fixtures/browser_reviews.json", spark)
    m_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)
    date = datetime(2022, 4, 14)

    checkin_df = create_df_from_json("fixtures/checkin.json", spark)
    checkin_df = create_df_from_json("fixtures/checkins_exploded.json", spark)
    reviews_df = count_reviews(checkin_df, m_reviews_df, b_reviews_df, date)  # <- This is what we care about


    inglewood_pizza = data_frame_to_json(reviews_df)[6]
    assert inglewood_pizza["business_id"] == "mpf3x-BjTdTEA3yCZrAYPw"
    assert inglewood_pizza["num_reviews"] == 1
############################# SAFF SQUEEZE #################################



