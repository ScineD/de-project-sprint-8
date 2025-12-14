import os
import sys
import datetime
import logging

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import types as T

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("table_3")


class BannerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"""
                ================================
                \n====[TABLE_3]==== {msg} ====\n
                ================================
                """, kwargs


banner_log = BannerAdapter(log, {})


def log_step(step_name: str):
    banner_log.info(step_name)


def log_df(name, df):
    cnt = df.count()
    banner_log.info(f"{name} count={cnt}")
    return cnt


def input_paths(date_str, depth, base_path):
    dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    base = base_path.rstrip('/')
    return [f"{base}={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]


def main(spark: SparkSession, process_date: str, depth: int, base_path: str):

    log_step("Reading cities_dict")

    dict_geo_folder = '/user/scined/project/dicts/geo.csv'

    dict_schema = (
        T.StructType()
        .add("id", T.StringType())
        .add("city", T.StringType())
        .add("lat", T.StringType())
        .add("lng", T.StringType())
    )
    dict_geo = (
        spark.read.csv(dict_geo_folder, header=True, sep=";", schema=dict_schema)
        .select(
            F.col("id").alias("city_id"),
            F.col("city").alias("city_name"),
            F.regexp_replace("lat", ",", ".").cast("double").alias("city_lat"),
            F.regexp_replace("lng", ",", ".").cast("double").alias("city_lon"),
        )
        .where("city_lat IS NOT NULL AND city_lon IS NOT NULL")
        .cache()
    )
    log_df("dict_cities", dict_geo)
    dict_geo.createOrReplaceTempView("dict_cities")

    paths = input_paths(process_date, depth, base_path)
    banner_log.info(f"Folders to read: {paths}")

    log_step("Reading data")
    data = spark.read.parquet(*paths)
    data.createOrReplaceTempView("data")
    log_df("data", data)

    log_step("SQL_step: distinct_channels")
    distinct_channels = spark.sql("""
        WITH subs AS (
            SELECT DISTINCT
                event.user AS user_id,
                event.subscription_channel AS subscription_channel
            FROM data
            WHERE event_type = 'subscription' AND event.user IS NOT NULL
            )
        SELECT * FROM subs
        """)
    distinct_channels.createOrReplaceTempView("distinct_channels")
    log_df("distinct_channels", distinct_channels)

    log_step("SQL_step: paired_subs")
    paired_subs = spark.sql("""
        SELECT CAST(dc1.user_id AS integer) AS user_left,
           CAST(dc2.user_id AS integer) AS user_right,
           dc1.subscription_channel
        FROM distinct_channels dc1
        JOIN distinct_channels dc2
          ON dc1.subscription_channel = dc2.subscription_channel
        WHERE CAST(dc1.user_id AS integer) < CAST(dc2.user_id AS integer)
        """)
    paired_subs.createOrReplaceTempView("paired_subs")
    log_df("paired_subs", paired_subs)

    log_step("SQL_step: distinct_message_pairs")
    distinct_message_pairs = spark.sql("""
        SELECT DISTINCT
            LEAST(CAST(event.message_from AS INT), CAST(event.message_to AS INT)) AS user_left,
            GREATEST(CAST(event.message_from AS INT), CAST(event.message_to AS INT)) AS user_right
        FROM data
        WHERE event_type = 'message' AND event.message_from IS NOT NULL AND event.message_to IS NOT NULL
        """)
    distinct_message_pairs.createOrReplaceTempView("distinct_message_pairs")
    log_df("distinct_message_pairs", distinct_message_pairs)

    same_subs_no_messages = spark.sql("""
        SELECT s.user_left, s.user_right
        FROM paired_subs s
        LEFT ANTI JOIN distinct_message_pairs m
        ON (s.user_left = m.user_left AND s.user_right = m.user_right)
    """)
    same_subs_no_messages.createOrReplaceTempView("same_subs_no_messages")
    log_df("same_subs_no_messages", same_subs_no_messages)

    log_step("SQL_step: latest_actions")
    latest_actions = spark.sql("""
        WITH actions AS (
                SELECT event.reaction_from AS user_id,
                       to_timestamp(event.datetime) AS action_time,
                       lat, lon, event_type
                FROM data
                WHERE event_type = 'reaction'
                  AND event.reaction_from IS NOT NULL
                  AND event.datetime IS NOT NULL
                  AND lat IS NOT NULL
                  AND lon IS NOT NULL
                UNION ALL
                SELECT event.message_from AS user_id,
                       to_timestamp(event.datetime) AS action_time,
                       lat, lon, event_type
                FROM data
                WHERE event_type = 'message'
                  AND event.message_from IS NOT NULL
                  AND event.datetime IS NOT NULL
                  AND lat IS NOT NULL
                  AND lon IS NOT NULL
                UNION ALL
                SELECT event.user AS user_id,
                       to_timestamp(event.datetime) AS action_time,
                       lat, lon, event_type
                FROM data
                WHERE event_type = 'subscription'
                  AND event.user IS NOT NULL
                  AND event.datetime IS NOT NULL
                  AND lat IS NOT NULL
                  AND lon IS NOT NULL
            ),
            action_ranked AS (
                SELECT user_id,
                       action_time,
                       lat,
                       lon,
                       event_type,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY to_timestamp(action_time) DESC) AS action_rank
                FROM actions
            )
            SELECT user_id, action_time, lat, lon, event_type
            FROM action_ranked
            WHERE action_rank = 1
    """)
    latest_actions.createOrReplaceTempView("latest_actions")
    log_df("latest_actions", latest_actions)

    log_step("SQL_step: same_subs_no_messages_coord")
    same_subs_no_messages_coord = spark.sql("""
        SELECT s.user_left,
            s.user_right,
            l_left.lat AS user_left_lat,
            l_left.lon AS user_left_lon,
            l_right.lat AS user_right_lat,
            l_right.lon AS user_right_lot,
            2 * 6371 * ASIN(SQRT(
                           POW(SIN(RADIANS(l_left.lat - l_right.lat) / 2), 2) +
                           COS(RADIANS(l_right.lat)) * COS(RADIANS(l_left.lat)) *
                           POW(SIN(RADIANS(l_left.lon - l_right.lon) / 2), 2)
                       )) AS distance_km
        FROM same_subs_no_messages s
        LEFT JOIN latest_actions l_left
        ON s.user_left = l_left.user_id
        LEFT JOIN latest_actions l_right
        ON s.user_right = l_right.user_id
        WHERE l_left.user_id IS NOT NULL AND l_right.user_id IS NOT NULL
    """)
    same_subs_no_messages_coord.createOrReplaceTempView("same_subs_no_messages_coord")
    log_df("same_subs_no_messages_coord", same_subs_no_messages_coord)

    log_step("SQL_step: same_subs_no_messages_nearby")
    same_subs_no_messages_nearby = spark.sql("""
        SELECT user_left,
            user_right,
            user_left_lat,
            user_left_lon,
            user_right_lat,
            user_right_lot,
            distance_km
        FROM same_subs_no_messages_coord
        WHERE distance_km < 1
    """)
    same_subs_no_messages_nearby.createOrReplaceTempView("same_subs_no_messages_nearby")
    log_df("same_subs_no_messages_nearby", same_subs_no_messages_nearby)

    log_step("SQL_step: same_subs_no_messages_nearby_with_cities")
    same_subs_no_messages_nearby_with_cities = spark.sql("""
            WITH distances AS (
                SELECT s.user_left,
                        s.user_right,
                        s.user_left_lat,
                        s.user_left_lon,
                       c.city_id,
                       c.city_name,
                       c.city_lat,
                       c.city_lon,
                       2 * 6371 * ASIN(SQRT(
                           POW(SIN(RADIANS(c.city_lat - s.user_left_lat) / 2), 2) +
                           COS(RADIANS(s.user_left_lat)) * COS(RADIANS(c.city_lat)) *
                           POW(SIN(RADIANS(c.city_lon - s.user_left_lon) / 2), 2)
                       )) AS distance_km
                FROM same_subs_no_messages_nearby s
                CROSS JOIN dict_cities c
            ),
            ranked_distances AS (
                SELECT d.*,
                       RANK() OVER (PARTITION BY user_left ORDER BY distance_km ASC) AS distance_rank
                FROM distances d
            )
            SELECT *
            FROM ranked_distances
            WHERE distance_rank = 1
        """)
    same_subs_no_messages_nearby_with_cities.createOrReplaceTempView("same_subs_no_messages_nearby_with_cities")
    log_df("same_subs_no_messages_nearby_with_cities", same_subs_no_messages_nearby_with_cities)

    final_answer = spark.sql("""
            SELECT user_left,
                user_right,
                current_date() AS processed_dttm,
                city_id AS zone_id,
                from_utc_timestamp(current_timestamp(), 'Australia/Sydney') AS local_time
            FROM
            same_subs_no_messages_nearby_with_cities
        """)
    log_df("final_answer", final_answer)

    log_step("final_answer_result")
    final_answer.show(20, truncate=False)

    output_path = '/user/scined/project/analytics/table_3'
    log_step(f"Saving table_3 to {output_path}")
    final_answer.write.mode('overwrite').parquet(output_path)
    banner_log.info(f"Successfully saved table_3 to {output_path}")


if __name__ == "__main__":
    # args: <process_date> <depth_days> <base_path>
    process_date_arg = sys.argv[1] if len(sys.argv) > 1 else '2022-05-21'
    depth_arg = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    base_path_arg = sys.argv[3] if len(sys.argv) > 3 else '/user/master/data/geo/events/date'

    spark = SparkSession.builder.appName(f'Project_table_3-{process_date_arg}').getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass

    main(spark, process_date_arg, depth_arg, base_path_arg)
    
    spark.stop()
