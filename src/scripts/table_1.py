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
log = logging.getLogger("table_1")


class BannerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"""
                ================================
                \n====[TABLE_1]==== {msg} ====\n
                ================================
                """, kwargs


banner_log = BannerAdapter(log, {})


def log_step(step_name: str):
    banner_log.info(step_name)


def log_df(name: str, df) -> int:
    cnt = df.count()
    banner_log.info(f"{name} count={cnt}")
    return cnt


def input_paths(date_str: str, depth: int, base_path: str):
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

    log_step("Reading messages data")
    messages = (
        spark.read.parquet(*paths)
        .select(
            F.col("event.message_id").alias("message_id"),
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_ts").alias("message_ts"),
            F.col("lat").alias("message_lat"),
            F.col("lon").alias("message_lon"),
            F.col("event_type").alias("event_type")
        )
        .where("""message_lat IS NOT NULL
               AND message_lon IS NOT NULL
               AND message_id IS NOT NULL
               AND user_id IS NOT NULL
               AND message_ts IS NOT NULL
               AND event_type = 'message' """)
    )
    messages.createOrReplaceTempView("messages_raw")
    log_df("messages_raw", messages)
    log_df("messages_raw", messages)

    log_step("SQL_step: distances with closest city")
    distances = spark.sql("""
        WITH distances AS (
            SELECT m.message_id,
                   m.user_id,
                   m.message_ts,
                   m.message_lat,
                   m.message_lon,
                   c.city_id,
                   c.city_name,
                   c.city_lat,
                   c.city_lon,
                   2 * 6371 * ASIN(SQRT(
                       POW(SIN(RADIANS(c.city_lat - m.message_lat) / 2), 2) +
                       COS(RADIANS(m.message_lat)) * COS(RADIANS(c.city_lat)) *
                       POW(SIN(RADIANS(c.city_lon - m.message_lon) / 2), 2)
                   )) AS distance_km
            FROM messages_raw m
            CROSS JOIN dict_cities c
        ),
        ranked_distances AS (
            SELECT message_id,
                   user_id,
                   message_ts,
                   distance_km,
                   city_id,
                   city_name,
                   RANK() OVER (PARTITION BY message_id ORDER BY distance_km ASC) AS distance_rank
            FROM distances
        )
        SELECT message_id,
               user_id,
               message_ts,
               distance_km,
               city_id,
               city_name
        FROM ranked_distances
        WHERE distance_rank = 1
    """)
    distances.createOrReplaceTempView("closest_cities")
    log_df("closest_cities", distances)

    log_step("SQL_step: act_cities (most recent city per user)")
    act_cities = spark.sql("""
        WITH cities_ranked_by_ts AS (
            SELECT user_id,
                city_name AS act_city,
                message_ts,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY message_ts DESC) AS ranking_cities_by_ts
            FROM closest_cities)
        SELECT user_id,
            act_city,
            from_utc_timestamp(message_ts, 'Australia/Sydney') AS local_time
        FROM cities_ranked_by_ts
        WHERE ranking_cities_by_ts = 1
    """)
    act_cities.createOrReplaceTempView('act_cities')
    log_df("act_cities", act_cities)

    log_step("SQL_step: home_cities (users with exactly one city)")
    home_cities = spark.sql("""
        WITH per_city AS (
            SELECT user_id, city_name, COUNT(*) AS messages_count
            FROM closest_cities
            GROUP BY user_id, city_name
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY messages_count DESC) AS rn,
                   COUNT(*) OVER (PARTITION BY user_id) AS city_cnt
            FROM per_city
        )
        SELECT user_id, city_name AS home_city
        FROM ranked
        WHERE rn = 1 AND city_cnt = 1
    """)
    home_cities.createOrReplaceTempView('home_cities')
    log_df("home_cities", home_cities)

    log_step("SQL_step: user_cities")
    user_cities = spark.sql("""
        SELECT ac.user_id, ac.act_city, hc.home_city, ac.local_time
        FROM act_cities AS ac
        LEFT JOIN home_cities hc
        ON ac.user_id = hc.user_id
    """)
    user_cities.createOrReplaceTempView('user_cities')
    log_df("user_cities", user_cities)

    log_step("SQL_step: travel_stats")
    travel_stats = spark.sql("""
        WITH user_city_visits AS (
            SELECT
                user_id,
                city_name,
                message_ts,
                LAG(city_name) OVER (PARTITION BY user_id ORDER BY message_ts) AS prev_city
            FROM closest_cities
        ),
        travel_events AS (
            SELECT
                user_id,
                city_name,
                message_ts
            FROM user_city_visits
            WHERE prev_city IS NULL OR city_name != prev_city
        ),
        travel_aggregated AS (
            SELECT
                user_id,
                COUNT(*) AS travel_count,
                COLLECT_LIST(city_name) AS travel_array
            FROM travel_events
            GROUP BY user_id
        )
        SELECT * FROM travel_aggregated
    """)
    travel_stats.createOrReplaceTempView('travel_stats')
    log_df("travel_stats", travel_stats)

    log_step("SQL_step: final table_1 result")
    table_1 = spark.sql("""
        SELECT uc.user_id, uc.act_city, uc.home_city, ts.travel_count, ts.travel_array, uc.local_time
        FROM user_cities AS uc
        LEFT JOIN travel_stats ts
        ON uc.user_id = ts.user_id
    """)
    log_df("table_1", table_1)

    log_step("final_table_1_result")
    table_1.show(40, truncate=False)

    output_path = '/user/scined/project/analytics/table_1'
    log_step(f"Saving table_1 to {output_path}")
    table_1.write.mode('overwrite').parquet(output_path)
    banner_log.info(f"Successfully saved table_1 to {output_path}")


if __name__ == "__main__":
    # args: <process_date> <depth_days> <base_path>
    process_date_arg = sys.argv[1] if len(sys.argv) > 1 else '2022-06-21'
    depth_arg = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    base_path_arg = sys.argv[3] if len(sys.argv) > 3 else '/user/master/data/geo/events/date'

    spark = SparkSession.builder.appName(f'Project_table_1-{process_date_arg}').getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass

    main(spark, process_date_arg, depth_arg, base_path_arg)

    spark.stop()
