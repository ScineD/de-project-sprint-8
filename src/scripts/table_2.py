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
log = logging.getLogger("table_2")


class BannerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"""
                ================================
                \n====[TABLE_2]==== {msg} ====\n
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

    log_step("Reading events data")
    events = (
        spark.read.parquet(*paths)
        .select(
            F.col("event.message_id").alias("message_id"),
            F.col("event.message_from").alias("message_from"),
            F.col("event.message_ts").alias("message_ts"),
            F.col("event.datetime").alias("datetime"),
            F.col("event.user").alias("user"),
            F.col("lat").alias("event_lat"),
            F.col("lon").alias("event_lon"),
            F.col("event_type").alias("event_type")
        )
        .where("event_lat IS NOT NULL AND event_lon IS NOT NULL")
    )
    events.createOrReplaceTempView("events_raw")
    log_df("events_raw", events)

    log_step("SQL_step: distances with closest city")
    distances = spark.sql("""
        WITH distances AS (
            SELECT e.message_id,
                   e.message_from,
                   e.message_ts,
                   e.datetime,
                   e.user,
                   e.event_lat,
                   e.event_lon,
                   e.event_type,
                   c.city_id,
                   c.city_name,
                   c.city_lat,
                   c.city_lon,
                   2 * 6371 * ASIN(SQRT(
                       POW(SIN(RADIANS(c.city_lat - e.event_lat) / 2), 2) +
                       COS(RADIANS(e.event_lat)) * COS(RADIANS(c.city_lat)) *
                       POW(SIN(RADIANS(c.city_lon - e.event_lon) / 2), 2)
                   )) AS distance_km
            FROM events_raw e
            CROSS JOIN dict_cities c
        ),
        ranked_distances AS (
            SELECT message_id,
                    message_from,
                    message_ts,
                    datetime,
                   user,
                   distance_km,
                   event_type,
                   city_id,
                   city_name,
                   RANK() OVER (PARTITION BY message_id, event_type ORDER BY distance_km ASC) AS distance_rank
            FROM distances
            WHERE distance_km IS NOT NULL
        )
        SELECT message_id,
               message_from,
               message_ts,
               datetime,
               user,
               distance_km,
               event_type,
               city_id,
               city_name
        FROM ranked_distances
        WHERE distance_rank = 1
    """)
    distances.createOrReplaceTempView("closest_cities_events")
    log_df("closest_cities_events", distances)

    log_step("SQL_step: month_metrics")
    month_metrics = spark.sql("""
        SELECT month(to_timestamp(COALESCE(message_ts, datetime))) AS month,
               city_id AS zone_id,
               SUM(CASE WHEN event_type = 'message' THEN 1 ELSE 0 END) AS month_message,
               SUM(CASE WHEN event_type = 'reaction' THEN 1 ELSE 0 END) AS month_reaction,
               SUM(CASE WHEN event_type = 'subscription' THEN 1 ELSE 0 END) AS month_subscription,
               SUM(CASE WHEN event_type = 'registration' THEN 1 ELSE 0 END) AS month_user
        FROM closest_cities_events
        GROUP BY month(to_timestamp(COALESCE(message_ts, datetime))), city_id
    """)
    month_metrics.createOrReplaceTempView('month_metrics')
    log_df("month_metrics", month_metrics)

    log_step("SQL_step: week_metrics")
    week_metrics = spark.sql("""
        SELECT month(to_timestamp(COALESCE(message_ts, datetime))) AS month,
               weekofyear(to_timestamp(COALESCE(message_ts, datetime))) AS week,
               city_id AS zone_id,
               SUM(CASE WHEN event_type = 'message' THEN 1 ELSE 0 END) AS week_message,
               SUM(CASE WHEN event_type = 'reaction' THEN 1 ELSE 0 END) AS week_reaction,
               SUM(CASE WHEN event_type = 'subscription' THEN 1 ELSE 0 END) AS week_subscription,
               SUM(CASE WHEN event_type = 'registration' THEN 1 ELSE 0 END) AS week_user
        FROM closest_cities_events
        GROUP BY month(to_timestamp(COALESCE(message_ts, datetime))),
                 weekofyear(to_timestamp(COALESCE(message_ts, datetime))),
                 city_id
    """)
    week_metrics.createOrReplaceTempView('week_metrics')
    log_df("week_metrics", week_metrics)

    log_step("SQL_step: combined_metrics")
    combined_metrics = spark.sql("""
        SELECT
            m.month,
            w.week,
            m.zone_id,
            w.week_message,
            w.week_reaction,
            w.week_subscription,
            w.week_user,
            m.month_message,
            m.month_reaction,
            m.month_subscription,
            m.month_user
        FROM month_metrics m
        LEFT JOIN week_metrics w
            ON m.month = w.month AND m.zone_id = w.zone_id
    """)
    log_df("combined_metrics", combined_metrics)

    log_step("final_combined_metrics_result")
    combined_metrics.show(40, truncate=False)

    output_path = '/user/scined/project/analytics/table_2'
    log_step(f"Saving table_2 to {output_path}")
    combined_metrics.write.mode('overwrite').parquet(output_path)
    banner_log.info(f"Successfully saved table_2 to {output_path}")


if __name__ == "__main__":
    # args: <process_date> <depth_days> <base_path>
    process_date_arg = sys.argv[1] if len(sys.argv) > 1 else '2022-05-15'
    depth_arg = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    base_path_arg = sys.argv[3] if len(sys.argv) > 3 else '/user/master/data/geo/events/date'

    spark = SparkSession.builder.appName(f'Project_table_2-{process_date_arg}').getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass

    main(spark, process_date_arg, depth_arg, base_path_arg)
    
    spark.stop()
