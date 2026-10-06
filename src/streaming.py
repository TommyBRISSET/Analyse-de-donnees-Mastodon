import logging
import os

import psycopg
from pyspark.sql import SparkSession, functions as F, types as T
from src.common import csv_setting, keyword_pattern

SCHEMA = T.StructType([
    T.StructField(name, T.StringType()) for name in
    ('id', 'instance', 'timestamp', 'text', 'user_id', 'username', 'language', 'location')
] + [T.StructField('hashtags', T.ArrayType(T.StringType())),
     T.StructField('favourites_count', T.LongType()), T.StructField('reblogs_count', T.LongType())])


def transform(raw):
    df = raw.select(F.from_json(F.col('value').cast('string'), SCHEMA).alias('toot')).select('toot.*')
    df = (df.withColumn('created_at', F.to_timestamp('timestamp'))
          .withColumn('text', F.trim(F.regexp_replace('text', r'\s+', ' ')))
          .withColumn('hashtags', F.coalesce('hashtags', F.array().cast('array<string>')))
          .withColumn('language', F.lower('language'))
          .withColumn('text_length', F.length('text')).drop('timestamp'))
    # Un JSON invalide ou un champ obligatoire absent est écarté.
    df = df.filter(F.col('created_at').isNotNull() & (F.length('id') > 0)
                   & (F.length('instance') > 0) & (F.length('user_id') > 0) & (F.col('text_length') > 0))
    languages = csv_setting('LANGUAGES', 'fr,en')
    words = csv_setting('KEYWORDS', 'AI,DataScience')
    if languages:
        df = df.filter(F.col('language').isin(languages))
    if words:
        df = df.filter(F.concat_ws(' ', 'text', F.concat_ws(' ', 'hashtags')).rlike(keyword_pattern(words)))
    return df


def save_batch(batch, batch_id):
    rows = batch.dropDuplicates(['instance', 'id']).collect()
    if not rows:
        return
    with psycopg.connect() as conn:
        with conn.cursor() as cur:
            fresh = []
            for row in rows:
                cur.execute('''INSERT INTO toots
                    (instance,id,created_at,text,user_id,username,language,hashtags,location,
                     favourites_count,reblogs_count,text_length)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (instance,id) DO NOTHING RETURNING id''',
                    tuple(row[k] for k in ('instance','id','created_at','text','user_id','username',
                                          'language','hashtags','location','favourites_count','reblogs_count','text_length')))
                if cur.fetchone():
                    fresh.append(row)
            if not fresh:
                return
            df = batch.sparkSession.createDataFrame(fresh, batch.schema)
            # Fenêtres paramétrée à une minute.
            windows = df.groupBy(F.window('created_at', '1 minute')).count()
            for row in windows.collect():
                cur.execute('''INSERT INTO window_stats VALUES (%s,%s,%s)
                    ON CONFLICT (window_start) DO UPDATE SET
                    toot_count=window_stats.toot_count + EXCLUDED.toot_count''',
                    (row.window.start, row.window.end, row['count']))
            stats = df.groupBy('instance', 'user_id').agg(
                F.count('*').alias('n'), F.sum('text_length').alias('total'))
            for row in stats.collect():
                cur.execute('''INSERT INTO user_stats (instance,user_id,toot_count,total_length)
                    VALUES (%s,%s,%s,%s) ON CONFLICT (instance,user_id) DO UPDATE SET
                    toot_count=user_stats.toot_count + EXCLUDED.toot_count,
                    total_length=user_stats.total_length + EXCLUDED.total_length''',
                    (row.instance, row.user_id, row.n, row.total))
        # Le contexte commit tout ensemble, ou rollback en cas d'exception.
    logging.info('Micro-batch %s : %s nouveaux toots', batch_id, len(fresh))


def main():
    spark = (SparkSession.builder.appName('5SPAR-Mastodon')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    raw = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'))
           .option('subscribe', os.getenv('KAFKA_TOPIC', 'mastodon_stream'))
           .option('startingOffsets', 'earliest').option('maxOffsetsPerTrigger', 1000).load())
    query = (transform(raw).writeStream.outputMode('append').foreachBatch(save_batch)
             .option('checkpointLocation', '/checkpoints/mastodon')
             .trigger(processingTime='10 seconds').start())
    query.awaitTermination()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
