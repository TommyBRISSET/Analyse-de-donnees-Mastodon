"""Tests réels Spark/PostgreSQL, dans un schéma SQL isolé puis supprimé."""
import os
import unittest
from unittest.mock import patch
import json
import psycopg
from pyspark.sql import SparkSession
from src.common import clean_html, keyword_pattern, normalize
from src.streaming import transform, save_batch


class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('tests').config(
            'spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_html_and_keywords(self):
        self.assertEqual(clean_html('<p>AI &amp; Spark</p><p>test<br>ok</p>'), 'AI & Spark test ok')
        self.assertNotRegex('travail', keyword_pattern(['AI']))
        self.assertRegex('#AI!', keyword_pattern(['AI']))
        self.assertIsNone(normalize({'reblog': True}, 'demo'))

    def test_filter_aggregate_and_replay(self):
        def event(i, text='AI test', language='fr', timestamp='2026-09-24T10:00:10Z'):
            return json.dumps(dict(id=i, instance='test', timestamp=timestamp, text=text,
                                   user_id='u', language=language, hashtags=[]))
        raw = self.spark.createDataFrame([(x,) for x in [event('1'), event('1'),
            event('2', 'AI exemple'), event('3', language='de'), event('4', text='travail'),
            event('5', timestamp='invalid'), '{bad json']], ['value'])
        with patch.dict(os.environ, KEYWORDS='AI', LANGUAGES='fr,en'):
            df = transform(raw)
        self.assertEqual(df.count(), 3)
        with psycopg.connect() as conn:
            conn.execute('CREATE SCHEMA pipeline_test')
            for table in ('toots', 'window_stats', 'user_stats'):
                conn.execute(f'CREATE TABLE pipeline_test.{table} (LIKE public.{table} INCLUDING ALL)')
        try:
            with patch.dict(os.environ, PGOPTIONS='-c search_path=pipeline_test'):
                save_batch(df, 0)
                save_batch(df, 0)  # Rejeu du même micro-batch.
                save_batch(df, 1)  # Doublons dans un autre micro-batch.
                # Arrivée tardive : même fenêtre, autre micro-batch.
                with patch.dict(os.environ, KEYWORDS='AI', LANGUAGES='fr,en'):
                    later = transform(self.spark.createDataFrame([(event('6'),)], ['value']))
                save_batch(later, 2)
                with psycopg.connect() as conn:
                    self.assertEqual(conn.execute('SELECT count(*) FROM toots').fetchone()[0], 3)
                    self.assertEqual(conn.execute('SELECT toot_count FROM window_stats').fetchone()[0], 3)
                    n, total, avg = conn.execute('SELECT toot_count,total_length,avg_toot_length FROM user_stats').fetchone()
                    self.assertEqual((n, total), (3, 24))
                    self.assertEqual(avg, 8.0)
        finally:
            with psycopg.connect() as conn:
                conn.execute('DROP SCHEMA pipeline_test CASCADE')


if __name__ == '__main__':
    unittest.main()
