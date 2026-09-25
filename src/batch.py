"""Partie 3 : Batch processing sur les données historiques stockées dans PostgreSQL."""
import os
from pyspark.sql import SparkSession, functions as F


def run_batch():
    # Initialisation de la SparkSession pour le traitement Batch
    spark = (SparkSession.builder.appName('5SPAR-Mastodon-Batch')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '4').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    # Récupération des paramètres de connexion Postgres depuis l'environnement Docker
    pg_host = os.getenv('PGHOST', 'postgres')
    pg_db = os.getenv('PGDATABASE', 'mastodon')
    pg_user = os.getenv('PGUSER', 'mastodon')
    pg_password = os.getenv('PGPASSWORD', 'local_demo_change_me')

    jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{pg_db}"
    connection_properties = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }

    print("--- 1. Chargement des données historiques depuis PostgreSQL (table 'toots') ---")
    df = spark.read.jdbc(url=jdbc_url, table="toots", properties=connection_properties)
    df.show(5, truncate=False)

    # Optimisation (Cours Jour 3) : Caching du DataFrame car plusieurs actions vont suivre
    df.cache()

    print("--- 2. Tâche Batch : Filtrage par activité utilisateur (ex: utilisateurs avec plus de X toots) ---")
    user_activity = df.groupBy("username") \
        .agg(F.count("*").alias("total_toots")) \
        .filter(F.col("total_toots") > 1)
    user_activity.show()

    print("--- 3. Tâche Batch : Analyse des hashtags les plus fréquents ---")
    # Comme la colonne 'hashtags' est un tableau (array<string>), on peut utiliser explode()
    exploded_tags = df.withColumn("tag", F.explode("hashtags"))
    top_tags = exploded_tags.groupBy("tag") \
        .agg(F.count("*").alias("tag_count")) \
        .orderBy(F.col("tag_count").desc())
    top_tags.show(10)

    print("--- 4. Tâche Batch : Métriques globales journalières (Total toots & longueur moyenne) ---")
    daily_metrics = df.withColumn("date", F.to_date(F.col("created_at"))) \
        .groupBy("date") \
        .agg(
        F.count("*").alias("toots_count"),
        F.avg("text_length").alias("avg_length"),
        F.avg("favourites_count").alias("avg_favourites")
    ) \
        .orderBy("date")
    daily_metrics.show()

    print("--- 5. Optimisation & Écriture (Repartition / Coalesce - Cours Jour 3) ---")
    # Utilisation de coalesce(1) pour l'écriture afin d'éviter la prolifération de micro-fichiers
    daily_metrics.coalesce(1).write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="batch_daily_metrics", properties=connection_properties)

    print("Traitement Batch (Partie 3) terminé avec succès !")
    spark.stop()


if __name__ == '__main__':
    run_batch()