# 5SPAR — Analyse de données Mastodon

Projet réalisé avec les notions du cours : DataFrames, transformations, micro-batchs et checkpoints.

**Mastodon → Kafka → Spark Structured Streaming → PostgreSQL**

- **Partie 1** : récupérer les publications publiques par hashtag et les envoyer dans Kafka.
- **Partie 2** : nettoyer et filtrer les publications, compter les toots par minute et calculer leur longueur moyenne par utilisateur.
- **Partie 3** : traitement batch, agrégations avancées et optimisation.
- **Partie 4** : analyse de sentiments avec Spark MLlib sur l'historique de données.
- **Partie 5** : visualisation des données avec Pandas, Matplotlib et Seaborn.

## Configuration

Prérequis : Docker Desktop démarré en mode conteneurs Linux et une connexion Internet.

Dans Mastodon, ouvrir **Préférences → Développement → Nouvelle application**, sélectionner le droit `read:statuses` et récupérer le jeton d’accès. Renseigner dans `.env` :

```dotenv
MASTODON_API_BASE_URL=https://mastodon.social
MASTODON_ACCESS_TOKEN=ton_jeton
```

Les sujets et langues sont configurables dans `.env` :

- `MASTODON_HASHTAGS` : flux suivis, par exemple `music,photography,art,nature,gaming,travel,football,books`.
- `KEYWORDS` : mots ou hashtags à conserver, séparés par des virgules.
- `LANGUAGES` : langues acceptées, par défaut `fr,en`.

Les flux par hashtag sont utilisés car le flux public général de `mastodon.social` ne renvoyait pas de nouvelles publications lors des essais.

## Lancement

Pour lancer la collecte en temps réel avec l'environnement d'analyse :

```powershell
docker compose --profile live up -d --build spark producer jupyter
```

Spark traite les données par lots toutes les 10 secondes. Les données et checkpoints sont conservés dans des volumes Docker.

## Suivre la collecte

```powershell
docker compose logs -f --tail 20 producer spark
```

- `Message envoyé` : publication envoyée dans Kafka.
- `Micro-batch … nouveaux toots` : publications enregistrées dans PostgreSQL.
- Toutes les 30 secondes, le producteur affiche le nombre de publications reçues avant filtrage.

`Ctrl+C` quitte les logs sans arrêter la collecte. L’interface Spark est accessible sur [localhost:4040](http://localhost:4040).

Pour voir les dernières publications en base :

```powershell
docker compose exec postgres psql -U mastodon -d mastodon
```

Puis dans PostgreSQL :

```sql
SELECT created_at, username, language, left(text, 120) AS contenu
FROM toots
WHERE instance <> 'demo'
ORDER BY created_at DESC
LIMIT 10;
\watch 2
```

Les tables `window_stats` et `user_stats` contiennent les compteurs par minute et les statistiques par utilisateur.

## Démonstration sans jeton

Avec les filtres du fichier d’exemple :

```powershell
docker compose up -d --build spark
docker compose --profile demo run --rm --build demo
```

La démo envoie 12 messages fictifs. Relancer la démo ne crée pas de doublons.

## Réinitialisation, tests et arrêt

```powershell
# Effacer complètement la base de données et les checkpoints pour repartir à zéro
 docker compose down -v

# Tests Spark / PostgreSQL
 docker compose run --rm --build --no-deps spark /opt/spark/bin/spark-submit --master local[2] /app/tests/test_pipeline.py

# Tests du lecteur de flux Mastodon
 docker compose --profile live run --rm --build --no-deps producer python3 -m unittest tests.test_listener

# Arrêter les services en conservant les données
 docker compose --profile live --profile demo down
```

## Partie 3 : Traitement par lots (Batch Processing) et Optimisations

Le script `src/batch.py` permet d'analyser les données historiques enregistrées dans la table PostgreSQL `toots` en s'appuyant sur les Structured APIs de Spark.

### Fonctionnalités réalisées :

- **Chargement et Caching :** Chargement des données depuis PostgreSQL via JDBC et mise en cache (`cache()`) pour optimiser les performances lors d'actions multiples.
- **Transformations et Agrégations :**
  - Filtrage par activité utilisateur.
  - Analyse et éclatement des hashtags les plus fréquents.
  - Calcul des métriques journalières (total de toots, longueur moyenne, favoris).
- **Optimisations de performance (Tuning) :** Application de `.coalesce(1)` lors de l'écriture pour éviter la prolifération de micro-fichiers, conformément aux bonnes pratiques du moteur Catalyst.

### Exécution du traitement Batch :

```bash
docker compose exec spark /opt/spark/bin/spark-submit --master local[2] --conf spark.jars.ivy=/tmp/ivy --packages org.postgresql:postgresql:42.6.0 /app/src/batch.py
```

## Environnement JupyterLab (Parties 4 & 5)

L'interface JupyterLab est accessible sur **http://localhost:8888** une fois le service démarré.

## Partie 4 : Analyse de sentiments avec Spark MLlib

L'analyse de sentiments est réalisée dans le notebook `part4_5.ipynb` à l'aide de l'API Spark MLlib.

** Prérequis important : Téléchargement du Dataset Kaggle**
Le jeu de données d'entraînement étant trop volumineux pour être inclus dans ce dépôt, vous devez le récupérer manuellement :

1. Rendez-vous sur Kaggle : [Sentiment140 dataset](https://www.kaggle.com/datasets/kazanova/sentiment140)
2. Téléchargez l'archive et extrayez le fichier `training.1600000.processed.noemoticon.csv`.
3. Placez ce fichier à la racine du projet (au même niveau que le fichier `compose.yaml`).

### Fonctionnalités réalisées :

* **Pipeline de Machine Learning :**
1. `Tokenizer` : Découpage du texte en mots.
2. `StopWordsRemover` : Suppression des mots de liaison.
3. `CountVectorizer` et `IDF` : Vectorisation des mots avec pondération TF-IDF.
4. `LogisticRegression` : Modèle de classification binaire (Positif/Négatif).


* **Inférence :** Le modèle entraîné sur le dataset Kaggle est appliqué sur l'historique des toots stockés dans PostgreSQL.
* **Stockage :** Les prédictions sont sauvegardées dans une nouvelle table PostgreSQL nommée `toots_sentiments` pour une exploitation ultérieure en Data Visualisation.

## Partie 5 : Visualisation des données

Les résultats sont exploités visuellement dans le même environnement Jupyter, à l'aide des bibliothèques **Pandas, Matplotlib et Seaborn**. Les requêtes SQL sont envoyées directement à PostgreSQL pour optimiser le traitement mémoire.

**Graphiques générés :**

1. **Distribution des Sentiments :** Diagramme en barres illustrant la proportion de toots positifs et négatifs classifiés par le modèle MLlib.
2. **Fréquence des publications :** Courbe temporelle affichant le nombre de toots ingérés par minute.
3. **Top 10 des Hashtags :** Diagramme en barres horizontal classant les hashtags les plus populaires de l'échantillon.
4. **Heatmap d'activité utilisateur :** Matrice thermique mettant en évidence les heures d'activité des utilisateurs les plus prolifiques.
5. **Social listening :** Courbe temporelle croisant le volume d'ingestion avec les prédictions du modèle de Machine Learning par minute. 
6. **Analyse comportementale :** Graphique en violon (*Violin Plot*) combinant boîte à moustaches et densité de probabilité pour analyser la distribution statistique du nombre de caractères selon la polarité du message.
- **Chargement et Caching :** Chargement des données depuis PostgreSQL via JDBC et mise en cache (`cache()`) pour optimiser les performances lors d'actions multiples.
- **Transformations et Agrégations :**
  - Filtrage par activité utilisateur.
  - Analyse et éclatement des hashtags les plus fréquents.
  - Calcul des métriques journalières (total de toots, longueur moyenne, favoris).
- **Optimisations de performance (Tuning) :** Application de `.coalesce(1)` lors de l'écriture pour éviter la prolifération de micro-fichiers, conformément aux bonnes pratiques du moteur Catalyst.

## Contributeurs

Ce projet a été développé par :
* **Tommy Brisset** - [TommyBrisset](https://github.com/TommyBRISSET)
* **Kilian Moreau** - [KilianMoreau](https://github.com/KilianMoreau)
* **Alexandre-Chuzel** - [AlexandreChuzel](https://github.com/Alexandre-Chuzel)