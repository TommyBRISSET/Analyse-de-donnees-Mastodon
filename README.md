# 5SPAR — Analyse de données Mastodon

Projet réalisé avec les notions du cours : DataFrames, transformations, micro-batchs et checkpoints.

**Mastodon → Kafka → Spark Structured Streaming → PostgreSQL**

- **Partie 1** : récupérer les publications publiques par hashtag et les envoyer dans Kafka.
- **Partie 2** : nettoyer et filtrer les publications, compter les toots par minute et calculer leur longueur moyenne par utilisateur.

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

```powershell
docker compose --profile live up -d --build spark producer
```

Spark traite les données par lots toutes les 10 secondes. Les données et checkpoints sont conservés dans des volumes Docker ; les doublons ne sont pas recomptés.

Après modification des filtres, relancer la même commande. Les nouvelles valeurs s’appliquent aux prochains messages.

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

## Tests et arrêt

```powershell
# Tests Spark / PostgreSQL
 docker compose run --rm --build --no-deps spark /opt/spark/bin/spark-submit --master local[2] /app/tests/test_pipeline.py

# Tests du lecteur de flux Mastodon
 docker compose --profile live run --rm --build --no-deps producer python3 -m unittest tests.test_listener

# Arrêter les services en conservant les données
 docker compose --profile live --profile demo down
```

Le détail des vérifications se trouve dans [docs/validation.md](docs/validation.md).

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
Pour lancer le script batch directement dans l'environnement conteneurisé du projet :
```bash
docker compose exec spark /opt/spark/bin/spark-submit --master local[2] --conf spark.jars.ivy=/tmp/ivy --packages org.postgresql:postgresql:42.6.0 /app/src/batch.py