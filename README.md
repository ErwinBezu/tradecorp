# TradeCorp — Pipeline Data Engineering

## Présentation

Ce projet met en place un pipeline de données avec **PySpark** permettant d'ingérer, nettoyer, transformer et enrichir des données métier.

Les données sources sont récupérées depuis le conteneur `raw` d'**Azure Blob Storage**, puis les données transformées sont enregistrées au format **Parquet** dans le conteneur `clean`.

## Technologies

- Python / PySpark
- Docker
- Azure Blob Storage
- PostgreSQL
- Parquet
- pytest
- Apache Airflow

## Architecture

Le pipeline est séparé en plusieurs modules :

- `reader.py` : lecture des données ;
- `transformer.py` : nettoyage et transformations ;
- `enrichment.py` : jointures et enrichissement ;
- `writer.py` : écriture des données ;
- `pipeline.py` : exécution du pipeline.

```text
Azure Blob Storage (raw)
        ↓
      Reader
        ↓
   Transformer
        ↓
   Enrichment
        ↓
      Writer
        ↓
Azure Blob Storage (clean)
```

## Orchestration avec Apache Airflow

Apache Airflow est utilisé pour orchestrer et automatiser l'exécution du pipeline de données.
Le DAG `ktradecorp_etl_pipeline` permet d'enchainer les différentes étapes du traitement et de suivre leur état
Le pipeline est planifié pour s'exécuter automatiquement tous les jours à 6h du matin. 

### DAG
Le DAG `tradecorp_etl_pipeline` est composé de 5 tâches exécutées successivement :

```text
attendre_go
    ↓
fetch_exchange_rates
    ↓
reader
    ↓
transformer
    ↓
writer

## Lancement

Démarrer l'environnement :

```bash
docker compose up -d
```

Vérifier les conteneurs :

```bash
docker compose ps
```

Exécuter le pipeline :

```bash
docker compose exec spark spark-submit /home/jovyan/src/pipeline.py
```

## Tests

```bash
docker compose exec spark pytest
```

## Configuration

Les identifiants Azure et PostgreSQL sont stockés dans un fichier `.env` afin de ne pas exposer les secrets dans le code source.

### Question 67

Le DAG est configuré avec une `start_date` au 1er janvier 2024 et `catchup=False`
Même si la date de début est ancienne, Airflow ne rejoue pas automatiquement toutes les exécutions planifiées entre 2024 et aujourd'hui. 
C'est grace à l'option `catchup=False`, elle indique à Airflow de ne pas créer les exécutions manquées lors de l'activation du DAG.
Donc la prochaine exécution correspond à la planification définie, pour l'exercice tous les jours à 6h du matin.



