# Cahier des charges — Pipeline ETL multi-sources

## 1. Objet
Conception, développement et déploiement d'un pipeline ETL automatisé permettant de centraliser les données de plusieurs systèmes sources dans un entrepôt analytique PostgreSQL, alimentant les tableaux de bord Power BI et les futures analyses ML.

## 2. Sources de données

| Source | Système | Type de connexion | Fréquence |
|---|---|---|---|
| ERP Ventes | SQL Server | JDBC | Quotidien (nuit) |
| ERP Stocks | SQL Server | JDBC | Quotidien (nuit) |
| CRM Clients | API REST | OAuth 2.0 | Quotidien |
| Agences (fichiers) | SFTP | CSV/Excel | Quotidien |
| Taux de change BEAC | API publique | HTTP GET | Hebdomadaire |

## 3. Exigences fonctionnelles

### Extraction
- Extraction incrémentale (delta) pour les tables volumineuses (ventes, stocks)
- Extraction complète pour les référentiels (clients, produits, géographie)
- Gestion des erreurs avec retry automatique (3 tentatives max)
- Journalisation de chaque extraction (source, volume, statut, durée)

### Transformation
- Déduplication sur clé métier pour chaque entité
- Normalisation des référentiels (codes pays ISO, noms de villes harmonisés)
- Calcul des indicateurs dérivés (marges, variations, ratios)
- Contrôle qualité automatique avec Great Expectations (tests sur nullité, unicité, plages de valeurs)

### Chargement
- Stratégie upsert (INSERT ON CONFLICT UPDATE) pour les chargements incrémentaux
- Chargement complet pour les dimensions stables
- Partitionnement des tables de faits par année et mois

## 4. Architecture technique

### Orchestration : Apache Airflow
- DAG principal : `etl_daily_pipeline` — s'exécute chaque nuit à 02h00
- DAG secondaire : `etl_quality_check` — contrôles de qualité après chargement
- Alertes email en cas d'échec d'une tâche

### Transformations : Python + dbt
- Scripts Python pour les extractions complexes (API, fichiers)
- Modèles dbt pour les transformations SQL (couches Silver et Gold)
- Tests dbt intégrés (not_null, unique, accepted_values)

### Stockage : PostgreSQL
- Schema `bronze` : données brutes
- Schema `silver` : données nettoyées
- Schema `gold` : modèle dimensionnel

### Conteneurisation : Docker
- `docker-compose.yml` orchestrant Airflow, PostgreSQL, Redis (pour Celery executor)
- Images versionnées pour reproductibilité

## 5. SLA et performance

| Indicateur | Cible |
|---|---|
| Disponibilité pipeline | 99% en semaine |
| Délai ingestion J+1 | Données disponibles avant 06h00 |
| Temps d'exécution DAG complet | < 2 heures |
| Taux de succès des DAGs | > 98% sur 30 jours glissants |

## 6. Livrables

- Code source Python des extracteurs (dépôt Git)
- Modèles dbt documentés (schema.yml)
- DAGs Airflow avec tests unitaires
- Documentation d'architecture
- Runbook de maintenance et dépannage
- Formation équipe IT (1 journée)
