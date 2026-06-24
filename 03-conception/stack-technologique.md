# Stack technologique — Pipeline ETL

## Apache Airflow
Orchestrateur de workflows data, standard de l'industrie. Permet de définir des DAGs (Directed Acyclic Graphs) en Python, avec scheduling, retry automatique, monitoring web et alertes. Choix retenu pour sa maturité, sa communauté et sa compatibilité avec toutes les sources de données.

## dbt (data build tool)
Outil de transformation SQL avec versioning, tests intégrés et documentation auto-générée. Il transforme le SQL en un vrai projet d'ingénierie logicielle (tests, CI/CD, lineage). Essentiel pour les couches Silver et Gold.

## Python (Pandas + SQLAlchemy + requests)
Pour les extractions complexes (APIs avec authentification, fichiers Excel multi-onglets) et les transformations nécessitant une logique Python non exprimable en SQL pur.

## PostgreSQL
Base de données relationnelle open source, performante et extensible. Supporte nativement les schémas multiples (bronze/silver/gold), les types JSON, les index partiels et les partitions. Connecteurs natifs pour Power BI, Python et dbt.

## Docker + Docker Compose
Garantit la reproductibilité de l'environnement (Airflow, PostgreSQL, Redis) sur n'importe quel serveur Linux. Facilite les déploiements, les montées de version et la reprise après sinistre.

## Great Expectations
Framework Python de validation de la qualité des données. Génère des rapports HTML détaillés et peut s'intégrer dans les DAGs Airflow comme étape de contrôle post-chargement.

## Tableau comparatif

| Outil | Alternative évaluée | Raison du choix |
|---|---|---|
| Airflow | Prefect, Dagster | Maturité, communauté, documentation |
| dbt | SQLAlchemy pur | Tests intégrés, documentation, lineage |
| PostgreSQL | MySQL, SQL Server | Coût zéro, partitions natives, JSON |
| Docker | VM classique | Portabilité, isolation, reproductibilité |
| Great Expectations | dbt tests seuls | Rapports HTML, intégration Airflow |
