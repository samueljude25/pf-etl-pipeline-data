# Architecture technique — Pipeline ETL

## Diagramme d'architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SERVEUR DOCKER                               │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │   Airflow    │    │   Python     │    │    PostgreSQL         │  │
│  │  Scheduler   │───►│  Extracteurs │───►│  bronze / silver /   │  │
│  │  + Webserver │    │  + dbt       │    │  gold schemas        │  │
│  └──────────────┘    └──────────────┘    └──────────────────────┘  │
│         │                                          │               │
│  ┌──────▼──────┐                         ┌────────▼───────────┐   │
│  │    Redis    │                         │    Power BI         │   │
│  │  (Celery)   │                         │  (connexion JDBC)   │   │
│  └─────────────┘                         └────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
         ▲                    ▲
         │                    │
   ┌─────┴──────┐    ┌────────┴───────┐
   │  ERP/CRM   │    │  SFTP / APIs   │
   │ (SQL + API)│    │  externes      │
   └────────────┘    └────────────────┘
```

## Flux de données détaillé

### Étape 1 : Extraction (02h00–03h00)
Le DAG Airflow déclenche les tâches d'extraction en parallèle :
- `extract_erp_ventes` : requête SQL delta sur la table des ventes (WHERE date_maj > dernière_extraction)
- `extract_erp_stocks` : snapshot quotidien des stocks
- `extract_crm_clients` : appel API REST avec pagination
- `extract_fichiers_agences` : lecture des CSV déposés sur le SFTP nocturne

### Étape 2 : Chargement Bronze (03h00–03h30)
Les données brutes sont chargées telles quelles dans le schema `bronze` avec horodatage d'ingestion.

### Étape 3 : Transformation Silver — dbt (03h30–04h30)
Les modèles dbt nettoient et normalisent :
```sql
-- models/silver/dim_clients.sql
WITH source AS (
    SELECT * FROM {{ source('bronze', 'crm_clients') }}
),
normalise AS (
    SELECT
        id_client,
        INITCAP(TRIM(nom)) AS nom,
        UPPER(TRIM(code_pays)) AS code_pays,
        INITCAP(TRIM(ville)) AS ville,
        CASE WHEN email ~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
             THEN email ELSE NULL END AS email_valide,
        date_creation::DATE AS date_creation
    FROM source
    WHERE id_client IS NOT NULL
)
SELECT * FROM normalise
```

### Étape 4 : Modèle Gold — agrégations (04h30–05h00)
Construction du modèle dimensionnel en étoile pour Power BI.

### Étape 5 : Contrôle qualité (05h00–05h30)
Le DAG `etl_quality_check` exécute les tests Great Expectations :
- Taux de nullité par colonne (alerte si > 5%)
- Unicité des clés primaires
- Plages de valeurs (montants positifs, dates cohérentes)
- Envoi d'un rapport de qualité par email si anomalies

## Gestion des erreurs

| Erreur | Comportement | Notification |
|---|---|---|
| Source indisponible | Retry × 3, puis skip | Email + Slack |
| Données corrompues | Mise en quarantaine Bronze | Email |
| Échec dbt | Arrêt pipeline, rollback | Email urgence |
| Délai dépassé | Alerte SLA | Email |
