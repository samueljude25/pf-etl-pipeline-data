# Pipeline ETL Data Engineering — Afrique centrale

**Portfolio de compétences — Samuel Jude Sendzi, Chef de Projet Digital & Consultant SI**

---

## Présentation du projet

Ce projet démontre la conception et l'implémentation d'un **pipeline ETL (Extract, Transform, Load) complet** pour centraliser des données multi-sources dans un entrepôt analytique. Le cas d'usage est une entreprise commerciale de taille intermédiaire opérant en Afrique centrale, qui souhaite consolider ses données ERP, CRM, fichiers CSV et APIs externes pour alimenter ses analyses BI.

---

## Problématique métier

L'entreprise dispose de plusieurs systèmes d'information opérationnels qui ne communiquent pas entre eux :
- Un ERP pour la gestion des ventes et des stocks
- Un CRM pour les données clients et prospects
- Des fichiers Excel produits par les agences commerciales
- Des APIs externes (taux de change, données économiques CEMAC)

Les équipes analytiques passent 30 à 40% de leur temps à consolider manuellement ces sources. Les données ne sont jamais parfaitement à jour et les erreurs de consolidation sont fréquentes.

---

## Solution : Pipeline ETL automatisé

L'architecture implémente une chaîne ETL orchestrée par **Apache Airflow**, avec des transformations réalisées en **Python (Pandas + SQLAlchemy)** et **dbt**, et un stockage dans **PostgreSQL**.

---

## Stack technique

| Composant | Technologie | Rôle |
|---|---|---|
| Orchestration | Apache Airflow | Planification et monitoring des DAGs |
| Extraction | Python + requests | Connexion ERP, CRM, APIs |
| Transformation | Python Pandas + dbt | Nettoyage, enrichissement, modélisation |
| Stockage | PostgreSQL | Entrepôt de données analytique |
| Conteneurisation | Docker + Docker Compose | Déploiement reproductible |
| Qualité données | Great Expectations | Tests automatiques de qualité |

---

## Architecture du pipeline

```
[ERP (SQL)]  ─────────────────────────────────────────────────────────┐
[CRM (API)]  ──────────────► [Airflow DAG] ──► [Python ETL] ──► [PostgreSQL]
[CSV Agences] ────────────────────────────►  [dbt models]    ──► [Power BI]
[API CEMAC]  ─────────────────────────────────────────────────────────┘
```

### Couche Bronze (Raw)
Données brutes copiées à l'identique depuis les sources, partitionnées par date d'ingestion.

### Couche Silver (Refined)
Données nettoyées, dédupliquées, normalisées selon un référentiel commun.

### Couche Gold (Analytique)
Agrégats et modèle dimensionnel prêts pour la consommation BI.

---

## Structure du dépôt

```
01-avant-projet/
   etude-opportunite.md
   etude-faisabilite.md
   analyse-swot.md
   analyse-pestel.md

02-cahier-des-charges/
   cahier-des-charges.md

03-conception/
   architecture-technique.md
   stack-technologique.md

04-roadmap/
   phases-projet.md
```

---

## Résultats attendus

- Réduction de 90% du temps de consolidation manuelle des données
- Données analytiques disponibles en J+1 (au lieu de J+7 à J+14)
- Zéro erreur de consolidation grâce aux tests automatiques de qualité
- Base analytique unique servant à la fois le BI et les modèles ML
