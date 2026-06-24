# Roadmap — Pipeline ETL multi-sources

## Vue d'ensemble

| Phase | Durée | Objectif |
|---|---|---|
| Phase 0 — Audit & cadrage | 2 semaines | Inventaire sources, qualité données |
| Phase 1 — Infrastructure | 2 semaines | Docker, Airflow, PostgreSQL |
| Phase 2 — Extracteurs | 3 semaines | Connecteurs ERP, CRM, SFTP, API |
| Phase 3 — Transformations | 3 semaines | dbt Silver + Gold |
| Phase 4 — Qualité & monitoring | 2 semaines | Great Expectations, alertes |
| Phase 5 — Déploiement | 2 semaines | Tests, formation, mise en production |

---

## Phase 0 — Audit & cadrage (S1–S2)
- Inventaire exhaustif des sources de données et de leurs formats
- Évaluation de la qualité initiale (profil de données)
- Définition du modèle cible (schéma dimensionnel Gold)
- Mise en place du dépôt Git et des conventions de nommage

## Phase 1 — Infrastructure (S3–S4)
- Installation Docker + Docker Compose sur le serveur
- Déploiement Airflow (CeleryExecutor + Redis)
- Création des bases PostgreSQL et des schémas bronze/silver/gold
- Configuration des variables et connexions Airflow

## Phase 2 — Extracteurs (S5–S7)
- Développement de l'extracteur ERP (SQL delta)
- Développement de l'extracteur CRM (API REST + OAuth)
- Développement de l'extracteur fichiers agences (SFTP + Pandas)
- Développement de l'extracteur API taux de change BEAC
- Chargement Bronze et tests unitaires

## Phase 3 — Transformations dbt (S8–S10)
- Modèles Silver : nettoyage et normalisation par entité
- Modèles Gold : schéma dimensionnel (faits + dimensions)
- Tests dbt (not_null, unique, accepted_values, relationships)
- Documentation dbt auto-générée (dbt docs generate)

## Phase 4 — Qualité & monitoring (S11–S12)
- Configuration Great Expectations (suites de tests par source)
- Intégration dans les DAGs Airflow
- Configuration des alertes email
- Tableau de bord de monitoring Airflow

## Phase 5 — Déploiement (S13–S14)
- Tests de charge et de performance
- Formation équipe IT (runbook, monitoring, dépannage)
- Mise en production officielle
- Validation des données avec les équipes métier (recette)
- Documentation finale et transfert de compétences
