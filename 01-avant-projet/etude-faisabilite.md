# Étude de faisabilité — Pipeline ETL multi-sources

## Faisabilité technique

### Sources de données et accès
| Source | Type | Accès | Complexité |
|---|---|---|---|
| ERP interne | Base SQL Server | JDBC direct | Faible |
| CRM | API REST JSON | Token OAuth | Moyenne |
| Fichiers agences | Excel/CSV via SFTP | Automatisable | Faible |
| APIs économiques (CEMAC/BCC) | REST public | Clé API | Faible |

### Infrastructure requise
- Serveur Linux (Ubuntu 22.04) pour héberger Airflow + PostgreSQL + Docker
- 4 vCPU, 8 Go RAM minimum pour Airflow avec plusieurs workers
- Stockage 500 Go pour 3 ans d'historique de données brutes

### Compétences disponibles
- Consultant data engineering : Python, Airflow, dbt, PostgreSQL (compétences certifiées)
- Équipe IT : administration Linux de base, supervision des services
- Utilisateurs finaux : Power BI (interface de consommation uniquement)

**Verdict technique : FAISABLE**

## Faisabilité organisationnelle

L'organisation dispose d'une équipe IT capable de maintenir l'infrastructure Docker. Le pipeline, une fois déployé, est largement automatisé et ne nécessite qu'une supervision légère. Les utilisateurs finaux n'interagissent pas avec le pipeline mais uniquement avec Power BI.

**Verdict organisationnel : FAISABLE**

## Faisabilité financière

| Poste | Coût estimé (FCFA) |
|---|---|
| Serveur Linux (location cloud 12 mois) | 1 200 000 |
| Prestation data engineering (60 jours) | 6 000 000 |
| Licences Power BI Pro (3 utilisateurs) | 1 080 000 |
| Formation équipe IT (2 jours) | 400 000 |
| **Total** | **8 680 000** |

Gains annuels estimés : 12 000 000 FCFA (économies temps + réduction erreurs)
**ROI : positif dès le 9ème mois**

**Verdict financier : VIABLE**
