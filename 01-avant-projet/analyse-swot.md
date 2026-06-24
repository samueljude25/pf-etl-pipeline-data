# Analyse SWOT — Pipeline ETL (Entreprise commerciale, Afrique centrale)

## Forces

**Expertise technique disponible**
Le consultant maîtrise l'ensemble de la stack technique retenue (Python, Airflow, dbt, PostgreSQL). Aucune compétence externe supplémentaire n'est nécessaire pour le développement.

**Sources de données existantes et structurées**
L'ERP et le CRM sont des systèmes structurés avec des APIs ou connecteurs disponibles. Les données existent déjà — il s'agit de les centraliser, pas de les créer.

**Besoin clairement quantifié**
Le problème est documenté et chiffré (780h/an perdues, 15-20% d'erreurs). Cela facilite la justification budgétaire et la mesure du succès.

**Technologies open source éprouvées**
Airflow, PostgreSQL et dbt sont des technologies matures, largement utilisées en production à l'échelle mondiale, avec des communautés actives.

## Faiblesses

**Dépendance à la connectivité**
Le pipeline nécessite des connexions stables vers les systèmes sources. Les interruptions réseau entre les filiales peuvent bloquer des ingestions.

**Courbe d'apprentissage Airflow**
L'équipe IT locale n'est pas familière avec Airflow et les DAGs Python. La maintenance nécessitera une formation ou un support consultant à long terme.

**Qualité initiale des données**
Les données historiques présentent des anomalies accumulées sur plusieurs années. La phase de nettoyage initial peut s'avérer plus longue que prévu.

**Absence de DBA dédié**
L'organisation ne dispose pas d'un administrateur de bases de données dédié. La maintenance de PostgreSQL repose sur l'équipe IT polyvalente.

## Opportunités

**Fondation pour le Machine Learning**
Une base analytique propre et centralisée est le prérequis indispensable aux modèles ML (prévision des ventes, scoring clients). Ce pipeline ouvre la voie aux projets d'IA futurs.

**Conformité réglementaire CEMAC**
La centralisation des données facilite la production de rapports réglementaires pour les autorités de la CEMAC et les partenaires financiers.

**Expansion géographique simplifiée**
Lorsque l'entreprise ouvrira une nouvelle filiale, l'intégration de sa source de données dans le pipeline existant sera rapide et standardisée.

**Valorisation auprès des investisseurs**
Un système de reporting fiable et automatisé renforce la crédibilité de l'entreprise auprès des banques et des investisseurs potentiels.

## Menaces

**Interruptions des systèmes sources**
Si l'ERP est mis à jour ou migré, les connecteurs du pipeline peuvent casser. Une surveillance proactive et des tests de non-régression sont nécessaires.

**Sécurité des données inter-sites**
Le transfert de données confidentielles (clients, prix) entre filiales via le réseau nécessite un chiffrement rigoureux et une politique d'accès stricte.

**Turnover du consultant**
Si le consultant principal quitte le projet sans transfert de compétences, la maintenance du pipeline sera compromise. La documentation et la formation sont critiques.

**Évolution des APIs sources**
Les APIs du CRM peuvent évoluer (nouveaux endpoints, authentification renforcée) sans préavis, nécessitant des mises à jour rapides des connecteurs.
