# Analyse PESTEL — Pipeline ETL (Afrique centrale)

## Politique
Les politiques de numérisation des économies CEMAC encouragent les investissements en systèmes d'information. La Banque des États de l'Afrique Centrale (BEAC) pousse à la standardisation des reportings financiers, ce qui valorise les entreprises disposant de systèmes de données fiables. Les réglementations sur les transferts de données transfrontaliers dans la zone CEMAC sont encore peu contraignantes, facilitant la centralisation multi-pays.

## Économique
La diversification économique hors hydrocarbures est une priorité régionale. Les entreprises commerciales sont en première ligne pour saisir ces opportunités, mais doivent être agiles. Un pipeline de données fiable leur permet de réagir rapidement aux signaux du marché. La stabilité du franc CFA (FCFA) limite le risque de change sur les investissements technologiques.

## Social
L'urbanisation rapide des capitales (Brazzaville, Libreville, Yaoundé, Douala) génère une demande commerciale croissante. Les comportements d'achat évoluent vers le mobile et le numérique. Intégrer ces nouvelles sources de données (ventes en ligne, paiements mobile) dans le pipeline est un enjeu stratégique à court terme.

## Technologique
L'adoption des services Cloud (AWS, Azure, GCP) progresse dans la région, même si l'on-premise reste dominant pour des raisons de coût et de souveraineté. Docker et les conteneurs réduisent drastiquement les problèmes d'environnement et facilitent le déploiement dans des contextes techniques variés. Les outils open source (Airflow, dbt, PostgreSQL) offrent des fonctionnalités comparables aux solutions propriétaires coûteuses.

## Environnemental
Les data centers consomment de l'énergie. En Afrique centrale, où la production électrique est insuffisante, privilégier des architectures légères et efficientes est à la fois une contrainte et une bonne pratique. Les solutions Docker on-premise sont plus sobres que des architectures cloud surdimensionnées.

## Légal
La protection des données personnelles clients collectées dans le CRM et l'ERP doit être encadrée. Même en l'absence d'une loi nationale équivalente au RGPD, les bonnes pratiques (minimisation des données, chiffrement, journalisation des accès) s'imposent. Les données financières sont soumises aux règles de conservation comptable (généralement 10 ans dans la zone CEMAC).
