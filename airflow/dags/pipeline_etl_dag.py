"""
DAG principal du pipeline ETL commerce Congo.
Orchestre les étapes Extract → Transform → Load quotidiennement.
Auteur : Samuel Jude SENDZI — Chef de Projet Digital & Consultant SI
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

# Ajout du chemin du projet pour les imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# ─── Paramètres par défaut du DAG ────────────────────────────────────────────

ARGUMENTS_PAR_DEFAUT = {
    "owner": "samuel.sendzi",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": [os.getenv("AIRFLOW_EMAIL_ALERT", "alertes@commerce-congo.cg")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─── Fonctions des tâches ────────────────────────────────────────────────────

def extraire_csv(**contexte):
    """
    Tâche d'extraction : lit tous les fichiers CSV sources.
    Les DataFrames sont poussés dans XCom pour les tâches suivantes.
    """
    from src.extract.extract_csv import extraire_toutes_sources

    ti = contexte["ti"]  # Task Instance pour XCom
    date_execution = contexte["ds"]

    print(f"[EXTRACT CSV] Extraction pour la date : {date_execution}")

    # Extraction de toutes les sources CSV
    sources = extraire_toutes_sources()

    # Sérialisation pour XCom (conversion en dictionnaire de records)
    for nom_source, df in sources.items():
        ti.xcom_push(
            key=f"nb_lignes_{nom_source}",
            value=len(df),
        )
        # Sauvegarde intermédiaire en Parquet pour passage entre tâches
        chemin_parquet = f"/tmp/etl_{nom_source}_{date_execution}.parquet"
        df.to_parquet(chemin_parquet, index=False)
        ti.xcom_push(key=f"chemin_{nom_source}", value=chemin_parquet)

    print(
        f"[EXTRACT CSV] Terminé : "
        + ", ".join(f"{k}={len(v)} lignes" for k, v in sources.items())
    )


def extraire_api(**contexte):
    """
    Tâche d'extraction : récupère les données depuis l'API simulée.
    """
    from src.extract.extract_api import extraire_donnees_api

    ti = contexte["ti"]
    date_execution = contexte["ds"]

    print(f"[EXTRACT API] Extraction API pour le {date_execution}")

    resultats = extraire_donnees_api()

    for nom_source, df in resultats.items():
        chemin_parquet = f"/tmp/etl_api_{nom_source}_{date_execution}.parquet"
        df.to_parquet(chemin_parquet, index=False)
        ti.xcom_push(key=f"chemin_api_{nom_source}", value=chemin_parquet)

    print(f"[EXTRACT API] Terminé : {sum(len(df) for df in resultats.values())} enregistrements")


def extraire_base_donnees(**contexte):
    """
    Tâche d'extraction : lit depuis la base transactionnelle source.
    """
    from src.extract.extract_db import extraire_toutes_sources_db

    ti = contexte["ti"]
    date_execution = contexte["ds"]

    # URL de connexion depuis les variables d'environnement
    url_db_source = os.getenv("SOURCE_DB_URL", None)

    print(f"[EXTRACT DB] Extraction DB pour le {date_execution}")

    resultats = extraire_toutes_sources_db(url_connexion=url_db_source)

    for nom_source, df in resultats.items():
        chemin_parquet = f"/tmp/etl_db_{nom_source}_{date_execution}.parquet"
        df.to_parquet(chemin_parquet, index=False)
        ti.xcom_push(key=f"chemin_db_{nom_source}", value=chemin_parquet)

    print(f"[EXTRACT DB] Terminé : {sum(len(df) for df in resultats.values())} enregistrements")


def transformer_clients(**contexte):
    """
    Tâche de transformation : nettoie et enrichit les données clients.
    """
    import pandas as pd
    from src.transform.clean_clients import nettoyer_clients

    ti = contexte["ti"]
    date_execution = contexte["ds"]

    # Lecture du fichier Parquet produit par la tâche extract_csv
    chemin_clients = ti.xcom_pull(task_ids="extraire_csv", key="chemin_clients")
    if not chemin_clients or not os.path.exists(chemin_clients):
        raise FileNotFoundError(f"Fichier clients introuvable : {chemin_clients}")

    print(f"[TRANSFORM CLIENTS] Chargement depuis {chemin_clients}")
    df_brut = pd.read_parquet(chemin_clients)

    df_nettoye = nettoyer_clients(df_brut)

    # Sauvegarde du résultat
    chemin_sortie = f"/tmp/etl_clients_clean_{date_execution}.parquet"
    df_nettoye.to_parquet(chemin_sortie, index=False)
    ti.xcom_push(key="chemin_clients_clean", value=chemin_sortie)
    ti.xcom_push(key="nb_clients_clean", value=len(df_nettoye))

    print(f"[TRANSFORM CLIENTS] {len(df_brut)} → {len(df_nettoye)} clients nettoyés")


def transformer_transactions(**contexte):
    """
    Tâche de transformation : nettoie, valide et enrichit les transactions.
    """
    import pandas as pd
    from src.transform.clean_transactions import nettoyer_transactions

    ti = contexte["ti"]
    date_execution = contexte["ds"]

    # Lecture des fichiers Parquet produits par extract_csv
    chemin_tx = ti.xcom_pull(task_ids="extraire_csv", key="chemin_transactions")
    chemin_clients = ti.xcom_pull(task_ids="transformer_clients", key="chemin_clients_clean")
    chemin_produits = ti.xcom_pull(task_ids="extraire_csv", key="chemin_produits")
    chemin_magasins = ti.xcom_pull(task_ids="extraire_csv", key="chemin_magasins")

    df_tx = pd.read_parquet(chemin_tx)
    df_clients = pd.read_parquet(chemin_clients) if chemin_clients else None
    df_produits = pd.read_parquet(chemin_produits) if chemin_produits else None
    df_magasins = pd.read_parquet(chemin_magasins) if chemin_magasins else None

    print(f"[TRANSFORM TX] Nettoyage de {len(df_tx)} transactions...")

    df_valide, df_rejete = nettoyer_transactions(df_tx, df_clients, df_produits, df_magasins)

    # Sauvegarde des transactions valides et rejetées
    chemin_valide = f"/tmp/etl_transactions_clean_{date_execution}.parquet"
    chemin_rejete = f"/tmp/etl_transactions_rejete_{date_execution}.parquet"
    df_valide.to_parquet(chemin_valide, index=False)
    df_rejete.to_parquet(chemin_rejete, index=False)

    ti.xcom_push(key="chemin_transactions_clean", value=chemin_valide)
    ti.xcom_push(key="chemin_transactions_rejete", value=chemin_rejete)
    ti.xcom_push(key="nb_transactions_valides", value=len(df_valide))
    ti.xcom_push(key="nb_transactions_rejetees", value=len(df_rejete))

    print(f"[TRANSFORM TX] {len(df_valide)} valides, {len(df_rejete)} rejetées")


def calculer_agregations(**contexte):
    """
    Tâche de transformation : calcule toutes les agrégations métier.
    """
    import pandas as pd
    from src.transform.aggregate import calculer_toutes_agregations

    ti = contexte["ti"]
    date_execution = contexte["ds"]

    chemin_tx = ti.xcom_pull(
        task_ids="transformer_transactions", key="chemin_transactions_clean"
    )
    df_transactions = pd.read_parquet(chemin_tx)

    print(f"[AGGREGATE] Calcul des agrégations sur {len(df_transactions)} transactions...")

    agregations = calculer_toutes_agregations(df_transactions)

    for nom, df in agregations.items():
        chemin = f"/tmp/etl_agregat_{nom}_{date_execution}.parquet"
        df.to_parquet(chemin, index=False)
        ti.xcom_push(key=f"chemin_agregat_{nom}", value=chemin)

    print(f"[AGGREGATE] {len(agregations)} agrégations calculées")


def charger_entrepot(**contexte):
    """
    Tâche de chargement : charge toutes les données dans l'entrepôt PostgreSQL.
    """
    import pandas as pd
    from src.load.load_warehouse import charger_pipeline_complet

    ti = contexte["ti"]
    date_execution = contexte["ds"]

    url_entrepot = os.getenv("WAREHOUSE_DB_URL", None)

    print(f"[LOAD] Chargement vers l'entrepôt...")

    # Récupération des données staging
    donnees_staging = {}
    for source in ["clients", "transactions", "produits", "magasins"]:
        if source == "clients":
            cle = "chemin_clients_clean"
            task_id = "transformer_clients"
        elif source == "transactions":
            cle = "chemin_transactions_clean"
            task_id = "transformer_transactions"
        else:
            cle = f"chemin_{source}"
            task_id = "extraire_csv"

        chemin = ti.xcom_pull(task_ids=task_id, key=cle)
        if chemin and os.path.exists(chemin):
            donnees_staging[source] = pd.read_parquet(chemin)

    # Récupération des agrégations (marts)
    donnees_marts = {}
    for agregat in ["ca_mensuel", "top_produits", "performance_magasins", "segmentation_rfm"]:
        chemin = ti.xcom_pull(
            task_ids="calculer_agregations", key=f"chemin_agregat_{agregat}"
        )
        if chemin and os.path.exists(chemin):
            donnees_marts[agregat] = pd.read_parquet(chemin)

    succes, rapport = charger_pipeline_complet(donnees_staging, donnees_marts, url_entrepot)

    ti.xcom_push(key="rapport_chargement", value=rapport.to_dict() if not rapport.empty else {})
    ti.xcom_push(key="chargement_succes", value=succes)

    if not succes:
        raise Exception("Le chargement a rencontré des erreurs. Vérifier les logs.")

    print(f"[LOAD] Chargement terminé avec succès : {len(rapport)} tables traitées")


def nettoyer_fichiers_temporaires(**contexte):
    """
    Tâche de nettoyage : supprime les fichiers Parquet temporaires.
    """
    import glob

    date_execution = contexte["ds"]
    pattern = f"/tmp/etl_*_{date_execution}.parquet"
    fichiers = glob.glob(pattern)

    for fichier in fichiers:
        try:
            os.remove(fichier)
        except OSError as e:
            print(f"Impossible de supprimer {fichier} : {e}")

    print(f"[CLEANUP] {len(fichiers)} fichiers temporaires supprimés")


# ─── Définition du DAG ───────────────────────────────────────────────────────

with DAG(
    dag_id="pipeline_etl_commerce_congo",
    description=(
        "Pipeline ETL complet pour la centralisation des données commerciales "
        "— Entreprise Afrique Centrale (Congo-Brazzaville)"
    ),
    default_args=ARGUMENTS_PAR_DEFAUT,
    schedule_interval="@daily",           # Exécution quotidienne à minuit
    catchup=False,                         # Pas de rattrapage des exécutions passées
    max_active_runs=1,                     # Une seule exécution simultanée
    tags=["etl", "commerce", "congo", "quotidien"],
    doc_md="""
    ## Pipeline ETL Commerce Congo

    Ce DAG orchestre le pipeline ETL complet pour centraliser les données
    commerciales multi-sources dans l'entrepôt analytique.

    ### Étapes
    1. **Extraction CSV** : lecture des fichiers sources (clients, transactions, produits, magasins)
    2. **Extraction API** : récupération des prix temps réel et des taux de change
    3. **Extraction DB** : extraction depuis le système ERP interne
    4. **Transformation clients** : nettoyage, déduplication, enrichissement
    5. **Transformation transactions** : validation montants, jointures, enrichissement temporel
    6. **Agrégations** : CA mensuel, top produits, segmentation RFM
    7. **Chargement entrepôt** : staging + marts PostgreSQL
    8. **Nettoyage** : suppression des fichiers temporaires

    ### Monitoring
    - Alertes email en cas d'échec
    - Métriques disponibles dans Airflow UI
    - Logs détaillés dans `/var/log/airflow/`
    """,
) as dag:

    # ─── Phase 1 : Extraction (parallélisable) ───────────────────────────────

    tache_extract_csv = PythonOperator(
        task_id="extraire_csv",
        python_callable=extraire_csv,
        doc="Extraction des fichiers CSV sources depuis data/raw/",
    )

    tache_extract_api = PythonOperator(
        task_id="extraire_api",
        python_callable=extraire_api,
        doc="Extraction depuis l'API REST (prix, taux de change, événements)",
    )

    tache_extract_db = PythonOperator(
        task_id="extraire_base_donnees",
        python_callable=extraire_base_donnees,
        doc="Extraction depuis la base de données transactionnelle source",
    )

    # ─── Phase 2 : Transformation (séquentielle) ─────────────────────────────

    tache_transform_clients = PythonOperator(
        task_id="transformer_clients",
        python_callable=transformer_clients,
        doc="Nettoyage et enrichissement des données clients",
    )

    tache_transform_transactions = PythonOperator(
        task_id="transformer_transactions",
        python_callable=transformer_transactions,
        doc="Validation, correction et enrichissement des transactions",
    )

    tache_aggregate = PythonOperator(
        task_id="calculer_agregations",
        python_callable=calculer_agregations,
        doc="Calcul des agrégations métier (CA, RFM, top produits)",
    )

    # ─── Phase 3 : Chargement ────────────────────────────────────────────────

    tache_load = PythonOperator(
        task_id="charger_entrepot",
        python_callable=charger_entrepot,
        doc="Chargement des données transformées vers PostgreSQL (staging + marts)",
    )

    # ─── Phase 4 : Nettoyage ─────────────────────────────────────────────────

    tache_cleanup = PythonOperator(
        task_id="nettoyer_fichiers_temporaires",
        python_callable=nettoyer_fichiers_temporaires,
        trigger_rule="all_done",          # S'exécute même en cas d'erreur partielle
        doc="Suppression des fichiers Parquet temporaires",
    )

    # ─── Graphe des dépendances ───────────────────────────────────────────────
    #
    # Extraction CSV ─┐
    # Extraction API  ├─→ Transform Clients ─→ Transform Transactions ─→ Agrégations ─→ Load ─→ Cleanup
    # Extraction DB  ─┘
    #

    [tache_extract_csv, tache_extract_api, tache_extract_db] >> tache_transform_clients
    tache_transform_clients >> tache_transform_transactions
    tache_transform_transactions >> tache_aggregate
    tache_aggregate >> tache_load
    tache_load >> tache_cleanup
