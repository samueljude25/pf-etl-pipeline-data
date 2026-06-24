"""
Script de lancement du pipeline ETL complet sans Airflow.
Utile pour les tests, le développement et l'exécution manuelle.

Usage :
    python scripts/run_pipeline.py
    python scripts/run_pipeline.py --etape extract
    python scripts/run_pipeline.py --etape transform
    python scripts/run_pipeline.py --etape load --db-url postgresql://user:pass@host:5432/db

Auteur : Samuel Jude SENDZI
"""

import argparse
import os
import sys
import time
from datetime import datetime

# Ajout de la racine du projet au PYTHONPATH
RACINE_PROJET = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RACINE_PROJET)

from src.utils.logger import get_logger

logger = get_logger("run_pipeline")


def afficher_banniere():
    """Affiche la bannière de démarrage du pipeline."""
    print("\n" + "=" * 65)
    print("   PIPELINE ETL — COMMERCE CONGO")
    print("   Centralisation des données commerciales multi-sources")
    print("   Auteur : Samuel Jude SENDZI — Chef de Projet Digital")
    print("=" * 65 + "\n")


def etape_extract(args) -> dict:
    """
    Lance l'étape d'extraction depuis toutes les sources.

    Returns:
        Dictionnaire {nom_source: DataFrame}
    """
    logger.info("ETAPE 1 : EXTRACTION DES DONNÉES")
    debut = time.time()

    from src.extract.extract_csv import extraire_toutes_sources
    from src.extract.extract_api import extraire_donnees_api
    from src.extract.extract_db import extraire_toutes_sources_db

    donnees_brutes = {}

    # Extraction CSV
    logger.info("  1.1 Extraction fichiers CSV...")
    sources_csv = extraire_toutes_sources()
    donnees_brutes.update(sources_csv)

    # Extraction API simulée
    logger.info("  1.2 Extraction API REST simulée...")
    sources_api = extraire_donnees_api()
    donnees_brutes.update({f"api_{k}": v for k, v in sources_api.items()})

    # Extraction base de données (mode simulation si pas d'URL)
    logger.info("  1.3 Extraction base de données source...")
    url_source = args.source_db_url or os.getenv("SOURCE_DB_URL")
    sources_db = extraire_toutes_sources_db(url_connexion=url_source)
    donnees_brutes.update({f"db_{k}": v for k, v in sources_db.items()})

    duree = time.time() - debut
    logger.info(
        f"EXTRACTION terminée en {duree:.2f}s — "
        f"{sum(len(df) for df in donnees_brutes.values()):,} enregistrements extraits"
    )
    return donnees_brutes


def etape_transform(donnees_brutes: dict) -> dict:
    """
    Lance l'étape de transformation des données.

    Args:
        donnees_brutes: Dictionnaire des DataFrames bruts

    Returns:
        Dictionnaire des DataFrames transformés
    """
    logger.info("ETAPE 2 : TRANSFORMATION DES DONNÉES")
    debut = time.time()

    from src.transform.clean_clients import nettoyer_clients
    from src.transform.clean_transactions import nettoyer_transactions
    from src.transform.aggregate import calculer_toutes_agregations

    donnees_transformees = {}

    # Nettoyage clients
    logger.info("  2.1 Nettoyage des clients...")
    if "clients" in donnees_brutes:
        df_clients_clean = nettoyer_clients(donnees_brutes["clients"])
        donnees_transformees["clients"] = df_clients_clean
    else:
        logger.warning("Données clients non disponibles, étape ignorée")
        df_clients_clean = None

    # Nettoyage transactions
    logger.info("  2.2 Nettoyage des transactions...")
    if "transactions" in donnees_brutes:
        df_tx_clean, df_tx_rejete = nettoyer_transactions(
            donnees_brutes["transactions"],
            df_clients=df_clients_clean,
            df_produits=donnees_brutes.get("produits"),
            df_magasins=donnees_brutes.get("magasins"),
        )
        donnees_transformees["transactions"] = df_tx_clean
        donnees_transformees["transactions_rejetees"] = df_tx_rejete

        # Sauvegarde des rejets pour audit
        if not df_tx_rejete.empty:
            chemin_rejets = os.path.join(
                RACINE_PROJET, "data", "processed",
                f"transactions_rejetees_{datetime.now().strftime('%Y%m%d')}.csv"
            )
            os.makedirs(os.path.dirname(chemin_rejets), exist_ok=True)
            df_tx_rejete.to_csv(chemin_rejets, index=False, encoding="utf-8")
            logger.info(f"  Rejets sauvegardés : {chemin_rejets}")
    else:
        logger.warning("Données transactions non disponibles, étape ignorée")
        df_tx_clean = None

    # Produits et magasins (pas de transformation complexe ici)
    if "produits" in donnees_brutes:
        donnees_transformees["produits"] = donnees_brutes["produits"]
    if "magasins" in donnees_brutes:
        donnees_transformees["magasins"] = donnees_brutes["magasins"]

    # Calcul des agrégations
    logger.info("  2.3 Calcul des agrégations métier...")
    if df_tx_clean is not None and not df_tx_clean.empty:
        agregations = calculer_toutes_agregations(df_tx_clean)
        donnees_transformees.update(agregations)

    duree = time.time() - debut
    logger.info(f"TRANSFORMATION terminée en {duree:.2f}s")

    return donnees_transformees


def etape_load(donnees_transformees: dict, url_entrepot: str = None) -> bool:
    """
    Lance l'étape de chargement vers l'entrepôt.

    Args:
        donnees_transformees: Dictionnaire des DataFrames transformés
        url_entrepot: URL de connexion PostgreSQL

    Returns:
        True si le chargement réussit
    """
    logger.info("ETAPE 3 : CHARGEMENT VERS L'ENTREPÔT")
    debut = time.time()

    from src.load.load_warehouse import charger_pipeline_complet

    # Séparation staging et marts
    cles_staging = {"clients", "transactions", "produits", "magasins"}
    donnees_staging = {k: v for k, v in donnees_transformees.items() if k in cles_staging}
    donnees_marts = {k: v for k, v in donnees_transformees.items() if k not in cles_staging and not k.endswith("_rejetees")}

    succes, rapport = charger_pipeline_complet(
        donnees_staging=donnees_staging,
        donnees_marts=donnees_marts,
        url_connexion=url_entrepot,
    )

    duree = time.time() - debut

    if succes:
        logger.info(f"CHARGEMENT terminé avec succès en {duree:.2f}s")
        if not rapport.empty:
            logger.info(f"\nRapport de chargement :\n{rapport.to_string()}")
    else:
        logger.error(f"CHARGEMENT échoué après {duree:.2f}s")

    return succes


def afficher_resume(
    donnees_brutes: dict,
    donnees_transformees: dict,
    succes_chargement: bool,
    duree_totale: float,
):
    """Affiche un résumé final de l'exécution du pipeline."""
    print("\n" + "=" * 65)
    print("RÉSUMÉ DE L'EXÉCUTION DU PIPELINE")
    print("=" * 65)

    print("\n1. EXTRACTION :")
    for nom, df in donnees_brutes.items():
        print(f"   {nom:30s} : {len(df):>6,} enregistrements")

    print("\n2. TRANSFORMATION :")
    for nom, df in donnees_transformees.items():
        print(f"   {nom:30s} : {len(df):>6,} enregistrements")

    print(f"\n3. CHARGEMENT : {'SUCCÈS' if succes_chargement else 'ÉCHEC'}")

    print(f"\nDurée totale : {duree_totale:.2f} secondes")
    print("=" * 65 + "\n")


def main():
    """Point d'entrée principal."""
    afficher_banniere()

    parser = argparse.ArgumentParser(
        description="Lancement du pipeline ETL Commerce Congo"
    )
    parser.add_argument(
        "--etape",
        choices=["all", "extract", "transform", "load"],
        default="all",
        help="Étape à exécuter (défaut: all)",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="URL de connexion à l'entrepôt PostgreSQL (ex: postgresql://user:pass@host:5432/db)",
    )
    parser.add_argument(
        "--source-db-url",
        default=None,
        help="URL de connexion à la base source ERP",
    )
    args = parser.parse_args()

    debut_global = time.time()
    logger.info(f"Démarrage du pipeline — étape : {args.etape}")

    donnees_brutes = {}
    donnees_transformees = {}
    succes = True

    try:
        if args.etape in ("all", "extract"):
            donnees_brutes = etape_extract(args)

        if args.etape in ("all", "transform"):
            if not donnees_brutes and args.etape == "transform":
                # Si on exécute seulement transform, charger depuis les CSV
                from src.extract.extract_csv import extraire_toutes_sources
                donnees_brutes = extraire_toutes_sources()
            donnees_transformees = etape_transform(donnees_brutes)

        if args.etape in ("all", "load"):
            if not donnees_transformees and args.etape == "load":
                logger.error("Aucune donnée transformée disponible pour le chargement")
                sys.exit(1)
            url_entrepot = args.db_url or os.getenv("WAREHOUSE_DB_URL")
            succes = etape_load(donnees_transformees, url_entrepot)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur critique dans le pipeline : {e}", exc_info=True)
        succes = False
        sys.exit(1)

    duree_totale = time.time() - debut_global

    if args.etape == "all":
        afficher_resume(donnees_brutes, donnees_transformees, succes, duree_totale)

    sys.exit(0 if succes else 1)


if __name__ == "__main__":
    main()
