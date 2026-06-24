"""
Module d'extraction des fichiers CSV sources.
Lit, valide et pré-qualifie les données brutes depuis le dossier data/raw/.
"""

import os
from typing import Dict, Optional

import pandas as pd

from src.utils.logger import get_logger, log_debut_etape, log_fin_etape
from src.utils.validators import generer_rapport_qualite

logger = get_logger("extract.csv")

# Chemin vers les données brutes (relatif à la racine du projet)
DOSSIER_RAW = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")


# ─── Schémas de colonnes attendues ──────────────────────────────────────────

SCHEMA_CLIENTS = {
    "client_id": str,
    "nom": str,
    "prenom": str,
    "email": str,
    "telephone": str,
    "ville": str,
    "quartier": str,
    "date_inscription": str,
    "sexe": str,
    "categorie": str,
}

SCHEMA_TRANSACTIONS = {
    "transaction_id": str,
    "date_transaction": str,
    "client_id": str,
    "magasin_id": str,
    "produit_id": str,
    "quantite": int,
    "prix_unitaire_xaf": float,
    "montant_total_xaf": float,
    "mode_paiement": str,
    "statut": str,
    "canal_vente": str,
}

SCHEMA_PRODUITS = {
    "produit_id": str,
    "nom_produit": str,
    "categorie": str,
    "sous_categorie": str,
    "prix_unitaire_xaf": float,
    "stock_initial": int,
    "fournisseur": str,
    "pays_origine": str,
}

SCHEMA_MAGASINS = {
    "magasin_id": str,
    "nom_magasin": str,
    "ville": str,
    "quartier": str,
    "adresse": str,
    "responsable": str,
    "telephone": str,
    "surface_m2": int,
    "date_ouverture": str,
    "statut": str,
}


def lire_csv(
    nom_fichier: str,
    schema: Dict[str, type],
    encodage: str = "utf-8",
    separateur: str = ",",
) -> pd.DataFrame:
    """
    Lit un fichier CSV et applique le schéma de typage défini.

    Args:
        nom_fichier: Nom du fichier CSV (sans chemin)
        schema: Dictionnaire {colonne: type} pour la validation
        encodage: Encodage du fichier (défaut: utf-8)
        separateur: Séparateur de colonnes (défaut: virgule)

    Returns:
        DataFrame pandas avec les données du fichier

    Raises:
        FileNotFoundError: Si le fichier n'existe pas
        ValueError: Si des colonnes obligatoires sont manquantes
    """
    chemin = os.path.join(DOSSIER_RAW, nom_fichier)

    # Vérification de l'existence du fichier
    if not os.path.exists(chemin):
        logger.error(f"Fichier introuvable : {chemin}")
        raise FileNotFoundError(f"Fichier source manquant : {chemin}")

    logger.info(f"Lecture du fichier : {nom_fichier}")

    # Lecture du CSV
    df = pd.read_csv(chemin, encoding=encodage, sep=separateur, dtype=str)
    logger.info(f"  → {len(df):,} lignes lues, {len(df.columns)} colonnes")

    # Vérification des colonnes obligatoires
    colonnes_manquantes = set(schema.keys()) - set(df.columns)
    if colonnes_manquantes:
        logger.error(f"Colonnes manquantes : {colonnes_manquantes}")
        raise ValueError(f"Colonnes obligatoires manquantes : {colonnes_manquantes}")

    # Application du typage avec gestion des erreurs
    for colonne, type_attendu in schema.items():
        if colonne not in df.columns:
            continue
        try:
            if type_attendu == int:
                df[colonne] = pd.to_numeric(df[colonne], errors="coerce").fillna(0).astype(int)
            elif type_attendu == float:
                df[colonne] = pd.to_numeric(df[colonne], errors="coerce")
            else:
                df[colonne] = df[colonne].astype(str).str.strip()
        except Exception as e:
            logger.warning(f"  Problème de typage sur '{colonne}' : {e}")

    logger.info(f"  → Schéma appliqué avec succès")
    return df


def extraire_clients() -> pd.DataFrame:
    """
    Extrait les données clients depuis clients.csv.

    Returns:
        DataFrame des clients avec schéma validé
    """
    log_debut_etape(logger, "Extraction clients CSV")
    df = lire_csv("clients.csv", SCHEMA_CLIENTS)

    # Rapport de qualité
    rapport = generer_rapport_qualite(df, "clients")
    logger.info(f"Score qualité clients : {rapport['score_qualite']}%")

    log_fin_etape(logger, "Extraction clients CSV", len(df))
    return df


def extraire_transactions() -> pd.DataFrame:
    """
    Extrait les données de transactions depuis transactions.csv.

    Returns:
        DataFrame des transactions avec schéma validé
    """
    log_debut_etape(logger, "Extraction transactions CSV")
    df = lire_csv("transactions.csv", SCHEMA_TRANSACTIONS)

    # Conversion des colonnes de dates
    df["date_transaction"] = pd.to_datetime(df["date_transaction"], errors="coerce")

    # Conversion des colonnes numériques
    df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce").fillna(0).astype(int)
    df["prix_unitaire_xaf"] = pd.to_numeric(df["prix_unitaire_xaf"], errors="coerce")
    df["montant_total_xaf"] = pd.to_numeric(df["montant_total_xaf"], errors="coerce")

    # Rapport de qualité
    rapport = generer_rapport_qualite(df, "transactions")
    logger.info(f"Score qualité transactions : {rapport['score_qualite']}%")

    log_fin_etape(logger, "Extraction transactions CSV", len(df))
    return df


def extraire_produits() -> pd.DataFrame:
    """
    Extrait les données produits depuis produits.csv.

    Returns:
        DataFrame des produits avec schéma validé
    """
    log_debut_etape(logger, "Extraction produits CSV")
    df = lire_csv("produits.csv", SCHEMA_PRODUITS)

    # Conversion des colonnes numériques
    df["prix_unitaire_xaf"] = pd.to_numeric(df["prix_unitaire_xaf"], errors="coerce")
    df["stock_initial"] = pd.to_numeric(df["stock_initial"], errors="coerce").fillna(0).astype(int)

    rapport = generer_rapport_qualite(df, "produits")
    logger.info(f"Score qualité produits : {rapport['score_qualite']}%")

    log_fin_etape(logger, "Extraction produits CSV", len(df))
    return df


def extraire_magasins() -> pd.DataFrame:
    """
    Extrait les données des magasins depuis magasins.csv.

    Returns:
        DataFrame des magasins avec schéma validé
    """
    log_debut_etape(logger, "Extraction magasins CSV")
    df = lire_csv("magasins.csv", SCHEMA_MAGASINS)

    # Conversion surface et date
    df["surface_m2"] = pd.to_numeric(df["surface_m2"], errors="coerce").fillna(0).astype(int)
    df["date_ouverture"] = pd.to_datetime(df["date_ouverture"], errors="coerce")

    rapport = generer_rapport_qualite(df, "magasins")
    logger.info(f"Score qualité magasins : {rapport['score_qualite']}%")

    log_fin_etape(logger, "Extraction magasins CSV", len(df))
    return df


def extraire_toutes_sources() -> Dict[str, pd.DataFrame]:
    """
    Extrait toutes les sources CSV en une seule opération.

    Returns:
        Dictionnaire {nom_source: DataFrame}
    """
    logger.info("Démarrage de l'extraction complète des fichiers CSV")

    sources = {}
    try:
        sources["clients"] = extraire_clients()
        sources["transactions"] = extraire_transactions()
        sources["produits"] = extraire_produits()
        sources["magasins"] = extraire_magasins()
        logger.info(
            f"Extraction CSV terminée : "
            f"{len(sources)} sources extraites avec succès"
        )
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction CSV : {e}", exc_info=True)
        raise

    return sources
