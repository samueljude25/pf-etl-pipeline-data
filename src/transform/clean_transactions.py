"""
Module de nettoyage et enrichissement des transactions.
Opérations : validation des montants, jointures avec dimensions,
             détection des anomalies, enrichissement temporel.
"""

from typing import Dict, Optional, Tuple

import pandas as pd
import numpy as np

from src.utils.logger import get_logger, log_debut_etape, log_fin_etape
from src.utils.validators import valider_montant, MONTANT_MAX_XAF, MONTANT_MIN_XAF

logger = get_logger("transform.clean_transactions")


# ─── Seuils de détection d'anomalies ────────────────────────────────────────

SEUIL_QUANTITE_MAX = 500        # Quantité maximum par transaction
SEUIL_ANOMALIE_ZSCORE = 3.0     # Z-score pour la détection des outliers


def valider_coherence_montant(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie la cohérence entre quantité, prix unitaire et montant total.
    Corrige les montants recalculables et marque les incohérences.

    Args:
        df: DataFrame des transactions

    Returns:
        DataFrame avec colonne 'montant_coherent' ajoutée
    """
    # Recalcul du montant attendu
    df["montant_calcule_xaf"] = df["quantite"] * df["prix_unitaire_xaf"]
    df["ecart_montant_xaf"] = (df["montant_total_xaf"] - df["montant_calcule_xaf"]).abs()

    # Tolérance de 1 XAF pour les arrondis
    df["montant_coherent"] = df["ecart_montant_xaf"] <= 1.0

    nb_incoherents = (~df["montant_coherent"]).sum()
    if nb_incoherents > 0:
        logger.warning(
            f"{nb_incoherents} transactions avec montant incohérent détectées. "
            "Les montants seront recalculés."
        )
        # Correction automatique : recalcul du montant total
        masque_correction = ~df["montant_coherent"]
        df.loc[masque_correction, "montant_total_xaf"] = df.loc[
            masque_correction, "montant_calcule_xaf"
        ]

    return df


def detecter_outliers_montants(df: pd.DataFrame) -> pd.DataFrame:
    """
    Détecte les transactions avec des montants aberrants par la méthode IQR.
    Les outliers sont marqués mais pas supprimés (décision métier).

    Args:
        df: DataFrame des transactions

    Returns:
        DataFrame avec colonne 'est_outlier' ajoutée
    """
    q1 = df["montant_total_xaf"].quantile(0.25)
    q3 = df["montant_total_xaf"].quantile(0.75)
    iqr = q3 - q1
    borne_inf = q1 - 1.5 * iqr
    borne_sup = q3 + 1.5 * iqr

    df["est_outlier"] = (
        (df["montant_total_xaf"] < borne_inf) |
        (df["montant_total_xaf"] > borne_sup)
    )

    nb_outliers = df["est_outlier"].sum()
    logger.info(
        f"Détection outliers : Q1={q1:,.0f} XAF, Q3={q3:,.0f} XAF, "
        f"IQR={iqr:,.0f} → {nb_outliers} outliers marqués"
    )
    return df


def enrichir_dimensions_temporelles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrichit les transactions avec des dimensions temporelles dérivées.
    Utiles pour les agrégations dans le data warehouse.

    Args:
        df: DataFrame avec colonne 'date_transaction'

    Returns:
        DataFrame enrichi avec dimensions temporelles
    """
    df["date_transaction"] = pd.to_datetime(df["date_transaction"], errors="coerce")

    # Extraction des composantes temporelles
    df["annee"] = df["date_transaction"].dt.year
    df["mois"] = df["date_transaction"].dt.month
    df["trimestre"] = df["date_transaction"].dt.quarter
    df["semaine"] = df["date_transaction"].dt.isocalendar().week.astype(int)
    df["jour_semaine"] = df["date_transaction"].dt.day_name()
    df["est_weekend"] = df["date_transaction"].dt.dayofweek >= 5
    df["mois_annee"] = df["date_transaction"].dt.to_period("M").astype(str)

    # Libellé du mois en français
    MOIS_FR = {
        1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
        5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
        9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre",
    }
    df["nom_mois"] = df["mois"].map(MOIS_FR)

    return df


def joindre_dimensions(
    df_transactions: pd.DataFrame,
    df_clients: Optional[pd.DataFrame] = None,
    df_produits: Optional[pd.DataFrame] = None,
    df_magasins: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Enrichit les transactions avec les données des tables de dimension.
    Effectue des jointures LEFT JOIN pour préserver toutes les transactions.

    Args:
        df_transactions: DataFrame des transactions nettoyées
        df_clients: DataFrame des clients (optionnel)
        df_produits: DataFrame des produits (optionnel)
        df_magasins: DataFrame des magasins (optionnel)

    Returns:
        DataFrame des transactions enrichi avec les dimensions
    """
    df = df_transactions.copy()

    # Jointure avec les clients
    if df_clients is not None and not df_clients.empty:
        colonnes_client = ["client_id", "ville", "categorie", "sexe", "region"]
        colonnes_dispo = [c for c in colonnes_client if c in df_clients.columns]
        df_clients_slim = df_clients[colonnes_dispo].rename(columns={
            "ville": "ville_client",
            "categorie": "categorie_client",
        })
        df = df.merge(df_clients_slim, on="client_id", how="left")
        logger.info("Jointure transactions ↔ clients effectuée")

    # Jointure avec les produits
    if df_produits is not None and not df_produits.empty:
        colonnes_produit = ["produit_id", "nom_produit", "categorie", "sous_categorie"]
        colonnes_dispo = [c for c in colonnes_produit if c in df_produits.columns]
        df_produits_slim = df_produits[colonnes_dispo].rename(columns={
            "categorie": "categorie_produit",
        })
        df = df.merge(df_produits_slim, on="produit_id", how="left")
        logger.info("Jointure transactions ↔ produits effectuée")

    # Jointure avec les magasins
    if df_magasins is not None and not df_magasins.empty:
        colonnes_magasin = ["magasin_id", "nom_magasin", "ville"]
        colonnes_dispo = [c for c in colonnes_magasin if c in df_magasins.columns]
        df_magasins_slim = df_magasins[colonnes_dispo].rename(columns={
            "ville": "ville_magasin",
        })
        df = df.merge(df_magasins_slim, on="magasin_id", how="left")
        logger.info("Jointure transactions ↔ magasins effectuée")

    return df


def nettoyer_transactions(
    df: pd.DataFrame,
    df_clients: Optional[pd.DataFrame] = None,
    df_produits: Optional[pd.DataFrame] = None,
    df_magasins: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pipeline complet de nettoyage et enrichissement des transactions.

    Étapes :
    1. Suppression des lignes avec transaction_id manquant
    2. Validation et correction des montants
    3. Détection des outliers
    4. Enrichissement temporel
    5. Jointures avec les dimensions
    6. Séparation des transactions valides / rejetées

    Args:
        df: DataFrame brut des transactions
        df_clients: DataFrame clients nettoyé (pour les jointures)
        df_produits: DataFrame produits nettoyé (pour les jointures)
        df_magasins: DataFrame magasins nettoyé (pour les jointures)

    Returns:
        Tuple (df_valide, df_rejete) — transactions acceptées et rejetées
    """
    log_debut_etape(logger, "Nettoyage transactions")
    nb_initial = len(df)

    # ─ Étape 1 : Nettoyage basique ────────────────────────────────
    # Suppression des lignes sans identifiant
    masque_id = df["transaction_id"].notna() & (df["transaction_id"] != "")
    df_rejete = df[~masque_id].copy()
    df_rejete["motif_rejet"] = "transaction_id manquant"
    df = df[masque_id].copy()

    # Déduplication sur transaction_id
    nb_avant_dedup = len(df)
    df = df.drop_duplicates(subset=["transaction_id"], keep="last")
    if len(df) < nb_avant_dedup:
        logger.warning(f"{nb_avant_dedup - len(df)} transactions dupliquées supprimées")

    # ─ Étape 2 : Validation des montants ──────────────────────────
    df = valider_coherence_montant(df)

    # Rejet des montants hors plage
    masque_montant = (
        df["montant_total_xaf"].between(MONTANT_MIN_XAF, MONTANT_MAX_XAF) &
        df["montant_total_xaf"].notna()
    )
    rejets_montant = df[~masque_montant].copy()
    rejets_montant["motif_rejet"] = "Montant hors plage acceptable"
    df_rejete = pd.concat([df_rejete, rejets_montant], ignore_index=True)
    df = df[masque_montant].copy()

    logger.info(f"{len(rejets_montant)} transactions rejetées (montant invalide)")

    # Rejet des quantités négatives ou nulles
    masque_qte = df["quantite"] > 0
    rejets_qte = df[~masque_qte].copy()
    rejets_qte["motif_rejet"] = "Quantité nulle ou négative"
    df_rejete = pd.concat([df_rejete, rejets_qte], ignore_index=True)
    df = df[masque_qte].copy()

    # ─ Étape 3 : Détection des outliers ───────────────────────────
    df = detecter_outliers_montants(df)

    # ─ Étape 4 : Enrichissement temporel ──────────────────────────
    df = enrichir_dimensions_temporelles(df)

    # ─ Étape 5 : Jointures avec les dimensions ────────────────────
    df = joindre_dimensions(df, df_clients, df_produits, df_magasins)

    # ─ Ordonnancement final ────────────────────────────────────────
    df = df.sort_values("date_transaction").reset_index(drop=True)

    log_fin_etape(logger, "Nettoyage transactions", len(df))
    logger.info(
        f"Résumé : {nb_initial} → {len(df)} valides, "
        f"{len(df_rejete)} rejetées"
    )

    return df, df_rejete
