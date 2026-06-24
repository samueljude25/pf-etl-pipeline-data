"""
Module d'agrégation des données du pipeline ETL.
Calcule les métriques métier : CA mensuel, panier moyen, top produits,
performance magasins, segmentation clients.
"""

from typing import Dict, Optional

import pandas as pd
import numpy as np

from src.utils.logger import get_logger, log_debut_etape, log_fin_etape

logger = get_logger("transform.aggregate")


def calculer_ca_mensuel(df_transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le chiffre d'affaires mensuel par magasin et par ville.

    Args:
        df_transactions: DataFrame des transactions nettoyées et enrichies

    Returns:
        DataFrame du CA mensuel avec colonnes :
        [mois_annee, magasin_id, ville_magasin, nb_transactions,
         nb_clients_uniques, ca_total_xaf, ca_moyen_par_transaction_xaf]
    """
    log_debut_etape(logger, "Agrégation CA mensuel")

    colonnes_groupe = ["mois_annee", "annee", "mois"]

    # Ajouter ville_magasin si disponible
    if "ville_magasin" in df_transactions.columns:
        colonnes_groupe.append("ville_magasin")
    if "magasin_id" in df_transactions.columns:
        colonnes_groupe.append("magasin_id")

    df_ca = (
        df_transactions
        .groupby(colonnes_groupe, observed=True)
        .agg(
            nb_transactions=("transaction_id", "count"),
            nb_clients_uniques=("client_id", "nunique"),
            ca_total_xaf=("montant_total_xaf", "sum"),
            quantite_totale=("quantite", "sum"),
            montant_min_xaf=("montant_total_xaf", "min"),
            montant_max_xaf=("montant_total_xaf", "max"),
        )
        .reset_index()
    )

    # Métriques dérivées
    df_ca["ca_moyen_par_transaction_xaf"] = (
        df_ca["ca_total_xaf"] / df_ca["nb_transactions"]
    ).round(0)

    df_ca["ca_moyen_par_client_xaf"] = (
        df_ca["ca_total_xaf"] / df_ca["nb_clients_uniques"]
    ).round(0)

    # Tri chronologique
    df_ca = df_ca.sort_values(["annee", "mois", "ca_total_xaf"], ascending=[True, True, False])

    log_fin_etape(logger, "Agrégation CA mensuel", len(df_ca))
    logger.info(
        f"CA total sur la période : "
        f"{df_transactions['montant_total_xaf'].sum():,.0f} XAF"
    )
    return df_ca


def calculer_top_produits(
    df_transactions: pd.DataFrame, top_n: int = 20
) -> pd.DataFrame:
    """
    Identifie les produits les plus performants en CA et en volume.

    Args:
        df_transactions: DataFrame des transactions enrichies
        top_n: Nombre de produits à retourner (défaut: 20)

    Returns:
        DataFrame des top produits avec métriques de performance
    """
    log_debut_etape(logger, f"Agrégation Top {top_n} produits")

    colonnes_groupe = ["produit_id"]
    if "nom_produit" in df_transactions.columns:
        colonnes_groupe.append("nom_produit")
    if "categorie_produit" in df_transactions.columns:
        colonnes_groupe.append("categorie_produit")

    df_produits = (
        df_transactions
        .groupby(colonnes_groupe, observed=True)
        .agg(
            nb_ventes=("transaction_id", "count"),
            quantite_totale=("quantite", "sum"),
            ca_total_xaf=("montant_total_xaf", "sum"),
            nb_magasins=("magasin_id", "nunique") if "magasin_id" in df_transactions.columns else ("transaction_id", "count"),
            nb_clients=("client_id", "nunique"),
            prix_moyen_xaf=("prix_unitaire_xaf", "mean"),
        )
        .reset_index()
    )

    # Rang par CA
    df_produits["rang_ca"] = df_produits["ca_total_xaf"].rank(
        ascending=False, method="min"
    ).astype(int)

    # Part de marché (% du CA total)
    ca_total = df_produits["ca_total_xaf"].sum()
    df_produits["part_ca_pct"] = (
        df_produits["ca_total_xaf"] / ca_total * 100
    ).round(2)

    # Tri par CA décroissant
    df_produits = (
        df_produits
        .sort_values("ca_total_xaf", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    log_fin_etape(logger, f"Agrégation Top {top_n} produits", len(df_produits))
    return df_produits


def calculer_performance_magasins(df_transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Analyse la performance commerciale de chaque magasin.

    Args:
        df_transactions: DataFrame des transactions enrichies

    Returns:
        DataFrame de performance par magasin
    """
    log_debut_etape(logger, "Agrégation performance magasins")

    colonnes_groupe = ["magasin_id"]
    if "nom_magasin" in df_transactions.columns:
        colonnes_groupe.append("nom_magasin")
    if "ville_magasin" in df_transactions.columns:
        colonnes_groupe.append("ville_magasin")

    df_magasins = (
        df_transactions
        .groupby(colonnes_groupe, observed=True)
        .agg(
            nb_transactions=("transaction_id", "count"),
            nb_clients_uniques=("client_id", "nunique"),
            ca_total_xaf=("montant_total_xaf", "sum"),
            ca_moyen_xaf=("montant_total_xaf", "mean"),
            panier_moyen_xaf=("montant_total_xaf", "mean"),
            quantite_totale=("quantite", "sum"),
            nb_produits_vendus=("produit_id", "nunique") if "produit_id" in df_transactions.columns else ("transaction_id", "count"),
        )
        .reset_index()
    )

    # Rang par CA
    df_magasins["rang_ca"] = df_magasins["ca_total_xaf"].rank(
        ascending=False, method="min"
    ).astype(int)

    # CA moyen mensuel (approximation sur 24 mois = 2 ans)
    df_magasins["ca_mensuel_moyen_xaf"] = (df_magasins["ca_total_xaf"] / 24).round(0)

    # Arrondi du panier moyen
    df_magasins["panier_moyen_xaf"] = df_magasins["panier_moyen_xaf"].round(0)

    # Tri par CA décroissant
    df_magasins = df_magasins.sort_values("ca_total_xaf", ascending=False).reset_index(drop=True)

    log_fin_etape(logger, "Agrégation performance magasins", len(df_magasins))
    return df_magasins


def calculer_segmentation_clients(df_transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les métriques RFM (Récence, Fréquence, Montant) par client
    pour la segmentation comportementale.

    Args:
        df_transactions: DataFrame des transactions enrichies

    Returns:
        DataFrame RFM avec scores et segments
    """
    log_debut_etape(logger, "Agrégation segmentation clients RFM")

    date_reference = df_transactions["date_transaction"].max()

    # Calcul des métriques RFM
    df_rfm = (
        df_transactions
        .groupby("client_id")
        .agg(
            derniere_transaction=("date_transaction", "max"),
            nb_transactions=("transaction_id", "count"),
            ca_total_xaf=("montant_total_xaf", "sum"),
        )
        .reset_index()
    )

    # Récence en jours
    df_rfm["recence_jours"] = (
        date_reference - df_rfm["derniere_transaction"]
    ).dt.days

    # Panier moyen
    df_rfm["panier_moyen_xaf"] = (
        df_rfm["ca_total_xaf"] / df_rfm["nb_transactions"]
    ).round(0)

    # Scores RFM (quintiles 1-5, 5 = meilleur)
    def scorer_quintile(serie: pd.Series, inverse: bool = False) -> pd.Series:
        """Attribue un score de 1 à 5 par quintile."""
        labels = [5, 4, 3, 2, 1] if not inverse else [1, 2, 3, 4, 5]
        return pd.qcut(serie, q=5, labels=labels, duplicates="drop").astype(float).fillna(3)

    df_rfm["score_R"] = scorer_quintile(df_rfm["recence_jours"], inverse=True)
    df_rfm["score_F"] = scorer_quintile(df_rfm["nb_transactions"])
    df_rfm["score_M"] = scorer_quintile(df_rfm["ca_total_xaf"])
    df_rfm["score_RFM"] = (
        df_rfm["score_R"] * 100 + df_rfm["score_F"] * 10 + df_rfm["score_M"]
    )

    # Segmentation en 4 catégories
    def attribuer_segment(ligne) -> str:
        score_moyen = (ligne["score_R"] + ligne["score_F"] + ligne["score_M"]) / 3
        if score_moyen >= 4.0:
            return "Champions"
        elif score_moyen >= 3.0:
            return "Clients fidèles"
        elif ligne["score_R"] <= 2 and score_moyen >= 2.5:
            return "Clients à risque"
        else:
            return "Clients inactifs"

    df_rfm["segment_rfm"] = df_rfm.apply(attribuer_segment, axis=1)

    # Distribution des segments
    distribution = df_rfm["segment_rfm"].value_counts()
    for segment, nb in distribution.items():
        logger.info(f"  Segment '{segment}' : {nb} clients ({nb/len(df_rfm)*100:.1f}%)")

    df_rfm = df_rfm.sort_values("score_RFM", ascending=False).reset_index(drop=True)
    log_fin_etape(logger, "Agrégation segmentation clients RFM", len(df_rfm))
    return df_rfm


def calculer_toutes_agregations(
    df_transactions: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """
    Calcule toutes les agrégations métier en une seule opération.

    Args:
        df_transactions: DataFrame des transactions nettoyées et enrichies

    Returns:
        Dictionnaire {nom_agregat: DataFrame}
    """
    logger.info("Démarrage du calcul de toutes les agrégations")

    resultats = {}
    try:
        resultats["ca_mensuel"] = calculer_ca_mensuel(df_transactions)
        resultats["top_produits"] = calculer_top_produits(df_transactions)
        resultats["performance_magasins"] = calculer_performance_magasins(df_transactions)
        resultats["segmentation_rfm"] = calculer_segmentation_clients(df_transactions)

        logger.info(f"Agrégations terminées : {len(resultats)} tables calculées")
    except Exception as e:
        logger.error(f"Erreur lors des agrégations : {e}", exc_info=True)
        raise

    return resultats
