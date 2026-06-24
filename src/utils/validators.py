"""
Fonctions de validation des données du pipeline ETL.
Vérifie la conformité des données avant traitement.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("utils.validators")


# ─── Constantes de validation ──────────────────────────────────────────────

MONTANT_MIN_XAF = 500       # Montant minimum acceptable en XAF
MONTANT_MAX_XAF = 5_000_000  # Montant maximum acceptable en XAF

VILLES_CONGO = [
    "Brazzaville", "Pointe-Noire", "Dolisie", "Ouesso",
    "Nkayi", "Impfondo", "Sibiti", "Madingou", "Kinkala",
]

MODES_PAIEMENT_VALIDES = [
    "Espèces", "Mobile Money", "Carte bancaire", "Virement",
]

CATEGORIES_CLIENTS = ["Standard", "Premium", "VIP"]

STATUTS_TRANSACTION = ["Validé", "Annulé", "En attente", "Remboursé"]


# ─── Validation des clients ─────────────────────────────────────────────────

def valider_email(email: str) -> bool:
    """
    Vérifie qu'une adresse email est valide.

    Args:
        email: Chaîne à valider

    Returns:
        True si l'email est valide, False sinon
    """
    if not isinstance(email, str) or not email.strip():
        return False
    pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email.strip()))


def valider_telephone_congo(telephone: str) -> bool:
    """
    Vérifie qu'un numéro de téléphone est au format congolais.
    Format attendu : +242 XX XXX XXXX ou 06 XXX XXXX

    Args:
        telephone: Numéro à valider

    Returns:
        True si valide, False sinon
    """
    if not isinstance(telephone, str):
        return False
    # Suppression des espaces pour la vérification
    numero_nettoye = telephone.replace(" ", "").replace("-", "")
    # Formats acceptés : +2420XXXXXXXX ou 0XXXXXXXXX
    pattern = r"^(\+242)?0[456]\d{7,8}$"
    return bool(re.match(pattern, numero_nettoye))


def valider_client(ligne: pd.Series) -> Tuple[bool, List[str]]:
    """
    Valide toutes les colonnes d'un enregistrement client.

    Args:
        ligne: Ligne du DataFrame clients

    Returns:
        Tuple (is_valide, liste_erreurs)
    """
    erreurs = []

    # Vérification client_id
    if pd.isna(ligne.get("client_id")) or not str(ligne["client_id"]).startswith("C"):
        erreurs.append("client_id manquant ou invalide")

    # Vérification nom et prénom
    if pd.isna(ligne.get("nom")) or len(str(ligne["nom"])) < 2:
        erreurs.append("nom manquant ou trop court")

    # Vérification email
    if not valider_email(str(ligne.get("email", ""))):
        erreurs.append(f"email invalide : {ligne.get('email')}")

    # Vérification ville
    if ligne.get("ville") not in VILLES_CONGO:
        erreurs.append(f"ville non reconnue : {ligne.get('ville')}")

    # Vérification catégorie
    if ligne.get("categorie") not in CATEGORIES_CLIENTS:
        erreurs.append(f"catégorie invalide : {ligne.get('categorie')}")

    return len(erreurs) == 0, erreurs


# ─── Validation des transactions ────────────────────────────────────────────

def valider_montant(montant: Any) -> bool:
    """
    Vérifie qu'un montant en XAF est dans une plage acceptable.

    Args:
        montant: Valeur à vérifier

    Returns:
        True si le montant est valide
    """
    try:
        valeur = float(montant)
        return MONTANT_MIN_XAF <= valeur <= MONTANT_MAX_XAF
    except (TypeError, ValueError):
        return False


def valider_transaction(ligne: pd.Series) -> Tuple[bool, List[str]]:
    """
    Valide toutes les colonnes d'une transaction.

    Args:
        ligne: Ligne du DataFrame transactions

    Returns:
        Tuple (is_valide, liste_erreurs)
    """
    erreurs = []

    # Vérification transaction_id
    if pd.isna(ligne.get("transaction_id")):
        erreurs.append("transaction_id manquant")

    # Vérification date
    try:
        pd.to_datetime(ligne["date_transaction"])
    except Exception:
        erreurs.append(f"date invalide : {ligne.get('date_transaction')}")

    # Vérification montant total
    if not valider_montant(ligne.get("montant_total_xaf")):
        erreurs.append(
            f"montant hors plage : {ligne.get('montant_total_xaf')} XAF "
            f"(attendu entre {MONTANT_MIN_XAF:,} et {MONTANT_MAX_XAF:,})"
        )

    # Cohérence quantité × prix = montant
    try:
        montant_calcule = float(ligne["quantite"]) * float(ligne["prix_unitaire_xaf"])
        montant_declare = float(ligne["montant_total_xaf"])
        if abs(montant_calcule - montant_declare) > 1:  # tolérance 1 XAF
            erreurs.append(
                f"incohérence montant : qté×prix={montant_calcule:,.0f} ≠ "
                f"déclaré={montant_declare:,.0f}"
            )
    except (TypeError, ValueError, KeyError):
        erreurs.append("impossible de vérifier la cohérence quantité/prix/montant")

    # Vérification mode de paiement
    if ligne.get("mode_paiement") not in MODES_PAIEMENT_VALIDES:
        erreurs.append(f"mode de paiement inconnu : {ligne.get('mode_paiement')}")

    return len(erreurs) == 0, erreurs


# ─── Rapport de qualité ─────────────────────────────────────────────────────

def generer_rapport_qualite(
    df: pd.DataFrame,
    nom_dataset: str,
    fonction_validation=None,
) -> Dict[str, Any]:
    """
    Génère un rapport de qualité des données pour un DataFrame.

    Args:
        df: DataFrame à analyser
        nom_dataset: Nom du jeu de données (pour les logs)
        fonction_validation: Fonction de validation ligne par ligne (optionnel)

    Returns:
        Dictionnaire contenant les métriques de qualité
    """
    logger.info(f"Génération du rapport qualité pour : {nom_dataset}")

    rapport = {
        "dataset": nom_dataset,
        "nb_lignes_total": len(df),
        "nb_colonnes": len(df.columns),
        "valeurs_nulles": {},
        "doublons": 0,
        "taux_completude": {},
        "lignes_invalides": 0,
        "erreurs_details": [],
    }

    # Analyse des valeurs nulles par colonne
    for col in df.columns:
        nb_nulls = df[col].isna().sum()
        taux = round((1 - nb_nulls / len(df)) * 100, 2) if len(df) > 0 else 0
        rapport["valeurs_nulles"][col] = int(nb_nulls)
        rapport["taux_completude"][col] = taux
        if nb_nulls > 0:
            logger.warning(f"  {col} : {nb_nulls} valeurs nulles ({100-taux:.1f}%)")

    # Détection des doublons
    rapport["doublons"] = int(df.duplicated().sum())
    if rapport["doublons"] > 0:
        logger.warning(f"  {rapport['doublons']} lignes dupliquées détectées")

    # Validation ligne par ligne (si fonction fournie)
    if fonction_validation is not None:
        nb_invalides = 0
        for _, ligne in df.iterrows():
            est_valide, erreurs = fonction_validation(ligne)
            if not est_valide:
                nb_invalides += 1
                rapport["erreurs_details"].extend(erreurs[:3])  # max 3 erreurs/ligne
        rapport["lignes_invalides"] = nb_invalides

    # Calcul du score global de qualité
    taux_moyen = sum(rapport["taux_completude"].values()) / len(rapport["taux_completude"])
    taux_doublons = (1 - rapport["doublons"] / max(len(df), 1)) * 100
    rapport["score_qualite"] = round((taux_moyen + taux_doublons) / 2, 2)

    logger.info(
        f"Rapport qualité {nom_dataset} : "
        f"score={rapport['score_qualite']}%, "
        f"doublons={rapport['doublons']}, "
        f"invalides={rapport['lignes_invalides']}"
    )

    return rapport
