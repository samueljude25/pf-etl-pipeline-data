"""
Module de nettoyage et standardisation des données clients.
Opérations : déduplication, normalisation des textes, enrichissement,
             standardisation des formats téléphone et email.
"""

import re
from typing import Dict, Tuple

import pandas as pd

from src.utils.logger import get_logger, log_debut_etape, log_fin_etape
from src.utils.validators import valider_email

logger = get_logger("transform.clean_clients")


# ─── Règles de normalisation ─────────────────────────────────────────────────

MAPPING_VILLES = {
    "brazzaville": "Brazzaville",
    "brazza": "Brazzaville",
    "bzv": "Brazzaville",
    "pointe-noire": "Pointe-Noire",
    "pointenoire": "Pointe-Noire",
    "pnr": "Pointe-Noire",
    "dolisie": "Dolisie",
    "loubomo": "Dolisie",  # Ancien nom colonial
    "ouesso": "Ouesso",
    "nkayi": "Nkayi",
    "impfondo": "Impfondo",
}

MAPPING_SEXE = {
    "m": "M",
    "h": "M",
    "homme": "M",
    "masculin": "M",
    "f": "F",
    "femme": "F",
    "féminin": "F",
    "feminin": "F",
}

MAPPING_CATEGORIE = {
    "standard": "Standard",
    "std": "Standard",
    "normal": "Standard",
    "premium": "Premium",
    "prem": "Premium",
    "vip": "VIP",
    "very important": "VIP",
}


def normaliser_nom(texte: str) -> str:
    """
    Normalise un nom ou prénom en majuscules accentuées correctes.

    Args:
        texte: Texte à normaliser

    Returns:
        Nom normalisé en majuscules
    """
    if pd.isna(texte) or not isinstance(texte, str):
        return ""
    # Suppression des espaces superflus et mise en majuscules
    return texte.strip().upper()


def normaliser_prenom(texte: str) -> str:
    """
    Normalise un prénom avec première lettre en majuscule.

    Args:
        texte: Prénom à normaliser

    Returns:
        Prénom normalisé (ex: "jean-baptiste" → "Jean-Baptiste")
    """
    if pd.isna(texte) or not isinstance(texte, str):
        return ""
    # Gestion des prénoms composés avec tiret
    parties = texte.strip().split("-")
    return "-".join(p.strip().capitalize() for p in parties)


def normaliser_telephone(telephone: str) -> str:
    """
    Standardise un numéro de téléphone au format international Congo.
    Format cible : +242 0X XXX XXXX

    Args:
        telephone: Numéro brut à normaliser

    Returns:
        Numéro au format standard ou chaîne vide si invalide
    """
    if pd.isna(telephone) or not isinstance(telephone, str):
        return ""

    # Suppression de tous les caractères non numériques sauf '+'
    chiffres = re.sub(r"[^\d+]", "", telephone.strip())

    # Normalisation vers le format international
    if chiffres.startswith("+242"):
        numero = chiffres[4:]
    elif chiffres.startswith("242"):
        numero = chiffres[3:]
    elif chiffres.startswith("0"):
        numero = chiffres
    else:
        return telephone.strip()

    # Formatage : +242 0X XXX XXXX
    if len(numero) == 9 and numero.startswith("0"):
        return f"+242 {numero[0:2]} {numero[2:5]} {numero[5:]}"
    elif len(numero) == 8:
        return f"+242 0{numero[0]} {numero[1:4]} {numero[4:]}"

    return telephone.strip()


def standardiser_email(email: str) -> str:
    """
    Standardise une adresse email en minuscules.

    Args:
        email: Email brut

    Returns:
        Email normalisé ou chaîne vide si invalide
    """
    if pd.isna(email) or not isinstance(email, str):
        return ""
    email_nettoye = email.strip().lower()
    return email_nettoye if valider_email(email_nettoye) else ""


def deduplication_clients(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Supprime les doublons dans le DataFrame clients.
    Stratégie : conserver le dernier enregistrement par client_id.
    Un doublon plus subtil est détecté via email + nom.

    Args:
        df: DataFrame brut des clients

    Returns:
        Tuple (DataFrame dédupliqué, nombre de doublons supprimés)
    """
    nb_avant = len(df)

    # Déduplication par client_id (stricte)
    df = df.drop_duplicates(subset=["client_id"], keep="last")

    # Déduplication par email + nom (doublons sémantiques)
    if "email" in df.columns and "nom" in df.columns:
        masque_email_valide = df["email"].str.len() > 0
        df_avec_email = df[masque_email_valide].copy()
        df_sans_email = df[~masque_email_valide].copy()

        df_avec_email = df_avec_email.drop_duplicates(
            subset=["email"], keep="last"
        )
        df = pd.concat([df_avec_email, df_sans_email], ignore_index=True)

    nb_apres = len(df)
    nb_supprimes = nb_avant - nb_apres

    if nb_supprimes > 0:
        logger.warning(f"{nb_supprimes} doublons clients supprimés")

    return df, nb_supprimes


def enrichir_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrichit le DataFrame clients avec des colonnes dérivées.
    - Ancienneté client en jours
    - Segment d'ancienneté (Nouveau / Fidèle / Très fidèle)
    - Indicateur Premium/VIP

    Args:
        df: DataFrame clients nettoyé

    Returns:
        DataFrame enrichi
    """
    today = pd.Timestamp.now().normalize()

    if "date_inscription" in df.columns:
        df["date_inscription"] = pd.to_datetime(df["date_inscription"], errors="coerce")
        df["anciennete_jours"] = (today - df["date_inscription"]).dt.days.fillna(0).astype(int)
        df["segment_anciennete"] = pd.cut(
            df["anciennete_jours"],
            bins=[0, 90, 365, float("inf")],
            labels=["Nouveau", "Fidèle", "Très fidèle"],
            right=True,
        ).astype(str)

    # Indicateur client premium ou VIP
    df["est_premium"] = df["categorie"].isin(["Premium", "VIP"])

    # Région (basé sur la ville)
    mapping_region = {
        "Brazzaville": "Pool",
        "Pointe-Noire": "Kouilou",
        "Dolisie": "Niari",
        "Ouesso": "Sangha",
        "Nkayi": "Bouenza",
        "Impfondo": "Likouala",
    }
    df["region"] = df["ville"].map(mapping_region).fillna("Autre")

    return df


def nettoyer_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline complet de nettoyage des données clients.

    Étapes :
    1. Normalisation des chaînes de texte
    2. Standardisation des codes (ville, sexe, catégorie)
    3. Déduplication
    4. Enrichissement avec colonnes dérivées
    5. Suppression des lignes sans client_id

    Args:
        df: DataFrame brut issu de l'extraction CSV

    Returns:
        DataFrame clients propre et enrichi
    """
    log_debut_etape(logger, "Nettoyage clients")
    nb_initial = len(df)

    # ─ Étape 1 : Normalisation des textes ─────────────────────────
    df["nom"] = df["nom"].apply(normaliser_nom)
    df["prenom"] = df["prenom"].apply(normaliser_prenom)
    df["email"] = df["email"].apply(standardiser_email)
    df["telephone"] = df["telephone"].apply(normaliser_telephone)

    # ─ Étape 2 : Standardisation des codes ────────────────────────
    df["ville"] = (
        df["ville"].str.strip().str.lower().map(MAPPING_VILLES).fillna(df["ville"])
    )
    df["sexe"] = (
        df["sexe"].str.strip().str.lower().map(MAPPING_SEXE).fillna("Inconnu")
    )
    df["categorie"] = (
        df["categorie"].str.strip().str.lower().map(MAPPING_CATEGORIE).fillna("Standard")
    )

    # ─ Étape 3 : Déduplication ────────────────────────────────────
    df, nb_doublons = deduplication_clients(df)

    # ─ Étape 4 : Suppression des lignes invalides ──────────────────
    masque_valide = df["client_id"].notna() & (df["client_id"] != "") & (df["nom"] != "")
    nb_invalides = (~masque_valide).sum()
    if nb_invalides > 0:
        logger.warning(f"{nb_invalides} clients sans client_id ou nom supprimés")
    df = df[masque_valide].reset_index(drop=True)

    # ─ Étape 5 : Enrichissement ───────────────────────────────────
    df = enrichir_clients(df)

    # Ordonnancement final
    df = df.sort_values("client_id").reset_index(drop=True)

    log_fin_etape(logger, "Nettoyage clients", len(df))
    logger.info(
        f"Résumé : {nb_initial} → {len(df)} clients "
        f"({nb_doublons} doublons, {nb_invalides} invalides supprimés)"
    )

    return df
