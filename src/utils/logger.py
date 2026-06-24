"""
Configuration du système de journalisation pour le pipeline ETL.
Fournit un logger centralisé avec rotation des fichiers de logs.
"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


def get_logger(nom_module: str, niveau: str = "INFO") -> logging.Logger:
    """
    Crée et configure un logger pour un module donné.

    Args:
        nom_module: Nom du module demandant le logger (ex: 'extract.csv')
        niveau: Niveau de journalisation ('DEBUG', 'INFO', 'WARNING', 'ERROR')

    Returns:
        Logger configuré avec handlers console et fichier
    """
    # Création du répertoire de logs si inexistant
    dossier_logs = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
    os.makedirs(dossier_logs, exist_ok=True)

    # Nom du fichier de log avec date du jour
    date_jour = datetime.now().strftime("%Y-%m-%d")
    fichier_log = os.path.join(dossier_logs, f"pipeline_{date_jour}.log")

    # Création du logger
    logger = logging.getLogger(nom_module)
    logger.setLevel(getattr(logging, niveau.upper(), logging.INFO))

    # Éviter les doublons de handlers si le logger est déjà configuré
    if logger.handlers:
        return logger

    # Format des messages de log
    format_log = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler console (sortie standard)
    handler_console = logging.StreamHandler()
    handler_console.setLevel(logging.INFO)
    handler_console.setFormatter(format_log)

    # Handler fichier avec rotation (max 10 Mo, 5 sauvegardes)
    handler_fichier = RotatingFileHandler(
        fichier_log,
        maxBytes=10 * 1024 * 1024,  # 10 Mo
        backupCount=5,
        encoding="utf-8",
    )
    handler_fichier.setLevel(logging.DEBUG)
    handler_fichier.setFormatter(format_log)

    logger.addHandler(handler_console)
    logger.addHandler(handler_fichier)

    return logger


def log_debut_etape(logger: logging.Logger, etape: str) -> None:
    """Journalise le début d'une étape du pipeline."""
    logger.info("=" * 60)
    logger.info(f"DEBUT ETAPE : {etape}")
    logger.info("=" * 60)


def log_fin_etape(logger: logging.Logger, etape: str, nb_lignes: int = None) -> None:
    """Journalise la fin d'une étape du pipeline avec statistiques."""
    message = f"FIN ETAPE : {etape}"
    if nb_lignes is not None:
        message += f" | {nb_lignes:,} lignes traitées"
    logger.info(message)
    logger.info("-" * 60)
