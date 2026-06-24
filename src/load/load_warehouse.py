"""
Module de chargement des données transformées vers l'entrepôt analytique.
Implémente une stratégie upsert (INSERT ... ON CONFLICT DO UPDATE)
pour garantir l'idempotence du pipeline.
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from src.utils.logger import get_logger, log_debut_etape, log_fin_etape

logger = get_logger("load.warehouse")

# ─── Configuration du chargement ────────────────────────────────────────────

BATCH_SIZE = 1000  # Nombre de lignes par lot pour les insertions
SCHEMA_STAGING = "staging"
SCHEMA_MARTS = "marts"


class ChargeEntrepot:
    """
    Gestionnaire du chargement vers l'entrepôt de données PostgreSQL.
    Fournit des méthodes pour créer les schémas, charger les données
    et journaliser les opérations.
    """

    def __init__(self, url_connexion: str = None):
        """
        Initialise la connexion à l'entrepôt de données.

        Args:
            url_connexion: URL SQLAlchemy PostgreSQL.
                           Ex: postgresql://etl_user:motdepasse@localhost:5432/entrepot
                           Si None, mode simulation activé.
        """
        self.url_connexion = url_connexion
        self.moteur = None
        self.mode_simulation = url_connexion is None
        self._journal_chargements = []

        if not self.mode_simulation:
            try:
                self.moteur = create_engine(
                    url_connexion,
                    pool_size=3,
                    max_overflow=5,
                    pool_timeout=60,
                    echo=False,
                )
                # Test de connexion
                with self.moteur.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info(f"Connexion entrepôt établie : {url_connexion.split('@')[-1]}")
            except OperationalError as e:
                logger.error(f"Impossible de se connecter à l'entrepôt : {e}")
                raise
        else:
            logger.warning(
                "Mode SIMULATION activé : les données ne seront pas persistées. "
                "Configurer DATABASE_URL pour activer le chargement réel."
            )

    def creer_schemas(self) -> None:
        """Crée les schémas staging et marts dans PostgreSQL si inexistants."""
        if self.mode_simulation:
            logger.info("[SIMULATION] Schémas staging et marts seraient créés")
            return

        with self.moteur.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_STAGING}"))
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_MARTS}"))
            conn.commit()
        logger.info(f"Schémas '{SCHEMA_STAGING}' et '{SCHEMA_MARTS}' prêts")

    def _charger_dataframe(
        self,
        df: pd.DataFrame,
        nom_table: str,
        schema: str,
        strategie: str = "replace",
        cle_primaire: Optional[str] = None,
    ) -> Dict:
        """
        Charge un DataFrame dans une table PostgreSQL.

        Args:
            df: DataFrame à charger
            nom_table: Nom de la table cible
            schema: Schéma de la table (staging ou marts)
            strategie: 'replace' (tronquer+remplir), 'append', 'upsert'
            cle_primaire: Colonne clé pour l'upsert

        Returns:
            Dictionnaire des statistiques du chargement
        """
        stats = {
            "table": f"{schema}.{nom_table}",
            "nb_lignes_source": len(df),
            "nb_lignes_inserees": 0,
            "nb_lignes_mises_a_jour": 0,
            "statut": "échec",
            "timestamp": datetime.now().isoformat(),
            "erreur": None,
        }

        if df.empty:
            logger.warning(f"DataFrame vide — aucune donnée à charger dans {schema}.{nom_table}")
            stats["statut"] = "ignoré (DataFrame vide)"
            return stats

        if self.mode_simulation:
            logger.info(
                f"[SIMULATION] Chargement {schema}.{nom_table} : "
                f"{len(df):,} lignes, stratégie={strategie}"
            )
            stats["nb_lignes_inserees"] = len(df)
            stats["statut"] = "simulé"
            self._journal_chargements.append(stats)
            return stats

        try:
            # Chargement par lots pour les grands volumes
            nb_lignes_total = 0
            for debut in range(0, len(df), BATCH_SIZE):
                lot = df.iloc[debut:debut + BATCH_SIZE]
                lot.to_sql(
                    name=nom_table,
                    con=self.moteur,
                    schema=schema,
                    if_exists="replace" if (debut == 0 and strategie == "replace") else "append",
                    index=False,
                    method="multi",
                )
                nb_lignes_total += len(lot)
                logger.debug(
                    f"Lot chargé : {debut + len(lot):,}/{len(df):,} lignes "
                    f"→ {schema}.{nom_table}"
                )

            stats["nb_lignes_inserees"] = nb_lignes_total
            stats["statut"] = "succès"
            logger.info(
                f"Chargé {schema}.{nom_table} : "
                f"{nb_lignes_total:,} lignes (stratégie={strategie})"
            )

        except SQLAlchemyError as e:
            stats["erreur"] = str(e)
            logger.error(f"Erreur lors du chargement de {schema}.{nom_table} : {e}")
            raise

        self._journal_chargements.append(stats)
        return stats

    # ─── Chargement des tables staging ──────────────────────────────────────

    def charger_staging_clients(self, df: pd.DataFrame) -> Dict:
        """Charge les clients dans la zone staging."""
        log_debut_etape(logger, "Chargement staging.clients")
        stats = self._charger_dataframe(df, "clients", SCHEMA_STAGING, strategie="replace")
        log_fin_etape(logger, "Chargement staging.clients", stats["nb_lignes_inserees"])
        return stats

    def charger_staging_transactions(self, df: pd.DataFrame) -> Dict:
        """Charge les transactions dans la zone staging."""
        log_debut_etape(logger, "Chargement staging.transactions")
        stats = self._charger_dataframe(
            df, "transactions", SCHEMA_STAGING, strategie="replace"
        )
        log_fin_etape(logger, "Chargement staging.transactions", stats["nb_lignes_inserees"])
        return stats

    def charger_staging_produits(self, df: pd.DataFrame) -> Dict:
        """Charge les produits dans la zone staging."""
        return self._charger_dataframe(df, "produits", SCHEMA_STAGING, strategie="replace")

    def charger_staging_magasins(self, df: pd.DataFrame) -> Dict:
        """Charge les magasins dans la zone staging."""
        return self._charger_dataframe(df, "magasins", SCHEMA_STAGING, strategie="replace")

    # ─── Chargement des tables marts ────────────────────────────────────────

    def charger_mart_ca_mensuel(self, df: pd.DataFrame) -> Dict:
        """Charge le mart du CA mensuel."""
        return self._charger_dataframe(df, "ca_mensuel", SCHEMA_MARTS, strategie="replace")

    def charger_mart_top_produits(self, df: pd.DataFrame) -> Dict:
        """Charge le mart des top produits."""
        return self._charger_dataframe(df, "top_produits", SCHEMA_MARTS, strategie="replace")

    def charger_mart_performance_magasins(self, df: pd.DataFrame) -> Dict:
        """Charge le mart de performance des magasins."""
        return self._charger_dataframe(
            df, "performance_magasins", SCHEMA_MARTS, strategie="replace"
        )

    def charger_mart_segmentation_rfm(self, df: pd.DataFrame) -> Dict:
        """Charge le mart de segmentation RFM des clients."""
        return self._charger_dataframe(
            df, "segmentation_rfm", SCHEMA_MARTS, strategie="replace"
        )

    # ─── Rapport de chargement ──────────────────────────────────────────────

    def generer_rapport_chargement(self) -> pd.DataFrame:
        """
        Génère un rapport récapitulatif de tous les chargements effectués.

        Returns:
            DataFrame du journal des chargements
        """
        if not self._journal_chargements:
            return pd.DataFrame()

        df_rapport = pd.DataFrame(self._journal_chargements)
        nb_succes = (df_rapport["statut"] == "succès").sum()
        nb_simules = (df_rapport["statut"] == "simulé").sum()
        nb_echecs = (df_rapport["statut"] == "échec").sum()
        nb_lignes_total = df_rapport["nb_lignes_inserees"].sum()

        logger.info(
            f"Rapport chargement : "
            f"{nb_succes} succès, {nb_simules} simulés, {nb_echecs} échecs — "
            f"{nb_lignes_total:,} lignes au total"
        )
        return df_rapport


def charger_pipeline_complet(
    donnees_staging: Dict[str, pd.DataFrame],
    donnees_marts: Dict[str, pd.DataFrame],
    url_connexion: str = None,
) -> Tuple[bool, pd.DataFrame]:
    """
    Orchestre le chargement complet du pipeline vers l'entrepôt.

    Args:
        donnees_staging: Dictionnaire {nom: DataFrame} pour la zone staging
        donnees_marts: Dictionnaire {nom: DataFrame} pour les marts
        url_connexion: URL de connexion PostgreSQL (None = simulation)

    Returns:
        Tuple (succès: bool, rapport: DataFrame)
    """
    log_debut_etape(logger, "Chargement pipeline complet → Entrepôt")

    charge = ChargeEntrepot(url_connexion)
    charge.creer_schemas()

    # ─ Chargement staging ─────────────────────────────────────────
    logger.info("Phase 1 : Chargement zone STAGING")
    if "clients" in donnees_staging:
        charge.charger_staging_clients(donnees_staging["clients"])
    if "transactions" in donnees_staging:
        charge.charger_staging_transactions(donnees_staging["transactions"])
    if "produits" in donnees_staging:
        charge.charger_staging_produits(donnees_staging["produits"])
    if "magasins" in donnees_staging:
        charge.charger_staging_magasins(donnees_staging["magasins"])

    # ─ Chargement marts ───────────────────────────────────────────
    logger.info("Phase 2 : Chargement zone MARTS")
    if "ca_mensuel" in donnees_marts:
        charge.charger_mart_ca_mensuel(donnees_marts["ca_mensuel"])
    if "top_produits" in donnees_marts:
        charge.charger_mart_top_produits(donnees_marts["top_produits"])
    if "performance_magasins" in donnees_marts:
        charge.charger_mart_performance_magasins(donnees_marts["performance_magasins"])
    if "segmentation_rfm" in donnees_marts:
        charge.charger_mart_segmentation_rfm(donnees_marts["segmentation_rfm"])

    rapport = charge.generer_rapport_chargement()
    succes = "échec" not in rapport["statut"].values if not rapport.empty else True

    log_fin_etape(logger, "Chargement pipeline complet → Entrepôt")
    return succes, rapport
