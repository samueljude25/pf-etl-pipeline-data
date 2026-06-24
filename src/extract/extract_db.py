"""
Module d'extraction depuis la base de données transactionnelle source.
Simule l'extraction depuis un système ERP/POS existant (PostgreSQL ou MySQL).
En production, remplacer les données simulées par de vraies requêtes SQL.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from src.utils.logger import get_logger, log_debut_etape, log_fin_etape

logger = get_logger("extract.db")


# ─── Configuration de la connexion ──────────────────────────────────────────

class ConnexionDB:
    """
    Gestionnaire de connexion à la base de données source.
    Utilise SQLAlchemy pour l'abstraction de la connexion.
    """

    def __init__(self, url_connexion: str = None):
        """
        Initialise la connexion à la base de données.

        Args:
            url_connexion: URL SQLAlchemy (ex: postgresql://user:pass@host:5432/db)
                           Si None, utilise le mode simulation.
        """
        self.url_connexion = url_connexion
        self.moteur = None
        self.mode_simulation = url_connexion is None

        if not self.mode_simulation:
            try:
                self.moteur = create_engine(
                    url_connexion,
                    pool_size=5,
                    max_overflow=10,
                    pool_timeout=30,
                    pool_recycle=3600,
                )
                logger.info("Connexion à la base de données établie")
            except Exception as e:
                logger.error(f"Impossible de créer le moteur SQLAlchemy : {e}")
                raise
        else:
            logger.warning(
                "Mode SIMULATION activé : pas de vraie base de données. "
                "Fournir url_connexion pour se connecter à un vrai système."
            )

    def executer_requete(self, sql: str, params: dict = None) -> pd.DataFrame:
        """
        Exécute une requête SQL et retourne le résultat en DataFrame.

        Args:
            sql: Requête SQL à exécuter
            params: Paramètres de la requête (pour les requêtes paramétrées)

        Returns:
            DataFrame avec les résultats de la requête
        """
        if self.mode_simulation:
            logger.debug(f"[SIMULATION] Requête non exécutée : {sql[:80]}...")
            return pd.DataFrame()

        try:
            with self.moteur.connect() as connexion:
                resultat = pd.read_sql(
                    text(sql),
                    con=connexion,
                    params=params or {},
                )
            logger.info(f"Requête exécutée : {len(resultat):,} lignes retournées")
            return resultat
        except OperationalError as e:
            logger.error(f"Erreur de connexion base de données : {e}")
            raise
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution de la requête : {e}")
            raise


def _generer_donnees_simulees_commandes(
    date_debut: str, date_fin: str
) -> pd.DataFrame:
    """
    Génère des données de commandes simulées imitant un système ERP.
    Simule ce que retournerait une vraie requête sur la table 'commandes' du POS.

    Args:
        date_debut: Date de début au format YYYY-MM-DD
        date_fin: Date de fin au format YYYY-MM-DD

    Returns:
        DataFrame simulant les données de la base transactionnelle
    """
    import random

    random.seed(42)  # Reproductibilité

    debut = datetime.strptime(date_debut, "%Y-%m-%d")
    fin = datetime.strptime(date_fin, "%Y-%m-%d")
    nb_jours = (fin - debut).days

    commandes = []
    for i in range(1, 51):  # 50 commandes B2B simulées
        date_cmd = debut + timedelta(days=random.randint(0, max(nb_jours, 1)))
        commandes.append({
            "commande_id": f"CMD{str(i).zfill(4)}",
            "date_commande": date_cmd.strftime("%Y-%m-%d"),
            "client_id": f"C{str(random.randint(1, 100)).zfill(3)}",
            "montant_ht_xaf": random.randint(100000, 2000000),
            "taux_tva": 18.0,  # TVA Congo 18%
            "montant_ttc_xaf": None,  # Calculé ci-dessous
            "mode_livraison": random.choice(["Livraison domicile", "Retrait magasin"]),
            "statut_commande": random.choice(["Livré", "En cours", "Annulé"]),
            "commercial_id": f"VDR{str(random.randint(1, 5)).zfill(2)}",
            "source": "ERP_INTERNE",
        })

    df = pd.DataFrame(commandes)
    # Calcul du montant TTC
    df["montant_ttc_xaf"] = (df["montant_ht_xaf"] * (1 + df["taux_tva"] / 100)).round()
    return df


def extraire_commandes_erp(
    connexion: ConnexionDB,
    date_debut: str = "2024-01-01",
    date_fin: str = "2025-12-31",
) -> pd.DataFrame:
    """
    Extrait les commandes depuis le système ERP interne.

    Args:
        connexion: Instance de ConnexionDB
        date_debut: Date de début de la période d'extraction
        date_fin: Date de fin de la période d'extraction

    Returns:
        DataFrame des commandes ERP
    """
    log_debut_etape(logger, "Extraction commandes ERP")
    logger.info(f"Période : {date_debut} → {date_fin}")

    # Requête SQL qui serait utilisée en production
    sql_production = """
        SELECT
            c.commande_id,
            c.date_commande,
            c.client_id,
            c.montant_ht_xaf,
            c.taux_tva,
            c.montant_ttc_xaf,
            c.mode_livraison,
            c.statut_commande,
            v.nom_commercial AS commercial_id
        FROM commandes c
        LEFT JOIN vendeurs v ON c.vendeur_id = v.vendeur_id
        WHERE c.date_commande BETWEEN :date_debut AND :date_fin
            AND c.statut_commande != 'Supprimé'
        ORDER BY c.date_commande ASC
    """

    if connexion.mode_simulation:
        logger.info("Mode simulation : génération de données ERP fictives")
        df = _generer_donnees_simulees_commandes(date_debut, date_fin)
    else:
        df = connexion.executer_requete(
            sql_production,
            params={"date_debut": date_debut, "date_fin": date_fin},
        )

    log_fin_etape(logger, "Extraction commandes ERP", len(df))
    return df


def extraire_historique_prix(connexion: ConnexionDB) -> pd.DataFrame:
    """
    Extrait l'historique des variations de prix depuis la base de données.

    Args:
        connexion: Instance de ConnexionDB

    Returns:
        DataFrame de l'historique des prix par produit
    """
    log_debut_etape(logger, "Extraction historique prix DB")

    sql_production = """
        SELECT
            produit_id,
            ancien_prix_xaf,
            nouveau_prix_xaf,
            date_changement,
            motif_changement,
            operateur_id
        FROM historique_prix
        ORDER BY produit_id, date_changement DESC
    """

    if connexion.mode_simulation:
        logger.info("Mode simulation : génération d'un historique de prix fictif")
        import random
        random.seed(99)

        enregistrements = []
        for pid in range(1, 21):
            for _ in range(random.randint(1, 5)):
                ancien = random.randint(10000, 500000)
                variation = random.uniform(-0.1, 0.1)
                enregistrements.append({
                    "produit_id": f"P{str(pid).zfill(3)}",
                    "ancien_prix_xaf": ancien,
                    "nouveau_prix_xaf": round(ancien * (1 + variation)),
                    "date_changement": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                    "motif_changement": random.choice([
                        "Révision tarifaire", "Promotion", "Hausse fournisseur",
                        "Fin promotion", "Ajustement marché"
                    ]),
                    "operateur_id": f"USR{random.randint(1, 5):02d}",
                })
        df = pd.DataFrame(enregistrements)
    else:
        df = connexion.executer_requete(sql_production)

    log_fin_etape(logger, "Extraction historique prix DB", len(df))
    return df


def extraire_toutes_sources_db(
    url_connexion: str = None,
    date_debut: str = "2024-01-01",
    date_fin: str = "2025-12-31",
) -> Dict[str, pd.DataFrame]:
    """
    Orchestre l'extraction complète depuis la base de données source.

    Args:
        url_connexion: URL de connexion SQLAlchemy (None = mode simulation)
        date_debut: Début de la période d'extraction
        date_fin: Fin de la période d'extraction

    Returns:
        Dictionnaire {nom_source: DataFrame}
    """
    logger.info("Démarrage de l'extraction base de données transactionnelle")

    connexion = ConnexionDB(url_connexion)
    resultats = {}

    try:
        resultats["commandes_erp"] = extraire_commandes_erp(connexion, date_debut, date_fin)
        resultats["historique_prix"] = extraire_historique_prix(connexion)

        logger.info(
            f"Extraction DB terminée : "
            f"{sum(len(df) for df in resultats.values()):,} enregistrements au total"
        )
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction DB : {e}", exc_info=True)
        raise

    return resultats
