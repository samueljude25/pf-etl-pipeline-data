"""
Module d'extraction depuis une API REST simulée.
Simule la récupération de données depuis une API externe :
- Catalogue produits (prix temps réel)
- Taux de change XAF/EUR
- Données météo des villes (impact sur les ventes)

Aucune vraie API n'est requise : les données sont générées en mémoire
pour démontrer la logique d'intégration d'une source API.
"""

import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from src.utils.logger import get_logger, log_debut_etape, log_fin_etape

logger = get_logger("extract.api")

# ─── Configuration de l'API fictive ─────────────────────────────────────────

API_BASE_URL = "https://api.commerce-congo.cg/v1"  # URL fictive (non réelle)
API_TIMEOUT_SECONDES = 30
API_MAX_RETRIES = 3
API_PAGE_SIZE = 100

# Taux de change XAF vers autres devises (fictifs mais réalistes)
TAUX_CHANGE = {
    "XAF_EUR": 0.00152,   # 1 XAF ≈ 0.00152 EUR (taux BEAC indicatif)
    "XAF_USD": 0.00165,   # 1 XAF ≈ 0.00165 USD
    "XAF_CDF": 0.0048,    # 1 XAF ≈ 0.0048 CDF (Franc congolais RDC)
    "XAF_NGN": 1.35,      # 1 XAF ≈ 1.35 NGN (Naira nigérian)
}


# ─── Simulateur d'appels API ─────────────────────────────────────────────────

class SimulateurAPI:
    """
    Simule un client API REST pour l'extraction de données externes.
    En production, remplacer les méthodes par de vrais appels HTTP (requests).
    """

    def __init__(self, base_url: str = API_BASE_URL, timeout: int = API_TIMEOUT_SECONDES):
        self.base_url = base_url
        self.timeout = timeout
        self._donnees_cache = {}
        logger.info(f"SimulateurAPI initialisé pour : {base_url}")

    def _simuler_latence(self, min_ms: int = 50, max_ms: int = 300) -> None:
        """Simule la latence réseau d'un vrai appel API."""
        latence = random.uniform(min_ms, max_ms) / 1000
        time.sleep(latence)

    def _appel_api(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """
        Simule un appel GET vers l'API.
        En production : return requests.get(f"{self.base_url}/{endpoint}", params=params).json()
        """
        self._simuler_latence()
        logger.debug(f"[API] GET {self.base_url}/{endpoint} | params={params}")

        # Simulation d'une réponse réussie
        return {
            "status": "success",
            "endpoint": endpoint,
            "timestamp": datetime.now().isoformat(),
            "params": params or {},
        }

    def obtenir_prix_temps_reel(self, ids_produits: List[str]) -> pd.DataFrame:
        """
        Récupère les prix actuels des produits depuis l'API catalogue.

        Args:
            ids_produits: Liste des identifiants produits

        Returns:
            DataFrame avec colonnes [produit_id, prix_actuel_xaf, date_maj_prix]
        """
        logger.info(f"Récupération des prix temps réel pour {len(ids_produits)} produits")

        # Simulation : variation de prix de ±5% autour d'un prix de base
        prix_base = {
            "P001": 85000, "P002": 65000, "P003": 55000, "P004": 25000,
            "P005": 250000, "P006": 450000, "P007": 380000, "P008": 180000,
            "P009": 320000, "P010": 185000, "P011": 380000, "P012": 35000,
            "P013": 450000, "P014": 420000, "P015": 28000, "P016": 22500,
            "P017": 8500, "P018": 5500, "P019": 38000, "P020": 35000,
        }

        resultats = []
        for produit_id in ids_produits:
            self._appel_api(f"catalogue/prix/{produit_id}")
            prix_ref = prix_base.get(produit_id, 50000)
            variation = random.uniform(-0.05, 0.05)
            prix_actuel = round(prix_ref * (1 + variation))

            resultats.append({
                "produit_id": produit_id,
                "prix_actuel_xaf": prix_actuel,
                "variation_pct": round(variation * 100, 2),
                "date_maj_prix": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "API_CATALOGUE",
            })

        df = pd.DataFrame(resultats)
        logger.info(f"Prix récupérés pour {len(df)} produits")
        return df

    def obtenir_taux_change(self, date: Optional[str] = None) -> pd.DataFrame:
        """
        Récupère les taux de change XAF depuis l'API financière BEAC.

        Args:
            date: Date au format YYYY-MM-DD (défaut: aujourd'hui)

        Returns:
            DataFrame avec colonnes [devise_source, devise_cible, taux, date]
        """
        date = date or datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Récupération des taux de change pour le {date}")

        self._appel_api("finance/taux-change", params={"date": date, "base": "XAF"})

        # Simulation avec légère variation quotidienne
        resultats = []
        for paire, taux_base in TAUX_CHANGE.items():
            devises = paire.split("_")
            variation = random.uniform(-0.002, 0.002)
            resultats.append({
                "devise_source": devises[0],
                "devise_cible": devises[1],
                "taux": round(taux_base * (1 + variation), 6),
                "date": date,
                "source": "API_BEAC_SIMULEE",
            })

        df = pd.DataFrame(resultats)
        logger.info(f"{len(df)} taux de change récupérés")
        return df

    def obtenir_evenements_commerciaux(
        self, date_debut: str, date_fin: str
    ) -> pd.DataFrame:
        """
        Récupère le calendrier des événements commerciaux (promotions, fêtes).

        Args:
            date_debut: Date de début YYYY-MM-DD
            date_fin: Date de fin YYYY-MM-DD

        Returns:
            DataFrame des événements avec impact estimé sur les ventes
        """
        logger.info(f"Récupération des événements commerciaux : {date_debut} → {date_fin}")

        self._appel_api("evenements", params={"debut": date_debut, "fin": date_fin})

        # Événements commerciaux fictifs typiques d'Afrique centrale
        evenements = [
            {
                "date": "2024-01-01",
                "type": "Fête nationale",
                "description": "Jour de l'An",
                "impact_ventes_pct": 25,
                "villes_concernees": "Toutes",
            },
            {
                "date": "2024-06-15",
                "type": "Promotion",
                "description": "Soldes mi-saison",
                "impact_ventes_pct": 15,
                "villes_concernees": "Brazzaville,Pointe-Noire",
            },
            {
                "date": "2024-08-15",
                "type": "Fête nationale",
                "description": "Fête nationale du Congo",
                "impact_ventes_pct": 30,
                "villes_concernees": "Toutes",
            },
            {
                "date": "2024-11-28",
                "type": "Black Friday",
                "description": "Promotions Black Friday",
                "impact_ventes_pct": 40,
                "villes_concernees": "Brazzaville,Pointe-Noire",
            },
            {
                "date": "2024-12-25",
                "type": "Fête",
                "description": "Noël",
                "impact_ventes_pct": 35,
                "villes_concernees": "Toutes",
            },
            {
                "date": "2025-01-01",
                "type": "Fête nationale",
                "description": "Jour de l'An 2025",
                "impact_ventes_pct": 28,
                "villes_concernees": "Toutes",
            },
        ]

        df = pd.DataFrame(evenements)
        # Filtrage sur la plage de dates demandée
        df["date"] = pd.to_datetime(df["date"])
        masque = (df["date"] >= date_debut) & (df["date"] <= date_fin)
        df = df[masque].reset_index(drop=True)

        logger.info(f"{len(df)} événements commerciaux récupérés")
        return df

    def obtenir_stocks_temps_reel(
        self, ids_magasins: List[str], ids_produits: List[str]
    ) -> pd.DataFrame:
        """
        Récupère les niveaux de stock actuels par magasin et produit.

        Args:
            ids_magasins: Liste des identifiants magasins
            ids_produits: Liste des identifiants produits

        Returns:
            DataFrame stock [magasin_id, produit_id, quantite_stock, alerte_rupture]
        """
        logger.info(
            f"Récupération des stocks : {len(ids_magasins)} magasins, "
            f"{len(ids_produits)} produits"
        )

        resultats = []
        for magasin_id in ids_magasins:
            for produit_id in ids_produits[:10]:  # Limiter pour la démo
                self._appel_api(
                    f"stocks/{magasin_id}/{produit_id}",
                    params={"date": datetime.now().strftime("%Y-%m-%d")},
                )
                stock = random.randint(0, 150)
                resultats.append({
                    "magasin_id": magasin_id,
                    "produit_id": produit_id,
                    "quantite_stock": stock,
                    "alerte_rupture": stock < 10,
                    "date_releve": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })

        df = pd.DataFrame(resultats)
        nb_alertes = df["alerte_rupture"].sum()
        logger.info(
            f"Stocks récupérés : {len(df)} entrées, "
            f"{nb_alertes} alertes rupture détectées"
        )
        return df


# ─── Fonctions principales d'extraction API ──────────────────────────────────

def extraire_donnees_api(
    ids_produits: List[str] = None,
    ids_magasins: List[str] = None,
    date_debut: str = "2024-01-01",
    date_fin: str = "2025-12-31",
) -> Dict[str, pd.DataFrame]:
    """
    Orchestre l'extraction complète depuis l'API simulée.

    Args:
        ids_produits: Liste des produits à interroger
        ids_magasins: Liste des magasins à interroger
        date_debut: Début de la période pour les événements
        date_fin: Fin de la période pour les événements

    Returns:
        Dictionnaire {nom_source: DataFrame}
    """
    log_debut_etape(logger, "Extraction API REST simulée")

    # Valeurs par défaut
    if ids_produits is None:
        ids_produits = [f"P{str(i).zfill(3)}" for i in range(1, 21)]
    if ids_magasins is None:
        ids_magasins = [f"M{str(i).zfill(3)}" for i in range(1, 11)]

    api = SimulateurAPI()
    resultats = {}

    try:
        resultats["prix_temps_reel"] = api.obtenir_prix_temps_reel(ids_produits)
        resultats["taux_change"] = api.obtenir_taux_change()
        resultats["evenements"] = api.obtenir_evenements_commerciaux(date_debut, date_fin)
        resultats["stocks"] = api.obtenir_stocks_temps_reel(
            ids_magasins[:5], ids_produits[:10]
        )

        nb_total = sum(len(df) for df in resultats.values())
        log_fin_etape(logger, "Extraction API REST simulée", nb_total)

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction API : {e}", exc_info=True)
        raise

    return resultats
