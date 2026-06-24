"""
Script de génération de données fictives pour le pipeline ETL.
Génère ou régénère les fichiers CSV dans data/raw/ avec des données
réalistes pour le contexte commercial d'Afrique centrale (Congo).

Usage :
    python scripts/generate_data.py
    python scripts/generate_data.py --clients 200 --transactions 1000

Dépendances : pandas, faker
"""

import argparse
import os
import random
import sys
from datetime import date, timedelta

import pandas as pd

# ─── Données de référence ─────────────────────────────────────────────────────

NOMS_CONGOLAIS = [
    "MOUKALA", "NGOMA", "BOUANGA", "LOEMBA", "MABIKA", "NZABA", "OKANDZA",
    "DJOMBO", "GOMA", "LEBONDZO", "NKODIA", "PAMBOU", "MALONGA", "NKEOUA",
    "BATCHI", "MOUSSAVOU", "KIHOUNGA", "NGUIMBI", "OSSOMBO", "MILANDOU",
    "NZOUSSI", "MBOUKOU", "KIBANGOU", "LOUVOUEZO", "NKOUNKOU", "BOUKAKA",
    "ONDONGO", "MAPOUATA", "NKOUKA", "BITSINDOU", "LOUBASSOU", "MAVOUNGOU",
    "NZINGOULA", "KIMBEMBE", "MBOUNGOU", "NKOUMOU", "POATY", "TSIBA",
    "MOUNDZIEGOU", "NGATSONO", "KOUMBA", "LOUZOLO", "MPASSI", "NDEMBET",
    "BANZOUZI", "MOUBAMBA", "NZINGA", "BOUITI", "MABANZA", "NGUILA",
]

PRENOMS_MASCULINS = [
    "Jean-Baptiste", "Pierre", "Rodrigue", "Firmin", "Achille", "Thierry",
    "Serge", "Emmanuel", "Barnabé", "Cyprien", "Gaston", "Théodore", "Marcel",
    "Gérard", "Alexis", "Dieudonné", "Stéphane", "Aubin", "Franck", "Patrick",
    "Sylvain", "Hyacinthe", "Félix", "Lambert", "Constant", "François",
    "Augustin", "Romain", "Bernard", "Gilles", "Armand", "Évariste", "Prosper",
    "Valentin", "Gaspard", "Hubert", "Edmond", "Désiré", "Octave", "Alphonse",
]

PRENOMS_FEMININS = [
    "Marie-Claire", "Angélique", "Cécile", "Nadège", "Pauline", "Élise",
    "Viviane", "Christelle", "Sandrine", "Joëlle", "Rosette", "Aimée",
    "Félicité", "Yvonne", "Marceline", "Georgette", "Honorine", "Albertine",
    "Laurence", "Bernadette", "Denise", "Colette", "Isabelle", "Clémence",
    "Thérèse", "Jeannette", "Louisette", "Annette", "Chantal", "Victorine",
    "Odette", "Philomène", "Marguerite", "Anastasie", "Fernande", "Clarisse",
    "Hortense", "Sylvie", "Henriette", "Laetitia",
]

VILLES_CONGO = [
    ("Brazzaville", ["Bacongo", "Poto-Poto", "Moungali", "Talangaï", "Ouenzé"]),
    ("Pointe-Noire", ["Lumumba", "Mvou-Mvou", "Tié-Tié", "Ngoyo"]),
    ("Dolisie", ["Centre", "Mvoulou"]),
    ("Ouesso", ["Centre"]),
]

CATEGORIES_CLIENTS = ["Standard", "Premium", "VIP"]
POIDS_CATEGORIES = [0.6, 0.3, 0.1]

MODES_PAIEMENT = ["Espèces", "Mobile Money", "Carte bancaire", "Virement"]
POIDS_PAIEMENT = [0.45, 0.35, 0.12, 0.08]

CANAUX_VENTE = ["Magasin", "En ligne"]
POIDS_CANAUX = [0.75, 0.25]


def date_aleatoire(debut: date, fin: date) -> date:
    """Génère une date aléatoire dans l'intervalle [debut, fin]."""
    delta = (fin - debut).days
    return debut + timedelta(days=random.randint(0, delta))


def generer_clients(nb: int = 100) -> pd.DataFrame:
    """
    Génère un DataFrame de clients fictifs avec des noms congolais.

    Args:
        nb: Nombre de clients à générer

    Returns:
        DataFrame des clients
    """
    clients = []
    random.seed(42)

    for i in range(1, nb + 1):
        sexe = random.choice(["M", "F"])
        nom = random.choice(NOMS_CONGOLAIS)
        prenom = random.choice(PRENOMS_MASCULINS if sexe == "M" else PRENOMS_FEMININS)
        ville_info = random.choices(VILLES_CONGO, weights=[4, 3, 2, 1])[0]
        ville = ville_info[0]
        quartier = random.choice(ville_info[1])
        categorie = random.choices(CATEGORIES_CLIENTS, weights=POIDS_CATEGORIES)[0]
        email = f"{prenom[0].lower()}.{nom.lower().replace('-', '')}@email.cg"
        # Numéro de téléphone au format Congo
        operateur = random.choice(["06", "05", "04"])
        tel = f"+242 {operateur} {random.randint(100, 999)} {random.randint(1000, 9999)}"
        date_inscription = date_aleatoire(date(2024, 1, 1), date(2025, 6, 30))

        clients.append({
            "client_id": f"C{i:03d}",
            "nom": nom,
            "prenom": prenom,
            "email": email,
            "telephone": tel,
            "ville": ville,
            "quartier": quartier,
            "date_inscription": date_inscription.strftime("%Y-%m-%d"),
            "sexe": sexe,
            "categorie": categorie,
        })

    return pd.DataFrame(clients)


def generer_transactions(
    nb: int = 500,
    ids_clients: list = None,
    ids_produits: list = None,
    ids_magasins: list = None,
    prix_produits: dict = None,
) -> pd.DataFrame:
    """
    Génère un DataFrame de transactions fictives.

    Args:
        nb: Nombre de transactions à générer
        ids_clients: Liste des IDs clients disponibles
        ids_produits: Liste des IDs produits disponibles
        ids_magasins: Liste des IDs magasins disponibles
        prix_produits: Dictionnaire {produit_id: prix_unitaire}

    Returns:
        DataFrame des transactions
    """
    random.seed(123)

    ids_clients = ids_clients or [f"C{i:03d}" for i in range(1, 101)]
    ids_produits = ids_produits or [f"P{i:03d}" for i in range(1, 51)]
    ids_magasins = ids_magasins or [f"M{i:03d}" for i in range(1, 11)]

    # Prix par défaut si non fournis
    if prix_produits is None:
        prix_produits = {pid: random.randint(1500, 850000) for pid in ids_produits}

    transactions = []
    for i in range(1, nb + 1):
        produit_id = random.choice(ids_produits)
        prix_unitaire = prix_produits.get(produit_id, 25000)
        quantite = random.choices([1, 2, 3, 4, 5, 6, 8, 10, 12, 20], weights=[40, 20, 12, 8, 6, 5, 4, 3, 1, 1])[0]
        montant = prix_unitaire * quantite
        date_tx = date_aleatoire(date(2024, 1, 1), date(2025, 10, 31))

        transactions.append({
            "transaction_id": f"T{i:04d}",
            "date_transaction": date_tx.strftime("%Y-%m-%d"),
            "client_id": random.choice(ids_clients),
            "magasin_id": random.choice(ids_magasins),
            "produit_id": produit_id,
            "quantite": quantite,
            "prix_unitaire_xaf": prix_unitaire,
            "montant_total_xaf": montant,
            "mode_paiement": random.choices(MODES_PAIEMENT, weights=POIDS_PAIEMENT)[0],
            "statut": "Validé",
            "canal_vente": random.choices(CANAUX_VENTE, weights=POIDS_CANAUX)[0],
        })

    return pd.DataFrame(transactions)


def sauvegarder_csv(df: pd.DataFrame, chemin: str) -> None:
    """Sauvegarde un DataFrame en CSV avec confirmation."""
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    df.to_csv(chemin, index=False, encoding="utf-8")
    print(f"  Sauvegardé : {chemin} ({len(df):,} lignes)")


def main():
    """Point d'entrée principal du script de génération."""
    parser = argparse.ArgumentParser(
        description="Génération des données fictives pour le pipeline ETL Congo"
    )
    parser.add_argument("--clients", type=int, default=100, help="Nombre de clients (défaut: 100)")
    parser.add_argument("--transactions", type=int, default=500, help="Nombre de transactions (défaut: 500)")
    parser.add_argument("--output-dir", default="data/raw", help="Répertoire de sortie")
    args = parser.parse_args()

    print("=" * 60)
    print("Génération des données fictives — Pipeline ETL Congo")
    print("=" * 60)

    # Chargement des prix depuis le CSV produits existant si disponible
    prix_produits = {}
    chemin_produits = os.path.join(args.output_dir, "produits.csv")
    if os.path.exists(chemin_produits):
        df_produits = pd.read_csv(chemin_produits)
        ids_produits = df_produits["produit_id"].tolist()
        prix_produits = dict(zip(df_produits["produit_id"], df_produits["prix_unitaire_xaf"].astype(float)))
        print(f"  {len(ids_produits)} produits chargés depuis {chemin_produits}")
    else:
        ids_produits = None

    chemin_magasins = os.path.join(args.output_dir, "magasins.csv")
    if os.path.exists(chemin_magasins):
        df_magasins = pd.read_csv(chemin_magasins)
        ids_magasins = df_magasins["magasin_id"].tolist()
    else:
        ids_magasins = None

    print(f"\nGénération de {args.clients} clients...")
    df_clients = generer_clients(args.clients)
    sauvegarder_csv(df_clients, os.path.join(args.output_dir, "clients.csv"))

    print(f"\nGénération de {args.transactions} transactions...")
    ids_clients = df_clients["client_id"].tolist()
    df_tx = generer_transactions(
        nb=args.transactions,
        ids_clients=ids_clients,
        ids_produits=ids_produits,
        ids_magasins=ids_magasins,
        prix_produits=prix_produits,
    )
    sauvegarder_csv(df_tx, os.path.join(args.output_dir, "transactions.csv"))

    print("\n" + "=" * 60)
    print(f"Génération terminée avec succès !")
    print(f"  Clients     : {len(df_clients):,}")
    print(f"  Transactions: {len(df_tx):,}")
    print(f"  CA total    : {df_tx['montant_total_xaf'].sum():,.0f} XAF")
    print("=" * 60)


if __name__ == "__main__":
    main()
