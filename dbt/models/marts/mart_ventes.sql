-- ============================================================
-- Modèle mart : mart_ventes
-- Grain : 1 ligne par mois × magasin × catégorie produit
-- Rôle : Table de faits agrégée pour le reporting commercial
-- Matérialisation : Table (rafraîchissement complet quotidien)
-- ============================================================

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

clients AS (
    SELECT * FROM {{ ref('stg_clients') }}
),

produits AS (
    SELECT * FROM {{ ref('stg_produits') }}
),

magasins AS (
    SELECT * FROM {{ ref('stg_magasins') }}
),

-- Jointure de toutes les dimensions sur les transactions
transactions_enrichies AS (
    SELECT
        t.transaction_id,
        t.date_transaction,
        t.annee,
        t.mois,
        t.trimestre,
        t.mois_annee,
        t.nom_mois,
        t.quantite,
        t.prix_unitaire_xaf,
        t.montant_total_xaf,
        t.mode_paiement,
        t.canal_vente,
        t.est_vente_en_ligne,

        -- Dimensions client
        c.client_id,
        c.ville                     AS ville_client,
        c.departement,
        c.categorie_client,
        c.est_client_premium,

        -- Dimensions produit
        p.produit_id,
        p.nom_produit,
        p.categorie                 AS categorie_produit,
        p.sous_categorie,
        p.segment_prix,

        -- Dimensions magasin
        m.magasin_id,
        m.nom_magasin,
        m.ville                     AS ville_magasin,
        m.zone_commerciale,
        m.taille_magasin

    FROM transactions t
    LEFT JOIN clients  c ON t.client_id  = c.client_id
    LEFT JOIN produits p ON t.produit_id = p.produit_id
    LEFT JOIN magasins m ON t.magasin_id = m.magasin_id
),

-- Agrégation au grain mensuel × magasin × catégorie produit
agregation AS (
    SELECT
        -- Clés de regroupement
        mois_annee,
        annee,
        mois,
        trimestre,
        nom_mois,
        magasin_id,
        nom_magasin,
        ville_magasin,
        zone_commerciale,
        taille_magasin,
        categorie_produit,

        -- Métriques de volume
        COUNT(DISTINCT transaction_id)          AS nb_transactions,
        COUNT(DISTINCT client_id)               AS nb_clients_uniques,
        COUNT(DISTINCT produit_id)              AS nb_produits_vendus,
        SUM(quantite)                           AS quantite_totale,

        -- Métriques financières (en XAF)
        SUM(montant_total_xaf)                  AS ca_total_xaf,
        AVG(montant_total_xaf)                  AS panier_moyen_xaf,
        MIN(montant_total_xaf)                  AS transaction_min_xaf,
        MAX(montant_total_xaf)                  AS transaction_max_xaf,

        -- Répartition canal de vente
        SUM(CASE WHEN est_vente_en_ligne THEN montant_total_xaf ELSE 0 END) AS ca_en_ligne_xaf,
        SUM(CASE WHEN NOT est_vente_en_ligne THEN montant_total_xaf ELSE 0 END) AS ca_magasin_xaf,

        -- Répartition mode de paiement
        SUM(CASE WHEN mode_paiement = 'Mobile Money' THEN 1 ELSE 0 END) AS nb_mobile_money,
        SUM(CASE WHEN mode_paiement = 'Espèces'      THEN 1 ELSE 0 END) AS nb_especes,
        SUM(CASE WHEN mode_paiement = 'Carte bancaire' THEN 1 ELSE 0 END) AS nb_carte_bancaire,
        SUM(CASE WHEN mode_paiement = 'Virement'     THEN 1 ELSE 0 END) AS nb_virement,

        -- Clients premium
        SUM(CASE WHEN est_client_premium THEN montant_total_xaf ELSE 0 END) AS ca_clients_premium_xaf,

        CURRENT_TIMESTAMP                       AS dbt_updated_at

    FROM transactions_enrichies
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)

SELECT
    *,
    -- Taux de vente en ligne
    ROUND(
        ca_en_ligne_xaf::NUMERIC / NULLIF(ca_total_xaf, 0) * 100, 2
    )                                           AS taux_vente_en_ligne_pct,

    -- Part du CA premium
    ROUND(
        ca_clients_premium_xaf::NUMERIC / NULLIF(ca_total_xaf, 0) * 100, 2
    )                                           AS part_ca_premium_pct

FROM agregation
ORDER BY annee, mois, ca_total_xaf DESC
