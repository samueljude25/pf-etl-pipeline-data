-- ============================================================
-- Modèle mart : mart_produits
-- Grain : 1 ligne par produit
-- Rôle : Performance commerciale des produits sur toute la période
-- Matérialisation : Table
-- ============================================================

WITH produits AS (
    SELECT * FROM {{ ref('stg_produits') }}
),

transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

-- Métriques de vente par produit
performance AS (
    SELECT
        produit_id,

        -- Volume
        COUNT(DISTINCT transaction_id)              AS nb_transactions,
        COUNT(DISTINCT client_id)                   AS nb_clients_uniques,
        COUNT(DISTINCT magasin_id)                  AS nb_magasins_vendeurs,
        SUM(quantite)                               AS quantite_totale_vendue,

        -- Financier
        SUM(montant_total_xaf)                      AS ca_total_xaf,
        AVG(montant_total_xaf)                      AS ca_moyen_par_vente_xaf,
        MIN(prix_unitaire_xaf)                      AS prix_min_observe_xaf,
        MAX(prix_unitaire_xaf)                      AS prix_max_observe_xaf,
        AVG(prix_unitaire_xaf)                      AS prix_moyen_observe_xaf,

        -- Répartition canal
        SUM(CASE WHEN est_vente_en_ligne THEN quantite ELSE 0 END) AS qte_vendue_en_ligne,
        SUM(CASE WHEN NOT est_vente_en_ligne THEN quantite ELSE 0 END) AS qte_vendue_magasin,

        -- Temporalité
        MIN(date_transaction)                       AS premiere_vente,
        MAX(date_transaction)                       AS derniere_vente,
        COUNT(DISTINCT mois_annee)                  AS nb_mois_avec_ventes

    FROM transactions
    GROUP BY produit_id
),

-- Rang et part de marché
classement AS (
    SELECT
        *,
        RANK() OVER (ORDER BY ca_total_xaf DESC)       AS rang_ca,
        RANK() OVER (ORDER BY quantite_totale_vendue DESC) AS rang_volume,
        ROUND(
            ca_total_xaf::NUMERIC / SUM(ca_total_xaf) OVER () * 100, 2
        )                                               AS part_ca_pct
    FROM performance
)

SELECT
    -- Référentiel produit
    p.produit_id,
    p.nom_produit,
    p.categorie,
    p.sous_categorie,
    p.prix_unitaire_xaf                             AS prix_catalogue_xaf,
    p.stock_initial,
    p.fournisseur,
    p.pays_origine,
    p.est_produit_local,
    p.segment_prix,

    -- Métriques de performance
    COALESCE(c.nb_transactions, 0)                  AS nb_transactions,
    COALESCE(c.nb_clients_uniques, 0)               AS nb_clients_uniques,
    COALESCE(c.nb_magasins_vendeurs, 0)             AS nb_magasins_vendeurs,
    COALESCE(c.quantite_totale_vendue, 0)           AS quantite_totale_vendue,
    COALESCE(c.ca_total_xaf, 0)                     AS ca_total_xaf,
    ROUND(COALESCE(c.ca_moyen_par_vente_xaf, 0), 0) AS ca_moyen_par_vente_xaf,
    COALESCE(c.prix_min_observe_xaf, p.prix_unitaire_xaf) AS prix_min_observe_xaf,
    COALESCE(c.prix_max_observe_xaf, p.prix_unitaire_xaf) AS prix_max_observe_xaf,
    COALESCE(c.qte_vendue_en_ligne, 0)              AS qte_vendue_en_ligne,
    COALESCE(c.qte_vendue_magasin, 0)               AS qte_vendue_magasin,

    -- Taux de vente en ligne
    ROUND(
        COALESCE(c.qte_vendue_en_ligne, 0)::NUMERIC
        / NULLIF(COALESCE(c.quantite_totale_vendue, 0), 0) * 100, 2
    )                                               AS taux_vente_en_ligne_pct,

    -- Classement
    COALESCE(c.rang_ca, 999)                        AS rang_ca,
    COALESCE(c.rang_volume, 999)                    AS rang_volume,
    COALESCE(c.part_ca_pct, 0)                      AS part_ca_pct,

    -- Temporalité
    c.premiere_vente,
    c.derniere_vente,
    COALESCE(c.nb_mois_avec_ventes, 0)              AS nb_mois_avec_ventes,

    -- Catégorie de performance
    CASE
        WHEN COALESCE(c.rang_ca, 999) <= 10 THEN 'Top 10 CA'
        WHEN COALESCE(c.part_ca_pct, 0) >= 5   THEN 'Produit clé'
        WHEN COALESCE(c.nb_transactions, 0) = 0 THEN 'Aucune vente'
        ELSE 'Produit standard'
    END                                             AS categorie_performance,

    CURRENT_TIMESTAMP                               AS dbt_updated_at

FROM produits p
LEFT JOIN classement c ON p.produit_id = c.produit_id
ORDER BY COALESCE(c.rang_ca, 999)
