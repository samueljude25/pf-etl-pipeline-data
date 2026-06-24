-- ============================================================
-- Modèle mart : mart_clients
-- Grain : 1 ligne par client
-- Rôle : Dimension clients enrichie avec métriques comportementales RFM
-- Matérialisation : Table
-- ============================================================

WITH clients AS (
    SELECT * FROM {{ ref('stg_clients') }}
),

transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

-- Métriques transactionnelles par client
metriques_client AS (
    SELECT
        client_id,

        -- Volume d'achats
        COUNT(DISTINCT transaction_id)          AS nb_achats_total,
        SUM(montant_total_xaf)                  AS ca_total_xaf,
        AVG(montant_total_xaf)                  AS panier_moyen_xaf,
        MAX(montant_total_xaf)                  AS achat_max_xaf,
        SUM(quantite)                           AS quantite_totale_achetee,

        -- Temporalité
        MIN(date_transaction)                   AS premiere_transaction,
        MAX(date_transaction)                   AS derniere_transaction,
        COUNT(DISTINCT mois_annee)              AS nb_mois_actifs,
        COUNT(DISTINCT annee)                   AS nb_annees_actif,

        -- Récence (jours depuis le dernier achat)
        (CURRENT_DATE - MAX(date_transaction))  AS recence_jours,

        -- Fréquence mensuelle moyenne
        ROUND(
            COUNT(DISTINCT transaction_id)::NUMERIC
            / NULLIF(COUNT(DISTINCT mois_annee), 0), 2
        )                                       AS frequence_mensuelle,

        -- Diversité des achats
        COUNT(DISTINCT produit_id)              AS nb_produits_differents,
        COUNT(DISTINCT magasin_id)              AS nb_magasins_frequentes,

        -- Canal préféré
        MODE() WITHIN GROUP (ORDER BY canal_vente) AS canal_prefere,
        MODE() WITHIN GROUP (ORDER BY mode_paiement) AS mode_paiement_prefere

    FROM transactions
    GROUP BY client_id
),

-- Scores RFM (quintiles 1-5, 5 = meilleur)
scores_rfm AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recence_jours DESC)     AS score_recence,
        NTILE(5) OVER (ORDER BY nb_achats_total ASC)    AS score_frequence,
        NTILE(5) OVER (ORDER BY ca_total_xaf ASC)       AS score_montant
    FROM metriques_client
),

segmentation AS (
    SELECT
        *,
        ROUND((score_recence + score_frequence + score_montant)::NUMERIC / 3, 2) AS score_rfm_moyen,

        CASE
            WHEN (score_recence + score_frequence + score_montant) >= 12 THEN 'Champions'
            WHEN (score_recence + score_frequence + score_montant) >= 9  THEN 'Clients fidèles'
            WHEN score_recence <= 2 AND score_frequence >= 3             THEN 'Clients à risque'
            WHEN recence_jours > 180                                     THEN 'Clients inactifs'
            ELSE 'Clients en développement'
        END                                                                        AS segment_rfm
    FROM scores_rfm
)

-- Assemblage final : dimensions + métriques
SELECT
    -- Données du client
    c.client_id,
    c.nom,
    c.prenom,
    c.nom_complet,
    c.email,
    c.ville,
    c.quartier,
    c.departement,
    c.sexe,
    c.sexe_libelle,
    c.categorie_client,
    c.est_client_premium,
    c.date_inscription,

    -- Métriques transactionnelles
    COALESCE(s.nb_achats_total, 0)              AS nb_achats_total,
    COALESCE(s.ca_total_xaf, 0)                 AS ca_total_xaf,
    ROUND(COALESCE(s.panier_moyen_xaf, 0), 0)   AS panier_moyen_xaf,
    COALESCE(s.achat_max_xaf, 0)                AS achat_max_xaf,
    COALESCE(s.quantite_totale_achetee, 0)      AS quantite_totale_achetee,
    s.premiere_transaction,
    s.derniere_transaction,
    COALESCE(s.recence_jours, 9999)             AS recence_jours,
    COALESCE(s.nb_mois_actifs, 0)               AS nb_mois_actifs,
    COALESCE(s.frequence_mensuelle, 0)          AS frequence_mensuelle,
    COALESCE(s.nb_produits_differents, 0)       AS nb_produits_differents,
    COALESCE(s.nb_magasins_frequentes, 0)       AS nb_magasins_frequentes,
    s.canal_prefere,
    s.mode_paiement_prefere,

    -- Scores et segmentation RFM
    COALESCE(s.score_recence, 1)                AS score_recence,
    COALESCE(s.score_frequence, 1)              AS score_frequence,
    COALESCE(s.score_montant, 1)                AS score_montant,
    COALESCE(s.score_rfm_moyen, 1.0)            AS score_rfm_moyen,
    COALESCE(s.segment_rfm, 'Jamais acheté')    AS segment_rfm,

    -- Est client actif (achat dans les 90 derniers jours)
    CASE WHEN COALESCE(s.recence_jours, 9999) <= 90 THEN TRUE ELSE FALSE END AS est_client_actif,

    CURRENT_TIMESTAMP                           AS dbt_updated_at

FROM clients c
LEFT JOIN segmentation s ON c.client_id = s.client_id
ORDER BY COALESCE(s.ca_total_xaf, 0) DESC
