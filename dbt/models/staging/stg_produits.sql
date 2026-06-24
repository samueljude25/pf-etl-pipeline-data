-- ============================================================
-- Modèle staging : stg_produits
-- Source : staging.produits
-- Rôle : Normaliser le référentiel produits
-- Matérialisation : Vue
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('staging', 'produits') }}
),

nettoyage AS (
    SELECT
        -- Identifiant produit
        TRIM(produit_id)                                        AS produit_id,

        -- Descriptif produit
        TRIM(nom_produit)                                       AS nom_produit,
        INITCAP(TRIM(categorie))                                AS categorie,
        INITCAP(TRIM(sous_categorie))                           AS sous_categorie,

        -- Tarification
        prix_unitaire_xaf::NUMERIC(15, 2)                       AS prix_unitaire_xaf,
        stock_initial::INT                                      AS stock_initial,

        -- Fournisseur
        TRIM(fournisseur)                                       AS fournisseur,
        TRIM(pays_origine)                                      AS pays_origine,

        -- Indicateur local (produit fabriqué au Congo)
        CASE
            WHEN TRIM(pays_origine) IN ('Congo', 'Congo-Brazzaville', 'RDC') THEN TRUE
            ELSE FALSE
        END                                                     AS est_produit_local,

        -- Segment de prix
        CASE
            WHEN prix_unitaire_xaf::NUMERIC >= 300000 THEN 'Prix élevé'
            WHEN prix_unitaire_xaf::NUMERIC >= 50000  THEN 'Prix moyen'
            WHEN prix_unitaire_xaf::NUMERIC >= 10000  THEN 'Prix abordable'
            ELSE 'Bas prix'
        END                                                     AS segment_prix,

        CURRENT_TIMESTAMP                                       AS dbt_updated_at

    FROM source
    WHERE produit_id IS NOT NULL AND produit_id != ''
)

SELECT * FROM nettoyage
