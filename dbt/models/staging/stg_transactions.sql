-- ============================================================
-- Modèle staging : stg_transactions
-- Source : staging.transactions (chargé depuis transactions.csv)
-- Rôle : Valider, typer et enrichir les transactions
-- Matérialisation : Vue
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('staging', 'transactions') }}
),

typage AS (
    SELECT
        -- Identifiants
        TRIM(transaction_id)                                    AS transaction_id,
        TRIM(client_id)                                         AS client_id,
        TRIM(magasin_id)                                        AS magasin_id,
        TRIM(produit_id)                                        AS produit_id,

        -- Dates
        DATE(date_transaction)                                  AS date_transaction,
        EXTRACT(YEAR  FROM DATE(date_transaction))::INT         AS annee,
        EXTRACT(MONTH FROM DATE(date_transaction))::INT         AS mois,
        EXTRACT(QUARTER FROM DATE(date_transaction))::INT       AS trimestre,
        TO_CHAR(DATE(date_transaction), 'YYYY-MM')              AS mois_annee,
        TO_CHAR(DATE(date_transaction), 'Day')                  AS jour_semaine,

        -- Nom du mois en français
        CASE EXTRACT(MONTH FROM DATE(date_transaction))
            WHEN 1  THEN 'Janvier'   WHEN 2  THEN 'Février'
            WHEN 3  THEN 'Mars'      WHEN 4  THEN 'Avril'
            WHEN 5  THEN 'Mai'       WHEN 6  THEN 'Juin'
            WHEN 7  THEN 'Juillet'   WHEN 8  THEN 'Août'
            WHEN 9  THEN 'Septembre' WHEN 10 THEN 'Octobre'
            WHEN 11 THEN 'Novembre'  WHEN 12 THEN 'Décembre'
        END                                                     AS nom_mois,

        -- Montants en XAF
        quantite::INT                                           AS quantite,
        prix_unitaire_xaf::NUMERIC(15, 2)                       AS prix_unitaire_xaf,
        montant_total_xaf::NUMERIC(15, 2)                       AS montant_total_xaf,

        -- Montant recalculé pour contrôle qualité
        (quantite::INT * prix_unitaire_xaf::NUMERIC)            AS montant_recalcule_xaf,

        -- Attributs commerciaux
        TRIM(mode_paiement)                                     AS mode_paiement,
        TRIM(statut)                                            AS statut_transaction,
        TRIM(canal_vente)                                       AS canal_vente,

        -- Indicateurs dérivés
        CASE TRIM(canal_vente)
            WHEN 'En ligne' THEN TRUE
            ELSE FALSE
        END                                                     AS est_vente_en_ligne,

        CASE
            WHEN montant_total_xaf::NUMERIC >= 200000 THEN 'Grande transaction'
            WHEN montant_total_xaf::NUMERIC >= 50000  THEN 'Transaction moyenne'
            ELSE 'Petite transaction'
        END                                                     AS segment_montant,

        CURRENT_TIMESTAMP                                       AS dbt_updated_at

    FROM source
    WHERE
        -- Contrôles qualité de base
        transaction_id IS NOT NULL
        AND transaction_id != ''
        AND date_transaction IS NOT NULL
        AND montant_total_xaf::NUMERIC BETWEEN {{ var('montant_min_valide') }} AND {{ var('montant_max_valide') }}
        AND quantite::INT > 0
        AND TRIM(statut) != 'Annulé'
),

deduplique AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY date_transaction DESC) AS rn
    FROM typage
)

SELECT
    transaction_id,
    client_id,
    magasin_id,
    produit_id,
    date_transaction,
    annee,
    mois,
    trimestre,
    mois_annee,
    nom_mois,
    jour_semaine,
    quantite,
    prix_unitaire_xaf,
    montant_total_xaf,
    montant_recalcule_xaf,
    mode_paiement,
    statut_transaction,
    canal_vente,
    est_vente_en_ligne,
    segment_montant,
    dbt_updated_at
FROM deduplique
WHERE rn = 1
