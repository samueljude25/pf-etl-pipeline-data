-- ============================================================
-- Modèle staging : stg_magasins
-- Source : staging.magasins
-- Rôle : Normaliser le référentiel des magasins
-- Matérialisation : Vue
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('staging', 'magasins') }}
),

nettoyage AS (
    SELECT
        -- Identifiant magasin
        TRIM(magasin_id)                                        AS magasin_id,

        -- Descriptif magasin
        TRIM(nom_magasin)                                       AS nom_magasin,
        INITCAP(TRIM(ville))                                    AS ville,
        INITCAP(TRIM(quartier))                                 AS quartier,
        TRIM(adresse)                                           AS adresse,

        -- Gestion
        TRIM(responsable)                                       AS responsable,
        TRIM(telephone)                                         AS telephone,

        -- Caractéristiques physiques
        surface_m2::INT                                         AS surface_m2,
        DATE(date_ouverture)                                    AS date_ouverture,
        TRIM(statut)                                            AS statut,

        -- Zone géographique
        CASE TRIM(ville)
            WHEN 'Brazzaville'  THEN 'Zone Nord'
            WHEN 'Pointe-Noire' THEN 'Zone Océan'
            WHEN 'Dolisie'      THEN 'Zone Sud'
            WHEN 'Ouesso'       THEN 'Zone Nord'
            ELSE 'Autre zone'
        END                                                     AS zone_commerciale,

        -- Taille du magasin basée sur la surface
        CASE
            WHEN surface_m2::INT >= 600 THEN 'Grand'
            WHEN surface_m2::INT >= 350 THEN 'Moyen'
            ELSE 'Petit'
        END                                                     AS taille_magasin,

        -- Ancienneté en années
        DATE_PART('year', AGE(CURRENT_DATE, DATE(date_ouverture)))::INT AS anciennete_annees,

        CURRENT_TIMESTAMP                                       AS dbt_updated_at

    FROM source
    WHERE
        magasin_id IS NOT NULL
        AND magasin_id != ''
        AND TRIM(statut) = 'Actif'
)

SELECT * FROM nettoyage
