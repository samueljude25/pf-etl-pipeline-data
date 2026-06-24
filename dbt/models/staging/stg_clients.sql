-- ============================================================
-- Modèle staging : stg_clients
-- Source : staging.clients (chargé depuis clients.csv)
-- Rôle : Nettoyer, typer et renommer les colonnes clients
-- Matérialisation : Vue (actualisation automatique)
-- ============================================================

WITH source AS (
    -- Sélection brute depuis la table staging chargée par le pipeline ETL
    SELECT * FROM {{ source('staging', 'clients') }}
),

nettoyage AS (
    SELECT
        -- Identifiant client (clé primaire)
        TRIM(client_id)                                         AS client_id,

        -- Informations nominatives
        UPPER(TRIM(nom))                                        AS nom,
        INITCAP(TRIM(prenom))                                   AS prenom,
        TRIM(nom) || ' ' || INITCAP(TRIM(prenom))               AS nom_complet,

        -- Coordonnées (email normalisé en minuscules)
        LOWER(TRIM(email))                                      AS email,
        TRIM(telephone)                                         AS telephone,

        -- Localisation
        INITCAP(TRIM(ville))                                    AS ville,
        INITCAP(TRIM(quartier))                                 AS quartier,

        -- Démographie
        UPPER(TRIM(sexe))                                       AS sexe,
        TRIM(categorie)                                         AS categorie_client,

        -- Dates (conversion en type DATE PostgreSQL)
        DATE(date_inscription)                                  AS date_inscription,

        -- Colonnes dérivées
        CASE
            WHEN UPPER(TRIM(sexe)) = 'M' THEN 'Masculin'
            WHEN UPPER(TRIM(sexe)) = 'F' THEN 'Féminin'
            ELSE 'Non renseigné'
        END                                                     AS sexe_libelle,

        CASE TRIM(ville)
            WHEN 'Brazzaville'  THEN 'Pool'
            WHEN 'Pointe-Noire' THEN 'Kouilou'
            WHEN 'Dolisie'      THEN 'Niari'
            WHEN 'Ouesso'       THEN 'Sangha'
            WHEN 'Nkayi'        THEN 'Bouenza'
            WHEN 'Impfondo'     THEN 'Likouala'
            ELSE 'Autre'
        END                                                     AS departement,

        -- Indicateur client haute valeur
        CASE
            WHEN TRIM(categorie) IN ('Premium', 'VIP') THEN TRUE
            ELSE FALSE
        END                                                     AS est_client_premium,

        -- Métadonnées de traitement
        CURRENT_TIMESTAMP                                       AS dbt_updated_at

    FROM source
    WHERE
        -- Filtre qualité : exclure les lignes sans identifiant
        client_id IS NOT NULL
        AND client_id != ''
        AND nom IS NOT NULL
        AND nom != ''
),

-- Déduplication : conserver le dernier enregistrement par client_id
deduplique AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_inscription DESC) AS rn
    FROM nettoyage
)

SELECT
    client_id,
    nom,
    prenom,
    nom_complet,
    email,
    telephone,
    ville,
    quartier,
    departement,
    sexe,
    sexe_libelle,
    categorie_client,
    est_client_premium,
    date_inscription,
    dbt_updated_at
FROM deduplique
WHERE rn = 1
