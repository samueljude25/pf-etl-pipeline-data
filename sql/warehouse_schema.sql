-- ============================================================
-- Schéma complet de l'entrepôt de données analytique
-- Pipeline ETL Commerce Congo — Samuel Jude SENDZI
-- Base : PostgreSQL 14+
-- ============================================================

-- ─── Création des schémas ─────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS 'Zone de staging : données brutes chargées par le pipeline ETL Python';

CREATE SCHEMA IF NOT EXISTS marts;
COMMENT ON SCHEMA marts IS 'Zone marts : données agrégées et enrichies pour le reporting';


-- ============================================================
-- ZONE STAGING : Tables sources
-- ============================================================

-- Table staging : Clients
DROP TABLE IF EXISTS staging.clients CASCADE;
CREATE TABLE staging.clients (
    client_id           VARCHAR(10)     NOT NULL,
    nom                 VARCHAR(100)    NOT NULL,
    prenom              VARCHAR(100),
    email               VARCHAR(200),
    telephone           VARCHAR(30),
    ville               VARCHAR(50),
    quartier            VARCHAR(100),
    date_inscription    DATE,
    sexe                CHAR(1),
    categorie           VARCHAR(20),
    anciennete_jours    INTEGER,
    segment_anciennete  VARCHAR(30),
    est_premium         BOOLEAN         DEFAULT FALSE,
    region              VARCHAR(50),
    -- Métadonnées de chargement
    loaded_at           TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id     VARCHAR(50),
    CONSTRAINT pk_staging_clients PRIMARY KEY (client_id)
);
COMMENT ON TABLE staging.clients IS 'Données clients brutes issues du CSV après nettoyage Python';
CREATE INDEX idx_staging_clients_ville ON staging.clients(ville);
CREATE INDEX idx_staging_clients_categorie ON staging.clients(categorie);


-- Table staging : Transactions
DROP TABLE IF EXISTS staging.transactions CASCADE;
CREATE TABLE staging.transactions (
    transaction_id          VARCHAR(10)     NOT NULL,
    date_transaction        DATE            NOT NULL,
    client_id               VARCHAR(10),
    magasin_id              VARCHAR(10),
    produit_id              VARCHAR(10),
    quantite                INTEGER         NOT NULL CHECK (quantite > 0),
    prix_unitaire_xaf       NUMERIC(15, 2)  NOT NULL,
    montant_total_xaf       NUMERIC(15, 2)  NOT NULL CHECK (montant_total_xaf > 0),
    mode_paiement           VARCHAR(30),
    statut                  VARCHAR(20)     DEFAULT 'Validé',
    canal_vente             VARCHAR(20),
    -- Colonnes enrichies par Python
    annee                   SMALLINT,
    mois                    SMALLINT,
    trimestre               SMALLINT,
    mois_annee              VARCHAR(7),
    nom_mois                VARCHAR(15),
    est_weekend             BOOLEAN,
    est_outlier             BOOLEAN         DEFAULT FALSE,
    montant_coherent        BOOLEAN         DEFAULT TRUE,
    -- Métadonnées
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id         VARCHAR(50),
    CONSTRAINT pk_staging_transactions PRIMARY KEY (transaction_id)
);
COMMENT ON TABLE staging.transactions IS 'Transactions commerciales nettoyées et enrichies par le pipeline Python';
CREATE INDEX idx_staging_tx_date ON staging.transactions(date_transaction);
CREATE INDEX idx_staging_tx_client ON staging.transactions(client_id);
CREATE INDEX idx_staging_tx_mois_annee ON staging.transactions(mois_annee);
CREATE INDEX idx_staging_tx_produit ON staging.transactions(produit_id);


-- Table staging : Produits
DROP TABLE IF EXISTS staging.produits CASCADE;
CREATE TABLE staging.produits (
    produit_id              VARCHAR(10)     NOT NULL,
    nom_produit             VARCHAR(200)    NOT NULL,
    categorie               VARCHAR(50),
    sous_categorie          VARCHAR(100),
    prix_unitaire_xaf       NUMERIC(15, 2),
    stock_initial           INTEGER,
    fournisseur             VARCHAR(200),
    pays_origine            VARCHAR(50),
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_staging_produits PRIMARY KEY (produit_id)
);
COMMENT ON TABLE staging.produits IS 'Référentiel produits chargé depuis produits.csv';
CREATE INDEX idx_staging_produits_categorie ON staging.produits(categorie);


-- Table staging : Magasins
DROP TABLE IF EXISTS staging.magasins CASCADE;
CREATE TABLE staging.magasins (
    magasin_id              VARCHAR(10)     NOT NULL,
    nom_magasin             VARCHAR(200),
    ville                   VARCHAR(50),
    quartier                VARCHAR(100),
    adresse                 TEXT,
    responsable             VARCHAR(200),
    telephone               VARCHAR(30),
    surface_m2              INTEGER,
    date_ouverture          DATE,
    statut                  VARCHAR(20)     DEFAULT 'Actif',
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_staging_magasins PRIMARY KEY (magasin_id)
);
COMMENT ON TABLE staging.magasins IS 'Référentiel des magasins chargé depuis magasins.csv';


-- Table staging : Journal des chargements
DROP TABLE IF EXISTS staging.journal_chargements;
CREATE TABLE staging.journal_chargements (
    id                      SERIAL          PRIMARY KEY,
    table_cible             VARCHAR(100)    NOT NULL,
    nb_lignes_source        INTEGER,
    nb_lignes_inserees      INTEGER,
    nb_lignes_rejetees      INTEGER         DEFAULT 0,
    strategie               VARCHAR(20),
    statut                  VARCHAR(20),
    erreur                  TEXT,
    debut_chargement        TIMESTAMP,
    fin_chargement          TIMESTAMP,
    duree_secondes          NUMERIC(10, 2),
    pipeline_run_id         VARCHAR(50),
    date_execution          DATE            DEFAULT CURRENT_DATE
);
COMMENT ON TABLE staging.journal_chargements IS 'Audit de tous les chargements du pipeline ETL';


-- ============================================================
-- ZONE MARTS : Tables analytiques
-- ============================================================

-- Mart : Chiffre d'affaires mensuel
DROP TABLE IF EXISTS marts.ca_mensuel CASCADE;
CREATE TABLE marts.ca_mensuel (
    id                          SERIAL          PRIMARY KEY,
    mois_annee                  VARCHAR(7)      NOT NULL,
    annee                       SMALLINT        NOT NULL,
    mois                        SMALLINT        NOT NULL,
    trimestre                   SMALLINT,
    ville_magasin               VARCHAR(50),
    magasin_id                  VARCHAR(10),
    nb_transactions             INTEGER         DEFAULT 0,
    nb_clients_uniques          INTEGER         DEFAULT 0,
    ca_total_xaf                NUMERIC(18, 2)  DEFAULT 0,
    ca_moyen_par_transaction_xaf NUMERIC(15, 2) DEFAULT 0,
    ca_moyen_par_client_xaf     NUMERIC(15, 2)  DEFAULT 0,
    quantite_totale             INTEGER         DEFAULT 0,
    montant_min_xaf             NUMERIC(15, 2),
    montant_max_xaf             NUMERIC(15, 2),
    dbt_updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ca_mensuel UNIQUE (mois_annee, magasin_id)
);
COMMENT ON TABLE marts.ca_mensuel IS 'CA mensuel par magasin — grain : mois × magasin';
CREATE INDEX idx_marts_ca_mois ON marts.ca_mensuel(mois_annee);
CREATE INDEX idx_marts_ca_ville ON marts.ca_mensuel(ville_magasin);


-- Mart : Top produits
DROP TABLE IF EXISTS marts.top_produits CASCADE;
CREATE TABLE marts.top_produits (
    produit_id              VARCHAR(10)     NOT NULL,
    nom_produit             VARCHAR(200),
    categorie_produit       VARCHAR(50),
    nb_ventes               INTEGER         DEFAULT 0,
    quantite_totale         INTEGER         DEFAULT 0,
    ca_total_xaf            NUMERIC(18, 2)  DEFAULT 0,
    nb_magasins             INTEGER         DEFAULT 0,
    nb_clients              INTEGER         DEFAULT 0,
    prix_moyen_xaf          NUMERIC(15, 2),
    rang_ca                 INTEGER,
    part_ca_pct             NUMERIC(6, 2),
    dbt_updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_top_produits PRIMARY KEY (produit_id)
);
COMMENT ON TABLE marts.top_produits IS 'Performance commerciale des produits sur toute la période';
CREATE INDEX idx_marts_produits_rang ON marts.top_produits(rang_ca);


-- Mart : Performance des magasins
DROP TABLE IF EXISTS marts.performance_magasins CASCADE;
CREATE TABLE marts.performance_magasins (
    magasin_id              VARCHAR(10)     NOT NULL,
    nom_magasin             VARCHAR(200),
    ville_magasin           VARCHAR(50),
    nb_transactions         INTEGER         DEFAULT 0,
    nb_clients_uniques      INTEGER         DEFAULT 0,
    ca_total_xaf            NUMERIC(18, 2)  DEFAULT 0,
    panier_moyen_xaf        NUMERIC(15, 2),
    quantite_totale         INTEGER         DEFAULT 0,
    nb_produits_vendus      INTEGER         DEFAULT 0,
    rang_ca                 INTEGER,
    ca_mensuel_moyen_xaf    NUMERIC(15, 2),
    dbt_updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_performance_magasins PRIMARY KEY (magasin_id)
);
COMMENT ON TABLE marts.performance_magasins IS 'Performance commerciale par magasin sur toute la période';


-- Mart : Segmentation RFM clients
DROP TABLE IF EXISTS marts.segmentation_rfm CASCADE;
CREATE TABLE marts.segmentation_rfm (
    client_id               VARCHAR(10)     NOT NULL,
    derniere_transaction    TIMESTAMP,
    nb_transactions         INTEGER         DEFAULT 0,
    ca_total_xaf            NUMERIC(18, 2)  DEFAULT 0,
    recence_jours           INTEGER,
    panier_moyen_xaf        NUMERIC(15, 2),
    score_R                 SMALLINT,
    score_F                 SMALLINT,
    score_M                 SMALLINT,
    score_RFM               SMALLINT,
    segment_rfm             VARCHAR(30),
    dbt_updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_segmentation_rfm PRIMARY KEY (client_id)
);
COMMENT ON TABLE marts.segmentation_rfm IS 'Scores et segments RFM par client';
CREATE INDEX idx_rfm_segment ON marts.segmentation_rfm(segment_rfm);
CREATE INDEX idx_rfm_score ON marts.segmentation_rfm(score_RFM DESC);


-- ============================================================
-- VUES ANALYTIQUES DE REPORTING
-- ============================================================

-- Vue : Résumé du tableau de bord exécutif
CREATE OR REPLACE VIEW marts.v_tableau_bord_executif AS
SELECT
    MAX(mois_annee)                         AS derniere_periode,
    SUM(ca_total_xaf)                       AS ca_total_global_xaf,
    SUM(nb_transactions)                    AS nb_transactions_total,
    SUM(nb_clients_uniques)                 AS nb_clients_actifs,
    ROUND(AVG(ca_moyen_par_transaction_xaf), 0) AS panier_moyen_global_xaf,
    COUNT(DISTINCT magasin_id)              AS nb_magasins_actifs,
    SUM(quantite_totale)                    AS quantite_totale_vendue
FROM marts.ca_mensuel;

COMMENT ON VIEW marts.v_tableau_bord_executif IS
    'Vue exécutive : KPIs globaux du commerce sur toute la période';


-- Vue : Évolution mensuelle du CA (pour graphiques tendance)
CREATE OR REPLACE VIEW marts.v_evolution_ca AS
SELECT
    mois_annee,
    annee,
    mois,
    trimestre,
    SUM(ca_total_xaf)               AS ca_mensuel_xaf,
    SUM(nb_transactions)            AS nb_transactions,
    SUM(nb_clients_uniques)         AS nb_clients,
    ROUND(AVG(ca_moyen_par_transaction_xaf), 0) AS panier_moyen_xaf,
    LAG(SUM(ca_total_xaf)) OVER (ORDER BY mois_annee) AS ca_mois_precedent_xaf,
    ROUND(
        (SUM(ca_total_xaf) - LAG(SUM(ca_total_xaf)) OVER (ORDER BY mois_annee))
        / NULLIF(LAG(SUM(ca_total_xaf)) OVER (ORDER BY mois_annee), 0) * 100,
        2
    )                               AS croissance_mois_pct
FROM marts.ca_mensuel
GROUP BY mois_annee, annee, mois, trimestre
ORDER BY mois_annee;

COMMENT ON VIEW marts.v_evolution_ca IS 'Évolution mensuelle du CA avec taux de croissance M/M-1';


-- ============================================================
-- INDEXATION SUPPLÉMENTAIRE POUR LES PERFORMANCES
-- ============================================================

-- Index composite pour les requêtes analytiques fréquentes
CREATE INDEX IF NOT EXISTS idx_staging_tx_annee_mois
    ON staging.transactions(annee, mois);

CREATE INDEX IF NOT EXISTS idx_staging_tx_statut_date
    ON staging.transactions(statut, date_transaction);

CREATE INDEX IF NOT EXISTS idx_staging_clients_premium
    ON staging.clients(est_premium, categorie);

-- ============================================================
-- FIN DU SCRIPT
-- ============================================================
