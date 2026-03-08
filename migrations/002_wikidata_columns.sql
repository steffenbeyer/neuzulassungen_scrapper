-- ============================================================
-- Migration 002: Wikidata/Wikipedia-Spalten hinzufuegen
-- Ermoeglicht die Zuordnung von Marken und Modellen zu
-- Wikidata-Entitaeten fuer automatische Datenanreicherung.
-- ============================================================

-- Marken: Wikidata-ID und Wikipedia-URL
ALTER TABLE marken
    ADD COLUMN IF NOT EXISTS wikidata_id VARCHAR(20) DEFAULT NULL AFTER website,
    ADD COLUMN IF NOT EXISTS wikipedia_url VARCHAR(500) DEFAULT NULL AFTER wikidata_id;

CREATE INDEX IF NOT EXISTS idx_marken_wikidata ON marken (wikidata_id);

-- Modelle: Wikidata-ID und Wikipedia-URL
ALTER TABLE modelle
    ADD COLUMN IF NOT EXISTS wikidata_id VARCHAR(20) DEFAULT NULL AFTER fahrzeugklasse,
    ADD COLUMN IF NOT EXISTS wikipedia_url VARCHAR(500) DEFAULT NULL AFTER wikidata_id;

CREATE INDEX IF NOT EXISTS idx_modelle_wikidata ON modelle (wikidata_id);
