-- ============================================================
-- Migration 004: Fuel estimation support
-- Adds data source for estimated fuel type data from FZ28->FZ10 mapping
-- ============================================================

-- Add estimation data source
INSERT IGNORE INTO datenquellen (land_id, kuerzel, name, typ, url_pattern) VALUES
(1, 'FZ10_EST', 'Geschaetzte Kraftstoff-Aufschluesselung (FZ10+FZ28)', 'monatlich', NULL);

-- Add estimation_method column to track how fuel data was derived
ALTER TABLE neuzulassungen
ADD COLUMN estimation_method VARCHAR(50) NULL DEFAULT NULL
AFTER kraftstoff;
