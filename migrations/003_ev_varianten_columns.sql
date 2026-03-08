-- ============================================================
-- Migration 003: Zusaetzliche EV-Spalten fuer modell_varianten
-- Fuer Daten aus OpenEV Data (Ladeleistung, Performance)
-- ============================================================

ALTER TABLE modell_varianten
    ADD COLUMN IF NOT EXISTS dc_ladeleistung_kw DECIMAL(5,1) DEFAULT NULL AFTER batteriekapazitaet_kwh,
    ADD COLUMN IF NOT EXISTS ac_ladeleistung_kw DECIMAL(4,1) DEFAULT NULL AFTER dc_ladeleistung_kw,
    ADD COLUMN IF NOT EXISTS beschleunigung_0_100 DECIMAL(3,1) DEFAULT NULL AFTER ac_ladeleistung_kw,
    ADD COLUMN IF NOT EXISTS hoechstgeschwindigkeit_kmh SMALLINT DEFAULT NULL AFTER beschleunigung_0_100;
