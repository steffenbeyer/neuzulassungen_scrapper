-- ============================================================
-- AlleZulassungen - Initiales Datenbank-Schema
-- Multi-Country-faehig, vorbereitet fuer CMS-Erweiterung
-- ============================================================

-- Laender (vorbereitet fuer europaweite Expansion)
CREATE TABLE IF NOT EXISTS laender (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code CHAR(2) NOT NULL,
    name VARCHAR(100) NOT NULL,
    UNIQUE KEY idx_laender_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO laender (code, name) VALUES ('DE', 'Deutschland');

-- Datenquellen (KBA, ACEA, etc.)
CREATE TABLE IF NOT EXISTS datenquellen (
    id INT AUTO_INCREMENT PRIMARY KEY,
    land_id INT NOT NULL,
    kuerzel VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    typ ENUM('monatlich', 'jaehrlich') NOT NULL,
    url_pattern VARCHAR(500),
    UNIQUE KEY idx_dq_kuerzel (land_id, kuerzel),
    CONSTRAINT fk_dq_land FOREIGN KEY (land_id) REFERENCES laender(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Deutsche KBA-Datenquellen einfuegen
INSERT IGNORE INTO datenquellen (land_id, kuerzel, name, typ, url_pattern) VALUES
(1, 'FZ10', 'Neuzulassungen PKW nach Marken und Modellreihen', 'monatlich',
 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ10/fz10_{YYYY}_{MM}.xlsx?__blob=publicationFile&v=2'),
(1, 'FZ11', 'Neuzulassungen PKW nach Segmenten und Modellreihen', 'monatlich',
 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ11/fz11_{YYYY}_{MM}.xlsx?__blob=publicationFile&v=2'),
(1, 'FZ8', 'Neuzulassungen Kraftfahrzeuge nach Merkmalen', 'monatlich',
 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ8/fz8_{YYYY}_{MM}.xlsx?__blob=publicationFile&v=2'),
(1, 'FZ9', 'Besitzumschreibungen Kraftfahrzeuge', 'monatlich',
 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ9/fz9_{YYYY}_{MM}.xlsx?__blob=publicationFile&v=2'),
(1, 'FZ4', 'Neuzulassungen nach Herstellern und Handelsnamen', 'jaehrlich',
 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ4/fz4_{YYYY}.xlsx?__blob=publicationFile&v=2'),
(1, 'FZ14', 'Neuzulassungen nach Umwelt-Merkmalen', 'jaehrlich',
 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ14/fz14_{YYYY}.xlsx?__blob=publicationFile&v=2'),
(1, 'FZ28', 'Neuzulassungen mit alternativem Antrieb', 'jaehrlich',
 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ28/fz28_{YYYY}.xlsx?__blob=publicationFile&v=2'),
(1, 'FZ1', 'Fahrzeugbestand', 'jaehrlich',
 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ1/fz1_{YYYY}.xlsx?__blob=publicationFile&v=2');

-- Marken-Stammdaten
CREATE TABLE IF NOT EXISTS marken (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(100) NOT NULL,
    logo_url VARCHAR(500),
    herkunftsland CHAR(2),
    gruendungsjahr SMALLINT,
    beschreibung TEXT,
    website VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_marken_name (name),
    UNIQUE KEY idx_marken_slug (slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Modell-Stammdaten
CREATE TABLE IF NOT EXISTS modelle (
    id INT AUTO_INCREMENT PRIMARY KEY,
    marke_id INT NOT NULL,
    name VARCHAR(200) NOT NULL,
    slug VARCHAR(200) NOT NULL,
    segment VARCHAR(50),
    bild_url VARCHAR(500),
    beschreibung TEXT,
    bauzeit_von SMALLINT,
    bauzeit_bis SMALLINT,
    fahrzeugklasse VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_modelle_marke_name (marke_id, name),
    INDEX idx_modelle_slug (slug),
    CONSTRAINT fk_modelle_marke FOREIGN KEY (marke_id) REFERENCES marken(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Technische Daten pro Modell-Variante (Phase 6 - CMS)
CREATE TABLE IF NOT EXISTS modell_varianten (
    id INT AUTO_INCREMENT PRIMARY KEY,
    modell_id INT NOT NULL,
    name VARCHAR(200) NOT NULL,
    kraftstoff VARCHAR(50),
    leistung_ps SMALLINT,
    leistung_kw SMALLINT,
    hubraum_ccm SMALLINT,
    getriebe VARCHAR(50),
    antrieb VARCHAR(50),
    verbrauch_l_100km DECIMAL(4,1),
    co2_g_km SMALLINT,
    reichweite_km SMALLINT,
    batteriekapazitaet_kwh DECIMAL(5,1),
    leergewicht_kg SMALLINT,
    laenge_mm SMALLINT UNSIGNED,
    breite_mm SMALLINT UNSIGNED,
    hoehe_mm SMALLINT UNSIGNED,
    kofferraum_liter SMALLINT UNSIGNED,
    grundpreis_euro INT,
    baujahr_von SMALLINT,
    baujahr_bis SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_variante_modell FOREIGN KEY (modell_id) REFERENCES modelle(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bilder-Galerie (Phase 6 - CMS)
CREATE TABLE IF NOT EXISTS bilder (
    id INT AUTO_INCREMENT PRIMARY KEY,
    marke_id INT,
    modell_id INT,
    url VARCHAR(500) NOT NULL,
    alt_text VARCHAR(300),
    typ ENUM('logo', 'hero', 'galerie', 'thumbnail') NOT NULL,
    sortierung SMALLINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_bild_marke FOREIGN KEY (marke_id) REFERENCES marken(id),
    CONSTRAINT fk_bild_modell FOREIGN KEY (modell_id) REFERENCES modelle(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Neuzulassungen pro Modell (Kerntabelle)
CREATE TABLE IF NOT EXISTS neuzulassungen (
    id INT AUTO_INCREMENT PRIMARY KEY,
    land_id INT NOT NULL,
    modell_id INT NOT NULL,
    jahr SMALLINT NOT NULL,
    monat SMALLINT NOT NULL,
    anzahl INT NOT NULL,
    kraftstoff VARCHAR(50),
    quelle_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_nzl_unique (land_id, modell_id, jahr, monat, kraftstoff, quelle_id),
    INDEX idx_nzl_zeit (jahr, monat),
    INDEX idx_nzl_land_zeit (land_id, jahr, monat),
    INDEX idx_nzl_modell (modell_id),
    CONSTRAINT fk_nzl_land FOREIGN KEY (land_id) REFERENCES laender(id),
    CONSTRAINT fk_nzl_modell FOREIGN KEY (modell_id) REFERENCES modelle(id),
    CONSTRAINT fk_nzl_quelle FOREIGN KEY (quelle_id) REFERENCES datenquellen(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Aggregierte Daten (FZ8, regionale Daten, etc.)
CREATE TABLE IF NOT EXISTS neuzulassungen_aggregiert (
    id INT AUTO_INCREMENT PRIMARY KEY,
    land_id INT NOT NULL,
    jahr SMALLINT NOT NULL,
    monat SMALLINT NOT NULL,
    fahrzeugart VARCHAR(100),
    region VARCHAR(100),
    kraftstoff VARCHAR(50),
    anzahl INT NOT NULL,
    quelle_id INT NOT NULL,
    INDEX idx_agg_land_zeit (land_id, jahr, monat),
    INDEX idx_agg_region (region),
    CONSTRAINT fk_agg_land FOREIGN KEY (land_id) REFERENCES laender(id),
    CONSTRAINT fk_agg_quelle FOREIGN KEY (quelle_id) REFERENCES datenquellen(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Fahrzeugbestand (aus FZ1, jaehrlich)
CREATE TABLE IF NOT EXISTS fahrzeugbestand (
    id INT AUTO_INCREMENT PRIMARY KEY,
    land_id INT NOT NULL,
    modell_id INT,
    marke_id INT,
    jahr SMALLINT NOT NULL,
    anzahl INT NOT NULL,
    kraftstoff VARCHAR(50),
    quelle_id INT NOT NULL,
    INDEX idx_bestand_land_jahr (land_id, jahr),
    INDEX idx_bestand_marke (marke_id),
    CONSTRAINT fk_bestand_land FOREIGN KEY (land_id) REFERENCES laender(id),
    CONSTRAINT fk_bestand_modell FOREIGN KEY (modell_id) REFERENCES modelle(id),
    CONSTRAINT fk_bestand_marke FOREIGN KEY (marke_id) REFERENCES marken(id),
    CONSTRAINT fk_bestand_quelle FOREIGN KEY (quelle_id) REFERENCES datenquellen(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Import-Log (Tracking welche Dateien bereits importiert wurden)
CREATE TABLE IF NOT EXISTS import_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    quelle_id INT NOT NULL,
    dateiname VARCHAR(500) NOT NULL,
    jahr SMALLINT,
    monat SMALLINT,
    status ENUM('pending', 'running', 'success', 'error') NOT NULL DEFAULT 'pending',
    rows_imported INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_import_datei (quelle_id, dateiname),
    INDEX idx_import_status (status),
    CONSTRAINT fk_import_quelle FOREIGN KEY (quelle_id) REFERENCES datenquellen(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
