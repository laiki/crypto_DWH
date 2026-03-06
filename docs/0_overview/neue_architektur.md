# Neue Architektur

## Ziel
VAULT 2.0 maximiert Zugriffsgeschwindigkeit durch dateibasierte Partitionierung, klare Indexstrategie und einen zentralen Manifest-Katalog fuer Query-Pruning.

## Kernprinzipien
1. Keine Symbol-Tabellen pro Coin.
2. Partitionierung ueber Dateien nach `exchange` und Zeitfenster (z. B. Stunde).
3. Normalisierte Filterspalten (`*_norm`) fuer indexfreundliche WHERE-Bedingungen.
4. Manifest-DB zur Vorauswahl relevanter Partitionen vor jedem Query.

## Datei-Layout
```text
data/vault2/
ingestion/exchange=binance/date=2026-03-06/hour=09/part_binance_20260306_09.db
ingestion/exchange=kraken/date=2026-03-06/hour=09/part_kraken_20260306_09.db
staging/exchange=binance/window=20260306_0800_0900/staging_binance_20260306_0800_0900.db
meta/vault_manifest.db
```

## Tabellenmodell pro Partition-DB
```sql
CREATE TABLE market_ticks (
  ingestion_ts_utc TEXT NOT NULL,
  ingestion_ts_epoch_s INTEGER NOT NULL,
  exchange_id TEXT NOT NULL,
  exchange_id_norm TEXT NOT NULL,
  symbol TEXT NOT NULL,
  symbol_norm TEXT NOT NULL,
  price REAL,
  bid REAL,
  ask REAL,
  last REAL
);

CREATE INDEX idx_ticks_pair_ts
  ON market_ticks(exchange_id_norm, symbol_norm, ingestion_ts_epoch_s);

CREATE INDEX idx_ticks_ts
  ON market_ticks(ingestion_ts_epoch_s);
```

## Manifest-DB fuer Partition-Pruning
```sql
CREATE TABLE partition_registry (
  partition_id TEXT PRIMARY KEY,
  layer TEXT NOT NULL,
  db_path TEXT NOT NULL,
  exchange_id_norm TEXT NOT NULL,
  ts_min_epoch_s INTEGER NOT NULL,
  ts_max_epoch_s INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  created_utc TEXT NOT NULL
);

CREATE TABLE partition_pairs (
  partition_id TEXT NOT NULL,
  exchange_id_norm TEXT NOT NULL,
  symbol_norm TEXT NOT NULL,
  ts_min_epoch_s INTEGER NOT NULL,
  ts_max_epoch_s INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (partition_id, exchange_id_norm, symbol_norm)
);

CREATE INDEX idx_registry_exchange_time
  ON partition_registry(exchange_id_norm, ts_min_epoch_s, ts_max_epoch_s);

CREATE INDEX idx_pairs_lookup
  ON partition_pairs(exchange_id_norm, symbol_norm, ts_min_epoch_s, ts_max_epoch_s);
```

## Forecast-Zugriffspfad
1. Cleansing liefert relevante `(exchange, symbol)`-Paare.
2. Manifest liefert dazu passende Partitionen vor dem `cutoff`.
3. Nur diese Dateien werden gelesen (`ATTACH` oder sequenziell).
4. Queries verwenden nur `*_norm`-Spalten ohne `lower(column)`.
5. Verarbeitung erfolgt optional chunk-basiert fuer stabilen RAM-Bedarf.

## Erwartete Performance-Effekte
1. Deutlich weniger Full-Scans.
2. Volle Indexnutzung durch sargable Filter.
3. Niedrigerer I/O durch Partition-Pruning.
4. Bessere Parallelisierbarkeit (Worker pro Exchange/Partition).

## Migrationsplan
1. `*_norm`-Spalten und Indizes in der aktuellen Struktur einfuehren.
2. Ingestion auf Exchange+Hour-Dateipartitionen umstellen.
3. Manifest bei jedem Partitionsschreibvorgang pflegen.
4. Staging-, Forecast- und Core-Reader auf Manifest-Pruning umstellen.
5. Alte monolithische Zugriffe entfernen.
