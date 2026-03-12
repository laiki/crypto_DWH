# Projektarbeit Logbuch

Zusammengeführt aus `docs/KW11/Tag1.txt` und `docs/KW11/Tag 2.txt`.

## Tag 1

- Projektvorstellung vorbereiten
An dem Projekt wurde schon seit 2 Wochen gearbeitet. 
Es steht unter https://github.com/laiki/crypto_DWH/ als open source Projekt mit MIT Lizenz z.V.
Die Architektur hat sich ende Letzter Woche geändert und soll die ETL Prozesse Daten über Redis key/value publish/subscribe Mechanismen routen.
Bisher wurden Daten von den Handelsplattformen empfangen und direkt in eine SQLite DB geschrieben.
Die Neue Architektur sieht vor, diesen Prozess aufzuteilen. 
Daten können dann von unterschiedlichen Plattformen und Quellen über eine Redis Instanz (soll im Docker/Podman Container laufen) publisht.
Subscriber lesen die Daten und bringen sie in das gewünschte Format, um sie in der Datenbank zu speichern.
Außerdem, soll die Datenarchitektur "Vault 2.0" zum Einsatz kommen um Datenbank Zugriffe performanter zu gestallten. 

- Prove of Concept (PoC) der neue Architektur umsetzen. 
Verwendung von Podman statt Docker um Redis zu starten. 
Skripte für Umgang mit Redis anpassen.

- Datensammlung über Nacht starten 
Die vergangenen 2 Wochen haben gezeigt, dass 

+ **ein lokaler Prozess aus dem Heimnetzwerk nicht sinnvoll möglich ist da der Internetprovider nachts kurzfristig die Verbindung neu aufbaut. (IP-reset)**
Prozesse die kontinuierlich Daten über Internet-Socketverbindungen wie Web-Sockets, RestAPIs empfangen, werden dadurch unterbrochen.
Eine Fehlerbehandlung zum Wiederaufbau der Verbindungen ist zwar technisch möglich, jedoch aufwändig, fehlernfällig, schlecht debugbar und außerdem fehlen Daten für den entsprechenden Zeitraum.
Die beste Lösung scheint den Prozess auf einen gehosteten Server laufen zu lassen. z.B. Contabo für knapp 5€/Monat.
+ **Eine sehr hohe Menge an Daten zusammengetragen wird.**
Erste Tests ließen vermuten, dass ohne Filterung auf Symbole und Handelsplattformen mit ca. 90GB/Tag an Daten zu rechnen ist. 
Wenn logging im DEBUG Level läuft, kann durchaus mit doppelt so viel Festplattenplatz gerechnet werden.
Ich habe mich daher entschieden, eine ausgewählte Liste von Symbolen abzufragen. 
Diese sind BTC/*, ETH/*, SOL/*, ADA/*

- Umsetzungsplan der nächsten Tage dokumentieren
Der Plan sieht folgendermaßen aus:

- Tag 1: 

- [x] Neue Architektur dokumentieren         
- [x] Diagramme updaten        
- [x] Skripte anpassen        
- [x] 1 Stunde Datensammlung für einen Testlauf        
- [x] Präsentation Überarbeiten        
- [x] Datensammlung über Nacht starten

- Tag 2:

- [x] Daten-Pipeline laufen lassen
- [x] Ergebnisse im Dashboard prüfen

- Tag 3:

- [x] ML forecast Modelle trainieren
- [x] forecasts durchführen und in DB speichern
- [x] Dashboard um forecasts erweitern
- [x] Wenn möglich Deep Learning forecasts hinzufügen

- Tag 4:  
- [ ] Präsentation anpassen

- Tag 5:  
- [ ] Präsentation

## Tag 2

### Datensammlung prüfen
gestriger Lauf von 

- *python scripts/1_ingestion/redis_stream_to_vault_writer.py --log-level INFO --duration-seconds 44000*
 war wie erwartet beendet, sicherlich nach der angegebenen Dauer von 12,5 Stunden
 Die erzeugten Datenbanken in eeiner Vault2 Verzeichnisstruktur ist enthalten in vault2_tree.txt

- *python scripts/1_ingestion/orchestrator_redis_auto_shard.py --workers 3 --only-spot --duration-seconds 43200 *
die Prozesse orchestrator_redis_auto_shard und die zugehörigen 3 worker waren nicht beendet, aber die Worker hatten laut Logs nach 12 Stunden aufgehört Daten zu empfangen.
Ein fix im Beenden nach Ablauf der Zeit wurde implementiert.
Offensichtlich habe ich vergessen die Symbole einzuschränken. Mal sehen wieviele Symbole enthalten sind.

- Redis Container gestoppt
``` 
(base) wgo@vmd191183:~$ podman ps
CONTAINER ID  IMAGE                           COMMAND               CREATED       STATUS       PORTS                   NAMES
8d2f74b9cbf4  docker.io/library/redis:trixie  redis-server --ap...  21 hours ago  Up 19 hours  0.0.0.0:6379->6379/tcp  crypto-dwh-redis
(base) wgo@vmd191183:~$ podman stop 8d2f74b9cbf4
8d2f74b9cbf4
(base) wgo@vmd191183:~$`
``` 

# Pipeline 

##Staging: 3 x jeweils eine Stunde exportieren aus dem nächtlichen 12 Stunden Lauf

* Exports the last N hours from worker_*.db files.
  Supports output formats: sqlite (default), csv, json.
  Supports filters for exchanges and assets.
  Prints unique exchange and asset lists from all selected source DBs.
  Writes run metadata as JSON for traceability.
  Supports incremental export mode with persisted watermark state.*

```
time \ 
for offset in 0 1 2 3 4 5 6 7 8; do
echo $offset
python scripts/2_staging/staging_exporter.py \
--vault-root data/vault2_redis \
--hours 1 \
--start-relative-from-hour "$offset" \
--output-dir data/staging \
--log-level INFO
done 
```

**hat keine Minute gedauert**

```

(crypto) wgo@vmd191183:~/dev/crypto_DWH$ tree -fh data/staging
[4.0K]  data/staging
├── [ 76M]  data/staging/staging_export_20260310_083648_last_1h.db
├── [ 32K]  data/staging/staging_export_20260310_083648_last_1h.db-shm
├── [   0]  data/staging/staging_export_20260310_083648_last_1h.db-wal
├── [ 69M]  data/staging/staging_export_20260310_083651_last_1h_from_1h.db
├── [ 32K]  data/staging/staging_export_20260310_083651_last_1h_from_1h.db-shm
├── [   0]  data/staging/staging_export_20260310_083651_last_1h_from_1h.db-wal
├── [ 69M]  data/staging/staging_export_20260310_083654_last_1h_from_2h.db
├── [ 32K]  data/staging/staging_export_20260310_083654_last_1h_from_2h.db-shm
├── [   0]  data/staging/staging_export_20260310_083654_last_1h_from_2h.db-wal
├── [ 55M]  data/staging/staging_export_20260310_130421_last_1h_from_8h.db
├── [ 76M]  data/staging/staging_export_20260310_131035_last_1h.db
├── [ 69M]  data/staging/staging_export_20260310_131038_last_1h_from_1h.db
├── [ 69M]  data/staging/staging_export_20260310_131040_last_1h_from_2h.db
├── [ 71M]  data/staging/staging_export_20260310_131042_last_1h_from_3h.db
├── [ 71M]  data/staging/staging_export_20260310_131044_last_1h_from_4h.db
├── [ 74M]  data/staging/staging_export_20260310_131046_last_1h_from_5h.db
├── [ 73M]  data/staging/staging_export_20260310_131048_last_1h_from_6h.db
├── [ 71M]  data/staging/staging_export_20260310_131050_last_1h_from_7h.db
├── [ 72M]  data/staging/staging_export_20260310_131051_last_1h_from_8h.db
├── [ 76M]  data/staging/staging_export_20260310_131108_last_1h.db
├── [ 69M]  data/staging/staging_export_20260310_131110_last_1h_from_1h.db
├── [ 69M]  data/staging/staging_export_20260310_131111_last_1h_from_2h.db
├── [ 71M]  data/staging/staging_export_20260310_131112_last_1h_from_3h.db
├── [ 71M]  data/staging/staging_export_20260310_131114_last_1h_from_4h.db
├── [ 74M]  data/staging/staging_export_20260310_131115_last_1h_from_5h.db
├── [ 73M]  data/staging/staging_export_20260310_131116_last_1h_from_6h.db
├── [ 71M]  data/staging/staging_export_20260310_131118_last_1h_from_7h.db
└── [ 72M]  data/staging/staging_export_20260310_131119_last_1h_from_8h.db

1 directory, 28 files
cross check einer Staging DB:

  (crypto) wgo@vmd191183:~/dev/crypto_DWH$ sqlite3 data/staging/staging_export_20260310_083648_last_1h.db
  SQLite version 3.51.2 2026-01-09 17:27:48
  Enter ".help" for usage hints.
  sqlite> .schema
  CREATE TABLE market_ticks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source_partition_id TEXT NOT NULL,
              source_db_path TEXT NOT NULL,
              source_row_id INTEGER NOT NULL,
              ingestion_ts_utc TEXT NOT NULL,
              ingestion_ts_epoch_s INTEGER NOT NULL,
              exchange_id TEXT NOT NULL,
              exchange_id_norm TEXT NOT NULL,
              symbol TEXT NOT NULL,
              symbol_norm TEXT NOT NULL,
              asset_norm TEXT NOT NULL,
              market_type TEXT,
              price REAL,
              bid REAL,
              ask REAL,
              last REAL,
              open REAL,
              high REAL,
              low REAL,
              base_volume REAL,
              quote_volume REAL,
              exchange_ts_ms INTEGER,
              exchange_ts_utc TEXT,
              raw_json TEXT NOT NULL
          );
  ...    

  sqlite> select count(*) from market_ticks;
  70304
  sqlite> select count(distinct(symbol_norm)) from market_ticks;
  753

    sqlite> select distinct(exchange_id) from market_ticks;
    aster
    backpack
    binance
    binanceus
    bingx
    bitget
    bithumb
    bittrade
    coinbaseadvanced
    coinex
    coinone
    deribit
    exmo
    gate
    gateio
    hyperliquid
    kraken
    ndax
    phemex
    upbit
    woo
    xt
    sqlite> select count(distinct(exchange_id)) from market_ticks;
    22
    sqlite>

    753 unterschiedliche Symbole aus 22 Plattformen
```

##Cleansing: Alle 9 x 1 Stunde Exporte durchlaufen den Cleansing-Prozess
  
+ Data quality checks (--enable-dq, default enabled):
+ NULL ingestion timestamp and NULL effective price checks.
+ Invalid timestamp parse checks.
+ Non-positive price checks (<= 0).
+ Negative numeric value checks (price, bid, ask, last, base_volume, quote_volume).
+ bid > ask consistency checks.
+ Duplicate tick checks (same pair + timestamp + effective price).
+ Outlier return checks on observed bins (--dq-outlier-threshold-pct).

```
time\
for s in `ls data/staging/*.db`  ; do   
  python ./scripts/3_cleansing/cleansing_resample.py  --input-db $s --fill-method interpolation --workers 4    
done
```
    
**knapp 3 Minuten Laufzeit um 8 x 1 Stunde Daten mit je ~1250 Symbolen zu prozessieren**

## Core: Erzeugung der core_kpi Datenbanken
  
```
time \
for s in `ls data/staging/*.db` ; do 

  stage="$(basename "$s" .db)" 
  cleaned="data/cleansing/cleaned_${stage}_60s.db" 
  core="data/core/core_kpi_${stage}.db"
  python scripts/4_core/build_core_db.py \
      --staging-db "$s" \
      --cleansing-db "$cleaned" \
      --output-db "$core" \
      --views-sql scripts/4_core/core_kpi_views.sql \
      --progress \
      --progress-interval-seconds 30   
done
```
  
**8 core KPI Datenbanken erzeugen benötigte in Summe 15Sek.**
cross checks:
  
```
(crypto) wgo@vmd191183:~/dev/crypto_DWH$ ll -h data/core/
total 751M
drwxrwxr-x 2 wgo wgo 4.0K Mar 10 14:26 ./
drwxrwxr-x 7 wgo wgo 4.0K Mar 10 12:56 ../
-rw-r--r-- 1 wgo wgo  88M Mar 10 14:26 core_kpi_staging_export_20260310_131956_last_1h.db
-rw-r--r-- 1 wgo wgo  80M Mar 10 14:26 core_kpi_staging_export_20260310_131959_last_1h_from_1h.db
-rw-r--r-- 1 wgo wgo  81M Mar 10 14:26 core_kpi_staging_export_20260310_132001_last_1h_from_2h.db
-rw-r--r-- 1 wgo wgo  83M Mar 10 14:26 core_kpi_staging_export_20260310_132004_last_1h_from_3h.db
-rw-r--r-- 1 wgo wgo  83M Mar 10 14:26 core_kpi_staging_export_20260310_132006_last_1h_from_4h.db
-rw-r--r-- 1 wgo wgo  85M Mar 10 14:26 core_kpi_staging_export_20260310_132009_last_1h_from_5h.db
-rw-r--r-- 1 wgo wgo  85M Mar 10 14:26 core_kpi_staging_export_20260310_132011_last_1h_from_6h.db
-rw-r--r-- 1 wgo wgo  85M Mar 10 14:26 core_kpi_staging_export_20260310_132013_last_1h_from_7h.db
-rw-r--r-- 1 wgo wgo  86M Mar 10 14:26 core_kpi_staging_export_20260310_132015_last_1h_from_8h.db
```
  
Jede DB hat ~80MG Größe
  
```
(crypto) wgo@vmd191183:~/dev/crypto_DWH$ sqlite3 data/core/core_kpi_staging_export_20260310_083648_last_1h.db
SQLite version 3.51.2 2026-01-09 17:27:48
Enter ".help" for usage hints.
sqlite> .schema
CREATE TABLE market_ticks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_partition_id TEXT NOT NULL,
          source_db_path TEXT NOT NULL,
          source_row_id INTEGER NOT NULL,
          ingestion_ts_utc TEXT NOT NULL,
          ingestion_ts_epoch_s INTEGER NOT NULL,
          exchange_id TEXT NOT NULL,
          exchange_id_norm TEXT NOT NULL,
          symbol TEXT NOT NULL,
          symbol_norm TEXT NOT NULL,
          asset_norm TEXT NOT NULL,
          market_type TEXT,
          price REAL,
          bid REAL,
          ask REAL,
          last REAL,
          open REAL,
          high REAL,
          low REAL,
          base_volume REAL,
          quote_volume REAL,
          exchange_ts_ms INTEGER,
          exchange_ts_utc TEXT,
          raw_json TEXT NOT NULL
      );
...            

sqlite> select count(*) from market_ticks;
70304
```
  
ebensoviele market_ticks Einträge wie die Staged DB :)
### Core Validate quality gate with scripts/4_core/core_validation_runner.py
```
time \
for s in `ls data/staging/*.db` ; do 
  stage="$(basename "$s" .db)" 
  echo $stage
  cleaned="data/cleansing/cleaned_${stage}_60s.db" 
  core="data/core/core_kpi_${stage}.db"

  python scripts/4_core/core_validation_runner.py \
      --staging-db $s \
      --cleansing-db $cleaned \
      --no-fail-on-error                    

done
```
      
#### Analyse 
Assertions gefunden in Prüfungen

- latency_upper_bound
- latency_upper_bound_hourly
- price_diff_pct_bounds
- price_diff_pct_bounds_hourly
- update_frequency_bounds
- update_frequency_bounds_hourly

#### Ergebnisse der Analysen

1. latency_upper_bound

- Fehlgeschlagen wegen extrem großer max_latency_ms-Werte.
- Hauptursache: exchange_ts_utc ist bei manchen Exchanges kein frischer Event-Zeitpunkt.
- Besonders auffällig: ndax.
- Beispiel:
  - ndax APT/CAD: ca. 230,816,598 ms
  - ndax ASTER/CAD: ca. 83,650,420 ms
- Bewertung:
  - eher KPI-/Semantikproblem als Ingestion-Fehler.

2. latency_upper_bound_hourly

- Gleiche Ursache wie oben.
- Die stündliche Aggregation erbt dieselben Ausreißer aus den Latenz-Samples.
- Bewertung:
  - ebenfalls methodisches Problem, nicht primär ein Pipeline-Defekt.

3. price_diff_pct_bounds

- Fehlgeschlagen wegen sehr hoher Preisabweichungen über Exchanges.
- Betroffene Symbole:
  - AAVE3L/USDT bis 1492.8%
  - ADA3L/USDT bis 420.2%
  - AAVE3S/USDT bis 379.7%
  - ADA3S/USDT bis 284.8%
- Ursache:
  - Leveraged Tokens (3L, 3S) sind fachlich kein sauberer Cross-Exchange-Preisvergleich.
  - Gleiches Symbol ist hier sehr wahrscheinlich kein wirklich identisches Instrument.
- Bewertung:
  - Symbol-/Instrument-Mapping-Problem.

4. price_diff_pct_bounds_hourly

- Gleiche Ursache wie bei daily.
- Die stündliche Sicht zeigt dieselben Ausreißer in kleinerem Aggregationsfenster.
- Bewertung:
  - ebenfalls eher Modellierungsproblem als Datenbankfehler.

5. update_frequency_bounds

- Fehlgeschlagen durch mindestens einen sehr kleinen mittleren Update-Abstand.
- Beispiel:
  - kraken ALCX/USD
  - avg_update_interval_s = 0.001971
  - update_frequency_hz = 507.247649
- Ursache:
  - sehr kleine Intervallzahl, Burst-Verhalten im Websocket-Feed

6. update_frequency_bounds_hourly

- Gleiche Ursache wie daily.
- Dieselbe Serie fällt auch auf Stundenebene aus dem Grenzbereich.
- Bewertung:
  - ebenfalls ein Threshold-/Stichprobenproblem.

Gesamtbewertung

- Kein Hinweis auf einen Defekt im core_pipeline.py
- Kein Hinweis auf kaputte SQL-View-Erzeugung
- Die Fehlermuster sprechen überwiegend für:
  - zu strenge Assertions
  - unzureichende Instrument-Filterung
  - unklare Semantik von exchange_ts_utc bei einzelnen Exchanges

Pragmatische Einordnung

- latency_*: aktuell als globale harte KPI so nicht robust
- price_diff_pct_*: braucht Symbol-/Instrument-Normalisierung oder Ausschluss von Leveraged Tokens
- update_frequency_*: braucht Mindeststichprobe oder höhere/andere Schwelle

## Marts: Erzeuge und fülle "dash_cache_*" Tabellen mit Daten der Viewsfür schnellen Zugriff aus dem Dashboard
```
time \
for c in `ls data/core/*.db`; do 
  python scripts/5_marts/build_dashboard_cache.py \
      --db-path $c \
      --apply-mart-views \
      --progress \
      --progress-interval-seconds 30        
done
```
  
gut investierte 90Sek. die das Laden der Daten im Dashboard enorm beschleunigen.
die views liefern die Daten der OLAPs. Diese werden in Cache Tabellen gespeichert für den schnellen Zugriff zur Laufzeit des Dashboards.

### OLAPs:
+ **Latenz & Disconnectcount pro Exchange**: _vw_mart_dashboard_platform_quality_daily und vw_mart_dashboard_platform_quality_hourly_  
+ **Max/Min Preisabweichung pro Symbol-Exchange Paar**: s_vw_mart_dashboard_price_deviation_daily und vw_mart_dashboard_price_deviation_hourly_
+ **Liste der Symbole pro Exchange**: _vw_mart_dashboard_symbol_deviation_bucket_
+ **Liste der Symbole pro Exchange mit Qualitätsangebe bzgl fehlender oder interpolierter Daten**: _vw_mart_dashboard_symbol_observed_quality_base_

## Dashboard
Erweiterung des Dashboards um die Möglichkeit der Auswahl der core_kpi.db
Bevorzugt wird core_kpi.db geladen, aber man kann über eine drop-down box andere gefundene core_kpi*.dbs laden
Umbau auf neue ingestion architektur um bei Bedarf die Rohdaten zu lesen
starten des Dashboards: streamlit run scripts/5_marts/dashboard_mvp_app.py

External URL: http://144.91.86.134:8501

## Forecasting: aktuell nur 2 ML Modelle in Verwendung
Ridge und HistGradientBoostingRegressor
Beschränkung auf ausgewählte Symbole: "BTC/%, ETH/%, SOL/%, ADA/%"
Skript aufgeteilt in train und forecast, da vorherige lösung nicht sinnvoll war. So kann man Modelle mit stagingexporten trainieren und andere Stagingexporte für den forecast nutzen.
Die Forecasting-Architektur ist jetzt sauber getrennt:
    

### Train ML

Train: mehrere Staging-DBs als Historie, nur Modelltraining und Registry
Forecast: ein Cleansing-Run nutzt bereits trainierte Modelle und schreibt forecast_predictions

```
time \
./scripts/6_forecasting/train_staging_models.py  \
    --staging-db data/staging/ \
    --staging-db-exclude data/staging/staging_export_20260310_131956_last_1h.db \
    --forecast-db data/core/core_kpi_staging_export_20260310_131956_last_1h.db \
    --workers 4 \
    --symbols "BTC/%,ETH/%,SOL/%,ADA/%"

real    365m9.163s                                                                                                                                            │To github.com:laiki/crypto_DWH.git
```
cross checks
```

sqlite> select distinct(symbol) from model_artifacts;
ADA/USDT
BTC/DAI
BTC/PLN
BTC/UAH
BTC/USD
BTC/USDR
BTC/USDT
ETH/DAI
ETH/PLN
ETH/USD
ETH/USDC
ETH/USDT
BTC/USDC
BTC/EUR
BTC/USDQ
ETH/BTC
ETH/EUR
ETH/UAH
sqlite> select count(distinct(symbol)) from model_artifacts;
18

sqlite> select distinct(exchange_id) from model_artifacts ;
exmo
backpack
bitvavo
deribit
woo


sqlite> select count(*) from model_artifacts;
154
```

Modelle von 18 Währungspaaren, 5 Handelsplattformen, in Summe wurden 154 ML Modelle in **6 Stunden trainiert**.

### forecast ML

```
 time \
 ./scripts/6_forecasting/forecast_with_trained_models.py \
    --cleansing-db data/cleansing/cleaned_staging_export_20260310_131956_last_1h_60s.db \
    --forecast-db data/core/core_kpi_staging_export_20260310_131956_last_1h.db --workers 4
real    1m8.149s
```

cross checks:
```
sqlite> select count(*) from forecast_predictions;
3196

sqlite> select distinct(symbol), exchange_id from forecast_predictions;
BTC/USDC|backpack
ETH/USDC|backpack
BTC/USDC|deribit
ETH/USDC|deribit
BTC/DAI|exmo
BTC/EUR|exmo
BTC/PLN|exmo
BTC/UAH|exmo
BTC/USD|exmo
BTC/USDC|exmo
BTC/USDQ|exmo
BTC/USDR|exmo
BTC/USDT|exmo
ETH/BTC|exmo
ETH/DAI|exmo
ETH/EUR|exmo
ETH/PLN|exmo
ETH/UAH|exmo
ETH/USD|exmo
ETH/USDC|exmo
ETH/USDT|exmo
BTC/USDC|woo
BTC/USDT|woo
ETH/BTC|woo
ETH/USDC|woo
ETH/USDT|woo

sqlite> select distinct symbol, exchange_id from forecast_predictions;
BTC/USDC|backpack
ETH/USDC|backpack
BTC/USDC|deribit
ETH/USDC|deribit
BTC/DAI|exmo
BTC/EUR|exmo
BTC/PLN|exmo
BTC/UAH|exmo
BTC/USD|exmo
BTC/USDC|exmo
BTC/USDQ|exmo
BTC/USDR|exmo
BTC/USDT|exmo
ETH/BTC|exmo
ETH/DAI|exmo
ETH/EUR|exmo
ETH/PLN|exmo
ETH/UAH|exmo
ETH/USD|exmo
ETH/USDC|exmo
ETH/USDT|exmo
BTC/USDC|woo
BTC/USDT|woo
ETH/BTC|woo
ETH/USDC|woo
ETH/USDT|woo
sqlite> SELECT COUNT(*) FROM (SELECT DISTINCT symbol, exchange_id FROM forecast_predictions);
26


sqlite>  WITH latest AS (
    SELECT training_run_id
    FROM forecast_training_runs
    WHERE status = 'completed'
    ORDER BY started_utc DESC
    LIMIT 1
  )
  SELECT
    (SELECT COUNT(*) FROM forecast_model_registry
     WHERE status = 'trained'
       AND training_run_id = (SELECT training_run_id FROM latest)) AS trained_models,
    (SELECT COUNT(*) FROM (
       SELECT DISTINCT exchange_id, symbol
       FROM forecast_model_registry
       WHERE status = 'trained'
         AND training_run_id = (SELECT training_run_id FROM latest)
     )) AS trained_pairs,
    (SELECT COUNT(*) FROM forecast_predictions
     WHERE training_run_id = (SELECT training_run_id FROM latest)) AS forecast_rows,
    (SELECT COUNT(*) FROM (
       SELECT DISTINCT exchange_id, symbol
       FROM forecast_predictions
       WHERE training_run_id = (SELECT training_run_id FROM latest)
     )) AS forecasted_pairs;

106|27|3196|26
```

- 106 = Anzahl trainierter Modelle
- 27 = Anzahl trainierter (exchange_id, symbol)-Paare
- 3196 = Anzahl geschriebener Forecast-Zeilen in forecast_predictions
- 26 = Anzahl (exchange_id, symbol)-Paare, für die tatsächlich Forecasts geschrieben wurden
  
26 Exchange/Symbol Paare in 3196 Forecasts wurden in 1Min berechnet.

## Tag 3

Forecasting hat gezeigt, dass größere Stagingblöcke benötigt werden.
Staging erneut ausgeführt um 2 Stunden staging Intervalle zu haben, denn ansonsten ist eine forecast Darstelllung gegen den tatsächlichen Wert für 30Min forecasts durch feature warm-up nicht möglich.
30 Min feature warm-up gehen am Anfang des Staging verlohren, wenn man dann mit dem nächsten Wert den forecast in 30Min erhält ist ein staging von 60Min nutzlos, um die Vorhersage mit der Wahrheit zu vergleichen.
 
Um nicht wieder 6 Stunden Trainingszeit zu verliehren, habe ich mich auf 2 Symbole zur Verhersage festgelegt
- BTC/USDC
- ETH/USDC

Folgende Schritte wurden durchlaufen:

## Staging

``` 
for offset in 0 1 2 3 4 5 6 7; do
    echo $offset
    python scripts/2_staging/staging_exporter.py \
    --vault-root data/vault2_redis \
    --hours 2 \
    --start-relative-from-hour "$offset" \
    --output-dir data/staging \
    --log-level INFO
done 
``` 

---

## Cleansing 

``` 
for s in `ls data/staging/*2h*.db`  ; do
    python ./scripts/3_cleansing/cleansing_resample.py  --input-db $s --fill-method forward_fill --workers 4
done
``` 

---

## Core

``` 
for s in `ls data/staging/*2h*.db` ; do 
    stage="$(basename "$s" .db)" 
    cleaned="data/cleansing/cleaned_${stage}_60s.db" 
    core="data/core/core_kpi_${stage}.db"
    python scripts/4_core/build_core_db.py \
        --staging-db "$s" \
        --cleansing-db "$cleaned" \
        --output-db "$core" \
        --views-sql scripts/4_core/core_kpi_views.sql \
        --progress \
        --progress-interval-seconds 30 
done
``` 

---

### Core validation

Ist eigentlich überflüssig, weil die gestrigen MEldungen wieder zu erwarten sind 
``` 

for s in `ls data/staging/*2h*.db` ; do 
    stage="$(basename "$s" .db)" 
    echo $stage
    cleaned="data/cleansing/cleaned_${stage}_60s.db" 
    core="data/core/core_kpi_${stage}.db"
    
    python scripts/4_core/core_validation_runner.py \
        --staging-db $s \
        --cleansing-db $cleaned \
        --no-fail-on-error					

done
``` 

---

## Marts 

``` 
for c in `ls data/core/*2h*.db`; do 
    python scripts/5_marts/build_dashboard_cache.py \
        --db-path $c \
        --apply-mart-views \
        --progress \
        --progress-interval-seconds 30		
done
``` 

---

## ML forecast

### ML Model training

``` 
time \
./scripts/6_forecasting/train_staging_models.py  \
    --staging-db "data/staging/staging_export_20260311_*2h*.db" \
    --staging-db-exclude "data/staging/staging_export_20260311_*_last_2h.db" \
    --forecast-db data/core/core_kpi_staging_export_20260311_115106_last_2h.db  \
    --workers 4 \
    --symbols "BTC/USDC,ETH/USDC"

``` 

Diesmal hat das Training nur 2 Stunden gedauert.

---

### ML forecasting

``` 
time ./scripts/6_forecasting/forecast_with_trained_models.py \
    --cleansing-db data/cleansing/cleaned_staging_export_20260311_115106_last_2h_60s.db \
    --forecast-db data/core/core_kpi_staging_export_20260311_115106_last_2h.db \
    --workers 4 \
    --symbols "BTC/USDC,ETH/USDC"
    
...
2026-03-11 14:27:50 | INFO | forecast_with_trained_models | Progress models 32/32 (100.0%) | completed=32 failed=0 skipped=0 predictions=2888 | elapsed=00:00:23 eta=00:00:00 | last_item=woo:ETH/USDC:hist_gradient_boosting:h1800
...
real    0m27.636s
user    1m29.510s
sys     0m4.838s
``` 
30 Sekunden Bearbbeitungszeit.

---

## forecasting with AI / Deepl Learning 

### training
Neue Skripte zum Tranieren von DL Modellen und forcasten implementiert.
Training mit 6 Stunden an Trainigsdaten ohne GPU und mit zahlreichen Modelllen wird sicherlich sehr lange dauern.
 

``` 
time \
python ./scripts/6_forecasting/train_ai_models.py \
    --staging-db "data/staging/staging_export_20260311_1151*_2h*.db" \
    --staging-db-exclude "data/staging/*_last_2h.db" \
    --forecast-db data/core/core_kpi_staging_export_20260311_115106_last_2h.db \
    --symbols "BTC/USDC,ETH/USDC"
``` 

Sollte das über Nacht nicht fertig werden, werden Trainingsdaten von nur zwei batches a 2 Stunden verwendet.

---

Der Lauf war in weniger als 2 Minuten fertig. 
Es scheint kein wirkliches Trainieren eies DL Modells zu sein. Dieses Chrinos2 Model ist schon vortraniert und erhält nur was auch immer an zusätzlicher Information.

### forecasting

``` 
time \
python ./scripts/6_forecasting/forecast_with_ai_models.py \
    --cleansing-db data/cleansing/cleaned_staging_export_20260311_115106_last_2h_60s.db \
    --forecast-db data/core/core_kpi_staging_export_20260311_115106_last_2h.db \
    --model-backends chronos2
``` 
Der Lauf war in weniger als einer Minute fertig.

Das Ergebnis / die Visualisierung im Dashboard ist aber nicht wirklich vielversprechend.

## Tag 4
MOIRA2 DL Modell integrieren
Präsentationsfolien schreiben
