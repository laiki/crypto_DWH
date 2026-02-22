# Multi-Prozess Layout fuer WebSocket Ingestion

## Ziel
Die Ingestion horizontal skalieren, ohne SQLite-Lock-Contention zu erzeugen.

## Kernidee
1. Ein Orchestrator verteilt Exchanges auf mehrere Worker-Prozesse.
2. Jeder Worker verarbeitet seine Exchanges asynchron (asyncio) und schreibt in eine eigene SQLite-Datei.
3. Ein Merger-Job fuehrt Worker-Daten inkrementell in eine zentrale Raw-DB zusammen.

## Warum so und nicht alle Prozesse auf eine DB
- SQLite ist stark bei einem Writer, aber mehrere Prozesse auf dieselbe DB erzeugen Lock-Wartezeiten.
- Worker-lokale DBs isolieren Last und Fehler.
- Zusammenfuehrung kann kontrolliert und inkrementell laufen.

## Prozessrollen
- Orchestrator
  - liest `--exchanges`, `--exchanges-exclude`
  - prueft Capabilities pro Exchange
  - plant Shards und startet Worker
  - startet Worker bei Crash neu
- Worker
  - nutzt bestehende asyncio-Logik (`watch_tickers` oder `watch_ticker`)
  - bei `AuthenticationError`: Exchange dauerhaft aus Worker-Liste entfernen
  - schreibt `market_ticks` und `connection_events` in `worker_i_raw.db`
- Merger
  - laeuft periodisch (z. B. alle 1-5 Minuten)
  - liest neue Zeilen aus Worker-DBs per `id > last_merged_id`
  - schreibt in `raw_ingestion.db`
  - speichert Fortschritt je Worker in `merge_state.db`

## Sharding-Strategie (praxisnah)
- Pro Exchange initial `symbol_count` und Stream-Modus laden.
- Lastscore:
  - `watch_tickers`: `score = max(1, symbol_count / 200)`
  - `watch_ticker`: `score = max(1, symbol_count / 25)`
- Exchanges mit Greedy-Bin-Packing auf `N` Worker verteilen (immer in den aktuell leichtesten Worker legen).

## Empfohlene Startparameter
- `N` Worker:
  - Startwert `min(8, max(2, cpu_count // 2))`
  - bei schwacher Maschine eher 2-4
- je Worker:
  - `queue-size`: 50_000 bis 200_000
  - `batch-size`: 500 bis 2_000
  - `flush-interval-s`: 0.5 bis 1.0
- Merger:
  - Laufintervall 60-300s
  - Batch-Insert im Ziel 1_000-5_000 Zeilen

## Betriebsstabilitaet
- Health:
  - jeder Worker schreibt Heartbeat-Event alle 30s
  - Queue-Lag als Metrik protokollieren
- Restart:
  - Worker-Restart mit exponentiellem Backoff
  - maximaler Backoff 60s
- Shutdown:
  - zuerst Stop-Signal an Worker
  - Queue flushen
  - dann Merger final laufen lassen

## Datenmodell-Hinweise
- Schema in Worker-DB identisch zum aktuellen Schema halten.
- Zentrale Raw-DB zusaetzlich:
  - `source_worker_id`
  - `source_row_id`
  - optional eindeutiger Constraint auf `(source_worker_id, source_row_id)` gegen Duplikate.

## Einfuehrungsplan in kleinen Schritten
1. Orchestrator + statische Worker-Aufteilung (ohne dynamisches Rebalancing).
2. Worker schreiben in eigene DB-Dateien.
3. Merger-Job fuer inkrementelles Zusammenfuehren.
4. Monitoring (Heartbeat, Queue-Lag, Restart-Zaehler).
5. Optional dynamisches Rebalancing bei Lastverschiebung.
