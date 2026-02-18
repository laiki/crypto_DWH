# Crypto Data Engineering Projektarbeit

## Ziel
Diese Projektarbeit baut ein Data Warehouse fuer Kryptowaehrungen und ein Dashboard zur Visualisierung zentraler KPIs auf.

Fokus:
- Preisvergleich gleicher Coins ueber mehrere Exchanges
- Latenz und Update-Frequenz von Echtzeitdaten
- Verbindungsabbrueche pro Plattform
- Forecasts pro Waehrung und Exchange

## Tech-Stack (geplant)
- Python
- CCXT (Datenaufnahme von Exchanges)
- SQLite (Rohdaten / Zwischenstufen)
- Scikit-learn + weitere Zeitreihenmodelle (Forecasting)
- Mermaid fuer Architekturdiagramme

## Zielarchitektur
1. ETL/Ingestion:
- Server-Prozess sammelt Marktdaten via CCXT
- Empfangszeitpunkt wird pro Datensatz gespeichert

2. Staging:
- Taeglicher Export der letzten 24h je Exchange in separate Staging-Datenbank

3. Cleansing:
- Datenluecken behandeln
- Ausreisser identifizieren/markieren

4. Core:
- Berechnung von KPIs (Update-Frequenz, Latenz, Abbrueche/Tag)

5. Marts:
- Preisvergleich gleicher Waehrung ueber Exchanges zum gleichen Zeitpunkt
- KPI-Auswertung fuer Dashboard

## Dashboard (MVP)
- Auswahl einer Kryptowaehrung
- Kursverlauf der letzten 24h (zunaechst Binance)
- Maximale Preisdifferenzen zwischen Exchanges
- Min/Max-Latenz pro Plattform

## Mermaid Diagramme
Architekturdiagramme werden als `.mmd` Dateien gepflegt.

Wenn `mermaid-cli` installiert ist:
```powershell
mmdc -i architecture.mmd -o architecture.svg
```

## Projektstatus
Aktuell: Konzeption und Architekturdefinition.

Naechste Schritte:
1. Detailliertes Datenmodell (Staging/Core/Marts)
2. Definition der KPI-Formeln
3. Ingestion-Prototyp mit CCXT
4. Erstes Dashboard-Layout

