# AGENTS.md

## Projektkontext
- Projektart: Projektarbeit im Bereich Data Engineering.
- Domaene: Analyse von Kryptowaehrungen ueber mehrere Handelsplattformen.
- Programmiersprache: Python.
- Fokus: Aufbau eines Data Warehouses und eines Dashboards zur Visualisierung der Ergebnisse.

## Kollaborationsregeln
- Antworte standardmaessig auf Deutsch.
- Ohne explizite Aufforderung zuerst konzeptionell arbeiten (Architektur, Datenmodell, KPIs, Vorgehen), nicht direkt implementieren.
- Aenderungen und Entscheidungen klar begruenden und in kleine, nachvollziehbare Schritte aufteilen.
- Architekturdiagramme standardmaessig als Mermaid-Dateien (`.mmd`) erstellen.
- Falls `mermaid-cli` verfuegbar ist, Diagramme zusaetzlich als SVG/PNG/PDF rendern.

## Fachliche Ziele
- Vergleich gleicher Kryptowaehrungen ueber verschiedene Exchanges.
- Analyse von:
  - Preisunterschieden zum gleichen Zeitpunkt.
  - Latenz der Echtzeitdaten.
  - Frequenz/Update-Rate der Echtzeitdaten.
  - Verbindungsabbruechen pro Plattform.
- Forecasts pro Waehrung und Exchange mit klassischen ML-Ansaetzen und modernen AI-Zeitreihenmodellen.

## Zielarchitektur (DWH-Pipeline)

### 1) ETL / Ingestion
- Datenquelle: `ccxt` (CryptoCurrency eXchange Trading Library), Echtzeit-/Marktdaten von Exchanges.
- Prozess: Server-Prozess schreibt kontinuierlich in SQLite.
- Pflichtfeld: Empfangszeitpunkt (ingestion timestamp) pro Datensatz.
- Zu pruefen:
  - Vereinheitlichung von Coin-/Symbolnamen durch CCXT ueber Exchanges hinweg.
  - Ob und wie CCXT die Update-Frequenz einzelner Boersen liefert.

### 2) Staging Area
- Taeglicher Export-Job (Cron): Daten der letzten 24h je Plattform in eine separate Datenbank/Zone.
- Ziel: Entkopplung von Rohdatenaufnahme und nachgelagerter Aufbereitung.

### 3) Cleansing Area
- Datenluecken identifizieren und behandeln.
- Ausreisser erkennen und markieren (oder bereinigen, je nach definierter Regel).

### 4) Core Layer
- Berechnung zentraler Meta-Informationen/KPIs:
  - Update-Frequenz.
  - Verbindungsabbrueche pro Tag.
  - Latenz.

### 5) Marts / Analytics
- Vergleich derselben Kryptowaehrung ueber verschiedene Handelsplattformen.
- Preisvergleich auf identischen Zeitpunkten (zeitliche Ausrichtung erforderlich).
- KPI-Fokus:
  - Maximale Preisunterschiede pro Kryptowaehrung (als Graph).
  - Latenz pro Handelsplattform.
  - Anzahl Verbindungsabbrueche pro Handelsplattform.

## Forecasting-Rahmen
- Pro Waehrung und Exchange.
- Modellfamilien:
  - Scikit-learn lineare Modelle (z. B. `Ridge`; weitere geeignete Regressoren evaluieren).
  - AI-Modelle aus externer Referenzliste:
    - Chronos-2
    - TimesFM-2.5
    - MOIRAI-2
- Wichtiger Hinweis: Modellwahl und Metriken (z. B. MAE/RMSE/MAPE) vorab pro Use Case festlegen.

## Dashboard-Anforderungen
- Auswahl einer Kryptowaehrung durch den Nutzer.
- Visualisierung:
  - Kursverlauf der letzten 24h (zunaechst Binance als Startpunkt).
  - Maximale Preisdifferenzen zwischen Exchanges.
  - Min/Max-Latenz je Plattform.
- Ziel: schnelle Vergleichbarkeit zwischen Plattformqualitaet und Preisabweichung.

## Offene Pruef- und Designfragen
- Kanonisches Symbol-Mapping fuer Coins/Trading-Pairs exchange-uebergreifend.
- Einheitliche Zeitsynchronisation (UTC) und Resampling-Strategie fuer "gleicher Zeitpunkt".
- Definition von "Verbindungsabbruch" und messbarer Latenzformel.
- Umgang mit fehlenden Ticks und extremen Ausreissern.
- Trennung von Online-Ingestion und Batch-Transformation (Betriebsstabilitaet).

## Erwartete Ergebnisse der Projektarbeit
- Nachvollziehbare DWH-Architektur mit dokumentierten Datenfluessen.
- KPI-basierte Analyse der Boersenqualitaet und Preisabweichungen.
- Vergleich klassischer und moderner Forecasting-Ansaetze.
- Ein Dashboard, das die wichtigsten Erkenntnisse klar visualisiert.
