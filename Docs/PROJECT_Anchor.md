LAST_CHANGE: 2026-03-06 11:31 (Europe/Berlin)

# InitialProjectDescription / ProjectAnchor

## Projekt

**Name:** TradeRepublic-Trading  
**Stand:** 2026-03-06  
**Quelle:** abgeleitet aus `tr_trading_pipeline.sh`, `tr_trading_state_from_pdfs.py`, `tr_trading_reports_from_state.py`

## Ziel + Scope

Dieses Projekt verarbeitet Trade-Republic-Trading-Abrechnungen als PDF-Dateien automatisiert zu einem dauerhaften Trading-State und daraus abgeleiteten Reports.

Im Scope der vorhandenen Scripte:
- Einlesen von Trading-PDFs aus einem Inbox-Ordner
- Extraktion einzelner Trades aus den PDFs
- Archivierung verarbeiteter PDFs
- Schreiben eines persistenten `tr_trading_state.json`
- Erzeugung von Jahres-, Monats- und Tagesreports als CSV
- Ableitung offener Positionen per FIFO
- Ergänzung eines globalen Kapitalertrags-/Steuer-States
- optionaler Dashboard-Refresh nach erfolgreichem Global-State-Update

Nicht im sichtbaren Scope:
- Download der PDFs
- UI / Frontend
- Orderausführung oder Broker-Anbindung

## Entry / Trigger

Primärer Entry-Point ist:
- `tr_trading_pipeline.sh`

Die Shell-Pipeline:
- setzt Locking und Anti-Doppeltrigger
- schreibt in ein dediziertes Log
- ruft nacheinander die beiden Python-Stufen auf
- mappt Worker-Ausgaben auf Notifications
- beendet sich nach außen immer mit `exit 0`

## Pipeline-Übersicht

### 1) PDF -> Trading-State

Script:
- `tr_trading_state_from_pdfs.py`

Aufgabe:
- liest alle `*.pdf` aus dem Inbox-Ordner
- wartet auf stabile Dateien
- extrahiert pro PDF Trading-Metadaten und Beträge
- erkennt `Kauf` / `Verkauf`, Trade-Datum, Uhrzeit, ISIN, Stückzahl, Gebühren, Steuern
- bildet eine stabile Trade-UID
- verschiebt valide PDFs ins Archiv
- verschiebt nicht parsebare PDFs nach `nicht_verarbeitet`
- schreibt neue Einträge in den persistenten State

Wesentliche Datenfelder im State:
- `uid`
- `source_pdf`
- `order_id`
- `exec_id`
- `side`
- `trade_date`
- `trade_time`
- `isin`
- `qty`
- `booking_amount`
- `raw_amount`
- `unit_price_raw`
- `fee_*`
- `tax_*`

State-Datei:
- `~/Library/Application Support/Finanzen/TR_Trading/tr_trading_state.json`

### 2) Trading-State -> Reports

Script:
- `tr_trading_reports_from_state.py`

Aufgabe:
- lädt den Trading-State
- gruppiert Trades nach Jahr
- matched Käufe und Verkäufe per FIFO
- erzeugt geschlossene Trade-Fills
- berechnet je Fill Invest, Erlös, Gebühren, Steuern, `G&V_roh`, `KEvST`, `G&V_Konto`
- schreibt Reports für Jahr, Monat und Tag
- gibt Kauf- und Verkaufszeit kompakt als `dd.mm.yy HH:MM` in den Übersichtsreports aus
- erzeugt Datei für offene Positionen
- ergänzt `global_capital_revenues_taxes.json`
- stößt optional ein Dashboard-Update-Script an

## Modul-/Dateirollen

- `tr_trading_pipeline.sh`
  - Orchestrator, Locking, Logging, Notification-Mapping, Fehlerentkopplung nach außen
- `tr_trading_state_from_pdfs.py`
  - Parser und Ingest-Stufe von PDF nach persistentem Trading-State
- `tr_trading_reports_from_state.py`
  - Reporting- und FIFO-Auswertungsstufe auf Basis des States

## Pfade

### IN

- `/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Abrechnungen`

### ARCHIV

- `/Users/joachimthomas/Finanzverwaltung/Archiv/TradeRepublic/Trading/WertpapierAbrechnungen/Trading`
- Fehler-PDFs:
  `/Users/joachimthomas/Finanzverwaltung/Archiv/TradeRepublic/Trading/WertpapierAbrechnungen/Trading/nicht_verarbeitet`

### STATE

- `~/Library/Application Support/Finanzen/TR_Trading/tr_trading_state.json`
- Lock Importer:
  `~/Library/Application Support/Finanzen/TR_Trading/.tr_trading_import.lock`

### REPORT OUT

- `/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Reports/<YEAR>/Jahresübersicht/Essenz`
- `/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Reports/<YEAR>/Jahresübersicht/Transaktionen`
- `/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Reports/<YEAR>/Monate/<MM>/Daily`
- `/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Reports/<YEAR>/Monate/<MM>/Transaktionen`
- `/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Reports/<YEAR>/Monate/<MM>/Essenz`
- `/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Reports/<YEAR>/OP`

### GLOBAL / DOWNSTREAM

- `~/Library/Application Support/Finanzen/global_capital_revenues_taxes.json`
- `/Users/joachimthomas/Finanzverwaltung/Programme/Visualisierung/Dashboard/updateDashboardfromState.sh`

### LOG / RUNTIME

- `/Users/joachimthomas/Finanzverwaltung/Programme/Logs/TradeRepublic-Trading/tr_trading_pipeline.log`
- `~/Library/Application Support/Finanzen/.locks/tr_trading_pipeline.lock`
- `~/Library/Application Support/Finanzen/.run/tr_trading_last`
- Notification-Hook:
  `/Users/joachimthomas/Finanzverwaltung/Programme/Global/finance_notify.sh`

## Outputs

### Trading-State

- JSON-State mit allen bekannten Einzeltrades
- je Trade jetzt zusätzlich mit `unit_price_raw` als aus `raw_amount / qty` abgeleitetem Einzelpreis

### Reports

Beobachtbare Report-Dateien:
- `TR_Trades_Overview_<YEAR>.csv`
- `TR_Trades_Daily_<YYYY-MM-DD>.csv`
- `TR_Trades_Overview_<YYYY-MM>.csv`
- `TR_Trades_Snap_Monat_<YYYY-MM>.csv`
- `TR_YTD_Trading_Summary.csv`
- `TR_Offene_Positionen.csv`

Aktuelles Format in den Übersichtsreports:
- Spalten `Kauf` und `Verkauf` statt nur Datum
- Format: `dd.mm.yy HH:MM`

### Globaler Kapital-State

- abgeschlossene Trade-Fills werden als Trading-Revenue-Einträge in `global_capital_revenues_taxes.json` ergänzt

## Returncodes / Statussignale

### `tr_trading_state_from_pdfs.py`

- `0` = neue Trades in State geschrieben
- `10` = NOOP, nichts neu / nichts zu tun / gelockt
- `1` = Fehler

Stdout-Signale:
- `NOTE|started`
- `NOTE|pdf_read|...`
- `NOTE|Pipeline-Summary|...`
- `NOTE|state_written|added=N`
- `NOTE|state_not_written|reason=...`
- `NOTE|ended|result=updated|noop|fail`

### `tr_trading_reports_from_state.py`

- `0` = Reports erfolgreich erzeugt
- `10` = NOOP, leerer State / keine gültigen Daten / Daily ohne Daten
- `1` = Fehler

Stdout-Signale:
- `REPORTS start`
- `REPORTS daily | ...`
- `REPORTS noop | ...`
- `REPORTS ok | years=... fills=... open_lots=... global_cap_added=... dashboard_updated=...`
- `REPORTS fail | error=...`

### `tr_trading_pipeline.sh`

Interne Steuerung:
- Step 1 Fehler -> Pipeline meldet Fail und endet nach außen trotzdem mit `0`
- Step 1 NOOP -> Reports werden nicht gestartet
- Step 2 Fehler -> Pipeline meldet Fail und endet nach außen trotzdem mit `0`

Externe Semantik:
- runner-/launchd-freundliches Verhalten durch konsequentes `exit 0`

## Leitprinzipien

Aus dem vorhandenen Code direkt erkennbar:
- Idempotenz über stabile UID-Bildung je Trade
- Dedup über vorhandene UIDs im State
- atomisches Schreiben bei JSON- und CSV-Ausgaben
- Single-Instance-Schutz per Lockfiles
- Anti-Doppeltrigger über Last-Run-Timestamp
- Trennung von Ingest und Reporting
- parsebare stdout-Zeilen für Orchestrierung
- Fehlerhafte PDFs werden nicht verworfen, sondern separiert archiviert
- nach außen robuste Pipeline-Semantik mit Logging und Notifications statt hartem Prozessabbruch

## Kurzfazit Projektzweck

Der operative Zweck dieses Repos ist nicht allgemeines "Trading", sondern eine lokale Backoffice-/Auswertungs-Pipeline für Trade-Republic-Wertpapierabrechnungen: PDFs werden in einen persistierten Trading-State überführt und daraus steuer- und reportingnahe CSV-Ausgaben sowie ein globaler Kapitalertrags-State erzeugt.

## Heute umgesetzt (2026-03-06)

- `tr_trading_state_from_pdfs.py` ergänzt den State pro Trade um `unit_price_raw`
- `tr_trading_reports_from_state.py` schreibt Kauf- und Verkaufszeit in die Jahres-, Monats- und Tagesübersichten
- bestehender produktiver State unter `~/Library/Application Support/Finanzen/TR_Trading/tr_trading_state.json` wurde einmalig nachgezogen
- Vor der State-Migration wurde ein Backup angelegt:
  `~/Library/Application Support/Finanzen/TR_Trading/tr_trading_state.backup_20260306_112921.json`
- Vor der Migration wurde geprüft: `qty <= 0` kam in keinem der 100 vorhandenen Trades vor
