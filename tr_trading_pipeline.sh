#!/bin/zsh
# shellcheck shell=bash
set -euo pipefail

PY="/usr/local/bin/python3"

PARSER="/Users/joachimthomas/Finanzverwaltung/Programme/Traderepublic/TradeRepublic-Trading/tr_trading_state_from_pdfs.py"
WORKER="/Users/joachimthomas/Finanzverwaltung/Programme/Traderepublic/TradeRepublic-Trading/tr_trading_reports_from_state.py"

LOG="/Users/joachimthomas/Finanzverwaltung/Programme/Logs/TradeRepublic-Trading/tr_trading_pipeline.log"
LOCK="$HOME/Library/Application Support/Finanzen/.locks/tr_trading_pipeline.lock"
LAST="$HOME/Library/Application Support/Finanzen/.run/tr_trading_last"

mkdir -p "$(dirname "$LOG")" "$(dirname "$LOCK")" "$(dirname "$LAST")"

# Notify Hook
NOTIFY="/Users/joachimthomas/Finanzverwaltung/Programme/Global/finance_notify.sh"
# usage: n OK|WARN|FAIL|INFO "Message"
n() { "$NOTIFY" "TR_TRADING" "$1" "$2" "$3" >/dev/null 2>&1 || true; }

# ------------------------------------------------------------
# Logging helpers (TR-Cash Style)
# ------------------------------------------------------------
ts() { date '+%Y-%m-%d %H:%M:%S'; }

# Format: [YYYY-MM-DD HH:MM:SS] SENDER | message
logline() {
    local sender="$1"
    shift
    echo "[$(ts)] ${sender} | $*"
}

# Keep only the last 2 runs (by MARK)
log_cut() {
    [[ -f "$LOG" ]] || return 0

    local marker="MARK | TRADING_PIPELINE_START"
    local tmp="$LOG.tmp"

    # find line numbers of marker occurrences
    local lines
    lines=$(grep -n "$marker" "$LOG" 2>/dev/null | cut -d: -f1 || true)

    # if 0 or 1 marker -> do nothing
    if [[ -z "$lines" ]]; then
        return 0
    fi

    local count
    count=$(echo "$lines" | wc -l | tr -d ' ')
    if [[ "$count" -le 2 ]]; then
        return 0
    fi

    # take the 2nd last marker line
    local start
    start=$(echo "$lines" | tail -n 2 | head -n 1)
    if [[ -n "$start" ]]; then
        tail -n +"$start" "$LOG" >"$tmp" 2>/dev/null || true
        /bin/mv "$tmp" "$LOG" 2>/dev/null || true
    fi
}

# Read/stream a worker line into log + optional noti mapping
pipe_worker() {
    local sender="$1"
    local line="$2"

    # Always log raw worker line (already compact)
    logline "$sender" "$line"

    # Optional: map NOTE/REPORTS to user-facing notis
    # Expected formats:
    #   NOTE|pdf_read|name=...
    #   NOTE|state_written|added=...
    #   NOTE|state_nochange
    #   REPORTS|ok|fills=..|open=..|years=..

    if [[ "$sender" == "PDF2STATE" ]]; then
        if [[ "$line" == NOTE\|pdf_read\|* ]]; then
            local isin side tdate qty
            isin=$(echo "$line" | sed -n 's/.*|isin=\([^|]*\).*/\1/p')
            side=$(echo "$line" | sed -n 's/.*|side=\([^|]*\).*/\1/p')
            tdate=$(echo "$line" | sed -n 's/.*|date=\([^|]*\).*/\1/p')
            qty=$(echo "$line" | sed -n 's/.*|qty=\([^|]*\).*/\1/p')

            # Build compact label, tolerate missing fields
            local label
            label="$side"
            [[ -n "$qty" ]] && label="$label · qty=$qty"
            [[ -n "$isin" ]] && label="$label · $isin"
            [[ -n "$tdate" ]] && label="$label · $tdate"
            [[ -n "$label" ]] && n "INFO" "PDF gelesen: $label" "PDF2STATE"
        elif [[ "$line" == NOTE\|state_written\|* ]]; then
            local added
            added=$(echo "$line" | sed -n 's/.*added=\([0-9][0-9]*\).*/\1/p')
            [[ -z "$added" ]] && added="?"
            n "INFO" "State aktualisiert: +$added, GRTS updated!" "PDF2STATE"
        fi
    elif [[ "$sender" == "STATE2REPORTS" ]]; then
        if [[ "$line" == REPORTS\ ok\ \|\ * ]]; then
            local fills open years
            years=$(echo "$line" | sed -n 's/.*years=\([^ ]*\).*/\1/p')
            fills=$(echo "$line" | sed -n 's/.*fills=\([0-9][0-9]*\).*/\1/p')
            open=$(echo "$line" | sed -n 's/.*open_lots=\([0-9][0-9]*\).*/\1/p')
            [[ -z "$fills" ]] && fills="?"
            [[ -z "$open" ]] && open="?"
            [[ -z "$years" ]] && years=""
            if [[ -n "$years" ]]; then
                n "INFO" "Reports erstellt | fills=$fills open=$open years=$years" "STATE2REPORTS"
            else
                n "INFO" "Reports erstellt | fills=$fills open=$open" "STATE2REPORTS"
            fi
        fi
    fi
}

cleanup() { rm -f "$LOCK" 2>/dev/null || true; }
trap cleanup EXIT

# Anti-Doppeltrigger (30s)
now=$(date +%s)
if [[ -f "$LAST" ]]; then
    last=$(cat "$LAST" 2>/dev/null || echo 0)
    ((now - last < 15)) && exit 0
fi

# Single-Instance Lock
(
    set -C
    : >"$LOCK"
) 2>/dev/null || exit 0
echo "$now" >"$LAST"

# Cut log before we start writing this run
log_cut

# Alles (stdout+stderr) ins Log UND ins Terminal
exec > >(tee -a "$LOG") 2>&1

# ------------------------------------------------------------
# RUN META
# ------------------------------------------------------------
RUN_ID="TRTRADING_$(date '+%Y%m%d_%H%M%S')"
logline "TRPIPELINE" "MARK | TRADING_PIPELINE_START | run_id=$RUN_ID"
logline "TRPIPELINE" "RUN_START | run_id=$RUN_ID"
logline "TRPIPELINE" "ENV | shell=$SHELL"
logline "TRPIPELINE" "ENV | pwd=$PWD"
logline "TRPIPELINE" "ENV | date=$(date)"

n "OK" "Trading-Pipeline gestartet" "TRADING_PIPELINE"

# ------------------------------------------------------------
# STEP 1: pdf -> state
#   RC 0  = state geändert
#   RC 10 = NOOP (alles schon bekannt / nichts zu tun)
#   sonst = Fehler
# ------------------------------------------------------------
logline "TRPIPELINE" "STEP_START | step=pdf_to_state | run_id=$RUN_ID"

set +e
PARSER_OUT="$("$PY" "$PARSER" 2>&1)"
RC=$?
set -e

if [[ -n "$PARSER_OUT" ]]; then
    while IFS= read -r ln; do
        [[ -z "$ln" ]] && continue
        pipe_worker "PDF2STATE" "$ln"
    done <<<"$PARSER_OUT"
fi

logline "TRPIPELINE" "STEP_END | step=pdf_to_state | rc=$RC | run_id=$RUN_ID"

# NOOP
if [[ $RC -eq 10 ]]; then
    logline "TRPIPELINE" "PIPELINE_END | result=noop | run_id=$RUN_ID"
    n "OK" "Keine Änderungen – State aktuell" "TRADING_PIPELINE"
    exit 0
fi

# Fehler
if [[ $RC -ne 0 ]]; then
    logline "TRPIPELINE" "PIPELINE_FAIL | step=pdf_to_state | rc=$RC | run_id=$RUN_ID"
    n "FAIL" "TR Trading: Fehler bei PDF→State (RC=$RC)"
    n "WARN" "Pipeline nach Fehler beendet" "TRADING_PIPELINE"
    logline "TRPIPELINE" "PIPELINE_END | result=fail | rc=$RC | run_id=$RUN_ID"
    exit 0
fi

# ------------------------------------------------------------
# STEP 2: state -> reports
#   (nur wenn STEP 1 wirklich etwas geändert hat)
# ------------------------------------------------------------
logline "TRPIPELINE" "STEP_START | step=state_to_reports | run_id=$RUN_ID"

set +e
WORKER_OUT="$("$PY" "$WORKER" 2>&1)"
WRC=$?
set -e

if [[ -n "$WORKER_OUT" ]]; then
    while IFS= read -r ln; do
        [[ -z "$ln" ]] && continue
        pipe_worker "STATE2REPORTS" "$ln"
    done <<<"$WORKER_OUT"
fi

logline "TRPIPELINE" "STEP_END | step=state_to_reports | rc=$WRC | run_id=$RUN_ID"

if [[ $WRC -ne 0 ]]; then
    logline "TRPIPELINE" "PIPELINE_FAIL | step=state_to_reports | rc=$WRC | run_id=$RUN_ID"
    n "FAIL" "TR Trading: Fehler bei State→Reports (RC=$WRC)" "TRADING_PIPELINE"
    n "WARN" "Pipeline nach Fehler beendet" "TRADING_PIPELINE"
    logline "TRPIPELINE" "PIPELINE_END | result=fail | rc=$WRC | run_id=$RUN_ID"
    exit 0
fi

logline "TRPIPELINE" "PIPELINE_END | result=ok | run_id=$RUN_ID"
n "OK" "Pipeline beendet" "TRADING_PIPELINE"

exit 0
