#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# LAST_CHANGE: 2026-03-06 11:06 (Europe/Berlin)

# ------------------------------------------------------------
# Exitcodes (für Pipeline-Steuerung)
#   0  = neue Trades in State geschrieben (Reports laufen)
#   10 = NO-OP (nichts neu / nur doppelt / nichts gefunden)
#   1  = Fehler
# ------------------------------------------------------------
RC_OK_UPDATED = 0
RC_NOOP = 10
RC_FAIL = 1


import re
import json
import time
import shutil
import hashlib
from pathlib import Path
from datetime import datetime
import pdfplumber
import os


# ------------------------------------------------------------
# Minimaler Fortschritts-Output für die Pipeline
# ------------------------------------------------------------
def note(msg: str):
    """Minimaler Fortschritts-Output für die Pipeline (stdout)."""
    try:
        print(f"NOTE|{msg}")
    except Exception:
        pass


# ------------------------------------------------------------
# Pfade
# ------------------------------------------------------------
IN_DIR = Path(
    "/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Abrechnungen"
)

ARCH_PDF = Path(
    "/Users/joachimthomas/Finanzverwaltung/Archiv/TradeRepublic/Trading/WertpapierAbrechnungen/Trading"
)
ERR_PDF = ARCH_PDF / "nicht_verarbeitet"

STATE_DIR = Path.home() / "Library" / "Application Support" / "Finanzen" / "TR_Trading"
STATE_PATH = STATE_DIR / "tr_trading_state.json"

LOCK_PATH = STATE_DIR / ".tr_trading_import.lock"

# ------------------------------------------------------------
# Regex / Money
# ------------------------------------------------------------
MONEY_RE = r"[+\-]?\s*[\d\.]+,\d{2}"

TAX_KEYS = [
    ("kapitalertragsteuer", "tax_kest"),
    ("soli", "tax_soli"),
    ("solidaritätszuschlag", "tax_soli"),
    ("kirchensteuer", "tax_kist"),
    ("quellensteuer", "tax_quellen"),
]

FEE_KEYS = [
    ("fremdkostenzuschlag", "fee_fremdkosten"),
    ("fremdkosten", "fee_fremdkosten"),
    ("gebühr", "fee_sonst"),
    ("entgelt", "fee_sonst"),
]

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------


def ensure_dirs():
    IN_DIR.mkdir(parents=True, exist_ok=True)
    ARCH_PDF.mkdir(parents=True, exist_ok=True)
    ERR_PDF.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def acquire_lock_or_exit() -> bool:
    """
    Einfache Single-Instance-Sperre.
    Wenn Lock existiert und Prozess lebt -> exit (False).
    Wenn Lock verwaist -> ersetzen (True).
    """
    now = time.time()

    if LOCK_PATH.exists():
        try:
            txt = LOCK_PATH.read_text(encoding="utf-8").strip()
            pid = int(txt.split("|", 1)[0])
            ts = float(txt.split("|", 1)[1]) if "|" in txt else 0.0
        except Exception:
            pid, ts = 0, 0.0

        if pid > 0:
            try:
                os.kill(pid, 0)
                return False  # lebt
            except Exception:
                pass

        # verwaist -> löschen
        try:
            LOCK_PATH.unlink(missing_ok=True)
        except Exception:
            pass

        # falls sehr alt -> egal, wird ersetzt
        if (now - ts) > 3600:
            try:
                LOCK_PATH.unlink(missing_ok=True)
            except Exception:
                pass

    try:
        LOCK_PATH.write_text(f"{os.getpid()}|{now}", encoding="utf-8")
        return True
    except Exception:
        return False


def release_lock():
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def stable_wait(p: Path, loops: int = 200, sleep_s: float = 0.25) -> bool:
    """
    Wartet, bis Datei über mehrere Zyklen stabil ist.
    Gibt True zurück wenn stabil, sonst False.
    """
    last_size = -1
    last_mtime = -1
    stable = 0

    for _ in range(loops):
        try:
            st = p.stat()
            sz = st.st_size
            mt = int(st.st_mtime)
        except Exception:
            time.sleep(sleep_s)
            continue

        if sz > 0 and sz == last_size and mt == last_mtime:
            stable += 1
            if stable >= 6:
                return True
        else:
            stable = 0
            last_size = sz
            last_mtime = mt

        time.sleep(sleep_s)

    return False


def unique_dest(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem, suf = dest.stem, dest.suffix
    i = 2
    while True:
        cand = dest.with_name(f"{stem}_{i}{suf}")
        if not cand.exists():
            return cand
        i += 1


def de_money_to_float(s: str) -> float:
    s = (s or "").strip().replace("EUR", "").replace("€", "").strip()
    if not s:
        return 0.0
    s = s.replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def pick(pattern, text, flags=0, default=""):
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else default


def safe_filename(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(" ", "_")
    s = re.sub(r"[^A-Za-z0-9._-]+", "", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("._-") or "unknown"


def date_to_ymd_from_ddmmyyyy(ddmmyyyy: str) -> str:
    try:
        dt = datetime.strptime(ddmmyyyy, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "1900-01-01"


def yyyymm_from_ddmmyyyy(ddmmyyyy: str) -> tuple[str, str]:
    try:
        dt = datetime.strptime(ddmmyyyy, "%d.%m.%Y")
        return (f"{dt.year:04d}", f"{dt.month:02d}")
    except Exception:
        return ("unknown", "00")


def build_archive_pdf_name(parsed_row: dict, fallback_name: str) -> str:
    isin = safe_filename(parsed_row.get("isin", ""))
    side = safe_filename((parsed_row.get("side", "") or "").lower())
    d = safe_filename(date_to_ymd_from_ddmmyyyy(parsed_row.get("trade_date", "")))

    if (
        isin in ("", "unknown")
        or side in ("", "unknown")
        or d in ("", "unknown", "1900-01-01")
    ):
        return safe_filename(Path(fallback_name).name)

    return f"{isin}_{side}_{d}.pdf"


def extract_text(pdf_path: Path) -> str:
    with pdfplumber.open(str(pdf_path)) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)


# ------------------------------------------------------------
# Gebühren & Steuern aus ABRECHNUNG-Block
# ------------------------------------------------------------
def parse_money_line(label: str, text: str) -> float:
    pat = rf"{re.escape(label)}\s+({MONEY_RE})\s+EUR"
    m = re.search(pat, text, flags=re.I)
    return de_money_to_float(m.group(1)) if m else 0.0


def parse_fee_tax_blocks(text: str) -> dict:
    out = {
        "fee_fremdkosten": 0.0,
        "fee_sonst": 0.0,
        "tax_kest": 0.0,
        "tax_soli": 0.0,
        "tax_kist": 0.0,
        "tax_quellen": 0.0,
        "tax_sonst": 0.0,
    }

    ab = ""
    m = re.search(r"\bABRECHNUNG\b(.*?)(\bBUCHUNG\b|$)", text, flags=re.S | re.I)
    if m:
        ab = m.group(1)

    for key, field in FEE_KEYS:
        v = parse_money_line(key, ab)
        if v != 0.0:
            out[field] += v

    for key, field in TAX_KEYS:
        v = parse_money_line(key, ab)
        if v != 0.0:
            out[field] += v

    for line in ab.splitlines():
        if re.search(r"steuer", line, flags=re.I):
            mm = re.search(rf"({MONEY_RE})\s+EUR", line, flags=re.I)
            if mm:
                val = de_money_to_float(mm.group(1))
                if not any(re.search(k, line, flags=re.I) for k, _ in TAX_KEYS):
                    out["tax_sonst"] += val

    return out


# ------------------------------------------------------------
# PDF -> Trade-Record
# ------------------------------------------------------------
def parse_one(text: str, source_pdf: str) -> dict:
    side = pick(r"\b[\w-]+-Order\s+(Kauf|Verkauf)\s+am\b", text, flags=re.I)
    side = side.capitalize() if side else ""

    trade_date = pick(
        r"\b[\w-]+-Order\s+(?:Kauf|Verkauf)\s+am\s+(\d{2}\.\d{2}\.\d{4})\b",
        text,
        flags=re.I,
    )
    trade_time = pick(r"\bum\s+(\d{2}:\d{2})\s+Uhr\b", text, flags=re.I)

    isin = pick(r"\bISIN:\s*([A-Z]{2}[A-Z0-9]{10})\b", text)

    qty_raw = pick(r"\b([\d\.,]+)\s*Stk\.?\b", text, flags=re.I)
    qty = 0.0
    if qty_raw:
        try:
            qty = float(qty_raw.replace(" ", "").replace(".", "").replace(",", "."))
        except Exception:
            qty = 0.0

    order_id = pick(r"\bAUFTRAG\s+([A-Za-z0-9-]+)\b", text)
    exec_id = pick(r"\bAUSFÜHRUNG\s+([A-Za-z0-9-]+)\b", text)

    book_amount = pick(
        rf"\b\d{{4}}-\d{{2}}-\d{{2}}\s+({MONEY_RE})\s+EUR\b", text, flags=re.I
    )
    booking_amount = de_money_to_float(book_amount) if book_amount else 0.0

    ft = parse_fee_tax_blocks(text)
    fee_total = ft["fee_fremdkosten"] + ft["fee_sonst"]
    tax_total = (
        ft["tax_kest"]
        + ft["tax_soli"]
        + ft["tax_kist"]
        + ft["tax_quellen"]
        + ft["tax_sonst"]
    )

    # Normalisierung: Beträge absolut
    booking_amount = abs(float(booking_amount or 0.0))
    for k in [
        "fee_fremdkosten",
        "fee_sonst",
        "tax_kest",
        "tax_soli",
        "tax_kist",
        "tax_quellen",
        "tax_sonst",
    ]:
        ft[k] = abs(float(ft.get(k, 0.0) or 0.0))
    fee_total = abs(float(fee_total or 0.0))
    tax_total = abs(float(tax_total or 0.0))

    raw_amount = max(0.0, booking_amount - fee_total)
    unit_price_raw = (raw_amount / qty) if qty > 0 else 0.0

    basekey = exec_id or (
        (order_id or "")
        + "|"
        + source_pdf
        + "|"
        + side
        + "|"
        + trade_date
        + "|"
        + isin
        + "|"
        + str(qty)
        + "|"
        + str(booking_amount)
    )
    uid = hashlib.sha1(basekey.encode("utf-8")).hexdigest()[:16]

    return {
        "uid": uid,
        "source_pdf": source_pdf,
        "order_id": order_id,
        "exec_id": exec_id,
        "side": side,
        "trade_date": trade_date,
        "trade_time": trade_time,
        "isin": isin,
        "qty": qty,
        "booking_amount": booking_amount,
        "raw_amount": raw_amount,
        "unit_price_raw": unit_price_raw,
        "fee_fremdkosten": ft["fee_fremdkosten"],
        "fee_sonst": ft["fee_sonst"],
        "fee_total": fee_total,
        "tax_kest": ft["tax_kest"],
        "tax_soli": ft["tax_soli"],
        "tax_kist": ft["tax_kist"],
        "tax_quellen": ft["tax_quellen"],
        "tax_sonst": ft["tax_sonst"],
        "tax_total": tax_total,
    }


# ------------------------------------------------------------
# State IO
# ------------------------------------------------------------
def load_state() -> dict:
    if not STATE_PATH.exists():
        return {
            "meta": {
                "schema": "tr_trading_state_v1",
                "created": datetime.now().isoformat(timespec="seconds"),
                "updated": datetime.now().isoformat(timespec="seconds"),
            },
            "trades": {},
        }
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if "trades" not in data or not isinstance(data["trades"], dict):
            data["trades"] = {}
        if "meta" not in data or not isinstance(data["meta"], dict):
            data["meta"] = {}
        return data
    except Exception:
        return {
            "meta": {
                "schema": "tr_trading_state_v1",
                "created": datetime.now().isoformat(timespec="seconds"),
                "updated": datetime.now().isoformat(timespec="seconds"),
            },
            "trades": {},
        }


def atomic_write_json(path: Path, data: dict):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def main() -> int:
    ensure_dirs()
    note("started")

    if not acquire_lock_or_exit():
        note("ended|result=noop|reason=locked")
        return RC_NOOP

    try:
        pdfs = sorted(IN_DIR.glob("*.pdf"), key=lambda p: p.stat().st_mtime)
        if not pdfs:
            note("ended|result=noop|reason=no_pdfs")
            return RC_NOOP

        state = load_state()
        trades = state.get("trades", {})
        if not isinstance(trades, dict):
            trades = {}
            state["trades"] = trades
        known_uids = set(trades.keys())

        skipped_unstable = 0
        moved_errors = 0
        added = 0
        already = 0

        for p in pdfs:
            if not stable_wait(p):
                skipped_unstable += 1
                continue

            try:
                text = extract_text(p)
                parsed = parse_one(text, p.name)
                note(
                    f"pdf_read|isin={parsed.get('isin','')}|side={parsed.get('side','')}|date={parsed.get('trade_date','')}|qty={parsed.get('qty','')}"
                )
            except Exception:
                parsed = {}

            ok = (
                bool(parsed)
                and bool(parsed.get("isin"))
                and bool(parsed.get("side"))
                and bool(parsed.get("trade_date"))
                and float(parsed.get("qty") or 0.0) != 0.0
            )

            if not ok:
                yy, mm = yyyymm_from_ddmmyyyy((parsed or {}).get("trade_date", ""))
                err_dir = ERR_PDF / yy / mm
                err_dir.mkdir(parents=True, exist_ok=True)

                dest_err = unique_dest(err_dir / safe_filename(p.name))
                try:
                    shutil.move(str(p), str(dest_err))
                    moved_errors += 1
                except Exception:
                    pass
                continue

            # Zielname + Zielordner (yyyy/mm)
            archive_name = build_archive_pdf_name(parsed, p.name)

            yy, mm = yyyymm_from_ddmmyyyy(parsed.get("trade_date", ""))
            dest_dir = ARCH_PDF / yy / mm
            dest_dir.mkdir(parents=True, exist_ok=True)

            dest = unique_dest(dest_dir / archive_name)

            try:
                shutil.move(str(p), str(dest))
            except Exception:
                continue  # move fehlgeschlagen -> nichts am State ändern

            parsed["source_pdf"] = dest.name

            uid = parsed.get("uid", "")
            if not uid:
                continue

            if uid in known_uids:
                already += 1
                continue

            trades[uid] = parsed
            known_uids.add(uid)
            added += 1

        # Pipeline-Summary (nur 1x)
        note(
            f"Pipeline-Summary |added={added}|already={already}|moved_errors={moved_errors}|skipped_unstable={skipped_unstable}"
        )

        # State nur schreiben, wenn wirklich etwas passiert ist
        if added > 0:
            state.setdefault("meta", {})
            state["meta"]["updated"] = datetime.now().isoformat(timespec="seconds")
            atomic_write_json(STATE_PATH, state)
            note(f"state_written|added={added}")
            note("ended|result=updated")
            return RC_OK_UPDATED

        note("state_not_written|reason=no_new_entries")
        note("ended|result=noop")
        return RC_NOOP

    except Exception:
        note("ended|result=fail")
        return RC_FAIL

    finally:
        release_lock()


if __name__ == "__main__":
    raise SystemExit(main())
