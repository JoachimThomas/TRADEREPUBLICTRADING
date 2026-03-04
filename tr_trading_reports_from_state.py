#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import csv
import hashlib
import sys
from pathlib import Path
from collections import defaultdict, deque
from datetime import datetime, date

# -----------------------------
# State + Output
# -----------------------------
STATE = Path.home() / "Library/Application Support/Finanzen/TR_Trading/tr_trading_state.json"

OUT_ROOT = Path(
    "/Users/joachimthomas/Documents/Joachim privat/Banken/Trade Republic/Trading_Reports"
)

GLOBAL_CAP_STATE = (
    Path.home() / "Library/Application Support/Finanzen/global_capital_revenues_taxes.json"
)

DASHBOARD_UPDATE_SH = Path(
    "/Users/joachimthomas/Finanzverwaltung/Programme/Visualisierung/Dashboard/updateDashboardfromState.sh"
)


# -----------------------------
# Helpers
# -----------------------------
def parse_ddmmyyyy(s: str):
    try:
        return datetime.strptime((s or "").strip(), "%d.%m.%Y")
    except Exception:
        return None


def ddmmyyyy_to_ymd(s: str) -> str:
    dt = parse_ddmmyyyy(s)
    return dt.strftime("%Y-%m-%d") if dt else "1900-01-01"


def month_from_ddmmyyyy(s: str) -> str:
    dt = parse_ddmmyyyy(s)
    if not dt:
        return "1900-01"
    return f"{dt.year:04d}-{dt.month:02d}"


def fmt_de(n: float) -> str:
    try:
        return f"{float(n):.2f}".replace(".", ",")
    except Exception:
        return "0,00"


def to_float(x) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace("EUR", "").replace("€", "").strip()
    s = s.replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def to_qty(x) -> float:
    return to_float(x)


def write_csv_atomic(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(header)
        w.writerows(rows)
    tmp.replace(path)


def atomic_write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# -----------------------------
# Global Capital State (v1)
# -----------------------------
def load_global_cap_state() -> dict:
    if not GLOBAL_CAP_STATE.exists():
        now = datetime.now().astimezone().isoformat(timespec="seconds")
        return {
            "meta": {
                "schema": "global_capital_revenues_taxes_v1",
                "created": now,
                "updated": now,
            },
            "entries": {},
        }
    try:
        d = json.loads(GLOBAL_CAP_STATE.read_text(encoding="utf-8"))
        if "entries" not in d or not isinstance(d["entries"], dict):
            d["entries"] = {}
        if "meta" not in d or not isinstance(d["meta"], dict):
            d["meta"] = {}
        return d
    except Exception:
        now = datetime.now().astimezone().isoformat(timespec="seconds")
        return {
            "meta": {
                "schema": "global_capital_revenues_taxes_v1",
                "created": now,
                "updated": now,
            },
            "entries": {},
        }


def upsert_trade_into_global_cap_in_memory(fill: dict, st: dict) -> bool:
    """
    Abgeschlossener Trade-Fill -> Eintrag in global_capital_revenues_taxes.json (in-memory, no file write)
    UID stabil aus buy_uid|sell_uid|isin|sell_ymd|qty
    Returns True if added, False if already present or invalid
    """
    isin = (fill.get("isin") or "").strip()
    sell_date = (fill.get("sell_date") or "").strip()
    buy_uid = (fill.get("buy_uid") or "").strip()
    sell_uid = (fill.get("sell_uid") or "").strip()
    qty = float(fill.get("qty") or 0.0)

    if not isin or not sell_date or not buy_uid or not sell_uid or qty <= 0:
        return False

    ymd = ddmmyyyy_to_ymd(sell_date)
    base = f"{buy_uid}|{sell_uid}|{isin}|{ymd}|{qty:.6f}"
    uid = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

    invest = float(fill.get("invest") or 0.0)
    erloes = float(fill.get("erloes") or 0.0)
    fees = float(fill.get("gebuehren") or 0.0)
    kest = float(fill.get("tax_kest") or 0.0)
    soli = float(fill.get("tax_soli") or 0.0)
    gv_roh = float(fill.get("gv_roh") or 0.0)
    kevsteu = float(fill.get("kevsteu") or 0.0)
    gv_konto = float(fill.get("gv_konto") or 0.0)
    sell_pdf = (fill.get("sell_source_pdf") or "").strip()
    buy_pdf = (fill.get("buy_source_pdf") or "").strip()

    ent = st.setdefault("entries", {})
    if uid in ent:
        return False

    ent[uid] = {
        "uid": uid,
        "issuer": "Trade Republic",
        "sourceSystem": "TradeRepublic",
        "account": "TR_Verrechnungskonto",
        "sourceRef": sell_pdf or "from tr_trading_state",
        "sourceKind": "pdf",
        "sourceUid": sell_uid,
        "docDate": ymd,
        "asOfDate": ymd,
        "bookingDate": ymd,
        "periodFrom": "",
        "periodTo": "",
        "assetType": "Wertpapier",
        "asset": isin,
        "assetName": "",
        "incomeType": "Tradingrevenue",
        "currency": "EUR",
        "kevSteu": round(kevsteu, 2),
        "kest": round(kest, 2),
        "soli": round(soli, 2),
        "keNet": round(gv_konto, 2),
        "note": f"TR-Trade qty={qty:.6f} buy_uid={buy_uid} sell_uid={sell_uid} buy_pdf={buy_pdf} sell_pdf={sell_pdf} invest={invest:.2f} erloes={erloes:.2f} fees={fees:.2f} gv_roh={gv_roh:.2f}",
    }
    return True


def year_paths(year: int):
    base = OUT_ROOT / f"{year:04d}"
    out_year = base / "Jahresübersicht"
    out_months = base / "Monate"
    out_op = base / "OP"
    return base, out_year, out_months, out_op


# -----------------------------
# FIFO Lots + Teilverkäufe
# -----------------------------
def sort_dt(r: dict):
    dt = parse_ddmmyyyy(r.get("trade_date", "")) or datetime(1900, 1, 1)
    tt = (r.get("trade_time", "") or "").strip()
    if tt and re.match(r"^\d{2}:\d{2}$", tt):
        try:
            dt = dt.replace(hour=int(tt[:2]), minute=int(tt[3:5]))
        except Exception:
            pass
    side_rank = 0 if (r.get("side", "").lower() == "kauf") else 1
    return (dt, side_rank, r.get("uid", ""))


def rebuild_reports(ledger_rows: list, year: int, daily_date=None, daily_only: bool = False):
    OUT_BASE, OUT_YEAR, OUT_MONTHS, OUT_OP = year_paths(year)

    OUT_BASE.mkdir(parents=True, exist_ok=True)
    OUT_YEAR.mkdir(parents=True, exist_ok=True)
    OUT_MONTHS.mkdir(parents=True, exist_ok=True)
    OUT_OP.mkdir(parents=True, exist_ok=True)

    # Ensure standard subfolders
    (OUT_YEAR / "Essenz").mkdir(parents=True, exist_ok=True)
    (OUT_YEAR / "Transaktionen").mkdir(parents=True, exist_ok=True)

    lots = defaultdict(deque)
    fills = []

    for r in sorted(ledger_rows, key=sort_dt):
        isin = (r.get("isin", "") or "").strip()
        qty = float(r.get("qty", 0.0) or 0.0)
        if not isin or qty <= 0:
            continue

        side = (r.get("side", "") or "").lower()

        if side == "kauf":
            lots[isin].append(
                {
                    "buy_uid": r.get("uid", ""),
                    "buy_date": r.get("trade_date", ""),
                    "buy_source_pdf": (r.get("source_pdf", "") or "").strip(),
                    "qty_rem": qty,
                    "buy_cash_rem": float(r.get("booking_amount", 0.0) or 0.0),
                    "buy_fee_rem": float(r.get("fee_total", 0.0) or 0.0),
                }
            )

        elif side == "verkauf":
            sell_qty_total = qty
            sell_cash_total = float(r.get("booking_amount", 0.0) or 0.0)
            sell_fee_total = float(r.get("fee_total", 0.0) or 0.0)

            sell_kest_total = float(r.get("tax_kest", 0.0) or 0.0)
            sell_soli_total = float(r.get("tax_soli", 0.0) or 0.0)

            sell_source_pdf = (r.get("source_pdf", "") or "").strip()

            qty_left = sell_qty_total
            while qty_left > 1e-9 and lots[isin]:
                lot = lots[isin][0]
                take = min(qty_left, lot["qty_rem"])

                frac_sell = take / sell_qty_total if sell_qty_total > 0 else 0.0
                sell_cash_part = sell_cash_total * frac_sell
                sell_fee_part = sell_fee_total * frac_sell
                sell_kest_part = sell_kest_total * frac_sell
                sell_soli_part = sell_soli_total * frac_sell

                frac_buy = take / lot["qty_rem"] if lot["qty_rem"] > 0 else 0.0
                buy_cash_part = lot["buy_cash_rem"] * frac_buy
                buy_fee_part = lot["buy_fee_rem"] * frac_buy

                lot["qty_rem"] -= take
                lot["buy_cash_rem"] -= buy_cash_part
                lot["buy_fee_rem"] -= buy_fee_part

                buy_source_pdf = lot.get("buy_source_pdf", "")

                if lot["qty_rem"] <= 1e-9:
                    lots[isin].popleft()

                qty_left -= take

                invest = buy_cash_part - buy_fee_part
                erloes = sell_cash_part + sell_fee_part + sell_kest_part + sell_soli_part
                gebuehren = buy_fee_part + sell_fee_part
                gv_roh = erloes - invest
                kevsteu = gv_roh - gebuehren
                gv_konto = sell_cash_part - buy_cash_part

                fills.append(
                    {
                        "month": month_from_ddmmyyyy(r.get("trade_date", "")),
                        "sell_date": r.get("trade_date", ""),
                        "isin": isin,
                        "qty": take,
                        "buy_date": lot.get("buy_date", ""),
                        "buy_uid": lot.get("buy_uid", ""),
                        "sell_uid": r.get("uid", ""),
                        "buy_source_pdf": buy_source_pdf,
                        "sell_source_pdf": sell_source_pdf,
                        "buy_net": buy_cash_part,
                        "sell_net": sell_cash_part,
                        "fee_buy": buy_fee_part,
                        "fee_sell": sell_fee_part,
                        "tax_kest": sell_kest_part,
                        "tax_soli": sell_soli_part,
                        "invest": invest,
                        "erloes": erloes,
                        "gebuehren": gebuehren,
                        "gv_roh": gv_roh,
                        "kevsteu": kevsteu,
                        "gv_konto": gv_konto,
                    }
                )

    # -------------------------------------------------
    # Jahres-Ledger (alle geschlossenen Trades dieses Jahres)
    # -------------------------------------------------
    year_tx_dir = OUT_YEAR / "Transaktionen"
    overview_path = year_tx_dir / f"TR_Trades_Overview_{year}.csv"

    sum_fees = sum(float(ff.get("gebuehren") or 0.0) for ff in fills)
    sum_kest = sum(float(ff.get("tax_kest") or 0.0) for ff in fills)
    sum_soli = sum(float(ff.get("tax_soli") or 0.0) for ff in fills)
    sum_gv_roh = sum(float(ff.get("gv_roh") or 0.0) for ff in fills)
    sum_kevsteu = sum(float(ff.get("kevsteu") or 0.0) for ff in fills)
    sum_gv = sum(float(ff.get("gv_konto") or 0.0) for ff in fills)

    write_csv_atomic(
        overview_path,
        [
            "Monat",
            "ISIN",
            "Qty",
            "Kauf_Datum",
            "Verkauf_Datum",
            "Kauf_UID",
            "Verkauf_UID",
            "Invest",
            "Erlös",
            "Gebühren",
            "KEST",
            "Soli",
            "G&V_roh",
            "KEvST",
            "G&V",
        ],
        [
            [
                "SUMME",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                fmt_de(sum_fees),
                fmt_de(sum_kest),
                fmt_de(sum_soli),
                fmt_de(sum_gv_roh),
                fmt_de(sum_kevsteu),
                fmt_de(sum_gv),
            ],
            *[
                [
                    f["month"],
                    f["isin"],
                    f"{f['qty']:.6f}".replace(".", ","),
                    f["buy_date"],
                    f["sell_date"],
                    f["buy_uid"],
                    f["sell_uid"],
                    fmt_de(f["invest"]),
                    fmt_de(f["erloes"]),
                    fmt_de(f["gebuehren"]),
                    fmt_de(f["tax_kest"]),
                    fmt_de(f["tax_soli"]),
                    fmt_de(f["gv_roh"]),
                    fmt_de(f["kevsteu"]),
                    fmt_de(f["gv_konto"]),
                ]
                for f in fills
            ],
        ],
    )

    # -------------------------------------------------
    # Tagesreports (pro Trading-Tag anhand sell_date)
    #   - normal run: schreibe ALLE Tage im Jahr, für die es Fills gibt
    #   - daily_only: schreibe NUR daily_date
    # -------------------------------------------------

    def write_daily_for_date(d: date, day_fills: list):
        daily_key = d.strftime("%Y-%m-%d")
        mm = d.strftime("%m")
        month_dir = OUT_MONTHS / mm / "Daily"
        month_dir.mkdir(parents=True, exist_ok=True)
        daily_path = month_dir / f"TR_Trades_Daily_{daily_key}.csv"

        sum_fees = sum(float(ff.get("gebuehren") or 0.0) for ff in day_fills)
        sum_kest = sum(float(ff.get("tax_kest") or 0.0) for ff in day_fills)
        sum_soli = sum(float(ff.get("tax_soli") or 0.0) for ff in day_fills)
        sum_gv_roh = sum(float(ff.get("gv_roh") or 0.0) for ff in day_fills)
        sum_kevsteu = sum(float(ff.get("kevsteu") or 0.0) for ff in day_fills)
        sum_gv = sum(float(ff.get("gv_konto") or 0.0) for ff in day_fills)

        write_csv_atomic(
            daily_path,
            [
                "Monat",
                "ISIN",
                "Qty",
                "Kauf_Datum",
                "Verkauf_Datum",
                "Kauf_UID",
                "Verkauf_UID",
                "Invest",
                "Erlös",
                "Gebühren",
                "KEST",
                "Soli",
                "G&V_roh",
                "KEvST",
                "G&V",
            ],
            [
                [
                    "SUMME",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    fmt_de(sum_fees),
                    fmt_de(sum_kest),
                    fmt_de(sum_soli),
                    fmt_de(sum_gv_roh),
                    fmt_de(sum_kevsteu),
                    fmt_de(sum_gv),
                ],
                *[
                    [
                        ff["month"],
                        ff["isin"],
                        f"{ff['qty']:.6f}".replace(".", ","),
                        ff["buy_date"],
                        ff["sell_date"],
                        ff["buy_uid"],
                        ff["sell_uid"],
                        fmt_de(ff["invest"]),
                        fmt_de(ff["erloes"]),
                        fmt_de(ff["gebuehren"]),
                        fmt_de(ff["tax_kest"]),
                        fmt_de(ff["tax_soli"]),
                        fmt_de(ff["gv_roh"]),
                        fmt_de(ff["kevsteu"]),
                        fmt_de(ff["gv_konto"]),
                    ]
                    for ff in day_fills
                ],
            ],
        )
        return daily_path

    # Gruppiere Fills nach Sell-Day
    by_day = defaultdict(list)
    for f in fills:
        dt_sell = parse_ddmmyyyy(f.get("sell_date", ""))
        if not dt_sell:
            continue
        by_day[dt_sell.date()].append(f)

    if daily_only:
        if daily_date is None:
            daily_date = datetime.now().astimezone().date()
        day_fills = by_day.get(daily_date, [])
        if not day_fills:
            daily_key = daily_date.strftime("%Y-%m-%d")
            print(f"REPORTS daily | date={daily_key} rows=0 result=no_trades")
            return (0, 0, 0, 0)
        p = write_daily_for_date(daily_date, day_fills)
        daily_key = daily_date.strftime("%Y-%m-%d")
        print(f"REPORTS daily | date={daily_key} rows={len(day_fills)} file={p}")
        return (0, 0, 0, 0)

    # Normal run: schreibe alle Tagesreports für vorhandene Trading-Tage
    for d in sorted(by_day.keys()):
        write_daily_for_date(d, by_day[d])

    # -------------------------------------------------
    # Monats-Ledger pro Monat
    # -------------------------------------------------
    by_month_fills = defaultdict(list)
    for f in fills:
        by_month_fills[f.get("month", "1900-01")].append(f)

    for m in sorted(by_month_fills.keys()):
        mm = m[-2:] if len(m) >= 7 else "00"
        month_dir = OUT_MONTHS / mm
        ledger_dir = month_dir / "Transaktionen"
        month_overview_path = ledger_dir / f"TR_Trades_Overview_{m}.csv"

        month_fills = by_month_fills[m]
        sum_fees = sum(float(ff.get("gebuehren") or 0.0) for ff in month_fills)
        sum_kest = sum(float(ff.get("tax_kest") or 0.0) for ff in month_fills)
        sum_soli = sum(float(ff.get("tax_soli") or 0.0) for ff in month_fills)
        sum_gv_roh = sum(float(ff.get("gv_roh") or 0.0) for ff in month_fills)
        sum_kevsteu = sum(float(ff.get("kevsteu") or 0.0) for ff in month_fills)
        sum_gv = sum(float(ff.get("gv_konto") or 0.0) for ff in month_fills)

        write_csv_atomic(
            month_overview_path,
            [
                "Monat",
                "ISIN",
                "Qty",
                "Kauf_Datum",
                "Verkauf_Datum",
                "Kauf_UID",
                "Verkauf_UID",
                "Invest",
                "Erlös",
                "Gebühren",
                "KEST",
                "Soli",
                "G&V_roh",
                "KEvST",
                "G&V",
            ],
            [
                [
                    "SUMME",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    fmt_de(sum_fees),
                    fmt_de(sum_kest),
                    fmt_de(sum_soli),
                    fmt_de(sum_gv_roh),
                    fmt_de(sum_kevsteu),
                    fmt_de(sum_gv),
                ],
                *[
                    [
                        ff["month"],
                        ff["isin"],
                        f"{ff['qty']:.6f}".replace(".", ","),
                        ff["buy_date"],
                        ff["sell_date"],
                        ff["buy_uid"],
                        ff["sell_uid"],
                        fmt_de(ff["invest"]),
                        fmt_de(ff["erloes"]),
                        fmt_de(ff["gebuehren"]),
                        fmt_de(ff["tax_kest"]),
                        fmt_de(ff["tax_soli"]),
                        fmt_de(ff["gv_roh"]),
                        fmt_de(ff["kevsteu"]),
                        fmt_de(ff["gv_konto"]),
                    ]
                    for ff in by_month_fills[m]
                ],
            ],
        )

    # Global Capital State aus abgeschlossenen Trades ergänzen (only if fills)
    upsert_added = 0
    dashboard_updated = 0
    if fills:
        st = load_global_cap_state()
        for f in fills:
            if upsert_trade_into_global_cap_in_memory(f, st):
                upsert_added += 1
        if upsert_added > 0:
            st.setdefault("meta", {})
            st["meta"]["updated"] = datetime.now().astimezone().isoformat(timespec="seconds")
            atomic_write_json(GLOBAL_CAP_STATE, st)
            if DASHBOARD_UPDATE_SH.exists():
                try:
                    from subprocess import run

                    run(["/bin/zsh", str(DASHBOARD_UPDATE_SH)], check=False)
                    dashboard_updated = 1
                except Exception:
                    dashboard_updated = 0
        else:
            dashboard_updated = 0
    else:
        upsert_added = 0
        dashboard_updated = 0

    # -------------------------------------------------
    # Monats-Snapshot (pro Monat EIN File)
    # -------------------------------------------------
    agg = defaultdict(
        lambda: {"count": 0, "gv_konto": 0.0, "kevsteu": 0.0, "kest": 0.0, "soli": 0.0, "fees": 0.0}
    )
    for f in fills:
        m = f["month"]
        agg[m]["count"] += 1
        agg[m]["gv_konto"] += float(f["gv_konto"] or 0.0)
        agg[m]["kevsteu"] += float(f["kevsteu"] or 0.0)
        agg[m]["kest"] += float(f["tax_kest"] or 0.0)
        agg[m]["soli"] += float(f["tax_soli"] or 0.0)
        agg[m]["fees"] += float(f["gebuehren"] or 0.0)

    for m in sorted(agg.keys()):
        mm = m[-2:] if len(m) >= 7 else "00"
        month_dir = OUT_MONTHS / mm
        snap_dir = month_dir / "Essenz"
        snap_path = snap_dir / f"TR_Trades_Snap_Monat_{m}.csv"

        write_csv_atomic(
            snap_path,
            [
                "Monat",
                "Trades_geschlossen",
                "G&V_Konto",
                "KEvST",
                "KEST",
                "Soli",
                "Gebuehren_BuySell",
            ],
            [
                [
                    m,
                    agg[m]["count"],
                    fmt_de(agg[m]["gv_konto"]),
                    fmt_de(agg[m]["kevsteu"]),
                    fmt_de(agg[m]["kest"]),
                    fmt_de(agg[m]["soli"]),
                    fmt_de(agg[m]["fees"]),
                ]
            ],
        )

    # Jahres-Summary (Einzeiler)
    ytd_trades = sum(v["count"] for v in agg.values())
    ytd_gv = sum(v["gv_konto"] for v in agg.values())
    ytd_kev = sum(v["kevsteu"] for v in agg.values())
    ytd_kest = sum(v["kest"] for v in agg.values())
    ytd_soli = sum(v["soli"] for v in agg.values())
    ytd_fees = sum(v["fees"] for v in agg.values())

    sell_dates = [f.get("sell_date", "") for f in fills if f.get("sell_date", "")]
    sell_dates_sorted = sorted(sell_dates, key=lambda s: parse_ddmmyyyy(s) or datetime(1900, 1, 1))
    first_sell = sell_dates_sorted[0] if sell_dates_sorted else ""
    last_sell = sell_dates_sorted[-1] if sell_dates_sorted else ""

    essenz_dir = OUT_YEAR / "Essenz"
    ytd_path = essenz_dir / "TR_YTD_Trading_Summary.csv"
    write_csv_atomic(
        ytd_path,
        [
            "Jahr",
            "Trades_geschlossen",
            "G&V_Konto",
            "KEvST",
            "KEST",
            "Soli",
            "Gebuehren",
            "Erster_Verkauf",
            "Letzter_Verkauf",
        ],
        [
            [
                str(year),
                ytd_trades,
                fmt_de(ytd_gv),
                fmt_de(ytd_kev),
                fmt_de(ytd_kest),
                fmt_de(ytd_soli),
                fmt_de(ytd_fees),
                first_sell,
                last_sell,
            ]
        ],
    )

    # Offene Positionen
    open_rows = []
    open_lots_count = 0
    for isin, dq in lots.items():
        for lot in dq:
            open_rows.append(
                [
                    isin,
                    lot.get("buy_date", ""),
                    lot.get("buy_uid", ""),
                    f"{float(lot.get('qty_rem', 0.0) or 0.0):.6f}".replace(".", ","),
                    fmt_de(lot.get("buy_cash_rem", 0.0)),
                    fmt_de(lot.get("buy_fee_rem", 0.0)),
                ]
            )
            open_lots_count += 1

    write_csv_atomic(
        OUT_OP / "TR_Offene_Positionen.csv",
        ["ISIN", "Kauf_Datum", "Kauf_UID", "Qty_Offen", "BuyNetto_Rest", "Fee_Rest"],
        open_rows,
    )
    return (len(fills), open_lots_count, upsert_added, dashboard_updated)


# -----------------------------
# State -> ledger_rows
# -----------------------------
def load_state_rows():
    if not STATE.exists():
        return []

    data = json.loads(STATE.read_text(encoding="utf-8"))
    trades_map = data.get("trades", {}) or {}
    rows = []

    for uid, t in trades_map.items():
        side = (t.get("side", "") or "").strip()
        trade_date = (t.get("trade_date", "") or "").strip()
        if not side or not trade_date:
            continue

        rows.append(
            {
                "uid": t.get("uid", uid) or uid,
                "side": side,
                "trade_date": trade_date,
                "trade_time": (t.get("trade_time", "") or "").strip(),
                "isin": (t.get("isin", "") or "").strip(),
                "qty": to_qty(t.get("qty", 0.0)),
                "booking_amount": to_float(t.get("booking_amount", 0.0)),
                "fee_total": to_float(t.get("fee_total", 0.0)),
                "tax_kest": to_float(t.get("tax_kest", 0.0)),
                "tax_soli": to_float(t.get("tax_soli", 0.0)),
                "source_pdf": (t.get("source_pdf", "") or "").strip(),
            }
        )

    return rows


def main() -> int:
    try:
        print("REPORTS start")

        # Optional: run daily-only for a specific day (dd.mm.yyyy)
        day_arg = None
        if len(sys.argv) >= 2:
            a1 = (sys.argv[1] or "").strip()
            if re.match(r"^\d{2}\.\d{2}\.\d{4}$", a1):
                day_arg = a1

        if day_arg:
            dt_day = parse_ddmmyyyy(day_arg)
            if not dt_day:
                print(f"REPORTS daily | date={day_arg} rows=0 result=bad_arg")
                return 10

            rows = load_state_rows()
            if not rows:
                print("REPORTS noop | reason=empty_state")
                return 10

            target_year = dt_day.year
            ledger_year = []
            for r in rows:
                dt = parse_ddmmyyyy(r.get("trade_date", ""))
                if dt and dt.year == target_year:
                    ledger_year.append(r)

            if not ledger_year:
                print(
                    f"REPORTS daily | date={dt_day.strftime('%Y-%m-%d')} rows=0 result=no_rows_for_year"
                )
                return 10

            rebuild_reports(ledger_year, target_year, daily_date=dt_day.date(), daily_only=True)
            return 0

        rows = load_state_rows()
        if not rows:
            print("REPORTS noop | reason=empty_state")
            return 10

        by_year = defaultdict(list)
        for r in rows:
            dt = parse_ddmmyyyy(r.get("trade_date", ""))
            if not dt:
                continue
            by_year[dt.year].append(r)

        if not by_year:
            print("REPORTS noop | reason=no_valid_dates")
            return 10

        years = sorted(by_year.keys())
        total_fills = 0
        total_open_lots = 0
        total_upsert_added = 0
        dashboard_updated_any = 0

        for year in years:
            fills_count, open_lots_count, upsert_added, dashboard_updated = rebuild_reports(
                by_year[year], year
            )
            total_fills += fills_count
            total_open_lots += open_lots_count
            total_upsert_added += upsert_added
            if dashboard_updated:
                dashboard_updated_any = 1

        print(
            f"REPORTS ok | years={','.join(str(y) for y in years)} fills={total_fills} open_lots={total_open_lots} global_cap_added={total_upsert_added} dashboard_updated={dashboard_updated_any}"
        )
        return 0

    except Exception as ex:
        print(f"REPORTS fail | error={type(ex).__name__}: {ex}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
