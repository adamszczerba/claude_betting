"""
Live Betting Dashboard  (Plotly Dash)
======================================
Displays:
  1. Scraper log tails for every registered Docker container.
  2. CoinCasino-anchored odds comparison table with fuzzy cross-bookmaker
     matching.  Best odds per market column are highlighted in green.

Run:
    python -m dashboard.app            # from project root
    python dashboard/app.py            # alternative

Opens at http://127.0.0.1:8050 by default.
"""

import datetime
import os
import sys
import urllib.request
import json

from dash import Dash, Input, Output, dcc, html, dash_table

# Make sure project root is on sys.path so ``dashboard.*`` imports work
# regardless of how the script is launched.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dashboard.data_service import scan_today, BOOKMAKERS
from dashboard.matcher import build_grouped_table
from dashboard.logs_service import get_all_logs, CONTAINERS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REFRESH_MS = 2_000  # 2-second auto-refresh
DB_ROOT = os.path.join(_PROJECT_ROOT, "match_database")

# Market columns where we want to highlight maxima across bookmakers
MARKET_COLS = ["odd_1", "odd_X", "odd_2", "odd_over", "odd_under"]

# Bookmaker keys in display order
BK_ORDER = ["coincasino", "betfair", "bet365", "betfair_exchange"]
BK_LABELS = {k: v for k, v in BOOKMAKERS.items()}

# Orchestrator REST API base URL
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://127.0.0.1:8051")
_API_TIMEOUT = 1.5   # seconds

# ---------------------------------------------------------------------------
# Dash app
# ---------------------------------------------------------------------------

app = Dash(__name__, title="Betting Dashboard")


# ---------------------------------------------------------------------------
# Orchestrator API helpers
# ---------------------------------------------------------------------------

def _api_get(path: str) -> list | dict | None:
    """Fetch JSON from orchestrator REST API. Returns None on failure."""
    try:
        url = f"{ORCHESTRATOR_URL}{path}"
        with urllib.request.urlopen(url, timeout=_API_TIMEOUT) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Style helpers  (defined early so all builder functions can use them)
# ---------------------------------------------------------------------------

_BORDER = "1px solid #2a2a4a"


def _th():
    return {"backgroundColor": "#16213e", "color": "#00d4ff",
            "padding": "6px 8px", "border": _BORDER,
            "textAlign": "center", "position": "sticky", "top": "0"}


def _td():
    return {"padding": "4px 8px", "border": _BORDER,
            "textAlign": "center", "verticalAlign": "top"}


def _td_match():
    return {**_td(), "textAlign": "left", "fontWeight": "bold",
            "color": "#fff", "maxWidth": "260px"}


def _td_green():
    return {**_td(), "backgroundColor": "#0a4a2a", "color": "#00ff88",
            "fontWeight": "bold"}


def _td_bk(bk: str):
    colors = {
        "coincasino": "#e6a800",
        "betfair": "#ffcc00",
        "bet365": "#00cc66",
        "betfair_exchange": "#4d94ff",
    }
    return {**_td(), "color": colors.get(bk, "#ccc"), "fontSize": "11px",
            "whiteSpace": "nowrap"}


def _td_ts():
    return {**_td(), "fontSize": "11px", "color": "#888"}


# ---------------------------------------------------------------------------

def _build_opportunities_html(signals: list) -> html.Div:
    """Build the Opportunities table from API signal dicts."""
    if not signals:
        return html.Div("No active signals.",
                        style={"color": "#888", "padding": "20px"})

    header = html.Tr([
        html.Th(h, style=_th()) for h in [
            "Type", "Match", "Tournament", "Bookmaker/Legs",
            "Market", "Edge / Profit %", "BK Odds", "Fair Odds", "Age (s)",
        ]
    ])

    rows = []
    now = datetime.datetime.now()
    for s in signals:
        sig_type = s.get("type", "VALUE")
        ts       = datetime.datetime.fromisoformat(s["timestamp"])
        age_s    = int((now - ts).total_seconds())
        match_   = f"{s.get('team1','')} vs {s.get('team2','')}"

        if sig_type == "VALUE":
            edge_str  = f"+{s['edge_pct']:.2f}%"
            bk_str    = s.get("bookmaker", "")
            market    = s.get("market", "")
            bk_odds   = f"{s.get('bookmaker_odds', ''):.2f}" if s.get("bookmaker_odds") else ""
            fair_odds = f"{s.get('fair_odds', ''):.2f}" if s.get("fair_odds") else ""
            row_color = "#0a4a2a"
            txt_color = "#00ff88"
        else:  # ARB
            profit    = s.get("guaranteed_profit_pct", 0)
            edge_str  = f"+{profit:.2f}%"
            legs      = s.get("legs", [])
            bk_str    = ", ".join(f"{l['bookmaker']}@{l['odds']:.2f}" for l in legs)
            market    = " / ".join(l["market"] for l in legs)
            bk_odds   = ""
            fair_odds = ""
            row_color = "#4a0a0a"
            txt_color = "#ff6666"

        cells = [
            html.Td(sig_type,  style={**_td(), "color": txt_color, "fontWeight": "bold"}),
            html.Td(match_,    style={**_td_match(), "maxWidth": "220px"}),
            html.Td(s.get("tournament", ""), style=_td()),
            html.Td(bk_str,    style={**_td(), "fontSize": "11px"}),
            html.Td(market,    style=_td()),
            html.Td(edge_str,  style={**_td(), "color": txt_color, "fontWeight": "bold"}),
            html.Td(bk_odds,   style=_td()),
            html.Td(fair_odds, style=_td()),
            html.Td(str(age_s), style=_td_ts()),
        ]
        rows.append(html.Tr(cells, style={"backgroundColor": row_color}))

    table = html.Table(
        style={"width": "100%", "borderCollapse": "collapse", "fontSize": "13px"},
        children=[html.Thead(header), html.Tbody(rows)],
    )
    return html.Div(table, style={"overflowX": "auto"})


def _build_ledger_html(bets: list, pnl: dict) -> html.Div:
    """Build the Bet Ledger section."""
    # KPI strip
    kpis = html.Div(
        style={"display": "flex", "gap": "20px", "marginBottom": "16px",
               "flexWrap": "wrap"},
        children=[
            _kpi("Total Bets",   str(pnl.get("total_bets", 0))),
            _kpi("Open",         str(pnl.get("open_bets", 0))),
            _kpi("Wins",         str(pnl.get("wins", 0)),  "#00ff88"),
            _kpi("Losses",       str(pnl.get("losses", 0)), "#ff4444"),
            _kpi("Win Rate",     f"{pnl.get('win_rate_pct', 0):.1f}%"),
            _kpi("Total Staked", f"€{pnl.get('total_staked', 0):.2f}"),
            _kpi("Total P&L",    f"€{pnl.get('total_pnl', 0):.2f}",
                 "#00ff88" if pnl.get("total_pnl", 0) >= 0 else "#ff4444"),
            _kpi("ROI",          f"{pnl.get('roi_pct', 0):.1f}%",
                 "#00ff88" if pnl.get("roi_pct", 0) >= 0 else "#ff4444"),
        ],
    )

    # Daily P&L chart
    daily = pnl.get("daily_pnl", [])
    if daily:
        chart = dcc.Graph(
            figure={
                "data": [{
                    "x": [d["day"] for d in daily],
                    "y": [d["pnl"] for d in daily],
                    "type": "bar",
                    "name": "Daily P&L",
                    "marker": {"color": [
                        "#00ff88" if d["pnl"] >= 0 else "#ff4444" for d in daily
                    ]},
                }],
                "layout": {
                    "plot_bgcolor":  "#1a1a2e",
                    "paper_bgcolor": "#1a1a2e",
                    "font":          {"color": "#e0e0e0"},
                    "title":         {"text": "Daily P&L (EUR)", "font": {"color": "#00d4ff"}},
                    "xaxis":         {"gridcolor": "#333"},
                    "yaxis":         {"gridcolor": "#333", "zeroline": True,
                                      "zerolinecolor": "#555"},
                    "margin":        {"t": 40, "b": 40, "l": 50, "r": 20},
                },
            },
            style={"height": "220px", "marginBottom": "16px"},
        )
    else:
        chart = html.Div()

    # Bets table
    if not bets:
        table = html.Div("No bets recorded yet.", style={"color": "#888", "padding": "20px"})
    else:
        _STATUS_COLORS = {
            "PENDING":       "#888",
            "PLACED":        "#00d4ff",
            "SETTLED_WIN":   "#00ff88",
            "SETTLED_LOSS":  "#ff4444",
            "ABORTED":       "#ff7777",
            "EXPIRED":       "#aaa",
        }
        header = html.Tr([
            html.Th(h, style=_th()) for h in [
                "Created", "Match", "Market", "Outcome",
                "Stake (€)", "Req. Price", "Acc. Price", "Edge %", "Type", "Status",
            ]
        ])
        rows = []
        for b in bets[:100]:  # last 100
            status = b.get("status", "")
            sc     = _STATUS_COLORS.get(status, "#ccc")
            ts     = (b.get("created_at") or "")
            if "T" in ts:
                ts = ts.replace("T", " ")[:19]
            match_ = f"{b.get('team1','')} vs {b.get('team2','')}"
            cells = [
                html.Td(ts,                            style=_td_ts()),
                html.Td(match_,                        style=_td_match()),
                html.Td(b.get("market", ""),           style=_td()),
                html.Td(b.get("outcome", ""),          style=_td()),
                html.Td(f"{b.get('stake_eur',0):.2f}", style=_td()),
                html.Td(f"{b.get('min_price',0):.4f}", style=_td()),
                html.Td(f"{b.get('accepted_price','')}" if b.get("accepted_price") else "—", style=_td()),
                html.Td(f"{b.get('edge_pct',0):+.1f}%", style=_td()),
                html.Td(b.get("signal_type", ""),      style=_td()),
                html.Td(status, style={**_td(), "color": sc, "fontWeight": "bold"}),
            ]
            rows.append(html.Tr(cells))
        table = html.Div(
            html.Table(
                style={"width": "100%", "borderCollapse": "collapse", "fontSize": "12px"},
                children=[html.Thead(header), html.Tbody(rows)],
            ),
            style={"overflowX": "auto"},
        )

    return html.Div([kpis, chart, table])


def _kpi(label: str, value: str, color: str = "#00d4ff") -> html.Div:
    return html.Div(
        style={"backgroundColor": "#16213e", "borderRadius": "8px",
               "padding": "10px 18px", "minWidth": "110px", "textAlign": "center"},
        children=[
            html.Div(label, style={"fontSize": "11px", "color": "#888", "marginBottom": "4px"}),
            html.Div(value, style={"fontSize": "20px", "fontWeight": "bold", "color": color}),
        ],
    )


def _build_snapshot(n_intervals: int = 0):
    """Build a full dashboard snapshot for layout init and periodic refresh."""
    now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    last_update = f"Last update: {now}  (refresh #{n_intervals})"

    # Logs panels
    logs_data = get_all_logs()
    log_panels = []
    for cname in CONTAINERS:
        info = logs_data.get(cname, {"label": cname, "running": False,
                                     "logs": "(no data)"})
        status_color = "#00ff88" if info["running"] else "#ff4444"
        status_text = "RUNNING" if info["running"] else "STOPPED"
        panel = html.Div(
            style={"flex": "1 1 48%", "minWidth": "450px",
                   "backgroundColor": "#16213e", "borderRadius": "8px",
                   "padding": "10px", "border": f"1px solid {status_color}30"},
            children=[
                html.Div(
                    style={"display": "flex", "justifyContent": "space-between",
                           "marginBottom": "5px"},
                    children=[
                        html.Strong(info["label"],
                                    style={"color": "#00d4ff"}),
                        html.Span(status_text,
                                  style={"color": status_color,
                                         "fontWeight": "bold",
                                         "fontSize": "12px"}),
                    ],
                ),
                html.Pre(
                    info["logs"],
                    style={"maxHeight": "180px", "overflowY": "auto",
                           "fontSize": "11px", "backgroundColor": "#0f0f23",
                           "padding": "8px", "borderRadius": "4px",
                           "whiteSpace": "pre-wrap", "wordBreak": "break-all",
                           "color": "#ccc", "margin": "0"},
                ),
            ],
        )
        log_panels.append(panel)

    # Odds table
    all_rows = scan_today(db_root=DB_ROOT)
    grouped = build_grouped_table(all_rows)
    try:
        odds_content = _build_odds_html(grouped) if grouped else html.Div(
            "No match data available yet.", style={"color": "#888", "padding": "20px"})
    except Exception:
        odds_content = html.Div("Loading odds data...",
                                style={"color": "#aaa", "padding": "20px"})

    # Opportunities (from orchestrator API)
    signals = _api_get("/api/signals") or []
    opps_content = _build_opportunities_html(signals)

    # Bet Ledger (from orchestrator API)
    bets = _api_get("/api/bets") or []
    pnl  = _api_get("/api/pnl") or {}
    ledger_content = _build_ledger_html(bets, pnl)

    return last_update, log_panels, odds_content, opps_content, ledger_content


_initial_last_update, _initial_log_panels, _initial_odds_content, \
    _initial_opps, _initial_ledger = _build_snapshot(0)

_TAB_STYLE        = {"backgroundColor": "#16213e", "color": "#888",
                     "borderColor": "#2a2a4a", "padding": "8px 18px"}
_TAB_SELECTED     = {"backgroundColor": "#1a1a2e", "color": "#00d4ff",
                     "borderColor": "#00d4ff", "borderBottom": "2px solid #00d4ff",
                     "padding": "8px 18px", "fontWeight": "bold"}

app.layout = html.Div(
    style={"fontFamily": "Consolas, monospace", "margin": "10px 20px",
           "backgroundColor": "#1a1a2e", "color": "#e0e0e0",
           "minHeight": "100vh", "padding": "20px"},
    children=[
        html.H1("Live Betting Dashboard",
                style={"textAlign": "center", "color": "#00d4ff",
                       "marginBottom": "5px"}),
        html.Div(id="last-update",
                 children=_initial_last_update,
                 style={"textAlign": "center", "fontSize": "12px",
                        "color": "#888", "marginBottom": "20px"}),

        dcc.Interval(id="interval", interval=REFRESH_MS, n_intervals=0),

        dcc.Tabs(
            id="main-tabs",
            value="tab-logs",
            style={"marginBottom": "20px"},
            children=[
                dcc.Tab(label="Scraper Logs",     value="tab-logs",
                        style=_TAB_STYLE, selected_style=_TAB_SELECTED),
                dcc.Tab(label="Odds Comparison",  value="tab-odds",
                        style=_TAB_STYLE, selected_style=_TAB_SELECTED),
                dcc.Tab(label="🔥 Opportunities", value="tab-opps",
                        style=_TAB_STYLE, selected_style=_TAB_SELECTED),
                dcc.Tab(label="📒 Bet Ledger",    value="tab-ledger",
                        style=_TAB_STYLE, selected_style=_TAB_SELECTED),
            ],
        ),

        # ── Tab 1: Scraper Logs ──────────────────────────────────────────
        html.Div(
            id="tab-logs-content",
            children=html.Div(
                id="logs-container",
                children=_initial_log_panels,
                style={"display": "flex", "flexWrap": "wrap", "gap": "10px"},
            ),
        ),

        # ── Tab 2: Odds Comparison ───────────────────────────────────────
        html.Div(id="tab-odds-content",
                 children=html.Div(id="odds-table-container",
                                   children=_initial_odds_content),
                 style={"display": "none"}),

        # ── Tab 3: Opportunities ─────────────────────────────────────────
        html.Div(id="tab-opps-content",
                 children=html.Div(id="opps-container",
                                   children=_initial_opps),
                 style={"display": "none"}),

        # ── Tab 4: Bet Ledger ────────────────────────────────────────────
        html.Div(id="tab-ledger-content",
                 children=html.Div(id="ledger-container",
                                   children=_initial_ledger),
                 style={"display": "none"}),
    ],
)


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

@app.callback(
    Output("last-update", "children"),
    Output("logs-container", "children"),
    Output("odds-table-container", "children"),
    Output("opps-container", "children"),
    Output("ledger-container", "children"),
    Input("interval", "n_intervals"),
)
def refresh(n_intervals: int):
    try:
        last_update, logs, odds, opps, ledger = _build_snapshot(n_intervals)
        return last_update, logs, odds, opps, ledger
    except Exception as exc:
        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        err = html.Div(
            f"Dashboard refresh error: {exc}",
            style={"color": "#ff7777", "padding": "12px", "fontWeight": "bold"},
        )
        return f"Last update: {now}  (refresh failed)", [], err, err, err


@app.callback(
    Output("tab-logs-content",   "style"),
    Output("tab-odds-content",   "style"),
    Output("tab-opps-content",   "style"),
    Output("tab-ledger-content", "style"),
    Input("main-tabs", "value"),
)
def switch_tab(tab: str):
    show = {"display": "block"}
    hide = {"display": "none"}
    return (
        show if tab == "tab-logs"   else hide,
        show if tab == "tab-odds"   else hide,
        show if tab == "tab-opps"   else hide,
        show if tab == "tab-ledger" else hide,
    )


# ---------------------------------------------------------------------------
# Odds table builder
# ---------------------------------------------------------------------------

def _safe_float(val) -> float | None:
    """Parse a string to float, returning None on failure."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def _fmt_score(bk_data: dict) -> str:
    h = bk_data.get("home_score", "")
    a = bk_data.get("away_score", "")
    if h and a:
        return f"{h} - {a}"
    return ""


def _build_odds_html(grouped: list) -> html.Div:
    """Build an HTML table with per-cell green highlights for best odds."""

    header_cells = [
        html.Th("Match", style=_th()),
        html.Th("Tournament", style=_th()),
        html.Th("Score", style=_th()),
        html.Th("Time", style=_th()),
        html.Th("Status", style=_th()),
    ]
    for col in MARKET_COLS:
        header_cells.append(html.Th(col, style=_th()))
    header_cells.append(html.Th("Bookmaker", style=_th()))
    header_cells.append(html.Th("Updated", style=_th()))

    rows = []
    for entry in sorted(grouped, key=lambda e: e["team1"].lower()):
        # Determine max per market column across all available bookmakers
        maxima: dict[str, float | None] = {}
        for col in MARKET_COLS:
            vals: list[float] = []
            for bk in BK_ORDER:
                bk_data = entry["odds"].get(bk)
                if bk_data:
                    v = _safe_float(bk_data.get(col))
                    if v is not None:
                        vals.append(v)
            maxima[col] = max(vals) if vals else None

        # One sub-row per bookmaker that has data
        first = True
        for bk in BK_ORDER:
            bk_data = entry["odds"].get(bk)
            if bk_data is None:
                continue

            cells = []
            if first:
                match_label = f"{entry['team1']} vs {entry['team2']}"
                cells.append(html.Td(match_label, style=_td_match()))
                cells.append(html.Td(entry["tournament"], style=_td()))
                first = False
            else:
                cells.append(html.Td("", style=_td()))
                cells.append(html.Td("", style=_td()))

            score_str = _fmt_score(bk_data)
            cells.append(html.Td(score_str, style=_td()))
            cells.append(html.Td(bk_data.get("match_time", ""), style=_td()))
            cells.append(html.Td(bk_data.get("match_status", ""), style=_td()))

            for col in MARKET_COLS:
                raw = bk_data.get(col, "")
                v = _safe_float(raw)
                is_best = (v is not None and maxima[col] is not None
                           and abs(v - maxima[col]) < 1e-9
                           and _count_sources(entry, col) > 1)
                style = _td_green() if is_best else _td()
                display = f"{v:.2f}" if v is not None else ""
                cells.append(html.Td(display, style=style))

            bk_label = BK_LABELS.get(bk, bk)
            score_info = entry["match_scores"].get(bk)
            if score_info is not None:
                bk_label += f" ({score_info:.0f}%)"
            cells.append(html.Td(bk_label, style=_td_bk(bk)))

            ts = bk_data.get("timestamp", "")
            if ts and "T" in ts:
                ts = ts.split("T")[1][:8]
            cells.append(html.Td(ts, style=_td_ts()))

            rows.append(html.Tr(cells))

        # Separator row between matches
        rows.append(html.Tr(
            [html.Td("", colSpan=len(header_cells),
                      style={"borderBottom": "1px solid #333",
                             "height": "2px", "padding": "0"})],
        ))

    table = html.Table(
        style={"width": "100%", "borderCollapse": "collapse",
               "fontSize": "13px"},
        children=[
            html.Thead(html.Tr(header_cells)),
            html.Tbody(rows),
        ],
    )
    return html.Div(table, style={"overflowX": "auto"})


def _count_sources(entry: dict, col: str) -> int:
    """How many bookmakers have a valid value for *col* in this entry."""
    n = 0
    for bk in BK_ORDER:
        bk_data = entry["odds"].get(bk)
        if bk_data and _safe_float(bk_data.get(col)) is not None:
            n += 1
    return n



# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Live Betting Dashboard")
    parser.add_argument("--debug", action="store_true", help="Enable Dash debug mode")
    parser.add_argument("--port", type=int, default=8050)
    args = parser.parse_args()
    app.run(debug=args.debug, host="0.0.0.0", port=args.port)
