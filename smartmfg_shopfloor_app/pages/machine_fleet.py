"""
pages/machine_fleet.py — Machine Fleet layout and callbacks.

Card grid showing every machine with real-time sensor health,
active order count, and a list of parts currently being manufactured.
Status filter chips (All / Active / Maintenance / Idle).
"""

import dash
from dash import html, Input, Output, callback
import dash_bootstrap_components as dbc

from server import backend
from components.ui import C, CARD_STYLE, STATUS_CFG, page_title


# ── Layout ─────────────────────────────────────────────────────────────────

def layout():
    return html.Div([
        page_title(
            "Machine Fleet",
            "All machines and the parts they are currently manufacturing",
        ),

        # Status filter chips
        html.Div([
            dbc.Button("All",         id="fleet-filter-all",   color="primary", size="sm",
                       outline=False, className="me-2 mb-3", n_clicks=0),
            dbc.Button("Active",      id="fleet-filter-active", color="success", size="sm",
                       outline=True,  className="me-2 mb-3", n_clicks=0),
            dbc.Button("Maintenance", id="fleet-filter-maint",  color="danger",  size="sm",
                       outline=True,  className="me-2 mb-3", n_clicks=0),
            dbc.Button("Idle",        id="fleet-filter-idle",   color="warning", size="sm",
                       outline=True,  className="me-2 mb-3", n_clicks=0),
        ]),

        # Machine card grid — populated by callback
        html.Div(id="fleet-cards", className="row g-3"),
    ])


# ── Callbacks ──────────────────────────────────────────────────────────────

@callback(
    Output("fleet-cards",        "children"),
    Input("refresh-interval",    "n_intervals"),
    Input("fleet-filter-all",    "n_clicks"),
    Input("fleet-filter-active", "n_clicks"),
    Input("fleet-filter-maint",  "n_clicks"),
    Input("fleet-filter-idle",   "n_clicks"),
    prevent_initial_call=False,
)
def refresh_fleet(_, _a, _act, _m, _i):
    ctx       = dash.callback_context
    triggered = ctx.triggered[0]["prop_id"] if ctx.triggered else ""

    fleet_df  = backend.get_machine_fleet()
    if fleet_df.empty:
        return html.P("No data available.", className="text-muted")

    if   "active" in triggered:
        fleet_df = fleet_df[fleet_df["status"] == "Active"]
    elif "maint"  in triggered:
        fleet_df = fleet_df[fleet_df["status"] == "Maintenance"]
    elif "idle"   in triggered:
        fleet_df = fleet_df[fleet_df["status"] == "Idle"]

    return [_machine_card(row) for _, row in fleet_df.iterrows()]


# ── Private ────────────────────────────────────────────────────────────────

def _machine_card(row):
    """Build one machine card for the fleet grid."""
    sc     = STATUS_CFG.get(row["status"], STATUS_CFG["Idle"])
    health = float(row.get("latest_health_score") or 0)
    wear   = float(row.get("tool_wear_pct")        or 0)

    health_c = C["green"] if health >= 70 else C["amber"] if health >= 40 else C["red"]
    wear_c   = C["red"]   if wear   >= 80 else C["amber"] if wear   >= 60 else C["green"]
    anomaly  = (
        dbc.Badge("⚠ Anomaly", color="danger", className="ms-1 px-2")
        if row.get("anomaly_flag") else html.Span()
    )

    # Part chips
    parts_raw = row.get("parts_in_progress") or ""
    part_chips = [
        html.Span(p.strip(), style={
            "display":         "inline-block",
            "backgroundColor": "#f0fdf4",
            "borderRadius":    "4px",
            "padding":         "1px 7px",
            "margin":          "1px 2px",
            "fontSize":        "0.7rem",
            "color":           "#15803d",
            "border":          "1px solid #bbf7d0",
        })
        for p in parts_raw.split(",") if p.strip()
    ]

    return dbc.Col(
        html.Div([
            # ── Header ──────────────────────────────────────────────────
            html.Div([
                html.Div([
                    html.I(className=f"{sc['icon']} me-2", style={"color": sc["color"]}),
                    html.Span(row["machine_id"], className="fw-bold me-2",
                              style={"color": C["text"]}),
                    dbc.Badge(sc["label"], color=sc["badge"],
                              className="px-2 py-1", style={"fontSize": "0.65rem"}),
                    anomaly,
                ], className="d-flex align-items-center"),
                html.Span(f"${row['hourly_rate_usd']}/hr",
                          className="small", style={"color": C["muted"]}),
            ], className="d-flex justify-content-between align-items-start mb-2"),

            # ── Machine name + type / work-centre tags ───────────────
            html.Div(row["machine_name"], className="fw-semibold mb-1",
                     style={"color": C["text"]}),
            html.Div([
                html.Span(row["machine_type"], className="badge me-2",
                          style={"backgroundColor": "#f1f5f9", "color": C["muted"],
                                 "fontSize": "0.65rem", "border": "1px solid #e2e8f0"}),
                html.Span(row["work_center"],  className="badge",
                          style={"backgroundColor": "#eff6ff", "color": C["blue"],
                                 "fontSize": "0.65rem", "border": "1px solid #dbeafe"}),
            ], className="mb-3"),

            html.Hr(style={"borderColor": C["border"], "margin": "0.5rem 0"}),

            # ── Metrics row ──────────────────────────────────────────
            dbc.Row([
                dbc.Col([
                    html.Div("Health Score", style={"color": C["muted"], "fontSize": "0.7rem"}),
                    html.Div(f"{health:.0f}", className="fw-bold",
                             style={"color": health_c, "fontSize": "1.3rem"}),
                ], width=4),
                dbc.Col([
                    html.Div("Tool Wear", style={"color": C["muted"], "fontSize": "0.7rem"}),
                    html.Div(f"{wear:.0f}%", className="fw-bold",
                             style={"color": wear_c, "fontSize": "1.3rem"}),
                ], width=4),
                dbc.Col([
                    html.Div("Active Orders", style={"color": C["muted"], "fontSize": "0.7rem"}),
                    html.Div(str(int(row.get("active_orders") or 0)),
                             className="fw-bold",
                             style={"color": C["cyan"], "fontSize": "1.3rem"}),
                ], width=4),
            ], className="mb-3 g-0"),

            # ── Parts in progress ────────────────────────────────────
            html.Div([
                html.Div("Parts Manufacturing",
                         style={"color": C["muted"], "fontSize": "0.7rem", "marginBottom": "4px"}),
                html.Div(
                    part_chips if part_chips else
                    html.Span("— no active orders —",
                              style={"color": C["muted"], "fontSize": "0.72rem"}),
                ),
            ]),
        ], style={**CARD_STYLE, "borderLeft": f"3px solid {sc['color']}", "height": "100%"}),
        md=4, className="mb-3",
    )
