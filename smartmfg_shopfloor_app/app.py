"""
app.py — SmartMFG Shop Floor Command Center
Entry point for the Databricks App.

Layout: top navbar → tabbed navigation → page content area.
All three pages are always present in the DOM; only one is visible at a time
so that every Dash callback ID is always reachable.

Import order matters:
  1. server.py  creates `app` and `backend`
  2. pages.*    register their callbacks against the global `app`
  3. app.layout is set
  4. app.run()  starts the server
"""

from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc

# 1 ── Initialise app + backend ─────────────────────────────────────────────
from server import app, server  # noqa: F401  (server re-exported for gunicorn)
from components.ui import C

# 2 ── Register page callbacks by importing page modules ────────────────────
import pages.command_center    # noqa: F401
import pages.machine_fleet     # noqa: F401
import pages.machine_inspector # noqa: F401


# ── Navigation ─────────────────────────────────────────────────────────────

_navbar = dbc.Navbar(
    dbc.Container([
        html.Div([
            html.Div(
                html.I(className="bi bi-gear-wide-connected fs-5",
                       style={"color": "#93c5fd"}),
                style={"width": "36px", "height": "36px", "borderRadius": "8px",
                       "backgroundColor": "rgba(255,255,255,0.12)",
                       "display": "flex", "alignItems": "center", "justifyContent": "center",
                       "marginRight": "10px"},
            ),
            dbc.NavbarBrand("SmartMFG", className="fw-bold mb-0 me-1",
                            style={"color": "#f8fafc"}),
            html.Span("Shop Floor Command Center",
                      className="d-none d-md-inline small",
                      style={"color": "#94a3b8"}),
        ], className="d-flex align-items-center"),
        html.Div([
            dbc.Badge("● Live", id="nav-refresh-badge", color="success",
                      className="me-3 px-2 py-1", style={"fontSize": "0.72rem"}),
            html.Span(id="nav-clock", className="small me-3 fw-medium",
                      style={"color": "#94a3b8", "fontVariantNumeric": "tabular-nums"}),
        ], className="d-flex align-items-center"),
    ], fluid=True),
    color="dark", dark=True,
    style={"backgroundColor": C["nav"],
           "borderBottom": "1px solid rgba(255,255,255,0.08)"},
)

_tabs = dbc.Tabs(
    id="page-tabs",
    active_tab="command",
    children=[
        dbc.Tab(tab_id="command",
                label_style={"color": "#000000", "fontWeight": "600"},
                active_label_style={"color": "#2563eb", "fontWeight": "700"},
                children=[html.I(className="bi bi-speedometer2 me-2"), "Command Centre"]),
        dbc.Tab(tab_id="fleet",
                label_style={"color": "#000000", "fontWeight": "600"},
                active_label_style={"color": "#2563eb", "fontWeight": "700"},
                children=[html.I(className="bi bi-grid-3x3-gap me-2"), "Machine Fleet"]),
        dbc.Tab(tab_id="inspector",
                label_style={"color": "#000000", "fontWeight": "600"},
                active_label_style={"color": "#2563eb", "fontWeight": "700"},
                children=[html.I(className="bi bi-display me-2"), "Machine Inspector"]),
    ],
    style={"backgroundColor": "#ffffff",
           "borderBottom": "2px solid #e2e8f0",
           "paddingLeft": "1.5rem",
           "boxShadow": "0 2px 4px rgba(0,0,0,0.06)"},
)

# 3 ── Layout ────────────────────────────────────────────────────────────────

app.layout = html.Div([
    _navbar,
    _tabs,

    # All three pages always in DOM — one visible at a time
    html.Div([
        html.Div(id="page-command",
                 children=pages.command_center.layout()),
        html.Div(id="page-fleet",
                 children=pages.machine_fleet.layout(),
                 style={"display": "none"}),
        html.Div(id="page-inspector",
                 children=pages.machine_inspector.layout(),
                 style={"display": "none"}),
    ], style={
        "backgroundColor": C["bg"],
        "minHeight":       "calc(100vh - 100px)",
        "padding":         "1.5rem 2rem",
    }),

    # Global timers
    dcc.Interval(id="refresh-interval", interval=5 * 60 * 1000, n_intervals=0),
    dcc.Interval(id="clock-interval",   interval=1_000,          n_intervals=0),
], style={"backgroundColor": C["bg"], "minHeight": "100vh"})


# ── Global callbacks ────────────────────────────────────────────────────────

@callback(
    Output("page-command",   "style"),
    Output("page-fleet",     "style"),
    Output("page-inspector", "style"),
    Input("page-tabs",       "active_tab"),
)
def toggle_pages(tab):
    show = {"display": "block"}
    hide = {"display": "none"}
    return (
        show if tab == "command"   else hide,
        show if tab == "fleet"     else hide,
        show if tab == "inspector" else hide,
    )


@callback(Output("nav-clock", "children"), Input("clock-interval", "n_intervals"))
def update_clock(_):
    from datetime import datetime
    return datetime.now().strftime("%H:%M:%S")


# 4 ── Run ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
