"""
components/ui.py — Shared design tokens, style dictionaries, and reusable
Dash component helpers used across all pages.
"""

from dash import html
import dash_bootstrap_components as dbc

# ── Design tokens ──────────────────────────────────────────────────────────
C = {
    "bg":     "#0d1117",
    "card":   "#161b22",
    "border": "#21262d",
    "text":   "#e6edf3",
    "muted":  "#7d8590",
    "green":  "#3fb950",
    "amber":  "#d29922",
    "red":    "#f85149",
    "blue":   "#58a6ff",
    "purple": "#bc8cff",
    "cyan":   "#39c5cf",
    "chart":  ["#58a6ff", "#3fb950", "#d29922", "#f85149", "#bc8cff", "#39c5cf", "#ff7b72"],
}

STATUS_CFG = {
    "Active":      {"color": C["green"],  "badge": "success", "icon": "bi-check-circle-fill",        "label": "RUNNING"},
    "Idle":        {"color": C["amber"],  "badge": "warning", "icon": "bi-pause-circle-fill",         "label": "IDLE"},
    "Maintenance": {"color": C["red"],    "badge": "danger",  "icon": "bi-exclamation-triangle-fill", "label": "DOWN"},
}

# ── Style dictionaries ─────────────────────────────────────────────────────
CARD_STYLE = {
    "backgroundColor": C["card"],
    "border":          f"1px solid {C['border']}",
    "borderRadius":    "8px",
    "padding":         "1rem",
}

DT_CELL_STYLE = {
    "backgroundColor": C["card"],
    "color":           C["text"],
    "border":          f"1px solid {C['border']}",
    "fontSize":        "0.82rem",
}

DT_HEADER_STYLE = {
    "backgroundColor": C["border"],
    "color":           C["muted"],
    "fontWeight":      "600",
    "fontSize":        "0.72rem",
    "textTransform":   "uppercase",
    "border":          f"1px solid {C['border']}",
}

# ── KPI card ───────────────────────────────────────────────────────────────

def kpi_card(title, value, subtitle="", icon="bi-bar-chart", color=C["blue"], width=2):
    """Metric tile used in the Command Centre KPI row."""
    return dbc.Col(
        html.Div([
            html.Div([
                html.I(className=f"{icon} fs-4", style={"color": color}),
                html.Span(title, className="ms-2 small fw-semibold",
                          style={"color": C["muted"]}),
            ], className="d-flex align-items-center mb-2"),
            html.Div(str(value), className="fs-3 fw-bold", style={"color": C["text"]}),
            html.Div(subtitle, className="small mt-1", style={"color": C["muted"]}),
        ], style=CARD_STYLE),
        width=width, className="mb-3",
    )


# ── Section header ─────────────────────────────────────────────────────────

def section_header(title, icon=""):
    """Small bold heading used at the top of a card section."""
    return html.Div([
        html.I(className=f"{icon} me-2", style={"color": C["blue"]}),
        html.Span(title, className="fw-semibold"),
    ], className="fs-6 mb-3", style={"color": C["text"]})


# ── Status badge ───────────────────────────────────────────────────────────

def status_badge(status: str):
    """Bootstrap badge coloured by machine status."""
    cfg = STATUS_CFG.get(status, {"badge": "secondary", "icon": "bi-question-circle", "label": status})
    return dbc.Badge(
        [html.I(className=f"{cfg['icon']} me-1"), cfg["label"]],
        color=cfg["badge"],
        className="me-1 px-2 py-1",
    )


# ── Page-title block ───────────────────────────────────────────────────────

def page_title(title: str, subtitle: str):
    """Standard two-line heading used at the top of each page."""
    return html.Div([
        html.H5(title, className="fw-bold mb-0", style={"color": C["text"]}),
        html.Span(subtitle, style={"color": C["muted"], "fontSize": "0.85rem"}),
    ], className="mb-4")
