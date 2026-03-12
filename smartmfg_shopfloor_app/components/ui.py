"""
components/ui.py — Shared design tokens, style dictionaries, and reusable
Dash component helpers used across all pages.  Light theme.
"""

from dash import html
import dash_bootstrap_components as dbc

# ── Design tokens ──────────────────────────────────────────────────────────
C = {
    "bg":       "#f1f5f9",   # slate-100  — page background
    "card":     "#ffffff",   # white      — card surface
    "border":   "#e2e8f0",   # slate-200  — dividers / borders
    "input":    "#f8fafc",   # slate-50   — input / table row background
    "text":     "#0f172a",   # slate-900  — primary text
    "muted":    "#64748b",   # slate-500  — secondary / label text
    "green":    "#16a34a",   # green-600
    "amber":    "#d97706",   # amber-600
    "red":      "#dc2626",   # red-600
    "blue":     "#2563eb",   # blue-600
    "purple":   "#7c3aed",   # violet-600
    "cyan":     "#0891b2",   # cyan-600
    "nav":      "#1e293b",   # slate-800  — top navbar
    "chart":    ["#2563eb", "#16a34a", "#d97706", "#dc2626",
                 "#7c3aed", "#0891b2", "#f97316"],
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
    "borderRadius":    "10px",
    "padding":         "1.1rem",
    "boxShadow":       "0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)",
}

DT_CELL_STYLE = {
    "backgroundColor": C["card"],
    "color":           C["text"],
    "border":          f"1px solid {C['border']}",
    "fontSize":        "0.82rem",
}

DT_HEADER_STYLE = {
    "backgroundColor": C["input"],
    "color":           C["muted"],
    "fontWeight":      "600",
    "fontSize":        "0.72rem",
    "textTransform":   "uppercase",
    "letterSpacing":   "0.04em",
    "border":          f"1px solid {C['border']}",
}

# ── KPI card ───────────────────────────────────────────────────────────────

def kpi_card(title, value, subtitle="", icon="bi-bar-chart", color=C["blue"], width=2):
    """Metric tile used in the Command Centre KPI row."""
    return dbc.Col(
        html.Div([
            html.Div([
                html.Div(
                    html.I(className=f"{icon} fs-5", style={"color": color}),
                    style={
                        "width": "38px", "height": "38px",
                        "borderRadius": "8px",
                        "backgroundColor": f"{color}18",
                        "display": "flex", "alignItems": "center", "justifyContent": "center",
                    },
                ),
                html.Span(title, className="ms-2 small fw-semibold",
                          style={"color": C["muted"]}),
            ], className="d-flex align-items-center mb-2"),
            html.Div(str(value), className="fs-3 fw-bold", style={"color": C["text"]}),
            html.Div(subtitle, className="small mt-1", style={"color": C["muted"]}),
        ], style={**CARD_STYLE, "borderTop": f"3px solid {color}"}),
        width=width, className="mb-3",
    )


# ── Section header ─────────────────────────────────────────────────────────

def section_header(title, icon=""):
    return html.Div([
        html.I(className=f"{icon} me-2", style={"color": C["blue"]}),
        html.Span(title, className="fw-semibold"),
    ], className="fs-6 mb-3", style={"color": C["text"]})


# ── Status badge ───────────────────────────────────────────────────────────

def status_badge(status: str):
    cfg = STATUS_CFG.get(status, {"badge": "secondary", "icon": "bi-question-circle", "label": status})
    return dbc.Badge(
        [html.I(className=f"{cfg['icon']} me-1"), cfg["label"]],
        color=cfg["badge"],
        className="me-1 px-2 py-1",
    )


# ── Page-title block ───────────────────────────────────────────────────────

def page_title(title: str, subtitle: str):
    return html.Div([
        html.H5(title, className="fw-bold mb-0", style={"color": C["text"]}),
        html.Span(subtitle, style={"color": C["muted"], "fontSize": "0.85rem"}),
    ], className="mb-4")
