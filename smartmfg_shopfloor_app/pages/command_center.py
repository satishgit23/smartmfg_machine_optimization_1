"""
pages/command_center.py — Command Centre layout and data callbacks.

KPI cards, utilization bar chart, maintenance donut, OTD trend,
farm-out cost by vendor, and work-centre capacity gauge.

Machine filter: clicking a bar in the Utilization chart scopes all KPI
cards to that machine. The Clear Filter button (or clicking the same bar
again) resets back to fleet-wide view.

A single combined callback handles both machine selection logic and all
rendering, so there are no race conditions between separate callbacks.
The dcc.Store persists the selection across interval refreshes.
"""

import pandas as pd
from dash import html, dcc, Input, Output, State, callback, ctx, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from server import backend
from components.ui import C, CARD_STYLE, kpi_card, section_header, page_title


# ── Layout ─────────────────────────────────────────────────────────────────

def layout():
    return html.Div([
        page_title("Command Centre", "Live operational overview — auto-refreshes every 5 minutes"),

        # Persists selected machine across interval refreshes
        dcc.Store(id="cc-machine-sel", data=None),

        # Filter banner row
        html.Div([
            html.Div(id="cc-filter-badge"),
            html.Button(
                [html.I(className="bi bi-x-circle me-1"), "Clear Filter"],
                id="cc-clear-filter-btn",
                className="btn btn-sm btn-outline-secondary ms-2",
                style={"display": "none"},
                n_clicks=0,
            ),
        ], className="d-flex align-items-center mb-2"),

        # Fleet-wide KPI row — always shown
        html.Div(id="cc-kpi-row", className="row g-3 mb-2"),

        # Machine-specific KPI row — shown only when a bar is selected
        html.Div(id="cc-machine-kpi-row"),

        dbc.Row([
            dbc.Col([
                html.Div([
                    section_header("Machine Utilization by Asset", "bi-cpu"),
                    html.Small(
                        "Click a bar to filter KPIs by machine · click again or Clear to reset",
                        className="d-block text-muted mb-2",
                        style={"fontSize": "0.75rem"},
                    ),
                    dcc.Graph(id="cc-util-chart", config={"displayModeBar": False},
                              style={"height": "320px"}),
                ], style=CARD_STYLE),
            ], md=8),
            dbc.Col([
                html.Div([
                    section_header("Maintenance Urgency", "bi-wrench-adjustable"),
                    dcc.Graph(id="cc-maint-donut", config={"displayModeBar": False},
                              style={"height": "320px"}),
                ], style=CARD_STYLE),
            ], md=4),
        ], className="mb-3"),

        dbc.Row([
            dbc.Col([
                html.Div([
                    section_header("On-Time Delivery Trend", "bi-calendar-check"),
                    dcc.Graph(id="cc-otd-chart", config={"displayModeBar": False},
                              style={"height": "280px"}),
                ], style=CARD_STYLE),
            ], md=6),
            dbc.Col([
                html.Div([
                    section_header("Farm-Out Cost vs In-House (by Vendor)", "bi-truck"),
                    dcc.Graph(id="cc-farmout-chart", config={"displayModeBar": False},
                              style={"height": "280px"}),
                ], style=CARD_STYLE),
            ], md=6),
        ], className="mb-3"),

        dbc.Row([
            dbc.Col([
                html.Div([
                    section_header("Work-Centre Capacity Load", "bi-buildings"),
                    dcc.Graph(id="cc-capacity-chart", config={"displayModeBar": False},
                              style={"height": "250px"}),
                ], style=CARD_STYLE),
            ], md=12),
        ]),
    ])


# ── Helpers ─────────────────────────────────────────────────────────────────

def _style_fig(fig):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color=C["text"],
        font_family="Inter, -apple-system, BlinkMacSystemFont, sans-serif",
        margin=dict(l=40, r=20, t=30, b=40),
        xaxis=dict(gridcolor="#e2e8f0", linecolor="#e2e8f0",
                   tickfont=dict(color=C["muted"], size=11)),
        yaxis=dict(gridcolor="#e2e8f0", linecolor="#e2e8f0",
                   tickfont=dict(color=C["muted"], size=11)),
    )


def _urgency_color(urgency: str) -> str:
    return {"Critical": C["red"], "Warning": C["amber"], "Normal": C["green"]}.get(
        urgency, C["muted"]
    )


def _status_color(status: str) -> str:
    return {"Active": C["green"], "Maintenance": C["red"], "Idle": C["amber"]}.get(
        status, C["muted"]
    )


def _metric_tile(label, value, subtitle, icon, accent):
    """Compact self-contained metric tile — uses flex layout, no Bootstrap cols."""
    return html.Div([
        html.Div([
            html.I(className=f"{icon} me-1",
                   style={"color": accent, "fontSize": "0.85rem"}),
            html.Span(label,
                      style={"color": "#64748b", "fontSize": "0.7rem",
                             "fontWeight": "600", "textTransform": "uppercase",
                             "letterSpacing": "0.04em"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
        html.Div(str(value),
                 style={"fontSize": "1.5rem", "fontWeight": "700",
                        "color": "#1e293b", "lineHeight": "1.1"}),
        html.Div(subtitle,
                 style={"color": "#94a3b8", "fontSize": "0.7rem", "marginTop": "3px"}),
    ], style={
        "backgroundColor": "#ffffff",
        "border":          "1px solid #dbeafe",
        "borderTop":       f"3px solid {accent}",
        "borderRadius":    "8px",
        "padding":         "0.8rem 1rem",
        "flex":            "1",
        "minWidth":        "160px",
    })


# ── Single combined callback ────────────────────────────────────────────────
#
# Handles both machine selection logic and all chart/KPI rendering in one
# callback to avoid race conditions between multiple callbacks. The Store
# is both a State (read before render) and an Output (written after render)
# so the selection persists across automatic interval refreshes.

@callback(
    Output("cc-kpi-row",           "children"),
    Output("cc-machine-kpi-row",   "children"),
    Output("cc-filter-badge",      "children"),
    Output("cc-clear-filter-btn",  "style"),
    Output("cc-machine-sel",       "data"),
    Output("cc-util-chart",        "figure"),
    Output("cc-maint-donut",       "figure"),
    Output("cc-otd-chart",         "figure"),
    Output("cc-farmout-chart",     "figure"),
    Output("cc-capacity-chart",    "figure"),
    Input("refresh-interval",      "n_intervals"),
    Input("cc-util-chart",         "clickData"),
    Input("cc-clear-filter-btn",   "n_clicks"),
    State("cc-machine-sel",        "data"),
)
def refresh_command_centre(_interval, click_data, _clear_n, machine_sel_state):
    triggered = ctx.triggered_id or "refresh-interval"

    # ── Determine selection state ─────────────────────────────────────────
    if triggered == "cc-clear-filter-btn":
        machine_sel = None
    elif triggered == "cc-util-chart" and click_data:
        pt         = click_data["points"][0]
        machine_id = pt.get("customdata")
        machine_nm = pt.get("x", "")
        if machine_sel_state and machine_sel_state.get("machine_id") == machine_id:
            machine_sel = None
        else:
            machine_sel = {"machine_name": machine_nm, "machine_id": machine_id}
    else:
        machine_sel = machine_sel_state

    machine_id   = machine_sel.get("machine_id")   if machine_sel else None
    machine_name = machine_sel.get("machine_name") if machine_sel else None

    # ── Fleet-wide KPI row (always shown) ────────────────────────────────
    mc   = backend.get_machine_status_counts()
    kpis = backend.get_performance_kpis()

    total  = int(mc.get("total", 0))
    active = int(mc.get("active", 0))
    maint  = int(mc.get("maintenance", 0))
    idle   = int(mc.get("idle", 0))
    util   = kpis.get("avg_utilization_pct", "—")
    otd    = kpis.get("avg_otd_pct", "—")
    cost   = kpis.get("ytd_farmout_cost", 0) or 0
    prem   = kpis.get("avg_cost_premium_pct", "—")
    crit   = int(kpis.get("critical_machines", 0))
    warn   = int(kpis.get("warning_machines", 0))
    orders = int(kpis.get("orders_in_progress", 0))

    kpi_row = [
        kpi_card("Active Machines",    f"{active}/{total}",
                 f"{maint} down · {idle} idle",  "bi-cpu-fill",       C["green"]),
        kpi_card("Avg Utilization",    f"{util}%",
                 "latest period · all machines", "bi-speedometer2",    C["blue"]),
        kpi_card("On-Time Delivery",   f"{otd}%",
                 "latest period · all machines", "bi-calendar2-check", C["cyan"]),
        kpi_card("Farm-Out Cost YTD",  f"${cost:,.0f}",
                 f"+{prem}% vs in-house",         "bi-truck",           C["amber"]),
        kpi_card("Maint. Alerts",      f"{crit} Critical",
                 f"{warn} warnings",              "bi-bell-fill",       C["red"]),
        kpi_card("Orders In Progress", f"{orders}",
                 "open + in progress · fleet",   "bi-list-task",       C["purple"]),
    ]

    # ── Machine-specific KPI row (shown only when a bar is selected) ──────
    if machine_id:
        try:
            mk = backend.get_machine_kpis(machine_id)
        except Exception:
            mk = {}

        m_util  = mk.get("avg_utilization_pct", "—")
        m_otd   = mk.get("avg_otd_pct")
        m_otd_s = f"{m_otd}%" if m_otd is not None else "—"
        m_fo    = mk.get("wc_farmout_cost", 0) or 0
        m_ord   = int(mk.get("orders_in_progress", 0))
        m_wc    = mk.get("work_center", "—")
        disp_nm = mk.get("machine_name") or machine_name or machine_id

        machine_kpi_row = html.Div([
            # Header label
            html.Div([
                html.I(className="bi bi-cpu-fill me-2",
                       style={"color": "#2563eb", "fontSize": "0.85rem"}),
                html.Span(str(disp_nm),
                          style={"fontWeight": "700", "color": "#2563eb",
                                 "fontSize": "0.85rem"}),
                html.Span(" — machine-level KPIs",
                          style={"color": "#64748b", "fontSize": "0.78rem",
                                 "marginLeft": "4px"}),
            ], style={"marginBottom": "10px", "display": "flex",
                      "alignItems": "center"}),

            # Four metric tiles in a flex row
            html.Div([
                _metric_tile("Avg Utilization",    f"{m_util}%",
                             f"{disp_nm} · all time",
                             "bi-speedometer2",    "#3b82f6"),
                _metric_tile("On-Time Delivery",   m_otd_s,
                             "completed work orders",
                             "bi-calendar2-check", "#06b6d4"),
                _metric_tile("Farm-Out Cost YTD",  f"${m_fo:,.0f}",
                             f"{m_wc} work center",
                             "bi-truck",           "#f59e0b"),
                _metric_tile("Orders In Progress", str(m_ord),
                             "open + in progress",
                             "bi-list-task",       "#8b5cf6"),
            ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),
        ], style={
            "backgroundColor": "#eff6ff",
            "border":          "1px solid #bfdbfe",
            "borderRadius":    "10px",
            "padding":         "1rem 1.1rem",
            "marginBottom":    "0.75rem",
        })

        filter_badge = [
            html.I(className="bi bi-funnel-fill me-2",
                   style={"color": "#2563eb"}),
            html.Span("Viewing: ", style={"fontWeight": "600", "color": "#1e293b"}),
            html.Span(str(disp_nm), style={"fontWeight": "700", "color": "#2563eb"}),
            html.Span(
                " · click same bar again or Clear to reset",
                style={"color": "#64748b", "fontSize": "0.78rem", "marginLeft": "6px"},
            ),
        ]
        clear_btn_style = {"display": "inline-flex", "alignItems": "center"}

    else:
        machine_kpi_row = html.Span("")  # empty but valid — hides the panel
        filter_badge    = html.Span(
            [html.I(className="bi bi-bar-chart-fill me-2",
                    style={"color": "#94a3b8"}),
             "Click a bar in the utilization chart to see machine-level KPIs"],
            style={"color": "#94a3b8", "fontSize": "0.78rem"},
        )
        clear_btn_style = {"display": "none"}

    # ── Utilization bar — highlight selected machine ───────────────────────
    util_df  = backend.get_utilization_by_machine()
    fig_util = go.Figure()
    if not util_df.empty:
        wcs = util_df["work_center"].unique().tolist()
        for wc_name in wcs:
            sub      = util_df[util_df["work_center"] == wc_name]
            base_clr = C["chart"][wcs.index(wc_name) % len(C["chart"])]

            if machine_id is not None:
                colors    = [base_clr if mid == machine_id else "#cbd5e1"
                             for mid in sub["machine_id"]]
                opacities = [1.0 if mid == machine_id else 0.3
                             for mid in sub["machine_id"]]
            else:
                colors    = [base_clr] * len(sub)
                opacities = [1.0]      * len(sub)

            fig_util.add_trace(go.Bar(
                name=wc_name,
                x=sub["machine_name"],
                y=sub["avg_utilization_pct"],
                customdata=list(sub["machine_id"]),
                marker=dict(color=colors, opacity=opacities),
                hovertemplate="<b>%{x}</b><br>Utilization: %{y:.1f}%<extra></extra>",
            ))

        fig_util.add_hline(y=80, line_dash="dot", line_color=C["amber"],
                           annotation_text="Target 80%",
                           annotation_font_color=C["amber"])
    _style_fig(fig_util)
    fig_util.update_layout(
        barmode="group",
        showlegend=True,
        legend=dict(orientation="h", y=-0.25, font_color=C["muted"]),
        clickmode="event+select",
    )
    fig_util.update_yaxes(range=[0, 110], ticksuffix="%")

    # ── Maintenance donut ──────────────────────────────────────────────────
    maint_df = backend.get_maintenance_summary()
    urg = (
        maint_df.groupby("maintenance_urgency").size().reset_index(name="count")
        if not maint_df.empty
        else pd.DataFrame({"maintenance_urgency": [], "count": []})
    )
    clr_map   = {"Critical": C["red"], "Warning": C["amber"], "Normal": C["green"]}
    fig_donut = go.Figure(go.Pie(
        labels=urg["maintenance_urgency"],
        values=urg["count"],
        hole=0.6,
        marker_colors=[clr_map.get(u, C["blue"]) for u in urg["maintenance_urgency"]],
        textfont_color=C["text"],
        hovertemplate="<b>%{label}</b><br>Count: %{value}<extra></extra>",
    ))
    _style_fig(fig_donut)
    fig_donut.update_layout(
        showlegend=True,
        legend=dict(font_color=C["muted"], orientation="h", y=-0.1),
        annotations=[dict(
            text=f"{len(maint_df)}<br><span style='font-size:10px'>machines</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=18, color=C["text"]),
        )],
    )

    # ── OTD trend ─────────────────────────────────────────────────────────
    trend_df = backend.get_scheduling_trend()
    fig_otd  = go.Figure()
    if not trend_df.empty:
        fig_otd.add_trace(go.Scatter(
            x=trend_df["month"], y=trend_df["avg_otd_pct"],
            mode="lines+markers", name="OTD %",
            line=dict(color=C["green"], width=2),
            fill="tozeroy", fillcolor="rgba(63,185,80,0.12)",
            hovertemplate="<b>%{x}</b><br>OTD: %{y:.1f}%<extra></extra>",
        ))
        fig_otd.add_trace(go.Bar(
            x=trend_df["month"], y=trend_df["late_orders"],
            name="Late Orders", yaxis="y2",
            marker_color=C["red"], opacity=0.5,
            hovertemplate="<b>%{x}</b><br>Late: %{y}<extra></extra>",
        ))
        fig_otd.add_hline(y=95, line_dash="dot", line_color=C["amber"],
                          annotation_text="Target 95%",
                          annotation_font_color=C["amber"])
    _style_fig(fig_otd)
    fig_otd.update_layout(
        yaxis=dict(title="OTD %",    ticksuffix="%", range=[0, 110]),
        yaxis2=dict(title="Late",    overlaying="y", side="right",
                    showgrid=False,  tickfont_color=C["muted"]),
        legend=dict(font_color=C["muted"], orientation="h", y=-0.3),
    )

    # ── Farm-out grouped bar ───────────────────────────────────────────────
    fo_df  = backend.get_farmout_by_vendor()
    fig_fo = go.Figure()
    if not fo_df.empty:
        fig_fo.add_trace(go.Bar(
            name="Farm-Out Cost", x=fo_df["farm_out_vendor"],
            y=fo_df["total_farmout_cost"],
            marker_color=C["red"],
            hovertemplate="<b>%{x}</b><br>Farm-Out: $%{y:,.0f}<extra></extra>",
        ))
        fig_fo.add_trace(go.Bar(
            name="Implied In-House", x=fo_df["farm_out_vendor"],
            y=fo_df["implied_inhouse_cost"],
            marker_color=C["green"],
            hovertemplate="<b>%{x}</b><br>In-House: $%{y:,.0f}<extra></extra>",
        ))
    _style_fig(fig_fo)
    fig_fo.update_layout(barmode="group",
                         legend=dict(font_color=C["muted"], orientation="h", y=-0.3),
                         yaxis=dict(tickprefix="$"))

    # ── Capacity bar ───────────────────────────────────────────────────────
    cap_df  = backend.get_capacity_by_workcenter()
    load_clr = {
        "Overloaded":    C["red"],
        "High Load":     C["amber"],
        "Normal":        C["green"],
        "Underutilized": C["muted"],
    }
    fig_cap = go.Figure()
    if not cap_df.empty:
        fig_cap.add_trace(go.Bar(
            x=cap_df["work_center"],
            y=cap_df["avg_capacity_pct"],
            marker_color=[load_clr.get(s, C["blue"]) for s in cap_df["load_status"]],
            text=cap_df["load_status"], textposition="outside",
            textfont=dict(color=C["muted"], size=10),
            hovertemplate="<b>%{x}</b><br>Capacity: %{y:.1f}%<extra></extra>",
        ))
        fig_cap.add_hline(y=90, line_dash="dot", line_color=C["red"],
                          annotation_text="Overload 90%",
                          annotation_font_color=C["red"])
    _style_fig(fig_cap)
    fig_cap.update_yaxes(range=[0, 115], ticksuffix="%")

    return (
        kpi_row, machine_kpi_row, filter_badge, clear_btn_style, machine_sel,
        fig_util, fig_donut, fig_otd, fig_fo, fig_cap,
    )
