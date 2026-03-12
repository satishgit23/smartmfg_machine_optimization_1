"""
pages/command_center.py — Command Centre layout and data callbacks.

KPI cards, utilization bar chart, maintenance donut, OTD trend,
farm-out cost by vendor, and work-centre capacity gauge.
"""

import pandas as pd
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from server import backend
from components.ui import C, CARD_STYLE, kpi_card, section_header, page_title


# ── Layout ─────────────────────────────────────────────────────────────────

def layout():
    return html.Div([
        page_title("Command Centre", "Live operational overview — auto-refreshes every 5 minutes"),

        # KPI row — populated by callback
        html.Div(id="cc-kpi-row", className="row g-3 mb-2"),

        dbc.Row([
            dbc.Col([
                html.Div([
                    section_header("Machine Utilization by Asset", "bi-cpu"),
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


# ── Helper ─────────────────────────────────────────────────────────────────

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


# ── Callbacks ──────────────────────────────────────────────────────────────

@callback(
    Output("cc-kpi-row",        "children"),
    Output("cc-util-chart",     "figure"),
    Output("cc-maint-donut",    "figure"),
    Output("cc-otd-chart",      "figure"),
    Output("cc-farmout-chart",  "figure"),
    Output("cc-capacity-chart", "figure"),
    Input("refresh-interval",   "n_intervals"),
)
def refresh_command_centre(_):
    # ── KPIs ──────────────────────────────────────────────────────────────
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
                 "latest period",               "bi-speedometer2",    C["blue"]),
        kpi_card("On-Time Delivery",   f"{otd}%",
                 "latest period",               "bi-calendar2-check", C["cyan"]),
        kpi_card("Farm-Out Cost YTD",  f"${cost:,.0f}",
                 f"+{prem}% vs in-house",        "bi-truck",           C["amber"]),
        kpi_card("Maint. Alerts",      f"{crit} Critical",
                 f"{warn} warnings",             "bi-bell-fill",       C["red"]),
        kpi_card("Orders In Progress", f"{orders}",
                 "open + in progress",           "bi-list-task",       C["purple"]),
    ]

    # ── Utilization bar ────────────────────────────────────────────────────
    util_df = backend.get_utilization_by_machine()
    fig_util = go.Figure()
    if not util_df.empty:
        wcs = util_df["work_center"].unique().tolist()
        for wc in wcs:
            sub = util_df[util_df["work_center"] == wc]
            fig_util.add_trace(go.Bar(
                name=wc, x=sub["machine_name"], y=sub["avg_utilization_pct"],
                marker_color=C["chart"][wcs.index(wc) % len(C["chart"])],
                hovertemplate="<b>%{x}</b><br>Utilization: %{y:.1f}%<extra></extra>",
            ))
        fig_util.add_hline(y=80, line_dash="dot", line_color=C["amber"],
                           annotation_text="Target 80%",
                           annotation_font_color=C["amber"])
    _style_fig(fig_util)
    fig_util.update_layout(barmode="group", showlegend=True,
                           legend=dict(orientation="h", y=-0.25, font_color=C["muted"]))
    fig_util.update_yaxes(range=[0, 110], ticksuffix="%")

    # ── Maintenance donut ──────────────────────────────────────────────────
    maint_df = backend.get_maintenance_summary()
    urg = (
        maint_df.groupby("maintenance_urgency").size().reset_index(name="count")
        if not maint_df.empty
        else pd.DataFrame({"maintenance_urgency": [], "count": []})
    )
    clr_map = {"Critical": C["red"], "Warning": C["amber"], "Normal": C["green"]}
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

    return kpi_row, fig_util, fig_donut, fig_otd, fig_fo, fig_cap
