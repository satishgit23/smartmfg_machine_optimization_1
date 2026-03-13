"""
pages/dashboard_tab.py -- SmartMFG Machine Optimization Dashboard

Recreates the 5-page Lakeview dashboard using the exact same SQL datasets:
  1. Machine Utilization
  2. Scheduling Performance
  3. Predictive Maintenance
  4. Capacity Planning
  5. Farm-Out Analysis

Data is loaded fresh each time a sub-tab is selected.
"""

from dash import html, dcc, Input, Output, callback
from dash import dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from server import backend
from components.ui import page_title

FQN = "satsen_catalog.smartmfg_machine_optimization_1"

# ---- Color palette ----------------------------------------------------------

_BLUE   = "#2563eb"
_AMBER  = "#d97706"
_RED    = "#ef4444"
_GREEN  = "#059669"
_PURPLE = "#7c3aed"
_SLATE  = "#64748b"

_BLUES  = ["#1e40af", "#2563eb", "#3b82f6", "#60a5fa", "#93c5fd", "#bfdbfe"]
_AMBERS = ["#92400e", "#b45309", "#d97706", "#f59e0b", "#fbbf24", "#fcd34d"]
_MIXED  = [_BLUE, _AMBER, _RED, _GREEN, _PURPLE, "#0891b2", "#db2777"]


# ---- Shared helpers ---------------------------------------------------------

def _kpi(label, value, icon_class, color):
    return html.Div([
        html.Div([
            html.I(className=f"{icon_class} me-2", style={"color": color, "fontSize": "1.1rem"}),
            html.Span(label, style={"color": _SLATE, "fontSize": "0.72rem",
                                    "fontWeight": "600", "textTransform": "uppercase",
                                    "letterSpacing": "0.05em"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"}),
        html.Div(str(value), style={"fontSize": "1.7rem", "fontWeight": "800",
                                     "color": "#1e293b", "lineHeight": "1"}),
    ], style={
        "backgroundColor": "#ffffff",
        "border":          "1px solid #e2e8f0",
        "borderTop":       f"3px solid {color}",
        "borderRadius":    "8px",
        "padding":         "1rem 1.2rem",
        "flex":            "1",
        "minWidth":        "160px",
    })


def _chart_card(title, figure, height=320):
    figure.update_layout(
        paper_bgcolor="#ffffff",
        plot_bgcolor="#f8fafc",
        font=dict(family="inherit", size=11, color="#1e293b"),
        margin=dict(l=50, r=20, t=36, b=50),
        height=height,
        xaxis=dict(gridcolor="#f1f5f9", zeroline=False),
        yaxis=dict(gridcolor="#f1f5f9", zeroline=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return html.Div([
        html.Div(title, style={"fontWeight": "700", "color": "#1e293b",
                                "fontSize": "0.88rem", "marginBottom": "8px"}),
        dcc.Graph(figure=figure, config={"displayModeBar": False}),
    ], style={
        "backgroundColor": "#ffffff",
        "border":          "1px solid #e2e8f0",
        "borderRadius":    "8px",
        "padding":         "1rem",
    })


def _data_table(rows, columns, color="#2563eb"):
    if not rows:
        return html.Div("No data available.",
                        style={"color": _SLATE, "padding": "1rem"})
    col_defs = [{"name": c.replace("_", " ").title(), "id": c} for c in columns]
    return dash_table.DataTable(
        columns=col_defs,
        data=rows,
        page_size=10,
        sort_action="native",
        filter_action="native",
        sort_mode="multi",
        style_table={"overflowX": "auto", "borderRadius": "8px",
                     "border": f"1px solid #e2e8f0"},
        style_cell={"fontFamily": "inherit", "fontSize": "0.81rem",
                    "padding": "6px 12px", "textAlign": "left",
                    "border": "1px solid #f1f5f9", "color": "#1e293b",
                    "minWidth": "80px", "maxWidth": "220px",
                    "overflow": "hidden", "textOverflow": "ellipsis"},
        style_header={"backgroundColor": color + "15", "color": "#1e293b",
                      "fontWeight": "700", "fontSize": "0.72rem",
                      "textTransform": "uppercase", "letterSpacing": "0.05em",
                      "border": f"1px solid {color}30"},
        style_data_conditional=[
            {"if": {"row_index": "odd"},  "backgroundColor": "#f8fafc"},
            {"if": {"row_index": "even"}, "backgroundColor": "#ffffff"},
        ],
        style_filter={"backgroundColor": "#f8fafc", "fontSize": "0.78rem"},
    )


def _safe(df, col, default=0):
    if df.empty or col not in df.columns:
        return default
    v = df.iloc[0][col]
    return v if v is not None else default


def _fmt(v, suffix="", prefix=""):
    try:
        n = float(v)
        if n >= 1_000_000:
            return f"{prefix}{n/1_000_000:.1f}M{suffix}"
        if n >= 1_000:
            return f"{prefix}{n/1_000:.0f}K{suffix}"
        return f"{prefix}{n:,.1f}{suffix}"
    except (TypeError, ValueError):
        return str(v)


def _to_records(df):
    return df.to_dict("records") if not df.empty else []


def _loading_div():
    return html.Div([
        dbc.Spinner(color="primary", size="sm"),
        html.Span(" Loading...", style={"color": _SLATE, "marginLeft": "8px"}),
    ], style={"padding": "2rem", "display": "flex", "alignItems": "center"})


# ---- Page renderers ---------------------------------------------------------

def _render_utilization():
    kpi = backend._query(f"""
        SELECT COUNT(DISTINCT machine_id) AS total_machines,
               ROUND(AVG(utilization_pct), 1) AS avg_util,
               ROUND(SUM(revenue_usd), 0)     AS total_revenue,
               SUM(work_order_count)           AS total_wo
        FROM {FQN}.gold_machine_utilization
    """)
    detail = backend._query(f"""
        SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month,
               machine_id,
               ROUND(utilization_pct, 1) AS utilization_pct,
               ROUND(revenue_usd, 0)     AS revenue_usd,
               work_order_count
        FROM {FQN}.gold_machine_utilization
        ORDER BY period_month, machine_id
    """)

    kpi_row = html.Div([
        _kpi("Total Machines",    _safe(kpi, "total_machines", "—"),   "bi bi-hdd-stack",  _BLUE),
        _kpi("Avg Utilization",   f"{_safe(kpi, 'avg_util', '—')}%",   "bi bi-speedometer2", _AMBER),
        _kpi("Total Revenue",     _fmt(_safe(kpi, "total_revenue"), prefix="$"), "bi bi-currency-dollar", _GREEN),
        _kpi("Total Work Orders", _fmt(_safe(kpi, "total_wo")),        "bi bi-clipboard-check", _PURPLE),
    ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "16px"})

    # Revenue by machine (latest month per machine)
    rev_df = backend._query(f"""
        SELECT machine_id, ROUND(SUM(revenue_usd), 0) AS revenue
        FROM {FQN}.gold_machine_utilization
        GROUP BY machine_id ORDER BY revenue DESC
    """)
    rev_fig = go.Figure(go.Bar(
        x=rev_df["machine_id"].tolist() if not rev_df.empty else [],
        y=rev_df["revenue"].tolist()    if not rev_df.empty else [],
        marker_color=_BLUES[:len(rev_df)],
        text=[_fmt(v, prefix="$") for v in (rev_df["revenue"].tolist() if not rev_df.empty else [])],
        textposition="outside",
    ))

    # Monthly utilization trend (avg across fleet)
    trend_df = backend._query(f"""
        SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month,
               ROUND(AVG(utilization_pct), 1) AS avg_util
        FROM {FQN}.gold_machine_utilization
        GROUP BY period_month ORDER BY period_month
    """)
    trend_fig = go.Figure(go.Scatter(
        x=trend_df["month"].tolist()    if not trend_df.empty else [],
        y=trend_df["avg_util"].tolist() if not trend_df.empty else [],
        mode="lines+markers",
        line=dict(color=_BLUE, width=2),
        marker=dict(size=6),
        fill="tozeroy", fillcolor=_BLUE + "18",
    ))

    charts = dbc.Row([
        dbc.Col(_chart_card("Revenue by Machine", rev_fig),            md=6),
        dbc.Col(_chart_card("Monthly Avg Utilization Trend (%)", trend_fig), md=6),
    ], className="mb-3")

    table_section = html.Div([
        html.Div("Utilization Detail", style={"fontWeight": "700", "color": "#1e293b",
                                               "marginBottom": "8px", "fontSize": "0.88rem"}),
        _data_table(_to_records(detail), list(detail.columns) if not detail.empty else [], _BLUE),
    ], style={"backgroundColor": "#fff", "border": "1px solid #e2e8f0",
              "borderRadius": "8px", "padding": "1rem"})

    return html.Div([kpi_row, charts, table_section])


def _render_scheduling():
    kpi = backend._query(f"""
        SELECT SUM(total_orders)              AS total_wo,
               ROUND(SUM(total_farm_out_cost),0) AS total_farmout,
               ROUND(AVG(on_time_delivery_pct),1) AS avg_otd
        FROM {FQN}.gold_scheduling_performance
    """)
    detail = backend._query(f"""
        SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month,
               work_center, total_orders, late_count, farm_out_count,
               ROUND(total_farm_out_cost,0) AS farm_out_cost_usd,
               ROUND(on_time_delivery_pct,1) AS otd_pct
        FROM {FQN}.gold_scheduling_performance
        ORDER BY period_month DESC, work_center
    """)

    kpi_row = html.Div([
        _kpi("Total Work Orders", _fmt(_safe(kpi, "total_wo")),           "bi bi-clipboard-data", _BLUE),
        _kpi("Farm-Out Cost",     _fmt(_safe(kpi, "total_farmout"), prefix="$"), "bi bi-truck",  _AMBER),
        _kpi("Avg On-Time Delivery", f"{_safe(kpi, 'avg_otd', '—')}%",   "bi bi-check2-circle",  _GREEN),
    ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "16px"})

    # OTD trend
    otd_df = backend._query(f"""
        SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month,
               ROUND(AVG(on_time_delivery_pct),1) AS avg_otd
        FROM {FQN}.gold_scheduling_performance
        GROUP BY period_month ORDER BY period_month
    """)
    otd_fig = go.Figure(go.Scatter(
        x=otd_df["month"].tolist()   if not otd_df.empty else [],
        y=otd_df["avg_otd"].tolist() if not otd_df.empty else [],
        mode="lines+markers",
        line=dict(color=_GREEN, width=2),
        marker=dict(size=6),
        fill="tozeroy", fillcolor=_GREEN + "18",
    ))

    # Farm-out cost by work center
    fo_df = backend._query(f"""
        SELECT work_center, ROUND(SUM(total_farm_out_cost),0) AS farmout
        FROM {FQN}.gold_scheduling_performance
        GROUP BY work_center ORDER BY farmout DESC
    """)
    fo_fig = go.Figure(go.Bar(
        x=fo_df["work_center"].tolist() if not fo_df.empty else [],
        y=fo_df["farmout"].tolist()     if not fo_df.empty else [],
        marker_color=_AMBERS[:len(fo_df)],
        text=[_fmt(v, prefix="$") for v in (fo_df["farmout"].tolist() if not fo_df.empty else [])],
        textposition="outside",
    ))

    charts = dbc.Row([
        dbc.Col(_chart_card("On-Time Delivery Trend (%)", otd_fig),        md=6),
        dbc.Col(_chart_card("Farm-Out Cost by Work Center ($)", fo_fig),   md=6),
    ], className="mb-3")

    table_section = html.Div([
        html.Div("Scheduling Detail", style={"fontWeight": "700", "color": "#1e293b",
                                              "marginBottom": "8px", "fontSize": "0.88rem"}),
        _data_table(_to_records(detail), list(detail.columns) if not detail.empty else [], _AMBER),
    ], style={"backgroundColor": "#fff", "border": "1px solid #e2e8f0",
              "borderRadius": "8px", "padding": "1rem"})

    return html.Div([kpi_row, charts, table_section])


def _render_maintenance():
    detail = backend._query(f"""
        WITH latest AS (
            SELECT machine_id, MAX(reading_date) AS latest_date
            FROM {FQN}.gold_predictive_maintenance GROUP BY machine_id
        )
        SELECT p.machine_id, p.machine_name, p.work_center,
               ROUND(p.avg_health_score,1) AS avg_health_score,
               ROUND(p.max_tool_wear_pct,1) AS max_tool_wear_pct,
               p.anomaly_count, p.maintenance_urgency, p.days_since_last_pm
        FROM {FQN}.gold_predictive_maintenance p
        JOIN latest l ON p.machine_id=l.machine_id AND p.reading_date=l.latest_date
        ORDER BY CASE p.maintenance_urgency WHEN 'Critical' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END
    """)

    avg_health = round(detail["avg_health_score"].mean(), 1) if not detail.empty else "—"
    total_anomalies = int(detail["anomaly_count"].sum()) if not detail.empty else 0
    avg_wear = round(detail["max_tool_wear_pct"].mean(), 1) if not detail.empty else "—"

    kpi_row = html.Div([
        _kpi("Fleet Avg Health Score", f"{avg_health}",          "bi bi-heart-pulse", _GREEN),
        _kpi("Total Anomalies",        f"{total_anomalies:,}",    "bi bi-exclamation-triangle", _RED),
        _kpi("Avg Tool Wear %",        f"{avg_wear}%",            "bi bi-tools",       _AMBER),
    ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "16px"})

    # Health score by machine (horizontal bar sorted)
    hs_sorted = detail.sort_values("avg_health_score") if not detail.empty else detail
    urgency_colors = {"Critical": _RED, "Warning": _AMBER, "OK": _GREEN, "Routine": _BLUE}
    bar_colors = [urgency_colors.get(u, _SLATE)
                  for u in (hs_sorted["maintenance_urgency"].tolist() if not hs_sorted.empty else [])]
    hs_fig = go.Figure(go.Bar(
        y=hs_sorted["machine_id"].tolist()      if not hs_sorted.empty else [],
        x=hs_sorted["avg_health_score"].tolist() if not hs_sorted.empty else [],
        orientation="h",
        marker_color=bar_colors,
        text=[f"{v:.1f}" for v in (hs_sorted["avg_health_score"].tolist() if not hs_sorted.empty else [])],
        textposition="outside",
    ))
    hs_fig.update_layout(height=360, yaxis=dict(autorange="reversed"))

    # Urgency distribution pie
    if not detail.empty:
        urgency_counts = detail["maintenance_urgency"].value_counts()
        pie_labels = urgency_counts.index.tolist()
        pie_values = urgency_counts.values.tolist()
    else:
        pie_labels, pie_values = [], []

    pie_fig = go.Figure(go.Pie(
        labels=pie_labels,
        values=pie_values,
        hole=0.45,
        marker=dict(colors=[urgency_colors.get(l, _SLATE) for l in pie_labels]),
        textinfo="label+percent",
    ))
    pie_fig.update_layout(showlegend=True, height=360)

    charts = dbc.Row([
        dbc.Col(_chart_card("Current Health Score by Machine", hs_fig, 360), md=7),
        dbc.Col(_chart_card("Maintenance Urgency Distribution", pie_fig, 360), md=5),
    ], className="mb-3")

    table_section = html.Div([
        html.Div("Machine Health Snapshot", style={"fontWeight": "700", "color": "#1e293b",
                                                    "marginBottom": "8px", "fontSize": "0.88rem"}),
        _data_table(_to_records(detail), list(detail.columns) if not detail.empty else [], _RED),
    ], style={"backgroundColor": "#fff", "border": "1px solid #e2e8f0",
              "borderRadius": "8px", "padding": "1rem"})

    return html.Div([kpi_row, charts, table_section])


def _render_capacity():
    detail = backend._query(f"""
        SELECT DATE_FORMAT(period_month, 'yyyy-MM') AS month,
               work_center,
               ROUND(demand_hrs, 0)              AS demand_hrs,
               ROUND(available_hrs, 0)            AS available_hrs,
               ROUND(capacity_utilization_pct, 1) AS capacity_util_pct,
               ROUND(farm_out_cost_usd, 0)        AS farm_out_cost_usd,
               load_status
        FROM {FQN}.gold_capacity_planning
        ORDER BY period_month DESC, work_center
    """)

    avg_util   = round(detail["capacity_util_pct"].mean(), 1) if not detail.empty else "—"
    total_fo   = int(detail["farm_out_cost_usd"].sum())       if not detail.empty else 0
    total_dem  = int(detail["demand_hrs"].sum())               if not detail.empty else 0

    kpi_row = html.Div([
        _kpi("Avg Capacity Util",  f"{avg_util}%",              "bi bi-bar-chart-fill",    _PURPLE),
        _kpi("Total Farm-Out Cost", _fmt(total_fo, prefix="$"), "bi bi-truck",             _AMBER),
        _kpi("Total Demand Hours",  _fmt(total_dem),            "bi bi-clock-history",     _BLUE),
    ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "16px"})

    # Demand vs Capacity by work center (latest month)
    latest_month = detail["month"].max() if not detail.empty else None
    if latest_month:
        wc_df = detail[detail["month"] == latest_month]
    else:
        wc_df = detail
    dvc_fig = go.Figure()
    if not wc_df.empty:
        dvc_fig.add_trace(go.Bar(
            name="Demand Hrs",
            x=wc_df["work_center"].tolist(),
            y=wc_df["demand_hrs"].tolist(),
            marker_color=_PURPLE,
        ))
        dvc_fig.add_trace(go.Bar(
            name="Available Hrs",
            x=wc_df["work_center"].tolist(),
            y=wc_df["available_hrs"].tolist(),
            marker_color=_BLUE,
        ))
    dvc_fig.update_layout(barmode="group",
                          title=f"Latest month: {latest_month}" if latest_month else "")

    # Load status distribution
    if not detail.empty:
        ls_counts = detail["load_status"].value_counts()
        ls_labels = ls_counts.index.tolist()
        ls_values = ls_counts.values.tolist()
        ls_colors = {"Overloaded": _RED, "Balanced": _GREEN, "Underloaded": _BLUE}
        pie_colors = [ls_colors.get(l, _SLATE) for l in ls_labels]
    else:
        ls_labels, ls_values, pie_colors = [], [], []

    pie_fig = go.Figure(go.Pie(
        labels=ls_labels, values=ls_values, hole=0.45,
        marker=dict(colors=pie_colors), textinfo="label+percent",
    ))
    pie_fig.update_layout(showlegend=True, height=340)

    charts = dbc.Row([
        dbc.Col(_chart_card("Demand vs Capacity by Work Center", dvc_fig), md=7),
        dbc.Col(_chart_card("Load Status Distribution", pie_fig, 340),    md=5),
    ], className="mb-3")

    table_section = html.Div([
        html.Div("Capacity Planning Detail", style={"fontWeight": "700", "color": "#1e293b",
                                                     "marginBottom": "8px", "fontSize": "0.88rem"}),
        _data_table(_to_records(detail), list(detail.columns) if not detail.empty else [], _PURPLE),
    ], style={"backgroundColor": "#fff", "border": "1px solid #e2e8f0",
              "borderRadius": "8px", "padding": "1rem"})

    return html.Div([kpi_row, charts, table_section])


def _render_farmout():
    detail = backend._query(f"""
        SELECT farm_out_vendor,
               SUM(farm_out_orders)                  AS total_orders,
               ROUND(SUM(total_farm_out_cost),  0)   AS total_farm_out_cost,
               ROUND(SUM(implied_internal_cost), 0)  AS implied_internal_cost,
               ROUND(AVG(cost_premium_pct), 1)       AS avg_cost_premium_pct
        FROM {FQN}.gold_farmout_analysis
        GROUP BY farm_out_vendor
        ORDER BY total_farm_out_cost DESC
    """)

    total_fo   = int(detail["total_farm_out_cost"].sum())    if not detail.empty else 0
    total_int  = int(detail["implied_internal_cost"].sum())  if not detail.empty else 0
    avg_prem   = round(detail["avg_cost_premium_pct"].mean(), 1) if not detail.empty else "—"

    kpi_row = html.Div([
        _kpi("Total Farm-Out Cost",     _fmt(total_fo,  prefix="$"), "bi bi-box-arrow-up-right", _RED),
        _kpi("Implied Internal Cost",   _fmt(total_int, prefix="$"), "bi bi-building",           _BLUE),
        _kpi("Avg Cost Premium",        f"{avg_prem}%",              "bi bi-percent",             _AMBER),
    ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "16px"})

    if not detail.empty:
        vendors  = detail["farm_out_vendor"].tolist()
        fo_cost  = detail["total_farm_out_cost"].tolist()
        int_cost = detail["implied_internal_cost"].tolist()
        orders   = detail["total_orders"].tolist()
    else:
        vendors = fo_cost = int_cost = orders = []

    # Farm-out vs internal cost by vendor
    cost_fig = go.Figure()
    cost_fig.add_trace(go.Bar(name="Farm-Out Cost",     x=vendors, y=fo_cost,
                              marker_color=_RED))
    cost_fig.add_trace(go.Bar(name="Implied Internal",  x=vendors, y=int_cost,
                              marker_color=_BLUE))
    cost_fig.update_layout(barmode="group")

    # Farm-out orders by vendor
    ord_fig = go.Figure(go.Bar(
        x=vendors, y=orders,
        marker_color=_AMBERS[:len(vendors)],
        text=[str(int(o)) for o in orders],
        textposition="outside",
    ))

    charts = dbc.Row([
        dbc.Col(_chart_card("Farm-Out vs Internal Cost by Vendor ($)", cost_fig), md=7),
        dbc.Col(_chart_card("Farm-Out Orders by Vendor", ord_fig),                md=5),
    ], className="mb-3")

    table_section = html.Div([
        html.Div("Farm-Out Vendor Detail", style={"fontWeight": "700", "color": "#1e293b",
                                                   "marginBottom": "8px", "fontSize": "0.88rem"}),
        _data_table(_to_records(detail), list(detail.columns) if not detail.empty else [], _RED),
    ], style={"backgroundColor": "#fff", "border": "1px solid #e2e8f0",
              "borderRadius": "8px", "padding": "1rem"})

    return html.Div([kpi_row, charts, table_section])


# ---- Embedded dashboard (AI/BI client SDK) ----------------------------------

def _render_embedded():
    """Render the container div for the @databricks/aibi-client SDK."""
    return html.Div([
        html.Div(
            id="aibi-dashboard-container",
            style={
                "width":         "100%",
                "minHeight":     "800px",
                "border":        "1px solid #e2e8f0",
                "borderRadius":  "10px",
                "overflow":      "hidden",
                "backgroundColor": "#f8fafc",
            },
        ),
        # Trigger JS re-init on each render via a clientside no-op
        dcc.Store(id="aibi-trigger", data=1),
    ])


# ---- Tab config -------------------------------------------------------------

_SUBTABS = [
    ("embedded",    "Live Dashboard",           _BLUE),
    ("util",        "Machine Utilization",      _BLUE),
    ("scheduling",  "Scheduling Performance",   _AMBER),
    ("maintenance", "Predictive Maintenance",   _RED),
    ("capacity",    "Capacity Planning",        _PURPLE),
    ("farmout",     "Farm-Out Analysis",        _GREEN),
]

_RENDERERS = {
    "embedded":    _render_embedded,
    "util":        _render_utilization,
    "scheduling":  _render_scheduling,
    "maintenance": _render_maintenance,
    "capacity":    _render_capacity,
    "farmout":     _render_farmout,
}


# ---- Layout -----------------------------------------------------------------

def layout():
    tabs = dbc.Tabs(
        id="dash-subtabs",
        active_tab="embedded",
        children=[
            dbc.Tab(
                tab_id=tid,
                label=label,
                label_style={"color": "#1e293b", "fontWeight": "600",
                              "fontSize": "0.85rem"},
                active_label_style={"color": color, "fontWeight": "700",
                                    "fontSize": "0.85rem"},
            )
            for tid, label, color in _SUBTABS
        ],
        style={"borderBottom": "2px solid #e2e8f0", "marginBottom": "18px"},
    )

    return html.Div([
        page_title(
            "Machine Optimization Dashboard",
            "Live SmartMFG dashboard powered by @databricks/aibi-client SDK",
        ),
        tabs,
        html.Div(id="dash-content", children=_loading_div()),
    ])


# ---- Callback ---------------------------------------------------------------

@callback(
    Output("dash-content", "children"),
    Input("dash-subtabs",  "active_tab"),
)
def render_tab(tab):
    renderer = _RENDERERS.get(tab)
    if renderer is None:
        return html.Div("Unknown tab.", style={"color": _SLATE})
    try:
        return renderer()
    except Exception as exc:
        return html.Div(
            f"Error loading data: {exc}",
            style={"color": "#b91c1c", "padding": "1rem",
                   "backgroundColor": "#fef2f2", "borderRadius": "8px"}
        )
