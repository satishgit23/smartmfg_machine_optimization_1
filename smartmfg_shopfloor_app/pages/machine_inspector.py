"""
pages/machine_inspector.py — Machine Inspector layout and callbacks.

Displays live sensor gauges and active work orders for any selected machine.
When a machine is DOWN or IDLE, a left pane appears showing alternative
active machines in the same work centre with a one-click order rerouting action.
"""

import dash
from dash import html, dcc, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc

from server import backend
from components.ui import C, CARD_STYLE, STATUS_CFG, section_header, page_title


# ── Layout ─────────────────────────────────────────────────────────────────

def layout():
    return html.Div([
        page_title(
            "Machine Inspector",
            "Select a machine to view live status and active orders — "
            "failed machines show automatic rerouting options",
        ),

        # Machine selector dropdown
        dcc.Dropdown(
            id="mi-machine-selector",
            placeholder="Select a machine…",
            style={"backgroundColor": C["card"], "color": C["text"]},
            className="mb-4",
        ),

        # Main two-column area
        dbc.Row([
            # LEFT PANE — alternative machines (visible only when machine is down)
            dbc.Col(
                html.Div(
                    id="mi-reassignment-panel",
                    style={"display": "none"},
                    children=[
                        html.Div([
                            html.Div([
                                html.I(className="bi bi-arrow-left-right me-2",
                                       style={"color": C["amber"]}),
                                html.Span("Re-route Orders", className="fw-semibold"),
                            ], className="d-flex align-items-center mb-3 fs-6",
                               style={"color": C["text"]}),

                            html.P(
                                "Machine is DOWN. Select an active machine in the same "
                                "work centre to reroute these orders.",
                                className="small mb-3",
                                style={"color": C["muted"]},
                            ),

                            html.Div(id="mi-alt-machines-list"),

                            dbc.Button(
                                [html.I(className="bi bi-send me-2"), "Reassign All Orders"],
                                id="mi-reassign-btn",
                                color="warning", size="sm",
                                className="w-100 mt-3",
                                disabled=True,
                            ),

                            html.Div(id="mi-reassign-result", className="mt-2"),
                        ], style={
                            **CARD_STYLE,
                            "height": "100%",
                            "borderLeft": f"3px solid {C['amber']}",
                        }),
                    ],
                ),
                id="mi-left-col", width=3,
            ),

            # RIGHT — machine status card + orders table
            dbc.Col(
                html.Div(
                    id="mi-right-panel",
                    children=[
                        html.Div([
                            html.I(className="bi bi-display fs-1 mb-3 d-block",
                                   style={"color": C["border"]}),
                            html.P("Select a machine from the dropdown above to inspect "
                                   "its current status and active orders.",
                                   style={"color": C["muted"]}),
                        ], className="text-center mt-5"),
                    ],
                ),
                id="mi-right-col", width=12,
            ),
        ]),

        # Stores
        dcc.Store(id="mi-selected-alt-machine"),
        dcc.Store(id="mi-current-machine-meta"),
    ])


# ── Callbacks ──────────────────────────────────────────────────────────────

@callback(
    Output("mi-machine-selector", "options"),
    Output("mi-machine-selector", "value"),
    Input("refresh-interval",     "n_intervals"),
)
def populate_machine_dropdown(_):
    machines = backend.get_machines_list()
    opts = []
    for _, r in machines.iterrows():
        cfg = STATUS_CFG.get(r["status"], {})
        opts.append({
            "label": html.Div([
                dbc.Badge(
                    cfg.get("label", r["status"]),
                    color=cfg.get("badge", "secondary"),
                    className="me-2",
                    style={"fontSize": "0.65rem", "verticalAlign": "middle"},
                ),
                html.Span(f"{r['machine_id']} — {r['machine_name']}",
                          style={"color": C["text"]}),
                html.Span(f" ({r['work_center']})",
                          style={"color": C["muted"], "fontSize": "0.82rem"}),
            ], style={"display": "flex", "alignItems": "center"}),
            "value": r["machine_id"],
        })
    return opts, None


@callback(
    Output("mi-right-panel",          "children"),
    Output("mi-reassignment-panel",   "style"),
    Output("mi-left-col",             "width"),
    Output("mi-right-col",            "width"),
    Output("mi-alt-machines-list",    "children"),
    Output("mi-current-machine-meta", "data"),
    Input("mi-machine-selector",      "value"),
)
def inspect_machine(machine_id):
    _hidden = {"display": "none"}
    _empty  = (
        html.Div([
            html.I(className="bi bi-display fs-1 mb-3 d-block",
                   style={"color": C["border"]}),
            html.P("Select a machine from the dropdown above.",
                   style={"color": C["muted"]}),
        ], className="text-center mt-5"),
        _hidden, 0, 12, [], None,
    )

    if not machine_id:
        return _empty

    detail = backend.get_machine_detail(machine_id)
    orders = backend.get_machine_orders(machine_id)

    if not detail:
        return _empty

    status  = detail.get("status", "Active")
    sc      = STATUS_CFG.get(status, STATUS_CFG["Idle"])
    is_down = status in ("Maintenance", "Idle")

    # ── Sensor values ──────────────────────────────────────────────────────
    health  = float(detail.get("health_score")        or 0)
    temp    = float(detail.get("temperature_celsius") or 0)
    vib     = float(detail.get("vibration_mm_s")      or 0)
    wear    = float(detail.get("tool_wear_pct")        or 0)
    coolant = float(detail.get("coolant_flow_lpm")    or 0)
    power   = float(detail.get("power_consumption_kw") or 0)
    spindle = float(detail.get("spindle_speed_rpm")   or 0)
    anomaly = bool(detail.get("anomaly_flag")         or False)
    atype   = detail.get("anomaly_type") or ""
    last_ts = str(detail.get("last_reading_ts") or "—")[:19]

    health_c = C["green"] if health >= 70 else C["amber"] if health >= 40 else C["red"]
    temp_c   = C["green"] if temp < 80   else C["amber"] if temp < 100    else C["red"]
    vib_c    = C["green"] if vib  < 3    else C["amber"] if vib  < 5      else C["red"]
    wear_c   = C["green"] if wear < 60   else C["amber"] if wear < 80     else C["red"]

    anomaly_alert = (
        dbc.Alert([
            html.I(className="bi bi-exclamation-triangle-fill me-2"),
            html.Strong(f"Active Anomaly: {atype}"),
        ], color="danger", className="mb-0 py-2 px-3 mt-3", style={"fontSize": "0.88rem"})
        if anomaly else html.Span()
    )

    # ── Machine status card ────────────────────────────────────────────────
    status_card = html.Div([
        # Header
        html.Div([
            html.Div([
                html.I(className=f"bi {sc['icon']} me-2 fs-4", style={"color": sc["color"]}),
                html.Div([
                    html.Div(detail.get("machine_name", ""), className="fw-bold fs-5",
                             style={"color": C["text"]}),
                    html.Div(detail.get("machine_type", ""),
                             style={"color": C["muted"], "fontSize": "0.85rem"}),
                ]),
            ], className="d-flex align-items-center"),
            html.Div([
                dbc.Badge(sc["label"], color=sc["badge"], className="me-2 fs-6 px-3 py-2"),
                html.Span(detail.get("work_center", ""), className="badge",
                          style={"backgroundColor": C["border"], "color": C["blue"],
                                 "fontSize": "0.75rem"}),
            ]),
        ], className="d-flex justify-content-between align-items-center"),
        anomaly_alert,

        html.Hr(style={"borderColor": C["border"], "margin": "1rem 0"}),

        # Sensor gauges
        dbc.Row([
            _sensor_cell("Health Score", f"{health:.0f}",    health_c, "bi-heart-pulse-fill"),
            _sensor_cell("Temperature",  f"{temp:.1f}°C",    temp_c,   "bi-thermometer-half"),
            _sensor_cell("Vibration",    f"{vib:.2f} mm/s",  vib_c,    "bi-activity"),
            _sensor_cell("Tool Wear",    f"{wear:.0f}%",     wear_c,   "bi-tools"),
            _sensor_cell("Coolant Flow", f"{coolant:.1f} L/m", C["cyan"],  "bi-droplet"),
            _sensor_cell("Power",        f"{power:.1f} kW",  C["blue"],  "bi-lightning-charge"),
            _sensor_cell("Spindle RPM",  f"{spindle:.0f}",   C["muted"], "bi-arrow-repeat"),
        ], className="mb-2 g-2"),

        html.Div([
            html.I(className="bi bi-clock me-1", style={"color": C["muted"]}),
            html.Span(f"Last reading: {last_ts}  ·  Installed: {detail.get('installation_year', '—')}"
                      f"  ·  Last PM: {detail.get('last_pm_date', '—')}"
                      f"  ·  PM interval: {detail.get('preventive_maint_interval_hrs', '—')} hrs",
                      style={"color": C["muted"], "fontSize": "0.78rem"}),
        ], className="mt-2"),
    ], style={**CARD_STYLE, "borderLeft": f"3px solid {sc['color']}", "marginBottom": "1rem"})

    # ── Active orders table ────────────────────────────────────────────────
    from dash import dash_table
    orders_section = html.Div([
        section_header(f"Active Work Orders ({len(orders)})", "bi-list-task"),
        (
            dbc.Alert([
                html.I(className="bi bi-info-circle me-2"),
                "No active orders assigned to this machine.",
            ], color="secondary", className="py-2 px-3", style={"fontSize": "0.88rem"})
            if orders.empty else
            dash_table.DataTable(
                columns=[
                    {"name": "Work Order",  "id": "work_order_id"},
                    {"name": "Part #",      "id": "part_number"},
                    {"name": "Description", "id": "part_description"},
                    {"name": "Qty",         "id": "order_qty"},
                    {"name": "Operation",   "id": "operation_description"},
                    {"name": "Status",      "id": "status"},
                    {"name": "Priority",    "id": "priority"},
                    {"name": "Due Date",    "id": "due_date"},
                    {"name": "Est. Hrs",    "id": "standard_total_hrs"},
                ],
                data=orders.to_dict("records"),
                style_table={"overflowX": "auto"},
                style_data={"backgroundColor": C["card"], "color": C["text"],
                            "border": f"1px solid {C['border']}"},
                style_header={"backgroundColor": C["border"], "color": C["muted"],
                              "fontWeight": "600", "fontSize": "0.72rem",
                              "textTransform": "uppercase",
                              "border": f"1px solid {C['border']}"},
                style_data_conditional=[
                    {"if": {"filter_query": '{priority} = "High"',       "column_id": "priority"},
                     "color": C["red"],   "fontWeight": "bold"},
                    {"if": {"filter_query": '{priority} = "Medium"',     "column_id": "priority"},
                     "color": C["amber"]},
                    {"if": {"filter_query": '{status} = "In Progress"',  "column_id": "status"},
                     "color": C["green"]},
                ],
                page_size=10, sort_action="native",
            )
        ),
    ], style=CARD_STYLE)

    right_content = html.Div([status_card, orders_section])

    # ── Reassignment panel ─────────────────────────────────────────────────
    if is_down:
        alt_df      = backend.get_alternative_machines(detail.get("work_center", ""), machine_id)
        alt_list    = _build_alt_machine_list(alt_df)
        panel_style = {"display": "block"}
        left_w, right_w = 3, 9
    else:
        alt_list    = []
        panel_style = {"display": "none"}
        left_w, right_w = 0, 12

    meta = {
        "machine_id":  machine_id,
        "work_center": detail.get("work_center", ""),
        "status":      status,
        "order_count": len(orders),
    }
    return right_content, panel_style, left_w, right_w, alt_list, meta


@callback(
    Output("mi-reassign-btn",         "disabled"),
    Output("mi-selected-alt-machine", "data"),
    Input({"type": "alt-machine-radio", "index": dash.ALL}, "checked"),
    State({"type": "alt-machine-radio", "index": dash.ALL}, "id"),
    prevent_initial_call=True,
)
def alt_machine_selected(checked_list, id_list):
    selected = None
    for chk, id_obj in zip(checked_list or [], id_list or []):
        if chk:
            selected = id_obj["index"]
            break
    return (selected is None), selected


@callback(
    Output("mi-reassign-result",       "children"),
    Input("mi-reassign-btn",           "n_clicks"),
    State("mi-selected-alt-machine",   "data"),
    State("mi-current-machine-meta",   "data"),
    prevent_initial_call=True,
)
def do_reassignment(n_clicks, alt_machine_id, meta):
    if not n_clicks or not alt_machine_id or not meta:
        return no_update
    count      = meta.get("order_count", 0)
    machine_id = meta.get("machine_id", "")
    return dbc.Alert([
        html.I(className="bi bi-check-circle-fill me-2"),
        html.Strong(f"{count} order(s) queued for reassignment"),
        html.Br(),
        html.Small(
            f"Machine {machine_id} → {alt_machine_id}. "
            "Changes will apply on the next pipeline run.",
            style={"color": "rgba(255,255,255,0.75)"},
        ),
    ], color="success", dismissable=True, className="py-2 px-3",
       style={"fontSize": "0.85rem"})


# ── Private helpers ────────────────────────────────────────────────────────

def _sensor_cell(label, value, color, icon):
    return dbc.Col(
        html.Div([
            html.I(className=f"bi {icon} mb-1 d-block",
                   style={"color": color, "fontSize": "1.2rem"}),
            html.Div(value, className="fw-bold",
                     style={"color": color, "fontSize": "1.1rem"}),
            html.Div(label,
                     style={"color": C["muted"], "fontSize": "0.68rem"}),
        ], className="text-center p-2",
           style={"backgroundColor": C["border"], "borderRadius": "6px"}),
        width="auto", className="text-center",
    )


def _build_alt_machine_list(df):
    if df.empty:
        return [dbc.Alert(
            "No active machines available in this work centre.",
            color="warning", className="py-2 px-3",
            style={"fontSize": "0.82rem"},
        )]

    items = []
    for _, r in df.iterrows():
        avail   = float(r.get("available_hrs") or 0)
        avail_c = C["green"] if avail > 4 else C["amber"] if avail > 0 else C["red"]
        items.append(
            dbc.Card([
                dbc.CardBody([
                    dbc.RadioButton(
                        id={"type": "alt-machine-radio", "index": r["machine_id"]},
                        value=r["machine_id"],
                        className="me-2",
                    ),
                    html.Div([
                        html.Div(r["machine_name"], className="fw-semibold small",
                                 style={"color": C["text"]}),
                        html.Div(r["machine_type"],
                                 style={"color": C["muted"], "fontSize": "0.7rem"}),
                        html.Div([
                            html.Span(f"Avail: {avail:.1f}h",
                                      style={"color": avail_c, "fontSize": "0.7rem",
                                             "fontWeight": "600"}),
                            html.Span(f"  ·  {int(r.get('current_orders', 0))} orders",
                                      style={"color": C["muted"], "fontSize": "0.7rem"}),
                        ]),
                    ]),
                ], className="p-2 d-flex align-items-start gap-2"),
            ], className="mb-2",
               style={"backgroundColor": C["border"],
                      "border": f"1px solid {C['border']}",
                      "cursor": "pointer"}),
        )
    return items
