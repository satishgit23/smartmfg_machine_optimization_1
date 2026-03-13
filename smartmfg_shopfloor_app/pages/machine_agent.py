"""
pages/machine_agent.py — Machine Recovery Agent chat interface.

A conversational agent that:
  1. Accepts free-text reports of a machine being down
  2. Queries the SmartMFG Genie Space to find affected work orders
     and available replacement machines in the same work center
  3. Presents the findings and asks the operator to confirm the replacement
  4. Executes a SQL UPDATE to reassign all active work orders

State machine stages stored in dcc.Store("ma-state"):
  idle       → waiting for the operator to report a machine down
  identified → machine ID extracted; querying Genie for work orders + alternatives
  confirming → results presented; waiting for operator to pick a replacement
  updating   → SQL UPDATE in progress
  done       → reassignment complete
"""

import json
from datetime import datetime

import dash
from dash import html, dcc, Input, Output, State, callback, ctx
import dash_bootstrap_components as dbc

from server import agent
from components.ui import page_title

# ── Styles ──────────────────────────────────────────────────────────────────

_CHAT_WRAP = {
    "height":     "520px",
    "overflowY":  "auto",
    "display":    "flex",
    "flexDirection": "column",
    "gap":        "12px",
    "padding":    "1rem",
    "backgroundColor": "#f8fafc",
    "border":     "1px solid #e2e8f0",
    "borderRadius": "10px",
}

_BTN_PRIMARY = {
    "backgroundColor": "#2563eb",
    "color":     "#fff",
    "border":    "none",
    "borderRadius": "6px",
    "padding":   "0.45rem 1.1rem",
    "fontWeight": "600",
    "cursor":    "pointer",
    "fontSize":  "0.88rem",
}

_BTN_GHOST = {
    "backgroundColor": "transparent",
    "color":     "#64748b",
    "border":    "1px solid #cbd5e1",
    "borderRadius": "6px",
    "padding":   "0.4rem 0.9rem",
    "cursor":    "pointer",
    "fontSize":  "0.82rem",
}


# ── Chat bubble helpers ──────────────────────────────────────────────────────

def _user_bubble(text: str):
    return html.Div([
        html.Div(text, style={
            "backgroundColor": "#2563eb",
            "color": "#fff",
            "borderRadius": "16px 16px 4px 16px",
            "padding": "0.6rem 1rem",
            "maxWidth": "70%",
            "fontSize": "0.9rem",
        }),
    ], style={"display": "flex", "justifyContent": "flex-end"})


def _agent_bubble(content, icon="🤖"):
    if isinstance(content, str):
        content = html.Span(content, style={"fontSize": "0.9rem", "color": "#1e293b"})
    return html.Div([
        html.Span(icon, style={"fontSize": "1.2rem", "marginRight": "8px",
                               "alignSelf": "flex-start", "marginTop": "2px"}),
        html.Div(content, style={
            "backgroundColor": "#fff",
            "border":          "1px solid #e2e8f0",
            "borderRadius":    "4px 16px 16px 16px",
            "padding":         "0.75rem 1rem",
            "maxWidth":        "85%",
            "boxShadow":       "0 1px 3px rgba(0,0,0,0.06)",
        }),
    ], style={"display": "flex", "alignItems": "flex-start"})


def _system_msg(text: str, color="#64748b"):
    return html.Div(text, style={
        "textAlign": "center",
        "color":     color,
        "fontSize":  "0.78rem",
        "fontStyle": "italic",
        "padding":   "2px 0",
    })


def _wo_table(rows: list, columns: list) -> html.Div:
    """Render a compact work-order table inside the agent bubble."""
    if not rows:
        return html.Span("No active work orders found.", style={"color": "#64748b"})
    # show only key columns if many exist
    show_cols = [c for c in columns
                 if c in ("work_order_id", "part_description", "order_qty",
                           "priority", "due_date", "status")]
    if not show_cols:
        show_cols = columns[:6]

    header = html.Tr([
        html.Th(c.replace("_", " ").title(),
                style={"backgroundColor": "#f1f5f9", "color": "#475569",
                       "fontSize": "0.7rem", "padding": "4px 8px",
                       "textTransform": "uppercase", "letterSpacing": "0.04em"})
        for c in show_cols
    ])
    body_rows = []
    for r in rows:
        cells = []
        for c in show_cols:
            val = r.get(c, "—")
            style = {"fontSize": "0.8rem", "padding": "4px 8px",
                     "color": "#1e293b", "borderBottom": "1px solid #f1f5f9"}
            if c == "priority":
                clr = {"HIGH": "#ef4444", "MEDIUM": "#f59e0b", "LOW": "#64748b"}.get(str(val).upper(), "#64748b")
                cells.append(html.Td(
                    html.Span(str(val),
                              style={"backgroundColor": clr + "15", "color": clr,
                                     "borderRadius": "4px", "padding": "2px 6px",
                                     "fontWeight": "600", "fontSize": "0.72rem"}),
                    style=style))
            else:
                cells.append(html.Td(str(val) if val is not None else "—", style=style))
        body_rows.append(html.Tr(cells))

    return html.Div(
        html.Table([html.Thead(header), html.Tbody(body_rows)],
                   style={"width": "100%", "borderCollapse": "collapse",
                          "fontSize": "0.82rem"}),
        style={"overflowX": "auto", "borderRadius": "6px",
               "border": "1px solid #e2e8f0"}
    )


def _machine_options(rows: list) -> list:
    """Build replacement machine option buttons."""
    btns = []
    for r in rows:
        mid   = r.get("machine_id", "")
        mname = r.get("machine_name", mid)
        wc    = r.get("work_center", "")
        cnt   = r.get("active_orders", 0)
        btns.append(html.Button(
            [html.Strong(mid), f"  {mname}  ",
             html.Span(f"({cnt} active orders)",
                       style={"color": "#64748b", "fontWeight": "400"})],
            id={"type": "ma-machine-btn", "machine_id": mid},
            style={**_BTN_GHOST, "marginBottom": "6px", "width": "100%",
                   "textAlign": "left"},
            n_clicks=0,
        ))
    return btns


# ── Layout ───────────────────────────────────────────────────────────────────

_WELCOME_MSG = _agent_bubble([
    html.Strong("SmartMFG Machine Recovery Agent", style={"color": "#2563eb"}),
    html.Br(),
    html.Span(
        "I can help reassign work orders when a machine goes down. "
        "Just tell me which machine is out of service — for example:",
        style={"fontSize": "0.9rem"},
    ),
    html.Ul([
        html.Li('"Machine MCH-010 is down"'),
        html.Li('"MCH-003 has failed"'),
        html.Li('"We have a breakdown on MCH-007"'),
    ], style={"marginTop": "8px", "marginBottom": "4px",
              "fontSize": "0.88rem", "color": "#475569"}),
])


def layout():
    return html.Div([
        page_title(
            "Machine Recovery Agent",
            "AI-powered work order reassignment — powered by SmartMFG Genie",
        ),

        # Agent state (stage + extracted data)
        dcc.Store(id="ma-state", data={"stage": "idle"}),

        dbc.Row([
            # ── Chat column ─────────────────────────────────────────────
            dbc.Col([
                # Chat history
                html.Div(id="ma-chat", children=[_WELCOME_MSG],
                         style=_CHAT_WRAP),

                # Status bar
                html.Div(id="ma-status",
                         style={"color": "#64748b", "fontSize": "0.78rem",
                                "marginTop": "6px", "minHeight": "18px"}),

                # Input row
                html.Div([
                    dcc.Textarea(
                        id="ma-input",
                        placeholder="Describe the machine issue…",
                        style={"flex": "1", "resize": "none", "height": "56px",
                               "borderRadius": "8px", "border": "1px solid #cbd5e1",
                               "padding": "0.5rem 0.75rem", "fontSize": "0.9rem",
                               "fontFamily": "inherit"},
                    ),
                    html.Div([
                        html.Button("Send", id="ma-send-btn",
                                    style=_BTN_PRIMARY, n_clicks=0),
                        html.Button("Reset", id="ma-reset-btn",
                                    style={**_BTN_GHOST, "marginTop": "4px"},
                                    n_clicks=0),
                    ], style={"display": "flex", "flexDirection": "column",
                              "gap": "4px", "marginLeft": "8px"}),
                ], style={"display": "flex", "marginTop": "10px",
                          "alignItems": "flex-start"}),
            ], md=8),

            # ── Info panel ──────────────────────────────────────────────
            dbc.Col([
                html.Div([
                    html.Div([
                        html.I(className="bi bi-info-circle me-2",
                               style={"color": "#3b82f6"}),
                        html.Span("How it works",
                                  style={"fontWeight": "700", "color": "#1e293b"}),
                    ], style={"marginBottom": "12px"}),

                    html.Ol([
                        html.Li("Tell the agent which machine is down", style={"marginBottom": "6px"}),
                        html.Li("Agent queries SmartMFG Genie for active work orders", style={"marginBottom": "6px"}),
                        html.Li("Genie finds available machines in the same work center", style={"marginBottom": "6px"}),
                        html.Li("Select the replacement machine from the options", style={"marginBottom": "6px"}),
                        html.Li("Agent updates all work orders in the database", style={"marginBottom": "6px"}),
                    ], style={"fontSize": "0.85rem", "color": "#475569",
                              "paddingLeft": "1.2rem"}),

                    html.Hr(style={"borderColor": "#e2e8f0", "margin": "14px 0"}),

                    html.Div([
                        html.I(className="bi bi-gear-wide-connected me-2",
                               style={"color": "#8b5cf6"}),
                        html.Span("Machines available",
                                  style={"fontWeight": "600", "color": "#1e293b",
                                         "fontSize": "0.85rem"}),
                    ], style={"marginBottom": "8px"}),
                    html.Div(id="ma-machine-list",
                             style={"fontSize": "0.8rem", "color": "#64748b"}),
                ], style={
                    "backgroundColor": "#f8fafc",
                    "border":          "1px solid #e2e8f0",
                    "borderRadius":    "10px",
                    "padding":         "1rem",
                }),
            ], md=4),
        ]),
    ])


# ── State helpers ────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _genie_tag(source: str, sql: str) -> html.Details:
    """Collapsible Genie SQL disclosure."""
    label = "SmartMFG Genie" if source == "genie" else "Direct SQL (Genie fallback)"
    return html.Details([
        html.Summary(
            [html.I(className="bi bi-lightning-charge-fill me-1",
                    style={"color": "#f59e0b", "fontSize": "0.75rem"}),
             html.Span(label,
                       style={"color": "#64748b", "fontSize": "0.72rem"})],
            style={"cursor": "pointer", "marginTop": "8px"}),
        html.Pre(sql or "(no SQL captured)",
                 style={"fontSize": "0.7rem", "color": "#475569",
                        "backgroundColor": "#f1f5f9", "borderRadius": "4px",
                        "padding": "6px 8px", "marginTop": "4px",
                        "overflowX": "auto", "whiteSpace": "pre-wrap"}),
    ])


# ── Main callback ─────────────────────────────────────────────────────────────

@callback(
    Output("ma-chat",         "children"),
    Output("ma-state",        "data"),
    Output("ma-status",       "children"),
    Output("ma-input",        "value"),
    Output("ma-machine-list", "children"),
    Input("ma-send-btn",      "n_clicks"),
    Input("ma-reset-btn",     "n_clicks"),
    Input({"type": "ma-machine-btn", "machine_id": dash.ALL}, "n_clicks"),
    State("ma-input",         "value"),
    State("ma-state",         "data"),
    State("ma-chat",          "children"),
    prevent_initial_call=True,
)
def handle_message(send_n, reset_n, machine_btns, user_text,
                   state, chat_history):
    triggered = ctx.triggered_id

    # ── Reset ─────────────────────────────────────────────────────────────
    if triggered == "ma-reset-btn" or (
        isinstance(triggered, str) and triggered == "ma-reset-btn"
    ):
        return ([_WELCOME_MSG],
                {"stage": "idle"},
                "",
                "",
                _machine_list_placeholder())

    # ── User sent a message ───────────────────────────────────────────────
    if triggered == "ma-send-btn":
        if not user_text or not user_text.strip():
            return chat_history, state, "⚠ Please type a message first.", "", _machine_list_placeholder()

        text   = user_text.strip()
        stage  = state.get("stage", "idle")
        chat   = list(chat_history) + [_user_bubble(text)]

        # Stage: idle → user reports a machine down
        if stage == "idle":
            machine_id = agent.extract_machine_id(text)

            if not machine_id:
                chat.append(_agent_bubble(
                    "I couldn't identify a machine ID in your message. "
                    "Please include the machine ID in format MCH-NNN — for example: "
                    '"MCH-010 is down" or "Machine MCH-003 has failed".',
                    icon="🤔"
                ))
                return chat, state, "", "", _machine_list_placeholder()

            # Identified machine — start querying
            machine_info = agent.get_machine_info(machine_id)
            mname = machine_info.get("machine_name", machine_id)
            wc    = machine_info.get("work_center", "—")

            chat.append(_system_msg(f"⏳ Querying SmartMFG Genie for work orders on {machine_id}…"))
            chat.append(_agent_bubble(
                [html.Strong(f"🔴 {mname} ({machine_id}) reported down"),
                 html.Br(),
                 html.Span(f"Work Center: {wc}",
                           style={"color": "#64748b", "fontSize": "0.85rem"})],
                icon="⚠️"
            ))

            # Query Genie for work orders
            wo_result = agent.find_affected_work_orders(machine_id)
            wo_rows   = wo_result.get("rows", [])
            wo_count  = len(wo_rows)

            if wo_count == 0:
                chat.append(_agent_bubble(
                    f"✅ Good news — {machine_id} currently has no active work orders. "
                    "No reassignment is needed.",
                    icon="✅"
                ))
                return chat, {"stage": "idle"}, f"[{_now()}] No active work orders on {machine_id}.", "", _machine_list_placeholder()

            chat.append(_agent_bubble([
                html.Span(f"Found ", style={"fontSize": "0.9rem"}),
                html.Strong(f"{wo_count} active work order{'s' if wo_count != 1 else ''}"),
                html.Span(f" on {machine_id}:", style={"fontSize": "0.9rem"}),
                html.Br(), html.Br(),
                _wo_table(wo_rows, wo_result.get("columns", [])),
                _genie_tag(wo_result.get("source", "genie"), wo_result.get("sql", "")),
            ]))

            # Query Genie for available machines
            chat.append(_system_msg(f"⏳ Looking for available machines in {wc}…"))
            avail_result = agent.find_available_machines(machine_id)
            avail_rows   = avail_result.get("rows", [])

            if not avail_rows:
                chat.append(_agent_bubble(
                    f"⚠ No other Active machines found in work center {wc}. "
                    "You may need to escalate or reschedule these work orders manually.",
                    icon="⚠️"
                ))
                return chat, {"stage": "idle"}, f"[{_now()}] No available machines in {wc}.", "", _machine_list_placeholder()

            chat.append(_agent_bubble([
                html.Span(f"Available machines in {wc}:"),
                html.Br(), html.Br(),
                html.Div(_machine_options(avail_rows),
                         style={"display": "flex", "flexDirection": "column"}),
                _genie_tag(avail_result.get("source", "genie"), avail_result.get("sql", "")),
                html.Br(),
                html.Span(
                    "Click a machine above or type its ID to reassign all "
                    f"{wo_count} work orders.",
                    style={"color": "#64748b", "fontSize": "0.82rem"},
                ),
            ]))

            new_state = {
                "stage":      "confirming",
                "down_machine_id":   machine_id,
                "down_machine_name": mname,
                "work_center":       wc,
                "wo_count":          wo_count,
                "available": [r.get("machine_id") for r in avail_rows],
            }
            return (chat, new_state,
                    f"[{_now()}] {wo_count} active work orders found on {machine_id} · select replacement",
                    "",
                    _render_machine_list(avail_rows))

        # Stage: confirming → user typed a machine ID to select
        if stage == "confirming":
            selection = agent.extract_machine_id(text) or text.strip().upper()
            available = state.get("available", [])

            if selection not in available:
                chat.append(_agent_bubble(
                    f"I didn't recognise **{selection}** as an available machine. "
                    "Please choose one of the machines listed above.",
                    icon="🤔"
                ))
                return chat, state, "", "", _machine_list_placeholder()

            return _execute_reassignment(chat, state, selection)

    # ── Machine button clicked ────────────────────────────────────────────
    if isinstance(triggered, dict) and triggered.get("type") == "ma-machine-btn":
        selection = triggered["machine_id"]
        stage     = state.get("stage", "idle")
        if stage != "confirming":
            return chat_history, state, "", "", _machine_list_placeholder()
        chat = list(chat_history) + [_user_bubble(f"Reassign to {selection}")]
        return _execute_reassignment(chat, state, selection)

    return chat_history, state, "", user_text, _machine_list_placeholder()


# ── Reassignment execution (shared by button + text input) ───────────────────

def _execute_reassignment(chat, state, to_machine_id):
    from_machine = state.get("down_machine_id", "")
    from_name    = state.get("down_machine_name", from_machine)
    wo_count     = state.get("wo_count", 0)

    to_info  = agent.get_machine_info(to_machine_id)
    to_name  = to_info.get("machine_name", to_machine_id)

    chat.append(_system_msg(f"💾 Updating {wo_count} work orders…"))
    try:
        updated = agent.reassign_work_orders(from_machine, to_machine_id)
        chat.append(_agent_bubble([
            html.Strong(f"✅ Reassignment complete",
                        style={"color": "#16a34a", "fontSize": "1rem"}),
            html.Br(), html.Br(),
            html.Div([
                html.Div([
                    html.Span("From: ", style={"color": "#64748b", "fontSize": "0.82rem"}),
                    html.Strong(f"{from_machine} — {from_name}",
                                style={"color": "#ef4444"}),
                ], style={"marginBottom": "4px"}),
                html.Div([
                    html.Span("To:   ", style={"color": "#64748b", "fontSize": "0.82rem"}),
                    html.Strong(f"{to_machine_id} — {to_name}",
                                style={"color": "#16a34a"}),
                ], style={"marginBottom": "4px"}),
                html.Div([
                    html.Span("Work orders updated: ",
                              style={"color": "#64748b", "fontSize": "0.82rem"}),
                    html.Strong(str(updated)),
                ]),
            ], style={"backgroundColor": "#f0fdf4", "border": "1px solid #bbf7d0",
                      "borderRadius": "6px", "padding": "0.75rem 1rem"}),
            html.Br(),
            html.Span(
                "The Machine Fleet and Command Centre will reflect the updated "
                "assignments on next refresh. Type a new machine ID to handle another failure.",
                style={"color": "#64748b", "fontSize": "0.82rem"},
            ),
        ], icon="✅"))
        status_msg = f"[{_now()}] ✅ {updated} work orders moved from {from_machine} → {to_machine_id}"
        new_state  = {"stage": "idle"}
    except Exception as exc:
        chat.append(_agent_bubble(
            f"❌ Update failed: {exc}. Please check the database permissions and try again.",
            icon="❌"
        ))
        status_msg = f"[{_now()}] ❌ Update failed — {exc}"
        new_state  = state

    return chat, new_state, status_msg, "", _machine_list_placeholder()


def _machine_list_placeholder():
    return html.Span("No active session", style={"color": "#94a3b8"})


def _render_machine_list(rows: list):
    if not rows:
        return _machine_list_placeholder()
    items = []
    for r in rows:
        cnt = r.get("active_orders", 0)
        items.append(html.Div([
            html.Span(r.get("machine_id", ""), style={"fontWeight": "700"}),
            html.Span(f" {r.get('machine_name', '')}",
                      style={"color": "#475569"}),
            html.Span(f" · {cnt} orders",
                      style={"color": "#94a3b8", "fontSize": "0.75rem"}),
        ], style={"padding": "3px 0", "borderBottom": "1px solid #f1f5f9"}))
    return items
