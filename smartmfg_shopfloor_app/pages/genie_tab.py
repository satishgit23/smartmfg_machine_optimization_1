"""
pages/genie_tab.py -- SmartMFG Genie chat interface.

Calls the Genie Conversation API directly so operators can ask natural
language questions about machines, work orders, utilisation, maintenance,
and farm-out costs -- without leaving the app.
"""

import dash
from dash import html, dcc, Input, Output, State, callback, ctx
import dash_bootstrap_components as dbc
from datetime import datetime

from server import agent
from components.ui import page_title

# ?? Styles ??????????????????????????????????????????????????????????????????

_CHAT_WRAP = {
    "height":        "600px",
    "overflowY":     "auto",
    "display":       "flex",
    "flexDirection": "column",
    "gap":           "14px",
    "padding":       "1.2rem",
    "backgroundColor": "#f8fafc",
    "border":        "1px solid #e2e8f0",
    "borderRadius":  "10px",
}

_BTN_PRIMARY = {
    "backgroundColor": "#d97706",
    "color":     "#fff",
    "border":    "none",
    "borderRadius": "6px",
    "padding":   "0.45rem 1.2rem",
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

_SAMPLE_QUESTIONS = [
    "Which machines have the highest utilization this month?",
    "Show me all high-priority work orders due this week",
    "Which machines are overdue for preventive maintenance?",
    "What is the total farm-out cost by work center?",
    "List machines currently in Active status with their work center",
    "Which work center has the most open work orders?",
]


# ?? Chat bubble helpers ??????????????????????????????????????????????????????

def _user_bubble(text: str):
    return html.Div([
        html.Div(text, style={
            "backgroundColor": "#d97706",
            "color": "#fff",
            "borderRadius": "16px 16px 4px 16px",
            "padding": "0.6rem 1rem",
            "maxWidth": "70%",
            "fontSize": "0.9rem",
            "lineHeight": "1.45",
        }),
    ], style={"display": "flex", "justifyContent": "flex-end"})


def _genie_bubble(content):
    return html.Div([
        html.Span("?", style={"fontSize": "1.3rem", "marginRight": "8px",
                               "alignSelf": "flex-start", "marginTop": "2px"}),
        html.Div(content, style={
            "backgroundColor": "#fff",
            "border":          "1px solid #e2e8f0",
            "borderRadius":    "4px 16px 16px 16px",
            "padding":         "0.85rem 1.1rem",
            "maxWidth":        "88%",
            "boxShadow":       "0 1px 4px rgba(0,0,0,0.06)",
        }),
    ], style={"display": "flex", "alignItems": "flex-start"})


def _result_table(rows: list, columns: list):
    """Compact scrollable result table."""
    if not rows:
        return html.Span("No results returned.", style={"color": "#64748b", "fontSize": "0.85rem"})

    header = html.Tr([
        html.Th(c.replace("_", " ").title(),
                style={"backgroundColor": "#fef3c7", "color": "#92400e",
                       "fontSize": "0.7rem", "padding": "5px 10px",
                       "textTransform": "uppercase", "letterSpacing": "0.04em",
                       "whiteSpace": "nowrap"})
        for c in columns
    ])
    body = []
    for r in rows[:50]:   # cap display at 50 rows
        body.append(html.Tr([
            html.Td(str(r.get(c, "")) if r.get(c) is not None else "--",
                    style={"fontSize": "0.82rem", "padding": "5px 10px",
                           "color": "#1e293b", "borderBottom": "1px solid #f8fafc",
                           "whiteSpace": "nowrap"})
            for c in columns
        ]))

    overflow_note = (
        html.Div(f"Showing 50 of {len(rows)} rows",
                 style={"color": "#94a3b8", "fontSize": "0.72rem", "marginTop": "4px"})
        if len(rows) > 50 else None
    )

    return html.Div([
        html.Div(
            html.Table([html.Thead(header), html.Tbody(body)],
                       style={"borderCollapse": "collapse", "width": "100%"}),
            style={"overflowX": "auto", "borderRadius": "6px",
                   "border": "1px solid #fde68a"}
        ),
        overflow_note,
    ])


def _sql_disclosure(sql: str, source: str):
    label = "SmartMFG Genie -- generated SQL" if source == "genie" else "Direct SQL"
    return html.Details([
        html.Summary(
            [html.Span("? ", style={"color": "#d97706"}),
             html.Span(label, style={"color": "#64748b", "fontSize": "0.72rem"})],
            style={"cursor": "pointer", "marginTop": "10px", "userSelect": "none"},
        ),
        html.Pre(sql or "(no SQL captured)",
                 style={"fontSize": "0.7rem", "color": "#475569",
                        "backgroundColor": "#fef9ee", "border": "1px solid #fde68a",
                        "borderRadius": "4px", "padding": "6px 8px",
                        "marginTop": "4px", "overflowX": "auto",
                        "whiteSpace": "pre-wrap"}),
    ])


def _error_bubble(message: str):
    return _genie_bubble(
        html.Span(f"? {message}",
                  style={"color": "#b91c1c", "fontSize": "0.88rem"})
    )


# ?? Layout ????????????????????????????????????????????????????????????????????

_WELCOME = _genie_bubble([
    html.Strong("SmartMFG Genie", style={"color": "#d97706", "fontSize": "1rem"}),
    html.Br(),
    html.Span("Ask me anything about your machines, work orders, utilisation, "
              "maintenance, or farm-out costs.",
              style={"fontSize": "0.9rem", "color": "#475569"}),
])


def layout():
    return html.Div([
        page_title(
            "SmartMFG Genie",
            "Natural language analytics -- ask questions, get answers powered by AI",
        ),

        dbc.Row([
            # ?? Chat column ????????????????????????????????????????????
            dbc.Col([
                html.Div(id="genie-chat", children=[_WELCOME], style=_CHAT_WRAP),

                html.Div(id="genie-status",
                         style={"color": "#64748b", "fontSize": "0.75rem",
                                "marginTop": "5px", "minHeight": "16px"}),

                html.Div([
                    dcc.Textarea(
                        id="genie-input",
                        placeholder="Ask a question about your shop floor data...",
                        style={"flex": "1", "resize": "none", "height": "60px",
                               "borderRadius": "8px", "border": "1px solid #fbbf24",
                               "padding": "0.55rem 0.8rem", "fontSize": "0.9rem",
                               "fontFamily": "inherit", "outline": "none"},
                    ),
                    html.Div([
                        html.Button("Ask", id="genie-ask-btn",
                                    style=_BTN_PRIMARY, n_clicks=0),
                        html.Button("Clear", id="genie-clear-btn",
                                    style={**_BTN_GHOST, "marginTop": "4px"},
                                    n_clicks=0),
                    ], style={"display": "flex", "flexDirection": "column",
                              "gap": "4px", "marginLeft": "8px"}),
                ], style={"display": "flex", "marginTop": "10px",
                          "alignItems": "flex-start"}),
            ], md=8),

            # ?? Sample questions panel ?????????????????????????????????
            dbc.Col([
                html.Div([
                    html.Div([
                        html.Span("?", style={"marginRight": "6px"}),
                        html.Span("Sample questions",
                                  style={"fontWeight": "700", "color": "#1e293b",
                                         "fontSize": "0.88rem"}),
                    ], style={"marginBottom": "12px"}),

                    html.Div([
                        html.Button(
                            q,
                            id={"type": "genie-sample-btn", "index": i},
                            style={**_BTN_GHOST,
                                   "width": "100%", "textAlign": "left",
                                   "marginBottom": "6px", "whiteSpace": "normal",
                                   "lineHeight": "1.35"},
                            n_clicks=0,
                        )
                        for i, q in enumerate(_SAMPLE_QUESTIONS)
                    ]),

                    html.Hr(style={"borderColor": "#fde68a", "margin": "14px 0"}),

                    html.Div([
                        html.Span("?", style={"marginRight": "6px"}),
                        html.Span("Tips", style={"fontWeight": "600",
                                                  "color": "#1e293b",
                                                  "fontSize": "0.85rem"}),
                    ], style={"marginBottom": "8px"}),
                    html.Ul([
                        html.Li("Reference machines by ID (e.g. MCH-010) or name",
                                style={"marginBottom": "5px"}),
                        html.Li("Ask for comparisons: 'which work center has more...'",
                                style={"marginBottom": "5px"}),
                        html.Li("Filter by time: 'last 30 days', 'this month'",
                                style={"marginBottom": "5px"}),
                        html.Li("Ask follow-up questions -- Genie remembers context",
                                style={"marginBottom": "5px"}),
                    ], style={"fontSize": "0.8rem", "color": "#64748b",
                              "paddingLeft": "1.1rem"}),
                ], style={
                    "backgroundColor": "#fffbeb",
                    "border":          "1px solid #fde68a",
                    "borderRadius":    "10px",
                    "padding":         "1rem",
                }),
            ], md=4),
        ]),
    ])


# ?? Callbacks ?????????????????????????????????????????????????????????????????

@callback(
    Output("genie-chat",    "children"),
    Output("genie-status",  "children"),
    Output("genie-input",   "value"),
    Input("genie-ask-btn",  "n_clicks"),
    Input("genie-clear-btn","n_clicks"),
    Input({"type": "genie-sample-btn", "index": dash.ALL}, "n_clicks"),
    State("genie-input",    "value"),
    State("genie-chat",     "children"),
    prevent_initial_call=True,
)
def handle_genie(ask_n, clear_n, sample_clicks, user_text, chat_history):
    triggered = ctx.triggered_id

    # ?? Clear ??????????????????????????????????????????????????????????
    if triggered == "genie-clear-btn":
        return [_WELCOME], "", ""

    # ?? Sample question clicked ????????????????????????????????????????
    if isinstance(triggered, dict) and triggered.get("type") == "genie-sample-btn":
        idx       = triggered["index"]
        user_text = _SAMPLE_QUESTIONS[idx]

    # ?? Ask ????????????????????????????????????????????????????????????
    if not user_text or not user_text.strip():
        return chat_history, "? Please enter a question first.", ""

    question = user_text.strip()
    chat      = list(chat_history) + [_user_bubble(question)]

    # Status message while waiting
    ts = datetime.now().strftime("%H:%M:%S")
    try:
        result = agent.ask_genie(question)
    except Exception as exc:
        chat.append(_error_bubble(f"Genie error: {exc}"))
        return chat, f"[{ts}] ? Error calling Genie", ""

    status = result.get("status", "")
    rows   = result.get("rows", [])
    cols   = result.get("columns", [])
    sql    = result.get("sql", "")
    src    = result.get("source", "genie")
    text   = result.get("text", "")

    if status in ("FAILED", "CANCELLED"):
        chat.append(_error_bubble(
            text or "Genie could not answer this question. Try rephrasing it."
        ))
        return chat, f"[{ts}] ? Genie returned {status}", ""

    # Build response content
    parts = []
    if text:
        parts.append(html.P(text, style={"color": "#1e293b", "fontSize": "0.9rem",
                                          "marginBottom": "10px"}))

    if rows:
        parts.append(html.Div([
            html.Span(f"{len(rows)} row{'s' if len(rows) != 1 else ''} returned",
                      style={"color": "#64748b", "fontSize": "0.78rem",
                             "marginBottom": "6px", "display": "block"}),
            _result_table(rows, cols),
        ]))
    elif not text:
        parts.append(html.Span("Genie didn't return any rows for this question.",
                               style={"color": "#64748b", "fontSize": "0.85rem"}))

    if sql:
        parts.append(_sql_disclosure(sql, src))

    chat.append(_genie_bubble(parts))
    row_note = f" ? {len(rows)} rows" if rows else ""
    return chat, f"[{ts}] ? Answer received{row_note}", ""
