"""
pages/genie_tab.py -- SmartMFG Genie chat interface.

Calls the Genie Conversation API and renders results with:
  - dash_table.DataTable  (sortable, filterable, paginated)
  - Auto Plotly chart     (bar / line / horizontal-bar based on column types)
"""

import dash
from dash import html, dcc, Input, Output, State, callback, ctx
from dash import dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from datetime import datetime

from server import agent
from components.ui import page_title

# ---- Styles -----------------------------------------------------------------

_CHAT_WRAP = {
    "height":        "660px",
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

_TABLE_STYLE_CELL = {
    "fontFamily": "inherit",
    "fontSize":   "0.82rem",
    "padding":    "6px 12px",
    "textAlign":  "left",
    "border":     "1px solid #f1f5f9",
    "color":      "#1e293b",
    "minWidth":   "80px",
    "maxWidth":   "240px",
    "overflow":   "hidden",
    "textOverflow": "ellipsis",
}

_TABLE_STYLE_HEADER = {
    "backgroundColor": "#fef3c7",
    "color":           "#92400e",
    "fontWeight":      "700",
    "fontSize":        "0.72rem",
    "textTransform":   "uppercase",
    "letterSpacing":   "0.05em",
    "border":          "1px solid #fde68a",
    "padding":         "7px 12px",
}

_TABLE_STYLE_DATA_ODD = {"backgroundColor": "#ffffff"}
_TABLE_STYLE_DATA_EVEN = {"backgroundColor": "#fffbeb"}

_AMBER_PALETTE = ["#d97706", "#f59e0b", "#fbbf24", "#fcd34d",
                  "#f97316", "#ea580c", "#b45309", "#78350f"]

_SAMPLE_QUESTIONS = [
    "Which machines have the highest utilization this month?",
    "Show me all high-priority work orders due this week",
    "Which machines are overdue for preventive maintenance?",
    "What is the total farm-out cost by work center?",
    "List machines in Active status with their work center",
    "Which work center has the most open work orders?",
]


# ---- Column-type detection --------------------------------------------------

def _is_numeric(values):
    """Return True if the majority of non-null values are numeric."""
    non_null = [v for v in values if v is not None and v != ""]
    if not non_null:
        return False
    count = 0
    for v in non_null[:20]:
        try:
            float(str(v).replace(",", ""))
            count += 1
        except (ValueError, TypeError):
            pass
    return count / len(non_null[:20]) >= 0.7


def _is_date(values):
    """Return True if values look like date strings."""
    non_null = [str(v) for v in values if v is not None and v != ""][:5]
    for v in non_null:
        if any(c.isdigit() for c in v) and ("-" in v or "/" in v):
            return True
    return False


def _classify_columns(rows, columns):
    """
    Returns:
      numeric_cols  : list of column names with numeric data
      category_cols : list of column names with categorical data
      date_cols     : list of column names with date-like data
    """
    numeric, category, date = [], [], []
    for col in columns:
        vals = [r.get(col) for r in rows]
        if _is_date(vals):
            date.append(col)
        elif _is_numeric(vals):
            numeric.append(col)
        else:
            category.append(col)
    return numeric, category, date


# ---- Auto chart builder -----------------------------------------------------

def _to_float(v):
    try:
        return float(str(v).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _auto_chart(rows, columns):
    """
    Return a dcc.Graph (or None if no chart makes sense).
    Strategy:
      - date + numeric(s)           -> line chart
      - 1 categorical + 1+ numeric  -> horizontal bar (sorted by first numeric)
      - 2+ numeric only             -> vertical bar / grouped bar
      - single numeric scalar       -> no chart
    """
    if len(rows) <= 1:
        return None

    numeric, category, date = _classify_columns(rows, columns)

    fig = None

    # --- Line chart: date/time axis + numeric series ---
    if date and numeric:
        x_col = date[0]
        x_vals = [str(r.get(x_col, "")) for r in rows]
        fig = go.Figure()
        for i, nc in enumerate(numeric[:4]):
            y_vals = [_to_float(r.get(nc)) for r in rows]
            fig.add_trace(go.Scatter(
                x=x_vals, y=y_vals,
                mode="lines+markers",
                name=nc.replace("_", " ").title(),
                line=dict(color=_AMBER_PALETTE[i % len(_AMBER_PALETTE)], width=2),
                marker=dict(size=5),
            ))
        fig.update_layout(title="Trend over time")

    # --- Horizontal bar: one categorical label + numeric(s) ---
    elif category and numeric:
        cat_col = category[0]
        num_col = numeric[0]
        labels  = [str(r.get(cat_col, "")) for r in rows]
        values  = [_to_float(r.get(num_col)) for r in rows]

        # Sort by value descending
        pairs  = sorted(zip(values, labels), key=lambda t: (t[0] or 0), reverse=True)
        values = [p[0] for p in pairs]
        labels = [p[1] for p in pairs]

        many_cats = len(labels) > 6
        if many_cats:
            fig = go.Figure(go.Bar(
                y=labels, x=values,
                orientation="h",
                marker_color=_AMBER_PALETTE[0],
                text=[f"{v:,.1f}" if v else "" for v in values],
                textposition="outside",
            ))
            fig.update_layout(
                title=f"{num_col.replace('_', ' ').title()} by {cat_col.replace('_', ' ').title()}",
                yaxis=dict(autorange="reversed"),
                height=max(300, len(labels) * 32),
            )
        else:
            fig = go.Figure(go.Bar(
                x=labels, y=values,
                marker_color=_AMBER_PALETTE[:len(labels)],
                text=[f"{v:,.1f}" if v else "" for v in values],
                textposition="outside",
            ))
            fig.update_layout(
                title=f"{num_col.replace('_', ' ').title()} by {cat_col.replace('_', ' ').title()}",
            )

        # Overlay a second numeric series as a line if present
        if len(numeric) > 1:
            nc2    = numeric[1]
            vals2  = [_to_float(r.get(nc2)) for r in rows]
            # Re-sort in same order
            lbl_idx = {l: i for i, l in enumerate(labels)}
            vals2s  = [None] * len(labels)
            for r in rows:
                lbl = str(r.get(cat_col, ""))
                if lbl in lbl_idx:
                    vals2s[lbl_idx[lbl]] = _to_float(r.get(nc2))
            axis_key = "x" if many_cats else "y"
            fig.add_trace(go.Scatter(
                **{"y" if many_cats else "x": labels,
                   "x" if many_cats else "y": vals2s},
                mode="lines+markers",
                name=nc2.replace("_", " ").title(),
                yaxis="y2",
                line=dict(color="#7c3aed", width=2),
                marker=dict(symbol="circle", size=7),
            ))
            fig.update_layout(
                yaxis2=dict(overlaying="y", side="right", showgrid=False),
            )

    # --- Vertical bar: multiple numeric columns, no category ---
    elif len(numeric) >= 2 and not category:
        x_col   = columns[0]
        x_vals  = [str(r.get(x_col, "")) for r in rows]
        fig = go.Figure()
        for i, nc in enumerate(numeric[:4]):
            fig.add_trace(go.Bar(
                x=x_vals,
                y=[_to_float(r.get(nc)) for r in rows],
                name=nc.replace("_", " ").title(),
                marker_color=_AMBER_PALETTE[i % len(_AMBER_PALETTE)],
            ))
        fig.update_layout(barmode="group",
                          title="Metrics comparison")

    if fig is None:
        return None

    # Common layout tweaks
    fig.update_layout(
        paper_bgcolor="#ffffff",
        plot_bgcolor="#fafafa",
        font=dict(family="inherit", size=11, color="#1e293b"),
        margin=dict(l=50, r=20, t=40, b=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1),
        xaxis=dict(gridcolor="#f1f5f9"),
        yaxis=dict(gridcolor="#f1f5f9"),
        height=320,
    )

    return dcc.Graph(figure=fig, config={"displayModeBar": False},
                     style={"marginBottom": "10px"})


# ---- DataTable builder -------------------------------------------------------

def _result_datatable(rows, columns, table_id):
    """Return a dash_table.DataTable with sorting, filtering, and pagination."""
    col_defs = [{"name": c.replace("_", " ").title(), "id": c} for c in columns]
    return html.Div([
        dash_table.DataTable(
            id=table_id,
            columns=col_defs,
            data=rows,
            page_size=10,
            sort_action="native",
            filter_action="native",
            sort_mode="multi",
            style_table={"overflowX": "auto", "borderRadius": "8px",
                         "border": "1px solid #fde68a"},
            style_cell=_TABLE_STYLE_CELL,
            style_header=_TABLE_STYLE_HEADER,
            style_data_conditional=[
                {"if": {"row_index": "odd"},  **_TABLE_STYLE_DATA_ODD},
                {"if": {"row_index": "even"}, **_TABLE_STYLE_DATA_EVEN},
            ],
            style_filter={"backgroundColor": "#fffbeb", "color": "#475569",
                          "fontSize": "0.78rem"},
        ),
        html.Div(
            f"{len(rows)} total rows" + (" -- showing 10 per page" if len(rows) > 10 else ""),
            style={"color": "#94a3b8", "fontSize": "0.72rem", "marginTop": "4px",
                   "textAlign": "right"},
        ),
    ])


# ---- Chat bubble helpers -----------------------------------------------------

def _user_bubble(text):
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
    if not isinstance(content, list):
        content = [content]
    return html.Div([
        html.Span("*", style={"fontSize": "1.3rem", "marginRight": "8px",
                               "alignSelf": "flex-start", "marginTop": "2px"}),
        html.Div(content, style={
            "backgroundColor": "#fff",
            "border":          "1px solid #e2e8f0",
            "borderRadius":    "4px 16px 16px 16px",
            "padding":         "0.85rem 1.1rem",
            "flex":            "1",
            "boxShadow":       "0 1px 4px rgba(0,0,0,0.06)",
            "minWidth":        "0",
        }),
    ], style={"display": "flex", "alignItems": "flex-start", "width": "100%"})


def _sql_disclosure(sql, source):
    label = "SmartMFG Genie -- generated SQL" if source == "genie" else "Direct SQL"
    return html.Details([
        html.Summary(
            [html.Span("SQL: ", style={"color": "#d97706", "fontWeight": "600",
                                       "fontSize": "0.72rem"}),
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


def _error_bubble(message):
    return _genie_bubble(
        html.Span(f"Error: {message}",
                  style={"color": "#b91c1c", "fontSize": "0.88rem"})
    )


# ---- Layout -----------------------------------------------------------------

_WELCOME = _genie_bubble([
    html.Strong("SmartMFG Genie", style={"color": "#d97706", "fontSize": "1rem"}),
    html.Br(),
    html.Span("Ask me anything about your machines, work orders, utilization, "
              "maintenance, or farm-out costs. Results show as interactive "
              "charts and tables.",
              style={"fontSize": "0.9rem", "color": "#475569"}),
])


def layout():
    return html.Div([
        page_title(
            "SmartMFG Genie",
            "Natural language analytics -- ask questions, get visual answers",
        ),

        dbc.Row([
            # ---- Chat column -----------------------------------------------
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
                               "fontFamily": "inherit"},
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

            # ---- Sample questions panel ------------------------------------
            dbc.Col([
                html.Div([
                    html.Div([
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

                    html.Div("Tips", style={"fontWeight": "600", "color": "#1e293b",
                                            "fontSize": "0.85rem",
                                            "marginBottom": "8px"}),
                    html.Ul([
                        html.Li("Results render as charts AND tables automatically",
                                style={"marginBottom": "5px"}),
                        html.Li("Sort and filter any column in the table",
                                style={"marginBottom": "5px"}),
                        html.Li("Reference machines by ID (e.g. MCH-010)",
                                style={"marginBottom": "5px"}),
                        html.Li("Ask follow-up questions -- Genie keeps context",
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


# ---- Callback ---------------------------------------------------------------

_table_counter = [0]   # simple counter for unique DataTable IDs


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

    if triggered == "genie-clear-btn":
        return [_WELCOME], "", ""

    if isinstance(triggered, dict) and triggered.get("type") == "genie-sample-btn":
        user_text = _SAMPLE_QUESTIONS[triggered["index"]]

    if not user_text or not user_text.strip():
        return chat_history, "Please enter a question first.", ""

    question = user_text.strip()
    chat      = list(chat_history) + [_user_bubble(question)]
    ts        = datetime.now().strftime("%H:%M:%S")

    try:
        result = agent.ask_genie(question)
    except Exception as exc:
        chat.append(_error_bubble(f"Genie error: {exc}"))
        return chat, f"[{ts}] Error calling Genie", ""

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
        return chat, f"[{ts}] Genie returned {status}", ""

    parts = []

    # Prose answer (if any)
    if text:
        parts.append(html.P(text, style={"color": "#1e293b", "fontSize": "0.9rem",
                                          "marginBottom": "8px", "lineHeight": "1.5"}))

    if rows and cols:
        row_label = f"{len(rows)} row{'s' if len(rows) != 1 else ''} returned"

        # Auto chart
        chart = _auto_chart(rows, cols)
        if chart:
            parts.append(html.Div([
                html.Span("Chart", style={"fontWeight": "600", "color": "#d97706",
                                           "fontSize": "0.78rem",
                                           "textTransform": "uppercase",
                                           "letterSpacing": "0.05em",
                                           "marginBottom": "6px",
                                           "display": "block"}),
                chart,
            ]))

        # DataTable
        _table_counter[0] += 1
        tid = f"genie-tbl-{_table_counter[0]}"
        parts.append(html.Div([
            html.Span("Table  " + row_label,
                      style={"fontWeight": "600", "color": "#d97706",
                             "fontSize": "0.78rem",
                             "textTransform": "uppercase",
                             "letterSpacing": "0.05em",
                             "marginBottom": "6px",
                             "display": "block"}),
            _result_datatable(rows, cols, tid),
        ]))

    elif not text:
        parts.append(html.Span("Genie did not return any data for this question.",
                               style={"color": "#64748b", "fontSize": "0.85rem"}))

    # SQL disclosure
    if sql:
        parts.append(_sql_disclosure(sql, src))

    chat.append(_genie_bubble(parts))
    row_note = f" -- {len(rows)} rows" if rows else ""
    return chat, f"[{ts}] Answer received{row_note}", ""
