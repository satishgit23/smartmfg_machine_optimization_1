"""
pages/genie_tab.py — Embedded SmartMFG Genie Space tab.

Renders the Genie Space in a full-height iframe.  Because the app runs on
the same Databricks domain, the user's existing browser session grants
access without a separate login.
"""

from dash import html
from components.ui import page_title

GENIE_SPACE_ID  = "01f11d7c958210c893bc6b2289a35847"
DATABRICKS_HOST = "https://e2-demo-field-eng.cloud.databricks.com"
GENIE_URL       = f"{DATABRICKS_HOST}/genie/spaces/{GENIE_SPACE_ID}"


def layout():
    return html.Div([
        page_title(
            "SmartMFG Genie",
            "Ask natural language questions about machines, work orders, "
            "utilization, maintenance, and farm-out costs",
        ),

        html.Iframe(
            src=GENIE_URL,
            style={
                "width":        "100%",
                "height":       "820px",
                "border":       "none",
                "borderRadius": "10px",
                "boxShadow":    "0 2px 12px rgba(0,0,0,0.08)",
            },
        ),
    ], style={"padding": "0 0.5rem"})
