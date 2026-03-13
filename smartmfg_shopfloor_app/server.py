"""
server.py — App and Backend singletons.
Imported by app.py and all page modules to avoid circular imports.
"""

import dash
import dash_bootstrap_components as dbc
from flask import jsonify
from backend.data  import Backend
from backend.agent import MachineRecoveryAgent

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY, dbc.icons.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="SmartMFG Shop Floor",
)

server  = app.server                         # Exposed for WSGI / gunicorn
backend = Backend()                          # Single shared SQL connection
agent   = MachineRecoveryAgent(backend)      # Machine Recovery Agent


@server.route("/api/aibi-token")
def aibi_token():
    """Return a short-lived Databricks token for the AI/BI embed SDK."""
    try:
        auth_headers = backend._cfg.authenticate()
        token = auth_headers.get("Authorization", "").removeprefix("Bearer ")
        return jsonify({"token": token})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
