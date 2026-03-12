"""
server.py — App and Backend singletons.
Imported by app.py and all page modules to avoid circular imports.
"""

import dash
import dash_bootstrap_components as dbc
from backend.data import Backend

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY, dbc.icons.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="SmartMFG Shop Floor",
)

server  = app.server   # Exposed for WSGI / gunicorn
backend = Backend()    # Single shared SQL connection
