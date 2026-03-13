"""
server.py — App and Backend singletons.
Imported by app.py and all page modules to avoid circular imports.
"""

import dash
import dash_bootstrap_components as dbc
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
