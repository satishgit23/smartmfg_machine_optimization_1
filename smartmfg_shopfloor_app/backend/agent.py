"""
backend/agent.py — Machine Recovery Agent

Accepts natural language reports of machine failures, uses the SmartMFG
Genie Space (via the Genie Conversation API) to find affected work orders
and available replacement machines, then executes SQL UPDATEs to reassign
all active work orders to the chosen machine.

Genie Space ID: 01f11d7c958210c893bc6b2289a35847
"""

import re
import time
import requests

GENIE_SPACE_ID = "01f11d7c958210c893bc6b2289a35847"
CATALOG        = "satsen_catalog"
SCHEMA         = "smartmfg_machine_optimization_1"
FQN            = f"{CATALOG}.{SCHEMA}"
_POLL_INTERVAL = 2   # seconds between status polls
_POLL_TIMEOUT  = 60  # max seconds to wait for Genie response


class MachineRecoveryAgent:
    """Orchestrates machine-failure triage using Genie + SQL."""

    def __init__(self, backend):
        self._backend = backend
        self._cfg     = backend._cfg
        self._host    = self._cfg.host.rstrip("/")

    # ── Auth ───────────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        """Return Bearer auth headers for REST API calls."""
        auth = self._cfg.authenticate()
        return {**auth, "Content-Type": "application/json"}

    # ── Genie Conversation API ─────────────────────────────────────────────

    def ask_genie(self, question: str) -> dict:
        """
        Ask the SmartMFG Genie a question and wait for the answer.
        Returns:
          {
            "status":  "COMPLETED" | "FAILED" | ...,
            "sql":     str,          # SQL Genie generated
            "columns": list[str],
            "rows":    list[dict],   # list of {col: value, ...}
            "text":    str,          # prose answer if any
            "source":  "genie"
          }
        """
        h = self._headers()

        # Start a new conversation
        r = requests.post(
            f"{self._host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
            headers=h,
            json={"content": question},
            timeout=15,
        )
        r.raise_for_status()
        resp_json    = r.json()
        conv_id      = resp_json.get("conversation_id") or resp_json.get("message", {}).get("conversation_id")
        msg_id       = resp_json.get("message_id")      or resp_json.get("message", {}).get("id")

        # Poll for completion
        poll_url = (
            f"{self._host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}"
            f"/conversations/{conv_id}/messages/{msg_id}"
        )
        result = {}
        elapsed = 0
        while elapsed < _POLL_TIMEOUT:
            time.sleep(_POLL_INTERVAL)
            elapsed += _POLL_INTERVAL
            result = requests.get(poll_url, headers=h, timeout=10).json()
            status = result.get("status", "")
            if status in ("COMPLETED", "FAILED", "CANCELLED",
                          "QUERY_RESULT_RECEIVED", "FILTERING_REQUIRED"):
                break

        # Parse attachments
        rows, columns, sql_used, text_response = [], [], "", ""
        for att in result.get("attachments", []):
            if att.get("query"):
                q       = att["query"]
                sql_used = q.get("query", "")
                raw      = q.get("result") or {}
                columns  = [c["name"] for c in raw.get("columns", [])]
                for row_vals in raw.get("data_typed_array", []):
                    row_dict = {}
                    for i, col in enumerate(columns):
                        val = row_vals[i] if i < len(row_vals) else None
                        if isinstance(val, dict):          # typed value
                            val = next(iter(val.values()), None)
                        row_dict[col] = val
                    rows.append(row_dict)
            elif att.get("text"):
                text_response = att["text"].get("content", "")

        return {
            "status":  result.get("status", "UNKNOWN"),
            "sql":     sql_used,
            "columns": columns,
            "rows":    rows,
            "text":    text_response,
            "source":  "genie",
        }

    # ── Machine ID extraction ──────────────────────────────────────────────

    def extract_machine_id(self, user_text: str) -> str | None:
        """
        Try to find a machine ID (MCH-NNN) in user text.
        Returns machine_id string or None if not found.
        """
        m = re.search(r'\bMCH[-\s]?(\d{3})\b', user_text.upper())
        if m:
            return f"MCH-{m.group(1)}"
        return None

    # ── Data queries ───────────────────────────────────────────────────────

    def get_machine_info(self, machine_id: str) -> dict:
        """Direct SQL: machine details."""
        df = self._backend._query(f"""
            SELECT machine_id, machine_name, machine_type, work_center, status
            FROM {FQN}.silver_machines
            WHERE machine_id = '{machine_id}' LIMIT 1
        """)
        return df.iloc[0].to_dict() if not df.empty else {}

    def find_affected_work_orders(self, machine_id: str) -> dict:
        """
        Ask Genie for active work orders on the specified machine.
        Falls back to direct SQL if Genie returns no rows.
        """
        question = (
            f"List all open and in-progress work orders for machine {machine_id}. "
            f"Show: work_order_id, part_description, order_qty, priority, due_date, status. "
            f"Order by priority and due_date."
        )
        try:
            result = self.ask_genie(question)
        except Exception as exc:
            result = {"rows": [], "sql": f"Genie error: {exc}", "source": "genie"}

        if not result.get("rows"):
            # Direct SQL fallback
            df = self._backend._query(f"""
                SELECT work_order_id, part_description, order_qty,
                       priority, due_date, status
                FROM {FQN}.silver_work_orders
                WHERE machine_id = '{machine_id}'
                  AND status IN ('Open', 'In Progress')
                ORDER BY
                  CASE priority WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
                  due_date
            """)
            result["rows"]    = df.to_dict("records")
            result["columns"] = list(df.columns)
            result["sql"]     = "(direct SQL — Genie returned no rows)"
            result["source"]  = "sql"

        return result

    def find_available_machines(self, machine_id: str) -> dict:
        """
        Ask Genie for available replacement machines in the same work center.
        Falls back to direct SQL if Genie returns no rows.
        """
        question = (
            f"Which machines are in the same work center as {machine_id}, "
            f"have Active status, and are NOT {machine_id}? "
            f"For each machine show machine_id, machine_name, work_center, status, "
            f"and how many open or in-progress work orders they currently have. "
            f"Order by number of active work orders ascending so the least-loaded is first."
        )
        try:
            result = self.ask_genie(question)
        except Exception as exc:
            result = {"rows": [], "sql": f"Genie error: {exc}", "source": "genie"}

        if not result.get("rows"):
            df = self._backend._query(f"""
                SELECT m.machine_id, m.machine_name, m.work_center, m.status,
                       COALESCE(wo.active_orders, 0) AS active_orders
                FROM {FQN}.silver_machines m
                LEFT JOIN (
                    SELECT machine_id, COUNT(*) AS active_orders
                    FROM {FQN}.silver_work_orders
                    WHERE status IN ('Open', 'In Progress')
                    GROUP BY machine_id
                ) wo ON m.machine_id = wo.machine_id
                WHERE m.work_center = (
                    SELECT work_center FROM {FQN}.silver_machines
                    WHERE machine_id = '{machine_id}'
                )
                  AND m.machine_id != '{machine_id}'
                  AND m.status = 'Active'
                ORDER BY active_orders ASC
            """)
            result["rows"]    = df.to_dict("records")
            result["columns"] = list(df.columns)
            result["sql"]     = "(direct SQL — Genie returned no rows)"
            result["source"]  = "sql"

        return result

    # ── Work order reassignment ────────────────────────────────────────────

    def count_active_orders(self, machine_id: str) -> int:
        """Count open/in-progress work orders on a machine."""
        n = self._backend._scalar(f"""
            SELECT COUNT(*) FROM {FQN}.silver_work_orders
            WHERE machine_id = '{machine_id}'
              AND status IN ('Open', 'In Progress')
        """, default=0)
        return int(n or 0)

    def reassign_work_orders(self, from_machine_id: str, to_machine_id: str) -> int:
        """
        UPDATE silver_work_orders: move all Open/In Progress orders from
        from_machine_id to to_machine_id.  Returns the count updated.
        """
        count = self.count_active_orders(from_machine_id)
        if count > 0:
            self._backend._execute(f"""
                UPDATE {FQN}.silver_work_orders
                SET machine_id = '{to_machine_id}'
                WHERE machine_id = '{from_machine_id}'
                  AND status IN ('Open', 'In Progress')
            """)
        return count
