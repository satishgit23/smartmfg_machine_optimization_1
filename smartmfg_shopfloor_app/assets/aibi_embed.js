/**
 * assets/aibi_embed.js
 *
 * Initialises the @databricks/aibi-client SDK inside the Dashboard tab.
 * Runs whenever the #aibi-dashboard-container div is added to the DOM
 * (Dash renders it lazily when the Dashboard tab is activated).
 *
 * Flow:
 *   1. MutationObserver watches for #aibi-dashboard-container
 *   2. Fetches a token from /api/aibi-token (Flask route on the same origin)
 *   3. Creates DatabricksDashboard and calls initialize()
 *   4. Cleans up the observer and old instances on re-render
 */

(function () {
  "use strict";

  var INSTANCE_URL  = "https://e2-demo-field-eng.cloud.databricks.com";
  var WORKSPACE_ID  = "1444828305810485";
  var DASHBOARD_ID  = "01f11d5f0fbe1c2ebccbff405a864e8c";
  var CONTAINER_ID  = "aibi-dashboard-container";

  var _dashboardInstance = null;

  function showSpinner(container) {
    container.innerHTML =
      '<div style="display:flex;align-items:center;justify-content:center;' +
      'height:500px;color:#64748b;font-family:inherit;font-size:0.9rem;">' +
      '<div style="text-align:center;">' +
      '<div style="width:36px;height:36px;border:3px solid #e2e8f0;' +
      'border-top-color:#2563eb;border-radius:50%;animation:aibi-spin 0.8s linear infinite;' +
      'margin:0 auto 12px;"></div>' +
      'Loading SmartMFG Dashboard...' +
      '</div></div>' +
      '<style>@keyframes aibi-spin{to{transform:rotate(360deg)}}</style>';
  }

  function showError(container, msg) {
    container.innerHTML =
      '<div style="padding:2rem;background:#fef2f2;border:1px solid #fecaca;' +
      'border-radius:8px;color:#b91c1c;font-family:inherit;font-size:0.88rem;">' +
      '<strong>Dashboard error:</strong> ' + msg + '</div>';
  }

  function initDashboard(container) {
    // Destroy any previous instance (tab re-activation)
    if (_dashboardInstance) {
      try { _dashboardInstance.destroy(); } catch (e) {}
      _dashboardInstance = null;
    }

    showSpinner(container);

    fetch("/api/aibi-token")
      .then(function (r) {
        if (!r.ok) throw new Error("Token endpoint returned " + r.status);
        return r.json();
      })
      .then(function (data) {
        if (data.error) throw new Error(data.error);

        // DatabricksDashboard is exposed as a global by aibi-client.bundle.js
        var Dashboard = window.DatabricksDashboard;
        if (!Dashboard) throw new Error("DatabricksDashboard not loaded");

        container.innerHTML = ""; // clear spinner

        _dashboardInstance = new Dashboard({
          instanceUrl: INSTANCE_URL,
          workspaceId: WORKSPACE_ID,
          dashboardId: DASHBOARD_ID,
          token:       data.token,
          container:   container,
        });

        _dashboardInstance.initialize();
      })
      .catch(function (err) {
        showError(container, err.message || String(err));
      });
  }

  // Watch for the container being added/removed by Dash
  var observer = new MutationObserver(function () {
    var container = document.getElementById(CONTAINER_ID);
    if (container && !container.dataset.aibiInit) {
      container.dataset.aibiInit = "1";
      initDashboard(container);
    }
  });

  observer.observe(document.body, { childList: true, subtree: true });

  // Also try immediately if already in DOM
  window.addEventListener("DOMContentLoaded", function () {
    var container = document.getElementById(CONTAINER_ID);
    if (container && !container.dataset.aibiInit) {
      container.dataset.aibiInit = "1";
      initDashboard(container);
    }
  });

  // Re-init when Dash re-renders the tab (dataset flag gets cleared)
  window._aibiReinit = function () {
    var container = document.getElementById(CONTAINER_ID);
    if (container) {
      container.dataset.aibiInit = "1";
      initDashboard(container);
    }
  };
})();
