(() => {
  var __getOwnPropNames = Object.getOwnPropertyNames;
  var __esm = (fn, res) => function __init() {
    return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
  };
  var __commonJS = (cb, mod) => function __require() {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  };

  // node_modules/@databricks/aibi-client/dist/index.js
  function buildErrorDisplay(rawError) {
    const error = wrapError(rawError);
    const iframe = document.createElement("iframe");
    iframe.srcdoc = ERROR_TEMPLATE_HTML;
    iframe.style.width = "100%";
    iframe.style.height = "100%";
    iframe.style.border = "none";
    iframe.onload = () => {
      const iframeDocument = iframe.contentDocument;
      if (!iframeDocument) {
        return;
      }
      const errorTitle = iframeDocument.querySelector(ERROR_TITLE_SELECTOR);
      if (errorTitle) {
        errorTitle.textContent = error.name;
      }
      const errorStack = iframeDocument.querySelector(ERROR_STACK_SELECTOR);
      if (errorStack && error.stack) {
        errorStack.textContent = error.stack;
      }
    };
    return iframe;
  }
  function wrapError(rawError) {
    if (rawError instanceof Error) {
      return rawError;
    }
    if (typeof rawError === "string") {
      return new Error(rawError);
    }
    return new Error(JSON.stringify(rawError));
  }
  var ErrorCode, DatabricksDashboardError, ERROR_TITLE_CLASSNAME, ERROR_STACK_CLASSNAME, ERROR_TITLE_SELECTOR, ERROR_STACK_SELECTOR, ERROR_TEMPLATE_HTML, FIVE_MIN_MS, WORKSPACE_URL_PARAM, TOKEN_HASH_PARAM, CURRENT_CONFIG_VERSION, DatabricksDashboard;
  var init_dist = __esm({
    "node_modules/@databricks/aibi-client/dist/index.js"() {
      ErrorCode = /* @__PURE__ */ (function(ErrorCode2) {
        ErrorCode2["INSTANCE_DESTROYED"] = "INSTANCE_DESTROYED";
        ErrorCode2["INVALID_TOKEN_FORMAT"] = "INVALID_TOKEN_FORMAT";
        ErrorCode2["IFRAME_NOT_LOADED"] = "IFRAME_NOT_LOADED";
        ErrorCode2["IFRAME_NOT_LOADED_TOKEN_EXPIRED"] = "IFRAME_NOT_LOADED_TOKEN_EXPIRED";
        ErrorCode2["TOKEN_EXPIRED_NO_REFRESH_METHOD"] = "TOKEN_EXPIRED_NO_REFRESH_METHOD";
        ErrorCode2["INVALID_CONFIG_VERSION"] = "INVALID_CONFIG_VERSION";
        return ErrorCode2;
      })({});
      DatabricksDashboardError = class _DatabricksDashboardError extends Error {
        errorCode;
        constructor(errorCode) {
          const message = _DatabricksDashboardError.getMessage(errorCode);
          super(message);
          this.errorCode = errorCode;
          this.name = "DatabricksDashboardError";
        }
        static getMessage(errorCode) {
          switch (errorCode) {
            case "INSTANCE_DESTROYED":
              return "The DatabricksDashboard instance has already been destroyed.";
            case "INVALID_TOKEN_FORMAT":
              return "The authentication token is incomplete or incorrectly formatted. It should include at least two parts separated by a dot (.).";
            case "IFRAME_NOT_LOADED":
              return "The iframe has not been fully initialized. Please wait for the dashboard to load before attempting to navigate.";
            case "IFRAME_NOT_LOADED_TOKEN_EXPIRED":
              return "The iframe has not been fully initialized, but the token is about to expire. Please refresh the page. If the issue persists, contact the page owner to check the configuration.";
            case "TOKEN_EXPIRED_NO_REFRESH_METHOD":
              return "The token is about to expire. Refresh the page to keep viewing.";
            case "INVALID_CONFIG_VERSION":
              return "The configuration version is invalid. Please check the docs to ensure you are using the correct version.";
            default:
              return "An unknown error occurred.";
          }
        }
      };
      ERROR_TITLE_CLASSNAME = "error-title";
      ERROR_STACK_CLASSNAME = "error-stack";
      ERROR_TITLE_SELECTOR = `.${ERROR_TITLE_CLASSNAME}`;
      ERROR_STACK_SELECTOR = `.${ERROR_STACK_CLASSNAME}`;
      ERROR_TEMPLATE_HTML = `<!DOCTYPE html>
    <html lang='en'>
    <head>
      <meta charset='UTF-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1.0'>
      <title>Dashboard Error</title>
      <style>
        .error {
          color: red;
          font-size: 13px;
          font-weight: bold;
          margin: 8px;
        }
        .error-title {
          font-weight: bold;
        }
      </style>
    </head>
    <body>
      <div class='error'>
      <h3 class="${ERROR_TITLE_CLASSNAME}"></h3>
      <pre class="${ERROR_STACK_CLASSNAME}"></pre></div>
    </body>
    </html>`;
      FIVE_MIN_MS = 5 * 60 * 1e3;
      WORKSPACE_URL_PARAM = "o";
      TOKEN_HASH_PARAM = "token";
      CURRENT_CONFIG_VERSION = 1;
      DatabricksDashboard = class {
        container;
        dashboardId;
        instanceUrl;
        workspaceId;
        pageId;
        getNewToken;
        iframe;
        isDestroyed = false;
        refreshTimeoutId;
        initialToken;
        config;
        // #region Iframe Loading
        isIframeLoaded = false;
        iframeLoadedPromiseResolver;
        isIframeLoadedPromise = new Promise((resolve) => {
          this.iframeLoadedPromiseResolver = resolve;
        });
        // #endregion Iframe Loading
        colorScheme;
        constructor({ workspaceId, instanceUrl, container, dashboardId, pageId, token, getNewToken, colorScheme, config }) {
          this.workspaceId = workspaceId;
          this.instanceUrl = instanceUrl;
          this.container = container;
          this.dashboardId = dashboardId;
          this.pageId = pageId;
          this.initialToken = token;
          this.getNewToken = getNewToken;
          this.colorScheme = colorScheme;
          this.config = this.migrateConfig(config);
          this.iframe = document.createElement("iframe");
          this.iframe.style.width = "100%";
          this.iframe.style.height = "100%";
          this.iframe.style.border = "none";
        }
        // #region public
        initialize() {
          if (this.isDestroyed) {
            throw new DatabricksDashboardError(ErrorCode.INSTANCE_DESTROYED);
          }
          try {
            this.initializeIframe(this.initialToken);
            if (this.initialToken) {
              this.initializeRefreshTokenTimer(this.initialToken);
            }
            this.initializeConfig();
          } catch (error) {
            this.displayError(error);
          }
        }
        destroy() {
          if (this.isDestroyed) {
            throw new DatabricksDashboardError(ErrorCode.INSTANCE_DESTROYED);
          }
          if (this.refreshTimeoutId) {
            window.clearTimeout(this.refreshTimeoutId);
          }
          window.removeEventListener("message", this.handleMessages);
          this.container.innerHTML = "";
          this.isDestroyed = true;
        }
        /**
        * Navigate to a different dashboard or page within the embedded iframe without reloading.
        * This provides a smooth transition between dashboards.
        *
        * @param options - Navigation options including dashboardId and optional pageId
        * @throws {DatabricksDashboardError} If the instance is destroyed or iframe is not loaded
        *
        * @example
        * ```typescript
        * // Navigate to a different dashboard
        * dashboard.navigate({ dashboardId: 'xyz789' });
        *
        * // Navigate to a specific page in a dashboard
        * dashboard.navigate({ dashboardId: 'xyz789', pageId: 'page123' });
        * ```
        */
        async navigate(options) {
          if (this.isDestroyed) {
            throw new DatabricksDashboardError(ErrorCode.INSTANCE_DESTROYED);
          }
          if (!this.isIframeLoaded) {
            throw new DatabricksDashboardError(ErrorCode.IFRAME_NOT_LOADED);
          }
          this.iframe.contentWindow?.postMessage({
            type: "DATABRICKS_NAVIGATE",
            dashboardId: options.dashboardId,
            pageId: options.pageId
          }, this.instanceUrl);
        }
        // #endregion public
        // #region Initialization
        initializeIframe(token) {
          const url = new URL(this.instanceUrl);
          const path = `/embed/dashboardsv3/${this.dashboardId}${this.pageId ? `/pages/${this.pageId}` : ""}`;
          url.pathname = path;
          url.searchParams.append(WORKSPACE_URL_PARAM, this.workspaceId);
          if (token) {
            url.hash = `#${TOKEN_HASH_PARAM}=${token}`;
          }
          window.addEventListener("message", this.handleMessages);
          this.iframe.src = url.href;
          if (this.colorScheme) {
            this.iframe.style.colorScheme = this.colorScheme;
          }
          this.container.appendChild(this.iframe);
        }
        initializeRefreshTokenTimer(token) {
          const getNewToken = this.getNewToken;
          if (!getNewToken) {
            return;
          }
          try {
            const rawTokenPayload = token.split(".")[1];
            if (!rawTokenPayload) {
              throw new DatabricksDashboardError(ErrorCode.INVALID_TOKEN_FORMAT);
            }
            const tokenPayload = JSON.parse(atob(rawTokenPayload));
            const expirationTimestampMs = tokenPayload.exp * 1e3;
            const timeLeftMs = expirationTimestampMs - Date.now();
            const refreshIntervalMs = Math.max(0, timeLeftMs - FIVE_MIN_MS);
            const executeRefresh = async () => {
              try {
                if (!getNewToken) {
                  throw new DatabricksDashboardError(ErrorCode.TOKEN_EXPIRED_NO_REFRESH_METHOD);
                }
                const newToken = await getNewToken();
                if (this.isDestroyed) return;
                if (this.isIframeLoaded) {
                  this.iframe.contentWindow?.postMessage({
                    type: "DATABRICKS_SET_TOKEN",
                    token: newToken
                  }, this.instanceUrl);
                } else {
                  throw new DatabricksDashboardError(ErrorCode.IFRAME_NOT_LOADED_TOKEN_EXPIRED);
                }
                this.initializeRefreshTokenTimer(newToken);
              } catch (error) {
                this.displayError(error);
              }
            };
            this.refreshTimeoutId = window.setTimeout(executeRefresh, refreshIntervalMs);
          } catch (error) {
            this.displayError(error);
          }
        }
        async initializeConfig() {
          await this.isIframeLoadedPromise;
          this.iframe.contentWindow?.postMessage({
            type: "DATABRICKS_SET_CONFIG",
            config: this.config
          }, this.instanceUrl);
        }
        // #endregion Initialization
        displayError(error) {
          const errorIframe = buildErrorDisplay(error);
          this.container.innerHTML = "";
          this.container.appendChild(errorIframe);
        }
        // Note: we use an arrow function here to bind `this` to the instance of the
        // class This is necessary because the event listener is called in the context
        // of the window object, not the instance of the class.
        handleMessages = (event) => {
          if (new URL(event.origin).origin !== new URL(this.instanceUrl).origin) {
            return;
          }
          if (event.data.type === "DATABRICKS_EMBED_READY") {
            this.isIframeLoaded = true;
            this.iframeLoadedPromiseResolver?.();
          }
        };
        /**
        * Validates and migrates the configuration to the latest version. Provides sensible defaults where possible.
        *
        * @param config - The configuration to migrate.
        * @returns The migrated configuration, or throws an error if the configuration is invalid.
        */
        migrateConfig(config) {
          try {
            if (!config) {
              return {
                version: CURRENT_CONFIG_VERSION,
                hideRefreshButton: false,
                hideDatabricksLogo: false
              };
            }
            if (config.version === 1) {
              return {
                version: CURRENT_CONFIG_VERSION,
                // We intentionally do not honor the hide refresh button configuration today
                hideRefreshButton: false,
                hideDatabricksLogo: Boolean(config.hideDatabricksLogo)
              };
            }
            throw new DatabricksDashboardError(ErrorCode.INVALID_CONFIG_VERSION);
          } catch (error) {
            this.displayError(error);
            throw error;
          }
        }
      };
    }
  });

  // entry.js
  var require_entry = __commonJS({
    "entry.js"() {
      init_dist();
      window.DatabricksDashboard = DatabricksDashboard;
    }
  });
  require_entry();
})();
