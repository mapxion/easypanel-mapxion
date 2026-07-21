/*
 * XProces - Telegram para usuarios
 * Modulo independiente. No modifica la logica de subida, pagos ni procesado.
 * Requiere los endpoints de usuario indicados en TELEGRAM_ENDPOINTS.
 */
(() => {
  "use strict";

  const API = String(window.API_BASE || "https://api.xproces.com").replace(/\/$/, "");
  const TELEGRAM_ENDPOINTS = Object.freeze({
    status: `${API}/telegram/status`,
    createLink: `${API}/telegram/link-code`,
    preferences: `${API}/telegram/preferences`,
    test: `${API}/telegram/test`,
    disconnect: `${API}/telegram/disconnect`
  });

  const state = {
    profileOpen: false,
    pollTimer: null,
    lastUserId: "",
    linkCode: "",
    botUsername: "XProcesBot",
    registrationRequested: false,
    status: null
  };

  function getUser() {
    try {
      const raw = localStorage.getItem("xproces_user");
      return raw ? JSON.parse(raw) : null;
    } catch (_) {
      return null;
    }
  }

  function getUserId(user = getUser()) {
    return String(user?.id || user?.user_id || "").trim();
  }

  function isRegisteredUser(user = getUser()) {
    return Boolean(user?.email && user?.auth_type !== "invite");
  }

  function requestHeaders(user = getUser()) {
    const headers = { "Content-Type": "application/json" };
    const userId = getUserId(user);
    if (userId) headers["x-user-id"] = userId;
    return headers;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  async function apiRequest(url, options = {}) {
    const user = getUser();
    const response = await fetch(url, {
      ...options,
      headers: {
        ...requestHeaders(user),
        ...(options.headers || {})
      }
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data?.ok === false) {
      const error = new Error(data?.message || data?.error || `HTTP ${response.status}`);
      error.status = response.status;
      error.data = data;
      throw error;
    }
    return data;
  }

  function injectStyles() {
    if (document.getElementById("xprocesTelegramStyles")) return;
    const style = document.createElement("style");
    style.id = "xprocesTelegramStyles";
    style.textContent = `
      .xptg-register-option{display:none;margin-top:12px;padding:13px 14px;border:1px solid rgba(42,171,238,.24);border-radius:14px;background:rgba(42,171,238,.055)}
      .xptg-register-option.show{display:block}
      .xptg-check{display:flex;align-items:flex-start;gap:10px;cursor:pointer;color:var(--text);font-size:13px;font-weight:800;line-height:1.4}
      .xptg-check input{margin-top:2px;accent-color:#229ED9}
      .xptg-register-help{margin:6px 0 0 25px;color:var(--muted);font-size:12px;line-height:1.4}
      .xptg-top-btn{color:#167faf!important;border-color:rgba(42,171,238,.30)!important;background:rgba(42,171,238,.055)!important}
      .xptg-top-btn:hover{background:rgba(42,171,238,.10)!important}
      .xptg-backdrop{position:fixed;inset:0;z-index:10050;display:none;align-items:center;justify-content:center;padding:18px;background:rgba(16,24,18,.22);backdrop-filter:blur(8px)}
      .xptg-backdrop.show{display:flex}
      .xptg-modal{width:min(100%,680px);max-height:92vh;display:flex;flex-direction:column;overflow:hidden;background:rgba(255,255,255,.98);border:1px solid var(--border);border-radius:24px;box-shadow:0 24px 70px rgba(24,33,29,.18)}
      .xptg-head{padding:20px 22px 15px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;gap:16px;align-items:flex-start}
      .xptg-title{margin:0;font-size:23px;color:var(--text)}
      .xptg-subtitle{margin-top:6px;color:var(--muted);font-size:14px;line-height:1.45}
      .xptg-close{border:1px solid var(--border);background:#fff;border-radius:12px;width:38px;height:38px;cursor:pointer;font-size:22px;color:var(--muted)}
      .xptg-body{padding:18px 22px;overflow:auto}
      .xptg-status-card{display:grid;grid-template-columns:auto 1fr;gap:13px;align-items:center;padding:15px;border:1px solid var(--border);border-radius:17px;background:#fafcfb}
      .xptg-icon{width:48px;height:48px;display:grid;place-items:center;border-radius:50%;background:rgba(42,171,238,.12);font-size:24px}
      .xptg-status-title{font-size:16px;font-weight:900;color:var(--text)}
      .xptg-status-text{margin-top:3px;font-size:13px;color:var(--muted);line-height:1.4}
      .xptg-badge{display:inline-flex;align-items:center;min-height:25px;padding:0 9px;border-radius:999px;font-size:10px;font-weight:900;text-transform:uppercase;letter-spacing:.04em;margin-top:7px}
      .xptg-badge.off{background:#f1f3f1;color:var(--muted);border:1px solid var(--border)}
      .xptg-badge.on{background:rgba(34,163,74,.08);color:var(--ok);border:1px solid rgba(34,163,74,.22)}
      .xptg-badge.wait{background:rgba(212,154,17,.08);color:var(--warning);border:1px solid rgba(212,154,17,.22)}
      .xptg-section{margin-top:15px;padding:15px;border:1px solid var(--border);border-radius:17px;background:#fff}
      .xptg-section-title{font-size:12px;font-weight:900;color:var(--muted);text-transform:uppercase;letter-spacing:.055em;margin-bottom:11px}
      .xptg-code{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:13px 14px;border-radius:14px;background:#f7faf7;border:1px dashed rgba(42,171,238,.45)}
      .xptg-code-value{font-family:Consolas,monospace;font-size:23px;font-weight:900;letter-spacing:.05em;color:#167faf;word-break:break-all}
      .xptg-copy{border:1px solid var(--border);background:#fff;border-radius:10px;padding:9px 12px;font-weight:800;cursor:pointer}
      .xptg-steps{margin:12px 0 0;padding-left:20px;color:var(--text);font-size:13px;line-height:1.7}
      .xptg-pref{display:flex;justify-content:space-between;gap:14px;align-items:center;padding:10px 0;border-bottom:1px solid #edf1ed}
      .xptg-pref:last-child{border-bottom:0}
      .xptg-pref strong{display:block;font-size:13px;color:var(--text)}
      .xptg-pref span{display:block;margin-top:2px;font-size:11px;color:var(--muted)}
      .xptg-switch{position:relative;width:44px;height:24px;flex:0 0 44px}
      .xptg-switch input{opacity:0;width:0;height:0}
      .xptg-slider{position:absolute;inset:0;border-radius:999px;background:#d8ded9;cursor:pointer;transition:.2s}
      .xptg-slider:before{content:"";position:absolute;width:18px;height:18px;left:3px;top:3px;border-radius:50%;background:#fff;box-shadow:0 2px 5px rgba(0,0,0,.18);transition:.2s}
      .xptg-switch input:checked + .xptg-slider{background:#229ED9}
      .xptg-switch input:checked + .xptg-slider:before{transform:translateX(20px)}
      .xptg-actions{display:flex;gap:9px;flex-wrap:wrap;margin-top:15px}
      .xptg-btn{border:1px solid var(--border);border-radius:13px;padding:11px 15px;background:#fff;color:var(--text);font-size:13px;font-weight:900;cursor:pointer}
      .xptg-btn.primary{color:#167faf;border-color:rgba(42,171,238,.32);background:rgba(42,171,238,.07)}
      .xptg-btn.danger{color:#9b3737;border-color:#f1d7d7;background:#fff7f7}
      .xptg-btn:disabled{opacity:.5;cursor:not-allowed}
      .xptg-message{display:none;margin-top:12px;padding:11px 12px;border-radius:12px;font-size:12px;line-height:1.45}
      .xptg-message.show{display:block}
      .xptg-message.ok{background:rgba(34,163,74,.07);border:1px solid rgba(34,163,74,.20);color:var(--ok)}
      .xptg-message.error{background:rgba(212,85,85,.07);border:1px solid rgba(212,85,85,.20);color:var(--danger)}
      .xptg-message.info{background:rgba(42,171,238,.06);border:1px solid rgba(42,171,238,.20);color:#167faf}
      .xptg-inline-cta{display:none;margin:14px 0 0;padding:13px 15px;border:1px solid rgba(42,171,238,.22);border-radius:15px;background:rgba(42,171,238,.045);align-items:center;justify-content:space-between;gap:14px}
      .xptg-inline-cta.show{display:flex}
      .xptg-inline-copy strong{display:block;color:var(--text);font-size:13px}
      .xptg-inline-copy span{display:block;color:var(--muted);font-size:12px;margin-top:3px}
      @media(max-width:650px){.xptg-head,.xptg-body{padding-left:15px;padding-right:15px}.xptg-inline-cta{align-items:flex-start;flex-direction:column}.xptg-actions .xptg-btn{width:100%}.xptg-code{align-items:flex-start;flex-direction:column}}
    `;
    document.head.appendChild(style);
  }

  function injectRegistrationOption() {
    const authBody = document.querySelector("#authBackdrop .modal-body");
    if (!authBody || document.getElementById("xptgRegisterOption")) return;
    const option = document.createElement("div");
    option.id = "xptgRegisterOption";
    option.className = "xptg-register-option";
    option.innerHTML = `
      <label class="xptg-check">
        <input type="checkbox" id="xptgRegisterCheckbox">
        <span>Quiero recibir avisos del estado de mis trabajos por Telegram</span>
      </label>
      <p class="xptg-register-help">Es opcional. Podrás conectarlo o desconectarlo más adelante desde «Mi perfil».</p>
    `;
    authBody.appendChild(option);

    const authSave = document.getElementById("authSaveBtn");
    authSave?.addEventListener("click", () => {
      const visible = option.classList.contains("show");
      const checked = document.getElementById("xptgRegisterCheckbox")?.checked;
      state.registrationRequested = Boolean(visible && checked);
      if (state.registrationRequested) sessionStorage.setItem("xproces_telegram_after_register", "1");
    }, true);
  }

  function syncRegistrationVisibility() {
    const option = document.getElementById("xptgRegisterOption");
    const title = document.getElementById("authTitle");
    if (!option || !title) return;
    const isRegister = title.textContent.trim().toLowerCase().includes("registr");
    option.classList.toggle("show", isRegister);
    if (!isRegister) {
      const checkbox = document.getElementById("xptgRegisterCheckbox");
      if (checkbox) checkbox.checked = false;
    }
  }

  function buildProfileModal() {
    if (document.getElementById("xptgBackdrop")) return;
    const backdrop = document.createElement("div");
    backdrop.id = "xptgBackdrop";
    backdrop.className = "xptg-backdrop";
    backdrop.innerHTML = `
      <div class="xptg-modal" role="dialog" aria-modal="true" aria-labelledby="xptgTitle">
        <div class="xptg-head">
          <div>
            <h2 class="xptg-title" id="xptgTitle">Mi perfil y notificaciones</h2>
            <div class="xptg-subtitle" id="xptgSubtitle">Gestiona los avisos automáticos de tus trabajos.</div>
          </div>
          <button class="xptg-close" id="xptgClose" type="button" aria-label="Cerrar">×</button>
        </div>
        <div class="xptg-body" id="xptgBody"></div>
      </div>
    `;
    document.body.appendChild(backdrop);
    backdrop.addEventListener("click", (event) => {
      if (event.target === backdrop) closeProfile();
    });
    document.getElementById("xptgClose")?.addEventListener("click", closeProfile);
  }

  function messageBox(type, text) {
    return `<div class="xptg-message show ${type}">${escapeHtml(text)}</div>`;
  }

  function preferenceRows(preferences = {}, disabled = false) {
    const items = [
      ["job_received", "Trabajo recibido", "Confirmación al registrar y pagar el trabajo"],
      ["processing_started", "Procesado iniciado", "Aviso cuando el equipo comience a procesarlo"],
      ["processing_finished", "Procesado finalizado", "Aviso cuando los resultados estén preparados"],
      ["processing_error", "Errores del procesado", "Aviso inmediato si el trabajo necesita revisión"],
      ["payment_received", "Pago recibido", "Confirmación del pago asociado al trabajo"]
    ];
    return items.map(([key, title, text]) => `
      <div class="xptg-pref">
        <div><strong>${title}</strong><span>${text}</span></div>
        <label class="xptg-switch">
          <input type="checkbox" data-xptg-pref="${key}" ${preferences[key] !== false ? "checked" : ""} ${disabled ? "disabled" : ""}>
          <span class="xptg-slider"></span>
        </label>
      </div>
    `).join("");
  }

  function renderLoading() {
    const body = document.getElementById("xptgBody");
    if (!body) return;
    body.innerHTML = `
      <div class="xptg-status-card">
        <div class="xptg-icon">✈️</div>
        <div><div class="xptg-status-title">Comprobando Telegram…</div><div class="xptg-status-text">Estamos consultando el estado de tu cuenta.</div><span class="xptg-badge wait">Cargando</span></div>
      </div>`;
  }

  function renderUnavailable(error) {
    const body = document.getElementById("xptgBody");
    if (!body) return;
    const message = error?.status === 404
      ? "La parte de Telegram todavía no está activa en el servidor. La web ya está preparada y se habilitará al desplegar el backend correspondiente."
      : "No se pudo consultar Telegram en este momento. Inténtalo de nuevo dentro de unos segundos.";
    body.innerHTML = `
      <div class="xptg-status-card">
        <div class="xptg-icon">✈️</div>
        <div><div class="xptg-status-title">Telegram temporalmente no disponible</div><div class="xptg-status-text">${escapeHtml(message)}</div><span class="xptg-badge off">No disponible</span></div>
      </div>
      <div class="xptg-actions"><button class="xptg-btn primary" id="xptgRetry" type="button">Volver a comprobar</button></div>`;
    document.getElementById("xptgRetry")?.addEventListener("click", loadStatus);
  }

  function renderDisconnected(data = {}) {
    const body = document.getElementById("xptgBody");
    if (!body) return;
    body.innerHTML = `
      <div class="xptg-status-card">
        <div class="xptg-icon">✈️</div>
        <div><div class="xptg-status-title">Telegram no conectado</div><div class="xptg-status-text">Conéctalo una sola vez y recibirás avisos aunque cierres la web.</div><span class="xptg-badge off">No conectado</span></div>
      </div>
      <div class="xptg-section">
        <div class="xptg-section-title">Avisos disponibles</div>
        ${preferenceRows(data.preferences || {}, true)}
      </div>
      <div class="xptg-actions"><button class="xptg-btn primary" id="xptgConnect" type="button">Conectar Telegram</button></div>
      <div id="xptgFeedback"></div>`;
    document.getElementById("xptgConnect")?.addEventListener("click", createLinkCode);
  }

  function renderPending(data = {}) {
    const body = document.getElementById("xptgBody");
    if (!body) return;
    state.linkCode = String(data.link_code || data.code || state.linkCode || "");
    state.botUsername = String(data.bot_username || state.botUsername || "XProcesBot").replace(/^@/, "");
    const command = `/vincular ${state.linkCode}`;
    const botUrl = `https://t.me/${encodeURIComponent(state.botUsername)}`;
    body.innerHTML = `
      <div class="xptg-status-card">
        <div class="xptg-icon">✈️</div>
        <div><div class="xptg-status-title">Esperando la conexión</div><div class="xptg-status-text">Completa estos pasos en Telegram. La web comprobará automáticamente la vinculación.</div><span class="xptg-badge wait">Pendiente</span></div>
      </div>
      <div class="xptg-section">
        <div class="xptg-section-title">Código temporal</div>
        <div class="xptg-code"><div class="xptg-code-value">${escapeHtml(state.linkCode || "Generando…")}</div><button class="xptg-copy" id="xptgCopy" type="button">Copiar</button></div>
        <ol class="xptg-steps">
          <li>Abre <a href="${botUrl}" target="_blank" rel="noopener">@${escapeHtml(state.botUsername)}</a> en Telegram.</li>
          <li>Envía el comando <strong>${escapeHtml(command)}</strong>.</li>
          <li>Espera la confirmación del bot. Esta ventana se actualizará automáticamente.</li>
        </ol>
      </div>
      <div class="xptg-actions">
        <a class="xptg-btn primary" href="${botUrl}" target="_blank" rel="noopener" style="text-decoration:none">Abrir Telegram</a>
        <button class="xptg-btn" id="xptgCheckNow" type="button">Comprobar ahora</button>
        <button class="xptg-btn" id="xptgNewCode" type="button">Generar otro código</button>
      </div>
      <div id="xptgFeedback">${messageBox("info", "El código es temporal y solo sirve para vincular esta cuenta.")}</div>`;
    document.getElementById("xptgCopy")?.addEventListener("click", async () => {
      await navigator.clipboard?.writeText(command).catch(() => {});
      showFeedback("ok", "Comando copiado.");
    });
    document.getElementById("xptgCheckNow")?.addEventListener("click", loadStatus);
    document.getElementById("xptgNewCode")?.addEventListener("click", createLinkCode);
    startStatusPolling();
  }

  function renderConnected(data = {}) {
    stopStatusPolling();
    const body = document.getElementById("xptgBody");
    if (!body) return;
    const username = data.telegram_username ? `@${String(data.telegram_username).replace(/^@/, "")}` : "Cuenta de Telegram vinculada";
    body.innerHTML = `
      <div class="xptg-status-card">
        <div class="xptg-icon">✈️</div>
        <div><div class="xptg-status-title">Telegram conectado</div><div class="xptg-status-text">${escapeHtml(username)}. Los avisos seleccionados se enviarán automáticamente.</div><span class="xptg-badge on">Conectado</span></div>
      </div>
      <div class="xptg-section">
        <div class="xptg-section-title">Notificaciones</div>
        ${preferenceRows(data.preferences || {})}
      </div>
      <div class="xptg-actions">
        <button class="xptg-btn primary" id="xptgSavePrefs" type="button">Guardar preferencias</button>
        <button class="xptg-btn" id="xptgTest" type="button">Enviar mensaje de prueba</button>
        <button class="xptg-btn danger" id="xptgDisconnect" type="button">Desconectar</button>
      </div>
      <div id="xptgFeedback"></div>`;
    document.getElementById("xptgSavePrefs")?.addEventListener("click", savePreferences);
    document.getElementById("xptgTest")?.addEventListener("click", sendTest);
    document.getElementById("xptgDisconnect")?.addEventListener("click", disconnect);
  }

  function showFeedback(type, text) {
    const feedback = document.getElementById("xptgFeedback");
    if (feedback) feedback.innerHTML = messageBox(type, text);
  }

  async function loadStatus({ silent = false } = {}) {
    const user = getUser();
    if (!isRegisteredUser(user) || !getUserId(user)) {
      renderDisconnected();
      showFeedback("info", "Inicia sesión con una cuenta registrada para conectar Telegram.");
      return;
    }
    if (!silent) renderLoading();
    try {
      const params = new URLSearchParams({ user_id: getUserId(user) });
      const data = await apiRequest(`${TELEGRAM_ENDPOINTS.status}?${params.toString()}`);
      state.status = data;
      if (data.connected) renderConnected(data);
      else if (data.pending || data.link_code || data.code) renderPending(data);
      else renderDisconnected(data);
      updateInlineCta(data.connected === true);
    } catch (error) {
      if (!silent) renderUnavailable(error);
    }
  }

  async function createLinkCode() {
    const user = getUser();
    if (!getUserId(user)) {
      showFeedback("error", "No se encontró el identificador de usuario. Cierra sesión y vuelve a entrar.");
      return;
    }
    showFeedback("info", "Generando código de vinculación…");
    try {
      const data = await apiRequest(TELEGRAM_ENDPOINTS.createLink, {
        method: "POST",
        body: JSON.stringify({ user_id: getUserId(user) })
      });
      renderPending(data);
    } catch (error) {
      showFeedback("error", error?.message || "No se pudo generar el código.");
    }
  }

  function collectPreferences() {
    const result = {};
    document.querySelectorAll("[data-xptg-pref]").forEach((input) => {
      result[input.dataset.xptgPref] = Boolean(input.checked);
    });
    return result;
  }

  async function savePreferences() {
    const user = getUser();
    try {
      await apiRequest(TELEGRAM_ENDPOINTS.preferences, {
        method: "POST",
        body: JSON.stringify({ user_id: getUserId(user), preferences: collectPreferences() })
      });
      showFeedback("ok", "Preferencias guardadas correctamente.");
    } catch (error) {
      showFeedback("error", error?.message || "No se pudieron guardar las preferencias.");
    }
  }

  async function sendTest() {
    const user = getUser();
    showFeedback("info", "Enviando mensaje de prueba…");
    try {
      await apiRequest(TELEGRAM_ENDPOINTS.test, {
        method: "POST",
        body: JSON.stringify({ user_id: getUserId(user) })
      });
      showFeedback("ok", "Mensaje de prueba enviado a Telegram.");
    } catch (error) {
      showFeedback("error", error?.message || "No se pudo enviar el mensaje.");
    }
  }

  async function disconnect() {
    if (!window.confirm("¿Desconectar Telegram de tu cuenta XProces?")) return;
    const user = getUser();
    try {
      await apiRequest(TELEGRAM_ENDPOINTS.disconnect, {
        method: "POST",
        body: JSON.stringify({ user_id: getUserId(user) })
      });
      renderDisconnected();
      updateInlineCta(false);
      showFeedback("ok", "Telegram se ha desconectado.");
    } catch (error) {
      showFeedback("error", error?.message || "No se pudo desconectar Telegram.");
    }
  }

  function startStatusPolling() {
    stopStatusPolling();
    state.pollTimer = window.setInterval(() => {
      if (state.profileOpen) loadStatus({ silent: true });
    }, 4000);
  }

  function stopStatusPolling() {
    if (state.pollTimer) {
      clearInterval(state.pollTimer);
      state.pollTimer = null;
    }
  }

  function openProfile() {
    const user = getUser();
    if (!isRegisteredUser(user)) return;
    buildProfileModal();
    state.profileOpen = true;
    document.getElementById("xptgBackdrop")?.classList.add("show");
    document.body.style.overflow = "hidden";
    const subtitle = document.getElementById("xptgSubtitle");
    if (subtitle) subtitle.textContent = `${user.name || "Usuario"} · ${user.email}`;
    loadStatus();
  }

  function closeProfile() {
    state.profileOpen = false;
    stopStatusPolling();
    document.getElementById("xptgBackdrop")?.classList.remove("show");
    document.body.style.overflow = "";
  }

  function injectProfileButton() {
    const actions = document.getElementById("topbarActions");
    const user = getUser();
    if (!actions) return;
    const existing = document.getElementById("xptgProfileBtn");
    if (!isRegisteredUser(user)) {
      existing?.remove();
      return;
    }
    if (existing) return;
    const logout = document.getElementById("logoutBtn");
    if (!logout) return;
    const button = document.createElement("button");
    button.id = "xptgProfileBtn";
    button.type = "button";
    button.className = "btn btn-secondary xptg-top-btn";
    button.textContent = "Mi perfil";
    button.addEventListener("click", openProfile);
    actions.insertBefore(button, logout);
  }

  function injectInlineCta() {
    if (document.getElementById("xptgInlineCta")) return;
    const statusBox = document.querySelector(".xproces-live-card, #statusPanel, .job-status-box");
    if (!statusBox) return;
    const cta = document.createElement("div");
    cta.id = "xptgInlineCta";
    cta.className = "xptg-inline-cta";
    cta.innerHTML = `
      <div class="xptg-inline-copy"><strong>📲 Recibe un aviso cuando termine</strong><span>Puedes cerrar la web y seguir el trabajo desde Telegram.</span></div>
      <button class="xptg-btn primary" id="xptgInlineButton" type="button">Activar Telegram</button>`;
    statusBox.appendChild(cta);
    document.getElementById("xptgInlineButton")?.addEventListener("click", openProfile);
  }

  function updateInlineCta(connected = false) {
    const cta = document.getElementById("xptgInlineCta");
    const user = getUser();
    if (!cta) return;
    cta.classList.toggle("show", isRegisteredUser(user) && !connected);
  }

  function monitorUi() {
    const observer = new MutationObserver(() => {
      injectRegistrationOption();
      syncRegistrationVisibility();
      injectProfileButton();
      injectInlineCta();
    });
    observer.observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ["class"] });
  }

  function watchUserChanges() {
    window.setInterval(() => {
      const user = getUser();
      const userId = getUserId(user);
      if (userId !== state.lastUserId) {
        state.lastUserId = userId;
        injectProfileButton();
        updateInlineCta(false);
        if (userId && sessionStorage.getItem("xproces_telegram_after_register") === "1") {
          sessionStorage.removeItem("xproces_telegram_after_register");
          window.setTimeout(openProfile, 350);
        }
      }
    }, 500);
  }

  function init() {
    injectStyles();
    injectRegistrationOption();
    syncRegistrationVisibility();
    buildProfileModal();
    injectProfileButton();
    injectInlineCta();
    monitorUi();
    watchUserChanges();
    state.lastUserId = getUserId();
    window.XProcesTelegram = Object.freeze({ openProfile, closeProfile, loadStatus });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();

/* XProces - acceso visible a Comunidad Telegram */
(() => {
  "use strict";
  const COMMUNITY_URL = "https://t.me/+9UL6gcS_wys2NmIx";

  function injectCommunityButton() {
    const actions = document.getElementById("topbarActions");
    if (!actions || document.getElementById("xptgCommunityBtn")) return;

    const button = document.createElement("a");
    button.id = "xptgCommunityBtn";
    button.className = "btn btn-secondary xptg-top-btn";
    button.href = COMMUNITY_URL;
    button.target = "_blank";
    button.rel = "noopener noreferrer";
    button.textContent = "Comunidad";
    button.title = "Comunidad XProces: ayuda, sugerencias, drones, toma de datos y procesado";

    const firstButton = actions.querySelector("button, a");
    if (firstButton) actions.insertBefore(button, firstButton);
    else actions.appendChild(button);
  }

  function injectCommunityCard() {
    if (document.getElementById("xptgCommunityCard")) return;
    const app = document.querySelector(".app");
    const topbar = document.querySelector(".topbar");
    if (!app || !topbar) return;

    const card = document.createElement("section");
    card.id = "xptgCommunityCard";
    card.style.cssText = "display:none;background:#fff;border:1px solid #dde5df;border-radius:18px;padding:16px 18px;box-shadow:0 10px 30px rgba(24,33,29,.08);";
    card.innerHTML = `
      <div style="display:flex;gap:14px;align-items:center;justify-content:space-between;flex-wrap:wrap">
        <div style="min-width:240px;flex:1">
          <div style="font-size:17px;font-weight:900;color:#18211d;margin-bottom:5px">Comunidad XProces</div>
          <div style="font-size:13px;line-height:1.45;color:#6f7d75">Ayuda entre usuarios y administrador, sugerencias, drones, toma de datos, fotogrametría, nubes de puntos, ortofotos, DSM, DTM y modelos 3D.</div>
        </div>
        <a href="${COMMUNITY_URL}" target="_blank" rel="noopener noreferrer" class="btn btn-primary" style="text-decoration:none;white-space:nowrap">Unirme a la comunidad</a>
      </div>`;
    topbar.insertAdjacentElement("afterend", card);
  }

  function ensureCommunity() {
    injectCommunityButton();
    injectCommunityCard();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", ensureCommunity, { once: true });
  } else {
    ensureCommunity();
  }

  new MutationObserver(ensureCommunity).observe(document.documentElement, { childList: true, subtree: true });
})();
