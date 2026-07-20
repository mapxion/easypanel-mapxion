// =====================
// TELEGRAM NOTIFICATIONS
// =====================
// Variables de entorno necesarias:
// TELEGRAM_BOT_TOKEN=...
// TELEGRAM_CHAT_ID=5000251214

const TELEGRAM_API_BASE = "https://api.telegram.org";

function telegramIsConfigured() {
  return Boolean(
    String(process.env.TELEGRAM_BOT_TOKEN || "").trim() &&
    String(process.env.TELEGRAM_CHAT_ID || "").trim()
  );
}

function escapeTelegramHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function formatMoney(value, currency = "EUR") {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return `0,00 ${currency}`;

  try {
    return new Intl.NumberFormat("es-ES", {
      style: "currency",
      currency: String(currency || "EUR").toUpperCase()
    }).format(amount);
  } catch {
    return `${amount.toFixed(2).replace(".", ",")} ${currency}`;
  }
}

function formatDate(value) {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) return "Sin fecha";

  return new Intl.DateTimeFormat("es-ES", {
    timeZone: "Europe/Madrid",
    dateStyle: "short",
    timeStyle: "medium"
  }).format(date);
}

async function sendTelegramMessage(text, options = {}) {
  const token = String(process.env.TELEGRAM_BOT_TOKEN || "").trim();
  const defaultChatId = String(process.env.TELEGRAM_CHAT_ID || "").trim();
  const chatId = String(options.chatId || defaultChatId).trim();

  if (!token || !chatId) {
    console.warn("Telegram no configurado: faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID");
    return { ok: false, skipped: true, error: "telegram_not_configured" };
  }

  try {
    const response = await fetch(
      `${TELEGRAM_API_BASE}/bot${encodeURIComponent(token)}/sendMessage`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatId,
          text: String(text || ""),
          parse_mode: options.parseMode || "HTML",
          disable_web_page_preview: true,
          ...(options.replyMarkup ? { reply_markup: options.replyMarkup } : {})
        })
      }
    );

    const data = await response.json().catch(() => ({}));
    if (!response.ok || data?.ok !== true) {
      console.error("Telegram sendMessage error", response.status, data);
      return {
        ok: false,
        error: data?.description || `Telegram HTTP ${response.status}`
      };
    }

    return { ok: true, messageId: data?.result?.message_id || null };
  } catch (error) {
    console.error("Telegram sendMessage exception", error?.message || error);
    return { ok: false, error: error?.message || "telegram_send_error" };
  }
}

async function notifyPaymentReceived({ job, payment }) {
  const projectName =
    job?.project_name || job?.name || job?.project_type || "Proyecto XProces";
  const client =
    job?.email || job?.user_email || payment?.payerEmail || "Sin correo";
  const photoCount = Number(job?.photos_count || job?.photo_count || 0);
  const quality =
    job?.quality_mode ||
    job?.exif_summary?._xproces?.quality_mode ||
    "No indicada";

  const lines = [
    "💰 <b>NUEVO PAGO EN XPROCES</b>",
    "",
    `👤 <b>Cliente:</b> ${escapeTelegramHtml(client)}`,
    `📦 <b>Proyecto:</b> ${escapeTelegramHtml(projectName)}`,
    `📷 <b>Fotografías:</b> ${photoCount || "No indicadas"}`,
    `⚡ <b>Calidad:</b> ${escapeTelegramHtml(quality)}`,
    `💶 <b>Importe:</b> ${escapeTelegramHtml(formatMoney(payment?.amount, payment?.currency))}`,
    `🆔 <b>Job:</b> <code>${escapeTelegramHtml(job?.id || "Sin ID")}</code>`,
    `🧾 <b>Captura PayPal:</b> <code>${escapeTelegramHtml(payment?.captureId || "Sin ID")}</code>`,
    `🕒 <b>Fecha:</b> ${escapeTelegramHtml(formatDate(payment?.paymentDate))}`
  ];

  return sendTelegramMessage(lines.join("\n"));
}

export {
  telegramIsConfigured,
  escapeTelegramHtml,
  sendTelegramMessage,
  notifyPaymentReceived
};
