import express from "express";
import multer from "multer";
import fs from "fs";
import path from "path";
import IORedis from "ioredis";
import { Queue } from "bullmq";
import pkg from "pg";
import archiver from "archiver";
import cors from "cors";
import bcrypt from "bcryptjs";
import { randomBytes, createHmac } from "crypto";
import { telegramIsConfigured, sendTelegramMessage, notifyPaymentReceived } from "./telegram.js";


const { Pool } = pkg;

const app = express();
app.use(express.json());
app.use(cors());


// =====================
// CONFIG
// =====================
const DATA_ROOT = process.env.DATA_ROOT || "/data/mapxion";
const WORKER_TOKEN = process.env.WORKER_TOKEN || "";

// Versiones estables del sistema de tiempos. Se guardan junto a cada muestra
// para no mezclar historicos incompatibles si cambian perfiles o predictor.
const TIMING_PREDICTOR_VERSION = "stage-v5-2026-07-19";
const TIMING_METRICS_VERSION = "metrics-v2-2026-07-19";
const SHORT_STAGE_MIN_DURATION_MS = 500;
const SHORT_STAGE_EXCLUDED_FROM_TRAINING = new Set([
  "export_dem", "export_dtm", "colorize_model",
  "export_tiled_model", "export_model", "export_point_cloud",
  "export_texture", "export_reference"
]);

const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "XprocesAdmin2026!";
const TELEGRAM_BOT_USERNAME = String(process.env.TELEGRAM_BOT_USERNAME || "XProcesBot").replace(/^@/, "").trim();
const TELEGRAM_WEBHOOK_SECRET = String(process.env.TELEGRAM_WEBHOOK_SECRET || "").trim();
const TELEGRAM_LINK_CODE_TTL_MINUTES = 15;
const PRICING_QUOTE_SECRET = String(
  process.env.PRICING_QUOTE_SECRET || WORKER_TOKEN || ADMIN_TOKEN
);
const PRICING_QUOTE_TTL_SECONDS = 60 * 60;

// =====================
// PAYPAL CHECKOUT
// =====================
const PAYPAL_CLIENT_ID = String(process.env.PAYPAL_CLIENT_ID || "AXq18BXXDy0_x4k-J6RmxbKRQsRX0zELhp6ve4FpfHJL9bv8LJ869sZtJUJykj3w7cN6q_vbUYUkcUxo").trim();
const PAYPAL_CLIENT_SECRET = String(process.env.PAYPAL_CLIENT_SECRET || "EM5QNdoH-tVCW3bTjnpaxIWFQdBjKw2Xh4uqonD6dJvvwGJ6ZKG7x1z9-dIjBGA7XkTJqjJNHFjw-foJ").trim();
const PAYPAL_ENV = String(process.env.PAYPAL_ENV || "sandbox").trim().toLowerCase() === "live" ? "live" : "sandbox";
const PAYPAL_CURRENCY = "EUR";
const PAYPAL_API_BASE = PAYPAL_ENV === "live"
  ? "https://api-m.paypal.com"
  : "https://api-m.sandbox.paypal.com";

function paypalIsConfigured() {
  return !!(PAYPAL_CLIENT_ID && PAYPAL_CLIENT_SECRET);
}

async function getPayPalAccessToken() {
  if (!paypalIsConfigured()) {
    throw new Error("PayPal no está configurado");
  }

  const auth = Buffer.from(`${PAYPAL_CLIENT_ID}:${PAYPAL_CLIENT_SECRET}`).toString("base64");
  const response = await fetch(`${PAYPAL_API_BASE}/v1/oauth2/token`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: "grant_type=client_credentials"
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok || !data.access_token) {
    console.error("PayPal OAuth error", response.status, data);
    throw new Error("No se pudo autenticar con PayPal");
  }
  return data.access_token;
}

async function paypalRequest(pathname, options = {}) {
  const accessToken = await getPayPalAccessToken();
  const response = await fetch(`${PAYPAL_API_BASE}${pathname}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(data?.message || `PayPal HTTP ${response.status}`);
    error.status = response.status;
    error.paypal = data;
    throw error;
  }
  return data;
}

function requireAdmin(req, res, next) {
  const token =
    req.headers["x-admin-token"] ||
    req.headers["authorization"]?.replace(/^Bearer\s+/i, "") ||
    req.query?.admin_token ||
    req.body?.admin_token;

  if (!ADMIN_TOKEN || token !== ADMIN_TOKEN) {
    return res.status(401).json({
      ok: false,
      error: "admin_unauthorized",
      message: "Acceso administrador no autorizado"
    });
  }

  return next();
}


let jobsHasQualityMode = false;

async function refreshSchemaFlags() {
  try {
    const result = await pool.query(
      `select exists (
         select 1
         from information_schema.columns
         where table_name = 'jobs'
           and column_name = 'quality_mode'
       ) as exists`
    );
    jobsHasQualityMode = !!result.rows?.[0]?.exists;
    console.log("Schema jobs.quality_mode:", jobsHasQualityMode ? "sí" : "no");
  } catch (e) {
    jobsHasQualityMode = false;
    console.error("No se pudo comprobar jobs.quality_mode", e?.message || e);
  }
}

function normalizeQualityMode(value) {
  const raw = String(value || "").trim().toLowerCase();

  if (!raw) return "normal";
  if (["rapido", "rápido", "fast"].includes(raw)) return "fast";
  if (["normal", "standard", "estandar", "estándar"].includes(raw)) return "normal";
  if (["full", "max", "maximum", "maxima", "máxima", "maxima calidad", "máxima calidad", "maxima_calidad", "máxima_calidad", "maximum_quality", "ultra", "high", "highest", "best"].includes(raw)) return "full";

  return "normal";
}

function getQualityModeLabel(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return "Rápido";
  if (normalized === "full") return "Máxima calidad";
  return "Normal";
}

function getQualityModeTimeFactor(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return 0.65;
  if (normalized === "full") return 2.2;
  return 1;
}

function getQualityModePriceFactor(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return 0.85;
  if (normalized === "full") return 1.75;
  return 1;
}


function stripNullCharsDeep(value) {
  if (typeof value === "string") {
    return value.replace(/\u0000/g, "");
  }

  if (Array.isArray(value)) {
    return value.map(stripNullCharsDeep);
  }

  if (value && typeof value === "object") {
    const out = {};
    for (const [key, val] of Object.entries(value)) {
      out[key] = stripNullCharsDeep(val);
    }
    return out;
  }

  return value;
}



function normalizeProcessingStage(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return null;

  if (["created", "receiving", "queued", "running", "done", "failed", "cancelled", "final"].includes(raw)) return raw;
  if (["upload", "uploading", "subida", "subiendo"].includes(raw)) return "uploading";
  if (["download", "downloading", "descarga", "descargando", "validating", "validation", "validando"].includes(raw)) return "downloading";
  if (["predownload", "predownloading", "predescarga", "predescargando", "receiving_download"].includes(raw)) return "predownloading";
  if (["result_download", "download_results", "descarga_resultados"].includes(raw)) return "result_download";
  if (["prepare", "preparing", "preparando", "batch", "batch_xml"].includes(raw)) return "preparing";
  if (["import", "importing", "importando", "adding_photos", "anadiendo_fotos"].includes(raw)) return "importing";
  if (["match", "matching", "detecting", "detectando", "detectando_puntos"].includes(raw)) return "matching";
  if (["align", "aligning", "alineando", "alineacion", "alineación"].includes(raw)) return "aligning";
  if (["clean", "cleaning", "limpiando", "optimizando", "optimization", "optimizing"].includes(raw)) return "cleaning";
  if (["depth", "depthmaps", "depth_maps", "profundidad", "mapas_profundidad"].includes(raw)) return "depth_maps";
  if (["model", "modelo", "mesh", "malla"].includes(raw)) return "model";
  if (["uv", "generating_uv"].includes(raw)) return "uv";
  if (["texture", "texturing", "textura", "texturizando"].includes(raw)) return "texture";
  if (["tiled", "tiled_model", "teselas", "modelo_teselas"].includes(raw)) return "tiled_model";
  if (["pointcloud", "point_cloud", "cloud", "nube", "nube_puntos"].includes(raw)) return "point_cloud";
  if (["classify_ground", "ground_classification", "clasificando_terreno", "clasificacion_terreno"].includes(raw)) return "ground_classification";
  if (["dem", "dsm", "mde"].includes(raw)) return "dem";
  if (["dtm", "mdt", "terreno"].includes(raw)) return "dtm";
  if (["orthomosaic", "ortho", "ortofoto", "ortomosaico"].includes(raw)) return "orthomosaic";
  if (["colorize_model", "coloring_model", "coloreando_modelo"].includes(raw)) return "colorize_model";
  if (["report", "pdf", "informe"].includes(raw)) return "report";
  if (["export_model", "exportando_modelo"].includes(raw)) return "export_model";
  if (["export_point_cloud", "exportando_nube", "export_las"].includes(raw)) return "export_point_cloud";
  if (["export_dem", "exportando_mde", "exportando_dsm"].includes(raw)) return "export_dem";
  if (["export_dtm", "exportando_mdt"].includes(raw)) return "export_dtm";
  if (["export_orthomosaic", "exportando_ortofoto", "exportando_ortomosaico"].includes(raw)) return "export_orthomosaic";
  if (["export_texture", "exportando_textura"].includes(raw)) return "export_texture";
  if (["export_tiled_model", "exportando_teselas"].includes(raw)) return "export_tiled_model";
  if (["export_reference", "exportando_referencia"].includes(raw)) return "export_reference";
  if (["export", "exporting", "exportando", "exports", "exportaciones"].includes(raw)) return "exporting";
  if (["zip", "compressing", "compression", "comprimiendo", "compresion"].includes(raw)) return "zip";
  if (["closing_metashape", "closing", "cerrando_metashape", "cerrando"].includes(raw)) return "closing_metashape";
  if (["zip_upload", "uploading_zip", "subiendo_zip", "subiendo_resultados"].includes(raw)) return "zip_upload";
  if (["processing_complete", "procesado_finalizado"].includes(raw)) return "processing_complete";

  return raw.replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "") || null;
}

function inferStageFromMessage(message) {
  const text = String(message || "").toLowerCase();
  if (!text) return null;

  if (/subiendo\s+(el\s+)?zip|zip\s+final|subiendo\s+resultados|outputs\.zip/.test(text)) return "zip_upload";
  if (/cerrando\s+metashape|closing\s+metashape/.test(text)) return "closing_metashape";
  if (/comprimiendo|creando\s+zip|compresi[oó]n/.test(text)) return "zip";

  if (/export.*(tesela|tiled)/.test(text)) return "export_tiled_model";
  if (/export(model|ando\s+modelo)|exportando\s+obj|saving\s+3d\s+model/.test(text)) return "export_model";
  if (/export(pointcloud|ando\s+nube)|exportando\s+las|saving\s+point\s+cloud/.test(text)) return "export_point_cloud";
  if (/export.*(mdt|dtm)|\/mdt\.tif|\\mdt\.tif/.test(text)) return "export_dtm";
  if (/export.*(mde|dem|dsm)|\/mde(_color)?\.tif|\\mde(_color)?\.tif/.test(text)) return "export_dem";
  if (/export.*(orto|orthomosaic)|\/orto\.tif|\\orto\.tif/.test(text)) return "export_orthomosaic";
  if (/export.*textur/.test(text)) return "export_texture";
  if (/export.*referen/.test(text)) return "export_reference";

  if (/generando\s+informe|generating\s+report|exportreport|informe\s+pdf/.test(text)) return "report";
  if (/coloriz(e|ing)model|coloreando\s+modelo|colorize\s+model/.test(text)) return "colorize_model";
  if (/buildorthomosaic|generando\s+orto|generando\s+ortomosaico|building\s+orthomosaic/.test(text)) return "orthomosaic";
  if (/builddem.*classes\s*=\s*(terreno|ground)|classes\s*=\s*terreno|generando\s+mdt|generando\s+dtm/.test(text)) return "dtm";
  if (/builddem|buildelevation|generando\s+mde|generando\s+dsm|generating\s+dem/.test(text)) return "dem";
  if (/classifygroundpoints|clasificando\s+(el\s+)?terreno|clasificando\s+puntos\s+de\s+terreno|ground\s+classification/.test(text)) return "ground_classification";
  if (/buildpointcloud|builddensecloud|generando\s+nube|building\s+point\s+cloud/.test(text)) return "point_cloud";
  if (/buildtiledmodel|modelo\s+de\s+teselas|building\s+tiled\s+model/.test(text)) return "tiled_model";
  if (/buildtexture|generando\s+textura|building\s+texture/.test(text)) return "texture";
  if (/builduv|generando\s+uv|building\s+uv/.test(text)) return "uv";
  if (/buildmodel|creando\s+modelo|generando\s+modelo|building\s+model|creando\s+malla/.test(text)) return "model";
  if (/builddepthmaps|mapas\s+de\s+profundidad|building\s+depth\s+maps/.test(text)) return "depth_maps";
  if (/aligncameras|orientando\s+(fotograf[ií]as|fotos|c[aá]maras)|alineando/.test(text)) return "aligning";
  if (/matchphotos|emparejando\s+(fotograf[ií]as|fotos)|matching\s+photos/.test(text)) return "matching";
  if (/a[nñ]adiendo\s+(fotograf[ií]as|fotos)|importando\s+fotos|importing\s+photos/.test(text)) return "importing";
  if (/worker\s+claimed|confirmando\s+descarga|descarga\s+y\s+validaci[oó]n|validando\s+descarga|download\s+validation/.test(text)) return "downloading";
  if (/procesando\s+proyecto|inicio\s+procesado\s+metashape|proyecto\s+preparado|preparando\s+(xml|proyecto)|motor\s+batch|abriendo\s+metashape|proces(?:o|amiento)\s+por\s+lotes|batch\s+xml/.test(text)) return "preparing";
  if (/exportaciones\s+detectadas|exportaciones\s+completas|exportando\s+archivos/.test(text)) return "exporting";
  if (/procesado\s+finalizado|fin\s+procesado\s+metashape/.test(text)) return "processing_complete";
  if (/trabajo\s+(procesado|finalizado)\s+correctamente/.test(text)) return "final";

  return null;
}

function inferProcessingStage({ status, stage, progress, message }) {
  const direct = normalizeProcessingStage(stage);
  // El worker nuevo envía un identificador de fase estable. Cuando existe, es
  // más fiable que buscar palabras en el mensaje (por ejemplo, "ZIP final
  // generado" no significa todavía que la subida haya comenzado).
  if (direct && !["created", "receiving", "queued", "running"].includes(direct)) {
    return direct;
  }

  const fromMessage = inferStageFromMessage(message);
  if (fromMessage) return fromMessage;

  if (direct) return direct;

  const st = String(status || "").toLowerCase();
  if (["done", "failed", "cancelled", "queued", "receiving", "created"].includes(st)) return st;

  const p = Number(progress);
  if (Number.isFinite(p)) {
    if (p >= 100) return "final";
    if (p >= 85) return "zip_upload";
    if (p >= 57) return "zip";
    if (p >= 43) return "exporting";
    if (p >= 33) return "orthomosaic";
    if (p >= 32) return "dem";
    if (p >= 27) return "point_cloud";
    if (p >= 22) return "model";
    if (p >= 20) return "depth_maps";
    if (p >= 19) return "matching";
    if (p >= 13) return "importing";
    if (p >= 11) return "preparing";
  }

  return null;
}

function extractProgressSignalFromLog(rawLog) {
  const text = String(rawLog || "");
  if (!text.trim()) return null;

  const stageMatches = [...text.matchAll(/XPROCES_STAGE\|([^|\r\n]+)\|([^\r\n]*)/g)];
  const progressMatches = [...text.matchAll(/XPROCES_PROGRESS\|(\d{1,3})\|([^\r\n]*)/g)];

  const lastStage = stageMatches.length ? stageMatches[stageMatches.length - 1] : null;
  const lastProgress = progressMatches.length ? progressMatches[progressMatches.length - 1] : null;

  const message = String(lastStage?.[2] || lastProgress?.[2] || "").trim() || null;
  const stage = normalizeProcessingStage(lastStage?.[1]) || inferStageFromMessage(message) || inferStageFromMessage(text);
  const percent = lastProgress ? Number(lastProgress[1]) : null;

  if (!stage && !message && !Number.isFinite(percent)) return null;
  return {
    stage,
    message,
    percent: Number.isFinite(percent) ? Math.max(0, Math.min(100, percent)) : null
  };
}

async function recordJobStageEvent(pool, jobId, stage, eventType) {
  const normalizedStage = normalizeProcessingStage(stage);
  const normalizedType = String(eventType || "progress").trim().toLowerCase();
  if (!normalizedStage || !["start", "end", "progress"].includes(normalizedType)) return;

  try {
    // El worker puede recibir la misma linea por stdout y por el log vivo.
    // Protegemos SQL para que una fase solo pueda tener un tramo abierto.
    const openResult = await pool.query(
      `select s.id
         from job_stage_events s
        where s.job_id = $1
          and s.stage = $2
          and s.event_type = 'start'
          and not exists (
            select 1
              from job_stage_events e
             where e.job_id = s.job_id
               and e.stage = s.stage
               and e.event_type = 'end'
               and e.id > s.id
          )
        order by s.id desc
        limit 1`,
      [jobId, normalizedStage]
    );

    const hasOpenStage = openResult.rows.length > 0;

    if (normalizedType === "start" && hasOpenStage) return;
    if (normalizedType === "end" && !hasOpenStage) return;

    if (normalizedType === "progress") {
      const lastProgress = await pool.query(
        `select created_at
           from job_stage_events
          where job_id = $1
            and stage = $2
            and event_type = 'progress'
          order by id desc
          limit 1`,
        [jobId, normalizedStage]
      );
      const previous = lastProgress.rows?.[0];
      if (previous) {
        const ageMs = Date.now() - new Date(previous.created_at).getTime();
        if (Number.isFinite(ageMs) && ageMs >= 0 && ageMs < 1500) return;
      }
    }

    await pool.query(
      `insert into job_stage_events (job_id, stage, event_type, created_at)
       values ($1, $2, $3, now())`,
      [jobId, normalizedStage, normalizedType]
    );
  } catch (e) {
    console.error("job_stage_events insert error", e?.message || e);
  }
}


function mergeJsonObjects(base, extra) {
  const left = base && typeof base === "object" && !Array.isArray(base) ? base : {};
  const right = extra && typeof extra === "object" && !Array.isArray(extra) ? extra : {};
  return stripNullCharsDeep({ ...left, ...right });
}

async function readStageEventAggregate(db, jobId, stage) {
  const normalizedStage = normalizeProcessingStage(stage);
  if (!normalizedStage) return null;

  const { rows } = await db.query(
    `select id, event_type, created_at
       from job_stage_events
      where job_id = $1
        and stage = $2
        and event_type in ('start','end')
      order by id asc`,
    [jobId, normalizedStage]
  );

  let openStart = null;
  let startedAt = null;
  let finishedAt = null;
  let durationMs = 0;
  let startEvents = 0;
  let endEvents = 0;
  let completedSegments = 0;
  let unmatchedStarts = 0;
  let unmatchedEnds = 0;

  // Una misma operación puede aparecer en varios tramos válidos (por ejemplo,
  // dos exportaciones raster del mismo tipo separadas por otra tarea). Sumamos
  // los tramos completos en vez de medir desde el primer start hasta el último
  // end, porque eso incluiría fases intermedias que no pertenecen a esta etapa.
  for (const event of rows) {
    const type = String(event.event_type || "").toLowerCase();
    const eventMs = new Date(event.created_at).getTime();
    if (!Number.isFinite(eventMs)) continue;

    if (type === "start") {
      startEvents += 1;
      if (openStart === null) {
        openStart = eventMs;
        if (!startedAt) startedAt = event.created_at;
      } else {
        // Red de seguridad ante datos antiguos o una carrera excepcional.
        unmatchedStarts += 1;
      }
      continue;
    }

    if (type === "end") {
      endEvents += 1;
      if (openStart === null) {
        unmatchedEnds += 1;
        continue;
      }

      if (eventMs >= openStart) {
        durationMs += eventMs - openStart;
        completedSegments += 1;
        finishedAt = event.created_at;
      } else {
        unmatchedEnds += 1;
      }
      openStart = null;
    }
  }

  if (openStart !== null) unmatchedStarts += 1;

  return {
    start_events: startEvents,
    end_events: endEvents,
    completed_segments: completedSegments,
    unmatched_starts: unmatchedStarts,
    unmatched_ends: unmatchedEnds,
    started_at: startedAt,
    finished_at: finishedAt,
    duration_ms: completedSegments > 0 ? durationMs : null
  };
}

async function upsertJobStageMetric(db, jobId, stage, extra = {}) {
  const normalizedStage = normalizeProcessingStage(stage);
  if (!normalizedStage) return null;

  const aggregate = await readStageEventAggregate(db, jobId, normalizedStage);
  if (!aggregate?.started_at || !aggregate?.finished_at) return null;

  const { rows: jobRows } = await db.query(
    `select id, status, error, photos_count, input_total_bytes,
            avg_photo_mb, avg_width, avg_height, avg_megapixels,
            total_megapixels, point_count, processing_load_score,
            project_type, quality, outputs, exif_summary
       from jobs
      where id = $1`,
    [jobId]
  );
  if (!jobRows.length) return null;

  const job = jobRows[0];
  const xproces = job?.exif_summary?._xproces || {};
  const durationMs = Number(aggregate.duration_ms || 0);
  const startEvents = Number(aggregate.start_events || 0);
  const endEvents = Number(aggregate.end_events || 0);

  let invalidReason = null;
  if (
    startEvents < 1 ||
    endEvents < 1 ||
    startEvents !== endEvents ||
    Number(aggregate.unmatched_starts || 0) > 0 ||
    Number(aggregate.unmatched_ends || 0) > 0
  ) invalidReason = "unpaired_or_incomplete_events";
  else if (!(durationMs > 0)) invalidReason = "non_positive_duration";
  else if (durationMs > 7 * 24 * 3600 * 1000) invalidReason = "duration_out_of_range";

  const metrics = mergeJsonObjects(extra.metrics, {
    event_start_count: startEvents,
    event_end_count: endEvents,
    completed_segments: Number(aggregate.completed_segments || 0),
    unmatched_starts: Number(aggregate.unmatched_starts || 0),
    unmatched_ends: Number(aggregate.unmatched_ends || 0),
    captured_at: new Date().toISOString()
  });

  const inputBytes = Number.isFinite(Number(extra.inputBytes))
    ? Math.max(0, Math.round(Number(extra.inputBytes)))
    : Math.max(0, Number(job.input_total_bytes || 0));
  const outputBytes = Number.isFinite(Number(extra.outputBytes))
    ? Math.max(0, Math.round(Number(extra.outputBytes)))
    : null;
  const itemCount = Number.isFinite(Number(extra.itemCount))
    ? Math.max(0, Math.round(Number(extra.itemCount)))
    : Math.max(0, Number(job.photos_count || 0));

  const result = await db.query(
    `insert into job_stage_metrics (
       job_id, stage, started_at, finished_at, duration_ms,
       photos_count, input_bytes, output_bytes, item_count,
       avg_photo_mb, avg_width, avg_height, avg_megapixels,
       total_megapixels, point_count, processing_load_score,
       project_type, quality, outputs,
       profile_version, worker_version, predictor_version, metrics_version,
       start_events, end_events, valid_for_training, invalid_reason, metrics,
       created_at, updated_at
     ) values (
       $1,$2,$3,$4,$5,
       $6,$7,$8,$9,
       $10,$11,$12,$13,
       $14,$15,$16,
       $17,$18,$19::jsonb,
       $20,$21,$22,$23,
       $24,$25,false,$26,$27::jsonb,
       now(),now()
     )
     on conflict (job_id, stage) do update set
       started_at = excluded.started_at,
       finished_at = excluded.finished_at,
       duration_ms = excluded.duration_ms,
       photos_count = excluded.photos_count,
       input_bytes = coalesce(excluded.input_bytes, job_stage_metrics.input_bytes),
       output_bytes = coalesce(excluded.output_bytes, job_stage_metrics.output_bytes),
       item_count = coalesce(excluded.item_count, job_stage_metrics.item_count),
       avg_photo_mb = excluded.avg_photo_mb,
       avg_width = excluded.avg_width,
       avg_height = excluded.avg_height,
       avg_megapixels = excluded.avg_megapixels,
       total_megapixels = excluded.total_megapixels,
       point_count = coalesce(excluded.point_count, job_stage_metrics.point_count),
       processing_load_score = excluded.processing_load_score,
       project_type = excluded.project_type,
       quality = excluded.quality,
       outputs = excluded.outputs,
       profile_version = coalesce(excluded.profile_version, job_stage_metrics.profile_version),
       worker_version = coalesce(excluded.worker_version, job_stage_metrics.worker_version),
       predictor_version = excluded.predictor_version,
       metrics_version = excluded.metrics_version,
       start_events = excluded.start_events,
       end_events = excluded.end_events,
       valid_for_training = false,
       invalid_reason = excluded.invalid_reason,
       metrics = coalesce(job_stage_metrics.metrics, '{}'::jsonb) || excluded.metrics,
       updated_at = now()
     returning *`,
    [
      jobId,
      normalizedStage,
      aggregate.started_at,
      aggregate.finished_at,
      durationMs,
      Number(job.photos_count || 0),
      inputBytes,
      outputBytes,
      itemCount,
      job.avg_photo_mb,
      job.avg_width,
      job.avg_height,
      job.avg_megapixels,
      job.total_megapixels,
      job.point_count,
      job.processing_load_score,
      String(job.project_type || xproces.project_type || "fotogrametria").toLowerCase(),
      normalizeQualityMode(job.quality || xproces.quality_mode || "normal"),
      JSON.stringify(normalizeOutputList(job.outputs || xproces.outputs_requested || [])),
      extra.profileVersion || xproces.profile_version || null,
      extra.workerVersion || xproces.worker_version || null,
      TIMING_PREDICTOR_VERSION,
      TIMING_METRICS_VERSION,
      startEvents,
      endEvents,
      invalidReason,
      JSON.stringify(metrics)
    ]
  );

  return result.rows?.[0] || null;
}

async function refreshCompletedStageMetrics(db, jobId) {
  const { rows } = await db.query(
    `select distinct stage
       from job_stage_events
      where job_id = $1
        and event_type in ('start','end')`,
    [jobId]
  );

  for (const row of rows) {
    await upsertJobStageMetric(db, jobId, row.stage);
  }
}

async function finalizeJobStageMetrics(db, jobId) {
  await refreshCompletedStageMetrics(db, jobId);

  const { rows: jobRows } = await db.query(
    `select id, status, error, photos_count, quality, project_type, outputs,
            avg_width, avg_height, avg_megapixels, total_megapixels,
            processing_load_score, exif_summary
       from jobs where id = $1`,
    [jobId]
  );
  if (!jobRows.length) return;

  const job = jobRows[0];
  const done = String(job.status || "").toLowerCase() === "done";
  const features = buildJobFeatures(job);
  // Si Metashape ejecutó una fase real y quedó bien medida, esa muestra es
  // útil aunque la fase sea una dependencia interna y no una salida elegida
  // expresamente por el cliente. Los outputs quedan guardados en la propia
  // muestra y el predictor ya los usa para comparar trabajos compatibles.
  const trainableStages = new Set([
    "uploading", "predownloading", "downloading", "zip_upload",
    ...TIMING_STAGE_ORDER
  ]);

  const { rows: metricsRows } = await db.query(
    `select id, stage, duration_ms, start_events, end_events, invalid_reason,
            profile_version, worker_version
       from job_stage_metrics
      where job_id = $1`,
    [jobId]
  );

  for (const metric of metricsRows) {
    const stage = normalizeProcessingStage(metric.stage);
    let reason = metric.invalid_reason || null;
    const durationMs = Number(metric.duration_ms || 0);

    if (!done) reason = String(job.status || "job_not_done").toLowerCase();
    else if (job.error) reason = "job_has_error";
    else if (
      Number(metric.start_events || 0) < 1 ||
      Number(metric.end_events || 0) < 1 ||
      Number(metric.start_events || 0) !== Number(metric.end_events || 0)
    ) reason = "unpaired_or_incomplete_events";
    else if (!(durationMs >= 100 && durationMs <= 7 * 24 * 3600 * 1000)) reason = "duration_out_of_range";
    else if (!features.qualityMode) reason = "missing_quality";
    else if (features.photosCount <= 0 && !["zip_upload"].includes(stage)) reason = "missing_photo_count";
    else if (!trainableStages.has(stage) && !["final", "running"].includes(stage)) reason = "unknown_stage";
    else if (!isTransferStage(stage) && !metric.profile_version) reason = "missing_profile_version";
    else if (stage !== "uploading" && !metric.worker_version) reason = "missing_worker_version";
    else if (
      SHORT_STAGE_EXCLUDED_FROM_TRAINING.has(stage) &&
      durationMs > 0 &&
      durationMs < SHORT_STAGE_MIN_DURATION_MS
    ) reason = "duration_too_short";
    else reason = null;

    // final/running son estados de control, no muestras de calculo.
    if (["final", "running", "done", "failed", "cancelled", "processing_complete"].includes(stage)) {
      reason = "control_stage";
    }

    await db.query(
      `update job_stage_metrics
          set valid_for_training = $2,
              invalid_reason = $3,
              updated_at = now()
        where id = $1`,
      [metric.id, !reason, reason]
    );
  }
}

function categorizeOutputFile(file) {
  const name = String(file?.relative_path || file?.name || file?.filename || "").replace(/\\/g, "/").toLowerCase();
  if (!name) return "other";
  if (/resultado\.zip$|outputs\.zip$/.test(name)) return "zip";
  if (/\.(las|laz|ply)$/.test(name) || /nube|point.?cloud/.test(name)) return "point_cloud";
  if (/mdt|dtm/.test(name)) return "dtm";
  if (/mde|dem|dsm/.test(name)) return "dem";
  if (/orto|orthomosaic/.test(name)) return "orthomosaic";
  if (/informe|report/.test(name) && /\.pdf$/.test(name)) return "report";
  if (/\.3tz$|tesela|tiled/.test(name)) return "tiled_model";
  // La extensión manda: model_textured.obj es modelo y
  // model_texture.jpg es una textura.
  if (/\.(obj|fbx|glb|gltf|3ds|dae)$/.test(name)) return "model";
  if (/\.mtl$|\.(jpg|jpeg|png|webp)$/.test(name)) return "texture";
  if (/modelo|model/.test(name)) return "model";
  if (/textur|texture/.test(name)) return "texture";
  if (/curva|contour|\.dxf$/.test(name)) return "contours";
  return "other";
}

async function applyTimingManifest(db, jobId, manifestInput) {
  const manifest = manifestInput && typeof manifestInput === "object" ? manifestInput : {};
  const files = Array.isArray(manifest.files) ? manifest.files : [];
  const normalizedFiles = files.map((file) => ({
    relative_path: String(file?.relative_path || file?.name || file?.filename || ""),
    bytes: Math.max(0, Number(file?.bytes || file?.size || 0)),
    category: String(file?.category || categorizeOutputFile(file)).toLowerCase()
  })).filter((file) => file.relative_path && file.bytes >= 0);

  const groups = new Map();
  for (const file of normalizedFiles) {
    if (!groups.has(file.category)) groups.set(file.category, []);
    groups.get(file.category).push(file);
  }

  const sumBytes = (list) => (list || []).reduce((sum, file) => sum + Number(file.bytes || 0), 0);
  const stageMap = {
    model: ["model", "export_model"],
    point_cloud: ["point_cloud", "export_point_cloud"],
    dem: ["dem", "export_dem"],
    dtm: ["dtm", "export_dtm"],
    orthomosaic: ["orthomosaic", "export_orthomosaic"],
    report: ["report"],
    tiled_model: ["tiled_model", "export_tiled_model"],
    texture: ["texture", "export_texture"]
  };

  for (const [category, stages] of Object.entries(stageMap)) {
    const categoryFiles = groups.get(category) || [];
    if (!categoryFiles.length) continue;
    for (const stage of stages) {
      await upsertJobStageMetric(db, jobId, stage, {
        outputBytes: sumBytes(categoryFiles),
        itemCount: categoryFiles.length,
        profileVersion: manifest.profile_version || null,
        workerVersion: manifest.worker_version || null,
        metrics: {
          output_category: category,
          files: categoryFiles
        }
      });
    }
  }

  const zipFiles = groups.get("zip") || [];
  const zipBytes = Math.max(
    0,
    Number(manifest.zip_bytes || 0),
    sumBytes(zipFiles)
  );
  const deliverableBytes = Math.max(
    0,
    Number(manifest.deliverable_total_bytes || 0),
    normalizedFiles.filter((file) => file.category !== "zip" && file.category !== "other")
      .reduce((sum, file) => sum + file.bytes, 0)
  );

  if (deliverableBytes > 0) {
    await upsertJobStageMetric(db, jobId, "exporting", {
      outputBytes: deliverableBytes,
      itemCount: normalizedFiles.filter((file) => file.category !== "zip" && file.category !== "other").length,
      profileVersion: manifest.profile_version || null,
      workerVersion: manifest.worker_version || null,
      metrics: {
        deliverable_total_bytes: deliverableBytes,
        output_categories: [...new Set(normalizedFiles
          .filter((file) => file.category !== "zip" && file.category !== "other")
          .map((file) => file.category))]
      }
    });
  }

  if (zipBytes > 0) {
    await upsertJobStageMetric(db, jobId, "zip", {
      inputBytes: deliverableBytes || null,
      outputBytes: zipBytes,
      itemCount: normalizedFiles.filter((file) => file.category !== "zip" && file.category !== "other").length,
      profileVersion: manifest.profile_version || null,
      workerVersion: manifest.worker_version || null,
      metrics: {
        zip_bytes: zipBytes,
        deliverable_total_bytes: deliverableBytes,
        compression_ratio: deliverableBytes > 0 ? zipBytes / deliverableBytes : null
      }
    });
    await upsertJobStageMetric(db, jobId, "zip_upload", {
      inputBytes: zipBytes,
      outputBytes: zipBytes,
      itemCount: 1,
      profileVersion: manifest.profile_version || null,
      workerVersion: manifest.worker_version || null,
      metrics: { zip_bytes: zipBytes }
    });
  }

  const { rows } = await db.query(
    `select exif_summary from jobs where id = $1`,
    [jobId]
  );
  if (rows.length) {
    const current = rows[0].exif_summary || {};
    const updated = stripNullCharsDeep({
      ...current,
      _xproces: {
        ...((current && current._xproces) || {}),
        worker_version: manifest.worker_version || current?._xproces?.worker_version || null,
        profile_version: manifest.profile_version || current?._xproces?.profile_version || null,
        predictor_version: TIMING_PREDICTOR_VERSION,
        output_metrics: {
          files: normalizedFiles,
          deliverable_total_bytes: deliverableBytes,
          zip_bytes: zipBytes,
          collected_at: new Date().toISOString()
        }
      }
    });
    await db.query(
      `update jobs
          set exif_summary = $2,
              updated_at = now()
        where id = $1`,
      [jobId, updated]
    );

    // Propagar inmediatamente las versiones a TODAS las fases ya medidas.
    // El manifiesto puede llegar al principio, antes del ZIP o al final. Esta
    // actualización evita que fases anteriores queden invalidadas por haber
    // sido consolidadas antes de recibir worker_version/profile_version.
    await db.query(
      `update job_stage_metrics
          set worker_version = coalesce(worker_version, $2),
              profile_version = coalesce(profile_version, $3),
              updated_at = now()
        where job_id = $1`,
      [
        jobId,
        manifest.worker_version || null,
        manifest.profile_version || null
      ]
    );

    // Reconstruir las métricas existentes con el exif_summary recién
    // actualizado. Es idempotente y no crea eventos ni altera el Batch.
    await refreshCompletedStageMetrics(db, jobId);
  }

  return {
    files: normalizedFiles.length,
    deliverable_total_bytes: deliverableBytes,
    zip_bytes: zipBytes
  };
}

// =====================
// AUTH WORKER
// =====================
function isValidEmail(email) {
  const value = String(email || "").trim();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}
function isValidUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(value || ""));
}


function generateInviteCode() {
  return randomBytes(4).toString("hex").toUpperCase();
}

function requireWorkerAuth(req, res, next) {
  const auth = String(req.headers.authorization || "").trim();
  const workerHeader = String(req.headers["x-worker-token"] || "").trim();

  if (!WORKER_TOKEN) {
    return res
      .status(500)
      .json({ ok: false, error: "WORKER_TOKEN not configured" });
  }

  const expectedBearer = `Bearer ${WORKER_TOKEN}`;

  // Mantiene la autenticacion existente del worker: Authorization: Bearer <token>.
  // Tambien acepta x-worker-token para pruebas manuales con curl sin tocar el flujo real.
  if (auth !== expectedBearer && workerHeader !== WORKER_TOKEN) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }

  next();
}

// =====================
// POSTGRES
// =====================
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

pool
  .query("select 1")
  .then(async () => {
    console.log("Postgres conectado");
    await ensurePaymentColumns();
    await ensureDownloadCleanupColumns();
    await ensureTelegramSchema();
    await ensureTimingAnalyticsSchema();
    await refreshSchemaFlags();
  })
  .catch((err) => console.error("Error Postgres", err));

// =====================
// REDIS + QUEUE
// =====================
let redis = null;
let redisReady = false;
let processQueue = null;

if (process.env.REDIS_URL) {
  redis = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });

  redis.on("ready", () => {
    redisReady = true;
    console.log("Redis conectado (ready)");
  });

  redis.on("error", (e) => {
    redisReady = false;
    console.error("Redis error", e?.message || e);
  });

  processQueue = new Queue("processQueue", { connection: redis });
} else {
  console.log("REDIS_URL no definido");
}

// =====================
// DRIVE (/data)
// =====================
const jobDir = (jobId) => path.join(DATA_ROOT, "jobs", jobId);
const inputDir = (jobId) => path.join(jobDir(jobId), "input");
const outputDir = (jobId) => path.join(jobDir(jobId), "output");

// XPROCES LIVE VIEWPORT: imagen en tiempo real del visor de Metashape.
// Se guarda siempre una sola imagen por job: jobs/<id>/live/latest.jpg
const liveDir = (jobId) => path.join(jobDir(jobId), "live");
const liveImagePath = (jobId) => path.join(liveDir(jobId), "latest.jpg");
const liveMetaPath = (jobId) => path.join(liveDir(jobId), "latest.json");

function ensureLiveDir(jobId) {
  fs.mkdirSync(liveDir(jobId), { recursive: true });
}

function getPublicApiBase(req) {
  const configured = String(process.env.PUBLIC_API_BASE || "").trim().replace(/\/+$/, "");
  if (configured) return configured;

  const host = req.get("host");
  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim();
  let proto = forwardedProto || req.protocol || "https";

  // En produccion Easypanel/nginx puede hablar con Node por http internamente,
  // pero el navegador debe recibir https para evitar mixed content.
  if (host && /xproces\.com$/i.test(host)) proto = "https";

  return `${proto}://${host}`;
}

function ensureJobDirs(jobId) {
  fs.mkdirSync(inputDir(jobId), { recursive: true });
  fs.mkdirSync(outputDir(jobId), { recursive: true });
}

function safeRemoveDir(dir) {
  try {
    if (!fs.existsSync(dir)) return true;

    // 🔥 intento 1
    fs.rmSync(dir, { recursive: true, force: true });

    // 🔥 comprobación
    if (!fs.existsSync(dir)) return true;

    // 🔥 intento 2 (muy importante)
    fs.rmSync(dir, { recursive: true, force: true });

    // 🔥 comprobación final
    if (!fs.existsSync(dir)) {
      console.log("✅ eliminado:", dir);
      return true;
    }

    console.error("❌ NO se pudo eliminar:", dir);
    return false;

  } catch (e) {
    console.error("❌ safeRemoveDir error", dir, e);
    return false;
  }
}

function cleanupJobStorage(jobId, opts = { input: true, output: true }) {
  const result = {
    inputRemoved: false,
    outputRemoved: false,
    jobDirRemovedIfEmpty: false
  };

  if (opts.input) result.inputRemoved = safeRemoveDir(inputDir(jobId));
  if (opts.output) result.outputRemoved = safeRemoveDir(outputDir(jobId));

  // Si ya no queda nada dentro de /jobs/{id}, eliminamos tambien la carpeta raiz del job.
  // Esto deja el VPS limpio y conserva solo el registro en PostgreSQL.
  try {
    const root = jobDir(jobId);
    if (fs.existsSync(root)) {
      const remaining = fs.readdirSync(root).filter(Boolean);
      if (!remaining.length) {
        result.jobDirRemovedIfEmpty = safeRemoveDir(root);
      }
    } else {
      result.jobDirRemovedIfEmpty = true;
    }
  } catch (e) {
    console.error("No se pudo comprobar carpeta raiz del job:", jobId, e?.message || e);
  }

  return result;
}

async function ensureDownloadCleanupColumns() {
  // Columnas ligeras para saber que el cliente ya descargó y que el VPS fue limpiado.
  // IF NOT EXISTS evita romper instalaciones donde ya existan.
  await pool.query(`
    alter table jobs
      add column if not exists download_started_at timestamptz,
      add column if not exists download_completed_at timestamptz,
      add column if not exists download_count integer not null default 0,
      add column if not exists download_verified boolean not null default false,
      add column if not exists output_purged boolean not null default false,
      add column if not exists output_purged_at timestamptz,
      add column if not exists storage_purged_at timestamptz,
      add column if not exists archived_log text,
      add column if not exists archived_outputs jsonb,
      add column if not exists archived_summary_saved_at timestamptz,
      add column if not exists upload_started_at timestamptz,
      add column if not exists upload_completed_at timestamptz,
      add column if not exists results_ready_at timestamptz
  `);
}


async function ensureTimingAnalyticsSchema() {
  // La instalación actual ya tiene casi todas estas columnas. IF NOT EXISTS
  // permite desplegar este server.js sin una migración manual separada.
  await pool.query(`
    alter table jobs
      add column if not exists quality text,
      add column if not exists outputs jsonb not null default '[]'::jsonb,
      add column if not exists avg_photo_mb numeric,
      add column if not exists avg_width integer,
      add column if not exists avg_height integer,
      add column if not exists avg_megapixels numeric,
      add column if not exists total_megapixels numeric,
      add column if not exists processing_load_score numeric,
      add column if not exists estimated_processing_seconds integer
  `);

  await pool.query(`
    create index if not exists idx_job_stage_events_job_time
      on job_stage_events (job_id, created_at)
  `);

  await pool.query(`
    create index if not exists idx_jobs_timing_similarity
      on jobs (status, project_type, quality, created_at desc)
  `);

  // Una fila limpia por trabajo y fase. job_stage_events conserva la cronologia
  // completa; esta tabla contiene la medicion validada que usa el predictor.
  await pool.query(`
    create table if not exists job_stage_metrics (
      id bigserial primary key,
      job_id uuid not null references jobs(id) on delete cascade,
      stage text not null,

      started_at timestamptz,
      finished_at timestamptz,
      duration_ms bigint,

      photos_count integer,
      input_bytes bigint,
      output_bytes bigint,
      item_count bigint,
      avg_photo_mb numeric,
      avg_width integer,
      avg_height integer,
      avg_megapixels numeric,
      total_megapixels numeric,
      point_count bigint,
      processing_load_score numeric,
      project_type text,
      quality text,
      outputs jsonb not null default '[]'::jsonb,

      profile_version text,
      worker_version text,
      predictor_version text,
      metrics_version text,

      start_events integer not null default 0,
      end_events integer not null default 0,
      valid_for_training boolean not null default false,
      invalid_reason text,
      metrics jsonb not null default '{}'::jsonb,

      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      unique(job_id, stage)
    )
  `);

  // También soporta despliegues intermedios donde la tabla pudiera existir
  // con menos columnas. Estas sentencias son idempotentes.
  await pool.query(`
    alter table job_stage_metrics
      add column if not exists started_at timestamptz,
      add column if not exists finished_at timestamptz,
      add column if not exists duration_ms bigint,
      add column if not exists photos_count integer,
      add column if not exists input_bytes bigint,
      add column if not exists output_bytes bigint,
      add column if not exists item_count bigint,
      add column if not exists avg_photo_mb numeric,
      add column if not exists avg_width integer,
      add column if not exists avg_height integer,
      add column if not exists avg_megapixels numeric,
      add column if not exists total_megapixels numeric,
      add column if not exists point_count bigint,
      add column if not exists processing_load_score numeric,
      add column if not exists project_type text,
      add column if not exists quality text,
      add column if not exists outputs jsonb not null default '[]'::jsonb,
      add column if not exists profile_version text,
      add column if not exists worker_version text,
      add column if not exists predictor_version text,
      add column if not exists metrics_version text,
      add column if not exists start_events integer not null default 0,
      add column if not exists end_events integer not null default 0,
      add column if not exists valid_for_training boolean not null default false,
      add column if not exists invalid_reason text,
      add column if not exists metrics jsonb not null default '{}'::jsonb,
      add column if not exists created_at timestamptz not null default now(),
      add column if not exists updated_at timestamptz not null default now()
  `);

  await pool.query(`
    create unique index if not exists idx_job_stage_metrics_job_stage_unique
      on job_stage_metrics (job_id, stage)
  `);

  await pool.query(`
    create index if not exists idx_job_stage_metrics_training
      on job_stage_metrics (stage, project_type, quality, valid_for_training, created_at desc)
  `);

  await pool.query(`
    create index if not exists idx_job_stage_metrics_job
      on job_stage_metrics (job_id, stage)
  `);

  // Vista directa para revisar, exportar o analizar los historicos por fase.
  await pool.query(`
    create or replace view job_stage_training_data as
    select
      m.job_id,
      j.created_at as job_created_at,
      m.stage,
      m.started_at,
      m.finished_at,
      m.duration_ms,
      round(m.duration_ms / 1000.0, 3) as stage_seconds,
      m.project_type,
      m.quality,
      m.outputs,
      m.photos_count,
      m.input_bytes,
      m.output_bytes,
      m.item_count,
      m.avg_photo_mb,
      m.avg_width,
      m.avg_height,
      m.avg_megapixels,
      m.total_megapixels,
      m.point_count,
      m.processing_load_score,
      m.profile_version,
      m.worker_version,
      m.predictor_version,
      m.valid_for_training,
      m.invalid_reason,
      m.metrics
    from job_stage_metrics m
    join jobs j on j.id = m.job_id
  `);
}


async function ensureTelegramSchema() {
  await pool.query(`
    create table if not exists telegram_user_settings (
      user_id uuid primary key references users(id) on delete cascade,
      chat_id bigint unique,
      username varchar(255),
      first_name varchar(255),
      is_active boolean not null default false,
      preferences jsonb not null default '{
        "job_received": true,
        "processing_started": true,
        "processing_finished": true,
        "processing_error": true,
        "payment_received": true
      }'::jsonb,
      linked_at timestamptz,
      disconnected_at timestamptz,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);

  await pool.query(`
    create table if not exists telegram_link_codes (
      code varchar(32) primary key,
      user_id uuid not null references users(id) on delete cascade,
      expires_at timestamptz not null,
      used_at timestamptz,
      created_at timestamptz not null default now()
    )
  `);

  await pool.query(`create index if not exists telegram_link_codes_user_id_idx on telegram_link_codes(user_id)`);
  await pool.query(`create index if not exists telegram_link_codes_expires_at_idx on telegram_link_codes(expires_at)`);
}

function getRequestUserId(req) {
  return String(req.headers["x-user-id"] || req.query?.user_id || req.body?.user_id || "").trim();
}

async function requireExistingUser(req, res) {
  const userId = getRequestUserId(req);
  if (!isValidUuid(userId)) {
    res.status(401).json({ ok: false, error: "missing_user", message: "Usuario no identificado" });
    return null;
  }
  const result = await pool.query(`select id, email, name from users where id=$1 limit 1`, [userId]);
  if (!result.rows.length) {
    res.status(404).json({ ok: false, error: "user_not_found", message: "Usuario no encontrado" });
    return null;
  }
  return result.rows[0];
}

function defaultTelegramPreferences() {
  return {
    job_received: true,
    processing_started: true,
    processing_finished: true,
    processing_error: true,
    payment_received: true
  };
}

function normalizeTelegramPreferences(value) {
  const defaults = defaultTelegramPreferences();
  const input = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  for (const key of Object.keys(defaults)) {
    if (Object.prototype.hasOwnProperty.call(input, key)) defaults[key] = input[key] !== false;
  }
  return defaults;
}

function generateTelegramLinkCode() {
  const digits = String(Math.floor(100000 + Math.random() * 900000));
  return `XP-${digits}`;
}

async function getTelegramUserSettings(userId) {
  await ensureTelegramSchema();
  const result = await pool.query(
    `select user_id, chat_id, username, first_name, is_active, preferences, linked_at, disconnected_at
       from telegram_user_settings where user_id=$1 limit 1`,
    [userId]
  );
  return result.rows[0] || null;
}

async function sendUserTelegramEvent(userId, preferenceKey, text) {
  if (!userId) return { ok: false, skipped: true, error: "missing_user_id" };
  const settings = await getTelegramUserSettings(userId);
  if (!settings?.is_active || !settings?.chat_id) {
    return { ok: false, skipped: true, error: "telegram_user_not_connected" };
  }
  const preferences = normalizeTelegramPreferences(settings.preferences);
  if (preferenceKey && preferences[preferenceKey] === false) {
    return { ok: false, skipped: true, error: "telegram_preference_disabled" };
  }
  return sendTelegramMessage(text, { chatId: String(settings.chat_id) });
}

function telegramJobTitle(job) {
  return String(job?.project_name || job?.name || job?.project_type || "Proyecto XProces");
}

async function notifyUserPaymentReceived(job, payment) {
  const amount = Number(payment?.amount || job?.payment_amount || job?.price || 0);
  const currency = String(payment?.currency || job?.payment_currency || "EUR").toUpperCase();
  const text = [
    "💳 <b>Pago recibido</b>",
    "",
    `Proyecto: <b>${telegramJobTitle(job).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")}</b>`,
    `Importe: <b>${amount.toFixed(2).replace(".", ",")} ${currency}</b>`,
    `Trabajo: <code>${String(job?.id || "")}</code>`,
    "",
    "El trabajo queda preparado para continuar con la subida y el procesado."
  ].join("\n");
  return sendUserTelegramEvent(job?.user_id, "payment_received", text);
}

async function notifyUserJobTransition(previousJob, updatedJob) {
  const oldStatus = String(previousJob?.status || "").toLowerCase();
  const newStatus = String(updatedJob?.status || "").toLowerCase();
  const oldStage = normalizeProcessingStage(previousJob?.stage);
  const newStage = normalizeProcessingStage(updatedJob?.stage);
  const title = telegramJobTitle(updatedJob).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const jobId = String(updatedJob?.id || "");

  if (newStatus === "running" && oldStatus !== "running") {
    return sendUserTelegramEvent(updatedJob?.user_id, "processing_started", [
      "⚙️ <b>Procesado iniciado</b>", "", `Proyecto: <b>${title}</b>`, `Trabajo: <code>${jobId}</code>`
    ].join("\n"));
  }
  if (newStatus === "done" && oldStatus !== "done") {
    return sendUserTelegramEvent(updatedJob?.user_id, "processing_finished", [
      "✅ <b>Procesado finalizado</b>", "", `Proyecto: <b>${title}</b>`, `Trabajo: <code>${jobId}</code>`, "", "Los resultados ya están preparados en XProces."
    ].join("\n"));
  }
  if (newStatus === "failed" && oldStatus !== "failed") {
    return sendUserTelegramEvent(updatedJob?.user_id, "processing_error", [
      "❌ <b>Incidencia en el procesado</b>", "", `Proyecto: <b>${title}</b>`, `Trabajo: <code>${jobId}</code>`, `Detalle: ${String(updatedJob?.error || updatedJob?.message || "Requiere revisión")}`
    ].join("\n"));
  }
  if (newStage && newStage !== oldStage && newStage === "processing_complete") {
    return { ok: false, skipped: true, error: "waiting_for_zip_upload" };
  }
  return { ok: false, skipped: true, error: "no_relevant_transition" };
}

async function ensurePaymentColumns() {
  await pool.query(`
    alter table jobs
      add column if not exists payment_status varchar(30) not null default 'pending',
      add column if not exists payment_provider varchar(20),
      add column if not exists payment_order_id varchar(120),
      add column if not exists payment_capture_id varchar(120),
      add column if not exists payment_amount numeric(10,2),
      add column if not exists payment_currency varchar(3) not null default 'EUR',
      add column if not exists paypal_payer_id varchar(120),
      add column if not exists paypal_payer_email varchar(320),
      add column if not exists payment_date timestamptz,
      add column if not exists paid_at timestamptz
  `);

  // Una orden o captura de PayPal no puede quedar vinculada a dos trabajos distintos.
  await pool.query(`
    create unique index if not exists jobs_payment_order_id_unique
      on jobs (payment_order_id)
      where payment_order_id is not null and payment_order_id <> ''
  `);
  await pool.query(`
    create unique index if not exists jobs_payment_capture_id_unique
      on jobs (payment_capture_id)
      where payment_capture_id is not null and payment_capture_id <> ''
  `);
}

function canUsePaidJob(job) {
  return ["paid", "exempt"].includes(String(job?.payment_status || "").toLowerCase());
}

async function requirePaidJobBeforeUpload(req, res, next) {
  try {
    await ensurePaymentColumns();
    const { rows } = await pool.query(
      `select id, status, payment_status, user_id from jobs where id=$1`,
      [req.params.id]
    );
    if (!rows.length) return res.status(404).json({ ok: false, error: "job_not_found" });
    const job = rows[0];
    const requestUserId = String(req.headers["x-user-id"] || "").trim();
    if (job.user_id && String(job.user_id) !== requestUserId) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }
    if (!canUsePaidJob(job)) {
      return res.status(402).json({
        ok: false,
        error: "payment_required",
        message: "El pago debe confirmarse antes de subir las fotografías."
      });
    }
    next();
  } catch (e) {
    console.error("payment upload guard error", e);
    res.status(500).json({ ok: false, error: "payment_guard_error" });
  }
}

async function markDownloadStarted(jobId) {
  try {
    await ensureDownloadCleanupColumns();

    const currentResult = await pool.query(
      `select stage, progress, download_started_at, exif_summary
         from jobs
        where id = $1`,
      [jobId]
    );
    if (!currentResult.rows.length) return;

    const current = currentResult.rows[0];
    if (!current.download_started_at) {
      const previousStage = normalizeProcessingStage(current.stage);
      const planned = getPlannedProgressForStage(current.exif_summary, "result_download");
      const nextProgress = Number.isFinite(planned)
        ? Math.max(Number(current.progress || 0), Math.round(planned))
        : Number(current.progress || 0);

      if (previousStage && previousStage !== "result_download") {
        await recordJobStageEvent(pool, jobId, previousStage, "end");
      }
      await recordJobStageEvent(pool, jobId, "result_download", "start");

      await pool.query(
        `update jobs
            set download_started_at = now(),
                stage = 'result_download',
                progress = greatest(coalesce(progress, 0), $2),
                message = 'Descargando resultados',
                updated_at = now()
          where id = $1`,
        [jobId, nextProgress]
      );
      return;
    }

    await pool.query(
      `update jobs set updated_at = now() where id = $1`,
      [jobId]
    );
  } catch (e) {
    console.error("No se pudo marcar inicio de descarga:", jobId, e?.message || e);
  }
}

async function markDownloadCompletedAndPurged(jobId, cleanup) {
  try {
    await ensureDownloadCleanupColumns();
    await recordJobStageEvent(pool, jobId, "result_download", "end");
    await pool.query(
      `update jobs
          set download_completed_at = now(),
              download_count = coalesce(download_count, 0) + 1,
              download_verified = true,
              output_purged = $2,
              output_purged_at = case when $2 then coalesce(output_purged_at, now()) else output_purged_at end,
              storage_purged_at = case when $3 then coalesce(storage_purged_at, now()) else storage_purged_at end,
              status = case when status in ('done', 'completed', 'complete', 'success', 'processed') then 'downloaded' else status end,
              stage = 'downloaded',
              message = 'No disponible para descarga directa.',
              updated_at = now()
        where id = $1`,
      [jobId, !!cleanup?.outputRemoved, !!cleanup?.jobDirRemovedIfEmpty]
    );
  } catch (e) {
    console.error("No se pudo marcar descarga completada/purgada:", jobId, e?.message || e);
  }
}

function listOutputFilesSnapshot(jobId) {
  const dir = outputDir(jobId);
  if (!fs.existsSync(dir)) return [];

  try {
    return fs.readdirSync(dir)
      .map((name) => {
        const full = path.join(dir, name);
        const stat = fs.statSync(full);
        if (!stat.isFile()) return null;
        return { filename: name, size: stat.size };
      })
      .filter(Boolean)
      .sort((a, b) => String(a.filename).localeCompare(String(b.filename)));
  } catch (e) {
    console.error("No se pudieron listar outputs para resumen:", jobId, e?.message || e);
    return [];
  }
}

function readJobLogSnapshot(jobId) {
  const logPath = path.join(outputDir(jobId), "metashape-python-log.txt");
  try {
    if (fs.existsSync(logPath)) {
      return fs.readFileSync(logPath, "utf8");
    }
  } catch (e) {
    console.error("No se pudo leer log para resumen:", jobId, e?.message || e);
  }
  return "";
}

async function saveArchivedJobSummary(jobId) {
  try {
    await ensureDownloadCleanupColumns();

    const archivedLog = readJobLogSnapshot(jobId);
    const archivedOutputs = listOutputFilesSnapshot(jobId);

    await pool.query(
      `update jobs
          set archived_log = coalesce(nullif($2, ''), archived_log),
              archived_outputs = case
                when $3::jsonb = '[]'::jsonb then archived_outputs
                else $3::jsonb
              end,
              archived_summary_saved_at = now(),
              updated_at = now()
        where id = $1`,
      [jobId, archivedLog, JSON.stringify(archivedOutputs)]
    );

    console.log("Resumen archivado del job:", jobId, {
      logChars: archivedLog.length,
      outputFiles: archivedOutputs.length
    });
  } catch (e) {
    console.error("No se pudo guardar resumen archivado:", jobId, e?.message || e);
  }
}


// ✅ helper: lista SOLO imágenes (evita mezclar txt, etc.)
function listInputImages(jobId) {
  const dir = inputDir(jobId);
  if (!fs.existsSync(dir)) return [];
  const exts = new Set([".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp"]);
  return fs
    .readdirSync(dir)
    .filter((name) => exts.has(path.extname(name).toLowerCase()))
    .sort();
}

function getInputTotalBytes(jobId) {
  const dir = inputDir(jobId);
  if (!fs.existsSync(dir)) return 0;

  return fs.readdirSync(dir).reduce((acc, name) => {
    const full = path.join(dir, name);
    const stat = fs.statSync(full);
    if (stat.isFile()) return acc + stat.size;
    return acc;
  }, 0);
}


function finiteNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeOutputList(value) {
  if (Array.isArray(value)) {
    return [...new Set(value.map((x) => String(x || "").trim().toLowerCase()).filter(Boolean))];
  }

  if (typeof value === "string") {
    const raw = value.trim();
    if (!raw) return [];
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return normalizeOutputList(parsed);
    } catch (_) {
      // Puede venir como lista separada por comas desde el worker/frontend.
    }
    return [...new Set(raw.split(",").map((x) => x.trim().toLowerCase()).filter(Boolean))];
  }

  return [];
}

function extractDeclaredImageStats(exifSummary, photosCount = 0, totalBytes = 0) {
  const meta = exifSummary?._xproces || {};
  const sources = [meta.image_stats || {}, meta, exifSummary || {}];

  const pick = (...keys) => {
    for (const source of sources) {
      for (const key of keys) {
        const n = finiteNumber(source?.[key], null);
        if (n !== null && n >= 0) return n;
      }
    }
    return null;
  };

  const photos = Math.max(0, Number(photosCount || meta.totalPhotos || 0));
  const bytes = Math.max(0, Number(totalBytes || meta.totalBytes || 0));
  const avgPhotoMb = pick("avg_photo_mb", "avgPhotoMb", "average_photo_mb", "averagePhotoMb")
    ?? (photos > 0 && bytes > 0 ? bytes / photos / (1024 * 1024) : null);
  const avgWidth = pick("avg_width", "avgWidth", "average_width", "averageWidth");
  const avgHeight = pick("avg_height", "avgHeight", "average_height", "averageHeight");
  const avgMegapixels = pick("avg_megapixels", "avgMegapixels", "average_megapixels", "averageMegapixels", "megapixels");
  const totalMegapixels = pick("total_megapixels", "totalMegapixels")
    ?? (avgMegapixels !== null && photos > 0 ? avgMegapixels * photos : null);

  return {
    avgPhotoMb,
    avgWidth,
    avgHeight,
    avgMegapixels,
    totalMegapixels
  };
}

function readImageHeader(filePath, maxBytes = 1024 * 1024) {
  const stat = fs.statSync(filePath);
  const size = Math.max(0, Math.min(stat.size, maxBytes));
  const buffer = Buffer.alloc(size);
  const fd = fs.openSync(filePath, "r");
  try {
    const bytesRead = fs.readSync(fd, buffer, 0, size, 0);
    return buffer.subarray(0, bytesRead);
  } finally {
    fs.closeSync(fd);
  }
}

function readImageDimensions(filePath) {
  try {
    const buffer = readImageHeader(filePath);
    if (buffer.length < 24) return null;

    // PNG
    if (
      buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4e &&
      buffer[3] === 0x47 && buffer[4] === 0x0d && buffer[5] === 0x0a
    ) {
      const width = buffer.readUInt32BE(16);
      const height = buffer.readUInt32BE(20);
      if (width > 0 && height > 0) return { width, height };
    }

    // JPEG: busca cualquier marcador SOF que incluya ancho y alto.
    if (buffer[0] === 0xff && buffer[1] === 0xd8) {
      const sofMarkers = new Set([
        0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7,
        0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf
      ]);
      let offset = 2;

      while (offset + 9 < buffer.length) {
        if (buffer[offset] !== 0xff) {
          offset += 1;
          continue;
        }

        while (offset < buffer.length && buffer[offset] === 0xff) offset += 1;
        if (offset >= buffer.length) break;

        const marker = buffer[offset];
        offset += 1;

        // Marcadores sin longitud.
        if (marker === 0xd8 || marker === 0xd9 || (marker >= 0xd0 && marker <= 0xd7) || marker === 0x01) {
          continue;
        }

        if (offset + 2 > buffer.length) break;
        const segmentLength = buffer.readUInt16BE(offset);
        if (segmentLength < 2 || offset + segmentLength > buffer.length) break;

        if (sofMarkers.has(marker) && segmentLength >= 7) {
          const height = buffer.readUInt16BE(offset + 3);
          const width = buffer.readUInt16BE(offset + 5);
          if (width > 0 && height > 0) return { width, height };
        }

        offset += segmentLength;
      }
    }

    // TIFF básico (little/big endian), suficiente para leer Width/Height del primer IFD.
    const littleEndian = buffer[0] === 0x49 && buffer[1] === 0x49;
    const bigEndian = buffer[0] === 0x4d && buffer[1] === 0x4d;
    if (littleEndian || bigEndian) {
      const readU16 = (offset) => littleEndian ? buffer.readUInt16LE(offset) : buffer.readUInt16BE(offset);
      const readU32 = (offset) => littleEndian ? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset);
      const ifdOffset = readU32(4);
      if (ifdOffset > 0 && ifdOffset + 2 < buffer.length) {
        const count = readU16(ifdOffset);
        let width = null;
        let height = null;
        for (let i = 0; i < count; i += 1) {
          const entry = ifdOffset + 2 + (i * 12);
          if (entry + 12 > buffer.length) break;
          const tag = readU16(entry);
          const type = readU16(entry + 2);
          const valueCount = readU32(entry + 4);
          if (![256, 257].includes(tag) || valueCount < 1) continue;

          let value = null;
          if (type === 3) value = readU16(entry + 8);
          else if (type === 4) value = readU32(entry + 8);

          if (tag === 256) width = value;
          if (tag === 257) height = value;
        }
        if (width > 0 && height > 0) return { width, height };
      }
    }
  } catch (e) {
    console.error("No se pudieron leer dimensiones de imagen:", path.basename(filePath), e?.message || e);
  }

  return null;
}

function collectInputImageStats(jobId, exifSummary = null) {
  const names = listInputImages(jobId);
  const count = names.length;
  const declared = extractDeclaredImageStats(exifSummary, count, 0);

  if (!count) {
    return {
      photosCount: 0,
      inputTotalBytes: 0,
      avgPhotoMb: declared.avgPhotoMb,
      avgWidth: declared.avgWidth,
      avgHeight: declared.avgHeight,
      avgMegapixels: declared.avgMegapixels,
      totalMegapixels: declared.totalMegapixels,
      sampledDimensions: 0
    };
  }

  let totalBytes = 0;
  for (const name of names) {
    try {
      totalBytes += fs.statSync(path.join(inputDir(jobId), name)).size;
    } catch (_) {
      // El submit se ejecuta cuando la subida ya está cerrada; un fallo aislado
      // de stat no debe impedir procesar el resto.
    }
  }

  // Las cámaras de un trabajo suelen usar la misma resolución. Muestrear 64
  // cabeceras limita E/S incluso con miles de fotos y mantiene una media fiable.
  const sampleLimit = 64;
  const sampleIndexes = new Set();
  const sampleCount = Math.min(sampleLimit, count);
  for (let i = 0; i < sampleCount; i += 1) {
    sampleIndexes.add(Math.round((i * (count - 1)) / Math.max(1, sampleCount - 1)));
  }

  let widthTotal = 0;
  let heightTotal = 0;
  let megapixelsTotal = 0;
  let validDimensions = 0;

  for (const index of sampleIndexes) {
    const dimensions = readImageDimensions(path.join(inputDir(jobId), names[index]));
    if (!dimensions) continue;
    widthTotal += dimensions.width;
    heightTotal += dimensions.height;
    megapixelsTotal += (dimensions.width * dimensions.height) / 1_000_000;
    validDimensions += 1;
  }

  const avgWidth = validDimensions > 0
    ? Math.round(widthTotal / validDimensions)
    : declared.avgWidth;
  const avgHeight = validDimensions > 0
    ? Math.round(heightTotal / validDimensions)
    : declared.avgHeight;
  const avgMegapixels = validDimensions > 0
    ? megapixelsTotal / validDimensions
    : declared.avgMegapixels;
  const avgPhotoMb = totalBytes > 0
    ? totalBytes / count / (1024 * 1024)
    : declared.avgPhotoMb;
  const totalMegapixels = avgMegapixels !== null && avgMegapixels !== undefined
    ? avgMegapixels * count
    : declared.totalMegapixels;

  return {
    photosCount: count,
    inputTotalBytes: totalBytes,
    avgPhotoMb,
    avgWidth,
    avgHeight,
    avgMegapixels,
    totalMegapixels,
    sampledDimensions: validDimensions
  };
}

const OUTPUT_LOAD_WEIGHTS = {
  las: 0.12,
  point_cloud_las: 0.12,
  orthomosaic_tif: 0.30,
  ortho_tif: 0.30,
  dem_tif: 0.10,
  dsm_tif: 0.10,
  dtm_tif: 0.18,
  contours_dxf: 0.05,
  mesh_obj: 0.18,
  model_obj: 0.18,
  obj: 0.18,
  glb: 0.14,
  mesh_fbx: 0.18,
  fbx: 0.18,
  textures: 0.20,
  texture: 0.20,
  tiled_model: 0.18,
  tiles_3tz: 0.18,
  pdf_report: 0.03
};

function calculateProcessingLoadScore({
  photosCount,
  totalBytes,
  avgMegapixels,
  totalMegapixels,
  qualityMode,
  outputsRequested,
  projectType
}) {
  const photos = Math.max(0, Number(photosCount || 0));
  const bytes = Math.max(0, Number(totalBytes || 0));
  const totalMb = bytes / (1024 * 1024);
  const avgMp = Math.max(0, Number(avgMegapixels || 0));
  const totalMp = Math.max(0, Number(totalMegapixels || 0))
    || (avgMp > 0 ? avgMp * photos : 0)
    || (photos * 12);

  const outputs = normalizeOutputList(outputsRequested);
  const outputFactor = 1 + outputs.reduce(
    (sum, output) => sum + (OUTPUT_LOAD_WEIGHTS[output] || 0.04),
    0
  );

  const type = String(projectType || "fotogrametria").toLowerCase();
  const typeFactor = type === "3d" ? 1.15 : (type === "tams" ? 0.75 : 1);
  const qualityFactor = getQualityModeTimeFactor(qualityMode);

  // Combina píxeles y volumen comprimido. No pretende ser "segundos":
  // es una unidad de carga estable para comparar trabajos similares.
  const baseWork = totalMp + (totalMb * 0.20);
  return Math.round(baseWork * qualityFactor * outputFactor * typeFactor * 100) / 100;
}

function estimateProcessingSecondsFromInputs(photosCount, totalBytes, qualityMode = "normal") {
  const photos = Number(photosCount || 0);
  const bytes = Number(totalBytes || 0);
  const gb = bytes / (1024 * 1024 * 1024);
  const factor = getQualityModeTimeFactor(qualityMode);

  return Math.round(
    (120 + (photos * 18) + (gb * 420)) * factor
  );
}

function calculatePriceFromInputs(photosCount, totalBytes, estimatedSeconds, qualityMode = "normal", outputsRequested = [], projectType = "fotogrametria") {
  const type = String(projectType || "fotogrametria").toLowerCase();

  // TAMS mantiene su precio fijo independiente del tiempo estimado.
  if (type === "tams") return 100;

  // XProces se cobra únicamente según el tiempo estimado de procesamiento.
  // Tramos acumulativos, sin coste mínimo y sin redondeo a euros enteros:
  //   minutos 1-45:    0,50 EUR/min
  //   minutos 46-120:  0,40 EUR/min
  //   desde minuto 121: 0,30 EUR/min
  const minutes = Math.max(0, Number(estimatedSeconds || 0) / 60);

  let price = 0;

  if (minutes <= 45) {
    price = minutes * 0.50;
  } else if (minutes <= 120) {
    price = (45 * 0.50) + ((minutes - 45) * 0.40);
  } else {
    price = (45 * 0.50) + (75 * 0.40) + ((minutes - 120) * 0.30);
  }

  // Se conservan céntimos exactos para la base de datos y PayPal.
  return Math.max(0, Math.round((price + Number.EPSILON) * 100) / 100);
}

// ✅ helper: status “bloqueado” (no permitir más uploads)
function isLockedStatus(status) {
  return ["queued", "running", "done", "tams_pending_download", "tams_downloaded"].includes(
    String(status || "").toLowerCase()
  );
}

// =====================
// MULTER INPUT (photos)
// =====================
const inputStorage = multer.diskStorage({
  destination: (req, _file, cb) => {
    const { id } = req.params;
    ensureJobDirs(id);
    cb(null, inputDir(id));
  },
  filename: (_req, file, cb) => {
    const safe = file.originalname.replace(/[^\w.\-() ]+/g, "_");
    cb(null, safe);
  },
});

const uploadInput = multer({
  storage: inputStorage,
  limits: { fileSize: 250 * 1024 * 1024 }, // 250MB/foto
});

// =====================
// MULTER OUTPUT (zip subido por worker Windows)
// =====================
const outputStorage = multer.diskStorage({
  destination: (req, _file, cb) => {
    const { id } = req.params;
    ensureJobDirs(id);
    cb(null, outputDir(id));
  },
  filename: (_req, _file, cb) => {
    // Nombre interno fijo: toda la lógica de validación y descarga espera outputs.zip.
    cb(null, "outputs.zip");
  },
});

const uploadOutput = multer({
  storage: outputStorage,
  limits: { fileSize: 25 * 1024 * 1024 * 1024 }, // 25GB
});

// XPROCES LIVE VIEWPORT: upload pequeño en memoria, no crea históricos.
const uploadLive = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 3 * 1024 * 1024 }, // 3MB por captura
});

// =====================
// BASIC
// =====================
app.get("/", (_req, res) => res.send("mapxion api ok"));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});


app.get("/admin/jobs", requireAdmin, async (_req, res) => {
  try {
    await ensurePaymentColumns();
    await ensureDownloadCleanupColumns();
    const { rows } = await pool.query(`
      select
        id,
        client_email,
        project_name,
        client_name,
        status,
        stage,
        photos_count,
        price,
        progress,
        message,
        error,
        created_at,
        started_at,
        finished_at,
        coalesce(upload_completed_at, processing_started_at, started_at) as upload_completed_at,
        processing_started_at,
        processing_finished_at,
        coalesce(results_ready_at, processing_finished_at, finished_at) as results_ready_at,
        download_started_at,
        download_completed_at,
        download_count,
        download_verified,
        download_seconds,
        processing_seconds,
        total_seconds,
        payment_status,
        payment_provider,
        payment_order_id as paypal_order_id,
        payment_capture_id as paypal_capture_id,
        payment_amount,
        payment_currency,
        paypal_payer_id,
        paypal_payer_email,
        coalesce(payment_date, paid_at) as payment_date
        ${jobsHasQualityMode ? ", quality_mode" : ""}
      from jobs
      order by created_at desc
      limit 100
    `);

    res.json({
      ok: true,
      jobs: rows.map((job) => ({
        ...job,
        quality_mode: normalizeQualityMode(job.quality_mode || job?.exif_summary?._xproces?.quality_mode || "normal")
      }))
    });

  } catch (e) {
    console.error("admin jobs error", e);
    res.status(500).json({
      ok: false,
      error: "admin jobs error"
    });
  }
});


app.get("/admin/users", requireAdmin, async (_req, res) => {
  try {
    const { rows } = await pool.query(`
      select
        u.id,
        u.email,
        u.name,
        u.created_at,
        count(j.id)::int as jobs_count,
        count(j.id) filter (
          where lower(coalesce(j.status, '')) in ('done', 'completed')
        )::int as completed_jobs,
        count(j.id) filter (
          where lower(coalesce(j.status, '')) = 'failed'
        )::int as failed_jobs,
        coalesce(sum(coalesce(j.photos_count, 0)), 0)::bigint as photos_count,
        coalesce(sum(
          case
            when lower(coalesce(j.status, '')) in ('done', 'completed')
            then coalesce(j.price, 0)
            else 0
          end
        ), 0) as revenue_total,
        max(j.created_at) as last_job_at
      from users u
      left join jobs j
        on lower(trim(coalesce(j.client_email, ''))) =
           lower(trim(coalesce(u.email, '')))
      group by u.id, u.email, u.name, u.created_at
      order by u.created_at desc
      limit 1000
    `);

    res.json({ ok: true, users: rows });
  } catch (e) {
    console.error("admin users error", e);
    res.status(500).json({
      ok: false,
      error: "admin_users_error",
      message: e?.message || "No se pudieron consultar los usuarios"
    });
  }
});

app.get("/admin/config", requireAdmin, (_req, res) => {
  res.json({
    ok: true,
    config: {
      api_base: process.env.PUBLIC_API_BASE || "https://api.xproces.com",
      support_email: "soportetams@intelsi.es",
      auto_refresh_seconds: 5,
      pricing_model: "estimated_processing_time",
      minimum_price: 0,
      rounding: "cents_only",
      time_rates: [
        { from_minute: 0, to_minute: 45, eur_per_minute: 0.50 },
        { from_minute: 45, to_minute: 120, eur_per_minute: 0.40 },
        { from_minute: 120, to_minute: null, eur_per_minute: 0.30 }
      ],
      tams_fixed_price: 100
    }
  });
});


app.get("/version", (_req, res) =>
  res.json({ version: "v42-stage-timing-predictor-v5", predictor_version: TIMING_PREDICTOR_VERSION })
);

app.get("/redis", (_req, res) =>
  res.json({ configured: !!process.env.REDIS_URL, ready: redisReady })
);

app.get("/queue", async (_req, res) => {
  try {
    if (!processQueue || !redisReady) {
      return res.status(503).json({ ok: false, error: "queue unavailable" });
    }
    const counts = await processQueue.getJobCounts();
    res.json({ ok: true, counts });
  } catch (e) {
    console.error("queue error", e);
    res.status(500).json({ ok: false, error: "queue error" });
  }
});

// =====================
// PRICING PREVIEW
// =====================
app.post("/pricing/preview", async (req, res) => {
  try {
    console.log("PRICING PREVIEW origin:", req.headers.origin);
    console.log("PRICING PREVIEW body:", req.body);

    const photosCount = Number(req.body?.photos_count || 0);
    const totalBytes = Number(req.body?.total_bytes || 0);
    console.log("[XPROCES QUALITY] BODY:", req.body);
console.log("[XPROCES QUALITY] RAW:", req.body?.quality_mode);
const qualityMode = normalizeQualityMode(req.body?.quality_mode);
console.log("[XPROCES QUALITY] NORMALIZED:", qualityMode);
    const outputsRequested = normalizeOutputList(Array.isArray(req.body?.outputs_requested) ? req.body.outputs_requested : []);
    const projectType = String(req.body?.project_type || req.body?.xproces_meta?.project_type || "fotogrametria").toLowerCase();
    const tamsExport = !!req.body?.tams_export || projectType === "tams";
    const previewExifSummary = stripNullCharsDeep(req.body?.exif_summary || null);
    const previewStats = extractDeclaredImageStats(previewExifSummary, photosCount, totalBytes);
    const previewLoadScore = calculateProcessingLoadScore({
      photosCount,
      totalBytes,
      avgMegapixels: previewStats.avgMegapixels,
      totalMegapixels: previewStats.totalMegapixels,
      qualityMode,
      outputsRequested,
      projectType
    });

    console.log("PRICING PREVIEW parsed:", { photosCount, totalBytes, qualityMode, tamsExport });

    if (!Number.isFinite(photosCount) || photosCount < 0) {
      return res.status(400).json({ ok: false, error: "invalid photos_count" });
    }

    if (!Number.isFinite(totalBytes) || totalBytes < 0) {
      return res.status(400).json({ ok: false, error: "invalid total_bytes" });
    }

    const previewFeatures = {
      photosCount,
      totalBytes,
      qualityMode,
      outputsRequested,
      projectType,
      tamsExport,
      avgPhotoMb: previewStats.avgPhotoMb,
      avgWidth: previewStats.avgWidth,
      avgHeight: previewStats.avgHeight,
      avgMegapixels: previewStats.avgMegapixels,
      totalMegapixels: previewStats.totalMegapixels,
      processingLoadScore: previewLoadScore
    };
    const previewFallback = await estimateProcessingDetailsHistorical(pool, previewFeatures);
    const timingPlan = await estimateStageTimingPlan(pool, previewFeatures, previewFallback);
    const estimatedSeconds = timingPlan.estimated_processing_seconds;

    console.log("PRICING PREVIEW estimatedSeconds:", estimatedSeconds);

    const price = calculatePriceFromInputs(
      photosCount,
      totalBytes,
      estimatedSeconds,
      qualityMode,
      outputsRequested,
      projectType
    );

    console.log("PRICING PREVIEW price:", price);

    const quotePayload = {
      version: 1,
      issued_at: Math.floor(Date.now() / 1000),
      expires_at: Math.floor(Date.now() / 1000) + PRICING_QUOTE_TTL_SECONDS,
      predictor_version: TIMING_PREDICTOR_VERSION,
      photos_count: photosCount,
      total_bytes: totalBytes,
      quality_mode: qualityMode,
      outputs_requested: outputsRequested,
      project_type: projectType,
      tams_export: tamsExport,
      avg_photo_mb: previewStats.avgPhotoMb,
      avg_width: previewStats.avgWidth,
      avg_height: previewStats.avgHeight,
      avg_megapixels: previewStats.avgMegapixels,
      total_megapixels: previewStats.totalMegapixels,
      processing_load_score: previewLoadScore,
      estimated_seconds: estimatedSeconds,
      price
    };
    const pricingQuote = createPricingQuote(quotePayload);

    return res.json({
      ok: true,
      photos_count: photosCount,
      total_bytes: totalBytes,
      quality_mode: qualityMode,
      quality_mode_label: getQualityModeLabel(qualityMode),
      tams_export: tamsExport,
      price,
      pricing_quote: pricingQuote,
      quote_expires_at: quotePayload.expires_at,
      estimated_seconds: estimatedSeconds,
      estimated_low_seconds: timingPlan.estimated_processing_low_seconds,
      estimated_high_seconds: timingPlan.estimated_processing_high_seconds,
      estimated_human: formatEtaSeconds(estimatedSeconds),
      estimated_total_seconds: timingPlan.estimated_total_service_seconds,
      estimated_total_low_seconds: timingPlan.estimated_total_low_seconds,
      estimated_total_high_seconds: timingPlan.estimated_total_high_seconds,
      estimated_total_human: formatEtaSeconds(timingPlan.estimated_total_service_seconds),
      confidence: timingPlan.confidence,
      timing_prediction: timingPlan
    });
  } catch (e) {
    console.error("pricing preview error", e);
    res.status(500).json({ ok: false, error: "pricing preview error" });
  }
});

// =====================
// AUTH
// =====================
app.post("/auth/register", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const password = String(req.body?.password || "");
    const name = String(req.body?.name || "").trim();

    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_email",
        message: "Email no válido"
      });
    }

    if (!password || password.length < 6) {
      return res.status(400).json({
        ok: false,
        error: "invalid_password",
        message: "La contraseña debe tener al menos 6 caracteres"
      });
    }

    const existing = await pool.query(
      `select id from users where email = $1 limit 1`,
      [email]
    );

    if (existing.rows.length) {
      return res.status(409).json({
        ok: false,
        error: "email_exists",
        message: "Ese email ya está registrado"
      });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const { rows } = await pool.query(
      `insert into users (email, password_hash, name)
       values ($1, $2, $3)
       returning id, email, name, created_at`,
      [email, passwordHash, name || null]
    );

    return res.json({
      ok: true,
      user: rows[0]
    });
  } catch (e) {
    console.error("register error", e);
    res.status(500).json({
      ok: false,
      error: "register_error"
    });
  }
});

app.post("/auth/login", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const password = String(req.body?.password || "");

    if (!email || !password) {
      return res.status(400).json({
        ok: false,
        error: "missing_credentials",
        message: "Faltan credenciales"
      });
    }

    const { rows } = await pool.query(
      `select id, email, name, password_hash
       from users
       where email = $1
       limit 1`,
      [email]
    );

    if (!rows.length) {
      return res.status(401).json({
        ok: false,
        error: "invalid_credentials",
        message: "Credenciales incorrectas"
      });
    }

    const user = rows[0];
    const valid = await bcrypt.compare(password, user.password_hash);

    if (!valid) {
      return res.status(401).json({
        ok: false,
        error: "invalid_credentials",
        message: "Credenciales incorrectas"
      });
    }

    return res.json({
      ok: true,
      user: {
        id: user.id,
        email: user.email,
        name: user.name
      }
    });
  } catch (e) {
    console.error("login error", e);
    res.status(500).json({
      ok: false,
      error: "login_error"
    });
  }
});

app.post("/auth/invite-login", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const name = String(req.body?.name || "").trim();
    const code = String(req.body?.code || "").trim().toUpperCase();

    if (!email || !isValidEmail(email)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_email",
        message: "Email no válido"
      });
    }

    if (!name) {
      return res.status(400).json({
        ok: false,
        error: "missing_name",
        message: "Debe indicar nombre o empresa"
      });
    }

    if (!code) {
      return res.status(400).json({
        ok: false,
        error: "missing_code",
        message: "Debe indicar un código"
      });
    }

    const invite = await pool.query(
      `select *
       from invite_codes
       where code = $1
       limit 1`,
      [code]
    );

    if (!invite.rows.length) {
      return res.status(404).json({
        ok: false,
        error: "invalid_code",
        message: "Código no válido"
      });
    }

    const inviteRow = invite.rows[0];

    if (inviteRow.is_used) {
      return res.status(409).json({
        ok: false,
        error: "code_already_used",
        message: "Ese código ya ha sido utilizado"
      });
    }

    let userRow = null;

    const existingUser = await pool.query(
      `select id, email, name
       from users
       where email = $1
       limit 1`,
      [email]
    );

    if (existingUser.rows.length) {
      userRow = existingUser.rows[0];
    } else {
      const guestPasswordHash = await bcrypt.hash(`guest:${code}:${Date.now()}`, 10);

      const createdUser = await pool.query(
        `insert into users (email, password_hash, name)
         values ($1, $2, $3)
         returning id, email, name`,
        [email, guestPasswordHash, name]
      );

      userRow = createdUser.rows[0];
    }

    await pool.query(
      `update invite_codes
          set is_used = true,
              used_at = now(),
              used_by_email = $2,
              used_by_name = $3
        where id = $1`,
      [inviteRow.id, email, name]
    );

    return res.json({
      ok: true,
      user: {
        id: userRow.id,
        email: userRow.email,
        name: userRow.name,
        auth_type: "invite"
      }
    });
  } catch (e) {
    console.error("invite login error", e);
    res.status(500).json({
      ok: false,
      error: "invite_login_error"
    });
  }
});

app.post("/admin/invite-codes/generate", requireAdmin, async (req, res) => {
  try {
    const count = Number(req.body?.count || 50);
    const rowsCreated = [];

    for (let i = 0; i < count; i++) {
      let code;
      let inserted = false;

      while (!inserted) {
        code = generateInviteCode();

        try {
          const { rows } = await pool.query(
            `insert into invite_codes (code)
             values ($1)
             returning *`,
            [code]
          );
          rowsCreated.push(rows[0]);
          inserted = true;
        } catch (e) {
          if (!String(e.message || "").toLowerCase().includes("duplicate")) {
            throw e;
          }
        }
      }
    }

    res.json({
      ok: true,
      created: rowsCreated.length,
      codes: rowsCreated
    });
  } catch (e) {
    console.error("generate invite codes error", e);
    res.status(500).json({
      ok: false,
      error: "generate_invite_codes_error"
    });
  }
});

app.get("/admin/invite-codes", requireAdmin, async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `select
         id,
         code,
         is_used,
         used_at,
         used_by_email,
         used_by_name,
         created_at
       from invite_codes
       order by created_at desc
       limit 500`
    );

    res.json({
      ok: true,
      codes: rows
    });
  } catch (e) {
    console.error("list invite codes error", e);
    res.status(500).json({
      ok: false,
      error: "list_invite_codes_error"
    });
  }
});
// =====================
// JOBS
// =====================
app.get("/jobs", async (_req, res) => {
  try {
    const { rows } = await pool.query(
      "select * from jobs order by created_at desc limit 50"
    );
    res.json(rows);
  } catch (e) {
    console.error("list jobs error", e);
    res.status(500).json({ error: "db error" });
  }
});

app.get("/jobs/mine", async (req, res) => {
  try {
    await ensureDownloadCleanupColumns();

    const userIdRaw =
      req.headers["x-user-id"] ||
      req.query.user_id ||
      req.query.userId ||
      "";

    const userId = String(userIdRaw || "").trim();

    if (!userId || userId === "null" || userId === "undefined") {
      return res.status(401).json({
        ok: false,
        error: "missing_user",
        message: "Falta user_id"
      });
    }

    const selectSql = jobsHasQualityMode
      ? `select
          id,
          project_name,
          status,
          stage,
          progress,
          price,
          created_at,
          quality_mode,
          exif_summary,
          download_verified,
          output_purged,
          archived_summary_saved_at,
          archived_outputs
        from jobs
        where user_id = $1
        order by created_at desc`
      : `select
          id,
          project_name,
          status,
          stage,
          progress,
          price,
          created_at,
          exif_summary,
          download_verified,
          output_purged,
          archived_summary_saved_at,
          archived_outputs
        from jobs
        where user_id = $1
        order by created_at desc`;

    const { rows } = await pool.query(selectSql, [userId]);

    res.json({
      ok: true,
      jobs: rows.map((job) => ({
        ...job,
        quality_mode: normalizeQualityMode(job.quality_mode || job?.exif_summary?._xproces?.quality_mode || "normal")
      }))
    });
  } catch (e) {
    console.error("jobs/mine error", e);
    res.status(500).json({
      ok: false,
      error: "jobs_mine_error",
      message: e?.message || String(e)
    });
  }
});

function getJobUploadProgress(job) {
  const expectedPhotos = Number(job?.exif_summary?._xproces?.totalPhotos || 0) || 0;
  const expectedBytes = Number(job?.exif_summary?._xproces?.totalBytes || 0) || 0;
  const files = listInputImages(job.id);
  const receivedPhotos = files.length;
  const receivedBytes = getInputTotalBytes(job.id);

  let lastReceivedAt = null;
  for (const filename of files) {
    try {
      const mtime = fs.statSync(path.join(inputDir(job.id), filename)).mtime;
      if (!lastReceivedAt || mtime > lastReceivedAt) lastReceivedAt = mtime;
    } catch (_) {}
  }

  const idleSeconds = lastReceivedAt
    ? Math.max(0, Math.floor((Date.now() - lastReceivedAt.getTime()) / 1000))
    : null;

  let percent = 0;
  if (expectedBytes > 0) percent = Math.min(100, (receivedBytes / expectedBytes) * 100);
  else if (expectedPhotos > 0) percent = Math.min(100, (receivedPhotos / expectedPhotos) * 100);

  return {
    received_photos: receivedPhotos,
    expected_photos: expectedPhotos,
    received_bytes: receivedBytes,
    expected_bytes: expectedBytes,
    percent: Number(percent.toFixed(1)),
    last_received_at: lastReceivedAt ? lastReceivedAt.toISOString() : null,
    idle_seconds: idleSeconds,
    complete: Boolean(
      expectedPhotos > 0 &&
      receivedPhotos >= expectedPhotos &&
      (expectedBytes <= 0 || receivedBytes === expectedBytes)
    )
  };
}

app.get("/jobs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!isValidUuid(id)) {
      return res.status(400).json({ ok: false, error: "invalid job id" });
    }

    const { rows } = await pool.query("select * from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "not found" });

    const job = rows[0];
    res.json({
      ...job,
      ok: true,
      quality_mode: normalizeQualityMode(job.quality_mode || job?.exif_summary?._xproces?.quality_mode || "normal"),
      upload_progress: ["created", "pending_payment", "receiving"].includes(String(job.status || "").toLowerCase())
        ? getJobUploadProgress(job)
        : null
    });
  } catch (e) {
    console.error("get job error", e);
    res.status(500).json({ ok: false, error: "db error", message: e?.message || String(e) });
  }
});

// =====================
// PAYPAL CHECKOUT
// =====================
// =====================
// TELEGRAM NOTIFICATIONS
// =====================

// =====================
// TELEGRAM USUARIOS
// =====================
app.get("/telegram/status", async (req, res) => {
  try {
    const user = await requireExistingUser(req, res);
    if (!user) return;
    await ensureTelegramSchema();

    const settings = await getTelegramUserSettings(user.id);
    const pending = await pool.query(
      `select code, expires_at from telegram_link_codes
        where user_id=$1 and used_at is null and expires_at > now()
        order by created_at desc limit 1`,
      [user.id]
    );

    return res.json({
      ok: true,
      configured: Boolean(String(process.env.TELEGRAM_BOT_TOKEN || "").trim()),
      connected: Boolean(settings?.is_active && settings?.chat_id),
      pending: pending.rows.length > 0,
      link_code: pending.rows[0]?.code || null,
      link_expires_at: pending.rows[0]?.expires_at || null,
      bot_username: TELEGRAM_BOT_USERNAME,
      username: settings?.username || null,
      first_name: settings?.first_name || null,
      preferences: normalizeTelegramPreferences(settings?.preferences)
    });
  } catch (e) {
    console.error("telegram status error", e);
    res.status(500).json({ ok: false, error: "telegram_status_error" });
  }
});

app.post("/telegram/link-code", async (req, res) => {
  try {
    const user = await requireExistingUser(req, res);
    if (!user) return;
    if (!String(process.env.TELEGRAM_BOT_TOKEN || "").trim()) {
      return res.status(503).json({ ok: false, error: "telegram_not_configured", message: "Telegram no está configurado en el servidor" });
    }
    await ensureTelegramSchema();
    await pool.query(`delete from telegram_link_codes where expires_at <= now() or used_at is not null`);
    await pool.query(`delete from telegram_link_codes where user_id=$1 and used_at is null`, [user.id]);

    let code = null;
    for (let attempt = 0; attempt < 10; attempt++) {
      const candidate = generateTelegramLinkCode();
      try {
        await pool.query(
          `insert into telegram_link_codes(code, user_id, expires_at)
           values($1,$2,now() + ($3 || ' minutes')::interval)`,
          [candidate, user.id, TELEGRAM_LINK_CODE_TTL_MINUTES]
        );
        code = candidate;
        break;
      } catch (e) {
        if (e?.code !== "23505") throw e;
      }
    }
    if (!code) throw new Error("No se pudo generar un código único");

    res.json({
      ok: true,
      pending: true,
      link_code: code,
      expires_in_minutes: TELEGRAM_LINK_CODE_TTL_MINUTES,
      bot_username: TELEGRAM_BOT_USERNAME,
      command: `/start ${code}`,
      bot_url: `https://t.me/${TELEGRAM_BOT_USERNAME}?start=${encodeURIComponent(code)}`
    });
  } catch (e) {
    console.error("telegram link-code error", e);
    res.status(500).json({ ok: false, error: "telegram_link_code_error" });
  }
});

app.post("/telegram/preferences", async (req, res) => {
  try {
    const user = await requireExistingUser(req, res);
    if (!user) return;
    await ensureTelegramSchema();
    const preferences = normalizeTelegramPreferences(req.body?.preferences);
    await pool.query(
      `insert into telegram_user_settings(user_id, preferences, updated_at)
       values($1,$2::jsonb,now())
       on conflict(user_id) do update set preferences=excluded.preferences, updated_at=now()`,
      [user.id, JSON.stringify(preferences)]
    );
    res.json({ ok: true, preferences });
  } catch (e) {
    console.error("telegram preferences error", e);
    res.status(500).json({ ok: false, error: "telegram_preferences_error" });
  }
});

app.post("/telegram/test", async (req, res) => {
  try {
    const user = await requireExistingUser(req, res);
    if (!user) return;
    const result = await sendUserTelegramEvent(user.id, null, [
      "✅ <b>Telegram conectado correctamente</b>", "", "Este es un mensaje de prueba de XProces."
    ].join("\n"));
    if (!result.ok) {
      return res.status(result.skipped ? 409 : 502).json({ ok: false, error: result.error || "telegram_test_error", message: "No se pudo enviar el mensaje de prueba" });
    }
    res.json({ ok: true, message_id: result.messageId || null });
  } catch (e) {
    console.error("telegram user test error", e);
    res.status(500).json({ ok: false, error: "telegram_user_test_error" });
  }
});

app.post("/telegram/disconnect", async (req, res) => {
  try {
    const user = await requireExistingUser(req, res);
    if (!user) return;
    await ensureTelegramSchema();
    await pool.query(
      `insert into telegram_user_settings(user_id, is_active, disconnected_at, updated_at)
       values($1,false,now(),now())
       on conflict(user_id) do update set chat_id=null, username=null, first_name=null, is_active=false, disconnected_at=now(), updated_at=now()`,
      [user.id]
    );
    await pool.query(`delete from telegram_link_codes where user_id=$1`, [user.id]);
    res.json({ ok: true, connected: false });
  } catch (e) {
    console.error("telegram disconnect error", e);
    res.status(500).json({ ok: false, error: "telegram_disconnect_error" });
  }
});

app.post("/telegram/webhook", async (req, res) => {
  try {
    if (TELEGRAM_WEBHOOK_SECRET) {
      const receivedSecret = String(req.headers["x-telegram-bot-api-secret-token"] || "").trim();
      if (receivedSecret !== TELEGRAM_WEBHOOK_SECRET) {
        return res.status(401).json({ ok: false, error: "invalid_telegram_webhook_secret" });
      }
    }

    const message = req.body?.message || req.body?.edited_message;
    const text = String(message?.text || "").trim();
    const chatId = message?.chat?.id;
    if (!message || !chatId || !text) return res.json({ ok: true, ignored: true });

    const startMatch = text.match(/^\/start(?:@\w+)?\s+(XP-\d{6})\s*$/i);
    const legacyMatch = text.match(/^\/vincular(?:@\w+)?\s+(XP-\d{6})\s*$/i);
    const match = startMatch || legacyMatch;
    if (!match) {
      if (/^\/start(?:@\w+)?(?:\s*)$/i.test(text)) {
        await sendTelegramMessage("Hola. Para conectar tu cuenta, abre «Mi perfil» en XProces y pulsa «Conectar Telegram».", { chatId: String(chatId) });
      }
      return res.json({ ok: true, ignored: true });
    }

    await ensureTelegramSchema();
    const code = match[1].toUpperCase();
    const client = await pool.connect();
    try {
      await client.query("begin");
      const linkResult = await client.query(
        `select code, user_id, expires_at, used_at from telegram_link_codes where code=$1 for update`,
        [code]
      );
      if (!linkResult.rows.length || linkResult.rows[0].used_at || new Date(linkResult.rows[0].expires_at).getTime() <= Date.now()) {
        await client.query("rollback");
        await sendTelegramMessage("❌ El código no es válido o ha caducado. Genera uno nuevo desde Mi perfil en XProces.", { chatId: String(chatId) });
        return res.json({ ok: true, linked: false });
      }

      const userId = linkResult.rows[0].user_id;
      await client.query(
        `insert into telegram_user_settings(user_id, chat_id, username, first_name, is_active, linked_at, disconnected_at, updated_at)
         values($1,$2,$3,$4,true,now(),null,now())
         on conflict(user_id) do update set chat_id=excluded.chat_id, username=excluded.username, first_name=excluded.first_name,
           is_active=true, linked_at=now(), disconnected_at=null, updated_at=now()`,
        [userId, String(chatId), message?.from?.username || null, message?.from?.first_name || null]
      );
      await client.query(`update telegram_link_codes set used_at=now() where code=$1`, [code]);
      await client.query(`delete from telegram_link_codes where user_id=$1 and code<>$2`, [userId, code]);
      await client.query("commit");

      await sendTelegramMessage("✅ <b>Cuenta vinculada</b>\n\nTelegram ya está conectado con tu cuenta de XProces.", { chatId: String(chatId) });
      return res.json({ ok: true, linked: true });
    } catch (e) {
      await client.query("rollback").catch(() => {});
      throw e;
    } finally {
      client.release();
    }
  } catch (e) {
    console.error("telegram webhook error", e);
    res.status(500).json({ ok: false, error: "telegram_webhook_error" });
  }
});

app.get("/admin/telegram/webhook", requireAdmin, async (_req, res) => {
  try {
    const token = String(process.env.TELEGRAM_BOT_TOKEN || "").trim();
    if (!token) return res.status(503).json({ ok: false, error: "telegram_not_configured" });
    const response = await fetch(`https://api.telegram.org/bot${encodeURIComponent(token)}/getWebhookInfo`);
    const data = await response.json().catch(() => ({}));
    res.status(response.ok && data?.ok ? 200 : 502).json({ ok: Boolean(response.ok && data?.ok), webhook: data?.result || null, telegram: data });
  } catch (e) {
    res.status(500).json({ ok: false, error: "telegram_webhook_info_error" });
  }
});

app.post("/admin/telegram/webhook", requireAdmin, async (req, res) => {
  try {
    const token = String(process.env.TELEGRAM_BOT_TOKEN || "").trim();
    const url = String(req.body?.url || "").trim();
    if (!token) return res.status(503).json({ ok: false, error: "telegram_not_configured" });
    if (!/^https:\/\//i.test(url)) return res.status(400).json({ ok: false, error: "invalid_webhook_url", message: "La URL debe usar HTTPS" });
    const payload = { url, allowed_updates: ["message", "edited_message"] };
    if (TELEGRAM_WEBHOOK_SECRET) payload.secret_token = TELEGRAM_WEBHOOK_SECRET;
    const response = await fetch(`https://api.telegram.org/bot${encodeURIComponent(token)}/setWebhook`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload)
    });
    const data = await response.json().catch(() => ({}));
    res.status(response.ok && data?.ok ? 200 : 502).json({ ok: Boolean(response.ok && data?.ok), telegram: data });
  } catch (e) {
    res.status(500).json({ ok: false, error: "telegram_set_webhook_error" });
  }
});

app.get("/admin/telegram/config", requireAdmin, (_req, res) => {
  res.json({
    ok: true,
    enabled: telegramIsConfigured(),
    chat_id_configured: Boolean(String(process.env.TELEGRAM_CHAT_ID || "").trim()),
    bot_token_configured: Boolean(String(process.env.TELEGRAM_BOT_TOKEN || "").trim())
  });
});

app.post("/admin/telegram/test", requireAdmin, async (_req, res) => {
  const result = await sendTelegramMessage(
    [
      "✅ <b>Telegram conectado con XProces</b>",
      "",
      "Este es un mensaje de prueba enviado desde el servidor.",
      `🕒 ${new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", dateStyle: "short", timeStyle: "medium" }).format(new Date())}`
    ].join("\n")
  );

  if (!result.ok) {
    return res.status(result.skipped ? 503 : 502).json(result);
  }

  return res.json(result);
});

app.get("/paypal/config", (_req, res) => {
  res.json({
    ok: true,
    enabled: paypalIsConfigured(),
    client_id: PAYPAL_CLIENT_ID || null,
    currency: PAYPAL_CURRENCY,
    environment: PAYPAL_ENV
  });
});

app.post("/jobs/:id/paypal/create-order", async (req, res) => {
  try {
    await ensurePaymentColumns();
    if (!paypalIsConfigured()) {
      return res.status(503).json({ ok: false, error: "paypal_not_configured", message: "PayPal no está configurado en el servidor." });
    }

    const { rows } = await pool.query("select * from jobs where id=$1", [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "job_not_found" });
    const job = rows[0];
    const requestUserId = String(req.headers["x-user-id"] || "").trim();
    if (job.user_id && String(job.user_id) !== requestUserId) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }
    if (String(job.payment_status || "").toLowerCase() === "paid") {
      return res.json({ ok: true, already_paid: true, order_id: job.payment_order_id, amount: Number(job.payment_amount || job.price || 0), currency: job.payment_currency || PAYPAL_CURRENCY });
    }

    // PayPal debe cobrar exactamente el precio calculado y guardado al crear el trabajo.
    // No se vuelve a estimar aquí para evitar diferencias con el importe mostrado en la web.
    const price = Number(job.price);

    if (!Number.isFinite(Number(price)) || Number(price) <= 0) {
      return res.status(409).json({
        ok: false,
        error: "invalid_payment_amount",
        message: "No se ha podido calcular un importe válido para este proyecto. El trabajo no puede iniciarse sin pago."
      });
    }

    const order = await paypalRequest("/v2/checkout/orders", {
      method: "POST",
      headers: { "PayPal-Request-Id": `xproces-create-${job.id}-${Date.now()}` },
      body: JSON.stringify({
        intent: "CAPTURE",
        purchase_units: [{
          custom_id: String(job.id),
          description: `XPROCES - ${String(job.project_name || "Proyecto fotogramétrico").slice(0, 120)}`,
          amount: { currency_code: PAYPAL_CURRENCY, value: Number(price).toFixed(2) }
        }],
        application_context: { shipping_preference: "NO_SHIPPING", user_action: "PAY_NOW" }
      })
    });

    await pool.query(
      `update jobs set status='pending_payment', stage='pending_payment', price=$2, payment_status='created', payment_provider='paypal', payment_order_id=$3, payment_amount=$2, payment_currency=$4, message='Pendiente de pago', updated_at=now() where id=$1`,
      [job.id, price, order.id, PAYPAL_CURRENCY]
    );

    res.json({ ok: true, order_id: order.id, amount: price, currency: PAYPAL_CURRENCY });
  } catch (e) {
    console.error("paypal create-order error", e?.paypal || e);
    res.status(500).json({ ok: false, error: "paypal_create_order_error", message: e?.message || "No se pudo crear el pago." });
  }
});

app.post("/jobs/:id/paypal/capture-order", async (req, res) => {
  try {
    await ensurePaymentColumns();
    const orderId = String(req.body?.order_id || "").trim();
    if (!orderId) return res.status(400).json({ ok: false, error: "missing_order_id" });

    const { rows } = await pool.query("select * from jobs where id=$1", [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "job_not_found" });
    const job = rows[0];
    const requestUserId = String(req.headers["x-user-id"] || "").trim();
    if (job.user_id && String(job.user_id) !== requestUserId) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }
    if (String(job.payment_status || "").toLowerCase() === "paid") {
      return res.json({
        ok: true,
        paid: true,
        already_paid: true,
        capture_id: job.payment_capture_id,
        amount: Number(job.payment_amount || 0),
        currency: job.payment_currency || PAYPAL_CURRENCY,
        payer_id: job.paypal_payer_id || null,
        payer_email: job.paypal_payer_email || null,
        payment_date: job.payment_date || job.paid_at || null
      });
    }
    if (String(job.payment_order_id || "") !== orderId) {
      return res.status(409).json({ ok: false, error: "order_mismatch", message: "La orden de PayPal no corresponde a este proyecto." });
    }

    let order;
    try {
      order = await paypalRequest(`/v2/checkout/orders/${encodeURIComponent(orderId)}/capture`, {
        method: "POST",
        headers: { "PayPal-Request-Id": `xproces-capture-${job.id}-${orderId}` },
        body: "{}"
      });
    } catch (captureError) {
      // Una repetición puede devolver ORDER_ALREADY_CAPTURED. Consultamos la orden y validamos su captura real.
      order = await paypalRequest(`/v2/checkout/orders/${encodeURIComponent(orderId)}`, { method: "GET" });
    }

    const unit = order?.purchase_units?.[0] || {};
    const capture = unit?.payments?.captures?.find((item) => String(item?.status || "").toUpperCase() === "COMPLETED") || unit?.payments?.captures?.[0];
    const amount = Number(capture?.amount?.value || unit?.amount?.value || 0);
    const currency = String(capture?.amount?.currency_code || unit?.amount?.currency_code || "").toUpperCase();
    const customId = String(unit?.custom_id || "");
    const expectedAmount = Number(job.payment_amount || job.price || 0);
    const payerId = String(order?.payer?.payer_id || "").trim() || null;
    const payerEmail = String(order?.payer?.email_address || "").trim().toLowerCase() || null;
    const paymentDate = capture?.create_time || capture?.update_time || order?.update_time || null;

    if (String(order?.status || "").toUpperCase() !== "COMPLETED" || String(capture?.status || "").toUpperCase() !== "COMPLETED") {
      return res.status(409).json({ ok: false, error: "payment_not_completed", message: "PayPal todavía no ha confirmado el pago." });
    }
    // La respuesta de CAPTURE de PayPal puede omitir purchase_units[].custom_id.
    // La orden ya se ha vinculado de forma segura al job mediante payment_order_id.
    // Si custom_id viene informado, debe coincidir; si viene vacío, validamos por orden, importe y moneda.
    const customIdMatches = !customId || customId === String(job.id);
    if (!customIdMatches || currency !== PAYPAL_CURRENCY || Math.abs(amount - expectedAmount) > 0.001) {
      console.error("PayPal validation mismatch", {
        jobId: job.id,
        orderId,
        expectedOrderId: String(job.payment_order_id || ""),
        customId,
        expectedCustomId: String(job.id),
        currency,
        expectedCurrency: PAYPAL_CURRENCY,
        amount,
        expectedAmount
      });
      return res.status(409).json({ ok: false, error: "payment_validation_failed", message: "El importe o la referencia del pago no coinciden con el proyecto." });
    }

    const captureId = String(capture?.id || "").trim();
    if (!captureId) {
      return res.status(409).json({
        ok: false,
        error: "missing_capture_id",
        message: "PayPal confirmó el pago, pero no devolvió el identificador de captura."
      });
    }

    // Protección adicional: una captura no puede utilizarse para pagar otro proyecto.
    const duplicateCapture = await pool.query(
      `select id from jobs where payment_capture_id=$1 and id<>$2 limit 1`,
      [captureId, job.id]
    );
    if (duplicateCapture.rows.length) {
      console.error("PayPal capture already assigned", {
        captureId,
        currentJobId: job.id,
        existingJobId: duplicateCapture.rows[0].id
      });
      return res.status(409).json({
        ok: false,
        error: "capture_already_used",
        message: "Esta captura de PayPal ya está vinculada a otro proyecto."
      });
    }

    const updateResult = await pool.query(
      `update jobs
          set status='created',
              stage='created',
              payment_status='paid',
              payment_provider='paypal',
              payment_capture_id=$2,
              payment_amount=$3,
              payment_currency=$4,
              paypal_payer_id=$5,
              paypal_payer_email=$6,
              payment_date=coalesce(payment_date, $7::timestamptz, now()),
              paid_at=coalesce(paid_at, $7::timestamptz, now()),
              message='Pago confirmado. Preparado para subir fotografías.',
              updated_at=now()
        where id=$1
          and payment_status <> 'paid'
        returning id, payment_capture_id, payment_amount, payment_currency,
                  paypal_payer_id, paypal_payer_email,
                  coalesce(payment_date, paid_at) as payment_date`,
      [job.id, captureId, amount, currency, payerId, payerEmail, paymentDate]
    );

    // Solo la petición que cambia realmente el estado a paid envía el aviso.
    // Un fallo de Telegram nunca revierte ni bloquea el pago confirmado.
    if (updateResult.rows.length) {
      const telegramResult = await notifyPaymentReceived({
        job,
        payment: {
          captureId,
          amount,
          currency,
          payerId,
          payerEmail,
          paymentDate
        }
      });

      if (!telegramResult.ok && !telegramResult.skipped) {
        console.error("Pago confirmado, pero no se pudo enviar el aviso de Telegram", {
          jobId: job.id,
          captureId,
          error: telegramResult.error
        });
      }

      try {
        const userTelegramResult = await notifyUserPaymentReceived(job, { captureId, amount, currency, payerId, payerEmail, paymentDate });
        if (!userTelegramResult.ok && !userTelegramResult.skipped) {
          console.error("Pago confirmado, pero no se pudo avisar al usuario por Telegram", userTelegramResult.error);
        }
      } catch (telegramUserError) {
        console.error("Error en aviso Telegram del usuario tras el pago", telegramUserError?.message || telegramUserError);
      }
    }

    // Si dos peticiones llegaron a la vez, la segunda se trata como respuesta idempotente.
    if (!updateResult.rows.length) {
      const current = await pool.query(
        `select payment_status, payment_capture_id, payment_amount, payment_currency,
                paypal_payer_id, paypal_payer_email,
                coalesce(payment_date, paid_at) as payment_date
           from jobs where id=$1`,
        [job.id]
      );
      const paidJob = current.rows[0];
      if (String(paidJob?.payment_status || "").toLowerCase() === "paid") {
        return res.json({
          ok: true,
          paid: true,
          already_paid: true,
          capture_id: paidJob.payment_capture_id,
          amount: Number(paidJob.payment_amount || 0),
          currency: paidJob.payment_currency || PAYPAL_CURRENCY,
          payer_id: paidJob.paypal_payer_id || null,
          payer_email: paidJob.paypal_payer_email || null,
          payment_date: paidJob.payment_date || null
        });
      }
    }

    res.json({
      ok: true,
      paid: true,
      capture_id: captureId,
      amount,
      currency,
      payer_id: payerId,
      payer_email: payerEmail,
      payment_date: paymentDate
    });
  } catch (e) {
    console.error("paypal capture-order error", e?.paypal || e);
    res.status(500).json({ ok: false, error: "paypal_capture_error", message: e?.message || "No se pudo confirmar el pago." });
  }
});

app.post("/jobs/:id/paypal/cancel", async (req, res) => {
  try {
    await ensurePaymentColumns();
    const { rows } = await pool.query("select id, status, payment_status, user_id from jobs where id=$1", [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "job_not_found" });
    const job = rows[0];
    const requestUserId = String(req.headers["x-user-id"] || "").trim();
    if (job.user_id && String(job.user_id) !== requestUserId) return res.status(403).json({ ok: false, error: "forbidden" });
    if (String(job.payment_status || "").toLowerCase() === "paid") return res.status(409).json({ ok: false, error: "already_paid" });
    await pool.query(`update jobs set status='cancelled', stage='cancelled', payment_status='cancelled', message='Pago cancelado', updated_at=now(), finished_at=now() where id=$1`, [job.id]);
    cleanupJobStorage(job.id, { input: true, output: true });
    res.json({ ok: true, cancelled: true });
  } catch (e) {
    console.error("paypal cancel error", e);
    res.status(500).json({ ok: false, error: "paypal_cancel_error" });
  }
});

// ✅ OPCION A: crear job (NO ENCOLA AQUI)
// 🔥 v13: ya NO exige photos_count. Crea job “created” con 0 fotos y precio 0.
app.post("/jobs", async (req, res) => {
  try {
   
const exifSummaryRaw = stripNullCharsDeep(req.body?.exif_summary || null);
const outputsRequested = stripNullCharsDeep(req.body?.outputs_requested || []);
const presetKey = stripNullCharsDeep(req.body?.preset_key || null);
const outputMode = stripNullCharsDeep(req.body?.output_mode || null);
const projectType = String(req.body?.project_type || exifSummaryRaw?._xproces?.project_type || "fotogrametria").toLowerCase();
const tamsExport = !!req.body?.tams_export || projectType === "tams";
console.log("[XPROCES QUALITY] BODY:", req.body);
console.log("[XPROCES QUALITY] RAW:", req.body?.quality_mode);
const qualityMode = normalizeQualityMode(req.body?.quality_mode);
console.log("[XPROCES QUALITY] NORMALIZED:", qualityMode);
const expectedPhotosCount = Number(req.body?.expected_photos_count || exifSummaryRaw?._xproces?.totalPhotos || 0) || 0;
const expectedTotalBytes = Number(req.body?.expected_total_bytes || exifSummaryRaw?._xproces?.totalBytes || 0) || 0;
const pricingQuote = verifyPricingQuote(req.body?.pricing_quote);
if (projectType !== "tams" && !pricingQuote) {
  return res.status(409).json({
    ok: false,
    error: "pricing_quote_invalid",
    message: "La cotización ha caducado o no es válida. Vuelva a calcular el precio antes de continuar."
  });
}
if (pricingQuote) {
  const quoteOutputs = normalizeOutputList(pricingQuote.outputs_requested);
  if (Number(pricingQuote.photos_count) !== expectedPhotosCount ||
      Number(pricingQuote.total_bytes) !== expectedTotalBytes ||
      normalizeQualityMode(pricingQuote.quality_mode) !== qualityMode ||
      !outputSetsEqual(quoteOutputs, outputsRequested) ||
      String(pricingQuote.project_type || "") !== projectType ||
      !!pricingQuote.tams_export !== !!tamsExport) {
    return res.status(409).json({
      ok: false,
      error: "pricing_quote_mismatch",
      message: "Las fotos, la calidad o los entregables han cambiado. Vuelva a calcular la cotización."
    });
  }
}

let exifSummary = stripNullCharsDeep({
  ...(exifSummaryRaw || {}),
  _xproces: {
    // 🔹 conserva lo que venga del frontend (CLAVE)
    ...((exifSummaryRaw && exifSummaryRaw._xproces) || {}),

    project_type: projectType,

    // 🔹 siempre actualiza outputs (esto ya lo hacías bien)
    outputs_requested: outputsRequested,

    // 🔹 si viene en body lo usa, si no usa lo del frontend
    preset_key:
      presetKey ??
      exifSummaryRaw?._xproces?.preset_key ??
      null,

    output_mode:
      outputMode ??
      exifSummaryRaw?._xproces?.output_mode ??
      null,

    tams_export: !!(
      req.body?.tams_export ??
      exifSummaryRaw?._xproces?.tams_export
    ),
    quality_mode:
      qualityMode ??
      exifSummaryRaw?._xproces?.quality_mode ??
      "normal",
    totalPhotos: expectedPhotosCount,
    totalBytes: expectedTotalBytes
  }
});

const clientEmail = req.body?.client_email || null;
const projectName = req.body?.project_name || null;
const clientName = req.body?.client_name || null;
const userId = req.body?.user_id || null;
    

    if (!clientEmail || !isValidEmail(clientEmail)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_client_email",
        message: "El email del cliente no es válido"
      });
    }

    await ensurePaymentColumns();
    await ensureTimingAnalyticsSchema();

    const declaredStats = pricingQuote ? {
      avgPhotoMb: Number(pricingQuote.avg_photo_mb || 0),
      avgWidth: Number(pricingQuote.avg_width || 0),
      avgHeight: Number(pricingQuote.avg_height || 0),
      avgMegapixels: Number(pricingQuote.avg_megapixels || 0),
      totalMegapixels: Number(pricingQuote.total_megapixels || 0)
    } : extractDeclaredImageStats(exifSummary, expectedPhotosCount, expectedTotalBytes);
    const initialLoadScore = pricingQuote
      ? Number(pricingQuote.processing_load_score || 0)
      : calculateProcessingLoadScore({
          photosCount: expectedPhotosCount,
          totalBytes: expectedTotalBytes,
          avgMegapixels: declaredStats.avgMegapixels,
          totalMegapixels: declaredStats.totalMegapixels,
          qualityMode,
          outputsRequested,
          projectType
        });
    const estimatedSecondsForPayment = pricingQuote
      ? Number(pricingQuote.estimated_seconds || 0)
      : 0;
    const initialPrice = pricingQuote
      ? Number(pricingQuote.price || 0)
      : calculatePriceFromInputs(expectedPhotosCount, expectedTotalBytes, 0, qualityMode, outputsRequested, projectType);

    exifSummary = stripNullCharsDeep({
      ...(exifSummary || {}),
      _xproces: {
        ...((exifSummary && exifSummary._xproces) || {}),
        processing_load_score: initialLoadScore,
        pricing_quote_locked: true,
        pricing_quote_issued_at: pricingQuote?.issued_at || null,
        pricing_quote_expires_at: pricingQuote?.expires_at || null,
        prediction_method: "locked_web_quote",
        predictor_version: pricingQuote?.predictor_version || TIMING_PREDICTOR_VERSION
      }
    });
    const initialPaymentStatus = initialPrice <= 0 ? "exempt" : "pending";

    const insertSql = `insert into jobs (
        status,
        photos_count,
        price,
        exif_summary,
        client_email,
        project_name,
        client_name,
        user_id,
        quality,
        project_type,
        outputs,
        avg_photo_mb,
        avg_width,
        avg_height,
        avg_megapixels,
        total_megapixels,
        processing_load_score,
        estimated_processing_seconds,
        payment_status,
        payment_currency
      )
      values (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb,
        $12, $13, $14, $15, $16, $17, $18, $19, $20
      )
      returning *`;

    const insertParams = [
      initialPrice <= 0 ? "created" : "pending_payment",
      0,
      initialPrice,
      exifSummary,
      clientEmail,
      projectName,
      clientName,
      userId,
      qualityMode,
      projectType,
      JSON.stringify(normalizeOutputList(outputsRequested)),
      declaredStats.avgPhotoMb,
      declaredStats.avgWidth,
      declaredStats.avgHeight,
      declaredStats.avgMegapixels,
      declaredStats.totalMegapixels,
      initialLoadScore,
      estimatedSecondsForPayment,
      initialPaymentStatus,
      PAYPAL_CURRENCY
    ];

    const { rows } = await pool.query(insertSql, insertParams);

    const jobRow = rows[0];
    ensureJobDirs(jobRow.id);

    res.json(jobRow);
  } catch (e) {
    console.error("create job error", e);
    res.status(500).json({ error: "db error" });
  }
});

// ✅ OPCION A: submit (encolar cuando ya hay fotos)
// 🔥 v13: calcula inputs reales, calcula precio, y actualiza DB antes de encolar
app.post("/jobs/:id/submit", async (req, res) => {
  try {
    const { id } = req.params;
    await ensureTimingAnalyticsSchema();

    const { rows } = await pool.query(
      `select id, status, photos_count, quality, exif_summary,
              price, estimated_processing_seconds,
              created_at, upload_started_at, upload_completed_at,
              payment_status, payment_amount, payment_currency
         from jobs
        where id = $1`,
      [id]
    );
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const job = rows[0];
    const requestUserId = String(req.headers["x-user-id"] || "").trim();
    const ownerResult = await pool.query("select user_id from jobs where id=$1", [id]);
    if (ownerResult.rows?.[0]?.user_id && String(ownerResult.rows[0].user_id) !== requestUserId) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }
    if (!canUsePaidJob(job)) {
      return res.status(402).json({
        ok: false,
        error: "payment_required",
        message: "El pago debe estar confirmado antes de iniciar el procesado."
      });
    }
    const qualityMode = normalizeQualityMode(
      req.body?.quality_mode ||
      job.quality ||
      job?.exif_summary?._xproces?.quality_mode ||
      "normal"
    );

    let updatedExifSummary = stripNullCharsDeep({
      ...(job.exif_summary || {}),
      _xproces: {
        ...((job.exif_summary && job.exif_summary._xproces) || {}),
        quality_mode: qualityMode
      }
    });

    if (isLockedStatus(job.status)) {
      return res.status(409).json({
        ok: false,
        error: "job_locked",
        message: `Job ya está en estado ${job.status}`,
      });
    }

    ensureJobDirs(id);

    const inputs = listInputImages(id);

    if (inputs.length === 0) {
      await pool.query(
        `update jobs
           set status='failed',
               progress=0,
               message=$2,
               error=$3,
               updated_at=now(),
               finished_at=now()
         where id=$1`,
        [id, "No hay fotos en input", "no_input_files"]
      );
      return res.status(400).json({
        ok: false,
        error: "no_input_files",
        inputs: 0,
      });
    }

    const inputStats = collectInputImageStats(id, updatedExifSummary);
    const photosCount = inputStats.photosCount;
    const inputTotalBytes = inputStats.inputTotalBytes;
    const expectedPhotos = Number(job.exif_summary?._xproces?.totalPhotos || 0) || 0;

    if (expectedPhotos > 0 && photosCount < expectedPhotos) {
      const message = `Subida incompleta: esperadas ${expectedPhotos} fotos, recibidas ${photosCount}. El procesado no se iniciará.`;
      await pool.query(
        `update jobs
           set status='failed',
               stage='failed',
               progress=100,
               photos_count=$2,
               input_total_bytes=$3,
               message=$4,
               error=$5,
               updated_at=now(),
               finished_at=now()
         where id=$1`,
        [id, photosCount, inputTotalBytes, message, 'upload_incomplete']
      );
      return res.status(409).json({
        ok: false,
        error: 'upload_incomplete',
        message,
        realCount: photosCount,
        expectedPhotos
      });
    }

    const projectType = String(job.exif_summary?._xproces?.project_type || "fotogrametria").toLowerCase();
    const outputsRequested = normalizeOutputList(job.exif_summary?._xproces?.outputs_requested || []);
    const tamsExport = !!job.exif_summary?._xproces?.tams_export || projectType === "tams";

    const processingLoadScore = calculateProcessingLoadScore({
      photosCount,
      totalBytes: inputTotalBytes,
      avgMegapixels: inputStats.avgMegapixels,
      totalMegapixels: inputStats.totalMegapixels,
      qualityMode,
      outputsRequested,
      projectType
    });

    const actualUploadStart = job.upload_started_at || job.created_at;
    const actualUploadSeconds = actualUploadStart && job.upload_completed_at
      ? Math.max(0, Math.round((new Date(job.upload_completed_at).getTime() - new Date(actualUploadStart).getTime()) / 1000))
      : null;

    // El precio y el tiempo quedaron fijados cuando la web terminó de analizar
    // todas las fotografías. Al finalizar la subida solo guardamos las métricas
    // reales de entrada para enriquecer el histórico; no se recalcula la cotización.
    const storedEstimatedSeconds = Number(job.estimated_processing_seconds);
    const exifEstimatedSeconds = Number(job?.exif_summary?._xproces?.quoted_processing_seconds);
    const estimatedSeconds = Math.max(
      0,
      Number.isFinite(storedEstimatedSeconds) && storedEstimatedSeconds > 0
        ? storedEstimatedSeconds
        : (Number.isFinite(exifEstimatedSeconds) ? exifEstimatedSeconds : 0)
    );

    const storedPrice = Number(job.price);
    const exifQuotedPrice = Number(job?.exif_summary?._xproces?.quoted_price);
    const paidAmountForRecovery = Number(job.payment_amount);

    let price =
      Number.isFinite(storedPrice) && storedPrice > 0
        ? storedPrice
        : (Number.isFinite(exifQuotedPrice) && exifQuotedPrice > 0
            ? exifQuotedPrice
            : 0);

    // Si el pago ya fue capturado y la cotización quedó a cero por una versión
    // anterior del servidor, el importe confirmado por PayPal es la referencia
    // autoritativa. Se repara el precio del job antes de validar el submit.
    if (
      String(job.payment_status || "").toLowerCase() === "paid" &&
      price <= 0 &&
      Number.isFinite(paidAmountForRecovery) &&
      paidAmountForRecovery > 0
    ) {
      price = paidAmountForRecovery;
      await pool.query(
        `update jobs
            set price=$2,
                estimated_processing_seconds=case
                  when coalesce(estimated_processing_seconds, 0) > 0 then estimated_processing_seconds
                  else $3
                end,
                updated_at=now()
          where id=$1`,
        [id, price, estimatedSeconds]
      );
    }

    updatedExifSummary = stripNullCharsDeep({
      ...(updatedExifSummary || {}),
      _xproces: {
        ...((updatedExifSummary && updatedExifSummary._xproces) || {}),
        quality_mode: qualityMode,
        project_type: projectType,
        outputs_requested: outputsRequested,
        totalPhotos: photosCount,
        totalBytes: inputTotalBytes,
        image_stats: {
          avg_photo_mb: inputStats.avgPhotoMb,
          avg_width: inputStats.avgWidth,
          avg_height: inputStats.avgHeight,
          avg_megapixels: inputStats.avgMegapixels,
          total_megapixels: inputStats.totalMegapixels,
          sampled_dimensions: inputStats.sampledDimensions
        },
        processing_load_score: processingLoadScore,
        pricing_quote_locked: true,
        prediction_method: "locked_web_quote",
        predictor_version: TIMING_PREDICTOR_VERSION
      }
    });

    if (String(job.payment_status || "").toLowerCase() === "paid") {
      const paidAmount = Number(job.payment_amount || 0);
      if (Math.abs(paidAmount - price) > 0.001) {
        return res.status(409).json({
          ok: false,
          error: "paid_amount_mismatch",
          message: `El pedido pagado es de ${paidAmount.toFixed(2)} EUR y la cotización guardada es de ${price.toFixed(2)} EUR.`
        });
      }
    }

    // TAMS 2026-06-12: ahora también entra en cola para procesado local del worker.
    // Se conserva el bloque antiguo desactivado por seguridad.
    if (false && projectType === "tams") {
      const tamsUpdateSql = jobsHasQualityMode
        ? `update jobs
             set status='tams_pending_download',
                 stage='download_pending',
                 photos_count=$2,
                 price=0,
                 input_total_bytes=$3,
                 quality_mode=$4,
                 exif_summary=$5,
                 project_type=$6,
                 outputs=$7::jsonb,
                 estimated_processing_seconds=0,
                 progress=100,
                 message='TAMS pendiente de descarga al PC',
                 error=null,
                 updated_at=now()
           where id=$1`
        : `update jobs
             set status='tams_pending_download',
                 stage='download_pending',
                 photos_count=$2,
                 price=0,
                 input_total_bytes=$3,
                 exif_summary=$4,
                 project_type=$5,
                 outputs=$6::jsonb,
                 estimated_processing_seconds=0,
                 progress=100,
                 message='TAMS pendiente de descarga al PC',
                 error=null,
                 updated_at=now()
           where id=$1`;

      const tamsUpdateParams = jobsHasQualityMode
        ? [id, photosCount, inputTotalBytes, qualityMode, updatedExifSummary, projectType, JSON.stringify(outputsRequested)]
        : [id, photosCount, inputTotalBytes, updatedExifSummary, projectType, JSON.stringify(outputsRequested)];

      await pool.query(tamsUpdateSql, tamsUpdateParams);

      return res.json({
        ok: true,
        enqueued: false,
        download_pending: true,
        jobId: id,
        inputs: photosCount,
        input_total_bytes: inputTotalBytes,
        project_type: projectType,
        tams_export: true,
        price: 0,
        estimated_seconds: 0,
        estimated_human: "Pendiente de descarga"
      });
    }

    if (!processQueue || !redisReady) {
      return res
        .status(503)
        .json({ error: "queue unavailable (redis not ready)" });
    }

    // Fotogrametría / 3D: guardar conteo real + precio real y poner en cola.
    // Fotogrametría / 3D: guarda las características reales que alimentan
    // el histórico y la estimación de los siguientes trabajos.
    const submitUpdateSql = `update jobs
         set status='queued',
             stage='queued',
             photos_count=$2,
             price=$3,
             input_total_bytes=$4,
             quality=$5,
             exif_summary=$6,
             project_type=$7,
             outputs=$8::jsonb,
             estimated_processing_seconds=$9,
             avg_photo_mb=$10,
             avg_width=$11,
             avg_height=$12,
             avg_megapixels=$13,
             total_megapixels=$14,
             processing_load_score=$15,
             progress=$16,
             message='En cola',
             error=null,
             updated_at=now(),
             started_at = case when started_at is null then now() else started_at end
       where id=$1`;

    const submitUpdateParams = [
      id,
      photosCount,
      price,
      inputTotalBytes,
      qualityMode,
      updatedExifSummary,
      projectType,
      JSON.stringify(outputsRequested),
      estimatedSeconds,
      inputStats.avgPhotoMb,
      inputStats.avgWidth,
      inputStats.avgHeight,
      inputStats.avgMegapixels,
      inputStats.totalMegapixels,
      processingLoadScore,
      0
    ];

    await pool.query(submitUpdateSql, submitUpdateParams);

    await processQueue.add(
      "process_job",
      { jobId: id, qualityMode },
      { attempts: 3, backoff: { type: "exponential", delay: 5000 } }
    );

    res.json({
      ok: true,
      enqueued: true,
      jobId: id,
      inputs: photosCount,
      input_total_bytes: inputTotalBytes,
      quality_mode: qualityMode,
      quality_mode_label: getQualityModeLabel(qualityMode),
      tams_export: tamsExport,
      price,
      estimated_seconds: estimatedSeconds,
      estimated_human: formatEtaSeconds(estimatedSeconds),
      estimated_total_seconds: estimatedSeconds,
      estimated_total_human: formatEtaSeconds(estimatedSeconds),
      prediction_method: "locked_web_quote",
      prediction_neighbors: null,
      confidence: job?.exif_summary?._xproces?.timing_prediction?.confidence || null,
      estimated_low_seconds: estimatedSeconds,
      estimated_high_seconds: estimatedSeconds,
      estimated_total_low_seconds: estimatedSeconds,
      estimated_total_high_seconds: estimatedSeconds,
      processing_load_score: processingLoadScore,
      image_stats: updatedExifSummary?._xproces?.image_stats || null,
      timing_prediction: null
    });
  } catch (e) {
    console.error("submit error", e);
    res.status(500).json({ error: "submit error" });
  }
});

// =====================
// INPUTS (photos)
// =====================

// ✅ acepta "photos" y "photos[]"
app.post("/jobs/:id/upload", requirePaidJobBeforeUpload, uploadInput.any(), async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id, status, created_at, exif_summary from jobs where id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const job = rows[0];

    if (isLockedStatus(job.status)) {
      return res.status(409).json({
        ok: false,
        error: "job_locked",
        message: `No se pueden subir más fotos: estado ${job.status}`,
      });
    }

    ensureJobDirs(id);

    const files = (req.files || [])
      .filter((f) => f.fieldname === "photos" || f.fieldname === "photos[]")
      .map((f) => ({
        field: f.fieldname,
        filename: f.filename,
        size: f.size,
      }));

    if (!files.length) {
      return res.status(400).json({
        ok: false,
        error: "no files uploaded (use photos or photos[])",
      });
    }

    const totalPhotos = listInputImages(id).length;
    const totalBytes = getInputTotalBytes(id);
    const expectedPhotos = Number(job.exif_summary?._xproces?.totalPhotos || 0) || null;
    const persistedTotalBytes = totalBytes;

    let nextStatus = String(job.status || "").toLowerCase();
    let nextMessage = "Recibiendo fotos";

    const uploadWasAlreadyActive = String(job.status || "").toLowerCase() === "receiving";

    if (nextStatus === "created") {
      nextStatus = "receiving";
    } else if (!nextStatus) {
      nextStatus = "receiving";
    }

    if (!uploadWasAlreadyActive && nextStatus === "receiving") {
      await recordJobStageEvent(pool, id, "uploading", "start");
    }

    if (expectedPhotos && totalPhotos >= expectedPhotos) {
      console.log("📥 Upload completo detectado en servidor:", id, `${totalPhotos}/${expectedPhotos}`);
    }

    await pool.query(
      `update jobs
         set photos_count = $1,
             input_total_bytes = $2,
             upload_started_at = coalesce(upload_started_at, now()),
             status = $3,
             message = $4,
             updated_at = now()
       where id = $5`,
      [totalPhotos, persistedTotalBytes, nextStatus, nextMessage, id]
    );

    res.json({
      ok: true,
      uploaded: files.length,
      totalPhotos,
      expectedPhotos,
      status: nextStatus,
      files,
    });
  } catch (e) {
    console.error("upload error", e);
    res.status(500).json({ ok: false, error: "upload error" });
  }
});


function validateUploadedImageFile(filePath) {
  const filename = path.basename(filePath);
  const ext = path.extname(filename).toLowerCase();

  let stat;
  try {
    stat = fs.statSync(filePath);
  } catch (e) {
    return {
      ok: false,
      filename,
      reason: "file_unreadable",
      message: "No se puede leer el archivo en el servidor."
    };
  }

  if (!stat.isFile()) {
    return {
      ok: false,
      filename,
      reason: "not_a_file",
      message: "La entrada recibida no es un archivo válido."
    };
  }

  if (stat.size <= 0) {
    return {
      ok: false,
      filename,
      reason: "empty_file",
      message: "El archivo está vacío."
    };
  }

  // Para JPEG exigimos cabecera SOI y marcador EOI.
  // El EOI se busca en los últimos 64 KiB para tolerar metadatos residuales.
  if (ext === ".jpg" || ext === ".jpeg") {
    if (stat.size < 1024) {
      return {
        ok: false,
        filename,
        reason: "jpeg_too_small",
        message: "El JPG es demasiado pequeño para ser una fotografía válida."
      };
    }

    const fd = fs.openSync(filePath, "r");
    try {
      const head = Buffer.alloc(2);
      const headRead = fs.readSync(fd, head, 0, 2, 0);

      if (headRead !== 2 || head[0] !== 0xff || head[1] !== 0xd8) {
        return {
          ok: false,
          filename,
          reason: "jpeg_missing_soi",
          message: "El JPG no contiene una cabecera JPEG válida."
        };
      }

      const tailLength = Math.min(stat.size, 64 * 1024);
      const tail = Buffer.alloc(tailLength);
      const tailRead = fs.readSync(fd, tail, 0, tailLength, stat.size - tailLength);

      let hasEoi = false;
      for (let i = tailRead - 2; i >= 0; i -= 1) {
        if (tail[i] === 0xff && tail[i + 1] === 0xd9) {
          hasEoi = true;
          break;
        }
      }

      if (!hasEoi) {
        return {
          ok: false,
          filename,
          reason: "jpeg_missing_eoi",
          message: "El JPG está incompleto o truncado: falta el final JPEG."
        };
      }
    } finally {
      fs.closeSync(fd);
    }
  }

  return {
    ok: true,
    filename,
    size: stat.size
  };
}

function validateUploadedJobImages(jobId) {
  const dir = inputDir(jobId);
  const files = listInputImages(jobId);
  const validFiles = [];
  const invalidFiles = [];

  for (const filename of files) {
    const filePath = path.join(dir, filename);
    const result = validateUploadedImageFile(filePath);

    if (result.ok) {
      validFiles.push(result);
      continue;
    }

    invalidFiles.push(result);

    // No conservar en el servidor archivos detectados como corruptos.
    try {
      fs.unlinkSync(filePath);
    } catch (e) {
      console.error("No se pudo eliminar archivo inválido", filePath, e);
    }
  }

  return {
    validFiles,
    invalidFiles,
    validCount: validFiles.length,
    validBytes: validFiles.reduce((sum, item) => sum + Number(item.size || 0), 0)
  };
}

app.post("/jobs/:id/complete-upload", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id, status, photos_count, input_total_bytes, exif_summary from jobs where id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ ok: false, error: "job not found" });

    const job = rows[0];
    const validation = validateUploadedJobImages(id);
    const realCount = validation.validCount;
    const realBytes = validation.validBytes;
    const expectedPhotos = Number(req.body?.expected_photos_count || job.exif_summary?._xproces?.totalPhotos || 0) || Number(job.photos_count || 0) || 0;
    const expectedBytes = Number(req.body?.expected_total_bytes || job.exif_summary?._xproces?.totalBytes || 0) || 0;
    const projectType = String(job.exif_summary?._xproces?.project_type || "fotogrametria").toLowerCase();
    const isTams = projectType === "tams" || !!job.exif_summary?._xproces?.tams_export;

    if (validation.invalidFiles.length > 0) {
      const invalidNames = validation.invalidFiles.map((item) => item.filename);
      const message = `Subida inválida: ${invalidNames.length} archivo(s) corrupto(s) o incompleto(s): ${invalidNames.join(", ")}. Se han eliminado del servidor y deben subirse de nuevo.`;

      await pool.query(
        `update jobs
           set status='receiving',
               stage='upload_invalid',
               photos_count=$2,
               input_total_bytes=$3,
               message=$4,
               error=$5,
               updated_at=now()
         where id=$1`,
        [id, realCount, realBytes, message, JSON.stringify(validation.invalidFiles)]
      );

      return res.status(422).json({
        ok: false,
        ready: false,
        error: "invalid_uploaded_files",
        message,
        invalidFiles: validation.invalidFiles,
        realCount,
        expectedPhotos,
        realBytes,
        expectedBytes
      });
    }

    if (expectedPhotos > 0 && realCount < expectedPhotos) {
      const message = `Subida incompleta: esperadas ${expectedPhotos} fotos, recibidas ${realCount}. Esperando a que terminen de llegar todos los archivos.`;
      await pool.query(
        `update jobs
           set status='receiving',
               stage='upload_waiting',
               photos_count=$2,
               input_total_bytes=$3,
               message=$4,
               error=null,
               updated_at=now()
         where id=$1`,
        [id, realCount, realBytes, message]
      );
      return res.status(202).json({
        ok: true,
        ready: false,
        error: "upload_waiting",
        message,
        realCount,
        expectedPhotos,
        realBytes,
        expectedBytes
      });
    }

    if (expectedBytes > 0 && realBytes !== expectedBytes) {
      const message = `Subida incompleta o alterada: esperados ${expectedBytes} bytes y recibidos ${realBytes}. El trabajo no se enviará a procesado.`;

      await pool.query(
        `update jobs
           set status='receiving',
               stage='upload_bytes_mismatch',
               photos_count=$2,
               input_total_bytes=$3,
               message=$4,
               error=$5,
               updated_at=now()
         where id=$1`,
        [id, realCount, realBytes, message, "upload_bytes_mismatch"]
      );

      return res.status(409).json({
        ok: false,
        ready: false,
        error: "upload_bytes_mismatch",
        message,
        realCount,
        expectedPhotos,
        realBytes,
        expectedBytes
      });
    }

    const nextStatus = isLockedStatus(job.status)
      ? job.status
      : "receiving";

    const nextMessage = nextStatus === "receiving" ? "Subida verificada" : job.status;

    if (nextStatus === "receiving") {
      await pool.query(
        `update jobs
           set status = 'receiving',
               stage = 'upload_complete',
               photos_count = $1,
               input_total_bytes = $2,
               message = $3,
               error = null,
               upload_completed_at = coalesce(upload_completed_at, now()),
               updated_at = now()
         where id = $4`,
        [realCount, realBytes, nextMessage, id]
      );
    } else if (nextStatus === "tams_pending_download") {
      await pool.query(
        `update jobs
           set status = 'tams_pending_download',
               stage = 'download_pending',
               photos_count = $1,
               input_total_bytes = $2,
               price = 0,
               estimated_processing_seconds = 0,
               progress = 100,
               message = $3,
               upload_completed_at = coalesce(upload_completed_at, now()),
               updated_at = now()
         where id = $4`,
        [realCount, realBytes, nextMessage, id]
      );
    }

    await recordJobStageEvent(pool, id, "uploading", "end");
    await upsertJobStageMetric(pool, id, "uploading", {
      inputBytes: realBytes,
      outputBytes: realBytes,
      itemCount: realCount,
      metrics: {
        expected_photos: expectedPhotos,
        expected_bytes: expectedBytes,
        valid_files: realCount
      }
    });

    return res.json({
      ok: true,
      ready: true,
      status: nextStatus,
      realCount,
      expectedPhotos,
      realBytes,
      expectedBytes,
      project_type: projectType,
      download_pending: false
    });
  } catch (e) {
    console.error("complete-upload error", e);
    res.status(500).json({ ok: false, error: "complete-upload error" });
  }
});

app.get("/jobs/:id/files", async (req, res) => {
  try {
    const { id } = req.params;
    const dir = inputDir(id);

    if (!fs.existsSync(dir)) return res.json({ ok: true, files: [] });

    const files = fs.readdirSync(dir).map((name) => {
      const stat = fs.statSync(path.join(dir, name));
      return { filename: name, size: stat.size };
    });

    res.json({ ok: true, files });
  } catch (e) {
    console.error("files error", e);
    res.status(500).json({ ok: false, error: "files error" });
  }
});

app.get("/jobs/:id/input.zip", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query("select id from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const dir = inputDir(id);
    if (!fs.existsSync(dir))
      return res.status(404).json({ error: "no input folder yet" });

    const files = fs.readdirSync(dir);
    if (!files.length) return res.status(404).json({ error: "no inputs yet" });

    res.setHeader("Content-Type", "application/zip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="mapxion-${id}-input.zip"`
    );

    const archive = archiver("zip", { zlib: { level: 9 } });

    archive.on("error", (err) => {
      console.error("input.zip error", err);
      try {
        res.status(500).end();
      } catch (_) {}
    });

    req.on("close", () => {
      if (!res.writableEnded) archive.abort();
    });

    archive.pipe(res);
    archive.directory(dir, false);
    await archive.finalize();
  } catch (e) {
    console.error("input.zip error", e);
    res.status(500).json({ error: "input.zip error" });
  }
});


// =====================
// XPROCES LIVE VIEWPORT
// =====================
// Worker Windows: sube una captura del visor de Metashape.
// IMPORTANTE: se sobrescribe siempre latest.jpg, así no se llena el servidor.
app.post("/worker/jobs/:id/live", requireWorkerAuth, uploadLive.single("image"), async (req, res) => {
  try {
    const { id } = req.params;

    if (!isValidUuid(id)) {
      return res.status(400).json({ ok: false, error: "invalid_job_id" });
    }

    if (!req.file || !req.file.buffer || !req.file.buffer.length) {
      return res.status(400).json({ ok: false, error: "missing_image_field" });
    }

    const { rows } = await pool.query("select id from jobs where id = $1", [id]);
    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job_not_found" });
    }

    ensureLiveDir(id);

    const mime = String(req.file.mimetype || "").toLowerCase();
    if (mime && !["image/jpeg", "image/jpg", "image/png"].includes(mime)) {
      return res.status(415).json({ ok: false, error: "unsupported_image_type" });
    }

    const finalPath = liveImagePath(id);
    const tmpPath = path.join(liveDir(id), `latest.${Date.now()}.tmp`);

    fs.writeFileSync(tmpPath, req.file.buffer);
    fs.renameSync(tmpPath, finalPath);

    const now = new Date().toISOString();
    const meta = {
      ok: true,
      jobId: id,
      filename: "latest.jpg",
      size: req.file.size,
      mimetype: req.file.mimetype || "image/jpeg",
      updatedAt: now,
    };

    fs.writeFileSync(liveMetaPath(id), JSON.stringify(meta, null, 2), "utf-8");

    return res.json(meta);
  } catch (e) {
    console.error("live viewport upload error", e);
    return res.status(500).json({ ok: false, error: "live_viewport_upload_error" });
  }
});

// Web: consulta si existe captura live y cuándo se actualizó.
app.get("/jobs/:id/live", async (req, res) => {
  try {
    const { id } = req.params;

    if (!isValidUuid(id)) {
      return res.status(400).json({ ok: false, error: "invalid_job_id" });
    }

    const file = liveImagePath(id);
    if (!fs.existsSync(file)) {
      return res.json({ ok: true, exists: false });
    }

    let meta = null;
    const metaFile = liveMetaPath(id);
    if (fs.existsSync(metaFile)) {
      try {
        meta = JSON.parse(fs.readFileSync(metaFile, "utf-8"));
      } catch (_) {
        meta = null;
      }
    }

    const stat = fs.statSync(file);
    return res.json({
      ok: true,
      exists: true,
      url: `${getPublicApiBase(req)}/jobs/${id}/live.jpg`,
      relativeUrl: `/jobs/${id}/live.jpg`,
      updatedAt: meta?.updatedAt || stat.mtime.toISOString(),
      size: stat.size,
    });
  } catch (e) {
    console.error("live viewport status error", e);
    return res.status(500).json({ ok: false, error: "live_viewport_status_error" });
  }
});

// Web: imagen actual del proceso. Usar ?t=Date.now() para evitar caché del navegador.
app.get("/jobs/:id/live.jpg", async (req, res) => {
  try {
    const { id } = req.params;

    if (!isValidUuid(id)) {
      return res.status(400).send("invalid job id");
    }

    const file = liveImagePath(id);
    if (!fs.existsSync(file)) {
      return res.status(404).send("live image not available");
    }

    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    res.type("jpg");
    return res.sendFile(file);
  } catch (e) {
    console.error("live viewport image error", e);
    return res.status(500).send("live image error");
  }
});

// =====================
// OUTPUTS
// =====================
app.post("/jobs/:id/output", uploadOutput.single("file"), async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query("select id from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    ensureJobDirs(id);

    if (!req.file)
      return res.status(400).json({ error: "missing file field (file)" });

    // Multer solo entra aquí cuando el ZIP final ya se ha guardado completo en el VPS.
    await pool.query(
      `update jobs
          set results_ready_at = coalesce(results_ready_at, now()),
              updated_at = now()
        where id = $1`,
      [id]
    );
    await recordJobStageEvent(pool, id, "zip_upload", "end");
    await upsertJobStageMetric(pool, id, "zip_upload", {
      inputBytes: req.file.size,
      outputBytes: req.file.size,
      itemCount: 1,
      metrics: {
        filename: req.file.filename,
        received_bytes: req.file.size
      }
    });

    // Red de seguridad: si aún quedaba input en el VPS, lo purgamos al recibir el ZIP final.
    const inputCleanupOk = safeRemoveDir(inputDir(id));
    if (inputCleanupOk) {
      await pool.query(
        `update jobs
            set input_purged = true,
                input_purged_at = coalesce(input_purged_at, now()),
                updated_at = now()
          where id = $1`,
        [id]
      );
      console.log("Input purgado al recibir output:", id);
    } else {
      console.error("No se pudo purgar input al recibir output:", id);
    }

    res.json({
      ok: true,
      saved: { filename: req.file.filename, size: req.file.size },
      inputPurged: inputCleanupOk,
    });
  } catch (e) {
    console.error("upload output error", e);
    res.status(500).json({ ok: false, error: "upload output error" });
  }
});

app.get("/jobs/:id/outputs", async (req, res) => {
  try {
    const { id } = req.params;
    const dir = outputDir(id);

    if (!fs.existsSync(dir)) return res.json({ ok: true, files: [] });

    const files = fs.readdirSync(dir).map((name) => {
      const stat = fs.statSync(path.join(dir, name));
      return { filename: name, size: stat.size };
    });

    res.json({ ok: true, files });
  } catch (e) {
    console.error("outputs error", e);
    res.status(500).json({ ok: false, error: "outputs error" });
  }
});

app.get("/jobs/:id/log", async (req, res) => {
  try {
    const { id } = req.params;
    if (!isValidUuid(id)) {
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
      return res.status(400).send("XPROCES_PROGRESS|0|ID de job no válido\n");
    }

    await ensureDownloadCleanupColumns();

    const { rows } = await pool.query(
      "select id, status, stage, progress, message, error, archived_log from jobs where id = $1",
      [id]
    );

    if (!rows.length) {
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
      return res.status(404).send("XPROCES_PROGRESS|0|Job no encontrado\n");
    }

    const job = rows[0];
    const logPath = path.join(outputDir(id), "metashape-python-log.txt");

    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");

    if (fs.existsSync(logPath)) {
      return res.send(fs.readFileSync(logPath, "utf8"));
    }

    if (job.archived_log) {
      return res.send(String(job.archived_log || ""));
    }

    // Fallback: si todavía no existe el log físico, devolver el progreso guardado en DB.
    // Así la web no recibe 404 y puede pintar algo real.
    const progress = Number.isFinite(Number(job.progress)) ? Number(job.progress) : 0;
    const message = String(job.message || job.stage || job.status || "Procesando").replace(/\r?\n/g, " ");
    return res.send(`XPROCES_PROGRESS|${progress}|${message}\n`);
  } catch (e) {
    console.error("log error", e);
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    return res.status(500).send("XPROCES_PROGRESS|0|Error leyendo progreso\n");
  }
});

app.get("/jobs/:id/download", async (req, res) => {
  try {
    const { id } = req.params;

    await ensureDownloadCleanupColumns();

    const { rows } = await pool.query(
      `select id,
              status,
              coalesce(download_verified, false) as download_verified,
              coalesce(output_purged, false) as output_purged
         from jobs
        where id = $1`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ error: "job not found" });
    }

    const jobForDownload = rows[0];
    const zipPath = path.join(outputDir(id), "outputs.zip");

    if (String(jobForDownload.status || "").toLowerCase() !== "done") {
      return res.status(409).json({
        ok: false,
        error: "results_not_ready",
        message: "Los resultados todavía se están subiendo al servidor. Espere a que finalice la entrega."
      });
    }

    if (!fs.existsSync(zipPath)) {
      return res.status(410).json({
        ok: false,
        error: "outputs_not_available",
        message: "Este trabajo no esta disponible para descarga directa. Contacte con soporte para recuperarlo."
      });
    }

    const stat = fs.statSync(zipPath);

    await markDownloadStarted(id);

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Length", stat.size);
    res.setHeader("Cache-Control", "no-store");

    // res.download(callback) se ejecuta cuando Express termina el envio o si hay error.
    // Solo purgamos si no hay error y no se ha cortado la respuesta.
    return res.download(zipPath, `xproces-${id}.zip`, async (err) => {
      if (err) {
        console.error("download transfer error", id, err?.message || err);
        return;
      }

      try {
        // Guardamos resumen/log/listado de outputs ANTES de borrar los archivos del VPS.
        await saveArchivedJobSummary(id);

        const cleanup = cleanupJobStorage(id, { input: true, output: true });
        console.log("Cleanup tras descarga completada:", id, cleanup);
        await markDownloadCompletedAndPurged(id, cleanup);
      } catch (cleanupError) {
        console.error("cleanup tras descarga error", id, cleanupError?.message || cleanupError);
      }
    });

  } catch (e) {
    console.error("download error", e);
    res.status(500).json({ error: "download error" });
  }
});



async function ensureRecoveryRequestTable() {
  await pool.query(`
    create table if not exists job_recovery_requests (
      id uuid primary key default gen_random_uuid(),
      job_id uuid not null,
      user_name text,
      user_email text,
      message text,
      support_email text not null default 'soportetams@intelsi.es',
      status text not null default 'pending',
      created_at timestamptz not null default now()
    )
  `);
}

app.post("/jobs/:id/recovery-request", async (req, res) => {
  try {
    const { id } = req.params;
    const body = req.body || {};

    await ensureRecoveryRequestTable();

    const userName = String(body.user || body.user_name || "").trim();
    const userEmail = String(body.email || body.user_email || "").trim();
    const message = String(body.message || "").trim();

    await pool.query(
      `insert into job_recovery_requests
        (job_id, user_name, user_email, message, support_email, status)
       values ($1, $2, $3, $4, 'soportetams@intelsi.es', 'pending')`,
      [id, userName, userEmail, message]
    );

    return res.json({
      ok: true,
      message: "Petición enviada correctamente. En breve le enviaremos al correo el enlace de descarga."
    });
  } catch (e) {
    console.error("recovery request error", e);
    return res.status(500).json({
      ok: false,
      error: "recovery_request_error",
      message: "No se pudo enviar la solicitud de recuperación."
    });
  }
});

app.post("/jobs/:id/cancel", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `select id, status
       from jobs
       where id = $1`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const job = rows[0];
    const status = String(job.status || "").toLowerCase();

    if (status === "done") {
      return res.status(409).json({
        ok: false,
        error: "job_already_done",
        message: "No se puede cancelar un trabajo completado"
      });
    }

    if (status === "failed") {
      return res.status(409).json({
        ok: false,
        error: "job_already_failed",
        message: "El trabajo ya está en estado failed"
      });
    }

    if (status === "cancelled") {
      return res.json({
        ok: true,
        alreadyCancelled: true
      });
    }

    const updated = await pool.query(
      `update jobs
          set status = 'cancelled',
              stage = 'failed',
              message = 'Cancelado manualmente desde admin',
              error = 'cancelled_by_admin',
              updated_at = now(),
              finished_at = now()
        where id = $1
        returning *`,
      [id]
    );

    return res.json({
      ok: true,
      job: updated.rows[0]
    });
  } catch (e) {
    console.error("cancel job error", e);
    res.status(500).json({ ok: false, error: "cancel job error" });
  }
});

app.post("/jobs/:id/priority", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `update jobs
         set priority = coalesce(priority, 0) + 1,
             updated_at = now()
       where id = $1
       returning *`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    res.json({ ok: true, job: rows[0] });
  } catch (e) {
    console.error("priority error", e);
    res.status(500).json({ ok: false, error: "priority error" });
  }
});

// =====================
// PATCH TRACKING
// =====================
app.patch("/jobs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const {
      status,
      stage,
      progress,
      message,
      error,
      download_seconds,
      processing_seconds,
      total_seconds,
      estimated_processing_seconds,
      process_log,
      log_line,
      log
    } = req.body;

    const p = progress === undefined ? null : Number(progress);
    if (p !== null && (!Number.isFinite(p) || p < 0 || p > 100)) {
      return res.status(400).json({ error: "progress must be 0..100" });
    }

    const currentJobResult = await pool.query(
      `select id, status, stage, progress, started_at, finished_at, created_at,
              processing_started_at, processing_finished_at, exif_summary,
              user_id, project_name, project_type, message, error
         from jobs
        where id = $1`,
      [id]
    );

    if (!currentJobResult.rows.length) {
      return res.status(404).json({ error: "not found" });
    }

    const currentJob = currentJobResult.rows[0];
    const currentStatus = String(currentJob.status || "").toLowerCase();
    const currentStage = normalizeProcessingStage(currentJob.stage);
    const currentProgress = Number(currentJob.progress || 0);

    let nextStatusParam = status ?? null;
    let nextProgressParam = p;
    let nextMessageParam = message ?? null;
    let nextStageInput = stage;

    const rawLiveLog = process_log ?? log_line ?? log;
    const logSignal = extractProgressSignalFromLog(rawLiveLog);
    if (logSignal) {
      if (nextProgressParam === null && logSignal.percent !== null) nextProgressParam = logSignal.percent;
      if (!nextMessageParam && logSignal.message) nextMessageParam = logSignal.message;
      if (!nextStageInput && logSignal.stage) nextStageInput = logSignal.stage;
    }

    const zipPathForJob = path.join(outputDir(id), "outputs.zip");
    let zipReadyForClient = false;
    try {
      zipReadyForClient = fs.existsSync(zipPathForJob) && fs.statSync(zipPathForJob).size > 0;
    } catch (_) {
      zipReadyForClient = false;
    }

    let incomingStatus = nextStatusParam === null ? null : String(nextStatusParam || "").toLowerCase();
    const msgForGuard = String(nextMessageParam || "").toLowerCase();

    // El procesado local puede terminar antes de que el ZIP esté subido al servidor.
    // No se permite status done ni botón de descarga hasta que outputs.zip exista en el VPS.
    if (incomingStatus === "done" && !zipReadyForClient) {
      const zipUploadProgress = getPlannedProgressForStage(currentJob.exif_summary, "zip_upload");
      nextStatusParam = "running";
      nextStageInput = "zip_upload";
      nextProgressParam = Number.isFinite(zipUploadProgress)
        ? Math.max(currentProgress, Math.min(99, Math.round(zipUploadProgress)))
        : Math.max(currentProgress, 85);
      nextMessageParam = "Subiendo ZIP final al servidor";
      incomingStatus = "running";
    } else if (
      zipReadyForClient &&
      (incomingStatus === "done" || (Number(nextProgressParam) >= 99 && /subiendo zip|zip final|outputs\.zip|subiendo resultados/i.test(msgForGuard)))
    ) {
      nextStatusParam = "done";
      nextStageInput = "final";
      nextProgressParam = 100;
      nextMessageParam = "Trabajo procesado correctamente";
      incomingStatus = "done";
    }

    const nextStage = inferProcessingStage({
      status: incomingStatus || currentStatus,
      stage: nextStageInput,
      progress: nextProgressParam,
      message: nextMessageParam
    });

    // Para trabajos nuevos, el porcentaje deja de depender de los números fijos
    // del script. Se toma el inicio previsto de la fase calculado con fotos,
    // megapíxeles, calidad, salidas solicitadas e histórico real.
    if (nextStage && incomingStatus !== "done") {
      const plannedProgress = getPlannedProgressForStage(currentJob.exif_summary, nextStage);
      if (Number.isFinite(plannedProgress)) {
        nextProgressParam = Math.max(
          currentProgress,
          Math.min(99, Math.round(plannedProgress))
        );
      }
    }

    const terminalStatus = incomingStatus && ["done", "failed", "cancelled"].includes(incomingStatus);

    if (currentStatus === "cancelled") {
      return res.json({
        ok: true,
        ignored: true,
        message: "Job cancelado; actualización ignorada"
      });
    }

    if (process_log !== undefined || log_line !== undefined || log !== undefined) {
      try {
        ensureJobDirs(id);
        const liveText = String(rawLiveLog ?? "").replace(/\u0000/g, "").slice(0, 20000);
        if (liveText.trim()) {
          const liveLogPath = path.join(outputDir(id), "metashape-python-log.txt");
          fs.appendFileSync(liveLogPath, liveText.endsWith("\n") ? liveText : liveText + "\n", "utf8");
        }
      } catch (logError) {
        console.error("live log append error", logError?.message || logError);
      }
    }

    const stageChanged = !!(nextStage && nextStage !== currentStage);
    if (stageChanged) {
      if (currentStage) {
        await recordJobStageEvent(pool, id, currentStage, "end");
        await upsertJobStageMetric(pool, id, currentStage);
      }
      await recordJobStageEvent(pool, id, nextStage, "start");
    } else if (nextStage && nextProgressParam !== null && nextProgressParam !== currentProgress) {
      await recordJobStageEvent(pool, id, nextStage, "progress");
    }

    // Al terminar, cerrar una sola vez la fase terminal. El bloque anterior ya
    // abrió la fase nueva cuando hubo transición.
    if (terminalStatus) {
      if (nextStage) {
        await recordJobStageEvent(pool, id, nextStage, "end");
        await upsertJobStageMetric(pool, id, nextStage);
      } else if (currentStage) {
        await recordJobStageEvent(pool, id, currentStage, "end");
        await upsertJobStageMetric(pool, id, currentStage);
      }
    }

    const { rows } = await pool.query(
      `update jobs
         set status = coalesce($2, status),
             stage = coalesce($3, stage),
             progress = coalesce($4, progress),
             message = coalesce($5, message),
             error = coalesce($6, error),
             download_seconds = coalesce($7, download_seconds),
             processing_seconds = coalesce(
               $8,
               case
                 when $2 in ('done','failed','cancelled') then coalesce(
                   processing_seconds,
                   greatest(0, extract(epoch from (coalesce(processing_finished_at, now()) - coalesce(processing_started_at, started_at, created_at)))::int)
                 )
                 else processing_seconds
               end
             ),
             total_seconds = coalesce(
               $9,
               case
                 when $2 in ('done','failed','cancelled') then greatest(0, extract(epoch from (now() - created_at))::int)
                 else total_seconds
               end
             ),
             estimated_processing_seconds = coalesce($10, estimated_processing_seconds),
             updated_at = now(),
             started_at = case when $2 = 'running' and started_at is null then now() else started_at end,
             processing_started_at = case
               when processing_started_at is null
                and $3 in (
                  'preparing','importing','matching','aligning','cleaning','depth_maps',
                  'model','uv','texture','tiled_model','point_cloud','ground_classification',
                  'dem','export_dem','dtm','export_dtm','orthomosaic','colorize_model',
                  'report','export_tiled_model','export_model','export_point_cloud',
                  'export_orthomosaic','export_texture','export_reference','exporting',
                  'zip','closing_metashape','processing_complete'
                )
               then now()
               else processing_started_at
             end,
             processing_finished_at = case
               when $3 = 'zip_upload' then coalesce(processing_finished_at, now())
               when $2 in ('done','failed','cancelled') then coalesce(processing_finished_at, now())
               else processing_finished_at
             end,
             finished_at = case when $2 in ('done','failed','cancelled') then now() else finished_at end,
             results_ready_at = case when $2 = 'done' then coalesce(results_ready_at, now()) else results_ready_at end
       where id = $1
       returning *`,
      [
        id,
        nextStatusParam,
        nextStage ?? null,
        nextProgressParam,
        nextMessageParam,
        error ?? null,
        download_seconds ?? null,
        processing_seconds ?? null,
        total_seconds ?? null,
        estimated_processing_seconds ?? null
      ]
    );

    if (!rows.length) return res.status(404).json({ error: "not found" });

    try {
      const telegramTransition = await notifyUserJobTransition(currentJob, rows[0]);
      if (!telegramTransition.ok && !telegramTransition.skipped) {
        console.error("No se pudo enviar el cambio de estado por Telegram", telegramTransition.error);
      }
    } catch (telegramTransitionError) {
      console.error("Error enviando transición del trabajo por Telegram", telegramTransitionError?.message || telegramTransitionError);
    }

    if (terminalStatus) {
      await finalizeJobStageMetrics(pool, id);
    } else if (stageChanged) {
      try {
        await recalculateJobTimingPlan(pool, id);
      } catch (timingError) {
        console.error("timing replan error", id, timingError?.message || timingError);
      }
    }

    res.json(rows[0]);
  } catch (e) {
    console.error("patch job error", e);
    res.status(500).json({ error: "db error" });
  }
});

async function completeTimingAnalyticsBeforeClaim(job) {
  const currentExif = job?.exif_summary || {};
  const qualityMode = normalizeQualityMode(
    job?.quality || currentExif?._xproces?.quality_mode || "normal"
  );
  const projectType = String(
    job?.project_type || currentExif?._xproces?.project_type || "fotogrametria"
  ).toLowerCase();
  const outputsRequested = normalizeOutputList(
    job?.outputs || currentExif?._xproces?.outputs_requested || []
  );

  const inputStats = collectInputImageStats(job.id, currentExif);
  const processingLoadScore = calculateProcessingLoadScore({
    photosCount: inputStats.photosCount,
    totalBytes: inputStats.inputTotalBytes,
    avgMegapixels: inputStats.avgMegapixels,
    totalMegapixels: inputStats.totalMegapixels,
    qualityMode,
    outputsRequested,
    projectType
  });

  const targetFeatures = {
    photosCount: inputStats.photosCount,
    totalBytes: inputStats.inputTotalBytes,
    avgMegapixels: inputStats.avgMegapixels,
    totalMegapixels: inputStats.totalMegapixels,
    processingLoadScore,
    qualityMode,
    outputsRequested,
    projectType,
    tamsExport: projectType === "tams",
    actualUploadSeconds: job?.upload_completed_at && (job?.upload_started_at || job?.created_at)
      ? Math.max(0, (new Date(job.upload_completed_at).getTime() - new Date(job.upload_started_at || job.created_at).getTime()) / 1000)
      : null
  };

  const prediction = await estimateProcessingDetailsHistorical(pool, targetFeatures);
  const timingPlan = await estimateStageTimingPlan(pool, targetFeatures, prediction);

  const updatedExifSummary = stripNullCharsDeep({
    ...currentExif,
    _xproces: {
      ...((currentExif && currentExif._xproces) || {}),
      quality_mode: qualityMode,
      project_type: projectType,
      outputs_requested: outputsRequested,
      totalPhotos: inputStats.photosCount,
      totalBytes: inputStats.inputTotalBytes,
      image_stats: {
        avg_photo_mb: inputStats.avgPhotoMb,
        avg_width: inputStats.avgWidth,
        avg_height: inputStats.avgHeight,
        avg_megapixels: inputStats.avgMegapixels,
        total_megapixels: inputStats.totalMegapixels,
        sampled_dimensions: inputStats.sampledDimensions
      },
      processing_load_score: processingLoadScore,
      timing_prediction: timingPlan,
      prediction_method: timingPlan.method,
      prediction_neighbors: timingPlan.historical_samples,
      predictor_version: TIMING_PREDICTOR_VERSION
    }
  });

  const result = await pool.query(
    `update jobs
        set photos_count = $2,
            input_total_bytes = $3,
            quality = $4,
            project_type = $5,
            outputs = $6::jsonb,
            avg_photo_mb = $7,
            avg_width = $8,
            avg_height = $9,
            avg_megapixels = $10,
            total_megapixels = $11,
            processing_load_score = $12,
            estimated_processing_seconds = $13,
            exif_summary = $14,
            updated_at = now()
      where id = $1
      returning *`,
    [
      job.id,
      inputStats.photosCount,
      inputStats.inputTotalBytes,
      qualityMode,
      projectType,
      JSON.stringify(outputsRequested),
      inputStats.avgPhotoMb,
      inputStats.avgWidth,
      inputStats.avgHeight,
      inputStats.avgMegapixels,
      inputStats.totalMegapixels,
      processingLoadScore,
      timingPlan.estimated_processing_seconds,
      updatedExifSummary
    ]
  );

  return result.rows[0] || job;
}

// =====================
// WORKER (HTTP por internet)
// =====================
app.post("/worker/claim", requireWorkerAuth, async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `update jobs
          set status='running',
              stage='downloading',
              progress=coalesce(progress, 0),
              message='Worker claimed',
              updated_at=now(),
              started_at=case when started_at is null then now() else started_at end
        where id = (
          select id
          from jobs
          where status='queued'
          order by coalesce(priority, 0) desc, created_at asc
          limit 1
          for update skip locked
        )
        returning *`
    );

    if (!rows.length) {
      return res.json({ ok: true, job: null });
    }

    // Algunos trabajos llegan a queued por la vigilancia de subida y no por
    // /submit. Antes de entregarlos al PC completamos siempre calidad, EXIF,
    // carga y plan de tiempos usando los ficheros reales del input.
    const job = await completeTimingAnalyticsBeforeClaim(rows[0]);
    await recordJobStageEvent(pool, job.id, "downloading", "start");
    res.json({
      ok: true,
      job: {
        ...job,
        quality_mode: normalizeQualityMode(job.quality || job?.exif_summary?._xproces?.quality_mode || "normal")
      }
    });
  } catch (e) {
    console.error("worker claim error", e);
    res.status(500).json({ ok: false, error: "worker claim error" });
  }
});
app.get("/worker/receiving", requireWorkerAuth, async (_req, res) => {
  try {
    const MAX_IDLE_MINUTES = 15;
    const MAX_STUCK_RECEIVING_MINUTES = 5;

    const staleResult = await pool.query(
      `select id, photos_count, updated_at, exif_summary
       from jobs
       where status = 'receiving'`
    );

    for (const job of staleResult.rows) {
      const expectedPhotos = Number(job.exif_summary?._xproces?.totalPhotos || 0) || null;
      const totalPhotos = Number(job.photos_count || 0);
      const serverFilesCount = listInputImages(job.id).length;
      const serverBytes = getInputTotalBytes(job.id);
      const updatedAtMs = job.updated_at ? new Date(job.updated_at).getTime() : null;
      const idleMinutes = updatedAtMs ? (Date.now() - updatedAtMs) / 60000 : 0;
      const projectType = String(job.exif_summary?._xproces?.project_type || "fotogrametria").toLowerCase();
      const isTams = projectType === "tams" || !!job.exif_summary?._xproces?.tams_export;

      const readyByExpected = expectedPhotos && totalPhotos >= expectedPhotos && serverFilesCount >= expectedPhotos;
      const readyByIdle = !expectedPhotos && totalPhotos > 0 && serverFilesCount >= totalPhotos && idleMinutes > MAX_STUCK_RECEIVING_MINUTES;

      if (readyByExpected || readyByIdle) {
        if (isTams) {
          await pool.query(
            `update jobs
               set status = 'tams_pending_download',
                   stage = 'download_pending',
                   photos_count = $2,
                   input_total_bytes = $3,
                   price = 0,
                   estimated_processing_seconds = 0,
                   progress = 100,
                   message = 'TAMS pendiente de descarga al PC',
                   upload_completed_at = coalesce(upload_completed_at, now()),
                   updated_at = now()
             where id = $1 and status = 'receiving'`,
            [job.id, serverFilesCount, serverBytes]
          );
          console.log("📥 TAMS pendiente de descarga al PC:", job.id, `${serverFilesCount}/${expectedPhotos || totalPhotos}`);
        } else {
          await pool.query(
            `update jobs
               set status = 'queued',
                   stage = 'queued',
                   photos_count = $2,
                   input_total_bytes = $3,
                   message = 'En cola para procesado',
                   upload_completed_at = coalesce(upload_completed_at, now()),
                   updated_at = now()
             where id = $1 and status = 'receiving'`,
            [job.id, serverFilesCount, serverBytes]
          );
          console.log("🚀 Job promovido a queued:", job.id, `${serverFilesCount}/${expectedPhotos || totalPhotos}`);
        }

        // Esta ruta de vigilancia puede cerrar la subida sin pasar por
        // /complete-upload. Registrar igualmente la fase evita perder las
        // muestras de subida de los trabajos promovidos automáticamente.
        await recordJobStageEvent(pool, job.id, "uploading", "end");
        await upsertJobStageMetric(pool, job.id, "uploading", {
          inputBytes: serverBytes,
          outputBytes: serverBytes,
          itemCount: serverFilesCount,
          metrics: {
            completion_source: "receiving_watchdog",
            expected_photos: expectedPhotos,
            valid_files: serverFilesCount
          }
        });
        continue;
      }

      if (expectedPhotos && totalPhotos > 0 && totalPhotos < expectedPhotos && idleMinutes > MAX_IDLE_MINUTES) {
        await pool.query(
          `update jobs
             set status = 'failed',
                 message = 'Subida incompleta: faltan fotografías',
                 error = 'upload_incomplete',
                 finished_at = now(),
                 total_seconds = greatest(0, extract(epoch from (now() - created_at))::int),
                 updated_at = now()
           where id = $1 and status = 'receiving'`,
          [job.id]
        );
        console.log("❌ Job marcado como failed por subida incompleta:", job.id, `${totalPhotos}/${expectedPhotos}`);
      }
    }

    const { rows } = await pool.query(
      `select id, status, photos_count, price, created_at, updated_at, message
       ${jobsHasQualityMode ? ", quality_mode" : ""}
       from jobs
       where status in ('receiving', 'tams_pending_download')
       order by created_at asc
       limit 10`
    );

    res.json({
      ok: true,
      jobs: rows.map((job) => ({
        ...job,
        upload_progress: getJobUploadProgress(job)
      }))
    });
  } catch (e) {
    console.error("worker receiving error", e);
    res.status(500).json({ ok: false, error: "worker receiving error" });
  }
});

// Lista de proyectos TAMS pendientes para que el PC los descargue.
// El PC puede llamar a /worker/jobs/:id/input.zip para bajar cada ZIP,
// y después a /worker/jobs/:id/confirm-download para confirmar descarga.
app.get("/worker/tams/pending", requireWorkerAuth, async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `select id, status, photos_count, input_total_bytes, created_at, updated_at, message, client_email, project_name, client_name, exif_summary
       from jobs
       where status = 'tams_pending_download'
       order by created_at asc
       limit 20`
    );

    res.json({ ok: true, jobs: rows });
  } catch (e) {
    console.error("worker tams pending error", e);
    res.status(500).json({ ok: false, error: "worker tams pending error" });
  }
});

app.get("/worker/jobs/:id/input.zip", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query("select id from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const dir = inputDir(id);
    if (!fs.existsSync(dir)) {
      return res.status(404).json({ error: "no input folder yet" });
    }

    const files = fs.readdirSync(dir);
    if (!files.length) {
      return res.status(404).json({ error: "no inputs yet" });
    }

    res.setHeader("Content-Type", "application/zip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="mapxion-${id}-input.zip"`
    );

    const archive = archiver("zip", { zlib: { level: 9 } });

    archive.on("error", (err) => {
      console.error("worker input.zip error", err);
      try {
        res.status(500).end();
      } catch (_) {}
    });

    req.on("close", () => {
      if (!res.writableEnded) archive.abort();
    });

    archive.pipe(res);
    archive.directory(dir, false);
    await archive.finalize();
  } catch (e) {
    console.error("worker input.zip error", e);
    res.status(500).json({ error: "worker input.zip error" });
  }
});


app.post("/worker/jobs/:id/confirm-files", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const filenames = Array.isArray(req.body?.filenames)
      ? req.body.filenames
          .filter((name) => typeof name === "string")
          .map((name) => path.basename(name))
          .filter(Boolean)
      : [];

    if (!filenames.length) {
      return res.status(400).json({ ok: false, error: "filenames required" });
    }

    const { rows } = await pool.query(
      "select id, input_purged from jobs where id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ ok: false, error: "job not found" });

    const job = rows[0];
    if (job.input_purged) {
      return res.json({ ok: true, alreadyPurged: true, deletedCount: 0 });
    }

    const dir = inputDir(id);
    let deletedCount = 0;

    if (fs.existsSync(dir)) {
      for (const filename of filenames) {
        const filePath = path.join(dir, filename);
        if (!filePath.startsWith(dir + path.sep)) continue;
        if (fs.existsSync(filePath)) {
          fs.rmSync(filePath, { force: true });
          deletedCount += 1;
        }
      }
    }

    let purged = false;
    if (!fs.existsSync(dir)) {
      purged = true;
    } else {
      const remaining = fs.readdirSync(dir).length;
      if (remaining === 0) {
        fs.rmSync(dir, { recursive: true, force: true });
        purged = true;
      }
    }

    await pool.query(
      `update jobs
          set input_purged = $2,
              input_purged_at = case when $2 then now() else input_purged_at end,
              updated_at = now()
        where id = $1`,
      [id, purged]
    );

    res.json({ ok: true, deletedCount, purged });
  } catch (e) {
    console.error("confirm-files error", e);
    res.status(500).json({ ok: false, error: "confirm-files error" });
  }
});

app.post("/worker/jobs/:id/confirm-download", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id, input_purged, stage, progress, exif_summary, photos_count, input_total_bytes from jobs where id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ ok: false, error: "job not found" });

    const job = rows[0];

    const dir = inputDir(id);
    const purged = safeRemoveDir(dir);

    if (!purged) {
      console.error("ERROR: input no se borró completamente:", dir);
    } else {
      console.log("Input eliminado correctamente:", id);
    }

    const processingStartProgress = getPlannedProgressForStage(job.exif_summary, "preparing");
    const nextProgress = Number.isFinite(processingStartProgress)
      ? Math.max(Number(job.progress || 0), Math.round(processingStartProgress))
      : Number(job.progress || 0);

    await recordJobStageEvent(pool, id, "downloading", "end");
    await upsertJobStageMetric(pool, id, "downloading", {
      inputBytes: Number(job.input_total_bytes || 0),
      outputBytes: Number(job.input_total_bytes || 0),
      itemCount: Number(job.photos_count || 0),
      metrics: { input_purged_after_download: purged }
    });
    await recordJobStageEvent(pool, id, "preparing", "start");

    await pool.query(
     `update jobs
         set input_purged = $2,
             input_purged_at = case when $2 then now() else input_purged_at end,
             download_finished_at = now(),
             processing_started_at = now(),
             status = case when status = 'tams_pending_download' and $2 then 'tams_downloaded' else status end,
             stage = case when status = 'tams_pending_download' and $2 then 'downloaded_pc' else 'preparing' end,
             progress = case when status = 'tams_pending_download' and $2 then progress else greatest(coalesce(progress, 0), $3) end,
             message = case when status = 'tams_pending_download' and $2 then 'TAMS descargado en PC' else 'Descarga validada. Iniciando procesado' end,
             updated_at = now()
       where id = $1`,
     [id, purged, nextProgress]
   );

    res.json({ ok: true, purged, processingStarted: true });
  } catch (e) {
    console.error("confirm-download error", e);
    res.status(500).json({ ok: false, error: "confirm-download error" });
  }
});

app.post("/worker/jobs/:id/stage-event", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const stage = normalizeProcessingStage(req.body?.stage);
    const eventType = String(req.body?.event_type || "").trim().toLowerCase();
    const metrics = req.body?.metrics && typeof req.body.metrics === "object" ? req.body.metrics : {};

    if (!stage) return res.status(400).json({ ok: false, error: "invalid stage" });
    if (!["start", "progress", "end"].includes(eventType)) {
      return res.status(400).json({ ok: false, error: "invalid event_type" });
    }

    const { rows } = await pool.query("select id from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "job not found" });

    await recordJobStageEvent(pool, id, stage, eventType);
    let metric = null;
    if (eventType === "end") {
      metric = await upsertJobStageMetric(pool, id, stage, { metrics });
    }

    return res.json({ ok: true, stage, event_type: eventType, metric_recorded: !!metric });
  } catch (e) {
    console.error("worker stage event error", e);
    return res.status(500).json({ ok: false, error: "worker stage event error" });
  }
});

app.post("/worker/jobs/:id/timing-manifest", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { rows } = await pool.query("select id, status from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: "job not found" });

    const summary = await applyTimingManifest(pool, id, req.body || {});
    let timingPlan = null;
    if (String(rows[0].status || "").toLowerCase() === "done") {
      await finalizeJobStageMetrics(pool, id);
    } else {
      timingPlan = await recalculateJobTimingPlan(pool, id);
    }

    res.json({ ok: true, summary, timing_prediction: timingPlan, predictor_version: TIMING_PREDICTOR_VERSION });
  } catch (e) {
    console.error("timing manifest error", e);
    res.status(500).json({ ok: false, error: "timing manifest error" });
  }
});

app.get("/worker/jobs/:id/files", requireWorkerAuth, async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id from jobs where id = $1",
      [id]
    );
    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const dir = inputDir(id);
    if (!fs.existsSync(dir)) {
      return res.json({ ok: true, files: [] });
    }

    const files = fs.readdirSync(dir).map((name) => {
      const stat = fs.statSync(path.join(dir, name));
      return {
        filename: name,
        size: stat.size,
      };
    });

    res.json({ ok: true, files });
  } catch (e) {
    console.error("worker files error", e);
    res.status(500).json({ ok: false, error: "worker files error" });
  }
});
app.get("/worker/jobs/:id/file/:name", requireWorkerAuth, async (req, res) => {
  try {
    const { id, name } = req.params;

    const { rows } = await pool.query(
      "select id from jobs where id = $1",
      [id]
    );
    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const safeName = path.basename(name);
    const filePath = path.join(inputDir(id), safeName);

    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ ok: false, error: "file not found" });
    }

    return res.sendFile(filePath);
  } catch (e) {
    console.error("worker file download error", e);
    res.status(500).json({ ok: false, error: "worker file download error" });
  }
});


function getJobQualityMode(job) {
  return normalizeQualityMode(
    job?.quality ||
    job?.quality_mode ||
    job?.exif_summary?._xproces?.quality_mode ||
    "normal"
  );
}

function getJobOutputs(job) {
  return normalizeOutputList(
    job?.outputs ||
    job?.exif_summary?._xproces?.outputs_requested ||
    []
  );
}

function buildJobFeatures(job = {}) {
  const photosCount = Number(job.photos_count ?? job.photosCount ?? 0) || 0;
  const totalBytes = Number(job.input_total_bytes ?? job.totalBytes ?? 0) || 0;
  const declared = extractDeclaredImageStats(
    job.exif_summary || null,
    photosCount,
    totalBytes
  );
  const avgMegapixels = finiteNumber(job.avg_megapixels ?? job.avgMegapixels, declared.avgMegapixels);
  const totalMegapixels = finiteNumber(job.total_megapixels ?? job.totalMegapixels, declared.totalMegapixels);
  const qualityMode = normalizeQualityMode(job.qualityMode || getJobQualityMode(job));
  const outputsRequested = normalizeOutputList(job.outputsRequested || getJobOutputs(job));
  const projectType = String(
    job.projectType ||
    job.project_type ||
    job?.exif_summary?._xproces?.project_type ||
    "fotogrametria"
  ).toLowerCase();
  const tamsExport = !!(
    job.tamsExport ??
    job?.exif_summary?._xproces?.tams_export ??
    (projectType === "tams")
  );

  const processingLoadScore = Number(
    job.processing_load_score ??
    job.processingLoadScore ??
    calculateProcessingLoadScore({
      photosCount,
      totalBytes,
      avgMegapixels,
      totalMegapixels,
      qualityMode,
      outputsRequested,
      projectType
    })
  ) || 0;

  return {
    id: job.id || null,
    photosCount,
    totalBytes,
    avgPhotoMb: finiteNumber(job.avg_photo_mb ?? job.avgPhotoMb, declared.avgPhotoMb),
    avgWidth: finiteNumber(job.avg_width ?? job.avgWidth, declared.avgWidth),
    avgHeight: finiteNumber(job.avg_height ?? job.avgHeight, declared.avgHeight),
    avgMegapixels,
    totalMegapixels,
    pointCount: finiteNumber(job.point_count ?? job.pointCount, null),
    processingLoadScore,
    qualityMode,
    outputsRequested,
    projectType,
    tamsExport,
    profileVersion: job.profileVersion || job.profile_version || job?.exif_summary?._xproces?.profile_version || null,
    workerVersion: job.workerVersion || job.worker_version || job?.exif_summary?._xproces?.worker_version || null
  };
}

function clampNumber(value, min, max) {
  return Math.max(min, Math.min(max, Number(value)));
}

function logRatioDistance(a, b) {
  const x = Math.max(0, Number(a || 0));
  const y = Math.max(0, Number(b || 0));
  if (x === 0 && y === 0) return 0;
  return Math.abs(Math.log((x + 1) / (y + 1)));
}

function outputSetDistance(a, b) {
  const setA = new Set(normalizeOutputList(a));
  const setB = new Set(normalizeOutputList(b));
  const union = new Set([...setA, ...setB]);
  if (!union.size) return 0;

  let intersection = 0;
  for (const item of setA) {
    if (setB.has(item)) intersection += 1;
  }
  return 1 - (intersection / union.size);
}

function outputSetsEqual(a, b) {
  const aa = normalizeOutputList(a).sort();
  const bb = normalizeOutputList(b).sort();
  return aa.length === bb.length && aa.every((value, index) => value === bb[index]);
}

function createPricingQuote(payload) {
  const body = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  const signature = createHmac("sha256", PRICING_QUOTE_SECRET).update(body).digest("base64url");
  return `${body}.${signature}`;
}

function verifyPricingQuote(token) {
  const raw = String(token || "").trim();
  const [body, signature, extra] = raw.split(".");
  if (!body || !signature || extra) return null;
  const expected = createHmac("sha256", PRICING_QUOTE_SECRET).update(body).digest("base64url");
  if (signature.length !== expected.length || signature !== expected) return null;
  try {
    const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
    if (!payload || Number(payload.expires_at || 0) < Math.floor(Date.now() / 1000)) return null;
    return payload;
  } catch (_) {
    return null;
  }
}

function processingFeatureDistance(target, candidate) {
  let distance = 0;
  distance += logRatioDistance(target.photosCount, candidate.photosCount) * 2.2;
  distance += logRatioDistance(target.totalBytes, candidate.totalBytes) * 1.4;
  distance += logRatioDistance(target.totalMegapixels, candidate.totalMegapixels) * 2.0;
  distance += logRatioDistance(target.avgMegapixels, candidate.avgMegapixels) * 0.7;
  distance += outputSetDistance(target.outputsRequested, candidate.outputsRequested) * 3.0;

  if (target.qualityMode !== candidate.qualityMode) distance += 2.5;
  if (target.projectType !== candidate.projectType) distance += 4.0;
  if (!!target.tamsExport !== !!candidate.tamsExport) distance += 4.0;

  return distance;
}

function estimateProcessingSecondsFromFallback(job) {
  const features = buildJobFeatures(job);
  const gb = features.totalBytes / (1024 * 1024 * 1024);
  const base = 120 + (features.photosCount * 18) + (gb * 420);
  const outputExtra = features.outputsRequested.reduce(
    (sum, output) => sum + (OUTPUT_LOAD_WEIGHTS[output] || 0.04),
    0
  );
  return Math.max(60, Math.round(
    base * getQualityModeTimeFactor(features.qualityMode) * (1 + outputExtra * 0.45)
  ));
}

async function estimateProcessingDetailsHistorical(pool, targetInput) {
  const target = buildJobFeatures(targetInput);
  const startDate = "2026-04-05";

  const { rows } = await pool.query(
    `select id, processing_seconds, total_seconds, photos_count, input_total_bytes,
            avg_photo_mb, avg_width, avg_height, avg_megapixels, total_megapixels,
            processing_load_score, project_type, quality, outputs, exif_summary,
            created_at
       from jobs
      where status = 'done'
        and processing_seconds is not null
        and processing_seconds > 0
        and photos_count > 0
        and created_at >= $1
        and ($2::uuid is null or id <> $2::uuid)
      order by created_at desc
      limit 500`,
    [startDate, target.id || null]
  );

  const candidates = rows
    .map((row) => {
      const features = buildJobFeatures(row);
      const seconds = Number(row.processing_seconds || 0);
      if (!Number.isFinite(seconds) || seconds <= 0) return null;
      if (features.qualityMode !== target.qualityMode) return null;
      // Respaldo global: permite trabajos con otros entregables. La similitud
      // del conjunto de salidas ya forma parte de processingFeatureDistance.
      if (features.projectType !== target.projectType) return null;
      if (!!features.tamsExport !== !!target.tamsExport) return null;

      const distance = processingFeatureDistance(target, features);
      const targetScore = Math.max(1, target.processingLoadScore);
      const candidateScore = Math.max(1, features.processingLoadScore);
      const scale = clampNumber(Math.pow(targetScore / candidateScore, 0.82), 0.25, 4);
      const predictedSeconds = seconds * scale;
      const weight = 1 / Math.pow(0.35 + distance, 2);

      return {
        id: row.id,
        seconds,
        predictedSeconds,
        distance,
        weight,
        features
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.distance - b.distance)
    .slice(0, 12);

  if (candidates.length >= 1) {
    const weightedSum = candidates.reduce((sum, item) => sum + (item.predictedSeconds * item.weight), 0);
    const weightSum = candidates.reduce((sum, item) => sum + item.weight, 0);
    const weightedAverage = weightSum > 0 ? weightedSum / weightSum : 0;

    const ordered = candidates.map((item) => item.predictedSeconds).sort((a, b) => a - b);
    const median = ordered[Math.floor(ordered.length / 2)];
    const blended = (weightedAverage * 0.75) + (median * 0.25);

    return {
      seconds: Math.max(60, Math.round(blended)),
      method: candidates.length === 1 ? "historical_exact_single_job" : "historical_exact_jobs",
      neighbors: candidates.length,
      neighborDetails: candidates
    };
  }

  return {
    seconds: estimateProcessingSecondsFromFallback(target),
    method: "formula_fallback",
    neighbors: candidates.length,
    neighborDetails: candidates
  };
}

async function estimateProcessingSecondsFromInputsHistorical(
  pool,
  photosCount,
  totalBytes,
  qualityMode = "normal",
  tamsExport = false,
  extra = {}
) {
  const details = await estimateProcessingDetailsHistorical(pool, {
    photosCount,
    totalBytes,
    qualityMode,
    tamsExport,
    ...extra
  });
  return details.seconds;
}

const TIMING_STAGE_ORDER = [
  "preparing",
  "importing",
  "matching",
  "aligning",
  "depth_maps",
  "model",
  "uv",
  "texture",
  "tiled_model",
  "point_cloud",
  "ground_classification",
  "dem",
  "export_dem",
  "dtm",
  "export_dtm",
  "orthomosaic",
  "colorize_model",
  "report",
  "export_tiled_model",
  "export_model",
  "export_point_cloud",
  "export_orthomosaic",
  "export_texture",
  "export_reference",
  "exporting",
  "zip",
  "closing_metashape"
];

const FALLBACK_STAGE_WEIGHTS = {
  preparing: 0.035,
  importing: 0.010,
  matching: 0.055,
  aligning: 0.025,
  depth_maps: 0.120,
  model: 0.160,
  uv: 0.080,
  texture: 0.150,
  tiled_model: 0.100,
  point_cloud: 0.110,
  ground_classification: 0.025,
  dem: 0.025,
  export_dem: 0.010,
  dtm: 0.025,
  export_dtm: 0.010,
  orthomosaic: 0.100,
  colorize_model: 0.015,
  report: 0.020,
  export_tiled_model: 0.015,
  export_model: 0.020,
  export_point_cloud: 0.020,
  export_orthomosaic: 0.025,
  export_texture: 0.010,
  export_reference: 0.005,
  exporting: 0.060,
  zip: 0.080,
  closing_metashape: 0.100
};

function requiredTimingStages(featuresInput) {
  const features = buildJobFeatures(featuresInput);
  const outputs = new Set(features.outputsRequested);
  const stages = new Set([
    "preparing",
    "importing",
    "matching",
    "aligning",
    "depth_maps",
    "model"
  ]);

  const needsPointCloud = outputs.has("las") || outputs.has("point_cloud_las") ||
    outputs.has("dem_tif") || outputs.has("dsm_tif") || outputs.has("dtm_tif") ||
    outputs.has("contours_dxf") || outputs.has("orthomosaic_tif") || outputs.has("ortho_tif");
  const requestedTexture = ["textures", "texture", "mesh_obj", "model_obj", "obj", "glb", "mesh_fbx", "fbx"].some((x) => outputs.has(x));
  const requestedTiled = ["tiled_model", "tiles_3tz", "3tz"].some((x) => outputs.has(x));
  // El flujo validado de fotogrametría construye malla -> textura -> teselas
  // antes de la nube. Aunque el cliente no exporte esos productos, su tiempo
  // sí forma parte del servicio cuando se necesita nube/raster.
  const needsTexture = requestedTexture || needsPointCloud;
  const needsTiled = requestedTiled || needsPointCloud;
  const needsDemOutput = outputs.has("dem_tif") || outputs.has("dsm_tif");
  const needsDtm = outputs.has("dtm_tif") || outputs.has("contours_dxf");
  const needsOrtho = outputs.has("orthomosaic_tif") || outputs.has("ortho_tif");
  const needsDemBuild = needsDemOutput || needsOrtho;
  const needsReport = outputs.has("pdf_report");

  if (needsTexture) {
    stages.add("uv");
    stages.add("texture");
  }
  if (needsTiled) stages.add("tiled_model");
  if (needsPointCloud) stages.add("point_cloud");
  if (needsDtm) stages.add("ground_classification");
  if (needsDemBuild) stages.add("dem");
  if (needsDemOutput) stages.add("export_dem");
  if (needsDtm) {
    stages.add("dtm");
    stages.add("export_dtm");
  }
  if (needsOrtho) {
    stages.add("orthomosaic");
    stages.add("colorize_model");
  }
  if (needsReport) stages.add("report");
  if (requestedTiled) stages.add("export_tiled_model");
  if (["mesh_obj", "model_obj", "obj", "glb", "mesh_fbx", "fbx"].some((x) => outputs.has(x))) {
    stages.add("export_model");
  }
  if (outputs.has("las") || outputs.has("point_cloud_las")) stages.add("export_point_cloud");
  if (needsOrtho) stages.add("export_orthomosaic");
  if (requestedTexture) stages.add("export_texture");

  stages.add("exporting");
  if (!features.tamsExport) stages.add("zip");
  stages.add("closing_metashape");

  return TIMING_STAGE_ORDER.filter((stage) => stages.has(stage));
}


function weightedTimingAverage(samples) {
  const valid = (samples || []).filter((item) =>
    Number.isFinite(Number(item?.seconds)) && Number(item.seconds) > 0 &&
    Number.isFinite(Number(item?.weight)) && Number(item.weight) > 0
  );
  if (!valid.length) return null;
  const weighted = valid.reduce((sum, item) => sum + Number(item.seconds) * Number(item.weight), 0);
  const weights = valid.reduce((sum, item) => sum + Number(item.weight), 0);
  return weights > 0 ? weighted / weights : null;
}


function metricRowFeatures(row) {
  return buildJobFeatures({
    id: row.job_id,
    photos_count: row.photos_count,
    input_total_bytes: row.input_bytes,
    avg_photo_mb: row.avg_photo_mb,
    avg_width: row.avg_width,
    avg_height: row.avg_height,
    avg_megapixels: row.avg_megapixels,
    total_megapixels: row.total_megapixels,
    point_count: row.point_count,
    processing_load_score: row.processing_load_score,
    project_type: row.project_type,
    quality: row.quality,
    outputs: row.outputs,
    profileVersion: row.profile_version,
    workerVersion: row.worker_version
  });
}

function timingConfidenceLabel(sampleCount, averageDistance) {
  if (sampleCount >= 8 && averageDistance <= 1.5) return "high";
  if (sampleCount >= 3 && averageDistance <= 3.0) return "medium";
  return "low";
}

function timingConfidenceScore(label, sampleCount = 0) {
  if (label === "high") return Math.min(0.95, 0.78 + Math.min(0.17, sampleCount * 0.01));
  if (label === "medium") return Math.min(0.77, 0.52 + Math.min(0.20, sampleCount * 0.025));
  return Math.min(0.50, 0.22 + Math.min(0.25, sampleCount * 0.04));
}

function isTransferStage(stage) {
  return ["uploading", "predownloading", "downloading", "zip_upload"].includes(normalizeProcessingStage(stage));
}

function isExportStage(stage) {
  return [
    "export_model", "export_point_cloud", "export_dem", "export_dtm",
    "export_orthomosaic", "export_texture", "export_tiled_model",
    "export_reference", "exporting", "zip"
  ].includes(normalizeProcessingStage(stage));
}

function stageFeatureDistance(stageInput, target, candidate, row = {}) {
  const stage = normalizeProcessingStage(stageInput);
  let distance = 0;

  if (target.projectType !== candidate.projectType) distance += 8.0;
  if (!!target.tamsExport !== !!candidate.tamsExport) distance += 8.0;

  if (isTransferStage(stage)) {
    distance += logRatioDistance(target.totalBytes, Number(row.input_bytes || candidate.totalBytes)) * 4.0;
    distance += logRatioDistance(target.photosCount, candidate.photosCount) * 0.6;
    if (target.workerVersion && candidate.workerVersion && target.workerVersion !== candidate.workerVersion) distance += 2.5;
    return distance;
  }

  if (target.qualityMode !== candidate.qualityMode) distance += 5.0;
  if (target.profileVersion && candidate.profileVersion && target.profileVersion !== candidate.profileVersion) distance += 7.0;
  if (target.workerVersion && candidate.workerVersion && target.workerVersion !== candidate.workerVersion) distance += 2.5;

  if (["matching", "aligning"].includes(stage)) {
    distance += logRatioDistance(target.photosCount, candidate.photosCount) * 2.8;
    distance += logRatioDistance(target.totalMegapixels, candidate.totalMegapixels) * 2.0;
    distance += logRatioDistance(target.avgMegapixels, candidate.avgMegapixels) * 0.7;
    return distance;
  }

  if (["depth_maps", "model", "uv", "texture", "tiled_model", "point_cloud"].includes(stage)) {
    distance += logRatioDistance(target.totalMegapixels, candidate.totalMegapixels) * 2.8;
    distance += logRatioDistance(target.photosCount, candidate.photosCount) * 1.5;
    distance += logRatioDistance(target.processingLoadScore, candidate.processingLoadScore) * 1.2;
    return distance;
  }

  if (["ground_classification", "dem", "dtm", "orthomosaic", "colorize_model"].includes(stage)) {
    distance += logRatioDistance(target.totalMegapixels, candidate.totalMegapixels) * 1.8;
    distance += logRatioDistance(target.photosCount, candidate.photosCount) * 1.0;
    distance += logRatioDistance(target.processingLoadScore, candidate.processingLoadScore) * 1.4;
    if (Number(target.pointCount || 0) > 0 && Number(row.point_count || 0) > 0) {
      distance += logRatioDistance(target.pointCount, row.point_count) * 1.8;
    }
    return distance;
  }

  if (isExportStage(stage)) {
    const targetOutputBytes = Number(target.targetOutputBytes || 0);
    const candidateOutputBytes = Number(row.output_bytes || 0);
    if (targetOutputBytes > 0 && candidateOutputBytes > 0) {
      distance += logRatioDistance(targetOutputBytes, candidateOutputBytes) * 4.0;
    } else {
      distance += logRatioDistance(target.processingLoadScore, candidate.processingLoadScore) * 1.8;
      distance += logRatioDistance(target.totalMegapixels, candidate.totalMegapixels) * 1.0;
    }
    return distance;
  }

  distance += logRatioDistance(target.photosCount, candidate.photosCount) * 1.2;
  distance += logRatioDistance(target.totalBytes, candidate.totalBytes) * 0.8;
  distance += logRatioDistance(target.processingLoadScore, candidate.processingLoadScore) * 0.8;
  return distance;
}

function stageScaleFactor(stageInput, target, candidate, row = {}) {
  const stage = normalizeProcessingStage(stageInput);
  const ratio = (a, b) => Math.max(0.01, (Math.max(0, Number(a || 0)) + 1) / (Math.max(0, Number(b || 0)) + 1));
  let scale = 1;

  if (isTransferStage(stage)) {
    scale = Math.pow(ratio(target.totalBytes, Number(row.input_bytes || candidate.totalBytes)), 0.98);
  } else if (stage === "matching") {
    scale = Math.pow(ratio(target.photosCount, candidate.photosCount), 1.32) *
      Math.pow(ratio(target.avgMegapixels, candidate.avgMegapixels), 0.35);
  } else if (stage === "aligning") {
    scale = Math.pow(ratio(target.photosCount, candidate.photosCount), 1.08) *
      Math.pow(ratio(target.avgMegapixels, candidate.avgMegapixels), 0.28);
  } else if (stage === "depth_maps") {
    scale = Math.pow(ratio(target.totalMegapixels, candidate.totalMegapixels), 0.98) *
      Math.pow(getQualityModeTimeFactor(target.qualityMode) / getQualityModeTimeFactor(candidate.qualityMode), 0.85);
  } else if (["model", "uv", "texture", "tiled_model", "point_cloud"].includes(stage)) {
    scale = Math.pow(ratio(target.processingLoadScore, candidate.processingLoadScore), 0.82);
  } else if (["ground_classification", "dem", "dtm", "orthomosaic", "colorize_model"].includes(stage)) {
    if (Number(target.pointCount || 0) > 0 && Number(row.point_count || 0) > 0) {
      scale = Math.pow(ratio(target.pointCount, row.point_count), 0.78);
    } else {
      scale = Math.pow(ratio(target.processingLoadScore, candidate.processingLoadScore), 0.72);
    }
  } else if (isExportStage(stage)) {
    const targetOutputBytes = Number(target.targetOutputBytes || 0);
    const candidateOutputBytes = Number(row.output_bytes || 0);
    scale = targetOutputBytes > 0 && candidateOutputBytes > 0
      ? Math.pow(ratio(targetOutputBytes, candidateOutputBytes), 0.92)
      : Math.pow(ratio(target.processingLoadScore, candidate.processingLoadScore), 0.62);
  } else {
    scale = Math.pow(ratio(target.processingLoadScore, candidate.processingLoadScore), 0.55);
  }

  return clampNumber(scale, 0.18, 5.5);
}

function estimateStageFromMetricRows(stage, targetInput, rows, fallbackSeconds) {
  const target = { ...buildJobFeatures(targetInput), ...targetInput };
  const now = Date.now();

  let candidates = (rows || []).map((row) => {
    const durationSeconds = Number(row.duration_ms || 0) / 1000;
    if (!(durationSeconds > 0) || durationSeconds > 7 * 24 * 3600) return null;

    const candidate = metricRowFeatures(row);
    if (candidate.qualityMode !== target.qualityMode) return null;
    // La muestra ya está filtrada por la misma fase. No se exige que el
    // conjunto completo de entregables sea idéntico: una fase válida puede
    // reutilizarse aunque el trabajo histórico generase otros productos.
    if (candidate.projectType !== target.projectType) return null;
    if (!!candidate.tamsExport !== !!target.tamsExport) return null;
    const distance = stageFeatureDistance(stage, target, candidate, row);
    const scale = stageScaleFactor(stage, target, candidate, row);
    const predictedSeconds = durationSeconds * scale;
    const ageDays = Math.max(0, (now - new Date(row.created_at || row.finished_at || now).getTime()) / 86400000);
    const recencyWeight = 1 / (1 + ageDays / 180);
    const weight = recencyWeight / Math.pow(0.30 + distance, 2);

    return {
      id: row.job_id,
      predictedSeconds,
      distance,
      weight,
      outputBytes: Number(row.output_bytes || 0),
      candidate,
      row
    };
  }).filter(Boolean).sort((a, b) => a.distance - b.distance).slice(0, 16);

  if (candidates.length >= 3) {
    const ordered = candidates.map((item) => item.predictedSeconds).sort((a, b) => a - b);
    const median = ordered[Math.floor(ordered.length / 2)] || 1;
    const filtered = candidates.filter((item) => item.predictedSeconds >= median * 0.22 && item.predictedSeconds <= median * 4.5);
    if (filtered.length >= 3) candidates = filtered;
  }

  const sampleCount = candidates.length;
  if (!sampleCount) {
    const seconds = Math.max(1, Math.round(fallbackSeconds));
    return {
      seconds,
      low_seconds: Math.max(1, Math.round(seconds * 0.55)),
      high_seconds: Math.max(seconds + 1, Math.round(seconds * 1.75)),
      sample_count: 0,
      average_distance: null,
      confidence: "low",
      confidence_score: 0.20,
      method: "stage_formula_fallback",
      estimated_output_bytes: null
    };
  }

  const weightSum = candidates.reduce((sum, item) => sum + item.weight, 0) || 1;
  const weightedAverage = candidates.reduce((sum, item) => sum + item.predictedSeconds * item.weight, 0) / weightSum;
  const ordered = candidates.map((item) => item.predictedSeconds).sort((a, b) => a - b);
  const median = ordered[Math.floor(ordered.length / 2)];
  let seconds = (weightedAverage * 0.72) + (median * 0.28);


  const variance = candidates.reduce((sum, item) => {
    const diff = item.predictedSeconds - weightedAverage;
    return sum + item.weight * diff * diff;
  }, 0) / weightSum;
  const std = Math.sqrt(Math.max(0, variance));
  const averageDistance = candidates.reduce((sum, item) => sum + item.distance, 0) / sampleCount;
  const confidence = timingConfidenceLabel(sampleCount, averageDistance);
  const minimumSpread = confidence === "high" ? seconds * 0.12 : confidence === "medium" ? seconds * 0.22 : seconds * 0.42;
  const spread = Math.max(std * 1.15, minimumSpread);

  const outputCandidates = candidates.filter((item) => item.outputBytes > 0);
  let estimatedOutputBytes = null;
  if (outputCandidates.length) {
    const outputWeight = outputCandidates.reduce((sum, item) => sum + item.weight, 0) || 1;
    estimatedOutputBytes = Math.round(outputCandidates.reduce((sum, item) => {
      const sourceLoad = Math.max(1, item.candidate.processingLoadScore);
      const targetLoad = Math.max(1, target.processingLoadScore);
      const scaled = item.outputBytes * clampNumber(Math.pow(targetLoad / sourceLoad, 0.72), 0.20, 5);
      return sum + scaled * item.weight;
    }, 0) / outputWeight);
  }

  return {
    seconds: Math.max(1, Math.round(seconds)),
    low_seconds: Math.max(1, Math.round(seconds - spread)),
    high_seconds: Math.max(Math.round(seconds) + 1, Math.round(seconds + spread)),
    sample_count: sampleCount,
    average_distance: Math.round(averageDistance * 1000) / 1000,
    confidence,
    confidence_score: timingConfidenceScore(confidence, sampleCount),
    method: sampleCount === 1 ? "stage_historical_exact_single" : "stage_historical_exact_neighbors",
    estimated_output_bytes: estimatedOutputBytes
  };
}

async function loadTimingMetricRows(pool, stages) {
  const normalized = [...new Set((stages || []).map(normalizeProcessingStage).filter(Boolean))];
  if (!normalized.length) return new Map();

  let rows = [];
  try {
    const result = await pool.query(
      `select job_id, stage, duration_ms, input_bytes, output_bytes, item_count,
              photos_count, avg_photo_mb, avg_width, avg_height, avg_megapixels,
              total_megapixels, point_count, processing_load_score,
              project_type, quality, outputs, profile_version, worker_version,
              created_at, finished_at
         from job_stage_metrics
        where valid_for_training = true
          and duration_ms > 0
          and stage = any($1::text[])
        order by created_at desc
        limit 4000`,
      [normalized]
    );
    rows = result.rows || [];
  } catch (e) {
    // Durante los pocos milisegundos de una primera migración, la web debe
    // seguir pudiendo calcular precio/ETA con las fórmulas de respaldo.
    if (String(e?.code || "") === "42P01") return new Map();
    throw e;
  }

  const byStage = new Map();
  for (const row of rows) {
    const stage = normalizeProcessingStage(row.stage);
    if (!byStage.has(stage)) byStage.set(stage, []);
    byStage.get(stage).push(row);
  }
  return byStage;
}


function estimateStageOutputBytesFromRows(stage, targetInput, rows) {
  const target = buildJobFeatures(targetInput);
  const candidates = (rows || [])
    .filter((row) => Number(row.output_bytes || 0) > 0)
    .map((row) => {
      const candidate = metricRowFeatures(row);
      const distance = stageFeatureDistance(stage, target, candidate, row);
      const sourceLoad = Math.max(1, candidate.processingLoadScore);
      const targetLoad = Math.max(1, target.processingLoadScore);
      const byteScale = clampNumber(Math.pow(targetLoad / sourceLoad, 0.72), 0.18, 5.5);
      return {
        bytes: Number(row.output_bytes || 0) * byteScale,
        distance,
        weight: 1 / Math.pow(0.35 + distance, 2)
      };
    })
    .sort((a, b) => a.distance - b.distance)
    .slice(0, 12);

  if (!candidates.length) return null;
  const weightSum = candidates.reduce((sum, item) => sum + item.weight, 0) || 1;
  return Math.max(1, Math.round(
    candidates.reduce((sum, item) => sum + item.bytes * item.weight, 0) / weightSum
  ));
}

function estimateZipBytesFromRows(targetInput, rows) {
  const target = buildJobFeatures(targetInput);
  const direct = estimateStageOutputBytesFromRows("zip", target, rows);
  if (direct && direct > 0) return direct;
  const candidates = (rows || []).filter((row) => Number(row.output_bytes || 0) > 0).map((row) => {
    const candidate = metricRowFeatures(row);
    const distance = stageFeatureDistance("zip", target, candidate, row);
    const loadScale = clampNumber(
      Math.pow(Math.max(1, target.processingLoadScore) / Math.max(1, candidate.processingLoadScore), 0.72),
      0.20,
      5
    );
    return {
      bytes: Number(row.output_bytes) * loadScale,
      weight: 1 / Math.pow(0.35 + distance, 2),
      distance
    };
  }).sort((a, b) => a.distance - b.distance).slice(0, 12);

  if (candidates.length) {
    const weights = candidates.reduce((sum, item) => sum + item.weight, 0) || 1;
    return Math.max(1, Math.round(candidates.reduce((sum, item) => sum + item.bytes * item.weight, 0) / weights));
  }

  const outputsFactor = 0.85 + Math.min(3.0, target.outputsRequested.length * 0.38);
  return Math.max(1, Math.round(Math.max(1, target.totalBytes) * outputsFactor));
}

async function estimateStageTimingPlan(pool, targetInput, predictionInput = null) {
  const target = buildJobFeatures(targetInput);
  const fallbackPrediction = predictionInput || await estimateProcessingDetailsHistorical(pool, target);
  const stages = requiredTimingStages(target);
  const metricStages = [...stages, "uploading", "downloading", "zip_upload"];
  const rowsByStage = await loadTimingMetricRows(pool, metricStages);

  const fallbackTotal = Math.max(60, Number(fallbackPrediction?.seconds || estimateProcessingSecondsFromFallback(target)));
  const fallbackWeightTotal = stages.reduce((sum, stage) => sum + Number(FALLBACK_STAGE_WEIGHTS[stage] || 0.01), 0) || 1;
  const actualStageSeconds = targetInput?.actualStageSeconds && typeof targetInput.actualStageSeconds === "object"
    ? targetInput.actualStageSeconds
    : {};
  const actualZipBytes = Math.max(0, Number(targetInput?.actualZipBytes || 0));
  const estimatedZipBytes = target.tamsExport
    ? 0
    : (actualZipBytes > 0 ? actualZipBytes : estimateZipBytesFromRows(target, rowsByStage.get("zip") || []));

  const stageEstimates = [];
  for (const stage of stages) {
    const actualSeconds = Number(actualStageSeconds[stage] || 0);
    const fallbackSeconds = fallbackTotal * Number(FALLBACK_STAGE_WEIGHTS[stage] || 0.01) / fallbackWeightTotal;
    const stageRows = rowsByStage.get(stage) || [];
    const estimatedOutputBytes = stage === "zip"
      ? estimatedZipBytes
      : (isExportStage(stage) ? estimateStageOutputBytesFromRows(stage, target, stageRows) : null);

    let estimate;
    if (actualSeconds > 0) {
      estimate = {
        seconds: Math.max(1, Math.round(actualSeconds)),
        low_seconds: Math.max(1, Math.round(actualSeconds)),
        high_seconds: Math.max(1, Math.round(actualSeconds)),
        sample_count: 1,
        average_distance: 0,
        confidence: "high",
        confidence_score: 1,
        method: "current_job_actual",
        estimated_output_bytes: estimatedOutputBytes || null
      };
    } else {
      const stageTarget = estimatedOutputBytes
        ? { ...target, targetOutputBytes: estimatedOutputBytes }
        : target;
      estimate = estimateStageFromMetricRows(stage, stageTarget, stageRows, fallbackSeconds);
    }

    stageEstimates.push({
      stage,
      ...estimate,
      estimated_output_bytes: estimatedOutputBytes || estimate.estimated_output_bytes || null
    });
  }

  const processingSeconds = Math.max(1, stageEstimates.reduce((sum, item) => sum + item.seconds, 0));
  const processingLow = Math.max(1, stageEstimates.reduce((sum, item) => sum + item.low_seconds, 0));
  const processingHigh = Math.max(processingSeconds + 1, stageEstimates.reduce((sum, item) => sum + item.high_seconds, 0));

  const actualUploadSeconds = finiteNumber(targetInput?.actualUploadSeconds, null);
  const uploadFallback = Math.max(5, target.totalBytes / (6 * 1024 * 1024));
  const uploadEstimate = actualUploadSeconds !== null && actualUploadSeconds > 0
    ? {
        seconds: Math.round(actualUploadSeconds),
        low_seconds: Math.round(actualUploadSeconds),
        high_seconds: Math.round(actualUploadSeconds),
        sample_count: 1,
        confidence: "high",
        confidence_score: 1,
        method: "current_job_actual"
      }
    : estimateStageFromMetricRows("uploading", target, rowsByStage.get("uploading") || [], uploadFallback);

  const actualDownloadSeconds = Math.max(0, Number(targetInput?.actualDownloadSeconds || 0));
  const downloadFallback = Math.max(5, target.totalBytes / (25 * 1024 * 1024));
  const downloadEstimate = actualDownloadSeconds > 0
    ? {
        seconds: Math.round(actualDownloadSeconds),
        low_seconds: Math.round(actualDownloadSeconds),
        high_seconds: Math.round(actualDownloadSeconds),
        sample_count: 1,
        confidence: "high",
        confidence_score: 1,
        method: "current_job_actual"
      }
    : estimateStageFromMetricRows("downloading", target, rowsByStage.get("downloading") || [], downloadFallback);

  const zipBytes = estimatedZipBytes;
  const zipUploadTarget = {
    ...target,
    totalBytes: zipBytes,
    targetOutputBytes: zipBytes,
    photosCount: 1
  };
  const zipUploadFallback = target.tamsExport ? 0 : Math.max(10, zipBytes / (8 * 1024 * 1024));
  const zipUploadEstimate = target.tamsExport
    ? { seconds: 0, low_seconds: 0, high_seconds: 0, sample_count: 0, confidence: "high", confidence_score: 1, method: "not_required" }
    : estimateStageFromMetricRows("zip_upload", zipUploadTarget, rowsByStage.get("zip_upload") || [], zipUploadFallback);

  const totalServiceSeconds = Math.max(1, uploadEstimate.seconds + downloadEstimate.seconds + processingSeconds + zipUploadEstimate.seconds);
  const totalLow = Math.max(1, uploadEstimate.low_seconds + downloadEstimate.low_seconds + processingLow + zipUploadEstimate.low_seconds);
  const totalHigh = Math.max(totalServiceSeconds + 1, uploadEstimate.high_seconds + downloadEstimate.high_seconds + processingHigh + zipUploadEstimate.high_seconds);

  const uploadEndPercent = (uploadEstimate.seconds / totalServiceSeconds) * 100;
  const processingStartPercent = ((uploadEstimate.seconds + downloadEstimate.seconds) / totalServiceSeconds) * 100;
  const processingEndPercent = target.tamsExport
    ? 100
    : ((uploadEstimate.seconds + downloadEstimate.seconds + processingSeconds) / totalServiceSeconds) * 100;
  const processingRange = Math.max(0.1, processingEndPercent - processingStartPercent);

  let accumulatedSeconds = 0;
  const planStages = stageEstimates.map((item) => {
    const startPercent = Math.round((processingStartPercent + (accumulatedSeconds / processingSeconds) * processingRange) * 10) / 10;
    accumulatedSeconds += item.seconds;
    const endPercent = Math.round((processingStartPercent + (Math.min(accumulatedSeconds, processingSeconds) / processingSeconds) * processingRange) * 10) / 10;
    return {
      ...item,
      start_percent: clampNumber(startPercent, processingStartPercent, processingEndPercent),
      end_percent: clampNumber(endPercent, processingStartPercent, processingEndPercent)
    };
  });

  const confidenceScore = stageEstimates.length
    ? stageEstimates.reduce((sum, item) => sum + Number(item.confidence_score || 0), 0) / stageEstimates.length
    : 0.2;
  const confidence = confidenceScore >= 0.78 ? "high" : confidenceScore >= 0.50 ? "medium" : "low";
  const historicalSamples = stageEstimates.reduce((sum, item) => sum + Number(item.sample_count || 0), 0);

  return {
    version: 3,
    predictor_version: TIMING_PREDICTOR_VERSION,
    method: "hybrid_per_stage",
    neighbors: historicalSamples,
    confidence,
    confidence_score: Math.round(confidenceScore * 1000) / 1000,
    historical_samples: historicalSamples,
    estimated_processing_seconds: processingSeconds,
    estimated_processing_low_seconds: processingLow,
    estimated_processing_high_seconds: processingHigh,
    estimated_total_service_seconds: totalServiceSeconds,
    estimated_total_low_seconds: totalLow,
    estimated_total_high_seconds: totalHigh,
    estimated_zip_bytes: zipBytes,
    service_segments: {
      // Claves planas conservadas para compatibilidad con la web actual.
      upload_seconds: uploadEstimate.seconds,
      download_seconds: downloadEstimate.seconds,
      processing_seconds: processingSeconds,
      zip_upload_seconds: zipUploadEstimate.seconds,
      upload: uploadEstimate,
      download: downloadEstimate,
      processing: {
        seconds: processingSeconds,
        low_seconds: processingLow,
        high_seconds: processingHigh,
        confidence,
        confidence_score: Math.round(confidenceScore * 1000) / 1000,
        method: "sum_of_stage_predictions"
      },
      zip_upload: zipUploadEstimate,
      total_service_seconds: totalServiceSeconds
    },
    upload_start_percent: 0,
    upload_end_percent: Math.round(uploadEndPercent * 10) / 10,
    download_start_percent: Math.round(uploadEndPercent * 10) / 10,
    download_end_percent: Math.round(processingStartPercent * 10) / 10,
    processing_start_percent: Math.round(processingStartPercent * 10) / 10,
    processing_end_percent: Math.round(processingEndPercent * 10) / 10,
    zip_upload_start_percent: Math.round(processingEndPercent * 10) / 10,
    final_percent: 100,
    stages: planStages
  };
}


async function recalculateJobTimingPlan(db, jobId) {
  const { rows } = await db.query(
    `select id, status, progress, created_at, upload_started_at, upload_completed_at,
            download_seconds, photos_count, input_total_bytes, quality, project_type, outputs,
            avg_photo_mb, avg_width, avg_height, avg_megapixels,
            total_megapixels, point_count, processing_load_score, exif_summary
       from jobs where id = $1`,
    [jobId]
  );
  if (!rows.length) return null;

  const job = rows[0];
  const status = String(job.status || "").toLowerCase();
  if (["done", "failed", "cancelled"].includes(status)) {
    return job?.exif_summary?._xproces?.timing_prediction || null;
  }

  const actualRows = await db.query(
    `select stage, duration_ms
       from job_stage_metrics
      where job_id = $1
        and duration_ms > 0`,
    [jobId]
  );
  const actualStageSeconds = {};
  for (const row of actualRows.rows) {
    actualStageSeconds[normalizeProcessingStage(row.stage)] = Number(row.duration_ms || 0) / 1000;
  }

  const actualUploadSeconds = job.upload_completed_at && (job.upload_started_at || job.created_at)
    ? Math.max(0, (new Date(job.upload_completed_at).getTime() - new Date(job.upload_started_at || job.created_at).getTime()) / 1000)
    : null;
  const actualZipBytes = Math.max(0, Number(job?.exif_summary?._xproces?.output_metrics?.zip_bytes || 0));

  const features = buildJobFeatures(job);
  features.actualUploadSeconds = actualUploadSeconds;
  features.actualDownloadSeconds = Number(job.download_seconds || actualStageSeconds.downloading || 0);
  features.actualStageSeconds = actualStageSeconds;
  features.actualZipBytes = actualZipBytes;

  const fallback = await estimateProcessingDetailsHistorical(db, features);
  const plan = await estimateStageTimingPlan(db, features, fallback);
  const currentExif = job.exif_summary || {};
  const updatedExif = stripNullCharsDeep({
    ...currentExif,
    _xproces: {
      ...((currentExif && currentExif._xproces) || {}),
      timing_prediction: plan,
      prediction_method: plan.method,
      prediction_neighbors: plan.historical_samples,
      predictor_version: TIMING_PREDICTOR_VERSION
    }
  });

  await db.query(
    `update jobs
        set estimated_processing_seconds = $2,
            exif_summary = $3,
            updated_at = now()
      where id = $1`,
    [jobId, plan.estimated_processing_seconds, updatedExif]
  );

  return plan;
}

function getPlannedProgressForStage(exifSummary, stage) {
  const normalizedStage = normalizeProcessingStage(stage);
  if (!normalizedStage) return null;

  const plan = exifSummary?._xproces?.timing_prediction;
  if (!plan || !Array.isArray(plan.stages)) {
    if (normalizedStage === "uploading" || normalizedStage === "receiving") return 0;
    if (normalizedStage === "downloading") return 9;
    if (normalizedStage === "zip_upload") return 85;
    if (normalizedStage === "final" || normalizedStage === "done") return 100;
    return null;
  }

  if (normalizedStage === "uploading" || normalizedStage === "receiving") return Number(plan.upload_start_percent ?? 0);
  if (normalizedStage === "queued" || normalizedStage === "running") return Number(plan.upload_end_percent ?? plan.download_start_percent ?? 0);
  if (normalizedStage === "downloading") return Number(plan.download_start_percent ?? plan.upload_end_percent ?? 0);
  if (normalizedStage === "zip_upload") return Number(plan.zip_upload_start_percent ?? 85);
  if (normalizedStage === "final" || normalizedStage === "done") return Number(plan.final_percent ?? 100);

  const found = plan.stages.find((item) => normalizeProcessingStage(item.stage) === normalizedStage);
  return found ? Number(found.start_percent) : null;
}

async function estimateProcessingSeconds(pool, job) {
  if (!job) return 0;
  const features = buildJobFeatures(job);
  const fallback = await estimateProcessingDetailsHistorical(pool, features);
  const plan = await estimateStageTimingPlan(pool, features, fallback);
  return plan.estimated_processing_seconds;
}

async function estimateRemainingServiceSeconds(pool, job) {
  if (!job) return 0;
  const status = String(job.status || "").toLowerCase();
  if (["done", "failed", "cancelled"].includes(status)) return 0;

  let plan = job?.exif_summary?._xproces?.timing_prediction || null;
  if (!plan) {
    const features = buildJobFeatures(job);
    const fallback = await estimateProcessingDetailsHistorical(pool, features);
    plan = await estimateStageTimingPlan(pool, features, fallback);
  }

  const segments = plan?.service_segments || {};
  const totalService = Math.max(1, Number(
    plan?.estimated_total_service_seconds ||
    segments.total_service_seconds ||
    plan?.estimated_processing_seconds ||
    1
  ));

  if (["queued", "tams_pending_download"].includes(status)) {
    // La subida inicial ya ha terminado cuando el trabajo entra en cola.
    return Math.max(1, Math.round(
      Number(segments.download_seconds || segments.download?.seconds || 0) +
      Number(segments.processing_seconds || segments.processing?.seconds || plan?.estimated_processing_seconds || 0) +
      Number(segments.zip_upload_seconds || segments.zip_upload?.seconds || 0)
    ));
  }

  if (status === "running") {
    // El porcentaje nuevo representa el servicio completo y nunca retrocede.
    const progress = clampNumber(Number(job.progress || 0), 0, 100);
    return Math.max(1, Math.round(totalService * (1 - progress / 100)));
  }

  return Math.max(1, Math.round(totalService));
}

function formatEtaSeconds(seconds) {
  const s = Math.max(0, Math.round(seconds));
  const h = Math.floor(s / 3600);
  const m = Math.ceil((s % 3600) / 60);

  if (h <= 0) return `${m} min`;
  return `${h} h ${m} min`;
}

app.get("/jobs/:id/eta", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `select id, status, progress, photos_count, input_total_bytes, created_at,
              quality, project_type, outputs, exif_summary,
              avg_photo_mb, avg_width, avg_height, avg_megapixels,
              total_megapixels, processing_load_score
         from jobs
        where id = $1`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const targetJob = rows[0];

    const allRows = await pool.query(
      `select id, status, progress, photos_count, input_total_bytes, created_at,
              quality, project_type, outputs, exif_summary,
              avg_photo_mb, avg_width, avg_height, avg_megapixels,
              total_megapixels, processing_load_score
         from jobs
        where status in ('queued', 'running')
        order by created_at asc`
    );

    const activeJobs = allRows.rows;

    let waitSeconds = 0;

    for (const j of activeJobs) {
      if (j.id === id) break;

      waitSeconds += await estimateRemainingServiceSeconds(pool, j);
    }

    const ownProcessingSeconds = await estimateProcessingSeconds(pool, targetJob);
    const ownServiceSeconds = await estimateRemainingServiceSeconds(pool, targetJob);
    const totalSeconds = waitSeconds + ownServiceSeconds;

    res.json({
      ok: true,
      job_id: id,
      status: targetJob.status,
      quality_mode: getJobQualityMode(targetJob),
      quality_mode_label: getQualityModeLabel(getJobQualityMode(targetJob)),
      queue_wait_seconds: waitSeconds,
      own_processing_seconds: ownProcessingSeconds,
      own_remaining_service_seconds: ownServiceSeconds,
      total_estimated_seconds: totalSeconds,
      queue_wait_human: formatEtaSeconds(waitSeconds),
      own_processing_human: formatEtaSeconds(ownProcessingSeconds),
      own_remaining_service_human: formatEtaSeconds(ownServiceSeconds),
      total_estimated_human: formatEtaSeconds(totalSeconds)
    });
  } catch (e) {
    console.error("eta error", e);
    res.status(500).json({ ok: false, error: "eta error" });
  }
});


app.get("/jobs/:id/timing", async (req, res) => {
  try {
    const { id } = req.params;
    const { rows } = await pool.query(
      `select id, status, progress, stage, photos_count, input_total_bytes,
              quality, project_type, outputs, exif_summary,
              avg_photo_mb, avg_width, avg_height, avg_megapixels,
              total_megapixels, processing_load_score,
              estimated_processing_seconds, processing_seconds, total_seconds
         from jobs
        where id = $1`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const job = rows[0];
    const actual = await pool.query(
      `with paired as (
         select s.stage,
                s.created_at as started_at,
                e.created_at as finished_at
           from job_stage_events s
           join lateral (
             select x.created_at
               from job_stage_events x
              where x.job_id = s.job_id
                and x.stage = s.stage
                and x.event_type = 'end'
                and x.id > s.id
              order by x.id
              limit 1
           ) e on true
          where s.job_id = $1
            and s.event_type = 'start'
       )
       select stage,
              min(started_at) as started_at,
              max(finished_at) as finished_at,
              round(sum(extract(epoch from (finished_at - started_at))))::integer as seconds
         from paired
        where finished_at >= started_at
        group by stage
        order by min(started_at)`,
      [id]
    );

    res.json({
      ok: true,
      job_id: id,
      status: job.status,
      progress: job.progress,
      stage: job.stage,
      features: buildJobFeatures(job),
      estimated_processing_seconds: job.estimated_processing_seconds,
      actual_processing_seconds: job.processing_seconds,
      total_seconds: job.total_seconds,
      prediction: job?.exif_summary?._xproces?.timing_prediction || null,
      actual_stages: actual.rows,
      training_metrics: (await pool.query(
        `select stage, duration_ms, input_bytes, output_bytes, item_count,
                valid_for_training, invalid_reason, profile_version, worker_version, metrics
           from job_stage_metrics
          where job_id = $1
          order by started_at nulls last`,
        [id]
      )).rows
    });
  } catch (e) {
    console.error("timing endpoint error", e);
    res.status(500).json({ ok: false, error: "timing error" });
  }
});

// =====================
// LISTEN
// =====================
const port = Number(process.env.PORT) || 3000;

// Limpieza de seguridad: elimina storage de jobs terminales con más de 24h.
setInterval(async () => {
  try {
    const { rows } = await pool.query(
      `select id
         from jobs
        where status in ('done', 'failed', 'cancelled')
          and updated_at < now() - interval '24 hours'`
    );

    for (const row of rows) {
      const cleanup = cleanupJobStorage(row.id, { input: true, output: true });
      console.log("Cleanup periódico:", row.id, cleanup);
    }
  } catch (e) {
    console.error("cleanup interval error", e);
  }
}, 60 * 60 * 1000);

app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});





