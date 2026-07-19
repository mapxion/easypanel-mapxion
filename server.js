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
import { randomBytes } from "crypto";


const { Pool } = pkg;

const app = express();
app.use(express.json());
app.use(cors());


// =====================
// CONFIG
// =====================
const DATA_ROOT = process.env.DATA_ROOT || "/data/mapxion";
const WORKER_TOKEN = process.env.WORKER_TOKEN || "";

const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "XprocesAdmin2026!";

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

  if (/export(model|ando\s+modelo)|exportando\s+obj|saving\s+3d\s+model/.test(text)) return "export_model";
  if (/export(pointcloud|ando\s+nube)|exportando\s+las|saving\s+point\s+cloud/.test(text)) return "export_point_cloud";
  if (/export.*(mdt|dtm)|\/mdt\.tif|\\mdt\.tif/.test(text)) return "export_dtm";
  if (/export.*(mde|dem|dsm)|\/mde(_color)?\.tif|\\mde(_color)?\.tif/.test(text)) return "export_dem";
  if (/export.*(orto|orthomosaic)|\/orto\.tif|\\orto\.tif/.test(text)) return "export_orthomosaic";
  if (/export.*textur/.test(text)) return "export_texture";
  if (/export.*(tesela|tiled)/.test(text)) return "export_tiled_model";
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
  const fromMessage = inferStageFromMessage(message);
  if (fromMessage) return fromMessage;

  const direct = normalizeProcessingStage(stage);
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
      const planned = getPlannedProgressForStage(current.exif_summary, "downloading");
      const nextProgress = Number.isFinite(planned)
        ? Math.max(Number(current.progress || 0), Math.round(planned))
        : Number(current.progress || 0);

      if (previousStage && previousStage !== "downloading") {
        await recordJobStageEvent(pool, jobId, previousStage, "end");
      }
      await recordJobStageEvent(pool, jobId, "downloading", "start");

      await pool.query(
        `update jobs
            set download_started_at = now(),
                stage = 'downloading',
                progress = greatest(coalesce(progress, 0), $2),
                message = 'Descargando y validando fotografías',
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
  const photos = Number(photosCount || 0);
  const type = String(projectType || "fotogrametria").toLowerCase();
  const outputs = Array.isArray(outputsRequested) ? outputsRequested.map(String) : [];

  if (type === "tams") return 100;

  let price = 10;

  if (photos > 1000) price += 40;
  else if (photos > 500) price += 20;
  else if (photos > 100) price += 10;

  if (qualityMode === "full") price += 20;

  const extras = {
    dem_tif: 10,       // DSM
    dtm_tif: 10,       // DTM
    contours_dxf: 5,   // Curvas de nivel
    mesh_obj: 10,
    glb: 10,
    mesh_fbx: 10,
    textures: 5
  };

  for (const output of outputs) {
    price += extras[output] || 0;
  }

  return Math.max(0, Math.ceil(price));
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
  limits: { fileSize: 5 * 1024 * 1024 * 1024 }, // 5GB
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
      base_price: 10,
      photo_supplements: [
        { range: "0–100 fotos", amount: 0 },
        { range: "101–500 fotos", amount: 10 },
        { range: "501–1000 fotos", amount: 20 },
        { range: "Más de 1000 fotos", amount: 40 }
      ],
      quality_supplements: {
        fast: 0,
        normal: 0,
        full: 20
      },
      output_supplements: {
        dsm: 10,
        dtm: 10,
        contours: 5
      }
    }
  });
});


app.get("/version", (_req, res) =>
  res.json({ version: "v40-photos-only-validation" })
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
    const outputsRequested = Array.isArray(req.body?.outputs_requested) ? req.body.outputs_requested : [];
    const projectType = String(req.body?.project_type || req.body?.xproces_meta?.project_type || "fotogrametria").toLowerCase();
    const tamsExport = !!req.body?.tams_export || projectType === "tams";

    console.log("PRICING PREVIEW parsed:", { photosCount, totalBytes, qualityMode, tamsExport });

    if (!Number.isFinite(photosCount) || photosCount < 0) {
      return res.status(400).json({ ok: false, error: "invalid photos_count" });
    }

    if (!Number.isFinite(totalBytes) || totalBytes < 0) {
      return res.status(400).json({ ok: false, error: "invalid total_bytes" });
    }

    const estimatedSeconds = await estimateProcessingSecondsFromInputsHistorical(
      pool,
      photosCount,
      totalBytes,
      qualityMode,
      tamsExport
    );

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

    return res.json({
      ok: true,
      photos_count: photosCount,
      total_bytes: totalBytes,
      quality_mode: qualityMode,
      quality_mode_label: getQualityModeLabel(qualityMode),
      tams_export: tamsExport,
      price,
      estimated_seconds: estimatedSeconds,
      estimated_human: formatEtaSeconds(estimatedSeconds)
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
      quality_mode: normalizeQualityMode(job.quality_mode || job?.exif_summary?._xproces?.quality_mode || "normal")
    });
  } catch (e) {
    console.error("get job error", e);
    res.status(500).json({ ok: false, error: "db error", message: e?.message || String(e) });
  }
});

// =====================
// PAYPAL CHECKOUT
// =====================
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

    const meta = job.exif_summary?._xproces || {};
    const photosCount = Number(meta.totalPhotos || 0) || 0;
    const totalBytes = Number(meta.totalBytes || 0) || 0;
    const qualityMode = normalizeQualityMode(job.quality_mode || meta.quality_mode || "normal");
    const projectType = String(job.project_type || meta.project_type || "fotogrametria").toLowerCase();
    const outputsRequested = Array.isArray(meta.outputs_requested) ? meta.outputs_requested : [];
    const estimatedSeconds = await estimateProcessingSecondsFromInputsHistorical(pool, photosCount, totalBytes, qualityMode, projectType === "tams");
    const price = calculatePriceFromInputs(photosCount, totalBytes, estimatedSeconds, qualityMode, outputsRequested, projectType);

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

const exifSummary = stripNullCharsDeep({
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

    const declaredStats = extractDeclaredImageStats(
      exifSummary,
      expectedPhotosCount,
      expectedTotalBytes
    );
    const initialLoadScore = calculateProcessingLoadScore({
      photosCount: expectedPhotosCount,
      totalBytes: expectedTotalBytes,
      avgMegapixels: declaredStats.avgMegapixels,
      totalMegapixels: declaredStats.totalMegapixels,
      qualityMode,
      outputsRequested,
      projectType
    });

    const estimatedSecondsForPayment = await estimateProcessingSecondsFromInputsHistorical(
      pool,
      expectedPhotosCount,
      expectedTotalBytes,
      qualityMode,
      tamsExport,
      {
        projectType,
        outputsRequested,
        avgMegapixels: declaredStats.avgMegapixels,
        totalMegapixels: declaredStats.totalMegapixels,
        processingLoadScore: initialLoadScore
      }
    );
    const initialPrice = calculatePriceFromInputs(
      expectedPhotosCount, expectedTotalBytes, estimatedSecondsForPayment, qualityMode, outputsRequested, projectType
    );
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
              created_at, upload_completed_at,
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

    const actualUploadSeconds = job.created_at && job.upload_completed_at
      ? Math.max(0, Math.round((new Date(job.upload_completed_at).getTime() - new Date(job.created_at).getTime()) / 1000))
      : null;

    const targetFeatures = {
      id,
      actualUploadSeconds,
      photosCount,
      totalBytes: inputTotalBytes,
      avgPhotoMb: inputStats.avgPhotoMb,
      avgWidth: inputStats.avgWidth,
      avgHeight: inputStats.avgHeight,
      avgMegapixels: inputStats.avgMegapixels,
      totalMegapixels: inputStats.totalMegapixels,
      processingLoadScore,
      qualityMode,
      outputsRequested,
      projectType,
      tamsExport
    };

    const prediction = await estimateProcessingDetailsHistorical(pool, targetFeatures);
    const estimatedSeconds = prediction.seconds;
    const timingPlan = await estimateStageTimingPlan(pool, targetFeatures, prediction);

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
        timing_prediction: timingPlan,
        prediction_method: prediction.method,
        prediction_neighbors: prediction.neighbors
      }
    });

    const price = calculatePriceFromInputs(
      photosCount,
      inputTotalBytes,
      estimatedSeconds,
      qualityMode,
      outputsRequested,
      projectType
    );

    if (String(job.payment_status || "").toLowerCase() === "paid") {
      const paidAmount = Number(job.payment_amount || 0);
      if (Math.abs(paidAmount - Number(price)) > 0.001) {
        return res.status(409).json({
          ok: false,
          error: "paid_amount_mismatch",
          message: `El pedido pagado es de ${paidAmount.toFixed(2)} EUR, pero la configuración final corresponde a ${Number(price).toFixed(2)} EUR.`
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
      Math.max(0, Math.min(99, Math.round(timingPlan.upload_end_percent ?? 0)))
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
      prediction_method: prediction.method,
      prediction_neighbors: prediction.neighbors,
      processing_load_score: processingLoadScore,
      image_stats: updatedExifSummary?._xproces?.image_stats || null,
      timing_prediction: timingPlan
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
              processing_started_at, processing_finished_at, exif_summary
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
      nextStatusParam = "running";
      nextStageInput = "zip_upload";
      nextProgressParam = 85;
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
      if (currentStage) await recordJobStageEvent(pool, id, currentStage, "end");
      await recordJobStageEvent(pool, id, nextStage, "start");
    } else if (nextStage && nextProgressParam !== null && nextProgressParam !== currentProgress) {
      await recordJobStageEvent(pool, id, nextStage, "progress");
    }

    // Al terminar, cerrar una sola vez la fase terminal. El bloque anterior ya
    // abrió la fase nueva cuando hubo transición.
    if (terminalStatus) {
      if (nextStage) {
        await recordJobStageEvent(pool, id, nextStage, "end");
      } else if (currentStage) {
        await recordJobStageEvent(pool, id, currentStage, "end");
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
                 when $2 in ('done','failed','cancelled') then greatest(0, extract(epoch from (now() - coalesce(processing_started_at, started_at, created_at)))::int)
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
    actualUploadSeconds: job?.upload_completed_at && job?.created_at
      ? Math.max(0, (new Date(job.upload_completed_at).getTime() - new Date(job.created_at).getTime()) / 1000)
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
      prediction_method: prediction.method,
      prediction_neighbors: prediction.neighbors
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
      prediction.seconds,
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

    res.json({ ok: true, jobs: rows });
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
      "select id, input_purged, stage, progress, exif_summary from jobs where id = $1",
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
    processingLoadScore,
    qualityMode,
    outputsRequested,
    projectType,
    tamsExport
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

  if (candidates.length >= 3) {
    const weightedSum = candidates.reduce((sum, item) => sum + (item.predictedSeconds * item.weight), 0);
    const weightSum = candidates.reduce((sum, item) => sum + item.weight, 0);
    const weightedAverage = weightSum > 0 ? weightedSum / weightSum : 0;

    const ordered = candidates.map((item) => item.predictedSeconds).sort((a, b) => a - b);
    const median = ordered[Math.floor(ordered.length / 2)];
    const blended = (weightedAverage * 0.75) + (median * 0.25);

    return {
      seconds: Math.max(60, Math.round(blended)),
      method: "historical_nearest_jobs",
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
  "closing_metashape",
  "processing_complete"
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
  closing_metashape: 0.100,
  processing_complete: 0.005
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

  const needsTexture = ["textures", "texture", "mesh_obj", "model_obj", "obj", "glb", "mesh_fbx", "fbx"].some((x) => outputs.has(x));
  const needsTiled = ["tiled_model", "tiles_3tz", "3tz"].some((x) => outputs.has(x));
  const needsPointCloud = outputs.has("las") || outputs.has("point_cloud_las") ||
    outputs.has("dem_tif") || outputs.has("dsm_tif") || outputs.has("dtm_tif") ||
    outputs.has("contours_dxf") || outputs.has("orthomosaic_tif") || outputs.has("ortho_tif");
  const needsDem = outputs.has("dem_tif") || outputs.has("dsm_tif");
  const needsDtm = outputs.has("dtm_tif") || outputs.has("contours_dxf");
  const needsOrtho = outputs.has("orthomosaic_tif") || outputs.has("ortho_tif");
  const needsReport = outputs.has("pdf_report");

  if (needsTexture) {
    stages.add("uv");
    stages.add("texture");
  }
  if (needsTiled) stages.add("tiled_model");
  if (needsPointCloud) stages.add("point_cloud");
  if (needsDtm) stages.add("ground_classification");
  if (needsDem) {
    stages.add("dem");
    stages.add("export_dem");
  }
  if (needsDtm) {
    stages.add("dtm");
    stages.add("export_dtm");
  }
  if (needsOrtho) {
    stages.add("orthomosaic");
    stages.add("colorize_model");
  }
  if (needsReport) stages.add("report");
  if (needsTiled) stages.add("export_tiled_model");
  if (["mesh_obj", "model_obj", "obj", "glb", "mesh_fbx", "fbx"].some((x) => outputs.has(x))) {
    stages.add("export_model");
  }
  if (outputs.has("las") || outputs.has("point_cloud_las")) stages.add("export_point_cloud");
  if (needsOrtho) stages.add("export_orthomosaic");
  if (needsTexture) stages.add("export_texture");

  stages.add("exporting");
  if (!features.tamsExport) stages.add("zip");
  stages.add("closing_metashape");
  stages.add("processing_complete");

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

async function estimateServiceTimingSegments(pool, targetInput, prediction) {
  const target = buildJobFeatures(targetInput);
  const actualUploadSeconds = finiteNumber(targetInput?.actualUploadSeconds, null);
  const neighbors = Array.isArray(prediction?.neighborDetails) ? prediction.neighborDetails : [];
  const uploadSamples = [];
  const downloadSamples = [];
  const zipUploadSamples = [];

  if (neighbors.length) {
    const ids = neighbors.map((item) => item.id);
    const { rows } = await pool.query(
      `select id,
              input_total_bytes,
              processing_load_score,
              download_seconds,
              processing_seconds,
              total_seconds,
              extract(epoch from (upload_completed_at - created_at))::double precision as upload_seconds,
              extract(epoch from (results_ready_at - processing_finished_at))::double precision as zip_upload_seconds
         from jobs
        where id = any($1::uuid[])`,
      [ids]
    );

    const neighborMap = new Map(neighbors.map((item) => [String(item.id), item]));
    for (const row of rows) {
      const neighbor = neighborMap.get(String(row.id));
      if (!neighbor) continue;

      const sourceBytes = Math.max(1, Number(row.input_total_bytes || neighbor.features?.totalBytes || 1));
      const byteScale = clampNumber(
        Math.pow(Math.max(1, target.totalBytes) / sourceBytes, 0.85),
        0.20,
        5
      );
      const sourceScore = Math.max(1, Number(row.processing_load_score || neighbor.features?.processingLoadScore || 1));
      const loadScale = clampNumber(
        Math.pow(Math.max(1, target.processingLoadScore) / sourceScore, 0.60),
        0.25,
        4
      );

      const uploadSeconds = Number(row.upload_seconds || 0);
      if (uploadSeconds > 0 && uploadSeconds < 6 * 3600) {
        uploadSamples.push({ seconds: uploadSeconds * byteScale, weight: neighbor.weight });
      }

      const downloadSeconds = Number(row.download_seconds || 0);
      if (downloadSeconds > 0 && downloadSeconds < 6 * 3600) {
        downloadSamples.push({ seconds: downloadSeconds * byteScale, weight: neighbor.weight });
      }

      let zipSeconds = Number(row.zip_upload_seconds || 0);
      if (!(zipSeconds > 0)) {
        const total = Number(row.total_seconds || 0);
        const processing = Number(row.processing_seconds || 0);
        const download = Number(row.download_seconds || 0);
        const upload = Number(row.upload_seconds || 0);
        const residual = total - processing - download - Math.max(0, upload);
        if (residual > 0) zipSeconds = residual;
      }
      if (zipSeconds > 0 && zipSeconds < 6 * 3600) {
        zipUploadSamples.push({ seconds: zipSeconds * loadScale, weight: neighbor.weight });
      }
    }
  }

  const inputBytes = Math.max(0, target.totalBytes);
  const fallbackUpload = Math.max(5, inputBytes / (6 * 1024 * 1024));
  const fallbackDownload = Math.max(5, inputBytes / (25 * 1024 * 1024));
  const fallbackZipUpload = target.tamsExport
    ? 0
    : Math.max(30, Math.min(1800, Number(prediction.seconds || 0) * 0.15));

  const uploadSeconds = actualUploadSeconds !== null && actualUploadSeconds > 0
    ? actualUploadSeconds
    : (weightedTimingAverage(uploadSamples) ?? fallbackUpload);
  const downloadSeconds = weightedTimingAverage(downloadSamples) ?? fallbackDownload;
  const processingSeconds = Math.max(1, Number(prediction.seconds || 1));
  const zipUploadSeconds = target.tamsExport
    ? 0
    : (weightedTimingAverage(zipUploadSamples) ?? fallbackZipUpload);
  const totalSeconds = Math.max(1, uploadSeconds + downloadSeconds + processingSeconds + zipUploadSeconds);

  return {
    upload_seconds: Math.max(0, Math.round(uploadSeconds)),
    download_seconds: Math.max(0, Math.round(downloadSeconds)),
    processing_seconds: Math.max(1, Math.round(processingSeconds)),
    zip_upload_seconds: Math.max(0, Math.round(zipUploadSeconds)),
    total_service_seconds: Math.max(1, Math.round(totalSeconds)),
    upload_samples: uploadSamples.length,
    download_samples: downloadSamples.length,
    zip_upload_samples: zipUploadSamples.length
  };
}

async function estimateStageTimingPlan(pool, targetInput, predictionInput = null) {
  const target = buildJobFeatures(targetInput);
  const prediction = predictionInput || await estimateProcessingDetailsHistorical(pool, target);
  const serviceSegments = await estimateServiceTimingSegments(pool, targetInput, prediction);
  const stages = requiredTimingStages(target);
  const neighborDetails = Array.isArray(prediction.neighborDetails) ? prediction.neighborDetails : [];

  const stageSamples = new Map();

  if (neighborDetails.length) {
    const ids = neighborDetails.map((item) => item.id);
    const { rows } = await pool.query(
      `with paired as (
         select s.job_id,
                s.stage,
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
          where s.job_id = any($1::uuid[])
            and s.event_type = 'start'
       )
       select job_id, stage,
              sum(extract(epoch from (finished_at - started_at)))::double precision as seconds
         from paired
        where finished_at >= started_at
        group by job_id, stage`,
      [ids]
    );

    const neighborMap = new Map(neighborDetails.map((item) => [String(item.id), item]));
    for (const row of rows) {
      const stage = normalizeProcessingStage(row.stage);
      const seconds = Number(row.seconds || 0);
      const neighbor = neighborMap.get(String(row.job_id));
      if (!neighbor || !stages.includes(stage) || !Number.isFinite(seconds) || seconds <= 0 || seconds > 7 * 24 * 3600) continue;

      const targetScore = Math.max(1, target.processingLoadScore);
      const sourceScore = Math.max(1, neighbor.features.processingLoadScore);
      const scaledSeconds = seconds * clampNumber(Math.pow(targetScore / sourceScore, 0.75), 0.25, 4);

      if (!stageSamples.has(stage)) stageSamples.set(stage, []);
      stageSamples.get(stage).push({
        seconds: scaledSeconds,
        weight: neighbor.weight
      });
    }
  }

  const rawDurations = {};
  for (const stage of stages) {
    const samples = stageSamples.get(stage) || [];
    if (samples.length >= 2) {
      const weighted = samples.reduce((sum, item) => sum + item.seconds * item.weight, 0);
      const weights = samples.reduce((sum, item) => sum + item.weight, 0);
      rawDurations[stage] = weights > 0 ? weighted / weights : 0;
    } else {
      rawDurations[stage] = Math.max(1, (FALLBACK_STAGE_WEIGHTS[stage] || 0.01) * prediction.seconds);
    }
  }

  const rawTotal = Object.values(rawDurations).reduce((sum, value) => sum + Number(value || 0), 0) || 1;
  const scale = prediction.seconds / rawTotal;

  // Convierte el tiempo completo del servicio a porcentaje: subida real ya
  // realizada + descarga estimada + procesado estimado + subida final del ZIP.
  const serviceTotal = Math.max(1, Number(serviceSegments.total_service_seconds || 1));
  const uploadEndPercent = (Number(serviceSegments.upload_seconds || 0) / serviceTotal) * 100;
  const processingStartPercent = (
    (Number(serviceSegments.upload_seconds || 0) + Number(serviceSegments.download_seconds || 0)) /
    serviceTotal
  ) * 100;
  const processingEndPercent = target.tamsExport
    ? 100
    : (
        (
          Number(serviceSegments.upload_seconds || 0) +
          Number(serviceSegments.download_seconds || 0) +
          Number(serviceSegments.processing_seconds || prediction.seconds || 0)
        ) / serviceTotal
      ) * 100;
  const processingRange = Math.max(0.1, processingEndPercent - processingStartPercent);

  let accumulatedSeconds = 0;
  const planStages = stages.map((stage) => {
    const seconds = Math.max(1, Math.round(rawDurations[stage] * scale));
    const startPercent = Math.round(
      (processingStartPercent + (accumulatedSeconds / prediction.seconds) * processingRange) * 10
    ) / 10;
    accumulatedSeconds += seconds;
    const endPercent = Math.round(
      (processingStartPercent + (Math.min(accumulatedSeconds, prediction.seconds) / prediction.seconds) * processingRange) * 10
    ) / 10;

    return {
      stage,
      seconds,
      start_percent: clampNumber(startPercent, processingStartPercent, processingEndPercent),
      end_percent: clampNumber(endPercent, processingStartPercent, processingEndPercent),
      sample_count: (stageSamples.get(stage) || []).length
    };
  });

  return {
    version: 2,
    method: prediction.method,
    neighbors: prediction.neighbors,
    estimated_processing_seconds: prediction.seconds,
    estimated_total_service_seconds: serviceSegments.total_service_seconds,
    service_segments: serviceSegments,
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
  const details = await estimateProcessingDetailsHistorical(pool, buildJobFeatures(job));
  return details.seconds;
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

      const est = await estimateProcessingSeconds(pool, j);

      if (j.status === "running") {
        const progress = Math.max(0, Math.min(100, Number(j.progress || 0)));
        const remaining = Math.round(est * (1 - progress / 100));
        waitSeconds += remaining;
      } else if (j.status === "queued") {
        waitSeconds += est;
      }
    }

    const ownProcessingSeconds = await estimateProcessingSeconds(pool, targetJob);
    const totalSeconds = waitSeconds + ownProcessingSeconds;

    res.json({
      ok: true,
      job_id: id,
      status: targetJob.status,
      quality_mode: getJobQualityMode(targetJob),
      quality_mode_label: getQualityModeLabel(getJobQualityMode(targetJob)),
      queue_wait_seconds: waitSeconds,
      own_processing_seconds: ownProcessingSeconds,
      total_estimated_seconds: totalSeconds,
      queue_wait_human: formatEtaSeconds(waitSeconds),
      own_processing_human: formatEtaSeconds(ownProcessingSeconds),
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
      actual_stages: actual.rows
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






