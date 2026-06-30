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
  if (["max", "maximum", "maxima", "máxima", "maxima calidad", "máxima calidad", "maxima_calidad", "máxima_calidad", "maximum_quality", "ultra", "high", "highest", "best"].includes(raw)) return "max";

  return "normal";
}

function getQualityModeLabel(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return "Rápido";
  if (normalized === "max") return "Máxima calidad";
  return "Normal";
}

function getQualityModeTimeFactor(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return 0.65;
  if (normalized === "max") return 2.2;
  return 1;
}

function getQualityModePriceFactor(mode) {
  const normalized = normalizeQualityMode(mode);
  if (normalized === "fast") return 0.85;
  if (normalized === "max") return 1.75;
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

  if (["created", "receiving", "queued", "running", "done", "failed", "cancelled"].includes(raw)) return raw;
  if (["prepare", "preparing", "preparando"].includes(raw)) return "preparing";
  if (["import", "importing", "importando"].includes(raw)) return "importing";
  if (["match", "matching", "detecting", "detectando", "detectando_puntos"].includes(raw)) return "matching";
  if (["align", "aligning", "alineando", "alineacion", "alineación"].includes(raw)) return "aligning";
  if (["clean", "cleaning", "limpiando", "optimizando", "optimization", "optimizing"].includes(raw)) return "cleaning";
  if (["depth", "depthmaps", "depth_maps", "profundidad", "mapas_profundidad"].includes(raw)) return "depth_maps";
  if (["pointcloud", "point_cloud", "cloud", "nube", "nube_puntos"].includes(raw)) return "point_cloud";
  if (["dem", "dtm", "terreno"].includes(raw)) return "dem";
  if (["model", "modelo", "mesh", "malla"].includes(raw)) return "model";
  if (["uv", "texture", "texturing", "textura", "texturizando"].includes(raw)) return "texture";
  if (["export", "exporting", "exportando"].includes(raw)) return "exporting";
  if (["report", "pdf", "informe"].includes(raw)) return "report";

  return raw.replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "") || null;
}

function inferProcessingStage({ status, stage, progress, message }) {
  const direct = normalizeProcessingStage(stage);
  if (direct) return direct;

  const st = String(status || "").toLowerCase();
  if (["done", "failed", "cancelled", "queued", "receiving", "created"].includes(st)) return st;

  const text = String(message || "").toLowerCase();
  if (text.includes("prepar")) return "preparing";
  if (text.includes("import")) return "importing";
  if (text.includes("detect") || text.includes("puntos clave") || text.includes("match")) return "matching";
  if (text.includes("aline")) return "aligning";
  if (text.includes("limpi") || text.includes("optim")) return "cleaning";
  if (text.includes("profundidad") || text.includes("depth")) return "depth_maps";
  if (text.includes("nube") || text.includes("point cloud")) return "point_cloud";
  if (text.includes("dem") || text.includes("terreno") || text.includes("dtm")) return "dem";
  if (text.includes("modelo") || text.includes("model") || text.includes("malla")) return "model";
  if (text.includes("textura") || text.includes("texture") || text.includes("uv")) return "texture";
  if (text.includes("export")) return "exporting";
  if (text.includes("informe") || text.includes("pdf")) return "report";

  const p = Number(progress);
  if (Number.isFinite(p)) {
    if (p >= 100) return "done";
    if (p >= 96) return "report";
    if (p >= 92) return "exporting";
    if (p >= 86) return "texture";
    if (p >= 84) return "model";
    if (p >= 82) return "dem";
    if (p >= 78) return "point_cloud";
    if (p >= 65) return "depth_maps";
    if (p >= 50) return "cleaning";
    if (p >= 45) return "aligning";
    if (p >= 30) return "matching";
    if (p >= 15) return "importing";
    if (p >= 10) return "preparing";
  }

  return null;
}

async function recordJobStageEvent(pool, jobId, stage, eventType) {
  const normalizedStage = normalizeProcessingStage(stage);
  const normalizedType = String(eventType || "progress").trim().toLowerCase();
  if (!normalizedStage || !["start", "end", "progress"].includes(normalizedType)) return;

  try {
    await pool.query(
      `insert into job_stage_events (job_id, stage, event_type, created_at)
       values ($1, $2, $3, now())`,
      [jobId, normalizedStage, normalizedType]
    );
  } catch (e) {
    // No paramos el proceso por métricas: si falla el histórico, el job debe seguir.
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
      add column if not exists archived_summary_saved_at timestamptz
  `);
}

async function markDownloadStarted(jobId) {
  try {
    await ensureDownloadCleanupColumns();
    await pool.query(
      `update jobs
          set download_started_at = coalesce(download_started_at, now()),
              updated_at = now()
        where id = $1`,
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

  if (type === "tams") return 0;

  let price = 10;

  if (photos > 1000) price += 40;
  else if (photos > 500) price += 20;
  else if (photos > 100) price += 10;

  if (qualityMode === "max") price += 20;

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
  filename: (_req, file, cb) => {
    const safe = file.originalname.replace(/[^\w.\-() ]+/g, "_");
    cb(null, safe);
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
        download_seconds,
        processing_seconds,
        total_seconds
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
    const qualityMode = normalizeQualityMode(req.body?.quality_mode);
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
const qualityMode = normalizeQualityMode(req.body?.quality_mode);
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

    const insertSql = jobsHasQualityMode
      ? `insert into jobs (
          status,
          photos_count,
          price,
          exif_summary,
          client_email,
          project_name,
          client_name,
          user_id,
          quality_mode,
          project_type,
          outputs
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
        returning *`
      : `insert into jobs (
          status,
          photos_count,
          price,
          exif_summary,
          client_email,
          project_name,
          client_name,
          user_id,
          project_type,
          outputs
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
        returning *`;

    const insertParams = jobsHasQualityMode
      ? [
          "created",
          0,
          0,
          exifSummary,
          clientEmail,
          projectName,
          clientName,
          userId,
          qualityMode,
          projectType,
          JSON.stringify(outputsRequested)
        ]
      : [
          "created",
          0,
          0,
          exifSummary,
          clientEmail,
          projectName,
          clientName,
          userId,
          projectType,
          JSON.stringify(outputsRequested)
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

    const selectJobSql = jobsHasQualityMode
      ? "select id, status, photos_count, quality_mode, exif_summary from jobs where id = $1"
      : "select id, status, photos_count, exif_summary from jobs where id = $1";

    const { rows } = await pool.query(selectJobSql, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const job = rows[0];
    const qualityMode = normalizeQualityMode(
      req.body?.quality_mode ||
      job.quality_mode ||
      job?.exif_summary?._xproces?.quality_mode ||
      "normal"
    );

    const updatedExifSummary = stripNullCharsDeep({
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

    const photosCount = inputs.length;
    const inputTotalBytes = getInputTotalBytes(id);
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
    const outputsRequested = Array.isArray(job.exif_summary?._xproces?.outputs_requested) ? job.exif_summary._xproces.outputs_requested : [];
    const tamsExport = !!job.exif_summary?._xproces?.tams_export || projectType === "tams";

    const estimatedSeconds = await estimateProcessingSecondsFromInputsHistorical(
      pool,
      photosCount,
      inputTotalBytes,
      qualityMode,
      tamsExport
    );

    const price = calculatePriceFromInputs(
      photosCount,
      inputTotalBytes,
      estimatedSeconds,
      qualityMode,
      outputsRequested,
      projectType
    );

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
    const submitUpdateSql = jobsHasQualityMode
      ? `update jobs
           set status='queued',
               stage='queued',
               photos_count=$2,
               price=$3,
               input_total_bytes=$4,
               quality_mode=$5,
               exif_summary=$6,
               project_type=$7,
               outputs=$8::jsonb,
               estimated_processing_seconds=$9,
               progress=0,
               message='En cola',
               error=null,
               updated_at=now(),
               started_at = case when started_at is null then now() else started_at end
         where id=$1`
      : `update jobs
           set status='queued',
               stage='queued',
               photos_count=$2,
               price=$3,
               input_total_bytes=$4,
               exif_summary=$5,
               project_type=$6,
               outputs=$7::jsonb,
               estimated_processing_seconds=$8,
               progress=0,
               message='En cola',
               error=null,
               updated_at=now(),
               started_at = case when started_at is null then now() else started_at end
         where id=$1`;

    const submitUpdateParams = jobsHasQualityMode
      ? [id, photosCount, price, inputTotalBytes, qualityMode, updatedExifSummary, projectType, JSON.stringify(outputsRequested), estimatedSeconds]
      : [id, photosCount, price, inputTotalBytes, updatedExifSummary, projectType, JSON.stringify(outputsRequested), estimatedSeconds];

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
      estimated_human: formatEtaSeconds(estimatedSeconds)
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
app.post("/jobs/:id/upload", uploadInput.any(), async (req, res) => {
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

    if (nextStatus === "created") {
      nextStatus = "receiving";
    } else if (!nextStatus) {
      nextStatus = "receiving";
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

app.post("/jobs/:id/complete-upload", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id, status, photos_count, input_total_bytes, exif_summary from jobs where id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ ok: false, error: "job not found" });

    const job = rows[0];
    const serverFiles = listInputImages(id);
    const realCount = serverFiles.length;
    const realBytes = getInputTotalBytes(id);
    const expectedPhotos = Number(req.body?.expected_photos_count || job.exif_summary?._xproces?.totalPhotos || 0) || Number(job.photos_count || 0) || 0;
    const expectedBytes = Number(req.body?.expected_total_bytes || job.exif_summary?._xproces?.totalBytes || 0) || 0;
    const projectType = String(job.exif_summary?._xproces?.project_type || "fotogrametria").toLowerCase();
    const isTams = projectType === "tams" || !!job.exif_summary?._xproces?.tams_export;

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
               updated_at = now()
         where id = $4`,
        [realCount, realBytes, nextMessage, id]
      );
    }

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
      `select id, status, stage, progress, started_at, finished_at, created_at, processing_started_at, processing_finished_at
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
      nextStageInput = "export";
      nextProgressParam = 99;
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

    const nextStage = inferProcessingStage({ status: incomingStatus || currentStatus, stage: nextStageInput, progress: nextProgressParam, message: nextMessageParam });
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
        const rawLiveLog = process_log ?? log_line ?? log;
        const liveText = String(rawLiveLog ?? "").replace(/\u0000/g, "").slice(0, 20000);
        if (liveText.trim()) {
          const liveLogPath = path.join(outputDir(id), "metashape-python-log.txt");
          fs.appendFileSync(liveLogPath, liveText.endsWith("\n") ? liveText : liveText + "\n", "utf8");
        }
      } catch (logError) {
        console.error("live log append error", logError?.message || logError);
      }
    }

    if (nextStage && nextStage !== currentStage) {
      if (currentStage) await recordJobStageEvent(pool, id, currentStage, "end");
      await recordJobStageEvent(pool, id, nextStage, "start");
    } else if (nextStage && p !== null && p !== currentProgress) {
      await recordJobStageEvent(pool, id, nextStage, "progress");
    }

    if (terminalStatus && currentStage && currentStage !== nextStage) {
      await recordJobStageEvent(pool, id, currentStage, "end");
    }
    if (terminalStatus && nextStage) {
      await recordJobStageEvent(pool, id, nextStage, "start");
      await recordJobStageEvent(pool, id, nextStage, "end");
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
             processing_started_at = case when $2 = 'running' and processing_started_at is null then now() else processing_started_at end,
             processing_finished_at = case when $2 in ('done','failed','cancelled') then now() else processing_finished_at end,
             finished_at = case when $2 in ('done','failed','cancelled') then now() else finished_at end
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

// =====================
// WORKER (HTTP por internet)
// =====================
app.post("/worker/claim", requireWorkerAuth, async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `update jobs
          set status='running',
              stage='running',
              progress=0,
              message='Worker claimed',
              updated_at=now(),
              started_at=case when started_at is null then now() else started_at end,
              processing_started_at=case when processing_started_at is null then now() else processing_started_at end
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

    const job = rows[0];
    await recordJobStageEvent(pool, job.id, "running", "start");
    res.json({
      ok: true,
      job: {
        ...job,
        quality_mode: normalizeQualityMode(job.quality_mode || job?.exif_summary?._xproces?.quality_mode || "normal")
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
      "select id, input_purged from jobs where id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ ok: false, error: "job not found" });

    const job = rows[0];

    if (job.input_purged) {
      return res.json({ ok: true, alreadyPurged: true });
    }

    const dir = inputDir(id);
    const purged = safeRemoveDir(dir);

    if (!purged) {
      console.error("ERROR: input no se borró completamente:", dir);
    } else {
      console.log("Input eliminado correctamente:", id);
    }

    await pool.query(
     `update jobs
         set input_purged = $2,
             input_purged_at = case when $2 then now() else input_purged_at end,
             status = case when status = 'tams_pending_download' and $2 then 'tams_downloaded' else status end,
             stage = case when status = 'tams_pending_download' and $2 then 'downloaded_pc' else stage end,
             message = case when status = 'tams_pending_download' and $2 then 'TAMS descargado en PC' else message end,
             updated_at = now()
       where id = $1`,
     [id, purged]
   );

    res.json({ ok: true, purged });
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

function estimateProcessingSecondsFromFallback(job) {
  const photos = Number(job.photos_count || 0);
  const bytes = Number(job.input_total_bytes || 0);
  const gb = bytes / (1024 * 1024 * 1024);
  const qualityMode = normalizeQualityMode(job?.quality_mode);

  return Math.round(
    (120 + (photos * 18) + (gb * 420)) * getQualityModeTimeFactor(qualityMode)
  );
}

async function estimateProcessingSecondsFromInputsHistorical(pool, photosCount, totalBytes, qualityMode = "normal", tamsExport = false) {
  const photos = Number(photosCount || 0);
  const bytes = Number(totalBytes || 0);
  const normalizedQualityMode = normalizeQualityMode(qualityMode);
  const startDate = "2026-04-05";

  const tryQueries = [];

  if (jobsHasQualityMode) {
    tryQueries.push({
      sql: `select processing_seconds, photos_count
            from jobs
            where status = 'done'
              and processing_seconds is not null
              and photos_count > 0
              and created_at >= $1
              and quality_mode = $2
              and coalesce((exif_summary->'_xproces'->>'tams_export')::boolean, false) = $3
            order by created_at desc
            limit 50`,
      params: [startDate, normalizedQualityMode, !!tamsExport]
    });
  }

  tryQueries.push({
    sql: `select processing_seconds, photos_count
          from jobs
          where status = 'done'
            and processing_seconds is not null
            and photos_count > 0
            and created_at >= $1
            and coalesce((exif_summary->'_xproces'->>'tams_export')::boolean, false) = $2
          order by created_at desc
          limit 50`,
    params: [startDate, !!tamsExport]
  });

  tryQueries.push({
    sql: `select processing_seconds, photos_count
          from jobs
          where status = 'done'
            and processing_seconds is not null
            and photos_count > 0
            and created_at >= $1
          order by created_at desc
          limit 50`,
    params: [startDate]
  });

  for (const q of tryQueries) {
    const { rows } = await pool.query(q.sql, q.params);
    if (rows.length >= 3) {
      const totalPhotos = rows.reduce((acc, r) => acc + Number(r.photos_count || 0), 0);
      const totalSeconds = rows.reduce((acc, r) => acc + Number(r.processing_seconds || 0), 0);
      if (totalPhotos > 0) {
        const secPerPhoto = totalSeconds / totalPhotos;
        return Math.round(photos * secPerPhoto * 1.25);
      }
    }
  }

  return estimateProcessingSecondsFromInputs(photos, bytes, normalizedQualityMode);
}

async function estimateProcessingSeconds(pool, job) {
  if (!job) return 0;

  const photos = Number(job.photos_count || 0);
  const bytes = Number(job.input_total_bytes || 0);
  const qualityMode = normalizeQualityMode(job?.quality_mode);
  const tamsExport = !!job?.exif_summary?._xproces?.tams_export;

  if (photos > 0 || bytes > 0) {
    return await estimateProcessingSecondsFromInputsHistorical(
      pool,
      photos,
      bytes,
      qualityMode,
      tamsExport
    );
  }

  return estimateProcessingSecondsFromFallback(job);
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
      `select id, status, progress, photos_count, input_total_bytes, created_at
       ${jobsHasQualityMode ? ", quality_mode" : ""}
       from jobs
       where id = $1`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const targetJob = rows[0];

    const allRows = await pool.query(
      `select id, status, progress, photos_count, input_total_bytes, created_at
       ${jobsHasQualityMode ? ", quality_mode" : ""}
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
      quality_mode: normalizeQualityMode(targetJob.quality_mode),
      quality_mode_label: getQualityModeLabel(targetJob.quality_mode),
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







