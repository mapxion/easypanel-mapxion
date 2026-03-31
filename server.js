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
  if (["max", "maximum", "maxima", "máxima", "ultra"].includes(raw)) return "max";

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

// =====================
// AUTH WORKER
// =====================
function isValidEmail(email) {
  const value = String(email || "").trim();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function generateInviteCode() {
  return randomBytes(4).toString("hex").toUpperCase();
}

function requireWorkerAuth(req, res, next) {
  const auth = req.headers.authorization || "";

  if (!WORKER_TOKEN) {
    return res
      .status(500)
      .json({ ok: false, error: "WORKER_TOKEN not configured" });
  }

  if (auth !== `Bearer ${WORKER_TOKEN}`) {
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

function ensureJobDirs(jobId) {
  fs.mkdirSync(inputDir(jobId), { recursive: true });
  fs.mkdirSync(outputDir(jobId), { recursive: true });
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

function calculatePriceFromInputs(photosCount, totalBytes, estimatedSeconds, qualityMode = "normal") {
  const photos = Number(photosCount || 0);
  const bytes = Number(totalBytes || 0);
  const gb = bytes / (1024 * 1024 * 1024);
  const hours = Number(estimatedSeconds || 0) / 3600;
  const factor = getQualityModePriceFactor(qualityMode);

  const base = 3;
  const byPhoto = photos * 0.03;
  const byGb = gb * 1.5;
  const byTime = hours * 6;

  return Math.ceil((base + byPhoto + byGb + byTime) * factor);
}

// ✅ helper: status “bloqueado” (no permitir más uploads)
function isLockedStatus(status) {
  return ["queued", "running", "done"].includes(
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

// =====================
// BASIC
// =====================
app.get("/", (_req, res) => res.send("mapxion api ok"));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});


app.get("/admin/jobs", async (_req, res) => {
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
      jobs: rows
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
  res.json({ version: "v34-worker-receiving-list" })
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

    console.log("PRICING PREVIEW parsed:", { photosCount, totalBytes, qualityMode });

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
      qualityMode
    );

    console.log("PRICING PREVIEW estimatedSeconds:", estimatedSeconds);

    const price = calculatePriceFromInputs(
      photosCount,
      totalBytes,
      estimatedSeconds,
      qualityMode
    );

    console.log("PRICING PREVIEW price:", price);

    return res.json({
      ok: true,
      photos_count: photosCount,
      total_bytes: totalBytes,
      quality_mode: qualityMode,
      quality_mode_label: getQualityModeLabel(qualityMode),
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

app.post("/admin/invite-codes/generate", async (req, res) => {
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

app.get("/admin/invite-codes", async (_req, res) => {
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
    const userId = req.headers["x-user-id"];

    if (!userId) {
      return res.status(401).json({
        ok: false,
        error: "missing_user",
        message: "Falta user_id"
      });
    }

    const { rows } = await pool.query(
      `select
        id,
        project_name,
        status,
        stage,
        progress,
        price,
        created_at
      from jobs
      where user_id = $1
      order by created_at desc`,
      [userId]
    );

    res.json({
      ok: true,
      jobs: rows
    });
  } catch (e) {
    console.error("jobs/mine error", e);
    res.status(500).json({
      ok: false,
      error: "jobs_mine_error"
    });
  }
});

app.get("/jobs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { rows } = await pool.query("select * from jobs where id = $1", [id]);
    if (!rows.length) return res.status(404).json({ error: "not found" });
    res.json(rows[0]);
  } catch (e) {
    console.error("get job error", e);
    res.status(500).json({ error: "db error" });
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
const tamsExport = !!req.body?.tams_export;

const exifSummary = stripNullCharsDeep({
  ...(exifSummaryRaw || {}),
  _xproces: {
    // 🔹 conserva lo que venga del frontend (CLAVE)
    ...((exifSummaryRaw && exifSummaryRaw._xproces) || {}),

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
    )
  }
});

const clientEmail = req.body?.client_email || null;
const projectName = req.body?.project_name || null;
const clientName = req.body?.client_name || null;
const userId = req.body?.user_id || null;
const qualityMode = normalizeQualityMode(req.body?.quality_mode);
    

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
          quality_mode
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        returning *`
      : `insert into jobs (
          status,
          photos_count,
          price,
          exif_summary,
          client_email,
          project_name,
          client_name,
          user_id
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8)
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
          qualityMode
        ]
      : [
          "created",
          0,
          0,
          exifSummary,
          clientEmail,
          projectName,
          clientName,
          userId
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
      ? "select id, status, quality_mode from jobs where id = $1"
      : "select id, status from jobs where id = $1";

    const { rows } = await pool.query(selectJobSql, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const job = rows[0];
    const qualityMode = normalizeQualityMode(
      req.body?.quality_mode || job.quality_mode
    );

    if (isLockedStatus(job.status)) {
      return res.status(409).json({
        ok: false,
        error: "job_locked",
        message: `Job ya está en estado ${job.status}`,
      });
    }

    if (!processQueue || !redisReady) {
      return res
        .status(503)
        .json({ error: "queue unavailable (redis not ready)" });
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

    const estimatedSeconds = await estimateProcessingSecondsFromInputsHistorical(
      pool,
      photosCount,
      inputTotalBytes,
      qualityMode
    );

    const price = calculatePriceFromInputs(
      photosCount,
      inputTotalBytes,
      estimatedSeconds,
      qualityMode
    );

    // Guardamos conteo real + precio real y ponemos en cola
    const submitUpdateSql = jobsHasQualityMode
      ? `update jobs
           set status='queued',
               photos_count=$2,
               price=$3,
               input_total_bytes=$4,
               quality_mode=$5,
               progress=0,
               message='En cola',
               error=null,
               updated_at=now(),
               started_at = case when started_at is null then now() else started_at end
         where id=$1`
      : `update jobs
           set status='queued',
               photos_count=$2,
               price=$3,
               input_total_bytes=$4,
               progress=0,
               message='En cola',
               error=null,
               updated_at=now(),
               started_at = case when started_at is null then now() else started_at end
         where id=$1`;

    const submitUpdateParams = jobsHasQualityMode
      ? [id, photosCount, price, inputTotalBytes, qualityMode]
      : [id, photosCount, price, inputTotalBytes];

    await pool.query(submitUpdateSql, submitUpdateParams);

    // encolar
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
      "select id, status from jobs where id = $1",
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

// Si entra la primera foto y el job aún está recién creado,
// lo pasamos a "receiving"
if (job.status === "created") {
  await pool.query(
    `update jobs
        set status='receiving',
            message='Recibiendo fotos',
            updated_at=now()
      where id=$1`,
    [id]
  );
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

    res.json({ ok: true, uploaded: files.length, files });
  } catch (e) {
    console.error("upload error", e);
    res.status(500).json({ ok: false, error: "upload error" });
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

    res.json({
      ok: true,
      saved: { filename: req.file.filename, size: req.file.size },
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

    const logPath = path.join(outputDir(id), "metashape-python-log.txt");

    if (!fs.existsSync(logPath)) {
      return res.status(404).json({ ok: false, error: "log not found" });
    }

    const text = fs.readFileSync(logPath, "utf8");

    res.setHeader("Content-Type", "text/plain");
    res.send(text);

  } catch (e) {
    console.error("log error", e);
    res.status(500).json({ ok: false, error: "log error" });
  }
});

app.get("/jobs/:id/log", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id from jobs where id = $1",
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "job not found" });
    }

    const logPath = path.join(outputDir(id), "metashape-python-log.txt");

    if (!fs.existsSync(logPath)) {
      return res.status(404).json({ ok: false, error: "log not found" });
    }

    const text = fs.readFileSync(logPath, "utf8");

    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    return res.send(text);
  } catch (e) {
    console.error("log error", e);
    return res.status(500).json({ ok: false, error: "log error" });
  }
});

app.get("/jobs/:id/download", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      "select id from jobs where id = $1",
      [id]
    );

    if (!rows.length)
      return res.status(404).json({ error: "job not found" });

    const zipPath = path.join(outputDir(id), "outputs.zip");

    if (!fs.existsSync(zipPath)) {
      return res.status(404).json({ error: "outputs.zip not found" });
    }

    const stat = fs.statSync(zipPath);

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Length", stat.size);

    return res.download(zipPath, `xproces-${id}.zip`);

  } catch (e) {
    console.error("download error", e);
    res.status(500).json({ error: "download error" });
  }
});


app.post("/jobs/:id/cancel", async (req, res) => {
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

app.post("/jobs/:id/priority", async (req, res) => {
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
      total_seconds
    } = req.body;

    const p = progress === undefined ? null : Number(progress);
    if (p !== null && (!Number.isFinite(p) || p < 0 || p > 100)) {
      return res.status(400).json({ error: "progress must be 0..100" });
    }

    const currentJobResult = await pool.query(
      `select id, status from jobs where id = $1`,
      [id]
    );

    if (!currentJobResult.rows.length) {
      return res.status(404).json({ error: "not found" });
    }

    const currentStatus = String(currentJobResult.rows[0].status || "").toLowerCase();

    if (currentStatus === "cancelled") {
      return res.json({
        ok: true,
        ignored: true,
        message: "Job cancelado; actualización ignorada"
      });
    }
    
    const { rows } = await pool.query(
      `update jobs
         set status = coalesce($2, status),
             stage = coalesce($3, stage),
             progress = coalesce($4, progress),
             message = coalesce($5, message),
             error = coalesce($6, error),
             download_seconds = coalesce($7, download_seconds),
             processing_seconds = coalesce($8, processing_seconds),
             total_seconds = coalesce($9, total_seconds),
             updated_at = now(),
             started_at = case when $2 = 'running' and started_at is null then now() else started_at end,
             finished_at = case when $2 in ('done','failed') then now() else finished_at end
       where id = $1
       returning *`,
      [
        id,
        status ?? null,
        stage ?? null,
        p,
        message ?? null,
        error ?? null,
        download_seconds ?? null,
        processing_seconds ?? null,
        total_seconds ?? null
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
              progress=0,
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

    res.json({ ok: true, job: rows[0] });
  } catch (e) {
    console.error("worker claim error", e);
    res.status(500).json({ ok: false, error: "worker claim error" });
  }
});
app.get("/worker/receiving", requireWorkerAuth, async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `select id, status, photos_count, price, created_at, updated_at, message
       ${jobsHasQualityMode ? ", quality_mode" : ""}
       from jobs
       where status = 'receiving'
       order by created_at asc
       limit 10`
    );

    res.json({ ok: true, jobs: rows });
  } catch (e) {
    console.error("worker receiving error", e);
    res.status(500).json({ ok: false, error: "worker receiving error" });
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
    if (fs.existsSync(dir)) {
      fs.rmSync(dir, { recursive: true, force: true });
    }

    await pool.query(
     `update jobs
         set input_purged = true,
             input_purged_at = now(),
             updated_at = now()
       where id = $1`,
     [id]
   );

    res.json({ ok: true, purged: true });
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

async function estimateProcessingSecondsFromInputsHistorical(pool, photosCount, totalBytes, qualityMode = "normal") {
  const photos = Number(photosCount || 0);
  const bytes = Number(totalBytes || 0);
  const normalizedQualityMode = normalizeQualityMode(qualityMode);

  const minPhotos = Math.max(0, photos - 200);
  const maxPhotos = photos + 200;

  const minBytes = Math.max(0, Math.round(bytes * 0.5));
  const maxBytes = Math.round(bytes * 1.5);

  const historicalSql = jobsHasQualityMode
    ? `select processing_seconds
       from jobs
       where status = 'done'
         and processing_seconds is not null
         and quality_mode = $5
         and photos_count between $1 and $2
         and input_total_bytes between $3 and $4
       order by created_at desc
       limit 20`
    : `select processing_seconds
       from jobs
       where status = 'done'
         and processing_seconds is not null
         and photos_count between $1 and $2
         and input_total_bytes between $3 and $4
       order by created_at desc
       limit 20`;

  const historicalParams = jobsHasQualityMode
    ? [minPhotos, maxPhotos, minBytes, maxBytes, normalizedQualityMode]
    : [minPhotos, maxPhotos, minBytes, maxBytes];

  const { rows } = await pool.query(historicalSql, historicalParams);

  if (rows.length >= 3) {
    const avg =
      rows.reduce((acc, r) => acc + Number(r.processing_seconds || 0), 0) / rows.length;
    return Math.round(avg);
  }

  return estimateProcessingSecondsFromInputs(photos, bytes, normalizedQualityMode);
}

async function estimateProcessingSeconds(pool, job) {
  if (!job) return 0;

  const photos = Number(job.photos_count || 0);
  const bytes = Number(job.input_total_bytes || 0);
  const qualityMode = normalizeQualityMode(job?.quality_mode);

  if (photos > 0 || bytes > 0) {
    return await estimateProcessingSecondsFromInputsHistorical(
      pool,
      photos,
      bytes,
      qualityMode
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
app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});



