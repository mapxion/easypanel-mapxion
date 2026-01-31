import multer from "multer";
import fs from "fs";
import path from "path";
import IORedis from "ioredis";
import { Queue } from "bullmq";
import express from "express";
import pkg from "pg";
import archiver from "archiver";

const { Pool } = pkg;

const app = express();
app.use(express.json());

// =====================
// POSTGRES
// =====================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

pool.query("select 1")
  .then(() => console.log("Postgres conectado"))
  .catch(err => console.error("Error Postgres", err));

// =====================
// REDIS (robusto) + QUEUE
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
// LOCAL DRIVE (/data)
// =====================
const DATA_ROOT = process.env.DATA_ROOT || "/data/mapxion";

function jobDir(jobId) {
  return path.join(DATA_ROOT, "jobs", jobId);
}

function ensureJobDirs(jobId) {
  fs.mkdirSync(path.join(jobDir(jobId), "input"), { recursive: true });
  fs.mkdirSync(path.join(jobDir(jobId), "output"), { recursive: true });
}

// =====================
// MULTER (INPUT UPLOAD)
// =====================
const inputStorage = multer.diskStorage({
  destination: (req, file, cb) => {
    const { id } = req.params;
    ensureJobDirs(id);
    cb(null, path.join(jobDir(id), "input"));
  },
  filename: (req, file, cb) => {
    const safe = file.originalname.replace(/[^\w.\-() ]+/g, "_");
    cb(null, safe);
  }
});

const uploadInput = multer({
  storage: inputStorage,
  limits: { fileSize: 250 * 1024 * 1024 } // 250MB por foto
});

// Para outputs (zip) — 5GB por ejemplo (ajusta)
const outputStorage = multer.diskStorage({
  destination: (req, file, cb) => {
    const { id } = req.params;
    ensureJobDirs(id);
    cb(null, path.join(jobDir(id), "output"));
  },
  filename: (_req, file, cb) => {
    // guardamos como outputs.zip o el nombre que venga
    const safe = file.originalname.replace(/[^\w.\-() ]+/g, "_");
    cb(null, safe);
  }
});

const uploadOutput = multer({
  storage: outputStorage,
  limits: { fileSize: 5 * 1024 * 1024 * 1024 } // 5GB
});

// =====================
// ENDPOINTS BASICOS
// =====================
app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/version", (_req, res) => res.json({ version: "v6-api-input-output-zip" }));

app.get("/redis", (_req, res) => {
  res.json({ configured: !!process.env.REDIS_URL, ready: redisReady });
});

app.get("/queue", async (_req, res) => {
  if (!processQueue || !redisReady) {
    return res.status(503).json({ ok: false, error: "queue unavailable" });
  }
  const counts = await processQueue.getJobCounts();
  res.json({ ok: true, counts });
});

// =====================
// JOBS
// =====================
app.get("/jobs", async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `select * from jobs order by created_at desc limit 50`
    );
    res.json(rows);
  } catch (e) {
    console.error("list jobs error", e);
    res.status(500).json({ error: "db error" });
  }
});

app.get("/jobs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { rows } = await pool.query(`select * from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "not found" });
    res.json(rows[0]);
  } catch (e) {
    console.error("get job error", e);
    res.status(500).json({ error: "db error" });
  }
});

// CREAR JOB + ENCOLAR
app.post("/jobs", async (req, res) => {
  try {
    const n = Number(req.body.photos_count);
    if (!Number.isFinite(n) || n <= 0) {
      return res.status(400).json({ error: "photos_count must be > 0" });
    }

    const price = n * 0.07;

    const { rows } = await pool.query(
      `insert into jobs (status, photos_count, price)
       values ($1, $2, $3)
       returning *`,
      ["queued", n, price]
    );

    const jobRow = rows[0];

    // ✅ BUG FIX: aquí NO existe "id". Hay que usar jobRow.id
    ensureJobDirs(jobRow.id);

    if (!processQueue || !redisReady) {
      return res.status(503).json({
        error: "queue unavailable (redis not ready)",
        job: jobRow
      });
    }

    await processQueue.add(
      "process_job",
      { jobId: jobRow.id },
      { attempts: 3, backoff: { type: "exponential", delay: 5000 } }
    );

    res.json(jobRow);
  } catch (e) {
    console.error("create job error", e);
    res.status(500).json({ error: "db error" });
  }
});

// =====================
// INPUTS (FOTOS)
// =====================

// SUBIR FOTOS (multipart)
app.post("/jobs/:id/upload", uploadInput.array("photos", 5000), async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(`select id from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    ensureJobDirs(id);

    const files = (req.files || []).map(f => ({ filename: f.filename, size: f.size }));
    res.json({ ok: true, uploaded: files.length, files });
  } catch (e) {
    console.error("upload error", e);
    res.status(500).json({ ok: false, error: "upload error" });
  }
});

// LISTAR INPUTS
app.get("/jobs/:id/files", async (req, res) => {
  try {
    const { id } = req.params;
    const inputDir = path.join(jobDir(id), "input");
    if (!fs.existsSync(inputDir)) return res.json({ ok: true, files: [] });

    const files = fs.readdirSync(inputDir).map(name => {
      const stat = fs.statSync(path.join(inputDir, name));
      return { filename: name, size: stat.size };
    });

    res.json({ ok: true, files });
  } catch (e) {
    console.error("files error", e);
    res.status(500).json({ ok: false, error: "files error" });
  }
});

// DESCARGAR INPUTS COMO ZIP (para worker Windows)
app.get("/jobs/:id/input.zip", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(`select id from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const inputDir = path.join(jobDir(id), "input");
    if (!fs.existsSync(inputDir)) return res.status(404).json({ error: "no input folder yet" });

    const files = fs.readdirSync(inputDir);
    if (!files.length) return res.status(404).json({ error: "no inputs yet" });

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename="mapxion-${id}-input.zip"`);

    const archive = archiver("zip", { zlib: { level: 9 } });
    archive.on("error", (err) => {
      console.error("input.zip error", err);
      try { res.status(500).end(); } catch (_) {}
    });

    req.on("close", () => {
      if (!res.writableEnded) archive.abort();
    });

    archive.pipe(res);
    archive.directory(inputDir, false);
    await archive.finalize();
  } catch (e) {
    console.error("input.zip error", e);
    res.status(500).json({ error: "input.zip error" });
  }
});

// =====================
// OUTPUTS (SUBIR + LISTAR + DESCARGAR)
// =====================

// SUBIR OUTPUTS (worker Windows sube un zip aquí)
app.post("/jobs/:id/output", uploadOutput.single("file"), async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(`select id from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    ensureJobDirs(id);

    if (!req.file) return res.status(400).json({ error: "missing file field (file)" });

    // Opcional: marcar progreso
    await pool.query(
      `update jobs set message = coalesce(message,'') where id = $1`,
      [id]
    );

    res.json({
      ok: true,
      saved: { filename: req.file.filename, size: req.file.size }
    });
  } catch (e) {
    console.error("upload output error", e);
    res.status(500).json({ ok: false, error: "upload output error" });
  }
});

// LISTAR OUTPUTS
app.get("/jobs/:id/outputs", async (req, res) => {
  try {
    const { id } = req.params;
    const outDir = path.join(jobDir(id), "output");
    if (!fs.existsSync(outDir)) return res.json({ ok: true, files: [] });

    const files = fs.readdirSync(outDir).map(name => {
      const stat = fs.statSync(path.join(outDir, name));
      return { filename: name, size: stat.size };
    });

    res.json({ ok: true, files });
  } catch (e) {
    console.error("outputs error", e);
    res.status(500).json({ ok: false, error: "outputs error" });
  }
});

// DESCARGAR OUTPUTS COMO ZIP (lo que el cliente bajará)
app.get("/jobs/:id/download", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(`select id from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const outDir = path.join(jobDir(id), "output");
    if (!fs.existsSync(outDir)) return res.status(404).json({ error: "no output folder yet" });

    const files = fs.readdirSync(outDir);
    if (!files.length) return res.status(404).json({ error: "no outputs yet" });

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename="mapxion-${id}.zip"`);

    const archive = archiver("zip", { zlib: { level: 9 } });
    archive.on("error", (err) => {
      console.error("zip error", err);
      try { res.status(500).end(); } catch (_) {}
    });

    req.on("close", () => {
      if (!res.writableEnded) archive.abort();
    });

    archive.pipe(res);
    archive.directory(outDir, false);
    await archive.finalize();
  } catch (e) {
    console.error("download error", e);
    res.status(500).json({ error: "download error" });
  }
});

// =====================
// PATCH TRACKING
// =====================
app.patch("/jobs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { status, progress, message, error } = req.body;

    const p = progress === undefined ? null : Number(progress);
    if (p !== null && (!Number.isFinite(p) || p < 0 || p > 100)) {
      return res.status(400).json({ error: "progress must be 0..100" });
    }

    const { rows } = await pool.query(
      `update jobs
         set status = coalesce($2, status),
             progress = coalesce($3, progress),
             message = coalesce($4, message),
             error = coalesce($5, error),
             updated_at = now(),
             started_at = case when $2 = 'running' and started_at is null then now() else started_at end,
             finished_at = case when $2 in ('done','failed') then now() else finished_at end
       where id = $1
       returning *`,
      [id, status ?? null, p, message ?? null, error ?? null]
    );

    if (!rows.length) return res.status(404).json({ error: "not found" });
    res.json(rows[0]);
  } catch (e) {
    console.error("patch job error", e);
    res.status(500).json({ error: "db error" });
  }
});

// =====================
// LISTEN
// =====================
const port = Number(process.env.PORT) || 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});







