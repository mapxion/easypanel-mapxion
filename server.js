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
  connectionString: process.env.DATABASE_URL,
});

pool
  .query("select 1")
  .then(() => console.log("Postgres conectado"))
  .catch((err) => console.error("Error Postgres", err));

// =====================
// REDIS (robusto) + QUEUE
// =====================
let redis = null;
let redisReady = false;
let processQueue = null;

if (process.env.REDIS_URL) {
  // ✅ importante para BullMQ
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
function inputDir(jobId) {
  return path.join(jobDir(jobId), "input");
}
function outputDir(jobId) {
  return path.join(jobDir(jobId), "output");
}
function ensureJobDirs(jobId) {
  fs.mkdirSync(inputDir(jobId), { recursive: true });
  fs.mkdirSync(outputDir(jobId), { recursive: true });
}

// =====================
// MULTER (INPUT UPLOAD)
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
  limits: { fileSize: 250 * 1024 * 1024 }, // 250MB por foto
});

// =====================
// MULTER (OUTPUT UPLOAD)  (zip subido por worker windows)
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
// ENDPOINTS BASICOS
// =====================
app.get("/", (_req, res) => res.send("mapxion api ok"));
app.get("/health", (_req, res) => res.json({ ok: true }));

// ✅ cambia si quieres
app.get("/version", (_req, res) =>
  res.json({ version: "v8-option-a-create-upload-submit" })
);

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

// ✅ OPCION A: CREAR JOB (NO ENCOLA AQUI)
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
      ["created", n, price]
    );

    const jobRow = rows[0];

    // crear carpetas en drive
    ensureJobDirs(jobRow.id);

    res.json(jobRow);
  } catch (e) {
    console.error("create job error", e);
    res.status(500).json({ error: "db error" });
  }
});

// ✅ OPCION A: SUBMIT (ENCOLAR CUANDO YA HAY FOTOS)
app.post("/jobs/:id/submit", async (req, res) => {
  try {
    const { id } = req.params;

    // existe job?
    const { rows } = await pool.query(`select * from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    // redis listo?
    if (!processQueue || !redisReady) {
      return res.status(503).json({ error: "queue unavailable (redis not ready)" });
    }

    // hay inputs?
    const inDir = inputDir(id);
    const files = fs.existsSync(inDir) ? fs.readdirSync(inDir) : [];
    if (!files.length) {
      return res.status(400).json({ error: "no inputs uploaded yet" });
    }

    await processQueue.add(
      "process_job",
      { jobId: id },
      { attempts: 3, backoff: { type: "exponential", delay: 5000 } }
    );

    await pool.query(
      `update jobs
         set status='queued',
             progress=0,
             message=null,
             error=null,
             updated_at=now()
       where id=$1`,
      [id]
    );

    res.json({ ok: true, enqueued: true, jobId: id, inputs: files.length });
  } catch (e) {
    console.error("submit error", e);
    res.status(500).json({ error: "submit error" });
  }
});

// =====================
// INPUTS (FOTOS)
// =====================
app.post("/jobs/:id/upload", uploadInput.array("photos", 5000), async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(`select id from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    ensureJobDirs(id);

    const files = (req.files || []).map((f) => ({
      filename: f.filename,
      size: f.size,
    }));

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

// INPUT.ZIP
app.get("/jobs/:id/input.zip", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(`select id from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const dir = inputDir(id);
    if (!fs.existsSync(dir)) return res.status(404).json({ error: "no input folder yet" });

    const files = fs.readdirSync(dir);
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
    archive.directory(dir, false);
    await archive.finalize();
  } catch (e) {
    console.error("input.zip error", e);
    res.status(500).json({ error: "input.zip error" });
  }
});

// =====================
// OUTPUTS (subir + listar + descargar)
// =====================
app.post("/jobs/:id/output", uploadOutput.single("file"), async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(`select id from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    ensureJobDirs(id);

    if (!req.file) return res.status(400).json({ error: "missing file field (file)" });

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

// DOWNLOAD = ZIP de la carpeta output
app.get("/jobs/:id/download", async (req, res) => {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(`select id from jobs where id = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: "job not found" });

    const dir = outputDir(id);
    if (!fs.existsSync(dir)) return res.status(404).json({ error: "no output folder yet" });

    const files = fs.readdirSync(dir);
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
    archive.directory(dir, false);
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

});









