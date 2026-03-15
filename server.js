import express from "express";
import multer from "multer";
import fs from "fs";
import path from "path";
import IORedis from "ioredis";
import { Queue } from "bullmq";
import pkg from "pg";
import archiver from "archiver";
import cors from "cors";


const { Pool } = pkg;

const app = express();
app.use(express.json());
app.use(cors());
// =====================
// CONFIG
// =====================
const PRICE_PER_PHOTO = Number(process.env.PRICE_PER_PHOTO ?? 0.07); // €/foto
const DATA_ROOT = process.env.DATA_ROOT || "/data/mapxion";
const WORKER_TOKEN = process.env.WORKER_TOKEN || "";

// =====================
// AUTH WORKER
// =====================
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
  .then(() => console.log("Postgres conectado"))
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
app.get("/health", (_req, res) => res.json({ ok: true }));
app.get("/version", (_req, res) =>
  res.json({ version: "v21-worker-receiving-list" })
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
app.post("/jobs", async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `insert into jobs (status, photos_count, price)
       values ($1, $2, $3)
       returning *`,
      ["created", 0, 0]
    );

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
    const price = Number((photosCount * PRICE_PER_PHOTO).toFixed(2));

    // Guardamos conteo real + precio real y ponemos en cola
    await pool.query(
      `update jobs
         set status='queued',
             photos_count=$2,
             price=$3,
             progress=0,
             message='En cola',
             error=null,
             updated_at=now(),
             started_at = case when started_at is null then now() else started_at end
       where id=$1`,
      [id, photosCount, price]
    );

    // encolar
    await processQueue.add(
      "process_job",
      { jobId: id },
      { attempts: 3, backoff: { type: "exponential", delay: 5000 } }
    );

    res.json({
      ok: true,
      enqueued: true,
      jobId: id,
      inputs: photosCount,
      price,
      price_per_photo: PRICE_PER_PHOTO,
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

    return res.download(zipPath, `mapxion-${id}.zip`);

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
          order by created_at asc
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
// =====================
// LISTEN
// =====================
const port = Number(process.env.PORT) || 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});



