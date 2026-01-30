import express from "express";
import pkg from "pg";
import { Queue } from "bullmq";
import IORedis from "ioredis";

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
// REDIS + BULLMQ (ROBUSTO)
// =====================
let redis = null;
let processQueue = null;
let redisReady = false;

if (process.env.REDIS_URL && process.env.REDIS_URL.trim().length > 0) {
  redis = new IORedis(process.env.REDIS_URL, {
    maxRetriesPerRequest: null,
    enableReadyCheck: true
  });

  redis.on("connect", () => console.log("Redis: connect"));
  redis.on("ready", () => {
    redisReady = true;
    console.log("Redis conectado (ready)");
  });
  redis.on("close", () => {
    redisReady = false;
    console.log("Redis: close");
  });
  redis.on("error", (e) => {
    redisReady = false;
    console.error("Redis error", e?.message || e);
  });

  processQueue = new Queue("processQueue", { connection: redis });
} else {
  console.log("REDIS_URL no definido: cola desactivada");
}

// =====================
// ENDPOINTS BASICOS
// =====================
app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/version", (req, res) => {
  res.json({ version: "v5-redis-robust" });
});

// (opcional) estado de redis para debug
app.get("/redis", (req, res) => {
  res.json({
    configured: !!process.env.REDIS_URL,
    ready: redisReady
  });
});

// =====================
// JOBS
// =====================
app.get("/jobs", async (req, res) => {
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
    const { rows } = await pool.query(
      `select * from jobs where id = $1`,
      [id]
    );
    if (!rows.length) return res.status(404).json({ error: "not found" });
    res.json(rows[0]);
  } catch (e) {
    console.error("get job error", e);
    res.status(500).json({ error: "db error" });
  }
});

app.post("/jobs", async (req, res) => {
  try {
    const { photos_count } = req.body;

    const n = Number(photos_count);
    if (!Number.isFinite(n) || n <= 0) {
      return res.status(400).json({ error: "photos_count must be > 0" });
    }

    const price = n * 0.07;

    // 1) crear job en Postgres
    const { rows } = await pool.query(
      `insert into jobs (status, photos_count, price)
       values ($1, $2, $3)
       returning *`,
      ["queued", n, price]
    );

    const jobRow = rows[0];

    // 2) encolar en Redis (si está listo)
    if (!processQueue || !redisReady) {
      // no tiramos el proceso, devolvemos error claro
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


