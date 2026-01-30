import express from "express";
import pkg from "pg";
import { Queue } from "bullmq";
import IORedis from "ioredis";

const { Pool } = pkg;

// =====================
// APP
// =====================
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
// REDIS + BULLMQ
// =====================
const redis = new IORedis(process.env.REDIS_URL);
redis.on("connect", () => console.log("Redis conectado"));
redis.on("error", (e) => console.error("Redis error", e));

const processQueue = new Queue("processQueue", { connection: redis });

// =====================
// ENDPOINTS BASICOS
// =====================
app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/version", (req, res) => {
  res.json({ version: "v4-redis-queue" });
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

    const { rows } = await pool.query(
      `insert into jobs (status, photos_count, price)
       values ($1, $2, $3)
       returning *`,
      ["queued", n, price]
    );

    await processQueue.add(
      "process_job",
      { jobId: rows[0].id },
      { attempts: 3, backoff: { type: "exponential", delay: 5000 } }
    );

    res.json(rows[0]);
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
// LISTEN (SOLO UNA VEZ)
// =====================
const port = 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});

app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});
