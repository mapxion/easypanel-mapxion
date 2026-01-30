import express from "express";
import pkg from "pg";
const { Pool } = pkg;

const app = express();
app.use(express.json());

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

pool.query("select 1")
  .then(() => console.log("Postgres conectado"))
  .catch(err => console.error("Error Postgres", err));

app.get("/health", (req, res) => res.json({ ok: true }));
app.get("/version", (req, res) => res.json({ version: "v3-stable-no-redis" }));

app.get("/jobs", async (req, res) => {
  const { rows } = await pool.query(`select * from jobs order by created_at desc limit 50`);
  res.json(rows);
});

app.get("/jobs/:id", async (req, res) => {
  const { id } = req.params;
  const { rows } = await pool.query(`select * from jobs where id = $1`, [id]);
  if (!rows.length) return res.status(404).json({ error: "not found" });
  res.json(rows[0]);
});

app.post("/jobs", async (req, res) => {
  const n = Number(req.body.photos_count);
  if (!Number.isFinite(n) || n <= 0) return res.status(400).json({ error: "photos_count must be > 0" });

  const price = n * 0.07;
  const { rows } = await pool.query(
    `insert into jobs (status, photos_count, price) values ($1, $2, $3) returning *`,
    ["queued", n, price]
  );
  res.json(rows[0]);
});

app.patch("/jobs/:id", async (req, res) => {
  const { id } = req.params;
  const { status, progress, message, error } = req.body;

  const p = progress === undefined ? null : Number(progress);
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
});

const port = Number(process.env.PORT) || 3000;
app.listen(port, "0.0.0.0", () => console.log(`mapxion api listening on ${port}`));





