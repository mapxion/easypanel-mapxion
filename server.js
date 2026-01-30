// 1️⃣ imports
import pkg from "pg";
const { Pool } = pkg;
import express from "express";

// 2️⃣ app + db
const app = express();
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// 3️⃣ endpoints básicos
app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

// 4️⃣ jobs
app.get("/jobs", async (req, res) => {
  const { rows } = await pool.query(
    `select * from jobs order by created_at desc limit 50`
  );
  res.json(rows);
});

app.get("/jobs/:id", async (req, res) => {
  const { id } = req.params;
  const { rows } = await pool.query(
    `select * from jobs where id = $1`,
    [id]
  );
  if (!rows.length) return res.status(404).json({ error: "not found" });
  res.json(rows[0]);
});

app.post("/jobs", async (req, res) => {
  const { photos_count } = req.body;
  const price = photos_count * 0.07;

  const { rows } = await pool.query(
    `insert into jobs (status, photos_count, price)
     values ($1, $2, $3)
     returning *`,
    ["created", photos_count, price]
  );

  res.json(rows[0]);
});

// ✅ 5️⃣ TRACKING (ESTE ES EL NUEVO)
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

// 6️⃣ listen
app.listen(3000, "0.0.0.0", () => {
  console.log("mapxion api listening on 3000");
});



