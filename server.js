import pkg from "pg";
const { Pool } = pkg;

import express from "express";

const app = express();
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// prueba conexión
pool.query("select 1")
  .then(() => console.log("Postgres conectado"))
  .catch(err => console.error("Error Postgres", err));

app.get("/version", (req, res) => {
  res.json({ version: "v2-jobs-get" });

});



app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

// ✅ LISTAR JOBS
app.get("/jobs", async (req, res) => {
  const { rows } = await pool.query(
    `select * from jobs order by created_at desc limit 50`
  );
  res.json(rows); // <-- array
});


// ✅ DETALLE JOB
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

// ✅ CREAR JOB
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
      ["created", n, price]
    );

    res.json(rows[0]);
  } catch (e) {
    console.error("create job error", e);
    res.status(500).json({ error: "db error" });
  }
});

const port = 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});
