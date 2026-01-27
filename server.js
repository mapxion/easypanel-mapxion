import pkg from "pg";
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});
pool.query("select 1")
  .then(() => console.log("Postgres conectado"))
  .catch(err => console.error("Error Postgres", err));

import express from "express";

const app = express();
app.use(express.json());

// ✅ INIT DB (temporal)
app.post("/admin/init-db", async (req, res) => {
  try {
    await pool.query(`create extension if not exists "pgcrypto"`);
    await pool.query(`
      create table if not exists jobs (
        id uuid primary key default gen_random_uuid(),
        status text not null,
        photos_count int not null,
        price numeric(10,2) not null,
        created_at timestamp default now()
      )
    `);
    res.json({ ok: true });
  } catch (e) {
    console.error("init-db error", e);
    res.status(500).json({ ok: false, error: String(e) });
  }
});


app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

const port = 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
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

