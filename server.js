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

app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

// 🔒 puerto interno fijo
const port = 3000;

app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});
