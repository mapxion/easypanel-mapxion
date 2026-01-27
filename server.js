import express from "express";

const app = express();
app.use(express.json());

app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

const port = 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});
