import express from "express";

const app = express();
app.use(express.json());

app.get("/", (req, res) => res.send("mapxion api ok"));
app.get("/health", (req, res) => res.json({ ok: true }));

// 🔒 Forzamos 3000 SIEMPRE, ignorando PORT que te mete EasyPanel
const port = 3000;

console.log("DEBUG env PORT =", process.env.PORT); // para ver qué mete EasyPanel
console.log("DEBUG forced port =", port);

app.listen(port, "0.0.0.0", () => {
  console.log(`mapxion api listening on ${port}`);
});
