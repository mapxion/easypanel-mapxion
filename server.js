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
{
  "name": "mapxion-api",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "start": "node server.js"
  },
  "dependencies": {
    "express": "^4.19.2",
    "pg": "^8.11.5"
  }
}
