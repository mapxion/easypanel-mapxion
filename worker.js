import { Worker } from "bullmq";
import IORedis from "ioredis";
import fs from "fs";
import path from "path";

// =====================
// ENV
// =====================
const REDIS_URL = process.env.REDIS_URL;
const API_BASE = process.env.API_BASE;
const DATA_ROOT = process.env.DATA_ROOT || "/data/mapxion";

if (!REDIS_URL) {
  console.error("Falta REDIS_URL");
  process.exit(1);
}
if (!API_BASE) {
  console.error("Falta API_BASE");
  process.exit(1);
}

// =====================
// REDIS CONNECTION (BullMQ requirement)
// =====================
const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null
});

console.log("Worker conectando a Redis...");
connection.on("ready", () => console.log("Redis ready (worker)"));
connection.on("error", (e) => console.error("Redis error (worker)", e?.message || e));

// =====================
// Helpers
// =====================
function jobOutputDir(jobId) {
  return path.join(DATA_ROOT, "jobs", jobId, "output");
}
function jobInputDir(jobId) {
  return path.join(DATA_ROOT, "jobs", jobId, "input");
}
function ensureDirs(jobId) {
  fs.mkdirSync(jobOutputDir(jobId), { recursive: true });
  fs.mkdirSync(jobInputDir(jobId), { recursive: true });
}

async function patchJob(jobId, data) {
  const r = await fetch(`${API_BASE}/jobs/${jobId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data)
  });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`PATCH ${r.status}: ${t}`);
  }
  return r.json();
}

function writeDummyOutputs(jobId) {
  ensureDirs(jobId);
  const outDir = jobOutputDir(jobId);

  // Conteo de inputs (si existen)
  let inputCount = 0;
  try {
    const inDir = jobInputDir(jobId);
    if (fs.existsSync(inDir)) {
      inputCount = fs.readdirSync(inDir).length;
    }
  } catch (_) {}

  const report = [
    `Mapxion dummy report`,
    `jobId: ${jobId}`,
    `date: ${new Date().toISOString()}`,
    `inputs_found: ${inputCount}`,
    `note: This is a placeholder output (no photogrammetry yet).`
  ].join("\n");

  fs.writeFileSync(path.join(outDir, "report.txt"), report, "utf-8");
  fs.writeFileSync(
    path.join(outDir, "outputs.json"),
    JSON.stringify(
      {
        jobId,
        ok: true,
        generatedAt: new Date().toISOString(),
        inputsFound: inputCount,
        outputs: ["report.txt", "outputs.json"]
      },
      null,
      2
    ),
    "utf-8"
  );
}

// =====================
// WORKER
// =====================
const worker = new Worker(
  "processQueue",
  async (job) => {
    const { jobId } = job.data;
    console.log("Procesando job:", jobId);

    try {
      // Asegurar dirs en drive
      ensureDirs(jobId);

      await patchJob(jobId, { status: "running", progress: 5, message: "Iniciando proceso" });
      await new Promise(r => setTimeout(r, 1200));

      await patchJob(jobId, { progress: 20, message: "Importando fotos" });
      await new Promise(r => setTimeout(r, 1200));

      await patchJob(jobId, { progress: 45, message: "Alineando cámaras" });
      await new Promise(r => setTimeout(r, 1200));

      await patchJob(jobId, { progress: 70, message: "Generando nube / malla" });
      await new Promise(r => setTimeout(r, 1200));

      await patchJob(jobId, { progress: 90, message: "Exportando outputs" });
      await new Promise(r => setTimeout(r, 800));

      // ✅ Genera outputs dummy en /data/.../output
      writeDummyOutputs(jobId);

const DATA_ROOT = process.env.DATA_ROOT || "/data/mapxion";
const outDir = path.join(DATA_ROOT, "jobs", jobId, "output");
const inDir  = path.join(DATA_ROOT, "jobs", jobId, "input");

fs.mkdirSync(outDir, { recursive: true });

const inputs = fs.existsSync(inDir) ? fs.readdirSync(inDir) : [];
fs.writeFileSync(
  path.join(outDir, "report.txt"),
  `jobId=${jobId}\ninputs=${inputs.length}\nfiles=${inputs.join(",")}\ncreated=${new Date().toISOString()}\n`,
  "utf-8"
);

fs.writeFileSync(
  path.join(outDir, "outputs.json"),
  JSON.stringify({ jobId, inputs, createdAt: new Date().toISOString() }, null, 2),
  "utf-8"
);

      await patchJob(jobId, { status: "done", progress: 100, message: "Completado" });
      console.log("Job completado:", jobId);

      return { ok: true };
    } catch (err) {
      console.error("Error procesando job:", jobId, err?.message || err);

      // Intentar escribir error en output
      try {
        ensureDirs(jobId);
        fs.writeFileSync(
          path.join(jobOutputDir(jobId), "error.txt"),
          String(err?.stack || err),
          "utf-8"
        );
      } catch (_) {}

      // Marcar failed en API
      try {
        await patchJob(jobId, { status: "failed", progress: 100, message: "Error", error: String(err?.message || err) });
      } catch (_) {}

      throw err;
    }
  },
  { connection }
);

worker.on("completed", (job) => console.log("Worker completed:", job.id));
worker.on("failed", (job, err) => console.error("Worker failed:", job?.id, err?.message || err));

worker.on("completed", (job) => console.log("Worker completed:", job.id));
worker.on("failed", (job, err) => console.error("Worker failed:", job?.id, err?.message || err));
