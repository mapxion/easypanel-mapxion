import { Worker } from "bullmq";
import IORedis from "ioredis";

const REDIS_URL = process.env.REDIS_URL;
const API_BASE = process.env.API_BASE;

if (!REDIS_URL) {
  console.error("Falta REDIS_URL");
  process.exit(1);
}
if (!API_BASE) {
  console.error("Falta API_BASE");
  process.exit(1);
}

const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null
});

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

console.log("Worker conectando a Redis...");
connection.on("ready", () => console.log("Redis ready (worker)"));
connection.on("error", (e) => console.error("Redis error (worker)", e?.message || e));

const worker = new Worker(
  "processQueue",
  async (job) => {
    const { jobId } = job.data;
    console.log("Procesando job:", jobId);

    await patchJob(jobId, { status: "running", progress: 5, message: "Iniciando proceso" });
    await new Promise(r => setTimeout(r, 1500));

    await patchJob(jobId, { progress: 20, message: "Importando fotos" });
    await new Promise(r => setTimeout(r, 1500));

    await patchJob(jobId, { progress: 45, message: "Alineando cámaras" });
    await new Promise(r => setTimeout(r, 1500));

    await patchJob(jobId, { progress: 70, message: "Generando nube / malla" });
    await new Promise(r => setTimeout(r, 1500));

    await patchJob(jobId, { progress: 90, message: "Exportando outputs" });
    await new Promise(r => setTimeout(r, 1500));

    await patchJob(jobId, { status: "done", progress: 100, message: "Completado" });
    console.log("Job completado:", jobId);

    return { ok: true };
  },
  { connection }
);

worker.on("completed", (job) => console.log("Worker completed:", job.id));
worker.on("failed", (job, err) => console.error("Worker failed:", job?.id, err?.message || err));
