import { Worker } from "bullmq";
import IORedis from "ioredis";
import fs from "fs";
import path from "path";
import archiver from "archiver";

const REDIS_URL = process.env.REDIS_URL;
const API_BASE = process.env.API_BASE;          // ej: http://api:3000 o http://mapxion-api:3000
const DATA_ROOT = process.env.DATA_ROOT || "/data/mapxion";

if (!REDIS_URL) { console.error("Falta REDIS_URL"); process.exit(1); }
if (!API_BASE) { console.error("Falta API_BASE"); process.exit(1); }

const connection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null });

function jobDir(jobId) { return path.join(DATA_ROOT, "jobs", jobId); }
function inputDir(jobId) { return path.join(jobDir(jobId), "input"); }
function outputDir(jobId) { return path.join(jobDir(jobId), "output"); }

function ensureDirs(jobId) {
  fs.mkdirSync(inputDir(jobId), { recursive: true });
  fs.mkdirSync(outputDir(jobId), { recursive: true });
}

async function patchJob(jobId, data) {
  const r = await fetch(`${API_BASE}/jobs/${jobId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`PATCH ${r.status}: ${t}`);
  }
  return r.json();
}

async function zipFolderToFile(folderPath, zipPath) {
  await new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    output.on("close", resolve);
    output.on("error", reject);
    archive.on("error", reject);

    archive.pipe(output);
    archive.directory(folderPath, false);
    archive.finalize();
  });
}

console.log("Worker conectando a Redis...");
connection.on("ready", () => console.log("Redis ready (worker)"));
connection.on("error", (e) => console.error("Redis error (worker)", e?.message || e));

const worker = new Worker(
  "processQueue",
  async (job) => {
    const { jobId } = job.data || {};
    if (!jobId) throw new Error("jobId missing in job.data");

    console.log("Procesando job:", jobId);

    ensureDirs(jobId);

    await patchJob(jobId, { status: "running", progress: 5, message: "Iniciando proceso" });

    const inDir = inputDir(jobId);
    const outDir = outputDir(jobId);

    const inputs = fs.existsSync(inDir) ? fs.readdirSync(inDir) : [];
    if (!inputs.length) {
      await patchJob(jobId, { status: "failed", progress: 0, message: "No hay fotos en input", error: "no_input_files" });
      return { ok: false, reason: "no_input_files" };
    }

    await patchJob(jobId, { progress: 20, message: `Encontradas ${inputs.length} fotos. Preparando...` });

    // ✅ output real de ejemplo (luego lo cambias por Pix4D/Metashape/etc.)
    const txt = [
      `JOB: ${jobId}`,
      `INPUTS: ${inputs.length}`,
      `FILES:`,
      ...inputs.map(f => ` - ${f}`),
      `GENERATED_AT: ${new Date().toISOString()}`
    ].join("\n");

    fs.writeFileSync(path.join(outDir, "result.txt"), txt);
    fs.writeFileSync(path.join(outDir, "summary.json"), JSON.stringify({
      jobId,
      inputs,
      createdAt: new Date().toISOString(),
    }, null, 2));

    await patchJob(jobId, { progress: 70, message: "Generando outputs.zip" });

    const zipPath = path.join(outDir, "outputs.zip");
    await zipFolderToFile(outDir, zipPath);

    await patchJob(jobId, { status: "done", progress: 100, message: "Completado" });
    console.log("Job completado:", jobId);

    return { ok: true };
  },
  { connection }
);

worker.on("completed", (job) => console.log("Worker completed:", job?.id));
worker.on("failed", (job, err) => console.error("Worker failed:", job?.id, err?.message || err));

