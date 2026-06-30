import fs from "fs";
import path from "path";
import multer from "multer";

function safeJobId(value) {
  const id = String(value || "").trim();
  if (!/^[a-zA-Z0-9._-]{8,120}$/.test(id)) return null;
  return id;
}

export function registerXprocesLiveRoutes(app, options = {}) {
  const DATA_ROOT = options.DATA_ROOT || process.env.DATA_ROOT || "/data/mapxion";
  const requireWorkerAuth = options.requireWorkerAuth || ((_req, _res, next) => next());

  const liveRoot = (jobId) => path.join(DATA_ROOT, "jobs", jobId, "live");
  const latestPath = (jobId) => path.join(liveRoot(jobId), "latest.jpg");

  const liveStorage = multer.diskStorage({
    destination: (req, _file, cb) => {
      const jobId = safeJobId(req.params.id);
      if (!jobId) return cb(new Error("invalid_job_id"));
      const dir = liveRoot(jobId);
      fs.mkdirSync(dir, { recursive: true });
      cb(null, dir);
    },
    filename: (_req, _file, cb) => cb(null, `latest.tmp.${Date.now()}.jpg`)
  });

  const uploadLive = multer({
    storage: liveStorage,
    limits: { fileSize: 3 * 1024 * 1024 },
    fileFilter: (_req, file, cb) => {
      const mime = String(file.mimetype || "").toLowerCase();
      if (mime === "image/jpeg" || mime === "image/jpg" || mime === "image/png") return cb(null, true);
      cb(new Error("invalid_live_image_type"));
    }
  });

  app.post("/worker/jobs/:id/live", requireWorkerAuth, uploadLive.single("image"), async (req, res) => {
    try {
      const jobId = safeJobId(req.params.id);
      if (!jobId) return res.status(400).json({ ok: false, error: "invalid_job_id" });
      if (!req.file?.path) return res.status(400).json({ ok: false, error: "missing_image" });

      const finalPath = latestPath(jobId);
      await fs.promises.mkdir(path.dirname(finalPath), { recursive: true });
      await fs.promises.rename(req.file.path, finalPath);

      res.json({
        ok: true,
        job_id: jobId,
        url: `/jobs/${encodeURIComponent(jobId)}/live.jpg`,
        updated_at: new Date().toISOString()
      });
    } catch (e) {
      console.error("live upload error", e);
      try { if (req.file?.path && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); } catch {}
      res.status(500).json({ ok: false, error: "live_upload_error" });
    }
  });

  app.get("/jobs/:id/live.jpg", async (req, res) => {
    try {
      const jobId = safeJobId(req.params.id);
      if (!jobId) return res.status(400).send("invalid job id");

      const file = latestPath(jobId);
      if (!fs.existsSync(file)) return res.status(404).send("live image not available");

      res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
      res.setHeader("Pragma", "no-cache");
      res.setHeader("Expires", "0");
      res.sendFile(file);
    } catch (e) {
      console.error("live image error", e);
      res.status(500).send("live image error");
    }
  });

  app.get("/jobs/:id/live/status", async (req, res) => {
    try {
      const jobId = safeJobId(req.params.id);
      if (!jobId) return res.status(400).json({ ok: false, error: "invalid_job_id" });

      const file = latestPath(jobId);
      if (!fs.existsSync(file)) return res.json({ ok: true, available: false });

      const st = await fs.promises.stat(file);
      res.json({
        ok: true,
        available: true,
        updated_at: st.mtime.toISOString(),
        size_bytes: st.size,
        url: `/jobs/${encodeURIComponent(jobId)}/live.jpg`
      });
    } catch (e) {
      console.error("live status error", e);
      res.status(500).json({ ok: false, error: "live_status_error" });
    }
  });
}
