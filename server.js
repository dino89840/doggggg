import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import fs from "fs";
import fsp from "fs/promises";
import os from "os";
import http from "http";
import https from "https";
import { pipeline } from "stream/promises";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

const WEBDAV_URL = (process.env.WEBDAV_URL || "https://dogpan.com/dav").replace(/\/$/, "");
const WEBDAV_USER = process.env.WEBDAV_USER || "";
const WEBDAV_PASS = process.env.WEBDAV_PASS || "";

const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || 8 * 1024 * 1024); // 8MB chunks
const MAX_RETRY = Number(process.env.MAX_RETRY || 6);
const CHUNK_DELAY = Number(process.env.CHUNK_DELAY || 200);
const REQUEST_TIMEOUT = Number(process.env.REQUEST_TIMEOUT || 120000);

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ── HTTP Agents for High Speed ──
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 64 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 64 });

function authHeader() {
  return "Basic " + Buffer.from(`${WEBDAV_USER}:${WEBDAV_PASS}`).toString("base64");
}

function getFileName(url, fallback) {
  try {
    const u = new URL(url);
    const name = decodeURIComponent(u.pathname.split("/").pop());
    if (name && name.includes(".")) return name;
  } catch {}
  return fallback || `file_${Date.now()}`;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchT(url, options = {}, timeout = REQUEST_TIMEOUT) {
  if (!timeout || timeout <= 0) {
    return await fetch(url, options);
  }
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeout);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(id);
  }
}

// ── Background Jobs Store ──
const jobs = new Map();
function newJob() {
  const id = crypto.randomBytes(8).toString("hex");
  const job = {
    id,
    status: "pending",
    loaded: 0,
    total: 0,
    percent: 0,
    result: null,
    error: null,
    listeners: new Set(),
    createdAt: Date.now(),
  };
  jobs.set(id, job);
  setTimeout(() => jobs.delete(id), 2 * 60 * 60 * 1000); // Clean after 2 hours
  return job;
}

function emit(job, patch = {}) {
  Object.assign(job, patch);
  const payload = JSON.stringify({
    status: job.status,
    loaded: job.loaded,
    total: job.total,
    percent: job.percent,
    result: job.result,
    error: job.error,
  });
  for (const res of job.listeners) {
    try { res.write(`data: ${payload}\n\n`); } catch (_) {}
  }
}

// ── Local Disk Downloader with progress ──
function downloadToFile(url, destPath, onProgress) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    const req = lib.get(url, {
      agent: url.startsWith("https") ? httpsAgent : httpAgent,
      headers: { "User-Agent": "Mozilla/5.0", "Accept": "*/*" },
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const next = new URL(res.headers.location, url).toString();
        downloadToFile(next, destPath, onProgress).then(resolve, reject);
        return;
      }
      if (res.statusCode !== 200 && res.statusCode !== 206) {
        res.resume();
        return reject(new Error(`Download failed: HTTP ${res.statusCode}`));
      }
      const total = Number(res.headers["content-length"]) || 0;
      let loaded = 0;
      res.on("data", (chunk) => {
        loaded += chunk.length;
        if (onProgress) onProgress(loaded, total);
      });
      const ws = fs.createWriteStream(destPath);
      pipeline(res, ws).then(() => resolve({ total: total || loaded })).catch(reject);
    });
    req.on("error", reject);
    req.setTimeout(180000, () => req.destroy(new Error("Download timeout")));
  });
}

async function putWithRetry(targetUrl, headers, body, label) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
    try {
      // Body can be a ReadStream
      const r = await fetchT(targetUrl, { method: "PUT", headers, body });
      if (r.ok || r.status === 201 || r.status === 204) return r;
      lastErr = new Error(`${label} fail (HTTP ${r.status})`);
    } catch (e) {
      lastErr = e;
    }
    if (attempt < MAX_RETRY) await sleep(2000 * attempt);
  }
  throw lastErr;
}

async function putAssembleWithRetry(uploadDir, destination, total) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
    try {
      const r = await fetchT(
        `${uploadDir}/.file`,
        {
          method: "MOVE",
          headers: {
            Authorization: authHeader(),
            Destination: destination,
            "OC-Total-Length": String(total),
          },
        },
        300000 // Assembly can take long
      );
      if (r.ok || r.status === 201 || r.status === 204) return r;
      lastErr = new Error(`MOVE/assemble fail (HTTP ${r.status})`);
    } catch (e) {
      lastErr = e;
    }
    if (attempt < MAX_RETRY) await sleep(3000 * attempt);
  }
  throw lastErr;
}

function buildPaths(name) {
  const userId = WEBDAV_USER.split("@")[0] || WEBDAV_USER;
  const base = WEBDAV_URL.replace(/\/dav$/, "").replace(/\/remote\.php\/dav$/, "");
  const uploadRoot = `${base}/remote.php/dav/uploads/${userId}`;
  const filesRoot = `${base}/remote.php/dav/files/${userId}`;
  const uploadId = `remoteupload-${crypto.randomUUID()}`;
  const uploadDir = `${uploadRoot}/${uploadId}`;
  const destination = `${filesRoot}/${encodeURIComponent(name)}`;
  return { uploadDir, destination };
}

// ── Upload from local disk in chunks (0 MB RAM used!) ──
async function uploadFromDiskInChunks(localPath, name, onProgress) {
  const { uploadDir, destination } = buildPaths(name);

  let r = await fetchT(uploadDir, {
    method: "MKCOL",
    headers: { Authorization: authHeader(), Destination: destination },
  });
  if (!r.ok && r.status !== 201 && r.status !== 405) {
    throw new Error(`MKCOL fail (HTTP ${r.status})`);
  }

  const stat = fs.statSync(localPath);
  const totalSize = stat.size;
  const totalChunks = Math.ceil(totalSize / CHUNK_SIZE);
  let uploaded = 0;

  for (let i = 0; i < totalChunks; i++) {
    const start = i * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE - 1, totalSize - 1);
    const chunkLength = end - start + 1;

    // Stream slice directly from disk without buffer allocation
    const chunkStream = fs.createReadStream(localPath, { start, end });
    const chunkName = String(i + 1).padStart(5, "0");

    await putWithRetry(
      `${uploadDir}/${chunkName}`,
      {
        Authorization: authHeader(),
        Destination: destination,
        "OC-Total-Length": String(totalSize),
        "Content-Type": "application/octet-stream",
        "Content-Length": String(chunkLength),
      },
      chunkStream,
      `Chunk ${i + 1}`
    );

    uploaded += chunkLength;
    if (onProgress) onProgress(uploaded, totalSize);
    if (CHUNK_DELAY > 0) await sleep(CHUNK_DELAY);
  }

  r = await putAssembleWithRetry(uploadDir, destination, totalSize);
  if (!r.ok && r.status !== 201 && r.status !== 204) {
    throw new Error(`MOVE/assemble fail (HTTP ${r.status})`);
  }

  return { destination, size: totalSize };
}

// ── Main Async Runner ──
async function runUpload(job, url, filename) {
  const tmpRoot = path.join(os.tmpdir(), `dogpan-${job.id}`);
  await fsp.mkdir(tmpRoot, { recursive: true });
  const localFile = path.join(tmpRoot, "download.tmp");

  try {
    const name = getFileName(url, filename);

    // Phase 1: Download to Local Disk (0% to 40% progress)
    emit(job, { status: "downloading", percent: 0 });
    await downloadToFile(url, localFile, (loaded, total) => {
      const pct = total ? Math.floor((loaded / total) * 40) : 0;
      emit(job, { loaded, total, percent: pct });
    });

    // Phase 2: Upload Chunks from Disk to WebDAV (40% to 95% progress)
    emit(job, { status: "uploading", percent: 40 });
    const { destination, size } = await uploadFromDiskInChunks(localFile, name, (uploaded, total) => {
      const pct = total ? Math.floor(40 + (uploaded / total) * 55) : 40;
      emit(job, { loaded: uploaded, total, percent: pct });
    });

    // Finished
    emit(job, {
      status: "done",
      percent: 100,
      result: {
        destination,
        size,
        message: "အောင်မြင်စွာ တင်ပြီးပါပြီ",
      },
    });
  } catch (err) {
    console.error(err);
    emit(job, { status: "error", error: err.message || "တင်မရပါ" });
  } finally {
    // Cleanup temporary files
    fsp.rm(tmpRoot, { recursive: true, force: true }).catch(() => {});
    for (const res of job.listeners) {
      try { res.end(); } catch (_) {}
    }
    job.listeners.clear();
  }
}

// ── Routes ──
app.post("/api/upload", (req, res) => {
  const { url, filename } = req.body || {};
  if (!url) return res.status(400).json({ error: "URL လိုအပ်ပါသည်" });
  if (!WEBDAV_USER || !WEBDAV_PASS) {
    return res.status(500).json({ error: "WEBDAV config များ မစုံလင်ပါ" });
  }

  const job = newJob();
  runUpload(job, url, filename).catch(() => {});
  res.json({ jobId: job.id });
});

// SSE progress monitor
app.get("/api/progress/:id", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).end();

  res.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
  });
  res.flushHeaders?.();

  // Initial event
  res.write(`data: ${JSON.stringify({
    status: job.status, loaded: job.loaded, total: job.total, percent: job.percent, result: job.result, error: job.error
  })}\n\n`);

  if (job.status === "done" || job.status === "error") {
    return res.end();
  }

  job.listeners.add(res);
  req.on("close", () => job.listeners.delete(res));
});

// Keep-Alive for SSE
setInterval(() => {
  for (const job of jobs.values()) {
    if (job.listeners.size === 0) continue;
    for (const res of job.listeners) {
      try { res.write(`: keep-alive\n\n`); } catch (_) {}
    }
  }
}, 15000).unref();

app.get("/health", (req, res) => res.json({ ok: true }));
app.listen(PORT, () => console.log(`🚀 DogPan Server on port ${PORT}`));
