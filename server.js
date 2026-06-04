import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// ===== WebDAV (dogpan) connection details — Railway Variables မှာ ထည့်ပါ =====
const WEBDAV_URL = (process.env.WEBDAV_URL || "https://dogpan.com/dav").replace(/\/$/, "");
const WEBDAV_USER = process.env.WEBDAV_USER || "";
const WEBDAV_PASS = process.env.WEBDAV_PASS || "";

// Chunk size — proxy 413 ရှောင်ဖို့ 8MB
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || 8 * 1024 * 1024);

// Chunk fail ရင် retry — 502/503/504 (server ယာယီ error) တွေအတွက် ပိုများအောင် 6 ကြိမ်
const MAX_RETRY = Number(process.env.MAX_RETRY || 6);

// Chunk တစ်ခုပြီးတိုင်း server ကို မဖိအောင် ခဏနား (ms)
const CHUNK_DELAY = Number(process.env.CHUNK_DELAY || 300);

// Request တစ်ခုစီအတွက် timeout (ms) — server မတုံ့ပြန်ရင် hang မဖြစ်အောင်
const REQUEST_TIMEOUT = Number(process.env.REQUEST_TIMEOUT || 120000);

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

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

// fetch + timeout wrapper (AbortController)
async function fetchT(url, options = {}, timeout = REQUEST_TIMEOUT) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeout);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(id);
  }
}

// Source URL ကို download stream ဖွင့်ပြီး file size ယူ
async function openSource(url) {
  const res = await fetchT(url, {
    headers: { "User-Agent": "Mozilla/5.0 (RemoteUploader)" },
  });
  if (!res.ok) throw new Error(`Source download fail (HTTP ${res.status})`);
  const size = Number(res.headers.get("content-length") || 0);
  return { res, size };
}

// ===== Helper: Range request နဲ့ chunk တစ်ပိုင်းကို download (retry နဲ့) =====
async function fetchRange(url, start, end) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
    try {
      const res = await fetchT(url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (RemoteUploader)",
          Range: `bytes=${start}-${end}`,
        },
      });
      if (res.ok || res.status === 206 || res.status === 200) {
        return Buffer.from(await res.arrayBuffer());
      }
      lastErr = new Error(`Range download fail (HTTP ${res.status})`);
    } catch (e) {
      lastErr = e;
    }
    if (attempt < MAX_RETRY) await sleep(1500 * attempt);
  }
  throw lastErr;
}

// ===== Helper: PUT တစ်ကြိမ်ကို retry နဲ့ စမ်း (502/503/504 အတွက် ပိုကြာကြာစောင့်) =====
async function putWithRetry(targetUrl, headers, body, label) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
    try {
      const r = await fetchT(targetUrl, { method: "PUT", headers, body });

      if (r.ok || r.status === 201 || r.status === 204) return r;

      // 413 — chunk အရမ်းကြီး၊ retry လုပ်လို့ အကျိုးမရှိ
      if (r.status === 413) {
        throw new Error(`${label} fail (HTTP 413 — chunk ကြီးနေတယ်၊ CHUNK_SIZE လျှော့ပါ)`);
      }

      // 502/503/504 — server ယာယီ error၊ ပိုကြာကြာ စောင့်ပြီး retry
      if (r.status === 502 || r.status === 503 || r.status === 504) {
        lastErr = new Error(`${label} fail (HTTP ${r.status} — server ယာယီ busy)`);
        if (attempt < MAX_RETRY) {
          await sleep(3000 * attempt); // 3s, 6s, 9s ... backoff
          continue;
        }
      }

      lastErr = new Error(`${label} fail (HTTP ${r.status})`);
    } catch (e) {
      lastErr = e;
    }
    if (attempt < MAX_RETRY) await sleep(2000 * attempt);
  }
  throw lastErr;
}

// ===== NEXTCLOUD-STYLE CHUNKED UPLOAD (Range download) =====
async function chunkedUpload(url, name, total, onProgress) {
  const userId = WEBDAV_USER.split("@")[0] || WEBDAV_USER;
  const base = WEBDAV_URL.replace(/\/dav$/, "").replace(/\/remote\.php\/dav$/, "");

  const uploadRoot = `${base}/remote.php/dav/uploads/${userId}`;
  const filesRoot = `${base}/remote.php/dav/files/${userId}`;
  const uploadId = `remoteupload-${crypto.randomUUID()}`;
  const uploadDir = `${uploadRoot}/${uploadId}`;
  const destination = `${filesRoot}/${encodeURIComponent(name)}`;

  // 1) Upload folder ဆောက် (MKCOL)
  let r = await fetchT(uploadDir, {
    method: "MKCOL",
    headers: { Authorization: authHeader(), Destination: destination },
  });
  if (!r.ok && r.status !== 201 && r.status !== 405) {
    throw new Error(`MKCOL fail (HTTP ${r.status})`);
  }

  // 2) Chunk တစ်ပိုင်းချင်း တင် (retry + delay)
  let index = 1;
  let uploaded = 0;
  for (let start = 0; start < total; start += CHUNK_SIZE) {
    const end = Math.min(start + CHUNK_SIZE - 1, total - 1);
    const chunk = await fetchRange(url, start, end);

    const chunkName = String(index).padStart(5, "0");
    await putWithRetry(
      `${uploadDir}/${chunkName}`,
      {
        Authorization: authHeader(),
        Destination: destination,
        "OC-Total-Length": String(total),
        "Content-Type": "application/octet-stream",
        "Content-Length": String(chunk.length),
      },
      chunk,
      `Chunk ${index}`
    );

    uploaded += chunk.length;
    index++;
    if (onProgress) onProgress(uploaded, total);

    // server ကို မဖိအောင် ခဏနား
    if (CHUNK_DELAY > 0) await sleep(CHUNK_DELAY);
  }

  // 3) Chunk တွေ ပြန်ပေါင်း (MOVE .file → destination) — assemble က ကြာတတ်လို့ timeout ပိုပေး
  r = await putAssembleWithRetry(uploadDir, destination, total);
  if (!r.ok && r.status !== 201 && r.status !== 204) {
    throw new Error(`MOVE/assemble fail (HTTP ${r.status})`);
  }

  return destination;
}

// MOVE/assemble ကို retry နဲ့ (assemble က server ဘက် ကြာတတ်လို့ timeout ပိုကြီးပေး)
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
        300000 // assemble timeout 5 မိနစ်
      );
      if (r.ok || r.status === 201 || r.status === 204) return r;
      lastErr = new Error(`MOVE/assemble fail (HTTP ${r.status})`);
      if ((r.status === 502 || r.status === 503 || r.status === 504) && attempt < MAX_RETRY) {
        await sleep(4000 * attempt);
        continue;
      }
    } catch (e) {
      lastErr = e;
    }
    if (attempt < MAX_RETRY) await sleep(3000 * attempt);
  }
  throw lastErr;
}

// ===== Fallback: size မသိ (သို့) Range မ support → download ပြီးမှ chunk တင် =====
async function downloadThenChunkedUpload(url, name, onProgress) {
  const { res } = await openSource(url);
  const reader = res.body.getReader();
  const parts = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(Buffer.from(value));
    total += value.length;
  }
  const full = Buffer.concat(parts, total);
  return await chunkedUploadFromBuffer(full, name, onProgress);
}

async function chunkedUploadFromBuffer(buffer, name, onProgress) {
  const total = buffer.length;
  const userId = WEBDAV_USER.split("@")[0] || WEBDAV_USER;
  const base = WEBDAV_URL.replace(/\/dav$/, "").replace(/\/remote\.php\/dav$/, "");

  const uploadRoot = `${base}/remote.php/dav/uploads/${userId}`;
  const filesRoot = `${base}/remote.php/dav/files/${userId}`;
  const uploadId = `remoteupload-${crypto.randomUUID()}`;
  const uploadDir = `${uploadRoot}/${uploadId}`;
  const destination = `${filesRoot}/${encodeURIComponent(name)}`;

  let r = await fetchT(uploadDir, {
    method: "MKCOL",
    headers: { Authorization: authHeader(), Destination: destination },
  });
  if (!r.ok && r.status !== 201 && r.status !== 405) {
    throw new Error(`MKCOL fail (HTTP ${r.status})`);
  }

  let index = 1;
  let uploaded = 0;
  for (let start = 0; start < total; start += CHUNK_SIZE) {
    const end = Math.min(start + CHUNK_SIZE, total);
    const chunk = buffer.subarray(start, end);

    const chunkName = String(index).padStart(5, "0");
    await putWithRetry(
      `${uploadDir}/${chunkName}`,
      {
        Authorization: authHeader(),
        Destination: destination,
        "OC-Total-Length": String(total),
        "Content-Type": "application/octet-stream",
        "Content-Length": String(chunk.length),
      },
      chunk,
      `Chunk ${index}`
    );

    uploaded += chunk.length;
    index++;
    if (onProgress) onProgress(uploaded, total);
    if (CHUNK_DELAY > 0) await sleep(CHUNK_DELAY);
  }

  r = await putAssembleWithRetry(uploadDir, destination, total);
  if (!r.ok && r.status !== 201 && r.status !== 204) {
    throw new Error(`MOVE/assemble fail (HTTP ${r.status})`);
  }

  return destination;
}

// ===== Transfer endpoint =====
app.post("/api/transfer", async (req, res) => {
  const { url, filename } = req.body;
  if (!url) return res.status(400).json({ ok: false, error: "URL လိုအပ်ပါတယ်" });
  if (!WEBDAV_USER || !WEBDAV_PASS) {
    return res.status(500).json({ ok: false, error: "WEBDAV_USER / WEBDAV_PASS env မထည့်ရသေးပါ" });
  }

  try {
    const name = getFileName(url, filename);

    let total = 0;
    let acceptRanges = false;
    try {
      const head = await fetchT(url, { method: "HEAD", headers: { "User-Agent": "Mozilla/5.0" } });
      total = Number(head.headers.get("content-length") || 0);
      acceptRanges = (head.headers.get("accept-ranges") || "").includes("bytes");
    } catch {}

    let finalUrl;
    if (total > 0 && acceptRanges) {
      finalUrl = await chunkedUpload(url, name, total);
    } else {
      finalUrl = await downloadThenChunkedUpload(url, name);
    }

    return res.json({
      ok: true,
      filename: name,
      url: finalUrl,
      size: total,
      message: "အောင်မြင်စွာ တင်ပြီးပါပြီ",
    });
  } catch (err) {
    console.error(err);
    return res.status(502).json({ ok: false, error: err.message });
  }
});

app.get("/health", (req, res) => res.json({ ok: true }));
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
