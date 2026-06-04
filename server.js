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

// Chunk size — Railway/Cloudflare proxy က ကြီးတဲ့ chunk ကို 413 ပြတတ်လို့
// 8MB ထားတယ်။ (Nextcloud default က 10MB) chunk များတာ ကိစ္စမရှိ၊ နောက်မှ ပြန်ပေါင်းတယ်။
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || 8 * 1024 * 1024);

// Chunk တစ်ခု upload fail ရင် ဘယ်နှစ်ကြိမ် ပြန်ကြိုးစားမလဲ
const MAX_RETRY = Number(process.env.MAX_RETRY || 3);

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

// small sleep helper (retry backoff အတွက်)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Source URL ကို download stream ဖွင့်ပြီး file size ယူ
async function openSource(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (RemoteUploader)" },
  });
  if (!res.ok) throw new Error(`Source download fail (HTTP ${res.status})`);
  const size = Number(res.headers.get("content-length") || 0);
  return { res, size };
}

// ===== Helper: Range request နဲ့ chunk တစ်ပိုင်းကို download =====
async function fetchRange(url, start, end) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (RemoteUploader)",
      Range: `bytes=${start}-${end}`,
    },
  });
  if (!res.ok && res.status !== 206 && res.status !== 200) {
    throw new Error(`Range download fail (HTTP ${res.status})`);
  }
  const buf = Buffer.from(await res.arrayBuffer());
  return buf;
}

// ===== Helper: PUT တစ်ကြိမ်ကို retry နဲ့ စမ်း =====
async function putWithRetry(targetUrl, headers, body, label) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
    try {
      const r = await fetch(targetUrl, { method: "PUT", headers, body });
      if (r.ok || r.status === 201 || r.status === 204) return r;

      // 413 ဆို chunk အရမ်းကြီးနေတယ်လို့ ရှင်းရှင်းပြောပေး
      if (r.status === 413) {
        throw new Error(
          `${label} fail (HTTP 413 — chunk အရမ်းကြီးနေတယ်။ CHUNK_SIZE ကို လျှော့ပါ)`
        );
      }
      lastErr = new Error(`${label} fail (HTTP ${r.status})`);
    } catch (e) {
      lastErr = e;
    }
    if (attempt < MAX_RETRY) await sleep(1000 * attempt); // backoff
  }
  throw lastErr;
}

// ===== NEXTCLOUD-STYLE CHUNKED UPLOAD =====
// DogPan က Nextcloud-based ဆိုရင် ဒီနည်း အလုပ်ဖြစ်ပါတယ်။
async function chunkedUpload(url, name, total, onProgress) {
  // userid ကို username ကနေ ယူ (Nextcloud က email ရဲ့ ရှေ့ပိုင်း သုံးတတ်)
  const userId = WEBDAV_USER.split("@")[0] || WEBDAV_USER;
  const base = WEBDAV_URL.replace(/\/dav$/, "").replace(/\/remote\.php\/dav$/, "");

  // Nextcloud upload path
  const uploadRoot = `${base}/remote.php/dav/uploads/${userId}`;
  const filesRoot = `${base}/remote.php/dav/files/${userId}`;
  const uploadId = `remoteupload-${crypto.randomUUID()}`;
  const uploadDir = `${uploadRoot}/${uploadId}`;
  const destination = `${filesRoot}/${encodeURIComponent(name)}`;

  // 1) Upload folder ဆောက် (MKCOL)
  let r = await fetch(uploadDir, {
    method: "MKCOL",
    headers: { Authorization: authHeader(), Destination: destination },
  });
  if (!r.ok && r.status !== 201 && r.status !== 405) {
    throw new Error(`MKCOL fail (HTTP ${r.status})`);
  }

  // 2) Chunk တစ်ပိုင်းချင်း တင် (retry နဲ့)
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
  }

  // 3) Chunk တွေ ပြန်ပေါင်း (MOVE .file → destination)
  r = await fetch(`${uploadDir}/.file`, {
    method: "MOVE",
    headers: {
      Authorization: authHeader(),
      Destination: destination,
      "OC-Total-Length": String(total),
    },
  });
  if (!r.ok && r.status !== 201 && r.status !== 204) {
    throw new Error(`MOVE/assemble fail (HTTP ${r.status})`);
  }

  return destination;
}

// ===== Fallback: size မသိတဲ့ဖိုင်ကို memory ထဲ stream → chunked upload =====
// size မသိရင်လည်း direct PUT မလုပ်တော့ဘဲ download ပြီးမှ chunk အဖြစ်ပိုင်းတင်တယ်။
// (Direct PUT ကြီးတဲ့ဖိုင်မှာ 413 ဖြစ်နိုင်လို့)
async function downloadThenChunkedUpload(url, name, onProgress) {
  const { res } = await openSource(url);

  // stream ကို chunk array အဖြစ် စုဆောင်း (size မသိလို့)
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

  // size သိသွားပြီမို့ chunked upload အတွက် local buffer ကနေ ပိုင်းတင်
  return await chunkedUploadFromBuffer(full, name, onProgress);
}

// Buffer ကနေ chunked upload (Range download မလိုတော့ဘဲ buffer slice သုံး)
async function chunkedUploadFromBuffer(buffer, name, onProgress) {
  const total = buffer.length;
  const userId = WEBDAV_USER.split("@")[0] || WEBDAV_USER;
  const base = WEBDAV_URL.replace(/\/dav$/, "").replace(/\/remote\.php\/dav$/, "");

  const uploadRoot = `${base}/remote.php/dav/uploads/${userId}`;
  const filesRoot = `${base}/remote.php/dav/files/${userId}`;
  const uploadId = `remoteupload-${crypto.randomUUID()}`;
  const uploadDir = `${uploadRoot}/${uploadId}`;
  const destination = `${filesRoot}/${encodeURIComponent(name)}`;

  let r = await fetch(uploadDir, {
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
  }

  r = await fetch(`${uploadDir}/.file`, {
    method: "MOVE",
    headers: {
      Authorization: authHeader(),
      Destination: destination,
      "OC-Total-Length": String(total),
    },
  });
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

    // Source size စစ် (HEAD)
    let total = 0;
    let acceptRanges = false;
    try {
      const head = await fetch(url, { method: "HEAD", headers: { "User-Agent": "Mozilla/5.0" } });
      total = Number(head.headers.get("content-length") || 0);
      acceptRanges = (head.headers.get("accept-ranges") || "").includes("bytes");
    } catch {}

    let finalUrl;
    if (total > 0 && acceptRanges) {
      // size သိ + Range support → chunked upload (memory မကုန်ဆုံး၊ အကောင်းဆုံး)
      finalUrl = await chunkedUpload(url, name, total);
    } else {
      // size မသိ (သို့) Range မ support → download ပြီးမှ chunk အဖြစ် တင်
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
