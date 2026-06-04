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

// Chunk size — Cloudflare 100MB limit အောက်မှာ ဘေးကင်းအောင် 90MB ထားတယ်
// (Nextcloud chunking spec က 5MB–5GB ခွင့်ပြုပေမယ့် Cloudflare ကြောင့် 90MB ထား)
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || 90 * 1024 * 1024);

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

  // 2) Chunk တစ်ပိုင်းချင်း တင်
  let index = 1;
  let uploaded = 0;
  for (let start = 0; start < total; start += CHUNK_SIZE) {
    const end = Math.min(start + CHUNK_SIZE - 1, total - 1);
    const chunk = await fetchRange(url, start, end);

    const chunkName = String(index).padStart(5, "0");
    r = await fetch(`${uploadDir}/${chunkName}`, {
      method: "PUT",
      headers: {
        Authorization: authHeader(),
        Destination: destination,
        "OC-Total-Length": String(total),
        "Content-Type": "application/octet-stream",
      },
      body: chunk,
    });
    if (!r.ok && r.status !== 201 && r.status !== 204) {
      throw new Error(`Chunk ${index} upload fail (HTTP ${r.status})`);
    }

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

// ===== Fallback: ဖိုင်က CHUNK_SIZE အောက်ဆို direct PUT =====
async function directUpload(url, name) {
  const { res } = await openSource(url);
  const target = `${WEBDAV_URL}/${encodeURIComponent(name)}`;
  const up = await fetch(target, {
    method: "PUT",
    headers: { Authorization: authHeader(), "Content-Type": "application/octet-stream" },
    body: res.body,
    duplex: "half",
  });
  if (!up.ok && up.status !== 201 && up.status !== 204) {
    throw new Error(`Direct PUT fail (HTTP ${up.status})`);
  }
  return target;
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
    try {
      const head = await fetch(url, { method: "HEAD", headers: { "User-Agent": "Mozilla/5.0" } });
      total = Number(head.headers.get("content-length") || 0);
    } catch {}

    let finalUrl;
    if (total > 0 && total > CHUNK_SIZE) {
      // ကြီးတဲ့ဖိုင် → chunked upload
      finalUrl = await chunkedUpload(url, name, total);
    } else {
      // သေးတဲ့ဖိုင် (သို့) size မသိ → direct PUT
      finalUrl = await directUpload(url, name);
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
