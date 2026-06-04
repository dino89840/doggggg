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

// Chunk size — proxy 413 ရှောင်ဖို့ 8MB (Railway variable မှာ ပြောင်းနိုင်)
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || 8 * 1024 * 1024);

// Chunk fail ရင် retry
const MAX_RETRY = Number(process.env.MAX_RETRY || 6);

// Chunk တစ်ခုပြီးတိုင်း server ကို မဖိအောင် ခဏနား (ms)
const CHUNK_DELAY = Number(process.env.CHUNK_DELAY || 300);

// Request တစ်ခုစီအတွက် timeout (ms)
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

// Source URL ကို stream download ဖွင့် (Range မသုံးတော့ — R2 မှာ 400 ဖြစ်လို့)
async function openSourceStream(url) {
  const res = await fetchT(
    url,
    { headers: { "User-Agent": "Mozilla/5.0 (RemoteUploader)" } },
    0 // stream ဆွဲတာ ကြာနိုင်လို့ timeout မထား (0 = no timeout)
  );
  if (!res.ok && res.status !== 206 && res.status !== 200) {
    throw new Error(`Source download fail (HTTP ${res.status})`);
  }
  const size = Number(res.headers.get("content-length") || 0);
  return { res, size };
}

// ===== Helper: PUT တစ်ကြိမ်ကို retry နဲ့ စမ်း =====
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
          await sleep(3000 * attempt);
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

// ===== Nextcloud paths ဆောက်ဖို့ helper =====
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

// ===== MAIN: STREAM download → chunk ပြည့်တိုင်း dogpan ဆီ တင် =====
// Range request လုံးဝ မသုံးတော့ပါ (R2 မှာ 400 ဖြစ်လို့)
// memory ထဲ file တစ်ခုလုံး မထည့်ဘဲ chunk တစ်ခုစီသာ ထား → 300MB+ ရ
async function streamUpload(url, name, total, onProgress) {
  const { uploadDir, destination } = buildPaths(name);

  // 1) Upload folder ဆောက် (MKCOL)
  let r = await fetchT(uploadDir, {
    method: "MKCOL",
    headers: { Authorization: authHeader(), Destination: destination },
  });
  if (!r.ok && r.status !== 201 && r.status !== 405) {
    throw new Error(`MKCOL fail (HTTP ${r.status})`);
  }

  // 2) Source ကို stream ဖွင့်
  const { res } = await openSourceStream(url);
  const reader = res.body.getReader();

  // 3) Stream ဖတ်ရင်း CHUNK_SIZE ပြည့်တိုင်း chunk တင်
  let index = 1;
  let uploaded = 0;
  let pending = []; // လက်ရှိ chunk အတွက် buffer အပိုင်းတွေ
  let pendingLen = 0;

  // chunk တစ်ခုကို dogpan ဆီ ပို့တဲ့ inner function
  const flushChunk = async (chunkBuf) => {
    const chunkName = String(index).padStart(5, "0");
    await putWithRetry(
      `${uploadDir}/${chunkName}`,
      {
        Authorization: authHeader(),
        Destination: destination,
        "OC-Total-Length": String(total || 0),
        "Content-Type": "application/octet-stream",
        "Content-Length": String(chunkBuf.length),
      },
      chunkBuf,
      `Chunk ${index}`
    );
    uploaded += chunkBuf.length;
    index++;
    if (onProgress) onProgress(uploaded, total);
    if (CHUNK_DELAY > 0) await sleep(CHUNK_DELAY);
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    pending.push(Buffer.from(value));
    pendingLen += value.length;

    // CHUNK_SIZE ပြည့်ရင် chunk ဖြတ်ပြီး တင် (ပြည့်နေသမျှ loop)
    while (pendingLen >= CHUNK_SIZE) {
      const merged = Buffer.concat(pending, pendingLen);
      const chunkBuf = merged.subarray(0, CHUNK_SIZE);
      const rest = merged.subarray(CHUNK_SIZE);

      await flushChunk(chunkBuf);

      // ကျန်တာကို pending ပြန်ထား
      pending = rest.length ? [Buffer.from(rest)] : [];
      pendingLen = rest.length;
    }
  }

  // 4) ကျန်တဲ့ နောက်ဆုံး chunk (CHUNK_SIZE မပြည့်တဲ့ အပိုင်း) တင်
  if (pendingLen > 0) {
    const lastBuf = Buffer.concat(pending, pendingLen);
    await flushChunk(lastBuf);
  }

  // total မသိခဲ့ရင် (HEAD fail) — တကယ်တင်ပြီးတဲ့ size ကို သုံး
  const finalTotal = total > 0 ? total : uploaded;

  // 5) Chunk တွေ ပြန်ပေါင်း (MOVE .file → destination)
  r = await putAssembleWithRetry(uploadDir, destination, finalTotal);
  if (!r.ok && r.status !== 201 && r.status !== 204) {
    throw new Error(`MOVE/assemble fail (HTTP ${r.status})`);
  }

  return { destination, size: uploaded };
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

    // file size သိရင် OC-Total-Length မှန်အောင် HEAD စစ် (Range မစစ်တော့)
    let total = 0;
    try {
      const head = await fetchT(url, {
        method: "HEAD",
        headers: { "User-Agent": "Mozilla/5.0" },
      });
      total = Number(head.headers.get("content-length") || 0);
    } catch {}

    // Range လုံးဝ မသုံးတော့ဘဲ stream upload သာ သုံး (R2 400 ရှောင်)
    const { destination, size } = await streamUpload(url, name, total);

    return res.json({
      ok: true,
      filename: name,
      url: destination,
      size: size || total,
      message: "အောင်မြင်စွာ တင်ပြီးပါပြီ",
    });
  } catch (err) {
    console.error(err);
    return res.status(502).json({ ok: false, error: err.message });
  }
});

app.get("/health", (req, res) => res.json({ ok: true }));
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
