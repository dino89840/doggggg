import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

const WEBDAV_URL = (process.env.WEBDAV_URL || "https://dogpan.com/dav").replace(/\/$/, "");
const WEBDAV_USER = process.env.WEBDAV_USER || "";
const WEBDAV_PASS = process.env.WEBDAV_PASS || "";

const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || 8 * 1024 * 1024);
const MAX_RETRY = Number(process.env.MAX_RETRY || 6);
const CHUNK_DELAY = Number(process.env.CHUNK_DELAY || 300);
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

// fetch + timeout wrapper
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

async function openSourceStream(url) {
  const res = await fetchT(
    url,
    { headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" } },
    0 // stream — timeout မထားပါ
  );
  if (!res.ok && res.status !== 206 && res.status !== 200) {
    throw new Error(`Source download fail (HTTP ${res.status})`);
  }
  const size = Number(res.headers.get("content-length") || 0);
  return { res, size };
}

async function putWithRetry(targetUrl, headers, body, label) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
    try {
      const r = await fetchT(targetUrl, { method: "PUT", headers, body });
      if (r.ok || r.status === 201 || r.status === 204) return r;
      if (r.status === 413) {
        throw new Error(`${label} fail (HTTP 413 — chunk size ကြီးလွန်းသဖြင့် CHUNK_SIZE ကို လျှော့ချပါ)`);
      }
      if (r.status === 502 || r.status === 503 || r.status === 504) {
        lastErr = new Error(`${label} fail (HTTP ${r.status} — server ယာယီမအားပါ)`);
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
        300000 // Assembly timeout ကို ၅ မိနစ်အထိ တိုးမြှင့်ထားသည်
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

// STREAM download → chunk ပြည့်တိုင်း dogpan ဆီ တင်
async function streamUpload(url, name, total, onProgress, checkAborted) {
  const { uploadDir, destination } = buildPaths(name);

  let r = await fetchT(uploadDir, {
    method: "MKCOL",
    headers: { Authorization: authHeader(), Destination: destination },
  });
  if (!r.ok && r.status !== 201 && r.status !== 405) {
    throw new Error(`MKCOL fail (HTTP ${r.status})`);
  }

  const { res } = await openSourceStream(url);
  const reader = res.body.getReader();

  let index = 1;
  let uploaded = 0;
  let pending = [];
  let pendingLen = 0;

  const flushChunk = async (chunkBuf) => {
    if (checkAborted && checkAborted()) {
      throw new Error("အသုံးပြုသူမှ ချိတ်ဆက်မှု ဖြတ်တောက်လိုက်ပါသည်");
    }
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

  try {
    while (true) {
      if (checkAborted && checkAborted()) {
        throw new Error("အသုံးပြုသူမှ ချိတ်ဆက်မှု ဖြတ်တောက်လိုက်ပါသည်");
      }
      const { done, value } = await reader.read();
      if (done) break;
      pending.push(Buffer.from(value));
      pendingLen += value.length;

      while (pendingLen >= CHUNK_SIZE) {
        const merged = Buffer.concat(pending, pendingLen);
        const chunkBuf = merged.subarray(0, CHUNK_SIZE);
        const rest = merged.subarray(CHUNK_SIZE);
        await flushChunk(chunkBuf);
        pending = rest.length ? [Buffer.from(rest)] : [];
        pendingLen = rest.length;
      }
    }

    if (pendingLen > 0) {
      const lastBuf = Buffer.concat(pending, pendingLen);
      await flushChunk(lastBuf);
    }
  } finally {
    // Memory ပြန်လည်ရှင်းလင်းပေးရန်
    pending = null;
    try { reader.releaseLock(); } catch (e) {}
  }

  if (checkAborted && checkAborted()) {
    throw new Error("ဖိုင်တွဲများ မပေါင်းစည်းမီ အသုံးပြုသူမှ ချိတ်ဆက်မှု ဖြတ်တောက်လိုက်ပါသည်");
  }

  const finalTotal = total > 0 ? total : uploaded;
  r = await putAssembleWithRetry(uploadDir, destination, finalTotal);
  if (!r.ok && r.status !== 201 && r.status !== 204) {
    throw new Error(`MOVE/assemble fail (HTTP ${r.status})`);
  }

  return { destination, size: uploaded };
}

// ===== SSE (streaming) endpoint =====
app.get("/api/transfer-stream", async (req, res) => {
  const url = req.query.url;
  const filename = req.query.filename;

  // SSE headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  let aborted = false;
  req.on("close", () => { aborted = true; });

  const send = (event, data) => {
    if (aborted || res.writableEnded) return;
    try {
      res.write(`event: ${event}\n`);
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    } catch (e) {
      console.error("SSE write error:", e);
    }
  };

  if (!url) {
    send("error", { error: "URL လိုအပ်ပါတယ်" });
    return res.end();
  }
  if (!WEBDAV_USER || !WEBDAV_PASS) {
    send("error", { error: "WEBDAV_USER / WEBDAV_PASS env မထည့်ရသေးပါ" });
    return res.end();
  }

  // connection alive ဖို့ heartbeat (15s တိုင်း comment ပို့)
  const heartbeat = setInterval(() => {
    if (!aborted && !res.writableEnded) {
      try {
        res.write(`: keep-alive\n\n`);
      } catch (e) {
        clearInterval(heartbeat);
      }
    } else {
      clearInterval(heartbeat);
    }
  }, 15000);

  try {
    const name = getFileName(url, filename);

    let total = 0;
    try {
      const head = await fetchT(url, {
        method: "HEAD",
        headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
      });
      total = Number(head.headers.get("content-length") || 0);
    } catch (e) {
      console.log("HEAD request failed, proceeding without total size:", e.message);
    }

    send("start", { filename: name, total });

    const { destination, size } = await streamUpload(
      url,
      name,
      total,
      (uploaded, t) => {
        if (aborted) return;
        const pct = t > 0 ? Math.round((uploaded / t) * 100) : 0;
        send("progress", { uploaded, total: t, percent: pct });
      },
      () => aborted
    );

    send("done", {
      ok: true,
      filename: name,
      url: destination,
      size: size || total,
      message: "အောင်မြင်စွာ တင်ပြီးပါပြီ",
    });
  } catch (err) {
    console.error(err);
    const errMsg = err instanceof Error ? err.message : String(err);
    send("error", { error: errMsg || "မသိနိုင်သော အမှားတစ်ခု ဖြစ်ပွားခဲ့သည်" });
  } finally {
    clearInterval(heartbeat);
    try { res.end(); } catch (e) {}
  }
});

// ===== ရိုးရိုး JSON endpoint =====
app.post("/api/transfer", async (req, res) => {
  const { url, filename } = req.body;
  if (!url) return res.status(400).json({ ok: false, error: "URL လိုအပ်ပါတယ်" });
  if (!WEBDAV_USER || !WEBDAV_PASS) {
    return res.status(500).json({ ok: false, error: "WEBDAV_USER / WEBDAV_PASS env မထည့်ရသေးပါ" });
  }
  try {
    const name = getFileName(url, filename);
    let total = 0;
    try {
      const head = await fetchT(url, { method: "HEAD", headers: { "User-Agent": "Mozilla/5.0" } });
      total = Number(head.headers.get("content-length") || 0);
    } catch {}
    const { destination, size } = await streamUpload(url, name, total, null, () => false);
    return res.json({
      ok: true,
      filename: name,
      url: destination,
      size: size || total,
      message: "အောင်မြင်စွာ တင်ပြီးပါပြီ",
    });
  } catch (err) {
    console.error(err);
    const errMsg = err instanceof Error ? err.message : String(err);
    return res.status(502).json({ ok: false, error: errMsg });
  }
});

app.get("/health", (req, res) => res.json({ ok: true }));
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
