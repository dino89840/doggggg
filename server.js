import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import http from "http";
import https from "https";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// ── Config ──
// dogpan = Cloudreve V4. API base = https://dogpan.com
const API_BASE = (process.env.DOGPAN_API || "https://dogpan.com").replace(/\/+$/, "");

// Cloudreve login token (Bearer). ပုံထဲက Authorization: Bearer eyJhbG... အတိုင်း
const DOGPAN_TOKEN = process.env.DOGPAN_TOKEN || "";

// ⚠️ session cookie — ပုံ(2:38PM) ထဲက Cookie: server_name_session=... အတိုင်း ထည့်ပါ။
//   ဥပမာ: DOGPAN_COOKIE="server_name_session=2a1841d7b53b8873ef7bf7d10e371077"
//   token တစ်ခုတည်းနဲ့ 401 ဖြစ်တာ ဒီ cookie မပါလို့ပါ။
const DOGPAN_COOKIE = process.env.DOGPAN_COOKIE || "";

// upload လုပ်မယ့် target folder (cloudreve URI). ဥပမာ cloudreve://my/iiii
const DEST_URI = (process.env.DOGPAN_DEST_URI || "cloudreve://my").replace(/\/+$/, "");

// ⚠️ storage policy id — ပုံ(1:37 & 1:51 PM) ထဲက အတိုင်း "GwcL" (G-w-c-အင်္ဂလိပ် L)။
//   "Gwc1"(ဂဏန်းတစ်) မဟုတ်ပါ! env နဲ့ override လို့ရတယ်။
const POLICY_ID = process.env.DOGPAN_POLICY_ID || "GwcL";

const MAX_RETRY = Number(process.env.MAX_RETRY || 4);
const DL_TIMEOUT = Number(process.env.DL_TIMEOUT || 600000);

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ── Keep-alive agents ──
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 64 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 64 });
const agentFor = (u) => (u.startsWith("https") ? httpsAgent : httpAgent);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function getFileName(url, fallback) {
  try {
    const u = new URL(url);
    const name = decodeURIComponent(u.pathname.split("/").pop());
    if (name && name.length) return name;
  } catch {}
  return fallback || `file_${Date.now()}.bin`;
}

// filename ကို cloudreve URI path အတွက် encode (space -> %20 …)
function encodeUriName(name) {
  return encodeURIComponent(name).replace(/%2F/gi, "/");
}

// extension ကနေ mime ခန့်မှန်း (session create အတွက်)
function guessMime(name) {
  const ext = (name.split(".").pop() || "").toLowerCase();
  const map = {
    mp4: "video/mp4", mkv: "video/x-matroska", mov: "video/quicktime",
    avi: "video/x-msvideo", webm: "video/webm", m4v: "video/x-m4v",
    mp3: "audio/mpeg", m4a: "audio/mp4", flac: "audio/flac",
    jpg: "image/jpeg", jpeg: "image/jpeg", png: "image/png", gif: "image/gif",
    webp: "image/webp", pdf: "application/pdf", zip: "application/zip",
  };
  return map[ext] || "application/octet-stream";
}

// ── Generic JSON request helper (Cloudreve API) ──
function apiRequest(method, urlPath, body, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const url = urlPath.startsWith("http") ? urlPath : `${API_BASE}${urlPath}`;
    const u = new URL(url);
    const lib = url.startsWith("https") ? https : http;
    const data = body ? Buffer.from(JSON.stringify(body)) : null;

    const headers = {
      Accept: "application/json, text/plain, */*",
      "User-Agent":
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
      ...extraHeaders,
    };
    // ⚠️ Cloudreve V4 (dogpan) က Bearer token ရော session cookie ရော နှစ်ခုလုံး လိုတတ်တယ်
    if (DOGPAN_TOKEN) headers["Authorization"] = `Bearer ${DOGPAN_TOKEN}`;
    if (DOGPAN_COOKIE) headers["Cookie"] = DOGPAN_COOKIE;
    if (data) {
      headers["Content-Type"] = "application/json";
      headers["Content-Length"] = String(data.length);
    }

    const req = lib.request(
      {
        method,
        hostname: u.hostname,
        port: u.port || (url.startsWith("https") ? 443 : 80),
        path: u.pathname + u.search,
        headers,
        agent: agentFor(url),
      },
      (res) => {
        let raw = "";
        res.on("data", (c) => (raw += c));
        res.on("end", () => {
          let json = null;
          try { json = JSON.parse(raw); } catch {}
          // Cloudreve က HTTP 200 ထဲမှာ code != 0 နဲ့ error ပြန်တတ်တယ်
          if (
            res.statusCode >= 200 && res.statusCode < 300 &&
            (!json || json.code === undefined || json.code === 0)
          ) {
            resolve({ status: res.statusCode, json, raw });
          } else {
            const msg = json?.msg || raw.slice(0, 300);
            const code = json?.code !== undefined ? json.code : res.statusCode;
            const err = new Error(`API ${method} ${urlPath} -> code ${code}: ${msg}`);
            err.code = code;
            reject(err);
          }
        });
      }
    );
    req.on("error", reject);
    req.setTimeout(60000, () => req.destroy(new Error("API timeout")));
    if (data) req.write(data);
    req.end();
  });
}

// ── Source URL ဖွင့်ပြီး stream + size + filename + contentType ပြန်ပေး (redirect follow) ──
function openSource(url, depth = 0) {
  return new Promise((resolve, reject) => {
    if (depth > 5) return reject(new Error("Too many redirects"));
    const lib = url.startsWith("https") ? https : http;
    const req = lib.get(
      url,
      { agent: agentFor(url), headers: { "User-Agent": "Mozilla/5.0", Accept: "*/*" } },
      (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          const next = new URL(res.headers.location, url).toString();
          openSource(next, depth + 1).then(resolve, reject);
          return;
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`Download failed: HTTP ${res.statusCode}`));
        }
        const total = Number(res.headers["content-length"]) || 0;
        const contentType = res.headers["content-type"] || "";
        let cdName = "";
        const cd = res.headers["content-disposition"];
        if (cd) {
          const m = /filename\*?=(?:UTF-8'')?["']?([^"';]+)/i.exec(cd);
          if (m) { try { cdName = decodeURIComponent(m[1]); } catch { cdName = m[1]; } }
        }
        resolve({ stream: res, total, cdName, contentType });
      }
    );
    req.on("error", reject);
    req.setTimeout(DL_TIMEOUT, () => req.destroy(new Error("Download timeout")));
  });
}

// ── source ကို buffer အဖြစ် download ──
function downloadToBuffer(url, onProgress) {
  return new Promise(async (resolve, reject) => {
    try {
      const { stream, total, cdName, contentType } = await openSource(url);
      const chunks = [];
      let got = 0;
      stream.on("data", (c) => {
        chunks.push(c);
        got += c.length;
        if (onProgress) onProgress(got, total);
      });
      stream.on("end", () =>
        resolve({ buffer: Buffer.concat(chunks), total: total || got, cdName, contentType })
      );
      stream.on("error", reject);
    } catch (e) { reject(e); }
  });
}

// ── presigned URL ဆီ chunk (Buffer) တစ်ခုကို PUT (S3 UploadPart). ETag ပြန်ပေး ──
function putChunk(uploadUrl, buf) {
  return new Promise((resolve, reject) => {
    const u = new URL(uploadUrl);
    const lib = uploadUrl.startsWith("https") ? https : http;
    const req = lib.request(
      {
        method: "PUT",
        hostname: u.hostname,
        port: u.port || (uploadUrl.startsWith("https") ? 443 : 80),
        path: u.pathname + u.search,
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length": String(buf.length),
        },
        agent: agentFor(uploadUrl),
      },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c.toString().slice(0, 300)));
        res.on("end", () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            const etag = (res.headers.etag || res.headers.ETag || "").replace(/"/g, "");
            resolve({ etag, status: res.statusCode });
          } else {
            reject(new Error(`Chunk PUT HTTP ${res.statusCode} ${body}`));
          }
        });
      }
    );
    req.on("error", reject);
    req.setTimeout(0);
    req.write(buf);
    req.end();
  });
}

// ── S3 CompleteMultipartUpload (XML) ──
function s3CompleteMultipart(completeUrl, parts) {
  return new Promise((resolve, reject) => {
    const xml =
      `<CompleteMultipartUpload>` +
      parts.map((p) => `<Part><PartNumber>${p.partNumber}</PartNumber><ETag>"${p.etag}"</ETag></Part>`).join("") +
      `</CompleteMultipartUpload>`;
    const buf = Buffer.from(xml);
    const u = new URL(completeUrl);
    const lib = completeUrl.startsWith("https") ? https : http;
    const req = lib.request(
      {
        method: "POST",
        hostname: u.hostname,
        port: u.port || (completeUrl.startsWith("https") ? 443 : 80),
        path: u.pathname + u.search,
        headers: { "Content-Type": "application/xml", "Content-Length": String(buf.length) },
        agent: agentFor(completeUrl),
      },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => {
          if (res.statusCode >= 200 && res.statusCode < 300) resolve({ status: res.statusCode, body });
          else reject(new Error(`S3 Complete HTTP ${res.statusCode} ${body.slice(0, 300)}`));
        });
      }
    );
    req.on("error", reject);
    req.write(buf);
    req.end();
  });
}

// ── Jobs store ──
const jobs = new Map();
function newJob() {
  const id = crypto.randomBytes(8).toString("hex");
  const job = {
    id, status: "pending", loaded: 0, total: 0, percent: 0,
    phase: "", result: null, error: null, listeners: new Set(), createdAt: Date.now(),
  };
  jobs.set(id, job);
  setTimeout(() => jobs.delete(id), 2 * 60 * 60 * 1000);
  return job;
}
function emit(job, patch = {}) {
  Object.assign(job, patch);
  const payload = JSON.stringify({
    status: job.status, phase: job.phase, loaded: job.loaded,
    total: job.total, percent: job.percent, result: job.result, error: job.error,
  });
  for (const res of job.listeners) { try { res.write(`data: ${payload}\n\n`); } catch {} }
}

// ── Cloudreve V4 S3 multipart upload (full flow) ──
async function cloudreveUpload(job, sourceUrl, filename) {
  // 1) source download (size သိဖို့ + chunk ခွဲဖို့)
  emit(job, { status: "downloading", phase: "source download", percent: 0 });
  const { buffer, total, cdName, contentType } = await downloadToBuffer(sourceUrl, (got, t) => {
    const pct = t ? Math.floor((got / t) * 40) : 0; // download = 0-40%
    emit(job, { loaded: got, total: t, percent: pct });
  });

  const name = filename || cdName || getFileName(sourceUrl);
  const size = buffer.length;
  const mime =
    (contentType && !contentType.includes("octet-stream"))
      ? contentType.split(";")[0]
      : guessMime(name);

  // 2) Create upload session (PUT /api/v4/file/upload)
  emit(job, { status: "uploading", phase: "create session", percent: 42 });

  if (!POLICY_ID) {
    throw new Error("DOGPAN_POLICY_ID မရှိပါ — ပုံထဲက အတိုင်း 'GwcL' ထည့်ပါ");
  }

  const sessionBody = {
    uri: `${DEST_URI}/${encodeUriName(name)}`,
    size,
    policy_id: POLICY_ID,            // "GwcL"
    last_modified: Date.now(),
    mime_type: mime,                 // video/mp4 …
  };

  let sessResp;
  try {
    sessResp = await apiRequest("PUT", "/api/v4/file/upload", sessionBody);
  } catch (e) {
    // 401 = token/cookie ပြဿနာ — အတိအကျ ပြ
    if (e.code === 401 || /login required|unauthor/i.test(e.message)) {
      throw new Error(
        "401 Login required — DOGPAN_TOKEN (Bearer) သို့ DOGPAN_COOKIE (server_name_session) " +
        "မှန်/သက်တမ်းမကုန်အောင် စစ်ပါ။ ပုံ(2:38PM) ထဲက Cookie: server_name_session=... ကို " +
        "DOGPAN_COOKIE env ထဲ ထည့်ဖို့ မမေ့ပါနဲ့ — token တစ်ခုတည်းနဲ့ 401 ဖြစ်တတ်ပါတယ်။"
      );
    }
    // unknown policy id ဆို env ကို ပြန်စစ်ဖို့ ရှင်းရှင်းပြ
    if (/policy/i.test(e.message)) {
      throw new Error(
        `Session create fail (${e.message}). ` +
        `policy_id="${POLICY_ID}" ကို စစ်ပါ — ပုံထဲက အတိုင်း "GwcL" (G-w-c-အင်္ဂလိပ်L) ဖြစ်ရမယ်၊ "Gwc1"(ဂဏန်း) မဟုတ်ပါ။`
      );
    }
    throw e;
  }

  const session = sessResp.json?.data || sessResp.json;
  if (!session || !session.upload_urls || !session.upload_urls.length) {
    throw new Error("Upload session response invalid: " + JSON.stringify(sessResp.json).slice(0, 300));
  }

  const sessionId = session.session_id;
  const chunkSize = session.chunk_size || size; // 0 ဆို တစ်ပိုင်းတည်း
  const uploadUrls = session.upload_urls;
  const completeUrl = session.complete_url || session.completeURL || null;

  // 3) chunk တစ်ခုချင်း PUT (S3 presigned UploadPart)
  const parts = [];
  const numChunks = uploadUrls.length;
  for (let i = 0; i < numChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, size);
    const slice = buffer.subarray(start, end);

    let lastErr;
    let ok = false;
    for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
      try {
        const r = await putChunk(uploadUrls[i], slice);
        parts.push({ partNumber: i + 1, etag: r.etag });
        ok = true;
        break;
      } catch (e) {
        lastErr = e;
        if (attempt < MAX_RETRY) await sleep(2000 * attempt);
      }
    }
    if (!ok) throw lastErr;

    // upload progress = 42-95%
    const pct = 42 + Math.floor(((i + 1) / numChunks) * 53);
    emit(job, { loaded: end, total: size, percent: pct, phase: `chunk ${i + 1}/${numChunks}` });
  }

  // 4) Complete
  emit(job, { phase: "finalize", percent: 96 });
  // 4a) S3 CompleteMultipartUpload (presigned complete URL ရှိရင်)
  if (completeUrl) {
    try { await s3CompleteMultipart(completeUrl, parts); } catch (e) {
      console.warn("[s3 complete] " + e.message);
    }
  }
  // 4b) Cloudreve "Complete S3 upload" callback — GET /api/v4/callback/s3/{sessionId}
  //  ⚠️ ပုံ(1:37 PM hSG1MeWE) မှာ ဒီ callback က GET method နဲ့ ဖြစ်တာ မြင်ရတယ်
  if (sessionId) {
    try {
      await apiRequest("GET", `/api/v4/callback/s3/${sessionId}`);
    } catch (e) {
      // POST လည်း fallback စမ်း
      try { await apiRequest("POST", `/api/v4/callback/s3/${sessionId}`, {}); }
      catch (e2) { console.warn("[callback] " + e.message + " | " + e2.message); }
    }
  }

  return { name, size, uri: sessionBody.uri };
}

// ── Runner ──
async function runUpload(job, url, filename) {
  try {
    const r = await cloudreveUpload(job, url, filename);
    emit(job, {
      status: "done", percent: 100, phase: "done",
      result: { ...r, message: "အောင်မြင်စွာ တင်ပြီးပါပြီ ✅" },
    });
  } catch (err) {
    console.error("[runUpload]", err);
    emit(job, { status: "error", error: err.message || "တင်မရပါ" });
  } finally {
    for (const res of job.listeners) { try { res.end(); } catch {} }
    job.listeners.clear();
  }
}

// ── Routes ──
app.post("/api/upload", (req, res) => {
  const { url, filename } = req.body || {};
  if (!url) return res.status(400).json({ error: "URL လိုအပ်ပါသည်" });
  if (!DOGPAN_TOKEN) return res.status(500).json({ error: "DOGPAN_TOKEN မရှိပါ (Cloudreve Bearer token)" });
  if (!DOGPAN_COOKIE) {
    console.warn("[warn] DOGPAN_COOKIE မပါ — 401 Login required ဖြစ်နိုင်ပါတယ်။");
  }
  const job = newJob();
  runUpload(job, url, filename).catch(() => {});
  res.json({ jobId: job.id });
});

app.get("/api/progress/:id", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).end();
  res.set({ "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
  res.flushHeaders?.();
  res.write(`data: ${JSON.stringify({
    status: job.status, phase: job.phase, loaded: job.loaded, total: job.total,
    percent: job.percent, result: job.result, error: job.error,
  })}\n\n`);
  if (job.status === "done" || job.status === "error") return res.end();
  job.listeners.add(res);
  req.on("close", () => job.listeners.delete(res));
});

setInterval(() => {
  for (const job of jobs.values())
    for (const res of job.listeners) { try { res.write(`: ping\n\n`); } catch {} }
}, 15000).unref();

// ── Simple UI ──
app.get("/", (req, res) => {
  res.send(`<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DogPan Remote Uploader</title>
<style>
body{font-family:system-ui;max-width:600px;margin:40px auto;padding:0 16px}
input{width:100%;padding:12px;font-size:16px;box-sizing:border-box;margin:8px 0}
button{padding:12px 24px;font-size:16px;background:#d97706;color:#fff;border:0;border-radius:6px}
button:disabled{opacity:.5}
.bar{height:24px;background:#e5e7eb;border-radius:12px;overflow:hidden;margin:12px 0;display:none}
.fill{height:100%;width:0;background:#d97706;transition:width .3s;color:#fff;text-align:center;font-size:13px;line-height:24px}
#stat{font-size:14px;color:#444;margin:6px 0}
pre{background:#f4f4f4;padding:12px;border-radius:6px;overflow:auto;white-space:pre-wrap}
</style></head><body>
<h2>DogPan Remote Uploader (Cloudreve)</h2>
<p>File link ထည့်ပါ — server က download ပြီး dogpan ဆီ S3 multipart နဲ့ auto တင်ပေးပါမယ်။</p>
<input id="url" placeholder="https://example.com/file.mp4">
<input id="fn" placeholder="(optional) filename — ဥပမာ movie.mp4">
<button id="btn" onclick="go()">Upload</button>
<div class="bar" id="bar"><div class="fill" id="fill">0%</div></div>
<div id="stat"></div>
<pre id="out"></pre>
<script>
function fmt(b){if(!b)return'?';const u=['B','KB','MB','GB'];let i=0;while(b>=1024&&i<3){b/=1024;i++;}return b.toFixed(1)+u[i];}
async function go(){
  const url=document.getElementById('url').value.trim();
  const fn=document.getElementById('fn').value.trim();
  const out=document.getElementById('out'),bar=document.getElementById('bar');
  const fill=document.getElementById('fill'),stat=document.getElementById('stat'),btn=document.getElementById('btn');
  if(!url){out.textContent='Link ထည့်ပါ';return;}
  out.textContent='';stat.textContent='';btn.disabled=true;
  bar.style.display='block';fill.style.width='0%';fill.textContent='0%';
  try{
    const r=await fetch('/api/upload',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url,filename:fn})});
    const d=await r.json();
    if(!d.jobId){out.textContent='Error: '+(d.error||'unknown');btn.disabled=false;return;}
    const es=new EventSource('/api/progress/'+d.jobId);
    es.onmessage=(ev)=>{
      const j=JSON.parse(ev.data);
      fill.style.width=j.percent+'%';fill.textContent=j.percent+'%';
      let line=(j.phase||j.status);
      if(j.loaded||j.total)line+=' — '+fmt(j.loaded)+(j.total?' / '+fmt(j.total):'');
      stat.textContent=line;
      if(j.status==='done'){es.close();btn.disabled=false;out.textContent=JSON.stringify(j.result,null,2);stat.textContent='✅ ပြီးပါပြီ';}
      if(j.status==='error'){es.close();btn.disabled=false;out.textContent='❌ '+j.error;}
    };
    es.onerror=()=>{es.close();btn.disabled=false;};
  }catch(e){out.textContent='Error: '+e.message;btn.disabled=false;}
}
</script>
</body></html>`);
});

app.get("/health", (req, res) => res.json({ ok: true }));
app.listen(PORT, () => console.log(`🚀 DogPan (Cloudreve) Uploader on port ${PORT}`));
