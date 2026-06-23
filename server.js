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
// dogpan က /dav ကို သုံးခိုင်းထားလို့ default အဖြစ် ထားပါတယ်
const WEBDAV_URL = (process.env.WEBDAV_URL || "https://dogpan.com/dav").replace(/\/+$/, "");
const WEBDAV_USER = process.env.WEBDAV_USER || "";
const WEBDAV_PASS = process.env.WEBDAV_PASS || "";
const MAX_RETRY = Number(process.env.MAX_RETRY || 4);
const DL_TIMEOUT = Number(process.env.DL_TIMEOUT || 180000);

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ── Keep-alive agents ──
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 32 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 32 });
const agentFor = (u) => (u.startsWith("https") ? httpsAgent : httpAgent);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function authHeader() {
  return "Basic " + Buffer.from(`${WEBDAV_USER}:${WEBDAV_PASS}`).toString("base64");
}

function getFileName(url, fallback) {
  try {
    const u = new URL(url);
    const name = decodeURIComponent(u.pathname.split("/").pop());
    if (name && name.length) return name;
  } catch {}
  return fallback || `file_${Date.now()}`;
}

// WebDAV path: dogpan က /dav ဆီ တိုက်ရိုက်တင်တာ (root = user ရဲ့ files)
// filename ထဲက path-unsafe char တွေကိုသာ encode လုပ်တယ်
function buildDestination(name) {
  const safe = encodeURIComponent(name);
  return `${WEBDAV_URL}/${safe}`;
}

// ── Jobs store ──
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
  setTimeout(() => jobs.delete(id), 2 * 60 * 60 * 1000);
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

// ── Source URL ကို request လုပ်ပြီး stream + size + filename ပြန်ပေး (redirect follow) ──
function openSource(url, depth = 0) {
  return new Promise((resolve, reject) => {
    if (depth > 5) return reject(new Error("Too many redirects"));
    const lib = url.startsWith("https") ? https : http;
    const req = lib.get(
      url,
      {
        agent: agentFor(url),
        headers: { "User-Agent": "Mozilla/5.0", Accept: "*/*" },
      },
      (res) => {
        // redirect
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

        // filename: content-disposition ရှိရင် ဦးစားပေး
        let cdName = "";
        const cd = res.headers["content-disposition"];
        if (cd) {
          const m = /filename\*?=(?:UTF-8'')?["']?([^"';]+)/i.exec(cd);
          if (m) { try { cdName = decodeURIComponent(m[1]); } catch { cdName = m[1]; } }
        }
        resolve({ stream: res, total, cdName });
      }
    );
    req.on("error", reject);
    req.setTimeout(DL_TIMEOUT, () => req.destroy(new Error("Download timeout")));
  });
}

// ── Streaming PUT: source stream ကို WebDAV ဆီ တိုက်ရိုက် pipe ──
// (disk မဖြတ်ဘူး — server မှာ နေရာမယူဘူး၊ ပိုမြန်တယ်)
function streamPut(destination, stream, total, onProgress) {
  return new Promise((resolve, reject) => {
    const lib = destination.startsWith("https") ? https : http;
    const u = new URL(destination);

    const headers = {
      Authorization: authHeader(),
      "Content-Type": "application/octet-stream",
    };
    // size သိရင် Content-Length ထည့် (မသိရင် chunked transfer သုံးမယ်)
    if (total > 0) headers["Content-Length"] = String(total);

    const req = lib.request(
      {
        method: "PUT",
        hostname: u.hostname,
        port: u.port || (destination.startsWith("https") ? 443 : 80),
        path: u.pathname + u.search,
        headers,
        agent: agentFor(destination),
      },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c.toString().slice(0, 500)));
        res.on("end", () => {
          if ([200, 201, 204].includes(res.statusCode)) {
            resolve({ status: res.statusCode });
          } else {
            reject(
              new Error(`Upload fail HTTP ${res.statusCode} ${body.slice(0, 200)}`)
            );
          }
        });
      }
    );

    req.on("error", reject);
    req.setTimeout(0); // upload အကြာကြီး timeout မဖြစ်စေဖို့

    let uploaded = 0;
    stream.on("data", (chunk) => {
      uploaded += chunk.length;
      if (onProgress) onProgress(uploaded, total);
    });
    stream.on("error", (e) => req.destroy(e));

    stream.pipe(req);
  });
}

// ── MKCOL မလို — root /dav ဆီ တိုက်ရိုက် PUT ──
// retry အတွက် source ကို ပြန်ဖွင့်ရတယ် (stream က once-only ဖြစ်လို့)
async function uploadWithRetry(url, destination, job) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
    try {
      const { stream, total, cdName } = await openSource(url);
      job.serverFileName = cdName; // optional
      if (total) emit(job, { total });

      const r = await streamPut(destination, stream, total, (uploaded, t) => {
        const pct = t ? Math.min(99, Math.floor((uploaded / t) * 100)) : 0;
        emit(job, { loaded: uploaded, total: t, percent: pct });
      });
      return r;
    } catch (e) {
      lastErr = e;
      const msg = String(e.message || "");
      // server busy (502/503/504) ဖြစ်ရင် ပိုစောင့်ပြီး retry
      const busy = /HTTP 50[234]/.test(msg);
      console.warn(`[upload] attempt ${attempt}/${MAX_RETRY} failed: ${msg}`);
      if (attempt < MAX_RETRY) {
        await sleep((busy ? 4000 : 2000) * attempt);
        emit(job, { status: "retrying", percent: 0, loaded: 0 });
      }
    }
  }
  throw lastErr;
}

// ── Main runner ──
async function runUpload(job, url, filename) {
  try {
    const name = getFileName(url, filename);
    const destination = buildDestination(name);

    emit(job, { status: "uploading", percent: 0 });
    await uploadWithRetry(url, destination, job);

    emit(job, {
      status: "done",
      percent: 100,
      result: {
        destination,
        name,
        message: "အောင်မြင်စွာ တင်ပြီးပါပြီ ✅",
      },
    });
  } catch (err) {
    console.error("[runUpload]", err);
    emit(job, { status: "error", error: err.message || "တင်မရပါ" });
  } finally {
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
    return res.status(500).json({ error: "WEBDAV_USER / WEBDAV_PASS မပြည့်စုံပါ" });
  }
  const job = newJob();
  runUpload(job, url, filename).catch(() => {});
  res.json({ jobId: job.id });
});

app.get("/api/progress/:id", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).end();

  res.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });
  res.flushHeaders?.();

  res.write(`data: ${JSON.stringify({
    status: job.status, loaded: job.loaded, total: job.total,
    percent: job.percent, result: job.result, error: job.error,
  })}\n\n`);

  if (job.status === "done" || job.status === "error") return res.end();

  job.listeners.add(res);
  req.on("close", () => job.listeners.delete(res));
});

// SSE keep-alive
setInterval(() => {
  for (const job of jobs.values()) {
    for (const res of job.listeners) {
      try { res.write(`: ping\n\n`); } catch (_) {}
    }
  }
}, 15000).unref();

// ── Simple UI (public folder မရှိရင်လည်း သုံးလို့ရအောင်) ──
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
<h2>DogPan Remote Uploader</h2>
<p>File link ထည့်ပါ — server က download ပြီး dogpan WebDAV ဆီ auto တင်ပေးပါမယ်။</p>
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
      let line=j.status;
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
app.listen(PORT, () => console.log(`🚀 DogPan Uploader on port ${PORT}`));
