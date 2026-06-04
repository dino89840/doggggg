import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import { Readable } from "stream";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// ===== WebDAV (dogpan) connection details =====
// Railway Variables မှာ ဒီတန်ဖိုးတွေ ထည့်ပါ (ကုဒ်ထဲ hardcode မလုပ်ပါနဲ့)
const WEBDAV_URL = process.env.WEBDAV_URL || "https://dogpan.com/dav";
const WEBDAV_USER = process.env.WEBDAV_USER || "";
const WEBDAV_PASS = process.env.WEBDAV_PASS || "";

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Basic Auth header ဆောက်ဖို့
function authHeader() {
  const token = Buffer.from(`${WEBDAV_USER}:${WEBDAV_PASS}`).toString("base64");
  return "Basic " + token;
}

// URL ကနေ filename ထုတ်ဖို့
function getFileName(url, fallback) {
  try {
    const u = new URL(url);
    const name = decodeURIComponent(u.pathname.split("/").pop());
    if (name && name.includes(".")) return name;
  } catch {}
  return fallback || `file_${Date.now()}`;
}

// ===== Remote transfer endpoint =====
app.post("/api/transfer", async (req, res) => {
  const { url, filename } = req.body;

  if (!url) {
    return res.status(400).json({ ok: false, error: "URL လိုအပ်ပါတယ်" });
  }
  if (!WEBDAV_USER || !WEBDAV_PASS) {
    return res
      .status(500)
      .json({ ok: false, error: "WEBDAV_USER / WEBDAV_PASS env မထည့်ရသေးပါ" });
  }

  try {
    // 1) Source URL ကနေ download stream စဖွင့်
    const sourceRes = await fetch(url, {
      headers: {
        // ချို့ site တွေက User-Agent လိုတယ်
        "User-Agent": "Mozilla/5.0 (RemoteUploader)",
      },
    });

    if (!sourceRes.ok) {
      return res.status(502).json({
        ok: false,
        error: `Source download မအောင်မြင်ပါ (HTTP ${sourceRes.status})`,
      });
    }

    const name = getFileName(url, filename);
    const target = `${WEBDAV_URL.replace(/\/$/, "")}/${encodeURIComponent(name)}`;
    const contentType =
      sourceRes.headers.get("content-type") || "application/octet-stream";
    const contentLength = sourceRes.headers.get("content-length");

    // 2) WebDAV ဆီကို PUT နဲ့ stream ထည့် (ဆာဗာချင်း တိုက်ရိုက်ကူး)
    const putHeaders = {
      Authorization: authHeader(),
      "Content-Type": contentType,
    };
    if (contentLength) putHeaders["Content-Length"] = contentLength;

    const uploadRes = await fetch(target, {
      method: "PUT",
      headers: putHeaders,
      body: sourceRes.body, // stream တိုက်ရိုက်ပို့
      duplex: "half", // Node fetch streaming အတွက် မဖြစ်မနေလိုအပ်
    });

    if (uploadRes.ok || uploadRes.status === 201 || uploadRes.status === 204) {
      return res.json({
        ok: true,
        filename: name,
        url: target,
        message: "အောင်မြင်စွာ တင်ပြီးပါပြီ",
      });
    } else {
      const text = await uploadRes.text().catch(() => "");
      return res.status(502).json({
        ok: false,
        error: `WebDAV upload မအောင်မြင်ပါ (HTTP ${uploadRes.status}) ${text}`,
      });
    }
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Health check
app.get("/health", (req, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
