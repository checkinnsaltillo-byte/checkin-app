import express from "express";
import dotenv from "dotenv";
import crypto from "crypto";
import path from "path";
import { fileURLToPath } from "url";
import { registerBreezewayRoutes } from "./breezeway.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

// ─── CORS para llamadas desde Ticket Vision (GitHub Pages / www.check-inn.mx) ───
// La UI vive en otro origen y necesita consumir /api/breezeway/* y otros
// endpoints AJAX. Middleware mínimo sin dependencia externa.
const CORS_ALLOWED_ORIGINS = new Set([
  "https://www.check-inn.mx",
  "https://check-inn.mx",
  "https://checkinnsaltillo-byte.github.io",
  "http://localhost:3000",
  "http://localhost:5173",
  "http://localhost:8080",
]);
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && CORS_ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
    res.setHeader("Access-Control-Allow-Credentials", "true");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Breezeway-Token");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Max-Age", "600");
  }
  if (req.method === "OPTIONS") return res.status(204).end();
  next();
});

const publicDir = path.join(__dirname, "public");
app.use("/registro", express.static(path.join(publicDir, "registro")));
app.use("/facturapi", express.static(path.join(publicDir, "facturapi")));

const FACTURAPI_BASE = "https://www.facturapi.io/v2";
// Llaves por organización. Configurar en Cloud Run:
//   FACTURAPI_SECRET_KEY_ORG1, FACTURAPI_SECRET_KEY_ORG2
// Si solo está la vieja FACTURAPI_SECRET_KEY, se usa como ORG1 (back-compat).
// El frontend manda ?org=1|2; aquí elegimos qué secret usar.
const FACTURAPI_SECRET_KEY_ORG1 = process.env.FACTURAPI_SECRET_KEY_ORG1
  || process.env.FACTURAPI_SECRET_KEY
  || "";
const FACTURAPI_SECRET_KEY_ORG2 = process.env.FACTURAPI_SECRET_KEY_ORG2 || "";
const FACTURAPI_DEFAULT_ORG = String(process.env.FACTURAPI_DEFAULT_ORG || "2");
const PORT = process.env.PORT || 8080;
const DEFAULT_CHECKIN_WEB_APP_URL = process.env.CHECKIN_WEB_APP_URL || "";

if (!FACTURAPI_SECRET_KEY_ORG1 && !FACTURAPI_SECRET_KEY_ORG2) {
  console.warn("⚠️ Ninguna FACTURAPI_SECRET_KEY_ORG* configurada. Facturapi no funcionará.");
} else {
  console.info(`[facturapi] ORG1=${FACTURAPI_SECRET_KEY_ORG1 ? "✓" : "✗"}, ORG2=${FACTURAPI_SECRET_KEY_ORG2 ? "✓" : "✗"}, default=${FACTURAPI_DEFAULT_ORG}`);
}

function resolveFacturapiSecret(org) {
  const o = (org === "1" || org === 1) ? "1"
          : (org === "2" || org === 2) ? "2"
          : FACTURAPI_DEFAULT_ORG;
  if (o === "1" && FACTURAPI_SECRET_KEY_ORG1) return FACTURAPI_SECRET_KEY_ORG1;
  if (o === "2" && FACTURAPI_SECRET_KEY_ORG2) return FACTURAPI_SECRET_KEY_ORG2;
  // Fallback si la org pedida no está configurada.
  return FACTURAPI_SECRET_KEY_ORG2 || FACTURAPI_SECRET_KEY_ORG1 || "";
}

function readOrgFromReq(req) {
  const raw = (req && (req.query?.org || req.body?.org)) || "";
  const s = String(raw).trim();
  return (s === "1" || s === "2") ? s : FACTURAPI_DEFAULT_ORG;
}

async function facturapiFetch(pathname, options = {}, org) {
  const secret = resolveFacturapiSecret(org);
  const res = await fetch(`${FACTURAPI_BASE}${pathname}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${secret}`,
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });
  return res;
}

async function parseFacturapiResponse(res) {
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return await res.json();
  }
  const text = await res.text();
  return { message: text };
}

function round2(value) {
  return Math.round((Number(value) + Number.EPSILON) * 100) / 100;
}

function resolveVisibleFacturapiFolio(receipt) {
  const candidates = [
    receipt?.folio_number,
    receipt?.folioNumber,
    receipt?.folio,
    receipt?.number
  ];
  for (const c of candidates) {
    if (c != null && String(c).trim() !== "") return String(c).trim();
  }
  return "";
}



function resolveCheckinWebAppUrl(overrideUrl) {
  const candidate = String(overrideUrl || "").trim();
  if (candidate && /^https:\/\//i.test(candidate)) return candidate;
  const envUrl = String(DEFAULT_CHECKIN_WEB_APP_URL || "").trim();
  if (envUrl && /^https:\/\//i.test(envUrl)) return envUrl;
  throw new Error("Falta configurar CHECKIN_WEB_APP_URL.");
}

async function getNextFacturapiFolioFromSheet_(overrideUrl) {
  const checkinUrl = resolveCheckinWebAppUrl(overrideUrl);
  const url = `${checkinUrl}?action=get_next_facturapi_folio`;
  const data = await checkinFetchJson(url);
  return {
    ok: true,
    checkinWebAppUrl: checkinUrl,
    max_folio: Number(data?.max_folio || 0),
    next_folio: Number(data?.next_folio || 1)
  };
}


async function checkinFetchJson(url, options = {}) {
  const res = await fetch(url, options);
  const text = await res.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_err) {
    data = { message: text || `Respuesta no JSON (${res.status})` };
  }
  if (!res.ok) {
    const err = new Error(data?.error || data?.message || `Error ${res.status}`);
    err.status = res.status;
    err.payload = data;
    throw err;
  }
  return data;
}


async function saveFacturapiFolioStrict_(recordId, folio, externalId, overrideUrl) {
  if (!recordId && !externalId) {
    throw new Error("Falta recordId/externalId para guardar el folio.");
  }
  if (!folio) {
    throw new Error("Falta folio para guardar.");
  }
  const payload = {
    action: "update_facturapi_folio_strict",
    record_id: recordId || "",
    folio_facturapi: folio,
    external_id: externalId || ""
  };
  const checkinUrl = resolveCheckinWebAppUrl(overrideUrl);
  const data = await checkinFetchJson(checkinUrl, {
    method: "POST",
    headers: { "Content-Type": "text/plain;charset=utf-8" },
    body: JSON.stringify(payload)
  });
  return { ...data, checkinWebAppUrl: checkinUrl };
}


async function saveReceiptPdfToCheckin_(receiptId, { recordId, rowNumber, externalId, folio, overrideUrl, org } = {}) {
  if (!receiptId) {
    throw new Error("Falta receiptId para guardar el PDF.");
  }
  if (!recordId && !externalId && !rowNumber) {
    throw new Error("Falta recordId/externalId/rowNumber para asociar el PDF en Check in.");
  }

  const pdfRes = await facturapiFetch(`/receipts/${encodeURIComponent(receiptId)}/pdf`, {
    headers: { Accept: "application/pdf" },
  }, org);

  if (!pdfRes.ok) {
    const contentType = pdfRes.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      const err = await pdfRes.json();
      throw new Error(err?.message || "Facturapi no devolvió el PDF.");
    }
    const text = await pdfRes.text();
    throw new Error(text || "Facturapi no devolvió el PDF.");
  }

  const pdfBuffer = Buffer.from(await pdfRes.arrayBuffer());
  const base64 = pdfBuffer.toString("base64");
  const checkinUrl = resolveCheckinWebAppUrl(overrideUrl);

  const payload = {
    action: "save_facturapi_pdf",
    record_id: recordId || "",
    row_number: rowNumber || "",
    external_id: externalId || "",
    receipt_id: receiptId,
    folio_facturapi: folio || "",
    file: {
      fileName: `ticket-${folio || receiptId}.pdf`,
      mimeType: "application/pdf",
      base64
    }
  };

  const data = await checkinFetchJson(checkinUrl, {
    method: "POST",
    headers: { "Content-Type": "text/plain;charset=utf-8" },
    body: JSON.stringify(payload)
  });

  return { ...data, checkinWebAppUrl: checkinUrl };
}

app.get("/api/checkin-config", (req, res) => {
  const checkinWebAppUrl = resolveCheckinWebAppUrl(req.query?.checkinWebAppUrl);
  return res.json({ ok: true, checkinWebAppUrl });
});

app.get("/health", (_req, res) => {
  return res.json({ ok: true, service: "checkin-unificado" });
});

app.get("/", (_req, res) => {
  return res.redirect("/registro/");
});

app.get("/facturapi", (_req, res) => {
  return res.redirect("/facturapi/");
});

app.get("/registro", (_req, res) => {
  return res.redirect("/registro/");
});

app.get("/api/products", async (req, res) => {
  try {
    const org = readOrgFromReq(req);
    const apiRes = await facturapiFetch("/products?limit=100", {}, org);
    const data = await parseFacturapiResponse(apiRes);

    if (!apiRes.ok) {
      return res.status(apiRes.status).json({
        message: data?.message || "Error al consultar productos en Facturapi.",
        facturapi: data,
      });
    }

    return res.json(data);
  } catch (error) {
    return res.status(500).json({
      message: "Error al consultar productos en Facturapi.",
      error: String(error?.message || error),
    });
  }
});

app.post("/api/create-receipt", async (req, res) => {
  try {
    const {
      amount = 1,
      productId,
      currency = "MXN",
      exchange = 1,
      paymentForm = "03",
      branch,
      externalId,
      includeTaxes = true,
      quantity = 1,
      email,
      taxRegime,
      whatsapp,
      recordId,
      rowNumber,
      assignedFolio,
      checkinWebAppUrl,
    } = req.body || {};
    const org = readOrgFromReq(req);

    if (Number(amount) !== 1) {
      return res.status(400).json({ message: "El monto unitario está fijo en 1 para esta versión." });
    }
    if (!productId) {
      return res.status(400).json({ message: "Debes seleccionar un producto." });
    }
    if (!quantity || Number(quantity) <= 0) {
      return res.status(400).json({ message: "La cantidad debe ser mayor a cero." });
    }

    const productRes = await facturapiFetch(`/products/${encodeURIComponent(productId)}`, {}, org);
    const product = await parseFacturapiResponse(productRes);

    if (!productRes.ok) {
      return res.status(productRes.status).json({
        message: product?.message || "No fue posible consultar el producto.",
        facturapi: product,
      });
    }

    const payload = {
      items: [
        {
          quantity: Number(quantity),
          discount: 0,
          product: {
            description: product.description,
            product_key: product.product_key,
            price: round2(Number(amount)),
            tax_included: Boolean(includeTaxes),
            taxability: product.taxability,
            taxes: Array.isArray(product.taxes) ? product.taxes : [],
            local_taxes: Array.isArray(product.local_taxes) ? product.local_taxes : [],
            unit_key: product.unit_key,
            unit_name: product.unit_name,
            sku: product.sku,
          },
        },
      ],
      payment_form: paymentForm,
      currency,
      exchange: Number(exchange || 1),
      ...(branch ? { branch } : {}),
      ...(externalId ? { external_id: externalId } : {}),
      // Folio asignado por el cliente = MAX("Folio facturapi" en Reservaciones)+1.
      // Facturapi acepta folio_number en /receipts para forzar el folio.
      ...(assignedFolio != null && String(assignedFolio).trim() !== "" && Number(assignedFolio) > 0
          ? { folio_number: Number(assignedFolio) }
          : {}),
      idempotency_key: crypto.randomUUID(),
    };

    const receiptRes = await facturapiFetch("/receipts", {
      method: "POST",
      body: JSON.stringify(payload),
    }, org);

    const receipt = await parseFacturapiResponse(receiptRes);

    if (!receiptRes.ok) {
      return res.status(receiptRes.status).json({
        message: receipt?.message || "No fue posible generar el ticket.",
        facturapi: receipt,
      });
    }

    const resolvedCheckinUrl = resolveCheckinWebAppUrl(checkinWebAppUrl);
    const facturapiFolio = resolveVisibleFacturapiFolio(receipt);

    let sheetUpdate = null;
    let sheetUpdateError = null;
    try {
      if ((recordId || externalId || rowNumber) && facturapiFolio) {
        const strictPayload = {
          record_id: recordId || "",
          row_number: rowNumber || "",
          external_id: externalId || "",
          folio_facturapi: facturapiFolio,
          monto_facturado: Number(quantity) || "",   // ← se persiste en "$ Monto facturado Total"
          org: org || "",                            // ← Apps Script lo mapea a "ACR"/"ACL"
        };
        const data = await checkinFetchJson(resolvedCheckinUrl, {
          method: "POST",
          headers: { "Content-Type": "text/plain;charset=utf-8" },
          body: JSON.stringify({ action: "update_facturapi_folio_strict", ...strictPayload })
        });
        if (!data?.ok) {
          throw new Error(data?.error || data?.message || "No se pudo guardar el Folio Facturapi en Check in.");
        }
        if (String(data?.written_value || "").trim() !== facturapiFolio) {
          throw new Error(`La hoja escribió "${data?.written_value || ""}" en vez de "${facturapiFolio}".`);
        }
        sheetUpdate = { ...data, checkinWebAppUrl: resolvedCheckinUrl };
      } else {
        sheetUpdateError = "Falta recordId/externalId/rowNumber o Folio Facturapi para guardar en Check in.";
      }
    } catch (saveErr) {
      sheetUpdateError = String(saveErr?.message || saveErr);
    }

    let pdfDriveSave = null;
    let pdfDriveSaveError = null;
    try {
      if (recordId || externalId || rowNumber) {
        pdfDriveSave = await saveReceiptPdfToCheckin_(receipt.id, {
          recordId,
          rowNumber,
          externalId,
          folio: facturapiFolio,
          overrideUrl: resolvedCheckinUrl,
          org,
        });
      } else {
        pdfDriveSaveError = "Falta recordId/externalId/rowNumber para guardar el PDF en Drive.";
      }
    } catch (savePdfErr) {
      pdfDriveSaveError = String(savePdfErr?.message || savePdfErr);
    }

    // Persistir el monto facturado total (= Cantidad del form, que viene del
    // campo "(+) $ Monto facturado Total" de la card de Control de huéspedes)
    // en la columna "$ Monto facturado Total" del sheet Reservaciones.
    let montoFacturadoSave = null;
    let montoFacturadoSaveError = null;
    try {
      const idForSave = recordId || (externalId ? String(externalId).replace(/^CHECKIN-/, "").trim() : "");
      if (idForSave && quantity != null && Number(quantity) > 0) {
        const data = await checkinFetchJson(resolvedCheckinUrl, {
          method: "POST",
          headers: { "Content-Type": "text/plain;charset=utf-8" },
          body: JSON.stringify({
            action: "update_facturado_total",
            record_id: idForSave,
            monto_facturado_total: String(quantity)
          })
        });
        if (!data?.ok) {
          throw new Error(data?.error || data?.message || "No se pudo guardar el monto facturado total.");
        }
        montoFacturadoSave = { ...data, checkinWebAppUrl: resolvedCheckinUrl };
      } else if (!idForSave) {
        montoFacturadoSaveError = "Falta record_id/externalId para guardar el Monto Facturado Total.";
      }
    } catch (saveMontoErr) {
      montoFacturadoSaveError = String(saveMontoErr?.message || saveMontoErr);
    }

    return res.json({
      ok: true,
      receipt,
      payloadSent: payload,
      assignedFolio: facturapiFolio,
      facturapiFolio,
      checkinWebAppUrl: resolvedCheckinUrl,
      contact: { email: email || null, taxRegime: taxRegime || null, whatsapp: whatsapp || null },
      sheetUpdate,
      sheetUpdateError,
      pdfDriveSave,
      pdfDriveSaveError,
      montoFacturadoSave,
      montoFacturadoSaveError,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Error al crear el ticket.",
      error: String(error?.message || error),
    });
  }
});

app.post("/api/send-receipt-email", async (req, res) => {
  try {
    const { receiptId, email } = req.body || {};
    const org = readOrgFromReq(req);

    if (!receiptId) {
      return res.status(400).json({ message: "Falta receiptId." });
    }
    if (!email) {
      return res.status(400).json({ message: "Falta el correo electrónico." });
    }

    const apiRes = await facturapiFetch(`/receipts/${encodeURIComponent(receiptId)}/email`, {
      method: "POST",
      body: JSON.stringify({ email }),
    }, org);

    const data = await parseFacturapiResponse(apiRes);

    if (!apiRes.ok) {
      return res.status(apiRes.status).json({
        message: data?.message || "Facturapi no pudo enviar el correo.",
        facturapi: data,
      });
    }

    return res.json({
      ok: true,
      message: `Correo enviado correctamente por Facturapi a ${email}.`,
      facturapi: data,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Error al enviar el correo por Facturapi.",
      error: String(error?.message || error),
    });
  }
});

app.get("/api/receipt-pdf/:receiptId", async (req, res) => {
  try {
    const { receiptId } = req.params;
    const org = readOrgFromReq(req);
    const pdfRes = await facturapiFetch(`/receipts/${encodeURIComponent(receiptId)}/pdf`, {
      headers: { Accept: "application/pdf" },
    }, org);

    if (!pdfRes.ok) {
      const contentType = pdfRes.headers.get("content-type") || "";
      if (contentType.includes("application/json")) {
        const err = await pdfRes.json();
        return res.status(pdfRes.status).json(err);
      }
      const text = await pdfRes.text();
      return res.status(pdfRes.status).send(text);
    }

    const pdfBuffer = Buffer.from(await pdfRes.arrayBuffer());
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename="ticket-${receiptId}.pdf"`);
    return res.send(pdfBuffer);
  } catch (error) {
    return res.status(500).json({
      message: "Error al obtener PDF.",
      error: String(error?.message || error),
    });
  }
});


app.get("/api/next-facturapi-folio", async (req, res) => {
  try {
    const data = await getNextFacturapiFolioFromSheet_(req.query?.checkinWebAppUrl);
    return res.json(data);
  } catch (error) {
    return res.status(error.status || 500).json({
      message: "No fue posible obtener el siguiente folio.",
      error: String(error?.message || error),
      details: error.payload || null
    });
  }
});

app.post("/api/save-facturapi-folio", async (req, res) => {
  try {
    const { recordId, folio, externalId, checkinWebAppUrl } = req.body || {};
    if (!recordId && !externalId) return res.status(400).json({ message: "Falta recordId o externalId." });
    if (!folio) return res.status(400).json({ message: "Falta folio." });

    const data = await saveFacturapiFolioStrict_(recordId, folio, externalId, checkinWebAppUrl);
    return res.json({ ok: true, ...data });
  } catch (error) {
    return res.status(error.status || 500).json({
      message: "No fue posible guardar el folio en la hoja.",
      error: String(error?.message || error),
      details: error.payload || null
    });
  }
});


// Rutas de integración Breezeway (alertas de aseo / estado de alojamientos)
registerBreezewayRoutes(app);

app.use((req, res) => {
  return res.status(404).json({
    ok: false,
    message: "Ruta no encontrada.",
    path: req.originalUrl
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Servidor corriendo en http://0.0.0.0:${PORT}`);
});
