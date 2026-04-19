import express from "express";
import dotenv from "dotenv";
import crypto from "crypto";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

const publicDir = path.join(__dirname, "public");
app.use("/registro", express.static(path.join(publicDir, "registro")));
app.use("/facturapi", express.static(path.join(publicDir, "facturapi")));

const FACTURAPI_BASE = "https://www.facturapi.io/v2";
const FACTURAPI_SECRET_KEY = process.env.FACTURAPI_SECRET_KEY;
const PORT = process.env.PORT || 8080;
const DEFAULT_CHECKIN_WEB_APP_URL = process.env.CHECKIN_WEB_APP_URL || "";

if (!FACTURAPI_SECRET_KEY) {
  console.error("Falta FACTURAPI_SECRET_KEY en variables de entorno.");
  process.exit(1);
}

async function facturapiFetch(pathname, options = {}) {
  const res = await fetch(`${FACTURAPI_BASE}${pathname}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${FACTURAPI_SECRET_KEY}`,
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


async function saveReceiptPdfToCheckin_(receiptId, { recordId, rowNumber, externalId, folio, overrideUrl } = {}) {
  if (!receiptId) {
    throw new Error("Falta receiptId para guardar el PDF.");
  }
  if (!recordId && !externalId && !rowNumber) {
    throw new Error("Falta recordId/externalId/rowNumber para asociar el PDF en Check in.");
  }

  const pdfRes = await facturapiFetch(`/receipts/${encodeURIComponent(receiptId)}/pdf`, {
    headers: { Accept: "application/pdf" },
  });

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

app.get("/api/products", async (_req, res) => {
  try {
    const apiRes = await facturapiFetch("/products?limit=100");
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
      whatsapp,
      recordId,
      rowNumber,
      assignedFolio,
      checkinWebAppUrl,
    } = req.body || {};

    if (Number(amount) !== 1) {
      return res.status(400).json({ message: "El monto unitario está fijo en 1 para esta versión." });
    }
    if (!productId) {
      return res.status(400).json({ message: "Debes seleccionar un producto." });
    }
    if (!quantity || Number(quantity) <= 0) {
      return res.status(400).json({ message: "La cantidad debe ser mayor a cero." });
    }

    const productRes = await facturapiFetch(`/products/${encodeURIComponent(productId)}`);
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
      idempotency_key: crypto.randomUUID(),
    };

    const receiptRes = await facturapiFetch("/receipts", {
      method: "POST",
      body: JSON.stringify(payload),
    });

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
          folio_facturapi: facturapiFolio
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
          overrideUrl: resolvedCheckinUrl
        });
      } else {
        pdfDriveSaveError = "Falta recordId/externalId/rowNumber para guardar el PDF en Drive.";
      }
    } catch (savePdfErr) {
      pdfDriveSaveError = String(savePdfErr?.message || savePdfErr);
    }

    return res.json({
      ok: true,
      receipt,
      payloadSent: payload,
      assignedFolio: facturapiFolio,
      facturapiFolio,
      checkinWebAppUrl: resolvedCheckinUrl,
      contact: { email: email || null, whatsapp: whatsapp || null },
      sheetUpdate,
      sheetUpdateError,
      pdfDriveSave,
      pdfDriveSaveError,
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

    if (!receiptId) {
      return res.status(400).json({ message: "Falta receiptId." });
    }
    if (!email) {
      return res.status(400).json({ message: "Falta el correo electrónico." });
    }

    const apiRes = await facturapiFetch(`/receipts/${encodeURIComponent(receiptId)}/email`, {
      method: "POST",
      body: JSON.stringify({ email }),
    });

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
    const pdfRes = await facturapiFetch(`/receipts/${encodeURIComponent(receiptId)}/pdf`, {
      headers: { Accept: "application/pdf" },
    });

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


app.use((req, res) => {
  return res.status(404).json({
    ok: false,
    message: "Ruta no encontrada.",
    path: req.originalUrl
  });
});

app.listen(PORT, () => {
  console.log(`Servidor corriendo en http://localhost:${PORT}`);
  console.log(`Apps Script Check in configurado en: ${DEFAULT_CHECKIN_WEB_APP_URL}`);
});
