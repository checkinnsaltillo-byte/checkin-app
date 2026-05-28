// ============================================================================
// Check-In backend (Ticket Vision · esquema normalizado v1)
// ----------------------------------------------------------------------------
// Hojas que mantiene este script:
//   1) Perfiles      — 1 fila por celular (datos personales/fiscales/identif.)
//   2) Vehiculos     — 1 fila por celular (datos del vehículo actual)
//   3) Reservaciones — N filas por celular (una por estancia)
//
// Para migrar desde la hoja única "Check in" legada:
//   - Ejecuta UNA SOLA VEZ la función migrateToNormalizedSchema()
//   - El script crea las 3 hojas, mueve los datos y renombra "Check in"
//     a "Check in (archivo)" como respaldo intacto.
// ============================================================================

const SPREADSHEET_ID = "1f_rdwQncSUXRNEvp5kM_kjyX1S-NnY7UE2z4HGvFL3Q";
const DRIVE_FOLDER_ID = "1DLwFUP8oQnv1AuMwI7Oqc6iFPiRgYWZv";
const PUBLIC_SHARING_WITH_LINK = true;
const MAX_BASE64_CHARS = 25 * 1024 * 1024;
const FOLDER_CACHE_ = {};

const PERFILES_SHEET = "Perfiles";
const VEHICULOS_SHEET = "Vehiculos";
const RESERVACIONES_SHEET = "Reservaciones";
const LEGACY_SHEET = "Check in";
const LEGACY_ARCHIVE = "Check in (archivo)";

const PERFILES_HEADERS = [
  "ID_Perfil","Cel/Whatsapp (principal)","Lada celular huésped","Nombre del huésped",
  "Lada contacto emergencia","Cel/Whatsapp (contacto de emergencia)",
  "Tipo de identificación","Identificación otro",
  "INE frontal","Link INE frontal","ID archivo INE frontal","Nombre archivo INE frontal",
  "INE trasero","Link INE trasero","ID archivo INE trasero","Nombre archivo INE trasero",
  "Identificación única","Link identificación única","ID archivo identificación única","Nombre archivo identificación",
  "¿Requiere factura?","Razón social","RFC","Régimen fiscal","Régimen otro","Código Postal",
  "Correo electrónico para el envío de la factura",
  "Fecha creación","Fecha actualización"
];

const VEHICULOS_HEADERS = [
  "ID_Vehiculo","Cel/Whatsapp (principal)","¿Cuenta con vehículo?",
  "Marca vehículo","Marca vehículo otro","Modelo vehículo","Modelo vehículo otro",
  "Color vehículo","Placas","Hora habitual de salida",
  "Foto vehículo","Link foto vehículo","ID archivo foto vehículo","Nombre archivo vehículo",
  "Fecha actualización"
];

const RESERVACIONES_HEADERS = [
  "ID","Cel/Whatsapp (principal)","ID_Vehiculo",
  "Marca temporal","MES","Mes correspondiente","Tipo de factura",
  "Medio de reservación","Cuenta",
  "Propiedad","Propiedad otra","# Departamento",
  "Motivo de tu hospedaje","Motivo otro",
  "Fecha de ingreso","Hora estimada de llegada","Fecha de salida","Hora estimada de salida",
  "# Noches","# Huéspedes","Nombres de TODOS los huéspedes (separados por comas)",
  "Nombre de la persona que hizo la reservación",
  "Forma de pago","Divisa monto pagado",
  "Comprobante transferencia","Link comprobante transferencia","ID archivo comprobante transferencia","Nombre archivo comprobante transferencia",
  "Correo electrónico","...enviar copia al siguiente correo:",
  "$ Noches","$ Cuota de limpieza","$ MONTO TOTAL Airbnb","$ Comisión Airbnb","$ Monto antes de impuestos",
  "($) Monto Total pagado","$ Monto facturado Total",
  "Folio facturapi","Folio CFDI","Folio Relación","Folio complemento de pago","Estatus",
  "Fecha de emisión","Concepto Factura","Método de pago",
  "Ticket facturapi url","Ticket facturapi id archivo","Ticket facturapi nombre archivo","Ticket facturapi carpeta url","Ticket facturapi carpeta ruta",
  "Envía tus comentarios","Envía tus comentarios con relación a la factura","Notas","Enviado por"
];

// ─── ENTRY POINTS ────────────────────────────────────────────────────────────

function doPost(e) {
  try {
    const data = JSON.parse((e && e.postData && e.postData.contents) || "{}");
    const action = data.action || "submit_form";
    if (action === "submit_form") return jsonOutput_(saveFormRecord_(data));
    if (action === "save_profile_only") return jsonOutput_(saveProfileOnly_(data));
    if (action === "upload_file") return jsonOutput_(saveDeferredFile_(data));
    if (action === "update_facturado_total") return jsonOutput_(updateFacturadoTotal_(data));
    if (action === "update_facturapi_folio") return jsonOutput_(updateFacturapiFolio_(data));
    if (action === "update_facturapi_folio_strict") return jsonOutput_(updateFacturapiFolioStrict_(data));
    if (action === "save_facturapi_pdf") return jsonOutput_(saveFacturapiPdf_(data));
    return jsonOutput_({ ok: false, error: "Acción no reconocida." });
  } catch (err) {
    return jsonOutput_({ ok: false, error: err.message || String(err) });
  }
}

function doGet(e) {
  try {
    const action = (e && e.parameter && e.parameter.action) || "";
    if (action === "list_records") return jsonOutput_(listGuestRecords_(e.parameter || {}));
    if (action === "list_filter_options") return jsonOutput_(getGuestFilterOptions_());
    if (action === "get_record_detail") return jsonOutput_(getGuestRecordDetail_(e.parameter || {}));
    if (action === "debug_dashboard") return jsonOutput_(debugDashboard_());
    if (action === "get_next_facturapi_folio") return jsonOutput_(getNextFacturapiFolio_());
    return jsonOutput_({ ok: true, message: "Web app activo (normalizado)." });
  } catch (err) {
    return jsonOutput_({ ok: false, error: err.message || String(err) });
  }
}

// ─── SHEET HELPERS ───────────────────────────────────────────────────────────

function getSpreadsheet_() { return SpreadsheetApp.openById(SPREADSHEET_ID); }

function ensureNormalizedSheets_() {
  const ss = getSpreadsheet_();
  ensureSheetWithHeaders_(ss, PERFILES_SHEET, PERFILES_HEADERS);
  ensureSheetWithHeaders_(ss, VEHICULOS_SHEET, VEHICULOS_HEADERS);
  ensureSheetWithHeaders_(ss, RESERVACIONES_SHEET, RESERVACIONES_HEADERS);
}

function ensureSheetWithHeaders_(ss, name, headers) {
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    sh.setFrozenRows(1);
    return sh;
  }
  const lastCol = Math.max(sh.getLastColumn(), 1);
  const current = sh.getRange(1, 1, 1, lastCol).getValues()[0];
  const missing = headers.filter(h => current.indexOf(h) === -1);
  if (missing.length) {
    sh.getRange(1, current.length + 1, 1, missing.length).setValues([missing]);
  }
  return sh;
}

function getSheet_(name) {
  const sh = getSpreadsheet_().getSheetByName(name);
  if (!sh) throw new Error('No existe la hoja "' + name + '". Ejecuta migrateToNormalizedSchema primero.');
  return sh;
}

function getHeaders_(sheet) {
  const lastColumn = sheet.getLastColumn();
  if (lastColumn < 1) throw new Error("La hoja " + sheet.getName() + " no tiene encabezados.");
  return sheet.getRange(1, 1, 1, lastColumn).getValues()[0].map(v => String(v || "").trim());
}

function getAllRows_(sheetName) {
  const sheet = getSheet_(sheetName);
  const headers = getHeaders_(sheet);
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return { sheet, headers, rows: [] };
  const values = sheet.getRange(2, 1, lastRow - 1, headers.length).getDisplayValues();
  const rows = values.map((rowValues, idx) => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = rowValues[i]);
    obj.__row_number = idx + 2;
    return obj;
  });
  return { sheet, headers, rows };
}

function findRowByValue_(sheet, headers, columnName, value) {
  const colIdx = headers.indexOf(columnName);
  if (colIdx < 0) return null;
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;
  const values = sheet.getRange(2, colIdx + 1, lastRow - 1, 1).getDisplayValues();
  const target = String(value || "").trim();
  for (let i = 0; i < values.length; i++) {
    if (String(values[i][0] || "").trim() === target) return i + 2;
  }
  return null;
}

function findRowByPhone_(sheet, headers, phone) {
  const colIdx = headers.indexOf("Cel/Whatsapp (principal)");
  if (colIdx < 0) return null;
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;
  const values = sheet.getRange(2, colIdx + 1, lastRow - 1, 1).getDisplayValues();
  const target = normalizePhone_(phone);
  for (let i = 0; i < values.length; i++) {
    if (normalizePhone_(values[i][0]) === target) return i + 2;
  }
  return null;
}

function findRowByRowNumber_(sheet, rowNumber) {
  const n = Number(rowNumber);
  if (!n || n < 2 || n > sheet.getLastRow()) return null;
  return n;
}

function readRow_(sheet, headers, rowNumber) {
  const values = sheet.getRange(rowNumber, 1, 1, headers.length).getValues()[0];
  const obj = {};
  headers.forEach((h, i) => obj[h] = values[i]);
  obj.__row_number = rowNumber;
  return obj;
}

function writeRow_(sheet, headers, rowNumber, dataMap) {
  const rowValues = headers.map(h => Object.prototype.hasOwnProperty.call(dataMap, h) ? dataMap[h] : "");
  sheet.getRange(rowNumber, 1, 1, headers.length).setValues([rowValues]);
}

function appendRow_(sheet, headers, dataMap) {
  const rowValues = headers.map(h => Object.prototype.hasOwnProperty.call(dataMap, h) ? dataMap[h] : "");
  sheet.appendRow(rowValues);
  return sheet.getLastRow();
}

function setCellByHeader_(sheet, headers, rowNumber, headerName, value) {
  const colIdx = headers.indexOf(headerName);
  if (colIdx < 0) return false;
  sheet.getRange(rowNumber, colIdx + 1).setValue(value);
  return true;
}

// ─── UPSERTS ─────────────────────────────────────────────────────────────────

function upsertPerfil_(data, cel) {
  ensureNormalizedSheets_();
  const sheet = getSheet_(PERFILES_SHEET);
  const headers = getHeaders_(sheet);
  const now = new Date();
  const newMap = buildPerfilFromData_(data, cel);
  const existingRow = findRowByPhone_(sheet, headers, cel);

  if (existingRow) {
    const existing = readRow_(sheet, headers, existingRow);
    const merged = {};
    headers.forEach(h => {
      if (h === "Fecha creación") merged[h] = existing[h] || now;
      else if (h === "Fecha actualización") merged[h] = now;
      else if (h === "ID_Perfil") merged[h] = existing[h] || newMap[h] || Utilities.getUuid();
      else {
        const newVal = newMap[h];
        const oldVal = existing[h];
        merged[h] = (newVal != null && String(newVal).trim() !== "") ? newVal : (oldVal != null ? oldVal : "");
      }
    });
    writeRow_(sheet, headers, existingRow, merged);
    return { id_perfil: merged["ID_Perfil"], row_number: existingRow, created: false };
  }
  const idPerfil = Utilities.getUuid();
  newMap["ID_Perfil"] = idPerfil;
  newMap["Fecha creación"] = now;
  newMap["Fecha actualización"] = now;
  const row = appendRow_(sheet, headers, newMap);
  return { id_perfil: idPerfil, row_number: row, created: true };
}

function upsertVehiculo_(data, cel) {
  ensureNormalizedSheets_();
  const sheet = getSheet_(VEHICULOS_SHEET);
  const headers = getHeaders_(sheet);
  const now = new Date();
  const tieneVeh = normalizeYesNo_(data.tiene_vehiculo);
  const existingRow = findRowByPhone_(sheet, headers, cel);

  if (!tieneVeh && !existingRow) return { id_vehiculo: "", row_number: null, created: false };

  const newMap = buildVehiculoFromData_(data, cel);
  if (existingRow) {
    const existing = readRow_(sheet, headers, existingRow);
    const merged = {};
    headers.forEach(h => {
      if (h === "Fecha actualización") merged[h] = now;
      else if (h === "ID_Vehiculo") merged[h] = existing[h] || newMap[h] || Utilities.getUuid();
      else {
        const newVal = newMap[h];
        const oldVal = existing[h];
        merged[h] = (newVal != null && String(newVal).trim() !== "") ? newVal : (oldVal != null ? oldVal : "");
      }
    });
    writeRow_(sheet, headers, existingRow, merged);
    return { id_vehiculo: merged["ID_Vehiculo"], row_number: existingRow, created: false };
  }
  const idVeh = Utilities.getUuid();
  newMap["ID_Vehiculo"] = idVeh;
  newMap["Fecha actualización"] = now;
  const row = appendRow_(sheet, headers, newMap);
  return { id_vehiculo: idVeh, row_number: row, created: true };
}

function insertReservacion_(data, cel, idVehiculo) {
  ensureNormalizedSheets_();
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  const now = new Date();
  const ingresoDate = parseDateSafe_(data.ingreso) || now;
  const recordId = Utilities.getUuid();

  const m = {};
  m["ID"] = recordId;
  m["Cel/Whatsapp (principal)"] = cel;
  m["ID_Vehiculo"] = idVehiculo || "";
  m["Marca temporal"] = now;
  m["MES"] = Utilities.formatDate(now, Session.getScriptTimeZone(), "MMMM");
  m["Mes correspondiente"] = Utilities.formatDate(ingresoDate, Session.getScriptTimeZone(), "MMMM yyyy");
  m["Tipo de factura"] = normalizeInvoiceType_(data.factura);
  m["Medio de reservación"] = safe_(data.medio);
  m["Cuenta"] = "";
  m["Propiedad"] = resolveOtherValue_(data.propiedad, data.propiedad_otra, ["Otra","Other"]);
  m["Propiedad otra"] = safe_(data.propiedad_otra);
  m["# Departamento"] = safe_(data.depto);
  m["Motivo de tu hospedaje"] = resolveOtherValue_(data.motivo, data.motivo_otro, ["Otro","Other"]);
  m["Motivo otro"] = safe_(data.motivo_otro);
  m["Fecha de ingreso"] = safe_(data.ingreso);
  m["Hora estimada de llegada"] = safe_(data.hora_llegada_estimada);
  m["Fecha de salida"] = safe_(data.salida);
  m["Hora estimada de salida"] = safe_(data.hora_salida_estimada);
  m["# Noches"] = calculateNights_(data.ingreso, data.salida);
  m["# Huéspedes"] = safe_(data.num_huespedes);
  m["Nombres de TODOS los huéspedes (separados por comas)"] = arrayToCsv_(data.huespedes);
  m["Nombre de la persona que hizo la reservación"] = safe_(data.nombre);
  m["Forma de pago"] = safe_(data.medio_pago);
  m["Divisa monto pagado"] = safe_(data.divisa_monto);
  m["Correo electrónico"] = safe_(data.correo1);
  m["...enviar copia al siguiente correo:"] = safe_(data.correo2);
  m["($) Monto Total pagado"] = safe_(data.monto_pagado);
  m["Envía tus comentarios"] = safe_(data.comentarios);
  m["Envía tus comentarios con relación a la factura"] = safe_(data.comentarios_factura);

  const rowNumber = appendRow_(sheet, headers, m);
  return { row_number: rowNumber, record_id: recordId };
}

function buildPerfilFromData_(data, cel) {
  const firstGuest = firstGuestName_(data.huespedes);
  return {
    "Cel/Whatsapp (principal)": cel,
    "Nombre del huésped": firstGuest || safe_(data.nombre_huesped) || safe_(data.nombre),
    "Cel/Whatsapp (contacto de emergencia)": safe_(data.celular_emergencia),
    "Tipo de identificación": resolveOtherValue_(data.identificacion_tipo, data.identificacion_otro, ["Otro","Other"]),
    "Identificación otro": safe_(data.identificacion_otro),
    "¿Requiere factura?": normalizeYesNo_(data.factura),
    "Razón social": safe_(data.razon_social),
    "RFC": safe_(data.rfc),
    "Régimen fiscal": resolveOtherValue_(data.regimen, data.regimen_otro, ["Otro","Other"]),
    "Régimen otro": safe_(data.regimen_otro),
    "Código Postal": safe_(data.codigo_postal),
    "Correo electrónico para el envío de la factura": safe_(data.correo1)
  };
}

function buildVehiculoFromData_(data, cel) {
  return {
    "Cel/Whatsapp (principal)": cel,
    "¿Cuenta con vehículo?": normalizeYesNo_(data.tiene_vehiculo),
    "Marca vehículo": resolveOtherValue_(data.vehiculo_marca, data.vehiculo_marca_otro, ["Otro","Other"]),
    "Marca vehículo otro": safe_(data.vehiculo_marca_otro),
    "Modelo vehículo": safe_(data.vehiculo_modelo),
    "Modelo vehículo otro": safe_(data.vehiculo_modelo_otro),
    "Color vehículo": safe_(data.vehiculo_color),
    "Placas": safe_(data.vehiculo_placas),
    "Hora habitual de salida": safe_(data.vehiculo_hora_salida)
  };
}

function firstGuestName_(value) {
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      const v = String(value[i] || "").trim();
      if (v) return v;
    }
    return "";
  }
  return safe_(value).split(",")[0].trim();
}

// ─── SAVE PROFILE ONLY (desde el wizard, sin reservación) ───────────────────

function saveProfileOnly_(data) {
  ensureNormalizedSheets_();
  const cel = safe_(data.celular_principal).trim();
  if (!cel) throw new Error("Falta celular_principal.");
  const perfilResult = upsertPerfil_(data, cel);
  const vehiculoResult = upsertVehiculo_(data, cel);
  return {
    ok: true,
    id_perfil: perfilResult.id_perfil,
    id_vehiculo: vehiculoResult.id_vehiculo,
    perfil_created: perfilResult.created,
    vehiculo_created: vehiculoResult.created,
    celular: cel
  };
}

// ─── SUBMIT FORM ────────────────────────────────────────────────────────────

function saveFormRecord_(data) {
  ensureNormalizedSheets_();
  const cel = safe_(data.celular_principal).trim();
  if (!cel) throw new Error("Falta celular_principal.");
  const perfilResult = upsertPerfil_(data, cel);
  const vehiculoResult = upsertVehiculo_(data, cel);
  const reservacionResult = insertReservacion_(data, cel, vehiculoResult.id_vehiculo);
  return {
    ok: true,
    record_id: reservacionResult.record_id,
    row_number: reservacionResult.row_number,
    id_perfil: perfilResult.id_perfil,
    id_vehiculo: vehiculoResult.id_vehiculo,
    perfil_created: perfilResult.created,
    vehiculo_created: vehiculoResult.created
  };
}

// ─── UPLOADS ────────────────────────────────────────────────────────────────

function saveDeferredFile_(data) {
  const recordId = safe_(data.record_id);
  const fieldName = safe_(data.field_name);
  const fileObj = data.file;
  if (!recordId) throw new Error("Falta record_id.");
  if (!fieldName) throw new Error("Falta field_name.");
  if (!fileObj || !fileObj.base64) throw new Error("No se recibió archivo.");

  const resSheet = getSheet_(RESERVACIONES_SHEET);
  const resHeaders = getHeaders_(resSheet);
  const resRowNum = findRowByValue_(resSheet, resHeaders, "ID", recordId);
  if (!resRowNum) throw new Error("No se encontró la reservación.");
  const resRow = readRow_(resSheet, resHeaders, resRowNum);
  const cel = resRow["Cel/Whatsapp (principal)"];
  if (!cel) throw new Error("La reservación no tiene celular asociado.");

  const ingreso = parseDateSafe_(resRow["Fecha de ingreso"]) || new Date();
  const year = Utilities.formatDate(ingreso, Session.getScriptTimeZone(), "yyyy");
  const month = Utilities.formatDate(ingreso, Session.getScriptTimeZone(), "MM");
  const propiedad = cleanFolderName_(resRow["Propiedad"] || "Sin propiedad");
  const guestName = cleanFolderName_(resRow["Nombre de la persona que hizo la reservación"] || "Sin nombre");
  const subfolders = [year, month, propiedad, guestName];

  const fileInfo = saveBase64FileToDrive_(fileObj, fieldName, subfolders);

  switch (fieldName) {
    case "ine_frontal":            updateProfileFile_(cel, "INE frontal", fileInfo); break;
    case "ine_trasero":            updateProfileFile_(cel, "INE trasero", fileInfo); break;
    case "identificacion_unica":   updateProfileFile_(cel, "Identificación única", fileInfo); break;
    case "vehiculo_foto":          updateVehicleFile_(cel, fileInfo); break;
    case "comprobante_transferencia":
      setCellByHeader_(resSheet, resHeaders, resRowNum, "Comprobante transferencia", fileInfo.url);
      setCellByHeader_(resSheet, resHeaders, resRowNum, "Link comprobante transferencia", fileInfo.url);
      setCellByHeader_(resSheet, resHeaders, resRowNum, "ID archivo comprobante transferencia", fileInfo.id);
      setCellByHeader_(resSheet, resHeaders, resRowNum, "Nombre archivo comprobante transferencia", fileInfo.name);
      break;
    default: throw new Error("Campo de archivo no válido: " + fieldName);
  }
  return { ok: true, url: fileInfo.url, file_id: fileInfo.id, row_number: resRowNum };
}

function updateProfileFile_(cel, mainField, fileInfo) {
  const sheet = getSheet_(PERFILES_SHEET);
  const headers = getHeaders_(sheet);
  let row = findRowByPhone_(sheet, headers, cel);
  if (!row) {
    const dataMap = {
      "ID_Perfil": Utilities.getUuid(),
      "Cel/Whatsapp (principal)": cel,
      "Fecha creación": new Date(),
      "Fecha actualización": new Date()
    };
    row = appendRow_(sheet, headers, dataMap);
  }
  setCellByHeader_(sheet, headers, row, mainField, fileInfo.url);
  setCellByHeader_(sheet, headers, row, "Link " + mainField, fileInfo.url);
  if (mainField === "Identificación única") {
    setCellByHeader_(sheet, headers, row, "Link identificación", fileInfo.url);
  }
  setCellByHeader_(sheet, headers, row, "ID archivo " + mainField, fileInfo.id);
  const nameHeader = mainField === "Identificación única" ? "Nombre archivo identificación" : ("Nombre archivo " + mainField);
  setCellByHeader_(sheet, headers, row, nameHeader, fileInfo.name);
  setCellByHeader_(sheet, headers, row, "Fecha actualización", new Date());
}

function updateVehicleFile_(cel, fileInfo) {
  const sheet = getSheet_(VEHICULOS_SHEET);
  const headers = getHeaders_(sheet);
  let row = findRowByPhone_(sheet, headers, cel);
  if (!row) {
    const dataMap = {
      "ID_Vehiculo": Utilities.getUuid(),
      "Cel/Whatsapp (principal)": cel,
      "¿Cuenta con vehículo?": "Sí",
      "Fecha actualización": new Date()
    };
    row = appendRow_(sheet, headers, dataMap);
  }
  setCellByHeader_(sheet, headers, row, "Foto vehículo", fileInfo.url);
  setCellByHeader_(sheet, headers, row, "Link foto vehículo", fileInfo.url);
  setCellByHeader_(sheet, headers, row, "ID archivo foto vehículo", fileInfo.id);
  setCellByHeader_(sheet, headers, row, "Nombre archivo vehículo", fileInfo.name);
  setCellByHeader_(sheet, headers, row, "Fecha actualización", new Date());
}

// ─── FACTURAPI updates (todo en Reservaciones) ──────────────────────────────

function updateFacturadoTotal_(data) {
  const recordId = safe_(data.record_id || data.id || data.row_id);
  const rawValue = safe_(data.monto_facturado_total);
  if (!recordId) throw new Error("Falta record_id.");
  const normalized = String(rawValue || "").trim();
  if (normalized && isNaN(Number(normalized.replace(/,/g, "")))) throw new Error("Monto debe ser numérico.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let row = findRowByValue_(sheet, headers, "ID", recordId);
  if (!row) row = findRowByRowNumber_(sheet, recordId);
  if (!row) throw new Error("No se encontró la reservación.");
  setCellByHeader_(sheet, headers, row, "$ Monto facturado Total", normalized);
  return { ok: true, row_number: row, record_id: recordId, monto_facturado_total: normalized };
}

function updateFacturapiFolio_(data) {
  const recordId = safe_(data.record_id || data.id || data.row_id);
  const rawValue = safe_(data.folio_facturapi);
  if (!recordId) throw new Error("Falta record_id.");
  const normalized = String(rawValue || "").trim();
  if (!normalized || isNaN(Number(normalized))) throw new Error("Folio debe ser numérico.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let row = findRowByValue_(sheet, headers, "ID", recordId);
  if (!row) row = findRowByRowNumber_(sheet, recordId);
  if (!row) throw new Error("No se encontró la reservación.");
  setCellByHeader_(sheet, headers, row, "Folio facturapi", normalized);
  return { ok: true, row_number: row, record_id: recordId, folio_facturapi: normalized };
}

function updateFacturapiFolioStrict_(data) {
  const recordId = safe_(data.record_id || data.id || data.row_id);
  const explicitRow = safe_(data.row_number || data.rowNumber);
  const externalId = safe_(data.external_id || data.externalId);
  const rawValue = safe_(data.folio_facturapi || data.folio);
  if (!recordId && !externalId && !explicitRow) throw new Error("Falta identificador de reservación.");
  const normalized = String(rawValue || "").trim();
  if (!normalized || isNaN(Number(normalized))) throw new Error("Folio debe ser numérico.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let row = null;
  if (explicitRow) row = findRowByRowNumber_(sheet, explicitRow);
  if (!row && recordId) { row = findRowByValue_(sheet, headers, "ID", recordId); if (!row) row = findRowByRowNumber_(sheet, recordId); }
  if (!row && externalId) {
    const clean = String(externalId).replace(/^CHECKIN-/, "").trim();
    row = findRowByValue_(sheet, headers, "ID", clean);
    if (!row) row = findRowByRowNumber_(sheet, clean);
  }
  if (!row) throw new Error("No se encontró la reservación.");
  const targetCol = headers.indexOf("Folio facturapi");
  if (targetCol < 0) throw new Error('No existe la columna "Folio facturapi"');
  sheet.getRange(row, targetCol + 1).setNumberFormat("@");
  sheet.getRange(row, targetCol + 1).setValue(normalized);
  SpreadsheetApp.flush();
  return {
    ok: true,
    row_number: row,
    record_id: recordId || String(externalId).replace(/^CHECKIN-/, "").trim(),
    folio_facturapi: normalized,
    target_column: "Folio facturapi",
    sheet_name: sheet.getName(),
    spreadsheet_name: getSpreadsheet_().getName()
  };
}

function saveFacturapiPdf_(data) {
  const recordId = safe_(data.record_id || data.id || data.row_id);
  const explicitRow = safe_(data.row_number || data.rowNumber);
  const externalId = safe_(data.external_id || data.externalId);
  const receiptId = safe_(data.receipt_id || data.receiptId);
  const folio = safe_(data.folio_facturapi || data.folio);
  const fileObj = data.file;
  if (!recordId && !externalId && !explicitRow) throw new Error("Falta identificación.");
  if (!receiptId && !folio) throw new Error("Falta receipt_id o folio.");
  if (!fileObj || !fileObj.base64) throw new Error("Sin PDF.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let row = null;
  if (explicitRow) row = findRowByRowNumber_(sheet, explicitRow);
  if (!row && recordId) { row = findRowByValue_(sheet, headers, "ID", recordId); if (!row) row = findRowByRowNumber_(sheet, recordId); }
  if (!row && externalId) {
    const clean = String(externalId).replace(/^CHECKIN-/, "").trim();
    row = findRowByValue_(sheet, headers, "ID", clean);
    if (!row) row = findRowByRowNumber_(sheet, clean);
  }
  if (!row) throw new Error("No se encontró la reservación.");
  const resRow = readRow_(sheet, headers, row);
  const ingreso = parseDateSafe_(resRow["Fecha de ingreso"]) || new Date();
  const year = Utilities.formatDate(ingreso, Session.getScriptTimeZone(), "yyyy");
  const month = Utilities.formatDate(ingreso, Session.getScriptTimeZone(), "MM");
  const propiedad = cleanFolderName_(resRow["Propiedad"] || "Sin propiedad");
  const guestName = cleanFolderName_(resRow["Nombre de la persona que hizo la reservación"] || "Sin nombre");
  const pdfName = cleanFolderName_(folio ? "ticket_facturapi_folio_" + folio : "ticket_facturapi_" + receiptId) + ".pdf";
  const fileInfo = saveFacturapiPdfToDrive_(
    { fileName: pdfName, mimeType: fileObj.mimeType || "application/pdf", base64: fileObj.base64 },
    [year, month, propiedad, guestName, "Tickets Facturapi"]
  );
  setCellByHeader_(sheet, headers, row, "Ticket facturapi url", fileInfo.url);
  setCellByHeader_(sheet, headers, row, "Ticket facturapi id archivo", fileInfo.id);
  setCellByHeader_(sheet, headers, row, "Ticket facturapi nombre archivo", fileInfo.name);
  setCellByHeader_(sheet, headers, row, "Ticket facturapi carpeta url", fileInfo.folder_url);
  setCellByHeader_(sheet, headers, row, "Ticket facturapi carpeta ruta", fileInfo.folder_path);
  SpreadsheetApp.flush();
  return {
    ok: true,
    row_number: row,
    record_id: recordId || String(externalId).replace(/^CHECKIN-/, "").trim(),
    receipt_id: receiptId,
    folio_facturapi: folio,
    ticket_facturapi_url: fileInfo.url,
    file_id: fileInfo.id,
    file_name: fileInfo.name,
    folder_url: fileInfo.folder_url,
    folder_path: fileInfo.folder_path,
    sheet_name: sheet.getName(),
    spreadsheet_name: getSpreadsheet_().getName()
  };
}

function getNextFacturapiFolio_() {
  ensureNormalizedSheets_();
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  const col = headers.indexOf("Folio facturapi");
  let maxFolio = 0;
  if (col >= 0 && sheet.getLastRow() > 1) {
    const values = sheet.getRange(2, col + 1, sheet.getLastRow() - 1, 1).getDisplayValues();
    values.forEach(r => {
      const v = String(r[0] || "").replace(/[^0-9]/g, "");
      const n = Number(v || 0);
      if (n > maxFolio) maxFolio = n;
    });
  }
  return { ok: true, max_folio: maxFolio, next_folio: maxFolio + 1, sheet_name: RESERVACIONES_SHEET, spreadsheet_name: getSpreadsheet_().getName() };
}

// ─── READS con JOIN ─────────────────────────────────────────────────────────

function getPerfilByCel_(cel) {
  const sheet = getSheet_(PERFILES_SHEET);
  const headers = getHeaders_(sheet);
  const row = findRowByPhone_(sheet, headers, cel);
  if (!row) return null;
  return readRow_(sheet, headers, row);
}

function getVehiculoByCel_(cel) {
  const sheet = getSheet_(VEHICULOS_SHEET);
  const headers = getHeaders_(sheet);
  const row = findRowByPhone_(sheet, headers, cel);
  if (!row) return null;
  return readRow_(sheet, headers, row);
}

function mergeReservacionWithProfile_(resRow, perfil, vehiculo) {
  const p = perfil || {};
  return {
    "ID": resRow["ID"] || "",
    "row_number": resRow.__row_number || "",
    "Fecha de ingreso": resRow["Fecha de ingreso"] || "",
    "Fecha de salida": resRow["Fecha de salida"] || "",
    "Nombre de la persona que hizo la reservación": resRow["Nombre de la persona que hizo la reservación"] || "",
    "Medio de reservación": resRow["Medio de reservación"] || "",
    "Cel/Whatsapp (principal)": resRow["Cel/Whatsapp (principal)"] || "",
    "¿Requiere factura?": p["¿Requiere factura?"] || "",
    "($) Monto Total pagado": resRow["($) Monto Total pagado"] || "",
    "$ Monto facturado Total": resRow["$ Monto facturado Total"] || "",
    "Folio facturapi": resRow["Folio facturapi"] || "",
    "Ticket facturapi url": resRow["Ticket facturapi url"] || "",
    "Razón social": p["Razón social"] || "",
    "Régimen fiscal": p["Régimen fiscal"] || "",
    "Forma de pago": resRow["Forma de pago"] || "",
    "Correo electrónico": resRow["Correo electrónico"] || p["Correo electrónico para el envío de la factura"] || "",
    "Propiedad": resRow["Propiedad"] || "",
    "# Departamento": resRow["# Departamento"] || "",
    "# Huéspedes": resRow["# Huéspedes"] || ""
  };
}

function listGuestRecords_(params) {
  ensureNormalizedSheets_();
  const { rows: reservaciones } = getAllRows_(RESERVACIONES_SHEET);
  const { rows: perfiles } = getAllRows_(PERFILES_SHEET);
  const { rows: vehiculos } = getAllRows_(VEHICULOS_SHEET);

  const perfilByCel = {};
  perfiles.forEach(p => {
    const key = normalizePhone_(p["Cel/Whatsapp (principal)"]);
    if (key) perfilByCel[key] = p;
  });
  const vehiculoByCel = {};
  vehiculos.forEach(v => {
    const key = normalizePhone_(v["Cel/Whatsapp (principal)"]);
    if (key) vehiculoByCel[key] = v;
  });

  const joined = reservaciones.map(r => {
    const key = normalizePhone_(r["Cel/Whatsapp (principal)"]);
    return { res: r, perfil: perfilByCel[key] || null, vehiculo: vehiculoByCel[key] || null };
  });

  const filtered = joined.filter(j => {
    const r = j.res;
    const p = j.perfil || {};
    return matchDateFrom_(r["Fecha de ingreso"], params.fecha_entrada_desde) &&
      matchDateTo_(r["Fecha de ingreso"], params.fecha_entrada_hasta) &&
      matchDateFrom_(r["Fecha de salida"], params.fecha_salida_desde) &&
      matchDateTo_(r["Fecha de salida"], params.fecha_salida_hasta) &&
      matchContains_(r["Nombre de la persona que hizo la reservación"], params.nombre_reservacion) &&
      matchContains_(r["Medio de reservación"], params.medio_reservacion) &&
      matchContainsPhone_(r["Cel/Whatsapp (principal)"], params.celular_principal) &&
      matchEqualsNormalized_(normalizeFacturaForFilter_(p["¿Requiere factura?"]), params.requiere_factura) &&
      matchContains_(p["Razón social"], params.razon_social) &&
      matchContains_(r["Forma de pago"], params.forma_pago) &&
      matchContains_(r["Correo electrónico"] || p["Correo electrónico para el envío de la factura"], params.correo);
  });

  filtered.sort((a, b) => {
    const da = parseDateSafe_(a.res["Fecha de ingreso"]);
    const db = parseDateSafe_(b.res["Fecha de ingreso"]);
    return (db ? db.getTime() : 0) - (da ? da.getTime() : 0);
  });

  const total = filtered.length;
  const pageSize = Math.max(1, Math.min(Number(params.page_size || 25), 200));
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const page = Math.max(1, Math.min(Number(params.page || 1), totalPages || 1));
  const start = (page - 1) * pageSize;
  const pageRows = filtered.slice(start, start + pageSize).map(j => mergeReservacionWithProfile_(j.res, j.perfil, j.vehiculo));

  return {
    ok: true,
    rows: pageRows,
    total,
    page,
    page_size: pageSize,
    total_pages: Math.max(1, totalPages),
    total_con_factura: filtered.filter(j => normalizeText_(normalizeFacturaForFilter_((j.perfil || {})["¿Requiere factura?"])) === "si").length,
    total_sin_factura: filtered.filter(j => normalizeText_(normalizeFacturaForFilter_((j.perfil || {})["¿Requiere factura?"])) === "no").length,
    total_medios_unicos: uniqueNonEmpty_(filtered.map(j => j.res["Medio de reservación"])).length
  };
}

function getGuestFilterOptions_() {
  ensureNormalizedSheets_();
  const { rows: reservaciones } = getAllRows_(RESERVACIONES_SHEET);
  const { rows: perfiles } = getAllRows_(PERFILES_SHEET);
  return {
    ok: true,
    options: {
      nombres_reservacion: uniqueNonEmpty_(reservaciones.map(r => r["Nombre de la persona que hizo la reservación"])),
      medios_reservacion: uniqueNonEmpty_(reservaciones.map(r => r["Medio de reservación"])),
      celulares_principales: uniqueNonEmpty_(perfiles.map(p => p["Cel/Whatsapp (principal)"])),
      razones_sociales: uniqueNonEmpty_(perfiles.map(p => p["Razón social"])),
      formas_pago: uniqueNonEmpty_(reservaciones.map(r => r["Forma de pago"])),
      correos: uniqueNonEmpty_(
        reservaciones.map(r => r["Correo electrónico"])
          .concat(perfiles.map(p => p["Correo electrónico para el envío de la factura"]))
      )
    }
  };
}

function getGuestRecordDetail_(params) {
  ensureNormalizedSheets_();
  const recordId = safe_(params.record_id);
  if (!recordId) return { ok: false, error: "Falta record_id." };
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let rowNum = findRowByValue_(sheet, headers, "ID", recordId);
  if (!rowNum) rowNum = findRowByRowNumber_(sheet, recordId);
  if (!rowNum) return { ok: false, error: "Reservación no encontrada." };
  const resRow = readRow_(sheet, headers, rowNum);
  const cel = resRow["Cel/Whatsapp (principal)"];
  const perfil = cel ? getPerfilByCel_(cel) : null;
  const vehiculo = cel ? getVehiculoByCel_(cel) : null;
  const merged = {};
  if (perfil) Object.keys(perfil).forEach(k => { if (k !== "__row_number") merged[k] = perfil[k]; });
  if (vehiculo) Object.keys(vehiculo).forEach(k => { if (k !== "__row_number") merged[k] = vehiculo[k]; });
  Object.keys(resRow).forEach(k => { if (k !== "__row_number") merged[k] = resRow[k]; });
  merged.__row_number = rowNum;
  return { ok: true, record: merged };
}

function debugDashboard_() {
  ensureNormalizedSheets_();
  const ss = getSpreadsheet_();
  const p = getSheet_(PERFILES_SHEET);
  const v = getSheet_(VEHICULOS_SHEET);
  const r = getSheet_(RESERVACIONES_SHEET);
  return {
    ok: true,
    spreadsheet_name: ss.getName(),
    perfiles_count: Math.max(0, p.getLastRow() - 1),
    vehiculos_count: Math.max(0, v.getLastRow() - 1),
    reservaciones_count: Math.max(0, r.getLastRow() - 1),
    perfiles_headers: getHeaders_(p),
    vehiculos_headers: getHeaders_(v),
    reservaciones_headers: getHeaders_(r),
    schema: "normalized"
  };
}

// ─── MIGRACIÓN ONE-TIME ─────────────────────────────────────────────────────

function migrateToNormalizedSchema() {
  ensureNormalizedSheets_();
  const ss = getSpreadsheet_();
  let src = ss.getSheetByName(LEGACY_SHEET);
  if (!src) src = ss.getSheetByName(LEGACY_ARCHIVE);
  if (!src) return { ok: false, error: "No se encontró la hoja '" + LEGACY_SHEET + "' ni '" + LEGACY_ARCHIVE + "'." };

  const perfilesSh = getSheet_(PERFILES_SHEET);
  const vehiculosSh = getSheet_(VEHICULOS_SHEET);
  const reservacionesSh = getSheet_(RESERVACIONES_SHEET);

  if (perfilesSh.getLastRow() > 1 || vehiculosSh.getLastRow() > 1 || reservacionesSh.getLastRow() > 1) {
    return {
      ok: false,
      error: "Las hojas normalizadas ya tienen datos. La migración solo debe correrse una vez. Bórralas manualmente si quieres re-migrar.",
      perfiles: Math.max(0, perfilesSh.getLastRow() - 1),
      vehiculos: Math.max(0, vehiculosSh.getLastRow() - 1),
      reservaciones: Math.max(0, reservacionesSh.getLastRow() - 1)
    };
  }

  const srcHeaders = getHeaders_(src);
  const srcLast = src.getLastRow();
  if (srcLast < 2) {
    if (src.getName() === LEGACY_SHEET) src.setName(LEGACY_ARCHIVE);
    return { ok: true, migrated: 0, message: "Hoja vacía. Renombrada a backup." };
  }
  const srcValues = src.getRange(2, 1, srcLast - 1, srcHeaders.length).getDisplayValues();
  const srcRows = srcValues.map(rowValues => {
    const obj = {};
    srcHeaders.forEach((h, i) => obj[h] = rowValues[i]);
    return obj;
  });

  function pick(row, aliases) {
    for (let i = 0; i < aliases.length; i++) {
      if (Object.prototype.hasOwnProperty.call(row, aliases[i])) {
        const v = row[aliases[i]];
        if (v != null && String(v).trim() !== "") return String(v);
      }
    }
    return "";
  }

  const byCel = {};
  srcRows.forEach(r => {
    const cel = pick(r, ["Cel/Whatsapp (principal)", "Celular principal"]);
    if (!cel) return;
    const key = normalizePhone_(cel);
    if (!byCel[key]) byCel[key] = { cel, rows: [] };
    byCel[key].rows.push(r);
  });

  const perfilesHeaders = getHeaders_(perfilesSh);
  const vehHeaders = getHeaders_(vehiculosSh);
  const resHeaders = getHeaders_(reservacionesSh);
  let perfilesCount = 0, vehiculosCount = 0, reservacionesCount = 0;
  const idVehiculoByCel = {};

  Object.keys(byCel).forEach(key => {
    const group = byCel[key];
    const latest = group.rows.slice().sort((a, b) => {
      const da = parseDateSafe_(pick(a, ["Marca temporal", "Fecha de ingreso"]));
      const db = parseDateSafe_(pick(b, ["Marca temporal", "Fecha de ingreso"]));
      return (db ? db.getTime() : 0) - (da ? da.getTime() : 0);
    })[0];

    function pickMerged(aliases) {
      const list = [latest].concat(group.rows);
      for (let i = 0; i < list.length; i++) {
        const v = pick(list[i], aliases);
        if (v) return v;
      }
      return "";
    }

    const perfilMap = {
      "ID_Perfil": Utilities.getUuid(),
      "Cel/Whatsapp (principal)": group.cel,
      "Lada celular huésped": pickMerged(["Lada celular huésped"]),
      "Nombre del huésped": (pickMerged(["Nombres de TODOS los huéspedes (separados por comas)"]).split(",")[0] || "").trim() || pickMerged(["Nombre de la persona que hizo la reservación"]),
      "Lada contacto emergencia": pickMerged(["Lada contacto emergencia", "Lada contacto de emergencia"]),
      "Cel/Whatsapp (contacto de emergencia)": pickMerged(["Cel/Whatsapp (contacto de emergencia)"]),
      "Tipo de identificación": pickMerged(["Tipo de identificación"]),
      "Identificación otro": pickMerged(["Identificación otro"]),
      "INE frontal": pickMerged(["INE frontal"]),
      "Link INE frontal": pickMerged(["Link INE frontal"]),
      "ID archivo INE frontal": pickMerged(["ID archivo INE frontal"]),
      "Nombre archivo INE frontal": pickMerged(["Nombre archivo INE frontal"]),
      "INE trasero": pickMerged(["INE trasero"]),
      "Link INE trasero": pickMerged(["Link INE trasero"]),
      "ID archivo INE trasero": pickMerged(["ID archivo INE trasero"]),
      "Nombre archivo INE trasero": pickMerged(["Nombre archivo INE trasero"]),
      "Identificación única": pickMerged(["Identificación única"]),
      "Link identificación única": pickMerged(["Link identificación única", "Link identificación"]),
      "ID archivo identificación única": pickMerged(["ID archivo identificación única"]),
      "Nombre archivo identificación": pickMerged(["Nombre archivo identificación"]),
      "¿Requiere factura?": normalizeYesNo_(pickMerged(["¿Requiere factura?"])),
      "Razón social": pickMerged(["Razón social"]),
      "RFC": pickMerged(["RFC"]),
      "Régimen fiscal": pickMerged(["Régimen fiscal"]),
      "Régimen otro": pickMerged(["Régimen otro"]),
      "Código Postal": pickMerged(["Código Postal"]),
      "Correo electrónico para el envío de la factura": pickMerged(["Correo electrónico para el envío de la factura", "Correo electrónico"]),
      "Fecha creación": new Date(),
      "Fecha actualización": new Date()
    };
    appendRow_(perfilesSh, perfilesHeaders, perfilMap);
    perfilesCount++;

    const tieneVeh = group.rows.some(r => normalizeYesNo_(pick(r, ["¿Cuenta con vehículo?"])) === "Sí");
    if (tieneVeh) {
      const idVeh = Utilities.getUuid();
      idVehiculoByCel[key] = idVeh;
      const vehMap = {
        "ID_Vehiculo": idVeh,
        "Cel/Whatsapp (principal)": group.cel,
        "¿Cuenta con vehículo?": "Sí",
        "Marca vehículo": pickMerged(["Marca vehículo"]),
        "Marca vehículo otro": pickMerged(["Marca vehículo otro"]),
        "Modelo vehículo": pickMerged(["Modelo vehículo"]),
        "Modelo vehículo otro": pickMerged(["Modelo vehículo otro"]),
        "Color vehículo": pickMerged(["Color vehículo"]),
        "Placas": pickMerged(["Placas"]),
        "Hora habitual de salida": pickMerged(["Hora habitual de salida"]),
        "Foto vehículo": pickMerged(["Foto vehículo"]),
        "Link foto vehículo": pickMerged(["Link foto vehículo"]),
        "ID archivo foto vehículo": pickMerged(["ID archivo foto vehículo"]),
        "Nombre archivo vehículo": pickMerged(["Nombre archivo vehículo"]),
        "Fecha actualización": new Date()
      };
      appendRow_(vehiculosSh, vehHeaders, vehMap);
      vehiculosCount++;
    }
  });

  srcRows.forEach(r => {
    const cel = pick(r, ["Cel/Whatsapp (principal)"]);
    if (!cel) return;
    const key = normalizePhone_(cel);
    const idVeh = idVehiculoByCel[key] || "";
    const resMap = {
      "ID": pick(r, ["ID"]) || Utilities.getUuid(),
      "Cel/Whatsapp (principal)": cel,
      "ID_Vehiculo": idVeh,
      "Marca temporal": pick(r, ["Marca temporal"]),
      "MES": pick(r, ["MES"]),
      "Mes correspondiente": pick(r, ["Mes correspondiente"]),
      "Tipo de factura": pick(r, ["Tipo de factura"]),
      "Medio de reservación": pick(r, ["Medio de reservación"]),
      "Cuenta": pick(r, ["Cuenta"]),
      "Propiedad": pick(r, ["Propiedad"]),
      "Propiedad otra": pick(r, ["Propiedad otra"]),
      "# Departamento": pick(r, ["# Departamento"]),
      "Motivo de tu hospedaje": pick(r, ["Motivo de tu hospedaje"]),
      "Motivo otro": pick(r, ["Motivo otro"]),
      "Fecha de ingreso": pick(r, ["Fecha de ingreso"]),
      "Hora estimada de llegada": pick(r, ["Hora estimada de llegada"]),
      "Fecha de salida": pick(r, ["Fecha de salida"]),
      "Hora estimada de salida": pick(r, ["Hora estimada de salida"]),
      "# Noches": pick(r, ["# Noches"]),
      "# Huéspedes": pick(r, ["# Huéspedes"]),
      "Nombres de TODOS los huéspedes (separados por comas)": pick(r, ["Nombres de TODOS los huéspedes (separados por comas)"]),
      "Nombre de la persona que hizo la reservación": pick(r, ["Nombre de la persona que hizo la reservación"]),
      "Forma de pago": pick(r, ["Forma de pago"]),
      "Divisa monto pagado": pick(r, ["Divisa monto pagado"]),
      "Comprobante transferencia": pick(r, ["Comprobante transferencia"]),
      "Link comprobante transferencia": pick(r, ["Link comprobante transferencia"]),
      "ID archivo comprobante transferencia": pick(r, ["ID archivo comprobante transferencia"]),
      "Nombre archivo comprobante transferencia": pick(r, ["Nombre archivo comprobante transferencia"]),
      "Correo electrónico": pick(r, ["Correo electrónico"]),
      "...enviar copia al siguiente correo:": pick(r, ["...enviar copia al siguiente correo:"]),
      "$ Noches": pick(r, ["$ Noches"]),
      "$ Cuota de limpieza": pick(r, ["$ Cuota de limpieza"]),
      "$ MONTO TOTAL Airbnb": pick(r, ["$ MONTO TOTAL Airbnb"]),
      "$ Comisión Airbnb": pick(r, ["$ Comisión Airbnb"]),
      "$ Monto antes de impuestos": pick(r, ["$ Monto antes de impuestos"]),
      "($) Monto Total pagado": pick(r, ["($) Monto Total pagado"]),
      "$ Monto facturado Total": pick(r, ["$ Monto facturado Total"]),
      "Folio facturapi": pick(r, ["Folio facturapi"]),
      "Folio CFDI": pick(r, ["Folio CFDI"]),
      "Folio Relación": pick(r, ["Folio Relación"]),
      "Folio complemento de pago": pick(r, ["Folio complemento de pago"]),
      "Estatus": pick(r, ["Estatus"]),
      "Fecha de emisión": pick(r, ["Fecha de emisión"]),
      "Concepto Factura": pick(r, ["Concepto Factura"]),
      "Método de pago": pick(r, ["Método de pago"]),
      "Ticket facturapi url": pick(r, ["Ticket facturapi url"]),
      "Ticket facturapi id archivo": pick(r, ["Ticket facturapi id archivo"]),
      "Ticket facturapi nombre archivo": pick(r, ["Ticket facturapi nombre archivo"]),
      "Ticket facturapi carpeta url": pick(r, ["Ticket facturapi carpeta url"]),
      "Ticket facturapi carpeta ruta": pick(r, ["Ticket facturapi carpeta ruta"]),
      "Envía tus comentarios": pick(r, ["Envía tus comentarios"]),
      "Envía tus comentarios con relación a la factura": pick(r, ["Envía tus comentarios con relación a la factura"]),
      "Notas": pick(r, ["Notas"]),
      "Enviado por": pick(r, ["Enviado por"])
    };
    appendRow_(reservacionesSh, resHeaders, resMap);
    reservacionesCount++;
  });

  if (src.getName() === LEGACY_SHEET) {
    let archiveName = LEGACY_ARCHIVE;
    if (ss.getSheetByName(archiveName)) {
      archiveName = LEGACY_ARCHIVE + " " + Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
    }
    src.setName(archiveName);
  }

  return {
    ok: true,
    perfiles_creados: perfilesCount,
    vehiculos_creados: vehiculosCount,
    reservaciones_creadas: reservacionesCount,
    backup_sheet: src.getName(),
    spreadsheet: ss.getName(),
    message: "Migración completada. La hoja original quedó como backup intacto."
  };
}

// ─── DRIVE / ARCHIVOS ────────────────────────────────────────────────────────

function saveBase64FileToDrive_(fileObj, prefix, subfolders) {
  if (String(fileObj.base64 || "").length > MAX_BASE64_CHARS) throw new Error("Archivo demasiado grande.");
  let folder = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const pathParts = [];
  (subfolders || []).forEach(name => {
    const clean = cleanFolderName_(name);
    folder = getOrCreateFolder_(folder, clean);
    pathParts.push(clean);
  });
  const bytes = Utilities.base64Decode(fileObj.base64);
  const ext = guessExtension_(fileObj.mimeType, fileObj.fileName);
  const ts = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyyMMdd_HHmmss");
  const safePrefix = cleanFolderName_(prefix || "archivo");
  const safeName = cleanFolderName_((fileObj.fileName || "imagen").replace(/\.[^.]+$/, ""));
  const blob = Utilities.newBlob(bytes, fileObj.mimeType || "image/jpeg", safePrefix + "_" + safeName + "_" + ts + "." + ext);
  const file = folder.createFile(blob);
  if (PUBLIC_SHARING_WITH_LINK) file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
  return {
    url: file.getUrl(),
    id: file.getId(),
    name: file.getName(),
    folder_id: folder.getId(),
    folder_name: folder.getName(),
    folder_url: folder.getUrl(),
    folder_path: pathParts.join(" / ")
  };
}

function saveFacturapiPdfToDrive_(fileObj, subfolders) {
  if (String(fileObj.base64 || "").length > MAX_BASE64_CHARS) throw new Error("Archivo demasiado grande.");
  let folder = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const pathParts = [];
  (subfolders || []).forEach(name => {
    const clean = cleanFolderName_(name);
    folder = getOrCreateFolder_(folder, clean);
    pathParts.push(clean);
  });
  const bytes = Utilities.base64Decode(fileObj.base64);
  const finalName = cleanFolderName_((fileObj.fileName || "ticket_facturapi").replace(/\.[^.]+$/, "")) + ".pdf";
  let file;
  const existing = folder.getFilesByName(finalName);
  if (existing.hasNext()) file = existing.next();
  else {
    const blob = Utilities.newBlob(bytes, fileObj.mimeType || "application/pdf", finalName);
    file = folder.createFile(blob);
  }
  if (PUBLIC_SHARING_WITH_LINK) file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
  return {
    url: file.getUrl(),
    id: file.getId(),
    name: file.getName(),
    folder_id: folder.getId(),
    folder_name: folder.getName(),
    folder_url: folder.getUrl(),
    folder_path: pathParts.join(" / ")
  };
}

function getOrCreateFolder_(parent, name) {
  const clean = cleanFolderName_(name);
  const cacheKey = String(parent.getId()) + '::' + clean;
  if (FOLDER_CACHE_[cacheKey]) return FOLDER_CACHE_[cacheKey];
  const existing = parent.getFoldersByName(clean);
  const folder = existing.hasNext() ? existing.next() : parent.createFolder(clean);
  FOLDER_CACHE_[cacheKey] = folder;
  return folder;
}

function cleanFolderName_(value) {
  return String(value || "Sin nombre")
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/[\\/:*?"<>|#%]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .substring(0, 80) || "Sin nombre";
}

function guessExtension_(mimeType, fileName) {
  const map = { "image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png", "image/webp": "webp", "application/pdf": "pdf" };
  if (map[mimeType]) return map[mimeType];
  const m = String(fileName || "").match(/\.([^.]+)$/);
  return m ? m[1].toLowerCase() : "jpg";
}

// ─── UTILS ──────────────────────────────────────────────────────────────────

function calculateNights_(checkIn, checkOut) {
  const inDate = parseDateSafe_(checkIn);
  const outDate = parseDateSafe_(checkOut);
  if (!inDate || !outDate) return "";
  const diff = Math.round((outDate - inDate) / (1000 * 60 * 60 * 24));
  return diff >= 0 ? diff : "";
}

function parseDateSafe_(value) {
  if (!value) return null;
  const d = new Date(value);
  return isNaN(d.getTime()) ? null : d;
}

function normalizeYesNo_(value) {
  const v = String(value || "").toLowerCase().trim();
  if (v === "si" || v === "sí" || v === "yes") return "Sí";
  if (v === "no") return "No";
  return safe_(value);
}

function normalizeInvoiceType_(value) {
  const v = String(value || "").toLowerCase().trim();
  if (v === "si" || v === "sí" || v === "yes") return "Factura";
  if (v === "no") return "Sin factura";
  return "";
}

function resolveOtherValue_(selectedValue, otherValue, otherTokens) {
  const selected = safe_(selectedValue).trim();
  const other = safe_(otherValue).trim();
  if (!selected) return other;
  const tokens = otherTokens || ["Otro", "Otra", "Other"];
  return tokens.indexOf(selected) !== -1 ? other : selected;
}

function arrayToCsv_(value) {
  if (Array.isArray(value)) {
    return value.map(v => String(v || "").trim()).filter(String).join(", ");
  }
  return safe_(value);
}

function safe_(value) { return value == null ? "" : String(value); }

function normalizeText_(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePhone_(value) {
  return String(value || "").replace(/[\s'‘’“”]/g, "").replace(/^[+]/, "").trim();
}

function normalizeFacturaForFilter_(value) {
  const v = normalizeText_(value);
  if (v === "factura" || v === "si" || v === "sí" || v === "yes") return "Sí";
  if (v === "sin factura" || v === "no") return "No";
  return safe_(value);
}

function matchContains_(cellValue, filterValue) {
  const filter = normalizeText_(filterValue);
  if (!filter) return true;
  return normalizeText_(cellValue).indexOf(filter) !== -1;
}

function matchContainsPhone_(cellValue, filterValue) {
  const filter = normalizePhone_(filterValue);
  if (!filter) return true;
  return normalizePhone_(cellValue).indexOf(filter) !== -1;
}

function matchEqualsNormalized_(cellValue, filterValue) {
  const filter = normalizeText_(filterValue);
  if (!filter) return true;
  return normalizeText_(cellValue) === filter;
}

function matchDateFrom_(cellValue, fromValue) {
  if (!fromValue) return true;
  const cellDate = parseDateSafe_(cellValue);
  const fromDate = parseDateSafe_(fromValue);
  if (!cellDate || !fromDate) return false;
  return stripTime_(cellDate).getTime() >= stripTime_(fromDate).getTime();
}

function matchDateTo_(cellValue, toValue) {
  if (!toValue) return true;
  const cellDate = parseDateSafe_(cellValue);
  const toDate = parseDateSafe_(toValue);
  if (!cellDate || !toDate) return false;
  return stripTime_(cellDate).getTime() <= stripTime_(toDate).getTime();
}

function stripTime_(d) { return new Date(d.getFullYear(), d.getMonth(), d.getDate()); }

function uniqueNonEmpty_(arr) {
  const seen = {};
  const out = [];
  (arr || []).forEach(value => {
    const txt = safe_(value).trim();
    if (!txt) return;
    const key = normalizeText_(txt);
    if (seen[key]) return;
    seen[key] = true;
    out.push(txt);
  });
  return out.sort();
}

function jsonOutput_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}
