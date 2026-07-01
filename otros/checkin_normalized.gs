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
const ALOJAMIENTOS_SHEET = "alojamientos";
const LEGACY_SHEET = "Check in";
const LEGACY_ARCHIVE = "Check in (archivo)";
const BREEZEWAY_ALERTS_SHEET = "Breezeway_Alerts";
const BREEZEWAY_ALERTS_HEADERS = [
  // ─── Identificación + clasificación ───
  "id","received_at","event_type","kind",
  "task_id","task_name","task_type",
  // ─── Fechas (schema BZW VERIFICADO) ───
  // BZW NO tiene un campo "due_date" separado. La "fecha límite" que se ve
  // en su UI ES scheduled_date. La columna due_date legacy queda vacía y
  // puede eliminarse manualmente del Sheet.
  // scheduled_date  : Cuándo debe ejecutarse (fecha "📅 jun 15" en BZW UI)
  // scheduled_time  : Hora prevista (puede ser null)
  // started_at      : Cuando el operario picó iniciar
  // finished_at     : Cuando se marcó como completada
  // created_at      : Cuando se generó la task en BZW
  // updated_at      : Última modificación en BZW
  // received_at     : Nuestro reloj al recibir webhook (NO es de BZW)
  // arrival_date    : Check-in de la reservación ligada
  // departure_date  : Check-out de la reservación ligada
  "scheduled_date","scheduled_time","started_at","finished_at",
  "created_at","updated_at","arrival_date","departure_date",
  // ─── Estado simplificado (computado en backend) ───
  // status_label: una de 3 etiquetas — "Terminado" | "En proceso" | "Pendiente"
  "status_label",
  // ─── Personas ───
  "finished_by","assigned_to",
  // ─── Propiedad ───
  "home_id","property_name",
  // ─── Vinculación a reservación ───
  "lodgify_id",
  // ─── Detalle de la task (schema BZW real) ───
  "priority","status","description","summary",
  "total_time","paused","tags",
  "rate_type","rate_paid",
  "template_id","report_url",
  // Calificación del huésped que el personal de limpieza responde en el
  // template "Limpieza Checkout" — item "Calificacion del Huesped Que tan
  // Limpio dejo el departamento ? 5 = MUY Limpio 1= MUY Sucio". Se extrae
  // de line_items del task detail en el backend Node.
  "guest_rating",
  "detail",
  // ─── Payload crudo (debug) ───
  "raw_json"
];
const BREEZEWAY_ALERTS_MAX = 200000; // tope alto para conservar histórico completo

const PUSH_SUBS_SHEET = "Push_Subscriptions";
const PUSH_SUBS_HEADERS = ["phoneKey","endpoint","p256dh","auth","ua","created_at","updated_at","badge_count","last_sent_at","categories"];
const OTP_CODES_SHEET = "OTP_Codes";
const OTP_CODES_HEADERS = ["phoneKey","method","target","code","created_at","expires_at","attempts","verified_at","status"];
const OTP_TTL_MS = 5 * 60 * 1000;        // 5 minutos
const OTP_MAX_ATTEMPTS = 5;              // por código
const PUSH_QUEUE_SHEET = "Notifications_Queue";
const PUSH_QUEUE_HEADERS = ["id","target","category","title","body","badge","url","tag","status","error","created_at","processed_at","source"];
// Inbox de notificaciones POR USUARIO (lo que se muestra en el panel 🔔 del header)
const NOTIF_INBOX_SHEET = "Notifications_Inbox";
const NOTIF_INBOX_HEADERS = ["id","phoneKey","type","title","body","data","created_at","read_at","archived_at"];
// Categorías de avisos disponibles
const PUSH_CATEGORIES = ["reservaciones","facturas","recordatorios","general"];
// phoneKey del admin — quien puede mandar avisos y ver el panel admin del portal.
// Cambiar si la cuenta admin cambia.
const ADMIN_PHONE_KEY = "528115569120";
// Clave pública VAPID (segura para exponer al cliente)
const VAPID_PUBLIC_KEY = "BH0SFnFLetMhyFlMaSFfl2ZfH-UYcuIvEQze-kPOcKGukvB4PmDW_Pu4WAe0zkpwYz3ks7oLHE7UGSI7QVOAb74";

const PERFILES_HEADERS = [
  "ID_Perfil","Cel/Whatsapp (principal)","Lada celular huésped","Nombre del huésped",
  "Lada contacto emergencia","Cel/Whatsapp (contacto de emergencia)",
  "Tipo de identificación","Identificación otro",
  "INE frontal","Link INE frontal","ID archivo INE frontal","Nombre archivo INE frontal",
  "INE trasero","Link INE trasero","ID archivo INE trasero","Nombre archivo INE trasero",
  "Identificación única","Link identificación única","ID archivo identificación única","Nombre archivo identificación",
  "¿Requiere factura?","Razón social","RFC","Régimen fiscal","Régimen otro","Código Postal",
  "Correo electrónico para el envío de la factura",
  "Fecha creación","Fecha actualización",
  "PIN hash","PIN actualizado"
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
  "Folio facturapi","Folio facturapi antiguo","Organización facturapi","Folio CFDI","Folio Relación","Folio complemento de pago","Estatus",
  "Fecha de emisión","Concepto Factura","Método de pago",
  "Ticket facturapi url","Ticket facturapi id archivo","Ticket facturapi nombre archivo","Ticket facturapi carpeta url","Ticket facturapi carpeta ruta",
  "Envía tus comentarios","Envía tus comentarios con relación a la factura","Notas","Enviado por",
  "Lodgify Id"
];

// ─── ENTRY POINTS ────────────────────────────────────────────────────────────

function doPost(e) {
  try {
    const data = JSON.parse((e && e.postData && e.postData.contents) || "{}");
    const action = data.action || "submit_form";
    if (action === "submit_form") return jsonOutput_(saveFormRecord_(data));
    if (action === "save_profile_only") return jsonOutput_(saveProfileOnly_(data));
    if (action === "upload_file") return jsonOutput_(saveDeferredFile_(data));
    if (action === "upload_profile_file") return jsonOutput_(saveProfileFile_(data));
    if (action === "migrate_phone") return jsonOutput_(migratePhone_(data));
    if (action === "update_facturado_total") return jsonOutput_(updateFacturadoTotal_(data));
    if (action === "update_monto_total_airbnb") return jsonOutput_(updateMontoTotalAirbnb_(data));
    if (action === "update_comision_airbnb") return jsonOutput_(updateComisionAirbnb_(data));
    if (action === "update_airbnb_amounts") return jsonOutput_(updateAirbnbAmounts_(data));
    if (action === "update_facturapi_folio") return jsonOutput_(updateFacturapiFolio_(data));
    if (action === "update_facturapi_folio_strict") return jsonOutput_(updateFacturapiFolioStrict_(data));
    if (action === "archive_folio_for_reemit") return jsonOutput_(archiveFolioForReemit_(data));
    if (action === "save_facturapi_pdf") return jsonOutput_(saveFacturapiPdf_(data));
    if (action === "send_otp") return jsonOutput_(sendOtp_(data));
    if (action === "verify_otp") return jsonOutput_(verifyOtp_(data));
    if (action === "check_user_status") return jsonOutput_(checkUserStatus_(data));
    if (action === "set_pin") return jsonOutput_(setPin_(data));
    if (action === "verify_pin") return jsonOutput_(verifyPin_(data));
    if (action === "mark_notifications_read") return jsonOutput_(markAllNotificationsRead_(data));
    if (action === "archive_notification") return jsonOutput_(archiveNotification_(data));
    if (action === "register_push_subscription") return jsonOutput_(registerPushSubscription_(data));
    if (action === "unregister_push_subscription") return jsonOutput_(unregisterPushSubscription_(data));
    if (action === "update_push_categories") return jsonOutput_(updatePushCategories_(data));
    if (action === "queue_notification") return jsonOutput_(queueNotification_(data));
    if (action === "mark_notification_processed") return jsonOutput_(markNotificationProcessed_(data));
    if (action === "delete_reservacion") return jsonOutput_(deleteReservacion_(data));
    if (action === "lodgify_list") return jsonOutput_(getLodgifyReservations_(data));
    if (action === "lodgify_sync") return jsonOutput_(syncLodgifyReservations_(data));
    if (action === "lg_hide_booking") return jsonOutput_(hideLodgifyBooking_(data));
    if (action === "unify_reservaciones") return jsonOutput_(unifyReservacionesRows_(data));
    if (action === "unhide_reservacion") return jsonOutput_(unhideReservacion_(data));
    if (action === "hide_reservacion") return jsonOutput_(hideReservacion_(data));
    if (action === "breezeway_alert") return jsonOutput_(saveBreezewayAlert_(data));
    if (action === "breezeway_alerts_bulk") return jsonOutput_(saveBreezewayAlertsBulk_(data));
    if (action === "breezeway_alerts_cleanup") return jsonOutput_(cleanBreezewayTestRows_());
    if (action === "breezeway_alerts_sort") return jsonOutput_(sortBreezewayBySched_());
    if (action === "breezeway_alerts_dedupe") return jsonOutput_(dedupeBreezewayAlerts_());
    // ─── Carga de datos bancarios (Registros contables → BANCOS) ───
    if (action === "bn_cuentas_bancarias_list") return jsonOutput_(bnCuentasBancariasList_());
    if (action === "bn_bancos_dedupe_index")    return jsonOutput_(bnBancosDedupeIndex_());
    if (action === "bn_bancos_insert_bulk")     return jsonOutput_(bnBancosInsertBulk_(data));
    if (action === "bn_bancos_classified_history") return jsonOutput_(bnBancosClassifiedHistory_());
    if (action === "bn_drive_list_files")       return jsonOutput_(bnDriveListFiles_(data));
    if (action === "bn_drive_get_file")         return jsonOutput_(bnDriveGetFile_(data));
    if (action === "bn_imported_files_list")    return jsonOutput_(bnImportedFilesList_());
    if (action === "bn_imported_files_mark")    return jsonOutput_(bnImportedFilesMark_(data));
    // ─── Tickets (Ticket Vision) ───
    if (action === "upload_ticket_image")          return jsonOutput_(uploadTicketImage_(data));
    if (action === "append_rows")                  return jsonOutput_(appendRows_(data));
    // ─── Incidencias ───
    if (action === "upload_incidencia_image")      return jsonOutput_(uploadIncidenciaImage_(data));
    if (action === "save_incidencia")              return jsonOutput_(saveIncidencia_(data));
    if (action === "update_incidencia")            return jsonOutput_(updateIncidencia_(data));
    if (action === "upload_objeto_image")          return jsonOutput_(uploadObjetoImage_(data));
    if (action === "save_objeto")                  return jsonOutput_(saveObjeto_(data));
    if (action === "update_objeto")                return jsonOutput_(updateObjeto_(data));
    if (action === "rh_upload_obligacion")         return jsonOutput_(rhUploadObligacion_(data));
    if (action === "rh_list_obligaciones")         return jsonOutput_(rhListObligaciones_(data));
    if (action === "rh_delete_obligacion")         return jsonOutput_(rhDeleteObligacion_(data));
    if (action === "rh_set_obligacion_total")      return jsonOutput_(rhSetObligacionTotal_(data));
    if (action === "rh_list_obligacion_totales")   return jsonOutput_(rhListObligacionTotales_(data));
    if (action === "rh_list_empleados")            return jsonOutput_(rhListEmpleados_());
    if (action === "rh_save_empleado")             return jsonOutput_(rhSaveEmpleado_(data));
    if (action === "rh_list_asistencia")           return jsonOutput_(rhListSimple_('RH_Asistencia'));
    if (action === "rh_save_asistencia")           return jsonOutput_(rhSaveSimple_('RH_Asistencia', data, RH_ASIST_HEADERS, 'AST'));
    if (action === "rh_list_ausencias")            return jsonOutput_(rhListSimple_('RH_Ausencias'));
    if (action === "rh_save_ausencia")             return jsonOutput_(rhSaveSimple_('RH_Ausencias', data, RH_AUSE_HEADERS, 'AUS'));
    if (action === "rh_list_compensaciones")       return jsonOutput_(rhListSimple_('RH_Compensaciones'));
    if (action === "rh_save_compensacion")         return jsonOutput_(rhSaveSimple_('RH_Compensaciones', data, RH_COMP_HEADERS, 'CMP'));
    if (action === "rh_delete_compensacion")       return jsonOutput_(rhDeleteByID_('RH_Compensaciones', String((data && data.ID) || '')));
    if (action === "rh_delete_asistencia")         return jsonOutput_(rhDeleteByID_('RH_Asistencia', String((data && data.ID) || '')));
    if (action === "rh_delete_ausencia")           return jsonOutput_(rhDeleteByID_('RH_Ausencias', String((data && data.ID) || '')));
    if (action === "sys_login")                    return jsonOutput_(sysLogin_(data));
    if (action === "get_tickets_index")            return jsonOutput_(getTicketsIndex_());
    if (action === "get_all_tickets")              return jsonOutput_(getAllTickets_());
    if (action === "update_ticket_classification") return jsonOutput_(updateTicketClassification_(data));
    if (action === "delete_ticket")                return jsonOutput_(deleteTicket_(data));
    // ─── Lectura/escritura BANCOS (Registros contables) ───
    if (action === "get_bancos_data")              return jsonOutput_(getBancosData_(SpreadsheetApp.openById(SPREADSHEET_ID)));
    if (action === "save_banco_clasificacion")     return jsonOutput_(saveBancoClasificacion_(SpreadsheetApp.openById(SPREADSHEET_ID), data));
    if (action === "bn_set_ticket_matches_bulk")   return jsonOutput_(bnSetTicketMatchesBulk_(SpreadsheetApp.openById(SPREADSHEET_ID), data));
    if (action === "save_presupuesto")             return jsonOutput_(savePresupuesto_(SpreadsheetApp.openById(SPREADSHEET_ID), data));
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
    if (action === "update_reservacion_cell") return jsonOutput_(updateReservacionCell_(e.parameter || {}));
    if (action === "get_vapid_public_key") return jsonOutput_({ ok: true, key: VAPID_PUBLIC_KEY });
    if (action === "list_alojamientos") return jsonOutput_(listAlojamientos_());
    if (action === "list_dispositivos") return jsonOutput_(listDispositivos_());
    if (action === "list_personal")     return jsonOutput_(listPersonal_());
    if (action === "rh_list_empleados") return jsonOutput_(rhListEmpleados_());
    if (action === "rh_save_empleado")  return jsonOutput_(rhSaveEmpleado_(e.parameter || {}));
    if (action === "rh_list_asistencia") return jsonOutput_(rhListSimple_('RH_Asistencia'));
    if (action === "rh_save_asistencia") return jsonOutput_(rhSaveSimple_('RH_Asistencia', e.parameter || {}, RH_ASIST_HEADERS, 'AST'));
    if (action === "rh_list_ausencias") return jsonOutput_(rhListSimple_('RH_Ausencias'));
    if (action === "rh_save_ausencia")  return jsonOutput_(rhSaveSimple_('RH_Ausencias', e.parameter || {}, RH_AUSE_HEADERS, 'AUS'));
    if (action === "rh_list_compensaciones") return jsonOutput_(rhListSimple_('RH_Compensaciones'));
    if (action === "rh_save_compensacion")   return jsonOutput_(rhSaveSimple_('RH_Compensaciones', e.parameter || {}, RH_COMP_HEADERS, 'CMP'));
    if (action === "rh_delete_compensacion") return jsonOutput_(rhDeleteByID_('RH_Compensaciones', String((e.parameter && e.parameter.ID) || '')));
    if (action === "rh_delete_asistencia")   return jsonOutput_(rhDeleteByID_('RH_Asistencia', String((e.parameter && e.parameter.ID) || '')));
    if (action === "rh_delete_ausencia")     return jsonOutput_(rhDeleteByID_('RH_Ausencias', String((e.parameter && e.parameter.ID) || '')));
    if (action === "sys_login")              return jsonOutput_(sysLogin_(e.parameter || {}));
    if (action === "upload_incidencia_image") return jsonOutput_(uploadIncidenciaImage_(e.parameter || {}));
    if (action === "save_incidencia")         return jsonOutput_(saveIncidencia_(e.parameter || {}));
    if (action === "list_incidencias")        return jsonOutput_(listIncidencias_());
    if (action === "update_incidencia")       return jsonOutput_(updateIncidencia_(e.parameter || {}));
    if (action === "upload_objeto_image")     return jsonOutput_(uploadObjetoImage_(e.parameter || {}));
    if (action === "save_objeto")             return jsonOutput_(saveObjeto_(e.parameter || {}));
    if (action === "list_objetos")            return jsonOutput_(listObjetos_());
    if (action === "update_objeto")           return jsonOutput_(updateObjeto_(e.parameter || {}));
    if (action === "breezeway_alerts_list") return jsonOutput_(listBreezewayAlerts_(e.parameter || {}));
    if (action === "get_otp_methods") return jsonOutput_(getOtpMethods_(e.parameter || {}));
    if (action === "list_push_subscriptions") return jsonOutput_(listPushSubscriptions_(e.parameter || {}));
    if (action === "send_push_to_user") return jsonOutput_(sendPushToUserFromAppsScript_(e.parameter || {}));
    if (action === "list_pending_notifications") return jsonOutput_(listPendingNotifications_(e.parameter || {}));
    if (action === "get_push_categories") return jsonOutput_({ ok: true, categories: PUSH_CATEGORIES });
    if (action === "is_admin") return jsonOutput_({ ok: true, isAdmin: String((e.parameter||{}).phoneKey||"").replace(/\D/g,"") === ADMIN_PHONE_KEY });
    if (action === "list_notifications") return jsonOutput_(listNotifications_(e.parameter || {}));
    if (action === "get_profile") return jsonOutput_(getProfile_(e.parameter || {}));
    if (action === "get_image_b64") return jsonOutput_(getImageB64_(e.parameter || {}));
    if (action === "lodgify_list") return jsonOutput_(getLodgifyReservations_(e.parameter || {}));
    if (action === "lodgify_sync") return jsonOutput_(syncLodgifyReservations_(e.parameter || {}));
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

  // ANTI-DUPLICADOS: si ya existe una fila con mismo teléfono (últimos 10
  // dígitos) + misma Fecha de ingreso (ISO), FUSIONAR el formulario manual
  // en esa fila en lugar de insertar otra. Se aplica tanto si la fila ya
  // tiene Lodgify Id (auto-propagada) como si NO lo tiene (manual previo
  // del mismo huésped) — en ambos casos es la misma reserva.
  const existingRow = findReservacionByPhoneArrival_(sheet, headers, cel, safe_(data.ingreso));
  if (existingRow) {
    return updateReservacionWithFormData_(sheet, headers, existingRow.row, existingRow.id, data, cel, idVehiculo, now, ingresoDate);
  }

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

/** Busca una fila en Reservaciones que matchee phone(últimos 10 dígitos) +
 *  fecha de ingreso (ISO). Devuelve { row, id } o null.
 *
 *  IMPORTANTE: usa `.getValues()` (no `getDisplayValues()`) para evitar el
 *  bug del locale es-MX donde las fechas tipadas como Date se renderizan
 *  "DD/MM/YYYY" y el parser previo las leía como "MM/DD/YYYY" (Lodgify),
 *  invirtiendo día y mes. La normalización pasa por `lodgifyDateToIso_`
 *  que maneja correctamente Date objects, ISO y MM/DD/YYYY (Lodgify). */
function findReservacionByPhoneArrival_(sheet, headers, cel, ingreso) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;
  const idxCel = headers.indexOf("Cel/Whatsapp (principal)");
  const idxFi  = headers.indexOf("Fecha de ingreso");
  const idxId  = headers.indexOf("ID");
  if (idxCel < 0 || idxFi < 0 || idxId < 0) return null;
  const tail = String(cel || "").replace(/\D/g, "").slice(-10);
  const ingIso = (lodgifyDateToIso_(ingreso) || String(ingreso || "").slice(0, 10));
  if (!tail || !ingIso) return null;
  const data = sheet.getRange(2, 1, lastRow - 1, headers.length).getValues();
  for (let i = 0; i < data.length; i++) {
    const rTail = String(data[i][idxCel] || "").replace(/\D/g, "").slice(-10);
    if (rTail !== tail) continue;
    const rIng = lodgifyDateToIso_(data[i][idxFi]);
    if (rIng !== ingIso) continue;
    return { row: i + 2, id: String(data[i][idxId] || "") };
  }
  return null;
}

/** Fusiona los campos del formulario manual en la fila existente (que ya
 *  tiene Lodgify Id). No sobrescribe campos identitarios (ID, Cel, Lodgify
 *  Id, Fecha de ingreso/salida). */
function updateReservacionWithFormData_(sheet, headers, row, recordId, data, cel, idVehiculo, now, ingresoDate) {
  const m = {};
  m["ID_Vehiculo"]                  = idVehiculo || "";
  m["Marca temporal"]                = now;
  m["MES"]                           = Utilities.formatDate(now, Session.getScriptTimeZone(), "MMMM");
  m["Mes correspondiente"]           = Utilities.formatDate(ingresoDate, Session.getScriptTimeZone(), "MMMM yyyy");
  m["Tipo de factura"]               = normalizeInvoiceType_(data.factura);
  m["Cuenta"]                        = "";
  m["Propiedad otra"]                = safe_(data.propiedad_otra);
  m["# Departamento"]                = safe_(data.depto);
  m["Motivo de tu hospedaje"]        = resolveOtherValue_(data.motivo, data.motivo_otro, ["Otro","Other"]);
  m["Motivo otro"]                   = safe_(data.motivo_otro);
  m["Hora estimada de llegada"]      = safe_(data.hora_llegada_estimada);
  m["Hora estimada de salida"]       = safe_(data.hora_salida_estimada);
  m["# Huéspedes"]                   = safe_(data.num_huespedes);
  m["Nombres de TODOS los huéspedes (separados por comas)"] = arrayToCsv_(data.huespedes);
  m["Forma de pago"]                 = safe_(data.medio_pago);
  m["Divisa monto pagado"]           = safe_(data.divisa_monto);
  m["Correo electrónico"]            = safe_(data.correo1);
  m["...enviar copia al siguiente correo:"] = safe_(data.correo2);
  m["($) Monto Total pagado"]        = safe_(data.monto_pagado);
  m["Envía tus comentarios"]         = safe_(data.comentarios);
  m["Envía tus comentarios con relación a la factura"] = safe_(data.comentarios_factura);
  // Sobrescribir solo si hay valor (no borrar lo que la propagación dejó)
  Object.keys(m).forEach(k => {
    const idx = headers.indexOf(k);
    if (idx < 0) return;
    const v = m[k];
    if (v === "" || v == null) return;     // no sobrescribir con vacío
    sheet.getRange(row, idx + 1).setValue(v);
  });
  return { row_number: row, record_id: recordId, merged_with_lodgify_row: true };
}

/** Wrapper PÚBLICO para correr el backfill de Propiedad/# Departamento
 *  contra el catálogo "alojamientos" sobre filas YA importadas de Lodgify. */
/** Test ultra-mínimo. No toca nada. Si esta falla, el problema es del
 *  entorno del editor (motor, autorización, sesión), no del código. */
function pingScript() {
  Logger.log("pong");
  return "pong";
}

function homologarPropiedades() {
  try {
    const res = homologarPropiedadesDesdeCatalogo_();
    Logger.log(JSON.stringify(res, null, 2));
    return res;
  } catch (e) {
    const errInfo = {
      ok: false,
      error: "Exception en wrapper: " + (e && e.message ? e.message : String(e)),
      stack: e && e.stack ? String(e.stack).split('\n').slice(0, 6).join(' | ') : '',
      type: e && e.name ? e.name : 'unknown',
    };
    Logger.log(JSON.stringify(errInfo, null, 2));
    return errInfo;
  }
}

/** Diagnóstico mínimo — NO toca datos. Verifica acceso a las 3 hojas y
 *  reporta sus dimensiones. Si esta falla, el problema es de
 *  permisos/autorización, no de la lógica. */
function diagAlojamientos() {
  try {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const out = {
      ok: true,
      spreadsheet: ss.getName(),
      sheets: ss.getSheets().map(s => ({
        name: s.getName(),
        rows: s.getLastRow(),
        cols: s.getLastColumn()
      })),
    };
    // Buscar específicamente las 3 hojas
    const find = (name) => {
      let s = ss.getSheetByName(name);
      if (s) return { found: true, exact: true, name: s.getName(), rows: s.getLastRow() };
      const all = ss.getSheets();
      for (var k = 0; k < all.length; k++) {
        if (all[k].getName().toLowerCase() === name.toLowerCase()) {
          return { found: true, exact: false, name: all[k].getName(), rows: all[k].getLastRow() };
        }
      }
      return { found: false };
    };
    out.alojamientos  = find(ALOJAMIENTOS_SHEET);
    out.reservas_lod  = find(LODGIFY_SHEET);
    out.reservaciones = find(RESERVACIONES_SHEET);
    Logger.log(JSON.stringify(out, null, 2));
    return out;
  } catch (e) {
    const errInfo = { ok: false, error: String(e && e.message || e), stack: String(e && e.stack || '').split('\n').slice(0,5).join(' | ') };
    Logger.log(JSON.stringify(errInfo, null, 2));
    return errInfo;
  }
}

/** One-shot cleanup: para cada fila de Reservaciones con "Lodgify Id" no
 *  vacío, busca el booking en Reservas_Lodgify para obtener HouseId/
 *  HouseName, resuelve la Propiedad/# Departamento canónica vía el
 *  catálogo "alojamientos", y reescribe esos campos si difieren.
 *  Idempotente: si ya está correcto, no toca. */
function homologarPropiedadesDesdeCatalogo_() {
  try {
    ensureNormalizedSheets_();
    const ss = getSpreadsheet_();
    const shR = ss.getSheetByName(RESERVACIONES_SHEET);
    if (!shR) return { ok:false, error:"No existe la hoja '" + RESERVACIONES_SHEET + "'." };
    // Tolerancia al case del nombre de la hoja Lodgify
    let shL = ss.getSheetByName(LODGIFY_SHEET);
    if (!shL) {
      const all = ss.getSheets();
      for (var k = 0; k < all.length; k++) {
        if (all[k].getName().toLowerCase() === LODGIFY_SHEET.toLowerCase()) { shL = all[k]; break; }
      }
    }
    if (!shL) return { ok:false, error:"No existe la hoja '" + LODGIFY_SHEET + "'." };

    const alojIdx = buildAlojamientosIndex_();
    const alojCount = Object.keys(alojIdx.byHouseId).length + Object.keys(alojIdx.byHouseName).length;
    if (!alojCount) {
      return { ok:false, error:"Catálogo '" + ALOJAMIENTOS_SHEET + "' vacío o no existe. Verifica que la hoja exista y tenga columnas: HouseName, HouseId, Propiedad, '# Departamento'." };
    }

    const headersR = shR.getRange(1, 1, 1, shR.getLastColumn()).getValues()[0];
    const idxLod  = headersR.indexOf("Lodgify Id");
    const idxProp = headersR.indexOf("Propiedad");
    const idxDpt  = headersR.indexOf("# Departamento");
    if (idxLod < 0 || idxProp < 0 || idxDpt < 0) {
      return { ok:false, error:"Faltan columnas en Reservaciones (Lodgify Id / Propiedad / # Departamento)." };
    }

    // Index Lodgify bookings por Id
    const headersL = shL.getRange(1, 1, 1, shL.getLastColumn()).getValues()[0];
    const colLId = headersL.indexOf("Id");
    const colLHId = headersL.indexOf("HouseId");
    const colLHN  = headersL.indexOf("HouseName");
    const colLRTN = headersL.indexOf("RoomTypeNames");
    if (colLId < 0) return { ok:false, error:"En hoja '" + LODGIFY_SHEET + "' no encontré la columna 'Id'." };
    const lastL = shL.getLastRow();
    const lgById = {};
    if (lastL >= 2) {
      const dataL = shL.getRange(2, 1, lastL - 1, headersL.length).getDisplayValues();
      for (var i = 0; i < dataL.length; i++) {
        const id = String(dataL[i][colLId] || "").trim();
        if (!id) continue;
        lgById[id] = {
          HouseId: colLHId >= 0 ? String(dataL[i][colLHId] || "").trim() : "",
          HouseName: colLHN >= 0 ? String(dataL[i][colLHN] || "").trim() : "",
          RoomTypeNames: colLRTN >= 0 ? String(dataL[i][colLRTN] || "").trim() : "",
        };
      }
    }

    const lastR = shR.getLastRow();
    if (lastR < 2) return { ok:true, scanned:0, fixed:0 };

    // BATCH WRITE: leemos las 2 columnas, modificamos en memoria, escribimos
    // todas las celdas en una sola llamada setValues. Esto evita el timeout
    // que tiene Apps Script al hacer 600+ setValue individuales.
    const numRows = lastR - 1;
    const propRange = shR.getRange(2, idxProp + 1, numRows, 1);
    const dptRange  = shR.getRange(2, idxDpt  + 1, numRows, 1);
    const lodRange  = shR.getRange(2, idxLod  + 1, numRows, 1);
    const propVals = propRange.getValues();
    const dptVals  = dptRange.getValues();
    const lodVals  = lodRange.getDisplayValues();
    let scanned = 0, fixed = 0;
    for (var r = 0; r < numRows; r++) {
      const lodId = String(lodVals[r][0] || "").trim();
      if (!lodId) continue;
      const lg = lgById[lodId];
      if (!lg) continue;
      scanned++;
      const expected = resolvePropiedadFromAloj_(alojIdx, lg);
      const currProp = String(propVals[r][0] || "").trim();
      const currDpt  = String(dptVals[r][0]  || "").trim();
      if (currProp !== expected.propiedad || currDpt !== expected.departamento) {
        if (expected.propiedad)    propVals[r][0] = expected.propiedad;
        if (expected.departamento) dptVals[r][0]  = expected.departamento;
        fixed++;
      }
    }
    // Escritura batch — 2 llamadas en total, no 1200
    propRange.setValues(propVals);
    dptRange.setValues(dptVals);
    SpreadsheetApp.flush();
    return { ok:true, scanned:scanned, fixed:fixed, aloj_count:alojCount };
  } catch (e) {
    return { ok:false, error:"Exception: " + (e && e.message ? e.message : String(e)),
             stack: e && e.stack ? String(e.stack).split('\n').slice(0, 5).join(' | ') : '' };
  }
}

/** Wrapper PÚBLICO para ejecutar la deduplicación one-shot desde el editor. */
function dedupeReservaciones() {
  const res = dedupeReservacionesByPhoneArrival_();
  Logger.log(JSON.stringify(res, null, 2));
  return res;
}

/** One-shot cleanup: agrupa filas de Reservaciones por (phoneTail10 +
 *  Fecha de ingreso ISO) y, cuando hay >1 filas en el grupo, las fusiona
 *  en una sola "keeper" preservando los valores no-vacíos de TODAS. Borra
 *  las duplicadas. Idempotente: si ya está limpio, no hace nada.
 *
 *  Política de "keeper" dentro del grupo (en este orden):
 *    1) La fila CON Lodgify Id (auto-propagada). Si hay varias con distintos
 *       Lodgify Id, se asume que son reservas distintas (mismo huésped,
 *       mismas fechas) — NO se tocan.
 *    2) Si ninguna tiene Lodgify Id (2 manuales): se queda la primera (la
 *       de menor número de fila) y el resto se merge en ella.
 *
 *  Merge: para cada columna (excepto ID/Cel/Lodgify Id/Fechas), si el
 *  keeper tiene vacío y alguna otra fila tiene valor, se copia ese valor.
 *  Se prioriza el valor más largo (formularios manuales > datos básicos
 *  de Lodgify-sync).
 *
 *  USA `.getValues()` para evitar el bug locale es-MX que invertía mes/día
 *  cuando "Fecha de ingreso" se almacenaba como Date object.
 *
 *  Llámala desde el editor (dropdown "dedupeReservaciones" → ▶ Ejecutar). */
function dedupeReservacionesByPhoneArrival_() {
  ensureNormalizedSheets_();
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  const idxCel = headers.indexOf("Cel/Whatsapp (principal)");
  const idxFi  = headers.indexOf("Fecha de ingreso");
  const idxLod = headers.indexOf("Lodgify Id");
  const idxId  = headers.indexOf("ID");
  if (idxCel < 0 || idxFi < 0 || idxLod < 0 || idxId < 0) {
    return { ok:false, error:"Faltan columnas (Cel/Fecha/Lodgify Id/ID)." };
  }
  const lastRow = sheet.getLastRow();
  if (lastRow < 3) return { ok:true, scanned:0, merged:0, deleted:0 };

  const data = sheet.getRange(2, 1, lastRow - 1, headers.length).getValues();
  // Agrupa por (phoneTail|ingresoIso)
  const groups = {};
  for (let i = 0; i < data.length; i++) {
    const tail = String(data[i][idxCel] || "").replace(/\D/g, "").slice(-10);
    const ingIso = lodgifyDateToIso_(data[i][idxFi]);
    if (!tail || !ingIso) continue;
    const key = tail + "|" + ingIso;
    if (!groups[key]) groups[key] = [];
    groups[key].push({ row: i + 2, idx: i, lodId: String(data[i][idxLod] || "").trim() });
  }

  // Identificadores que NO se merge / no se sobrescriben
  const PROTECTED = new Set([
    "ID", "Cel/Whatsapp (principal)",
    "Fecha de ingreso", "Fecha de salida",
  ]);

  const rowsToDelete = [];
  let mergedCount = 0;
  let skippedDifferentLodgify = 0;
  Object.keys(groups).forEach(key => {
    const grp = groups[key];
    if (grp.length < 2) return;
    // ¿Varias filas con DISTINTOS Lodgify Id? → son reservas distintas
    const distinctLodIds = new Set(grp.map(g => g.lodId).filter(Boolean));
    if (distinctLodIds.size > 1) { skippedDifferentLodgify++; return; }
    // Keeper: la fila con Lodgify Id (si existe), o la primera por número de
    // fila (estable, predecible).
    const withLod = grp.find(g => g.lodId);
    const keeper = withLod || grp.slice().sort((a,b) => a.row - b.row)[0];
    const others = grp.filter(g => g.row !== keeper.row);

    // Merge column-by-column: preserva el valor más informativo.
    //   - PROTECTED y "Lodgify Id" no se sobrescriben en el keeper.
    //   - Si el keeper está vacío y alguna otra fila tiene valor → copia ese.
    //   - Si ambos tienen valor, conserva el del keeper (no sobrescribir
    //     datos que el usuario ya vio).
    for (let col = 0; col < headers.length; col++) {
      const header = headers[col];
      if (PROTECTED.has(header) || header === "Lodgify Id") continue;
      const keepVal = String(data[keeper.idx][col] == null ? "" : data[keeper.idx][col]).trim();
      if (keepVal) continue; // ya tiene algo, no tocar
      // Busca el primer valor no vacío entre las demás
      let bestVal = "", bestRaw = null;
      for (const o of others) {
        const raw = data[o.idx][col];
        const s = String(raw == null ? "" : raw).trim();
        if (s && s.length > bestVal.length) { bestVal = s; bestRaw = raw; }
      }
      if (bestVal) sheet.getRange(keeper.row, col + 1).setValue(bestRaw);
    }
    others.forEach(o => rowsToDelete.push(o.row));
    mergedCount++;
  });

  // Borrar de mayor a menor para no invalidar índices
  rowsToDelete.sort((a, b) => b - a);
  rowsToDelete.forEach(r => sheet.deleteRow(r));
  SpreadsheetApp.flush();

  return {
    ok: true,
    scanned: data.length,
    groups_with_dupes: mergedCount,
    rows_deleted: rowsToDelete.length,
    skipped_different_lodgify: skippedDifferentLodgify,
  };
}

/** Devuelve TODAS las filas de la hoja "alojamientos" como objetos
 *  { columna: valor }. Se usa desde el frontend para homologar los
 *  nombres de propiedad entre las hojas "Reservas_Lodgify" (HouseName /
 *  HouseId) y "Reservaciones" (Propiedad / # Departamento). */
// ─── Breezeway: persistencia de alertas en sheet ───────────────────────────
/** Inserta una alerta del webhook de Breezeway en la hoja Breezeway_Alerts.
 *  Crea la hoja con encabezados si no existe. Hace dedupe por (task_id +
 *  event_type + finished_at) para que reintentos de Breezeway no metan
 *  filas duplicadas.  El backend Cloud Run llama esto vía CHECKIN_WEB_APP_URL
 *  con action="breezeway_alert" en cada webhook recibido. */
/** Detecta columnas de BREEZEWAY_ALERTS_HEADERS que faltan en la hoja
 *  existente y las AGREGA al final (sin tocar las posiciones de las
 *  existentes, así no rompemos referencias). Idempotente. */
function migrateBreezewayHeaders_(sh) {
  if (!sh) return;
  const lastCol = Math.max(sh.getLastColumn(), 1);
  const current = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(v => String(v || ""));
  const missing = BREEZEWAY_ALERTS_HEADERS.filter(h => current.indexOf(h) < 0);
  if (!missing.length) return;
  // Agrega las que faltan al final
  sh.getRange(1, lastCol + 1, 1, missing.length)
    .setValues([missing])
    .setFontWeight("bold")
    .setBackground("#7c3aed")
    .setFontColor("#ffffff");
}

function saveBreezewayAlert_(data) {
  if (!data || typeof data !== "object") {
    return { ok: false, error: "Payload vacío." };
  }
  const ss = getSpreadsheet_();
  let sh = ss.getSheetByName(BREEZEWAY_ALERTS_SHEET);
  if (!sh) {
    sh = ss.insertSheet(BREEZEWAY_ALERTS_SHEET);
    sh.getRange(1, 1, 1, BREEZEWAY_ALERTS_HEADERS.length)
      .setValues([BREEZEWAY_ALERTS_HEADERS])
      .setFontWeight("bold")
      .setBackground("#7c3aed")
      .setFontColor("#ffffff");
    sh.setFrozenRows(1);
  } else {
    // Auto-migra: si la hoja existe pero le faltan columnas nuevas
    // (due_date, started_at, assigned_to, etc.), las agrega al final.
    migrateBreezewayHeaders_(sh);
  }
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  // UPSERT — busca mismo task_id + event_type + finished_at en últimas 500
  // filas. Si existe, SOBRESCRIBE esa fila con los datos nuevos (reprogramaciones
  // u otros cambios sin event_type distinto se reflejan). Si no, inserta.
  const idxTaskId = headers.indexOf("task_id");
  const idxEvent  = headers.indexOf("event_type");
  const idxFin    = headers.indexOf("finished_at");
  const idxId     = headers.indexOf("id");
  const lastRow = sh.getLastRow();
  let matchRow = -1, matchId = "";
  if (lastRow > 1 && idxTaskId >= 0) {
    const window = Math.min(500, lastRow - 1);
    const startRow = lastRow - window + 1;
    const rows = sh.getRange(startRow, 1, window, headers.length).getValues();
    const tId = normalizeKeyValue_(data.task_id);
    const tEv = normalizeKeyValue_(data.event_type);
    const tFin = normalizeKeyValue_(data.finished_at);
    for (let i = 0; i < rows.length; i++) {
      if (normalizeKeyValue_(rows[i][idxTaskId]) === tId &&
          normalizeKeyValue_(rows[i][idxEvent])  === tEv &&
          normalizeKeyValue_(rows[i][idxFin])    === tFin && tId && tEv) {
        matchRow = startRow + i;
        if (idxId >= 0) matchId = rows[i][idxId];
        break;
      }
    }
  }
  const id = matchId || Utilities.getUuid();
  const row = headers.map(h => {
    if (h === "id") return id;
    if (h === "received_at") return nowIso_();
    const v = data[h];
    if (v == null) return "";
    if (typeof v === "object") return JSON.stringify(v);
    return String(v);
  });
  if (matchRow > 0) {
    sh.getRange(matchRow, 1, 1, headers.length).setValues([row]);
    return { ok: true, upserted: "updated", row_number: matchRow };
  }
  sh.appendRow(row);
  // Recorta si crece demasiado (mantener solo BREEZEWAY_ALERTS_MAX filas)
  const after = sh.getLastRow();
  if (after - 1 > BREEZEWAY_ALERTS_MAX) {
    const excess = after - 1 - BREEZEWAY_ALERTS_MAX;
    sh.deleteRows(2, excess);
  }
  // Notificación push al admin SOLO en eventos accionables:
  //   - event_type = task-completed
  //   - task_type = housekeeping (los Checkouts terminados son los importantes)
  // Otros eventos (in-progress, scheduled, mantenimiento) NO disparan push
  // para evitar spam.
  try {
    const ev = String(data.event_type || "").toLowerCase();
    const td = String(data.task_type || "").toLowerCase();
    if (ev === "task-completed" && td.indexOf("housekeeping") >= 0) {
      const propName = String(data.property_name || "Alojamiento").trim();
      const finishedBy = String(data.finished_by || "").trim();
      const taskName = String(data.task_name || "Aseo").trim();
      queueNotification_({
        target: ADMIN_PHONE_KEY,
        category: "recordatorios",
        title: "✅ Aseo terminado",
        body: propName + " · " + taskName + (finishedBy ? " · por " + finishedBy : ""),
        tag: "bzw-task-" + String(data.task_id || id),
        source: "breezeway",
      });
    }
  } catch (e) {
    // Si la notificación falla, no rompemos la persistencia.
    Logger.log("[BZW] notify push falló: " + (e && e.message ? e.message : e));
  }
  return { ok: true, id, inserted: true };
}

/** Inserta MUCHAS alertas en una sola invocación. Esperado:
 *  data.alerts = [{event_type, task_id, ...}, ...]
 *  Dedupe contra el sheet en una sola lectura (no por cada inserción).
 *  USA LockService para SERIALIZAR: si manual-sync + auto-sync corren
 *  en paralelo, sin lock ambos leen el sheet vacío y AMBOS insertan
 *  todo → duplicados. El lock fuerza ejecución secuencial. */
function saveBreezewayAlertsBulk_(data) {
  const incoming = Array.isArray(data && data.alerts) ? data.alerts : [];
  if (!incoming.length) return { ok: true, inserted: 0, skipped: 0 };
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(30000); // espera hasta 30s
  } catch (e) {
    return { ok: false, error: "No se pudo adquirir lock (otro sync en curso)." };
  }
  try {
    return saveBreezewayAlertsBulkLocked_(incoming);
  } finally {
    lock.releaseLock();
  }
}

function saveBreezewayAlertsBulkLocked_(incoming) {
  const ss = getSpreadsheet_();
  let sh = ss.getSheetByName(BREEZEWAY_ALERTS_SHEET);
  if (!sh) {
    sh = ss.insertSheet(BREEZEWAY_ALERTS_SHEET);
    sh.getRange(1, 1, 1, BREEZEWAY_ALERTS_HEADERS.length)
      .setValues([BREEZEWAY_ALERTS_HEADERS])
      .setFontWeight("bold")
      .setBackground("#7c3aed")
      .setFontColor("#ffffff");
    sh.setFrozenRows(1);
  } else {
    // Auto-migra columnas faltantes (idempotente)
    migrateBreezewayHeaders_(sh);
  }
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const idxTaskId = headers.indexOf("task_id");
  const idxEvent  = headers.indexOf("event_type");
  const idxFin    = headers.indexOf("finished_at");
  const idxId     = headers.indexOf("id");
  // Mapa key → rowNumber de las filas existentes (una sola lectura del sheet).
  // CRÍTICO: normalizeKeyValue_ para que Date objects se serialicen igual
  // que strings ISO. Si no, las keys no matcheaban → duplicados.
  const existingRowByKey = new Map();
  const existingIdByRow = new Map(); // rowNumber → uuid existente (para conservarlo)
  const lastRow = sh.getLastRow();
  if (lastRow > 1 && idxTaskId >= 0) {
    const rows = sh.getRange(2, 1, lastRow - 1, headers.length).getValues();
    for (let i = 0; i < rows.length; i++) {
      const k = normalizeKeyValue_(rows[i][idxTaskId]) + "|" +
                normalizeKeyValue_(rows[i][idxEvent])  + "|" +
                normalizeKeyValue_(rows[i][idxFin]);
      const rowNum = i + 2; // +2 = header + 0-based
      existingRowByKey.set(k, rowNum);
      if (idxId >= 0) existingIdByRow.set(rowNum, rows[i][idxId]);
    }
  }
  const toInsert = [];
  const toUpdate = []; // [{rowNum, rowValues}]
  let skipped = 0;
  const now = nowIso_();
  for (const a of incoming) {
    const tId  = normalizeKeyValue_(a.task_id);
    const tEv  = normalizeKeyValue_(a.event_type);
    const tFin = normalizeKeyValue_(a.finished_at);
    const k    = tId + "|" + tEv + "|" + tFin;
    if (tId && tEv && existingRowByKey.has(k)) {
      // UPSERT: ya hay una fila con esta key → la reescribimos con los datos
      // nuevos. Conservamos el UUID original (id) y el received_at original
      // si vienen en la fila vieja (para no perder timeline). Esto resuelve
      // el caso "reprogramé scheduled_date pero el webhook no llegó → al
      // sincronizar no se actualiza la fecha".
      const rowNum = existingRowByKey.get(k);
      const existingId = existingIdByRow.get(rowNum);
      const rowValues = headers.map(h => {
        if (h === "id") return existingId || Utilities.getUuid();
        if (h === "received_at") return a.received_at || now;
        const v = a[h];
        if (v == null) return "";
        if (typeof v === "object") return JSON.stringify(v);
        return String(v);
      });
      toUpdate.push({ rowNum, rowValues });
      continue;
    }
    if (tId && tEv) existingRowByKey.set(k, -1); // marca dentro del batch
    const row = headers.map(h => {
      if (h === "id") return Utilities.getUuid();
      if (h === "received_at") return a.received_at || now;
      const v = a[h];
      if (v == null) return "";
      if (typeof v === "object") return JSON.stringify(v);
      return String(v);
    });
    toInsert.push(row);
  }
  // UPDATES en bloque — una llamada setValues por fila (no hay forma batch
  // de update no-contiguo en Apps Script, pero son pocas filas usualmente).
  for (const u of toUpdate) {
    sh.getRange(u.rowNum, 1, 1, headers.length).setValues([u.rowValues]);
  }
  if (toInsert.length) {
    const startRow = sh.getLastRow() + 1;
    sh.getRange(startRow, 1, toInsert.length, headers.length).setValues(toInsert);
    // Recorta si crece demasiado
    const after = sh.getLastRow();
    if (after - 1 > BREEZEWAY_ALERTS_MAX) {
      sh.deleteRows(2, (after - 1) - BREEZEWAY_ALERTS_MAX);
    }
  }
  return { ok: true, inserted: toInsert.length, updated: toUpdate.length, skipped };
}

/** Borra filas de prueba/smoke-test del sheet de alertas. Detecta por
 *  task_id que empieza con TEST-/SMOKE- o es 99999999, o por raw_json vacío. */
/** Normaliza un valor para usarlo como key de dedupe.
 *  CRÍTICO: Sheets auto-convierte strings ISO a Date objects al escribir.
 *  Al leer de vuelta, String(date) devuelve formato local
 *  ("Mon Jun 15 2026 11:56:48 GMT-0600") — distinto del ISO original.
 *  Sin esta normalización las keys no matcheaban → todo se duplicaba. */
function normalizeKeyValue_(v) {
  if (v == null || v === "") return "";
  // Date object → "yyyy-MM-ddTHH:mm:ss" en TZ del script (mismo formato que
  // entrega la API de Breezeway, evita el shift UTC de .toISOString()).
  if (Object.prototype.toString.call(v) === "[object Date]") {
    return Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd'T'HH:mm:ss");
  }
  // String: trim + cortar a 19 chars (descarta milisegundos y zona horaria).
  // Cubre "2026-05-18T14:00:59.000Z" vs "2026-05-18T14:00:59" como iguales.
  return String(v).trim().slice(0, 19);
}

/** RESET TOTAL de la hoja Breezeway_Alerts.
 *  Borra TODAS las filas de datos (conserva el header). Una sola operación
 *  → no hay timeout. Después de esto, hacer 🔄 Sincronizar en el módulo
 *  Breezeway repoblará la hoja con los datos correctos y SIN duplicados
 *  (gracias al dedupe normalizado).
 *  USO: en el editor de Apps Script, selecciona esta función y dale ▶. */
function resetBreezewayAlerts() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheetByName(BREEZEWAY_ALERTS_SHEET);
  if (!sh) { Logger.log("Sheet Breezeway_Alerts no existe."); return; }
  const lastRow = sh.getLastRow();
  if (lastRow < 2) { Logger.log("Hoja ya vacía."); return; }
  // Borra rango de datos (mantiene header)
  sh.getRange(2, 1, lastRow - 1, sh.getLastColumn()).clearContent();
  Logger.log("Reset OK: " + (lastRow - 1) + " filas borradas. Header conservado.");
  return { deleted: lastRow - 1 };
}

function cleanBreezewayTestRows_() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheetByName(BREEZEWAY_ALERTS_SHEET);
  if (!sh) return { ok: true, deleted: 0, reason: "Sheet no existe." };
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: true, deleted: 0 };
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const idxTaskId = headers.indexOf("task_id");
  const idxRaw    = headers.indexOf("raw_json");
  const idxEvent  = headers.indexOf("event_type");
  if (idxTaskId < 0) return { ok: false, error: "Falta columna task_id." };
  const data = sh.getRange(2, 1, lastRow - 1, headers.length).getValues();
  const rowsToDelete = [];
  for (let i = 0; i < data.length; i++) {
    const tId = String(data[i][idxTaskId] || "");
    const ev  = String(data[i][idxEvent] || "");
    const raw = String(data[i][idxRaw] || "");
    const isTest =
      /^(TEST|SMOKE)[-_]/i.test(tId) ||
      tId === "99999999" ||
      /smoke|test/i.test(ev) ||
      (!tId && !ev && !raw); // filas totalmente vacías
    if (isTest) rowsToDelete.push(i + 2);
  }
  // Borra de mayor a menor para no invalidar índices
  rowsToDelete.sort(function(a, b) { return b - a; });
  rowsToDelete.forEach(function(r) { sh.deleteRow(r); });
  SpreadsheetApp.flush();
  return { ok: true, deleted: rowsToDelete.length };
}

/** Devuelve las últimas N alertas (más recientes primero). */
/** Elimina filas duplicadas en Breezeway_Alerts comparando por
 *  (task_id|event_type|finished_at) — la misma key que usa el upsert.
 *  Conserva la fila MÁS RECIENTE por received_at (la última escrita gana).
 *  Devuelve { ok, scanned, deleted, remaining }. */
function dedupeBreezewayAlerts_() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheetByName(BREEZEWAY_ALERTS_SHEET);
  if (!sh) return { ok: false, error: "sheet no encontrado" };
  const lastRow = sh.getLastRow();
  if (lastRow < 3) return { ok: true, scanned: 0, deleted: 0, remaining: lastRow - 1 };
  const lastCol = sh.getLastColumn();
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0];
  const idxTask = headers.indexOf("task_id");
  const idxEv   = headers.indexOf("event_type");
  const idxFin  = headers.indexOf("finished_at");
  const idxRecv = headers.indexOf("received_at");
  if (idxTask < 0 || idxEv < 0 || idxFin < 0) {
    return { ok: false, error: "faltan columnas task_id/event_type/finished_at" };
  }
  const all = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
  // Por cada key, conservar la fila completa con received_at más reciente.
  const winnerByKey = new Map(); // key → row[]
  for (let i = 0; i < all.length; i++) {
    const row = all[i];
    if (!row[idxTask] && !row[idxEv]) continue; // saltar vacías
    const k = normalizeKeyValue_(row[idxTask]) + "|" +
              normalizeKeyValue_(row[idxEv])   + "|" +
              normalizeKeyValue_(row[idxFin]);
    const recv = idxRecv >= 0 ? String(row[idxRecv] || "") : "";
    const prev = winnerByKey.get(k);
    if (!prev) { winnerByKey.set(k, { row, recv }); continue; }
    if (recv > prev.recv) winnerByKey.set(k, { row, recv });
  }
  const winners = Array.from(winnerByKey.values()).map(w => w.row);
  // Borrado masivo: limpiar datos en bloque + reescribir solo winners.
  // (sh.deleteRow loop es O(N) cellEvent ≈ 50ms cada uno → tarda >6min con 22k.)
  if (lastRow > 1) sh.getRange(2, 1, lastRow - 1, lastCol).clearContent();
  if (winners.length) {
    sh.getRange(2, 1, winners.length, lastCol).setValues(winners);
  }
  SpreadsheetApp.flush();
  return {
    ok: true,
    scanned: all.length,
    deleted: all.length - winners.length,
    remaining: winners.length,
  };
}

/** Ordena Breezeway_Alerts por scheduled_date ASC (data rows). Después de
 *  esto, listBreezewayAlerts_ (que lee del final) devolverá siempre las
 *  tasks con scheduled_date MÁS RECIENTE en sus N filas. */
function sortBreezewayBySched_() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheetByName(BREEZEWAY_ALERTS_SHEET);
  if (!sh) return { ok: false, error: "sheet no encontrado" };
  const lastRow = sh.getLastRow();
  if (lastRow < 3) return { ok: true, sorted: 0, message: "nada que ordenar" };
  const lastCol = sh.getLastColumn();
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0];
  const schedIdx = headers.indexOf("scheduled_date");
  if (schedIdx < 0) return { ok: false, error: "columna scheduled_date no existe" };
  sh.getRange(2, 1, lastRow - 1, lastCol).sort({ column: schedIdx + 1, ascending: true });
  return { ok: true, sorted: lastRow - 1 };
}

function listBreezewayAlerts_(params) {
  const limit = Math.min(parseInt(params.limit, 10) || 100, 200000);
  // Filtro opcional por scheduled_date >= from_sched (formato YYYY-MM-DD).
  // El orden de inserción del sheet no siempre corresponde al orden de
  // scheduled_date — por eso si pedimos "latest N" puede haber tasks recientes
  // fuera de la ventana. Con from_sched, leemos toda la hoja y filtramos por
  // fecha en memoria, garantizando que TODAS las tasks con scheduled_date
  // >= from_sched salgan en la respuesta.
  const fromSched = String(params.from_sched || "").slice(0, 10);
  const ss = getSpreadsheet_();
  const sh = ss.getSheetByName(BREEZEWAY_ALERTS_SHEET);
  if (!sh) return { ok: true, alerts: [], count: 0 };
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: true, alerts: [], count: 0 };
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  let alerts;
  if (fromSched) {
    // Leer TODA la hoja y filtrar por scheduled_date en memoria.
    const all = sh.getRange(2, 1, lastRow - 1, headers.length).getValues();
    const schedIdx = headers.indexOf("scheduled_date");
    const finIdx = headers.indexOf("finished_at");
    const filtered = [];
    for (const r of all) {
      let sched = "";
      if (schedIdx >= 0) {
        const v = r[schedIdx];
        if (v instanceof Date) {
          sched = Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd");
        } else {
          sched = String(v || "").slice(0, 10);
        }
      }
      if (!sched && finIdx >= 0) {
        const v = r[finIdx];
        if (v instanceof Date) sched = Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd");
        else sched = String(v || "").slice(0, 10);
      }
      if (sched && sched >= fromSched) filtered.push(r);
    }
    // Ordenar por scheduled_date DESC (más reciente primero) y cortar al límite
    if (schedIdx >= 0) {
      filtered.sort((a, b) => {
        const sa = a[schedIdx] instanceof Date ? Utilities.formatDate(a[schedIdx], Session.getScriptTimeZone(), "yyyy-MM-dd") : String(a[schedIdx]||"").slice(0,10);
        const sb = b[schedIdx] instanceof Date ? Utilities.formatDate(b[schedIdx], Session.getScriptTimeZone(), "yyyy-MM-dd") : String(b[schedIdx]||"").slice(0,10);
        return sb.localeCompare(sa);
      });
    }
    const sliced = filtered.slice(0, limit);
    alerts = sliced.map(r => {
      const obj = {};
      headers.forEach((h, i) => { obj[h] = r[i]; });
      if (obj.raw_json) { try { obj.raw = JSON.parse(obj.raw_json); } catch (_) {} }
      return obj;
    });
  } else {
    // Comportamiento legacy: leer desde el final (latest N por inserción).
    const window = Math.min(limit, lastRow - 1);
    const startRow = lastRow - window + 1;
    const rows = sh.getRange(startRow, 1, window, headers.length).getValues();
    alerts = rows.map(r => {
      const obj = {};
      headers.forEach((h, i) => { obj[h] = r[i]; });
      if (obj.raw_json) { try { obj.raw = JSON.parse(obj.raw_json); } catch (_) {} }
      return obj;
    }).reverse();
  }
  return { ok: true, count: alerts.length, alerts };
}

function listAlojamientos_() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheetByName(ALOJAMIENTOS_SHEET);
  if (!sh) return { ok: true, rows: [], note: 'Hoja "alojamientos" no existe.' };
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return { ok: true, rows: [] };
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(v => String(v || '').trim());
  const data = sh.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();
  const rows = data.map(r => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      if (headers[i]) obj[headers[i]] = r[i];
    }
    return obj;
  });
  return { ok: true, rows, total: rows.length };
}

function listDispositivos_() {
  const ss = getSpreadsheet_();
  function _normName(s){
    return String(s || '').trim().toLowerCase()
      .normalize("NFD").replace(new RegExp("[̀-ͯ]", "g"), "")
      .replace(/[^a-z0-9]/g, '');
  }
  // Acepta "Dispositivos", "dispositivos", "DISPOSITIVOS", "Dispositívos", etc.
  const sheets = ss.getSheets();
  const sh = sheets.find(function(s){
    var n = _normName(s.getName());
    return n === 'dispositivos' || n === 'dispositivo' || n.indexOf('dispositiv') === 0;
  });
  if (!sh) {
    var names = sheets.map(function(s){ return s.getName(); });
    return { ok: true, rows: [], note: 'No se encontró hoja "Dispositivos". Hojas disponibles: ' + names.join(', ') };
  }
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return { ok: true, rows: [] };
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(v => String(v || '').trim());
  const data = sh.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();
  const rows = data.map(r => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      if (headers[i]) obj[headers[i]] = r[i];
    }
    return obj;
  });
  return { ok: true, rows, total: rows.length };
}

/** Lista la hoja "Personal" como filas de objetos { <header>: <valor> }.
 *  Consumido por el módulo Incidencias del frontend para poblar el
 *  catálogo de personas involucradas. */
function listPersonal_() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheetByName('Personal');
  if (!sh) return { ok: true, rows: [], note: 'Hoja "Personal" no existe.' };
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return { ok: true, rows: [] };
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(v => String(v || '').trim());
  const data = sh.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();
  const rows = data.map(r => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      if (headers[i]) obj[headers[i]] = r[i];
    }
    return obj;
  }).filter(o => String(o['Nombre'] || '').trim()); // descarta filas sin Nombre
  return { ok: true, rows, total: rows.length };
}

// ─── INCIDENCIAS ─────────────────────────────────────────────────────────────
// Sube una foto base64 a la carpeta /Drive/Incidencias/{año}/{mesEs} y
// devuelve la URL pública. Crea la jerarquía si no existe.
function uploadIncidenciaImage_(data) {
  try {
    var fileObj = data.file ? (typeof data.file === 'string' ? JSON.parse(data.file) : data.file) : null;
    if (!fileObj || !fileObj.base64) return { ok: false, error: 'Sin base64' };
    var rawFecha = String(data.fecha || '').slice(0, 10) || Utilities.formatDate(new Date(), Session.getScriptTimeZone() || 'America/Monterrey', 'yyyy-MM-dd');
    var parts = rawFecha.split('-');
    var anio = parts[0] || String(new Date().getFullYear());
    var mes = parseInt(parts[1] || '1', 10);
    var meses = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
    var mesStr = meses[mes - 1] || 'Enero';
    var aloj = String(data.alojamiento || 'sin_alojamiento').replace(/[\/\\:*?"<>|]/g, '_').slice(0, 60);
    var ts = Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyyMMdd_HHmmss');
    var ext = (fileObj.fileName || '.jpg').split('.').pop().toLowerCase();
    if (ext.length > 5) ext = 'jpg';
    var name = aloj.replace(/\s+/g, '_').slice(0, 30) + '_' + ts + '.' + ext;
    var folder = DriveApp.getRootFolder();
    folder = getOrCreateFolder_(folder, 'Check Inn - Sistemas');
    folder = getOrCreateFolder_(folder, 'Drive');
    folder = getOrCreateFolder_(folder, 'Incidencias');
    folder = getOrCreateFolder_(folder, anio);
    folder = getOrCreateFolder_(folder, mesStr);
    var bytes = Utilities.base64Decode(fileObj.base64);
    var blob = Utilities.newBlob(bytes, fileObj.mimeType || 'image/jpeg', name);
    var file = folder.createFile(blob);
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    var id = file.getId();
    // URL para EMBED en <img>: el thumbnail de Drive funciona cross-origin
    // y devuelve la imagen real. file.getUrl() devuelve la página viewer.
    var directUrl = 'https://drive.google.com/thumbnail?id=' + id + '&sz=w2000';
    return { ok: true, url: directUrl, viewerUrl: file.getUrl(), id: id, name: file.getName() };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function getOrCreateFolder_(parent, name) {
  var iter = parent.getFoldersByName(name);
  if (iter.hasNext()) return iter.next();
  return parent.createFolder(name);
}

// ═══════════════════════════════════════════════════════════════════════
// RH › Obligaciones — sube archivos a la carpeta raíz de RH/Obligaciones
// Estructura: {ROOT}/{year}/{MM-MesNombre}/{kind}/[empleado/]archivo.ext
// ═══════════════════════════════════════════════════════════════════════
var RH_OBL_ROOT_FOLDER_ID = '1S4M4PPG0UmSlDjY8QHu7-sr3Z3bhzCrs';
var RH_OBL_KINDS = {
  'cuota_formato':     { label: 'Formato_cuotas',      empleado: false },
  'cuota_comprobante': { label: 'Comprobante_cuotas',  empleado: false },
  'recibo_xml':        { label: 'Recibo_XML',          empleado: true  },
  'recibo_pdf':        { label: 'Recibo_PDF',          empleado: true  },
};
var RH_OBL_MESES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];

function rhObligacionMonthFolderName_(month) {
  var mm = ('0' + month).slice(-2);
  return mm + '-' + RH_OBL_MESES[month - 1];
}
function rhObligacionSafe_(s) {
  return String(s || '').replace(/[\/\\:*?"<>|]/g, '_').replace(/\s+/g, '_').slice(0, 80);
}

function rhUploadObligacion_(data) {
  try {
    var year = parseInt(data.year, 10);
    var month = parseInt(data.month, 10);
    var kind = String(data.kind || '');
    var kindDef = RH_OBL_KINDS[kind];
    if (!year || !month || month < 1 || month > 12 || !kindDef) {
      return { ok: false, error: 'Parámetros inválidos (year/month/kind)' };
    }
    var fileObj = data.file && (typeof data.file === 'string' ? JSON.parse(data.file) : data.file);
    if (!fileObj || !fileObj.base64) return { ok: false, error: 'Sin archivo' };

    var empleadoId = String(data.empleadoId || '');
    var empleadoNombre = String(data.empleadoNombre || '');
    if (kindDef.empleado && !empleadoId) return { ok: false, error: 'Falta empleadoId para ' + kind };

    var root = DriveApp.getFolderById(RH_OBL_ROOT_FOLDER_ID);
    var fYear = getOrCreateFolder_(root, String(year));
    var fMonth = getOrCreateFolder_(fYear, rhObligacionMonthFolderName_(month));
    var fKind = getOrCreateFolder_(fMonth, kindDef.label);
    var targetFolder = fKind;
    if (kindDef.empleado) {
      var empFolderName = rhObligacionSafe_((empleadoNombre || empleadoId)) + '_' + rhObligacionSafe_(empleadoId);
      targetFolder = getOrCreateFolder_(fKind, empFolderName);
    }

    // Borra archivos previos del mismo kind/empleado para que "subir" reemplace
    var existing = targetFolder.getFiles();
    while (existing.hasNext()) {
      try { existing.next().setTrashed(true); } catch (_) {}
    }

    var origName = String(fileObj.fileName || 'archivo');
    var ext = (origName.split('.').pop() || '').toLowerCase();
    if (!ext || ext.length > 6) ext = (kind === 'recibo_xml' ? 'xml' : kind === 'recibo_pdf' ? 'pdf' : 'pdf');
    var ts = Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyyMMdd_HHmmss');
    var baseName = kindDef.label + (kindDef.empleado ? '_' + rhObligacionSafe_(empleadoNombre || empleadoId) : '') + '_' + ts + '.' + ext;

    var bytes = Utilities.base64Decode(fileObj.base64);
    var blob = Utilities.newBlob(bytes, fileObj.mimeType || 'application/octet-stream', baseName);
    var file = targetFolder.createFile(blob);
    try { file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW); } catch (_) {}
    return { ok: true, url: file.getUrl(), id: file.getId(), name: file.getName() };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

var RH_OBL_TOTALES_SHEET = 'RH_Obligaciones_Totales';
function rhGetOrCreateTotalesSheet_() {
  var ss = getSpreadsheet_();
  var sh = ss.getSheetByName(RH_OBL_TOTALES_SHEET);
  if (!sh) {
    sh = ss.insertSheet(RH_OBL_TOTALES_SHEET);
    sh.appendRow(['Año', 'Mes', 'Total_pagado', 'Actualizado']);
    sh.getRange(1, 1, 1, 4).setFontWeight('bold').setBackground('#e0e7ff');
    sh.setFrozenRows(1);
  }
  return sh;
}
function rhSetObligacionTotal_(data) {
  try {
    var year = parseInt(data.year, 10);
    var month = parseInt(data.month, 10);
    var total = Number(data.total);
    if (!year || !month || month < 1 || month > 12 || !isFinite(total) || total < 0) {
      return { ok: false, error: 'Parámetros inválidos' };
    }
    var sh = rhGetOrCreateTotalesSheet_();
    var rng = sh.getDataRange().getValues();
    var ts = Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyy-MM-dd HH:mm:ss');
    for (var i = 1; i < rng.length; i++) {
      if (parseInt(rng[i][0], 10) === year && parseInt(rng[i][1], 10) === month) {
        sh.getRange(i + 1, 3).setValue(total);
        sh.getRange(i + 1, 4).setValue(ts);
        return { ok: true, total: total, actualizado: ts };
      }
    }
    sh.appendRow([year, month, total, ts]);
    return { ok: true, total: total, actualizado: ts };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}
function rhListObligacionTotales_(data) {
  try {
    var year = parseInt(data.year, 10) || (new Date()).getFullYear();
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName(RH_OBL_TOTALES_SHEET);
    if (!sh) return { ok: true, totales: {} };
    var rng = sh.getDataRange().getValues();
    var out = {};
    for (var i = 1; i < rng.length; i++) {
      if (parseInt(rng[i][0], 10) === year) {
        var m = parseInt(rng[i][1], 10);
        if (m >= 1 && m <= 12) out[m] = { total: Number(rng[i][2]) || 0, actualizado: String(rng[i][3] || '') };
      }
    }
    return { ok: true, totales: out };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function rhDeleteObligacion_(data) {
  try {
    var fileId = String(data.fileId || '').trim();
    if (!fileId) return { ok: false, error: 'Falta fileId' };
    var file = DriveApp.getFileById(fileId);
    file.setTrashed(true);
    return { ok: true, id: fileId };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function rhListObligaciones_(data) {
  try {
    var year = parseInt(data.year, 10) || (new Date()).getFullYear();
    var root = DriveApp.getFolderById(RH_OBL_ROOT_FOLDER_ID);
    var items = [];
    var iterYear = root.getFoldersByName(String(year));
    if (!iterYear.hasNext()) return { ok: true, items: items };
    var fYear = iterYear.next();
    var months = fYear.getFolders();
    while (months.hasNext()) {
      var fMonth = months.next();
      var monthName = fMonth.getName();
      // Espera "MM-Mes"
      var mm = parseInt(monthName.slice(0, 2), 10);
      if (!mm || mm < 1 || mm > 12) continue;
      var kinds = fMonth.getFolders();
      while (kinds.hasNext()) {
        var fKind = kinds.next();
        var kindLabel = fKind.getName();
        var kindKey = null;
        Object.keys(RH_OBL_KINDS).forEach(function (k) {
          if (RH_OBL_KINDS[k].label === kindLabel) kindKey = k;
        });
        if (!kindKey) continue;
        var kindDef = RH_OBL_KINDS[kindKey];
        if (kindDef.empleado) {
          var empFolders = fKind.getFolders();
          while (empFolders.hasNext()) {
            var fEmp = empFolders.next();
            // nombre = "Nombre_seguro_ID"
            var fname = fEmp.getName();
            var idMatch = fname.match(/_([^_]+)$/);
            var empleadoId = idMatch ? idMatch[1] : '';
            var files = fEmp.getFiles();
            while (files.hasNext()) {
              var f = files.next();
              items.push({ month: mm, kind: kindKey, empleadoId: empleadoId, name: f.getName(), url: f.getUrl(), id: f.getId() });
            }
          }
        } else {
          var ff = fKind.getFiles();
          while (ff.hasNext()) {
            var f2 = ff.next();
            items.push({ month: mm, kind: kindKey, empleadoId: '', name: f2.getName(), url: f2.getUrl(), id: f2.getId() });
          }
        }
      }
    }
    return { ok: true, items: items };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

// Inserta una fila en la hoja "Incidencias" con todos los campos del reporte.
// Si la hoja no existe, la crea con los headers correctos. fotos_urls: array
// de strings (URLs públicas de Drive) — se persisten como "url1, url2, ...".
function saveIncidencia_(data) {
  try {
    var payload = data.payload ? (typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload) : data;
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('Incidencias');
    var headers = [
      'ID', 'Timestamp', 'Fecha', 'Propiedad', '# Departamento', 'Alojamiento',
      'Personas', 'Motivos', 'Clasificacion', 'Nivel', 'Estatus', 'Reportante',
      'Descripcion', 'Acciones', 'Seguimiento', 'Fotos_count', 'Fotos_URLs'
    ];
    if (!sh) {
      sh = ss.insertSheet('Incidencias');
      sh.appendRow(headers);
      sh.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#fee2e2');
      sh.setFrozenRows(1);
    }
    var id = 'INC-' + Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyyMMdd-HHmmss') +
             '-' + Math.floor(Math.random() * 10000);
    var ts = Utilities.formatDate(new Date(), 'America/Monterrey', "yyyy-MM-dd HH:mm:ss");
    var personas = Array.isArray(payload.personas) ? payload.personas.join(', ') : String(payload.personas || '');
    var motivos  = Array.isArray(payload.motivos)  ? payload.motivos.join(', ')  : String(payload.motivos  || '');
    var clasif   = Array.isArray(payload.clasificaciones) ? payload.clasificaciones.join(', ') : String(payload.clasificaciones || '');
    var fotosUrls = Array.isArray(payload.fotos_urls) ? payload.fotos_urls.join(', ') : String(payload.fotos_urls || '');
    var fotosCount = Array.isArray(payload.fotos_urls) ? payload.fotos_urls.length : 0;
    var row = [
      id, ts,
      String(payload.fecha || ''),
      String(payload.propiedad || ''),
      String(payload.depto || ''),
      String(payload.alojamiento || ''),
      personas, motivos, clasif,
      String(payload.nivel || ''),
      String(payload.estatus || ''),
      String(payload.reportante || ''),
      String(payload.descripcion || ''),
      String(payload.acciones || ''),
      String(payload.seguimiento || ''),
      fotosCount, fotosUrls
    ];
    sh.appendRow(row);
    return { ok: true, id: id, timestamp: ts };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

// Actualiza una fila existente en "Incidencias". Encuentra la fila por la
// columna ID, y sobreescribe SOLO las columnas presentes en payload.fields.
// Arrays (personas, motivos, clasificaciones) llegan como CSV o array.
function updateIncidencia_(data) {
  try {
    var payload = data.payload ? (typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload) : data;
    var id = String(payload.id || '').trim();
    if (!id) return { ok: false, error: 'Falta id' };
    var fields = payload.fields || {};
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('Incidencias');
    if (!sh) return { ok: false, error: 'Hoja Incidencias no existe' };
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2) return { ok: false, error: 'Hoja vacía' };
    var headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(function (v) { return String(v || '').trim(); });
    var idCol = headers.indexOf('ID') + 1;
    if (!idCol) return { ok: false, error: 'Columna ID no encontrada' };
    // Busca la fila por ID (display values para match de string exacto)
    var ids = sh.getRange(2, idCol, lastRow - 1, 1).getDisplayValues();
    var rowIdx = -1;
    for (var i = 0; i < ids.length; i++) {
      if (String(ids[i][0]).trim() === id) { rowIdx = i + 2; break; }
    }
    if (rowIdx < 0) return { ok: false, error: 'ID no encontrado: ' + id };
    // Mapa de campos frontend → header en hoja
    var fieldMap = {
      fecha: 'Fecha',
      propiedad: 'Propiedad',
      depto: '# Departamento',
      alojamiento: 'Alojamiento',
      personas: 'Personas',
      motivos: 'Motivos',
      clasificaciones: 'Clasificacion',
      nivel: 'Nivel',
      estatus: 'Estatus',
      reportante: 'Reportante',
      descripcion: 'Descripcion',
      acciones: 'Acciones',
      seguimiento: 'Seguimiento',
      fotos_urls: 'Fotos_URLs',
      fotos_count: 'Fotos_count',
    };
    var updates = [];
    Object.keys(fields).forEach(function (k) {
      var header = fieldMap[k];
      if (!header) return;
      var col = headers.indexOf(header) + 1;
      if (!col) return;
      var v = fields[k];
      if (Array.isArray(v)) v = v.join(', ');
      sh.getRange(rowIdx, col).setValue(v == null ? '' : String(v));
      updates.push(header);
    });
    return { ok: true, id: id, row: rowIdx, updated: updates };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

// ═══════════════════════════════════════════════════════════════════
// OBJETOS OLVIDADOS — paralelo a Incidencias con campos propios
// ═══════════════════════════════════════════════════════════════════

function uploadObjetoImage_(data) {
  try {
    var fileObj = data.file ? (typeof data.file === 'string' ? JSON.parse(data.file) : data.file) : null;
    if (!fileObj || !fileObj.base64) return { ok: false, error: 'Sin base64' };
    var rawFecha = String(data.fecha || '').slice(0, 10) || Utilities.formatDate(new Date(), Session.getScriptTimeZone() || 'America/Monterrey', 'yyyy-MM-dd');
    var parts = rawFecha.split('-');
    var anio = parts[0] || String(new Date().getFullYear());
    var mes = parseInt(parts[1] || '1', 10);
    var meses = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
    var mesStr = meses[mes - 1] || 'Enero';
    var aloj = String(data.alojamiento || 'sin_alojamiento').replace(/[\/\\:*?"<>|]/g, '_').slice(0, 60);
    var ts = Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyyMMdd_HHmmss');
    var ext = (fileObj.fileName || '.jpg').split('.').pop().toLowerCase();
    if (ext.length > 5) ext = 'jpg';
    var name = aloj.replace(/\s+/g, '_').slice(0, 30) + '_' + ts + '.' + ext;
    var folder = DriveApp.getRootFolder();
    folder = getOrCreateFolder_(folder, 'Check Inn - Sistemas');
    folder = getOrCreateFolder_(folder, 'Drive');
    folder = getOrCreateFolder_(folder, 'Objetos_olvidados');
    folder = getOrCreateFolder_(folder, anio);
    folder = getOrCreateFolder_(folder, mesStr);
    var bytes = Utilities.base64Decode(fileObj.base64);
    var blob = Utilities.newBlob(bytes, fileObj.mimeType || 'image/jpeg', name);
    var file = folder.createFile(blob);
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    var id = file.getId();
    var directUrl = 'https://drive.google.com/thumbnail?id=' + id + '&sz=w2000';
    return { ok: true, url: directUrl, viewerUrl: file.getUrl(), id: id, name: file.getName() };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function saveObjeto_(data) {
  try {
    var payload = data.payload ? (typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload) : data;
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('Objetos_Olvidados');
    var headers = [
      'ID', 'Timestamp',
      'Fecha_encontrado', 'Fecha_entregado', 'Entregado_a',
      'Propiedad', '# Departamento', 'Alojamiento',
      'Reportante',
      'Categoria', 'Categoria_otro', 'Descripcion',
      'Lugar_resguardo', 'Lugar_otro',
      'Comentarios',
      'Fotos_count', 'Fotos_URLs',
    ];
    if (!sh) {
      sh = ss.insertSheet('Objetos_Olvidados');
      sh.appendRow(headers);
      sh.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#dbeafe');
      sh.setFrozenRows(1);
    }
    var id = 'OBJ-' + Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyyMMdd-HHmmss') +
             '-' + Math.floor(Math.random() * 10000);
    var ts = Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyy-MM-dd HH:mm:ss');
    var fotosUrls = Array.isArray(payload.fotos_urls) ? payload.fotos_urls.join(', ') : String(payload.fotos_urls || '');
    var fotosCount = Array.isArray(payload.fotos_urls) ? payload.fotos_urls.length : 0;
    // Mapa nombre-columna → valor. La fila se arma según el orden REAL de
    // headers en el sheet (robusto a reorden / inserción manual de columnas).
    var values = {
      'ID': id,
      'Timestamp': ts,
      'Fecha_encontrado': String(payload.fecha_encontrado || ''),
      'Fecha_entregado': String(payload.fecha_entregado || ''),
      'Entregado_a': String(payload.entregado_a || ''),
      'Propiedad': String(payload.propiedad || ''),
      '# Departamento': String(payload.depto || ''),
      'Alojamiento': String(payload.alojamiento || ''),
      'Reportante': String(payload.reportante || ''),
      'Categoria': String(payload.categoria || ''),
      'Categoria_otro': String(payload.categoria_otro || ''),
      'Descripcion': String(payload.descripcion || ''),
      'Lugar_resguardo': String(payload.lugar_resguardo || ''),
      'Lugar_otro': String(payload.lugar_otro || ''),
      'Comentarios': String(payload.comentarios || ''),
      'Fotos_count': fotosCount,
      'Fotos_URLs': fotosUrls,
    };
    var lastCol = sh.getLastColumn();
    var currentHeaders = lastCol ? sh.getRange(1, 1, 1, lastCol).getValues()[0].map(function(h){ return String(h||'').trim(); }) : [];
    // Si falta alguna columna esperada, la creamos al final preservando los datos.
    Object.keys(values).forEach(function(name) {
      if (currentHeaders.indexOf(name) === -1) {
        currentHeaders.push(name);
        sh.getRange(1, currentHeaders.length).setValue(name);
      }
    });
    var row = currentHeaders.map(function(h) {
      return Object.prototype.hasOwnProperty.call(values, h) ? values[h] : '';
    });
    sh.appendRow(row);
    return { ok: true, id: id, timestamp: ts };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function listObjetos_() {
  try {
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('Objetos_Olvidados');
    if (!sh) return { ok: true, rows: [], note: 'Hoja "Objetos_Olvidados" no existe.' };
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2 || lastCol < 1) return { ok: true, rows: [] };
    var headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(function (v) { return String(v || '').trim(); });
    var data = sh.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();
    var rows = data.map(function (r) {
      var obj = {};
      for (var i = 0; i < headers.length; i++) {
        if (headers[i]) obj[headers[i]] = r[i];
      }
      return obj;
    }).filter(function (o) { return String(o['ID'] || '').trim(); });
    rows.sort(function (a, b) { return String(b['Timestamp'] || '').localeCompare(String(a['Timestamp'] || '')); });
    return { ok: true, rows: rows, total: rows.length };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function updateObjeto_(data) {
  try {
    var payload = data.payload ? (typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload) : data;
    var id = String(payload.id || '').trim();
    if (!id) return { ok: false, error: 'Falta id' };
    var fields = payload.fields || {};
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('Objetos_Olvidados');
    if (!sh) return { ok: false, error: 'Hoja Objetos_Olvidados no existe' };
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2) return { ok: false, error: 'Hoja vacía' };
    var headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(function (v) { return String(v || '').trim(); });
    var idCol = headers.indexOf('ID') + 1;
    if (!idCol) return { ok: false, error: 'Columna ID no encontrada' };
    var ids = sh.getRange(2, idCol, lastRow - 1, 1).getDisplayValues();
    var rowIdx = -1;
    for (var i = 0; i < ids.length; i++) {
      if (String(ids[i][0]).trim() === id) { rowIdx = i + 2; break; }
    }
    if (rowIdx < 0) return { ok: false, error: 'ID no encontrado: ' + id };
    var fieldMap = {
      fecha_encontrado: 'Fecha_encontrado',
      fecha_entregado: 'Fecha_entregado',
      entregado_a: 'Entregado_a',
      propiedad: 'Propiedad',
      depto: '# Departamento',
      alojamiento: 'Alojamiento',
      reportante: 'Reportante',
      categoria: 'Categoria',
      categoria_otro: 'Categoria_otro',
      descripcion: 'Descripcion',
      lugar_resguardo: 'Lugar_resguardo',
      lugar_otro: 'Lugar_otro',
      comentarios: 'Comentarios',
      fotos_urls: 'Fotos_URLs',
      fotos_count: 'Fotos_count',
    };
    var updates = [];
    Object.keys(fields).forEach(function (k) {
      var header = fieldMap[k];
      if (!header) return;
      var col = headers.indexOf(header) + 1;
      if (!col) return;
      var v = fields[k];
      if (Array.isArray(v)) v = v.join(', ');
      sh.getRange(rowIdx, col).setValue(v == null ? '' : String(v));
      updates.push(header);
    });
    return { ok: true, id: id, row: rowIdx, updated: updates };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

// Lee todas las filas de la hoja "Incidencias" y las devuelve más nuevas
// primero (por Timestamp descendente). Cada fila como objeto {<header>: valor}.
function listIncidencias_() {
  try {
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('Incidencias');
    if (!sh) return { ok: true, rows: [], note: 'Hoja "Incidencias" no existe.' };
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2 || lastCol < 1) return { ok: true, rows: [] };
    var headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(function (v) { return String(v || '').trim(); });
    var data = sh.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();
    var rows = data.map(function (r) {
      var obj = {};
      for (var i = 0; i < headers.length; i++) {
        if (headers[i]) obj[headers[i]] = r[i];
      }
      return obj;
    }).filter(function (o) { return String(o['ID'] || '').trim(); });
    // Orden descendente por Timestamp (string ISO ordena bien)
    rows.sort(function (a, b) { return String(b['Timestamp'] || '').localeCompare(String(a['Timestamp'] || '')); });
    return { ok: true, rows: rows, total: rows.length };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

// ═══════════════════════════════════════════════════════════════════════
// ║  RECURSOS HUMANOS — Empleados, Asistencia, Ausencias, Compensaciones ║
// ═══════════════════════════════════════════════════════════════════════
// Columnas extendidas para la hoja "Personal". Si la hoja ya existe con
// otras columnas (la actual solo tiene Nombre/Puesto), agregamos las
// faltantes manteniendo lo que ya esté.
var RH_PERSONAL_HEADERS = [
  'ID',
  // Datos generales
  'Nombre', 'Apellido_paterno', 'Apellido_materno',
  'Fecha_nacimiento', 'CURP', 'Direccion',
  'Telefono', 'Celular', 'Email',
  // Nómina
  'Puesto', 'Estado', 'Activo',
  'Fecha_ingreso', 'Fecha_retiro',
  'Tipo_contrato', 'Salario_mensual', 'Periodicidad_pago',
  'Hora_entrada', 'Hora_salida', 'Dias_trabajo',
  // IMSS / SAT
  'NSS', 'RFC',
  // Bancarios
  'Banco', 'CLABE', 'Tipo_cuenta', 'Cuentahabiente',
  // Emergencia (legado)
  'Contacto_emergencia', 'Tel_emergencia',
];
var RH_ASIST_HEADERS = ['ID','Timestamp','Empleado_ID','Empleado_Nombre','Fecha','Entrada','Salida','Horas','Horas_extra','Observaciones'];
var RH_AUSE_HEADERS  = ['ID','Timestamp','Empleado_ID','Empleado_Nombre','Tipo','Fecha_inicio','Fecha_fin','Dias','Estatus','Comentarios'];
var RH_COMP_HEADERS  = ['ID','Timestamp','Empleado_ID','Empleado_Nombre','Concepto','Periodo','Monto','Metodo_pago','Fecha_pago','Comentarios'];

function rhEnsureHeaders_(sh, headers) {
  if (sh.getLastRow() === 0) {
    sh.appendRow(headers);
    sh.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#dbeafe');
    sh.setFrozenRows(1);
    return headers.slice();
  }
  var existing = sh.getRange(1, 1, 1, Math.max(1, sh.getLastColumn())).getValues()[0].map(function (v) { return String(v || '').trim(); });
  var missing = headers.filter(function (h) { return existing.indexOf(h) === -1; });
  if (missing.length) {
    var startCol = existing.length + 1;
    sh.getRange(1, startCol, 1, missing.length).setValues([missing]).setFontWeight('bold').setBackground('#dbeafe');
    return existing.concat(missing);
  }
  return existing;
}

function rhGenId_(prefix) {
  return prefix + '-' + Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyyMMdd-HHmmss') +
         '-' + Math.floor(Math.random() * 10000);
}

function rhRowsToObjects_(sh) {
  if (!sh) return [];
  var lastRow = sh.getLastRow();
  var lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return [];
  var headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(function (v) { return String(v || '').trim(); });
  var data = sh.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();
  return data.map(function (r) {
    var obj = {};
    for (var i = 0; i < headers.length; i++) {
      if (headers[i]) obj[headers[i]] = r[i];
    }
    return obj;
  });
}

// ── Empleados (Personal) ──
function rhListEmpleados_() {
  try {
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('Personal');
    if (!sh) {
      sh = ss.insertSheet('Personal');
      rhEnsureHeaders_(sh, RH_PERSONAL_HEADERS);
      return { ok: true, rows: [], headers: RH_PERSONAL_HEADERS };
    }
    rhEnsureHeaders_(sh, RH_PERSONAL_HEADERS);
    var rows = rhRowsToObjects_(sh).filter(function (o) { return String(o['Nombre'] || '').trim(); });
    // Asegurar que toda fila tenga ID (si fueron capturadas a mano sin ID,
    // generamos uno y lo escribimos de vuelta)
    var headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(function (v) { return String(v || '').trim(); });
    var idCol = headers.indexOf('ID') + 1;
    if (idCol) {
      for (var i = 0; i < rows.length; i++) {
        if (!rows[i].ID) {
          var newId = rhGenId_('EMP');
          sh.getRange(i + 2, idCol).setValue(newId);
          rows[i].ID = newId;
        }
      }
    }
    return { ok: true, rows: rows, headers: RH_PERSONAL_HEADERS };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function rhSaveEmpleado_(data) {
  try {
    var payload = data.payload ? (typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload) : data;
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('Personal');
    if (!sh) sh = ss.insertSheet('Personal');
    var headers = rhEnsureHeaders_(sh, RH_PERSONAL_HEADERS);
    var idCol = headers.indexOf('ID') + 1;
    var id = String(payload.ID || payload.id || '').trim();
    // Update si tiene ID y existe; insert si no
    if (id && idCol) {
      var lastRow = sh.getLastRow();
      if (lastRow >= 2) {
        var ids = sh.getRange(2, idCol, lastRow - 1, 1).getDisplayValues();
        for (var i = 0; i < ids.length; i++) {
          if (String(ids[i][0]).trim() === id) {
            var rowIdx = i + 2;
            // Update solo las columnas presentes en payload
            for (var k in payload) {
              var col = headers.indexOf(k) + 1;
              if (col) sh.getRange(rowIdx, col).setValue(payload[k] == null ? '' : String(payload[k]));
            }
            return { ok: true, id: id, mode: 'update' };
          }
        }
      }
    }
    // Insert
    if (!id) id = rhGenId_('EMP');
    payload.ID = id;
    var row = headers.map(function (h) {
      var v = payload[h];
      return v == null ? '' : String(v);
    });
    sh.appendRow(row);
    return { ok: true, id: id, mode: 'insert' };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

// ── List/Save genéricos para Asistencia, Ausencias, Compensaciones ──
function rhListSimple_(sheetName) {
  try {
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName(sheetName);
    if (!sh) return { ok: true, rows: [] };
    var rows = rhRowsToObjects_(sh).filter(function (o) { return String(o['ID'] || '').trim(); });
    // Más nuevos primero (por Timestamp)
    rows.sort(function (a, b) { return String(b['Timestamp'] || '').localeCompare(String(a['Timestamp'] || '')); });
    return { ok: true, rows: rows };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function sysLogin_(data) {
  try {
    var payload = data && data.payload ? (typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload) : data;
    var pwd = String((payload && (payload.password || payload.sys_password)) || '').trim();
    if (!pwd) return { ok: false, error: 'Falta contraseña' };
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName('sys_users');
    if (!sh) return { ok: false, error: 'Hoja sys_users no encontrada' };
    var values = sh.getDataRange().getDisplayValues();
    if (values.length < 2) return { ok: false, error: 'Sin usuarios registrados' };
    var headers = values[0].map(function (h) { return String(h || '').trim(); });
    var colName = -1, colPwd = -1, colSt = -1;
    headers.forEach(function (h, idx) {
      var hl = h.toLowerCase();
      if (hl === 'nombre') colName = idx;
      else if (hl === 'sys_password' || hl === 'password' || hl === 'contraseña') colPwd = idx;
      else if (hl === 'status' || hl === 'estado' || hl === 'estatus') colSt = idx;
    });
    if (colName < 0 || colPwd < 0) return { ok: false, error: 'Encabezados sys_users inválidos (faltan Nombre / sys_password)' };
    // Mapear columnas de módulos por número romano
    var modCols = {};
    headers.forEach(function (h, idx) {
      var m = String(h).match(/m[oó]dulo\s+(VIII|VII|VI|IV|V|III|II|I)\b/i);
      if (m) modCols[m[1].toUpperCase()] = idx;
    });
    for (var i = 1; i < values.length; i++) {
      var row = values[i];
      if (String(row[colPwd] || '').trim() !== pwd) continue;
      if (colSt >= 0 && String(row[colSt] || '').trim().toLowerCase() !== 'activo') return { ok: false, error: 'Usuario inactivo' };
      var modulos = {};
      for (var key in modCols) {
        modulos[key] = String(row[modCols[key]] || '').trim() === '1';
      }
      return { ok: true, user: { Nombre: String(row[colName]).trim(), modulos: modulos } };
    }
    return { ok: false, error: 'Contraseña incorrecta' };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function rhDeleteByID_(sheetName, id) {
  try {
    id = String(id || '').trim();
    if (!id) return { ok: false, error: 'ID requerido' };
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName(sheetName);
    if (!sh) return { ok: false, error: 'Hoja no encontrada: ' + sheetName };
    var headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(function (v) { return String(v || '').trim(); });
    var idCol = headers.indexOf('ID') + 1;
    if (!idCol) return { ok: false, error: 'Columna ID no encontrada' };
    var lastRow = sh.getLastRow();
    if (lastRow < 2) return { ok: false, error: 'Hoja vacía' };
    var ids = sh.getRange(2, idCol, lastRow - 1, 1).getDisplayValues();
    for (var i = 0; i < ids.length; i++) {
      if (String(ids[i][0]).trim() === id) {
        sh.deleteRow(i + 2);
        return { ok: true, id: id };
      }
    }
    return { ok: false, error: 'ID no encontrado: ' + id };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

function rhSaveSimple_(sheetName, data, headersTemplate, idPrefix) {
  try {
    var payload = data.payload ? (typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload) : data;
    var ss = getSpreadsheet_();
    var sh = ss.getSheetByName(sheetName);
    if (!sh) {
      sh = ss.insertSheet(sheetName);
      rhEnsureHeaders_(sh, headersTemplate);
    } else {
      rhEnsureHeaders_(sh, headersTemplate);
    }
    var headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(function (v) { return String(v || '').trim(); });
    var idCol = headers.indexOf('ID') + 1;
    var id = String(payload.ID || payload.id || '').trim();
    var ts = Utilities.formatDate(new Date(), 'America/Monterrey', 'yyyy-MM-dd HH:mm:ss');
    // Update si tiene ID existente
    if (id && idCol) {
      var lastRow = sh.getLastRow();
      if (lastRow >= 2) {
        var ids = sh.getRange(2, idCol, lastRow - 1, 1).getDisplayValues();
        for (var i = 0; i < ids.length; i++) {
          if (String(ids[i][0]).trim() === id) {
            var rowIdx = i + 2;
            for (var k in payload) {
              var col = headers.indexOf(k) + 1;
              if (col) sh.getRange(rowIdx, col).setValue(payload[k] == null ? '' : String(payload[k]));
            }
            return { ok: true, id: id, mode: 'update' };
          }
        }
      }
    }
    // Insert
    if (!id) id = rhGenId_(idPrefix);
    payload.ID = id;
    payload.Timestamp = ts;
    var row = headers.map(function (h) {
      var v = payload[h];
      return v == null ? '' : String(v);
    });
    sh.appendRow(row);
    return { ok: true, id: id, mode: 'insert' };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
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
  // Auto-push al huésped: "Reservación recibida"
  try {
    const phoneKey = normalizePhone_(cel);
    if (phoneKey) {
      const nombreReservante = safe_(data.nombre || "").trim().split(" ")[0] || "huésped";
      queueNotification_({
        target: phoneKey,
        category: "reservaciones",
        title: "Reservación recibida ✓",
        body: `¡Gracias ${nombreReservante}! Procesaremos tu solicitud y te avisaremos cuando esté lista.`,
        url: "./",
        tag: "reservacion-" + reservacionResult.record_id,
        source: "auto:submit_form"
      });
    }
  } catch(e) { Logger.log("[auto-push submit_form] " + e); }
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

// Sube archivo del wizard de perfil (sin reservación). Routea por field_name
// hacia Perfiles (INE/identif) o Vehiculos (foto vehículo). La carpeta de
// Drive se organiza por celular del huésped.
function saveProfileFile_(data) {
  const cel = safe_(data.celular_principal);
  const fieldName = safe_(data.field_name);
  const fileObj = data.file;
  if (!cel) throw new Error("Falta celular_principal.");
  if (!fieldName) throw new Error("Falta field_name.");
  if (!fileObj || !fileObj.base64) throw new Error("No se recibió archivo.");

  const ALLOWED = ["ine_frontal","ine_trasero","identificacion_unica","vehiculo_foto"];
  if (ALLOWED.indexOf(fieldName) === -1) {
    throw new Error("field_name no válido para upload_profile_file: " + fieldName);
  }

  // Carpeta Drive: Perfiles/<celular sanitizado>/<field_name>
  const celClean = cleanFolderName_(String(cel).replace(/[\s'+]/g, ""));
  const subfolders = ["Perfiles", celClean];
  const fileInfo = saveBase64FileToDrive_(fileObj, fieldName, subfolders);

  switch (fieldName) {
    case "ine_frontal":          updateProfileFile_(cel, "INE frontal", fileInfo); break;
    case "ine_trasero":          updateProfileFile_(cel, "INE trasero", fileInfo); break;
    case "identificacion_unica": updateProfileFile_(cel, "Identificación única", fileInfo); break;
    case "vehiculo_foto":        updateVehicleFile_(cel, fileInfo); break;
  }

  return { ok: true, url: fileInfo.url, file_id: fileInfo.id, file_name: fileInfo.name, field_name: fieldName, celular: cel };
}

// ─── MIGRACIÓN DE CELULAR ───────────────────────────────────────────────────
// Cuando un huésped cambia su celular (vía wizard "Cambiar celular"),
// actualizamos las 3 hojas para que su historial siga vinculado al perfil.
//
//   Perfiles:      si new_phone ya existe → merge campos no vacíos del viejo
//                  hacia el nuevo, luego borrar la fila vieja.
//                  Si solo existe el viejo → reescribir la celda con el nuevo.
//   Vehiculos:     misma lógica de merge/reescritura por celular.
//   Reservaciones: actualizar TODAS las rows donde
//                  "Cel/Whatsapp (principal)" === old_phone → new_phone.
//
// Idempotente: si old_phone === new_phone, devuelve ok sin cambios.
function migratePhone_(data) {
  ensureNormalizedSheets_();
  const oldPhone = safe_(data.old_phone).trim();
  const newPhone = safe_(data.new_phone).trim();
  if (!oldPhone || !newPhone) throw new Error("Faltan old_phone y new_phone.");
  if (normalizePhone_(oldPhone) === normalizePhone_(newPhone)) {
    return { ok: true, message: "old_phone y new_phone son iguales; no hay nada que migrar.", perfiles_updated: 0, vehiculos_updated: 0, reservaciones_updated: 0 };
  }

  const result = {
    ok: true,
    perfiles_updated: 0,
    vehiculos_updated: 0,
    reservaciones_updated: 0,
    perfiles_deleted: 0,
    vehiculos_deleted: 0
  };

  // ─── PERFILES ──
  (function migratePerfiles(){
    const sheet = getSheet_(PERFILES_SHEET);
    const headers = getHeaders_(sheet);
    const oldRow = findRowByPhone_(sheet, headers, oldPhone);
    if (!oldRow) return; // nada que migrar
    const newRow = findRowByPhone_(sheet, headers, newPhone);
    if (newRow && newRow !== oldRow) {
      // Existe row con el celular nuevo → merge (los del viejo llenan nulos del nuevo)
      const oldData = readRow_(sheet, headers, oldRow);
      const newData = readRow_(sheet, headers, newRow);
      const merged = {};
      headers.forEach(h => {
        if (h === "ID_Perfil") merged[h] = newData[h];                // preservar ID del nuevo
        else if (h === "Cel/Whatsapp (principal)") merged[h] = newPhone;
        else if (h === "Fecha creación") merged[h] = newData[h] || oldData[h] || new Date();
        else if (h === "Fecha actualización") merged[h] = new Date();
        else {
          const nv = newData[h];
          const ov = oldData[h];
          merged[h] = (nv != null && String(nv).trim() !== "") ? nv : (ov != null ? ov : "");
        }
      });
      writeRow_(sheet, headers, newRow, merged);
      // Borrar el row viejo (deleteRow ajusta índices arriba; como oldRow puede ser
      // mayor o menor que newRow lo manejamos correctamente)
      sheet.deleteRow(oldRow);
      result.perfiles_updated++;
      result.perfiles_deleted++;
    } else {
      // Solo existe el viejo → reescribir la celda del cel
      setCellByHeader_(sheet, headers, oldRow, "Cel/Whatsapp (principal)", newPhone);
      setCellByHeader_(sheet, headers, oldRow, "Fecha actualización", new Date());
      result.perfiles_updated++;
    }
  })();

  // ─── VEHICULOS ──
  (function migrateVehiculos(){
    const sheet = getSheet_(VEHICULOS_SHEET);
    const headers = getHeaders_(sheet);
    const oldRow = findRowByPhone_(sheet, headers, oldPhone);
    if (!oldRow) return;
    const newRow = findRowByPhone_(sheet, headers, newPhone);
    if (newRow && newRow !== oldRow) {
      const oldData = readRow_(sheet, headers, oldRow);
      const newData = readRow_(sheet, headers, newRow);
      const merged = {};
      headers.forEach(h => {
        if (h === "ID_Vehiculo") merged[h] = newData[h];
        else if (h === "Cel/Whatsapp (principal)") merged[h] = newPhone;
        else if (h === "Fecha actualización") merged[h] = new Date();
        else {
          const nv = newData[h];
          const ov = oldData[h];
          merged[h] = (nv != null && String(nv).trim() !== "") ? nv : (ov != null ? ov : "");
        }
      });
      writeRow_(sheet, headers, newRow, merged);
      sheet.deleteRow(oldRow);
      result.vehiculos_updated++;
      result.vehiculos_deleted++;
    } else {
      setCellByHeader_(sheet, headers, oldRow, "Cel/Whatsapp (principal)", newPhone);
      setCellByHeader_(sheet, headers, oldRow, "Fecha actualización", new Date());
      result.vehiculos_updated++;
    }
  })();

  // ─── RESERVACIONES ──
  (function migrateReservaciones(){
    const sheet = getSheet_(RESERVACIONES_SHEET);
    const headers = getHeaders_(sheet);
    const colIdx = headers.indexOf("Cel/Whatsapp (principal)");
    if (colIdx < 0) return;
    const lastRow = sheet.getLastRow();
    if (lastRow < 2) return;
    const range = sheet.getRange(2, colIdx + 1, lastRow - 1, 1);
    const values = range.getDisplayValues();
    const targetOld = normalizePhone_(oldPhone);
    // Identificar los rows que coinciden y actualizar con batch update
    const updates = [];
    for (let i = 0; i < values.length; i++) {
      if (normalizePhone_(values[i][0]) === targetOld) {
        updates.push(i);
      }
    }
    if (!updates.length) return;
    // Mantener compatibilidad con cells text-formatted (apóstrofo)
    updates.forEach(rowOffset => {
      sheet.getRange(rowOffset + 2, colIdx + 1).setValue(newPhone);
    });
    result.reservaciones_updated = updates.length;
  })();

  SpreadsheetApp.flush();
  return result;
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
  // Cuando el frontend manda también los montos de Airbnb (auto-cálculo en el
  // card o auto-fill desde Lodgify Líneas de cobro), persistirlos también para
  // que las 3 columnas queden sincronizadas.
  const comisionAirbnb = safe_(data.comision_airbnb);
  const totalAirbnb    = safe_(data.monto_total_airbnb);
  const comNorm  = String(comisionAirbnb || "").trim();
  const totNorm  = String(totalAirbnb    || "").trim();
  if (comNorm && isNaN(Number(comNorm.replace(/,/g, "")))) throw new Error("Comisión Airbnb debe ser numérica.");
  if (totNorm && isNaN(Number(totNorm.replace(/,/g, "")))) throw new Error("Monto total Airbnb debe ser numérico.");
  if (comNorm) setCellByHeader_(sheet, headers, row, "$ Comisión Airbnb",     comNorm);
  if (totNorm) setCellByHeader_(sheet, headers, row, "$ MONTO TOTAL Airbnb",  totNorm);
  return {
    ok: true, row_number: row, record_id: recordId,
    monto_facturado_total: normalized,
    comision_airbnb:    comNorm || null,
    monto_total_airbnb: totNorm || null,
  };
}

// Copia idéntica de updateFacturadoTotal_ pero para "$ MONTO TOTAL Airbnb"
function updateMontoTotalAirbnb_(data) {
  const recordId = safe_(data.record_id || data.id || data.row_id);
  const rawValue = safe_(data.monto_total_airbnb);
  if (!recordId) throw new Error("Falta record_id.");
  const normalized = String(rawValue || "").trim();
  if (normalized && isNaN(Number(normalized.replace(/,/g, "")))) throw new Error("Monto debe ser numérico.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let row = findRowByValue_(sheet, headers, "ID", recordId);
  if (!row) row = findRowByRowNumber_(sheet, recordId);
  if (!row) throw new Error("No se encontró la reservación.");
  setCellByHeader_(sheet, headers, row, "$ MONTO TOTAL Airbnb", normalized);
  return { ok: true, row_number: row, record_id: recordId, monto_total_airbnb: normalized };
}

// Copia idéntica de updateFacturadoTotal_ pero para "$ Comisión Airbnb"
function updateComisionAirbnb_(data) {
  const recordId = safe_(data.record_id || data.id || data.row_id);
  const rawValue = safe_(data.comision_airbnb);
  if (!recordId) throw new Error("Falta record_id.");
  const normalized = String(rawValue || "").trim();
  if (normalized && isNaN(Number(normalized.replace(/,/g, "")))) throw new Error("Monto debe ser numérico.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let row = findRowByValue_(sheet, headers, "ID", recordId);
  if (!row) row = findRowByRowNumber_(sheet, recordId);
  if (!row) throw new Error("No se encontró la reservación.");
  setCellByHeader_(sheet, headers, row, "$ Comisión Airbnb", normalized);
  return { ok: true, row_number: row, record_id: recordId, comision_airbnb: normalized };
}

// Acción GET genérica para actualizar UNA celda de Reservaciones por record_id.
// Uso: ?action=update_reservacion_cell&record_id=...&header=$%20MONTO%20TOTAL%20Airbnb&value=600
// Es defensiva: si el header no existe en la hoja devuelve ok:false con detalle.
function updateReservacionCell_(params) {
  const recordId = safe_(params.record_id || params.id || params.row_id);
  const headerName = safe_(params.header);
  const rawValue = safe_(params.value);
  if (!recordId) return { ok: false, error: "Falta record_id." };
  if (!headerName) return { ok: false, error: "Falta header." };
  const normalized = String(rawValue || "").trim();
  if (normalized && isNaN(Number(normalized.replace(/,/g, "")))) {
    return { ok: false, error: "Valor debe ser numérico.", header: headerName };
  }
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  if (headers.indexOf(headerName) < 0) {
    return { ok: false, error: "Header no encontrado en la hoja.", header: headerName, available: headers.filter(h => /airbnb|facturado/i.test(h)) };
  }
  let row = findRowByValue_(sheet, headers, "ID", recordId);
  if (!row) row = findRowByRowNumber_(sheet, recordId);
  if (!row) return { ok: false, error: "No se encontró la reservación.", record_id: recordId };
  const written = setCellByHeader_(sheet, headers, row, headerName, normalized);
  return { ok: written, record_id: recordId, row_number: row, header: headerName, value: normalized };
}

// Sincroniza los 3 campos del bloque Airbnb en una sola llamada:
// "$ MONTO TOTAL Airbnb", "$ Comisión Airbnb" y "$ Monto facturado Total".
function updateAirbnbAmounts_(data) {
  const recordId = safe_(data.record_id || data.id || data.row_id);
  if (!recordId) throw new Error("Falta record_id.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let row = findRowByValue_(sheet, headers, "ID", recordId);
  if (!row) row = findRowByRowNumber_(sheet, recordId);
  if (!row) throw new Error("No se encontró la reservación.");
  const norm = v => {
    const s = String(safe_(v) || "").trim();
    if (s && isNaN(Number(s.replace(/,/g, "")))) throw new Error("Monto debe ser numérico.");
    return s;
  };
  const total    = norm(data.monto_total_airbnb);
  const comision = norm(data.comision_airbnb);
  const facturado = norm(data.monto_facturado_total);
  setCellByHeader_(sheet, headers, row, "$ MONTO TOTAL Airbnb", total);
  setCellByHeader_(sheet, headers, row, "$ Comisión Airbnb", comision);
  setCellByHeader_(sheet, headers, row, "$ Monto facturado Total", facturado);
  return {
    ok: true, row_number: row, record_id: recordId,
    monto_total_airbnb: total, comision_airbnb: comision, monto_facturado_total: facturado
  };
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
  // Detectar transición vacío → no-vacío para disparar notificación al huésped.
  const prevRow = readRow_(sheet, headers, row);
  const prevFolio = String(prevRow["Folio facturapi"] || "").trim();
  setCellByHeader_(sheet, headers, row, "Folio facturapi", normalized);
  if (!prevFolio && normalized) {
    const updatedRow = readRow_(sheet, headers, row);
    const phoneKey = String(updatedRow["Cel/Whatsapp (principal)"] || "").replace(/\D/g, "");
    if (phoneKey) notifyTicketIssued_(phoneKey, updatedRow);
  }
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
  // Detectar transición vacío → no-vacío para disparar notificación al huésped.
  const prevFolio = String(readRow_(sheet, headers, row)["Folio facturapi"] || "").trim();
  sheet.getRange(row, targetCol + 1).setNumberFormat("@");
  sheet.getRange(row, targetCol + 1).setValue(normalized);
  // También persistir la organización (ACR/ACL) que emitió el folio.
  // Acepta cualquiera de: org_label, organizacion_facturapi, org (con map 1→ACR, 2→ACL).
  const orgRaw = safe_(data.org_label || data.organizacion_facturapi || data.org);
  let orgLabel = String(orgRaw || "").trim();
  if (orgLabel === "1") orgLabel = "ACR";
  else if (orgLabel === "2") orgLabel = "ACL";
  if (orgLabel) {
    const orgCol = headers.indexOf("Organización facturapi");
    if (orgCol >= 0) {
      sheet.getRange(row, orgCol + 1).setNumberFormat("@");
      sheet.getRange(row, orgCol + 1).setValue(orgLabel);
    }
  }
  SpreadsheetApp.flush();
  if (!prevFolio && normalized) {
    const updatedRow = readRow_(sheet, headers, row);
    const phoneKey = String(updatedRow["Cel/Whatsapp (principal)"] || "").replace(/\D/g, "");
    if (phoneKey) notifyTicketIssued_(phoneKey, updatedRow);
  }
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

/** Auto-migra la hoja de Reservaciones: agrega cualquier header de
 *  RESERVACIONES_HEADERS que no exista, al FINAL de la fila 1. Sin tocar
 *  posiciones de columnas existentes. Idempotente. */
function migrateReservacionesHeaders_(sh) {
  if (!sh) return;
  const lastCol = Math.max(sh.getLastColumn(), 1);
  const current = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(v => String(v || ""));
  const missing = RESERVACIONES_HEADERS.filter(h => current.indexOf(h) < 0);
  if (!missing.length) return;
  sh.getRange(1, lastCol + 1, 1, missing.length).setValues([missing]).setFontWeight("bold");
}

/** ARCHIVA el folio actual antes de re-emitir un ticket.
 *  Mueve el valor actual de "Folio facturapi" → "Folio facturapi antiguo"
 *  y limpia las celdas relacionadas para que Facturapi escriba los nuevos
 *  valores. Idempotente — si no hay folio actual, no hace nada.
 *  Recibido vía action="archive_folio_for_reemit" con { record_id } o
 *  { row_number } o { external_id }. */
function archiveFolioForReemit_(data) {
  const recordId = safe_(data.record_id || data.id || data.row_id);
  const explicitRow = safe_(data.row_number || data.rowNumber);
  const externalId = safe_(data.external_id || data.externalId);
  if (!recordId && !externalId && !explicitRow) throw new Error("Falta identificador de reservación.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  // Auto-agrega "Folio facturapi antiguo" si no existe en la hoja
  migrateReservacionesHeaders_(sheet);
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
  const prev = readRow_(sheet, headers, row);
  const prevFolio = String(prev["Folio facturapi"] || "").trim();
  if (!prevFolio) {
    // Nada que archivar — caller puede proceder con la emisión nueva
    return { ok: true, archived: false, message: "Sin folio previo." };
  }
  // 1) Mueve "Folio facturapi" → "Folio facturapi antiguo"
  //    Si "antiguo" ya tenía un valor previo, lo concatenamos con ", "
  //    (historial de re-emisiones).
  const prevAntiguo = String(prev["Folio facturapi antiguo"] || "").trim();
  const newAntiguo = prevAntiguo ? `${prevAntiguo}, ${prevFolio}` : prevFolio;
  setCellByHeader_(sheet, headers, row, "Folio facturapi antiguo", newAntiguo);
  // 2) Limpia campos que Facturapi va a reescribir
  const fieldsToClear = [
    "Folio facturapi","Folio CFDI","Folio Relación","Folio complemento de pago",
    "Fecha de emisión","Estatus",
    "Ticket facturapi url","Ticket facturapi id archivo","Ticket facturapi nombre archivo",
    "Ticket facturapi carpeta url","Ticket facturapi carpeta ruta",
  ];
  for (const f of fieldsToClear) {
    if (headers.indexOf(f) >= 0) setCellByHeader_(sheet, headers, row, f, "");
  }
  SpreadsheetApp.flush();
  return {
    ok: true,
    archived: true,
    row_number: row,
    record_id: recordId || String(externalId || "").replace(/^CHECKIN-/, "").trim(),
    previous_folio: prevFolio,
    archived_to: "Folio facturapi antiguo",
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
    "Hora estimada de llegada": resRow["Hora estimada de llegada"] || "",
    "Fecha de salida": resRow["Fecha de salida"] || "",
    "Hora estimada de salida": resRow["Hora estimada de salida"] || "",
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
    "# Huéspedes": resRow["# Huéspedes"] || "",
    "# Noches": resRow["# Noches"] || "",
    "Nombres de TODOS los huéspedes (separados por comas)": resRow["Nombres de TODOS los huéspedes (separados por comas)"] || "",
    "Motivo de tu hospedaje": resRow["Motivo de tu hospedaje"] || "",
    "Envía tus comentarios": resRow["Envía tus comentarios"] || "",
    "Lodgify Id": resRow["Lodgify Id"] || "",
    "$ Noches": resRow["$ Noches"] || "",
    "$ Cuota de limpieza": resRow["$ Cuota de limpieza"] || "",
    "$ MONTO TOTAL Airbnb": resRow["$ MONTO TOTAL Airbnb"] || "",
    "Organización facturapi": resRow["Organización facturapi"] || ""
  };
}

function listGuestRecords_(params) {
  ensureNormalizedSheets_();
  let { rows: reservaciones } = getAllRows_(RESERVACIONES_SHEET);
  const { rows: perfiles } = getAllRows_(PERFILES_SHEET);
  const { rows: vehiculos } = getAllRows_(VEHICULOS_SHEET);
  // Excluir reservaciones marcadas como "unificadas" (ocultas tras un merge
  // probable Lodgify ↔ manual). NO se borra del sheet, solo se oculta.
  const hiddenSet = getReservacionesHiddenSet_();
  if (hiddenSet.size) {
    reservaciones = reservaciones.filter(r => !hiddenSet.has(String(r["ID"] || "").trim()));
  }

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

// ─── RESYNC INCREMENTAL DESDE LEGACY ─────────────────────────────────────────
// Lee la hoja "Check in (archivo)" y agrega a las hojas normalizadas SOLO los
// registros que aún NO existan. NO destruye datos: los perfiles/vehículos/
// reservaciones ya guardadas por la app viva se preservan tal cual.
//
// Detección de duplicados:
//   - Perfiles: por celular normalizado.
//   - Vehículos: por celular normalizado.
//   - Reservaciones: primero por columna "ID" si coincide; si no, por la combinación
//     (celular + fecha de ingreso + # depto) — heurística suficiente porque un
//     mismo huésped no toma 2 deptos distintos el mismo día.
function resyncFromLegacyArchive() {
  ensureNormalizedSheets_();
  const ss = getSpreadsheet_();
  const src = ss.getSheetByName(LEGACY_ARCHIVE) || ss.getSheetByName(LEGACY_SHEET);
  if (!src) return { ok: false, error: "No se encontró la hoja '" + LEGACY_ARCHIVE + "' ni '" + LEGACY_SHEET + "'." };

  const srcHeaders = getHeaders_(src);
  const srcLast = src.getLastRow();
  if (srcLast < 2) return { ok: true, message: "Hoja legacy vacía. Nada que sincronizar." };
  const srcValues = src.getRange(2, 1, srcLast - 1, srcHeaders.length).getDisplayValues();
  const srcRows = srcValues.map(rowValues => {
    const obj = {};
    srcHeaders.forEach((h, i) => obj[h] = rowValues[i]);
    return obj;
  });

  // Helper de extracción robusta
  function pick(row, aliases) {
    for (let i = 0; i < aliases.length; i++) {
      if (Object.prototype.hasOwnProperty.call(row, aliases[i])) {
        const v = row[aliases[i]];
        if (v != null && String(v).trim() !== "") return String(v);
      }
    }
    return "";
  }

  // Cargar hojas normalizadas existentes
  const perfilesSh = getSheet_(PERFILES_SHEET);
  const perfilesHeaders = getHeaders_(perfilesSh);
  const perfilesExistentes = getAllRows_(PERFILES_SHEET).rows;
  const perfilesByPhone = {};
  perfilesExistentes.forEach(p => {
    const key = normalizePhone_(p["Cel/Whatsapp (principal)"]);
    if (key) perfilesByPhone[key] = p;
  });

  const vehiculosSh = getSheet_(VEHICULOS_SHEET);
  const vehiculosHeaders = getHeaders_(vehiculosSh);
  const vehiculosExistentes = getAllRows_(VEHICULOS_SHEET).rows;
  const vehiculosByPhone = {};
  vehiculosExistentes.forEach(v => {
    const key = normalizePhone_(v["Cel/Whatsapp (principal)"]);
    if (key) vehiculosByPhone[key] = v;
  });

  const reservacionesSh = getSheet_(RESERVACIONES_SHEET);
  const reservacionesHeaders = getHeaders_(reservacionesSh);
  const reservacionesExistentes = getAllRows_(RESERVACIONES_SHEET).rows;
  const reservacionesById = {};
  const reservacionesByKey = {};
  function resvKey(cel, fechaIngreso, depto) {
    return normalizePhone_(cel) + "|" + String(fechaIngreso || "").trim() + "|" + String(depto || "").trim();
  }
  reservacionesExistentes.forEach(r => {
    const rid = String(r.ID || "").trim();
    if (rid) reservacionesById[rid] = r;
    const k = resvKey(r["Cel/Whatsapp (principal)"], r["Fecha de ingreso"], r["# Departamento"]);
    reservacionesByKey[k] = r;
  });

  // Agrupar legacy por celular (para perfiles + vehículos)
  const byCel = {};
  srcRows.forEach(r => {
    const cel = pick(r, ["Cel/Whatsapp (principal)", "Celular principal"]);
    if (!cel) return;
    const key = normalizePhone_(cel);
    if (!byCel[key]) byCel[key] = { cel, rows: [] };
    byCel[key].rows.push(r);
  });

  let perfilesCreados = 0, vehiculosCreados = 0;
  let reservacionesCreadas = 0, reservacionesSkip = 0;

  // ───────── Perfiles + Vehículos ─────────
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

    // Perfil: solo crear si no existe (preservar lo capturado por la app)
    if (!perfilesByPhone[key]) {
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
        "Fecha creación": nowIso_(),
        "Fecha actualización": nowIso_()
      };
      appendRow_(perfilesSh, perfilesHeaders, perfilMap);
      // Registrar en cache para que el sweep de reservaciones encuentre su ID_Vehiculo si se crea ahora.
      perfilesByPhone[key] = perfilMap;
      perfilesCreados++;
    }

    // Vehículo: solo crear si no existe Y el legacy declara que tiene vehículo
    const tieneVeh = group.rows.some(r => normalizeYesNo_(pick(r, ["¿Cuenta con vehículo?"])) === "Sí");
    if (tieneVeh && !vehiculosByPhone[key]) {
      const idVeh = Utilities.getUuid();
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
        "Fecha actualización": nowIso_()
      };
      appendRow_(vehiculosSh, vehiculosHeaders, vehMap);
      vehiculosByPhone[key] = vehMap;
      vehiculosCreados++;
    }
  });

  // ───────── Reservaciones (1 fila por cada row del legacy, idempotente) ─────────
  srcRows.forEach(r => {
    const cel = pick(r, ["Cel/Whatsapp (principal)"]);
    if (!cel) return;
    const id = String(pick(r, ["ID"]) || "").trim();
    const k = resvKey(cel, pick(r, ["Fecha de ingreso"]), pick(r, ["# Departamento"]));
    const exists = (id && reservacionesById[id]) || reservacionesByKey[k];
    if (exists) { reservacionesSkip++; return; }

    const phoneKey = normalizePhone_(cel);
    const veh = vehiculosByPhone[phoneKey];
    const idVeh = veh ? (veh.ID_Vehiculo || "") : "";

    const resMap = {
      "ID": id || Utilities.getUuid(),
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
    appendRow_(reservacionesSh, reservacionesHeaders, resMap);
    if (resMap.ID) reservacionesById[resMap.ID] = resMap;
    reservacionesByKey[k] = resMap;
    reservacionesCreadas++;
  });

  return {
    ok: true,
    legacy_rows_leidas: srcRows.length,
    perfiles_creados: perfilesCreados,
    vehiculos_creados: vehiculosCreados,
    reservaciones_creadas: reservacionesCreadas,
    reservaciones_existentes_skip: reservacionesSkip,
    message: "Sync completada. Solo se agregaron los registros nuevos; los existentes se preservaron."
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
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
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
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePhone_(value) {
  return String(value || "").replace(/[\s'‘’“”]/g, "").replace(/^[+]/, "").trim();
}

// Devuelve la fecha-hora actual en zona horaria de Saltillo/Monterrey (CST/CDT),
// formateada como ISO 8601 con offset explícito. Ejemplo: 2026-05-30T21:42:15-06:00
// Reemplaza `new Date().toISOString()` (que devuelve UTC) para que los
// timestamps que guardamos en las hojas coincidan con la hora local del huésped.
function nowIso_() {
  return Utilities.formatDate(new Date(), "America/Monterrey", "yyyy-MM-dd'T'HH:mm:ssXXX");
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

// ═══ OTP via Email/SMS ═══════════════════════════════════════════════════════
// Storage server-side de códigos OTP. Sustituye al mock client-side ("123456").
// Métodos soportados:
//   - "email": MailApp.sendEmail nativo de Apps Script (gratis, hasta 100/día por usuario)
//   - "sms"  : Twilio REST API (requiere TWILIO_* en Script Properties)

function ensureOtpSheet_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  let sh = ss.getSheetByName(OTP_CODES_SHEET);
  if (!sh) {
    sh = ss.insertSheet(OTP_CODES_SHEET);
    sh.appendRow(OTP_CODES_HEADERS);
  }
  return sh;
}

function generateOtpCode_() {
  // 6 dígitos
  return String(Math.floor(100000 + Math.random() * 900000));
}

// Helper: obtiene info del perfil (correo de facturación + nombre) por phoneKey.
function getProfileContactByPhone_(phoneKey) {
  try {
    const key = String(phoneKey).replace(/\D/g, "");
    const { rows: perfiles } = getAllRows_(PERFILES_SHEET);
    for (let i = 0; i < perfiles.length; i++) {
      const p = perfiles[i];
      const cel = String(p["Cel/Whatsapp (principal)"] || "").replace(/\D/g, "");
      if (cel && cel === key) {
        return {
          email: safe_(p["Correo electrónico para el envío de la factura"]).trim(),
          name: safe_(p["Nombre del huésped"]).trim()
        };
      }
    }
  } catch(e) { Logger.log("[getProfileContact] " + e); }
  return { email: "", name: "" };
}

// GET helper: devuelve métodos disponibles (target del usuario por phoneKey)
// y enmascara para mostrar al cliente (ej. "an***@gmail.com")
function getOtpMethods_(params) {
  const phoneKey = String(params.phoneKey || "").replace(/\D/g, "");
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  const contact = getProfileContactByPhone_(phoneKey);
  const sms = phoneKey;
  return {
    ok: true,
    methods: {
      email: contact.email ? { available: true, target: contact.email, masked: maskEmail_(contact.email) } : { available: false },
      sms: { available: true, target: sms, masked: maskPhone_(sms) }
    },
    name: contact.name
  };
}
function maskEmail_(em) {
  if (!em) return "";
  const at = em.indexOf("@");
  if (at < 2) return em;
  return em.slice(0, 2) + "***" + em.slice(at);
}
function maskPhone_(p) {
  if (!p || p.length < 4) return p;
  return "*** *** " + p.slice(-4);
}

// POST send_otp: genera código, guarda y envía por email o sms.
// Args: { phoneKey, method, target? }
//   method ∈ "email" | "sms"
//   target opcional — si no viene, se toma de perfil (email) o phoneKey (sms).
function sendOtp_(data) {
  const phoneKey = String(data.phoneKey || "").replace(/\D/g, "");
  const method = String(data.method || "").trim().toLowerCase();
  let target = safe_(data.target || "").trim();
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  if (method !== "email" && method !== "sms") return { ok: false, error: "method debe ser email o sms." };
  // Si no llega target, intentar resolverlo
  if (!target) {
    const contact = getProfileContactByPhone_(phoneKey);
    if (method === "email") target = contact.email;
    else if (method === "sms") target = phoneKey;
  }
  if (!target) return { ok: false, error: "No hay correo registrado en el perfil. Captura uno o usa SMS." };
  // Generar y guardar
  const code = generateOtpCode_();
  const now = new Date();
  const expiresAt = new Date(now.getTime() + OTP_TTL_MS);
  const sh = ensureOtpSheet_();
  // Invalidar códigos pendientes previos del mismo phoneKey
  invalidateOldOtp_(sh, phoneKey);
  // Append
  const row = {
    phoneKey, method, target, code,
    created_at: now.toISOString(),
    expires_at: expiresAt.toISOString(),
    attempts: 0,
    verified_at: "",
    status: "pending"
  };
  sh.appendRow(OTP_CODES_HEADERS.map(h => row[h]));
  // Enviar
  try {
    if (method === "email") {
      sendOtpEmail_(target, code);
    } else if (method === "sms") {
      sendOtpSms_(target, code);
    }
  } catch (err) {
    return { ok: false, error: "No se pudo enviar el código: " + (err.message || err) };
  }
  return { ok: true, method, sent_to: method === "email" ? maskEmail_(target) : maskPhone_(target), expires_in: Math.floor(OTP_TTL_MS / 1000) };
}

function invalidateOldOtp_(sh, phoneKey) {
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return;
  const data = sh.getRange(2, 1, lastRow - 1, OTP_CODES_HEADERS.length).getValues();
  const idxPhone = OTP_CODES_HEADERS.indexOf("phoneKey");
  const idxStatus = OTP_CODES_HEADERS.indexOf("status");
  for (let i = 0; i < data.length; i++) {
    if (String(data[i][idxPhone]).replace(/\D/g,"") === phoneKey && String(data[i][idxStatus]) === "pending") {
      sh.getRange(i + 2, idxStatus + 1).setValue("invalidated");
    }
  }
}

function sendOtpEmail_(toEmail, code) {
  const subject = "Tu código de acceso a Check-inn Saltillo";
  const body =
    "Hola,\n\n" +
    "Tu código de acceso al portal de Check-inn Saltillo es:\n\n" +
    "    " + code + "\n\n" +
    "Este código vence en 5 minutos. Si tú no lo solicitaste, ignora este mensaje.\n\n" +
    "— Check-inn Saltillo · www.check-inn-saltillo.com";
  const htmlBody =
    '<div style="font-family:-apple-system,Helvetica,Arial,sans-serif;color:#0f172a;max-width:480px;margin:0 auto;padding:24px;">' +
    '<h2 style="margin:0 0 12px;letter-spacing:-.02em;">Tu código de acceso</h2>' +
    '<p style="font-size:14px;color:#4b5563;margin:0 0 18px;">Hola, ingresa este código en el portal de <strong>Check-inn Saltillo</strong>:</p>' +
    '<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:14px;padding:18px;text-align:center;font-size:32px;font-weight:800;letter-spacing:.4em;color:#991b1b;">' + code + '</div>' +
    '<p style="font-size:13px;color:#6b7280;margin:16px 0 0;">Vence en 5 minutos. Si tú no lo solicitaste, ignora este correo.</p>' +
    '<hr style="border:0;border-top:1px solid #e5e7eb;margin:20px 0;">' +
    '<p style="font-size:12px;color:#9ca3af;margin:0;">Check-inn Saltillo · <a href="https://www.check-inn-saltillo.com" style="color:#dc2626;">check-inn-saltillo.com</a></p>' +
    '</div>';
  MailApp.sendEmail({
    to: toEmail,
    subject: subject,
    body: body,
    htmlBody: htmlBody,
    name: "Check-inn Saltillo"
  });
}

function sendOtpSms_(toPhoneDigits, code) {
  const props = PropertiesService.getScriptProperties();
  const sid = props.getProperty("TWILIO_ACCOUNT_SID");
  const tok = props.getProperty("TWILIO_AUTH_TOKEN");
  const from = props.getProperty("TWILIO_FROM_NUMBER");
  if (!sid || !tok || !from) {
    throw new Error("Twilio no configurado. Falta TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_FROM_NUMBER en Script Properties.");
  }
  // Twilio API: POST a https://api.twilio.com/2010-04-01/Accounts/{SID}/Messages.json
  const to = "+" + String(toPhoneDigits).replace(/\D/g, "");
  const url = "https://api.twilio.com/2010-04-01/Accounts/" + sid + "/Messages.json";
  const body =
    "Tu código de acceso a Check-inn Saltillo es: " + code +
    ". Vence en 5 minutos. Si tú no lo solicitaste, ignora este mensaje.";
  const res = UrlFetchApp.fetch(url, {
    method: "post",
    headers: { "Authorization": "Basic " + Utilities.base64Encode(sid + ":" + tok) },
    payload: { From: from, To: to, Body: body },
    muteHttpExceptions: true
  });
  const status = res.getResponseCode();
  if (status >= 400) {
    const text = res.getContentText();
    throw new Error("Twilio error " + status + ": " + text);
  }
}

// POST verify_otp: valida { phoneKey, code }.
function verifyOtp_(data) {
  const phoneKey = String(data.phoneKey || "").replace(/\D/g, "");
  const code = String(data.code || "").trim();
  if (!phoneKey || !code) return { ok: false, error: "Falta phoneKey o code." };
  const sh = ensureOtpSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: false, error: "No hay códigos. Solicita uno primero." };
  const all = sh.getRange(2, 1, lastRow - 1, OTP_CODES_HEADERS.length).getValues();
  const idxPhone = OTP_CODES_HEADERS.indexOf("phoneKey");
  const idxCode = OTP_CODES_HEADERS.indexOf("code");
  const idxExp = OTP_CODES_HEADERS.indexOf("expires_at");
  const idxAttempts = OTP_CODES_HEADERS.indexOf("attempts");
  const idxStatus = OTP_CODES_HEADERS.indexOf("status");
  const idxVerified = OTP_CODES_HEADERS.indexOf("verified_at");
  // Buscar pendiente más reciente del phoneKey
  let foundIdx = -1;
  for (let i = all.length - 1; i >= 0; i--) {
    if (String(all[i][idxPhone]).replace(/\D/g, "") === phoneKey && String(all[i][idxStatus]) === "pending") {
      foundIdx = i;
      break;
    }
  }
  if (foundIdx < 0) return { ok: false, error: "Solicita un código primero." };
  const row = all[foundIdx];
  const sheetRow = foundIdx + 2;
  const expiresAt = new Date(String(row[idxExp]));
  if (Date.now() > expiresAt.getTime()) {
    sh.getRange(sheetRow, idxStatus + 1).setValue("expired");
    return { ok: false, error: "El código expiró. Solicita uno nuevo." };
  }
  const attempts = Number(row[idxAttempts] || 0);
  if (attempts >= OTP_MAX_ATTEMPTS) {
    sh.getRange(sheetRow, idxStatus + 1).setValue("locked");
    return { ok: false, error: "Demasiados intentos. Solicita un código nuevo." };
  }
  sh.getRange(sheetRow, idxAttempts + 1).setValue(attempts + 1);
  if (String(row[idxCode]) !== code) {
    return { ok: false, error: "Código incorrecto." };
  }
  // OK
  sh.getRange(sheetRow, idxStatus + 1).setValue("verified");
  sh.getRange(sheetRow, idxVerified + 1).setValue(nowIso_());
  return { ok: true, phoneKey: phoneKey };
}

// ─── PIN persistido en Perfiles ─────────────────────────────────────────────
// El PIN se hashea en cliente (sha256("checkinn|"+phoneKey+"|"+pin)) y solo
// se guarda el hash. checkUserStatus indica si hay PIN sin revelarlo.

function checkUserStatus_(data) {
  const phoneKey = String(data.phoneKey || "").replace(/\D/g, "");
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  ensureNormalizedSheets_();
  const sh = getSheet_(PERFILES_SHEET);
  const headers = getHeaders_(sh);
  const rowNum = findRowByPhone_(sh, headers, phoneKey);
  if (!rowNum) return { ok: true, exists: false, hasPin: false };
  const row = readRow_(sh, headers, rowNum);
  const pinHash = String(row["PIN hash"] || "").trim();
  return { ok: true, exists: true, hasPin: !!pinHash };
}

function setPin_(data) {
  const phoneKey = String(data.phoneKey || "").replace(/\D/g, "");
  const pinHash = String(data.pinHash || "").trim();
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  if (!pinHash) return { ok: false, error: "Falta pinHash." };
  ensureNormalizedSheets_();
  const sh = getSheet_(PERFILES_SHEET);
  const headers = getHeaders_(sh);
  let rowNum = findRowByPhone_(sh, headers, phoneKey);
  const nowIso = nowIso_();
  if (!rowNum) {
    // No existe el perfil — crear uno mínimo con solo celular + PIN.
    // El wizard llenará después el resto de los campos.
    appendRow_(sh, headers, {
      "ID_Perfil": Utilities.getUuid(),
      "Cel/Whatsapp (principal)": phoneKey,
      "PIN hash": pinHash,
      "PIN actualizado": nowIso,
      "Fecha creación": nowIso,
      "Fecha actualización": nowIso
    });
    return { ok: true, created: true };
  }
  setCellByHeader_(sh, headers, rowNum, "PIN hash", pinHash);
  setCellByHeader_(sh, headers, rowNum, "PIN actualizado", nowIso);
  setCellByHeader_(sh, headers, rowNum, "Fecha actualización", nowIso);
  return { ok: true, created: false };
}

function verifyPin_(data) {
  const phoneKey = String(data.phoneKey || "").replace(/\D/g, "");
  const pinHash = String(data.pinHash || "").trim();
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  if (!pinHash) return { ok: false, error: "Falta pinHash." };
  ensureNormalizedSheets_();
  const sh = getSheet_(PERFILES_SHEET);
  const headers = getHeaders_(sh);
  const rowNum = findRowByPhone_(sh, headers, phoneKey);
  if (!rowNum) return { ok: false, error: "Usuario no encontrado." };
  const row = readRow_(sh, headers, rowNum);
  const saved = String(row["PIN hash"] || "").trim();
  if (!saved) return { ok: false, error: "Sin PIN configurado." };
  if (saved !== pinHash) return { ok: false, error: "PIN incorrecto." };
  return { ok: true };
}

// ─── get_profile: hidrata el wizard desde el backend ─────────────────────
// Devuelve el perfil completo del usuario (Perfiles + Vehiculos) mapeado al
// formato { step1, step2, step3, step4, completed, lastStep, updatedAt } que
// usa el frontend en localStorage. El frontend lo invoca al iniciar sesión
// para que el wizard NO le pida llenar datos que ya están guardados.

function getProfile_(params) {
  const phoneKey = String(params.phoneKey || "").replace(/\D/g, "");
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  ensureNormalizedSheets_();
  const shP = getSheet_(PERFILES_SHEET);
  const headersP = getHeaders_(shP);
  const rowP = findRowByPhone_(shP, headersP, phoneKey);
  if (!rowP) return { ok: true, exists: false, profile: null };
  const p = readRow_(shP, headersP, rowP);

  // Extraer celular de emergencia (separar lada + celular si vienen juntos)
  const emergCel = String(p["Cel/Whatsapp (contacto de emergencia)"] || "").replace(/\D/g, "");
  const emergLada = String(p["Lada contacto emergencia"] || "52").replace(/\D/g, "") || "52";
  let emergLocal = emergCel;
  // Si el cel de emergencia incluye la lada al inicio, separarla.
  if (emergCel.length > 10 && emergCel.startsWith(emergLada)) {
    emergLocal = emergCel.slice(emergLada.length);
  } else if (emergCel.length > 10) {
    // Asumir últimos 10 dígitos = local; resto = lada
    emergLocal = emergCel.slice(-10);
  }

  const step1 = {
    nombre: String(p["Nombre del huésped"] || "").trim(),
    emerg_lada: emergLada,
    emerg_celular: emergLocal
  };
  const step2 = {
    tipo: String(p["Tipo de identificación"] || "").trim(),
    id_otro: String(p["Identificación otro"] || "").trim(),
    ine_frontal: String(p["Link INE frontal"] || "").trim(),
    ine_trasero: String(p["Link INE trasero"] || "").trim(),
    ident_unica: String(p["Link identificación única"] || "").trim()
  };
  const step3 = {
    factura: String(p["¿Requiere factura?"] || "").trim(),
    razon_social: String(p["Razón social"] || "").trim(),
    rfc: String(p["RFC"] || "").trim(),
    regimen: String(p["Régimen fiscal"] || "").trim(),
    regimen_otro: String(p["Régimen otro"] || "").trim(),
    codigo_postal: String(p["Código Postal"] || "").trim(),
    correo_factura: String(p["Correo electrónico para el envío de la factura"] || "").trim()
  };

  // Vehículo (otra hoja)
  let step4 = {
    tiene_vehiculo: "", marca: "", marca_otro: "",
    modelo: "", color: "", placas: "", hora_salida: "", foto: ""
  };
  try {
    const shV = getSheet_(VEHICULOS_SHEET);
    const headersV = getHeaders_(shV);
    const rowV = findRowByPhone_(shV, headersV, phoneKey);
    if (rowV) {
      const v = readRow_(shV, headersV, rowV);
      step4 = {
        tiene_vehiculo: String(v["¿Cuenta con vehículo?"] || "").trim(),
        marca: String(v["Marca vehículo"] || "").trim(),
        marca_otro: String(v["Marca vehículo otro"] || "").trim(),
        modelo: String(v["Modelo vehículo"] || "").trim(),
        color: String(v["Color vehículo"] || "").trim(),
        placas: String(v["Placas"] || "").trim(),
        hora_salida: String(v["Hora habitual de salida"] || "").trim(),
        foto: String(v["Link foto vehículo"] || "").trim()
      };
    }
  } catch(_e){}

  const updatedAt = (function(){
    const v = p["Fecha actualización"] || p["Fecha creación"];
    if (!v) return 0;
    const d = new Date(v);
    return isNaN(d.getTime()) ? 0 : d.getTime();
  })();

  return {
    ok: true, exists: true,
    profile: { step1, step2, step3, step4, lastStep: 4, updatedAt }
  };
}

// ─── get_image_b64: convierte un archivo de Drive a dataURL ──────────────
// El frontend pasa el ID del archivo (o URL completa, extrae el ID) y el
// backend descarga el blob con DriveApp + Utilities.base64Encode → dataURL.
// Esto soluciona el problema de iOS WebView que no carga URLs de Drive
// (CORS / redirect 302 / permisos), porque el dataURL es self-contained.
function getImageB64_(params) {
  let idOrUrl = String(params.id || params.url || "").trim();
  if (!idOrUrl) return { ok: false, error: "Falta id." };
  // Extraer ID si vino una URL completa.
  const matchers = [
    /\/file\/d\/([a-zA-Z0-9_-]+)/,
    /\/d\/([a-zA-Z0-9_-]+)/,
    /[?&]id=([a-zA-Z0-9_-]+)/
  ];
  for (const re of matchers) {
    const m = idOrUrl.match(re);
    if (m) { idOrUrl = m[1]; break; }
  }
  try {
    const file = DriveApp.getFileById(idOrUrl);
    const blob = file.getBlob();
    const mime = blob.getContentType() || "image/jpeg";
    const b64 = Utilities.base64Encode(blob.getBytes());
    const dataUrl = "data:" + mime + ";base64," + b64;
    return { ok: true, mime: mime, dataUrl: dataUrl };
  } catch (e) {
    return { ok: false, error: e.message || String(e) };
  }
}

// ─── Inbox de notificaciones POR USUARIO ─────────────────────────────────
// Cada huésped tiene su propio "inbox" en la hoja Notifications_Inbox.
// El frontend muestra una campana 🔔 con badge rojo en la cantidad de
// notificaciones no leídas. Al abrir el panel se marcan todas como leídas
// (el badge baja a 0). Al hacer click en una notificación que tiene acción
// (ej: abrir PDF de ticket), se archiva.

function ensureNotifInboxSheet_() {
  const ss = getSpreadsheet_();
  return ensureSheetWithHeaders_(ss, NOTIF_INBOX_SHEET, NOTIF_INBOX_HEADERS);
}

// Crea una notificación nueva en el inbox del usuario.
// type: identificador del tipo ("ticket_issued", etc).
// title/body: texto visible.
// data: objeto que se serializa a JSON (ticketUrl, folio, etc.).
function createInboxNotification_(phoneKey, type, title, body, data) {
  const cleanPhone = String(phoneKey || "").replace(/\D/g, "");
  if (!cleanPhone) return { ok: false, error: "Falta phoneKey." };
  const sh = ensureNotifInboxSheet_();
  const headers = getHeaders_(sh);
  appendRow_(sh, headers, {
    id: Utilities.getUuid(),
    phoneKey: cleanPhone,
    type: String(type || "general"),
    title: String(title || ""),
    body: String(body || ""),
    data: data ? JSON.stringify(data) : "",
    created_at: nowIso_(),
    read_at: "",
    archived_at: ""
  });
  return { ok: true };
}

function listNotifications_(params) {
  const phoneKey = String(params.phoneKey || "").replace(/\D/g, "");
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  ensureNotifInboxSheet_();
  const data = getAllRows_(NOTIF_INBOX_SHEET).rows;
  const rows = data
    .filter(r => String(r.phoneKey).replace(/\D/g, "") === phoneKey)
    .filter(r => !String(r.archived_at || "").trim())
    .map(r => ({
      id: r.id,
      type: r.type,
      title: r.title,
      body: r.body,
      data: r.data ? (function(){ try { return JSON.parse(r.data); } catch(_e){ return {}; } })() : {},
      created_at: r.created_at,
      read_at: r.read_at || ""
    }))
    .sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));
  const unread = rows.filter(r => !r.read_at).length;
  return { ok: true, rows: rows, unread: unread };
}

function markAllNotificationsRead_(data) {
  const phoneKey = String(data.phoneKey || "").replace(/\D/g, "");
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  const sh = ensureNotifInboxSheet_();
  const headers = getHeaders_(sh);
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: true, updated: 0 };
  const values = sh.getRange(2, 1, lastRow - 1, headers.length).getValues();
  const idxPhone = headers.indexOf("phoneKey");
  const idxRead = headers.indexOf("read_at");
  const idxArch = headers.indexOf("archived_at");
  const nowIso = nowIso_();
  let updated = 0;
  for (let i = 0; i < values.length; i++) {
    const samePhone = String(values[i][idxPhone]).replace(/\D/g, "") === phoneKey;
    const notRead = !String(values[i][idxRead] || "").trim();
    const notArchived = !String(values[i][idxArch] || "").trim();
    if (samePhone && notRead && notArchived) {
      sh.getRange(i + 2, idxRead + 1).setValue(nowIso);
      updated++;
    }
  }
  return { ok: true, updated: updated };
}

function archiveNotification_(data) {
  const id = String(data.id || "").trim();
  if (!id) return { ok: false, error: "Falta id." };
  const sh = ensureNotifInboxSheet_();
  const headers = getHeaders_(sh);
  const rowNum = findRowByValue_(sh, headers, "id", id);
  if (!rowNum) return { ok: false, error: "Notificación no encontrada." };
  const nowIso = nowIso_();
  setCellByHeader_(sh, headers, rowNum, "archived_at", nowIso);
  // Si aún no estaba leída, marcarla también como leída.
  const row = readRow_(sh, headers, rowNum);
  if (!String(row.read_at || "").trim()) {
    setCellByHeader_(sh, headers, rowNum, "read_at", nowIso);
  }
  return { ok: true };
}

// ─── Helpers de prueba e instalación de triggers ───────────────────────

// Crea una notificación de prueba en el inbox del usuario admin.
// Ejecutar UNA VEZ desde el editor para verificar que el panel 🔔 funciona.
function testCreateNotification() {
  createInboxNotification_(
    ADMIN_PHONE_KEY,
    "ticket_issued",
    "🧾 Ticket de factura emitido",
    "Tu factura de Calle Cumbres (Folio TEST-1234) está lista. Toca para descargar.",
    { folio: "TEST-1234", ticketUrl: "", monto: "1500", propiedad: "Calle Cumbres" }
  );
  Logger.log("Notificación de prueba creada en Notifications_Inbox para phoneKey=" + ADMIN_PHONE_KEY);
}

// Crea notificación EN INBOX + encola Web Push (badge en ícono + lock screen).
// Tras correr esto, dispara manualmente el workflow "Push Notifications · Drain
// Queue" en GitHub Actions para enviarlo al dispositivo en segundos.
function testCreateNotificationFullPush() {
  const folioStr = "TEST-" + Date.now().toString().slice(-6);
  const title = "🧾 Ticket de factura emitido";
  const body = "Tu factura de Calle Cumbres (Folio " + folioStr + ") está lista. Toca para descargar.";
  // 1) Inbox (panel 🔔 en la app)
  createInboxNotification_(
    ADMIN_PHONE_KEY, "ticket_issued", title, body,
    { folio: folioStr, ticketUrl: "", propiedad: "Calle Cumbres" }
  );
  // 2) Web Push queue (badge en ícono + lock screen)
  queueNotification_({
    target: ADMIN_PHONE_KEY,
    category: "facturas",
    title: title,
    body: body,
    url: "./",
    tag: "test-" + Date.now(),
    badge: 1,
    source: "test-full-push"
  });
  Logger.log("✓ Inbox + Queue creados para " + ADMIN_PHONE_KEY);
  Logger.log("→ Para enviar el push ya: GitHub → Actions → 'Push Notifications · Drain Queue' → Run workflow.");
}

// Trigger onEdit instalable: detecta cuando alguien edita manualmente la
// celda "Folio facturapi" en la hoja Reservaciones. Si el valor pasó de
// vacío a no-vacío, dispara la notificación al huésped.
function onEditReservacionesTrigger(e) {
  try {
    if (!e || !e.range || !e.value) return;
    const sheet = e.range.getSheet();
    if (sheet.getName() !== RESERVACIONES_SHEET) return;
    const row = e.range.getRow();
    if (row < 2) return; // ignorar header
    const col = e.range.getColumn();
    const headers = getHeaders_(sheet);
    const folioCol = headers.indexOf("Folio facturapi") + 1;
    if (col !== folioCol) return;
    const prevValue = String(e.oldValue || "").trim();
    const newValue = String(e.value || "").trim();
    if (prevValue || !newValue) return; // solo vacío → no-vacío
    const rowData = readRow_(sheet, headers, row);
    const phoneKey = String(rowData["Cel/Whatsapp (principal)"] || "").replace(/\D/g, "");
    if (!phoneKey) return;
    notifyTicketIssued_(phoneKey, rowData);
  } catch (err) {
    console.warn("onEditReservacionesTrigger error:", err);
  }
}

// Instala el trigger onEdit en la spreadsheet. Ejecutar UNA SOLA VEZ.
// Si la corres dos veces, no pasa nada — borra el anterior y reinstala.
function installOnEditReservacionesTrigger() {
  const ssId = SPREADSHEET_ID;
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(t => {
    if (t.getHandlerFunction() === "onEditReservacionesTrigger") {
      ScriptApp.deleteTrigger(t);
    }
  });
  ScriptApp.newTrigger("onEditReservacionesTrigger")
    .forSpreadsheet(ssId)
    .onEdit()
    .create();
  Logger.log("Trigger onEditReservacionesTrigger instalado correctamente.");
}

// Trigger: al asignar/actualizar Folio facturapi de una reserva, si el folio
// pasó de vacío a no-vacío, creamos una notificación tipo "ticket_issued"
// para el huésped + encolamos un Web Push (categoría "facturas").
function notifyTicketIssued_(phoneKey, reservacionRow) {
  try {
    const cleanPhone = String(phoneKey || "").replace(/\D/g, "");
    if (!cleanPhone) return;
    const folio = String(reservacionRow["Folio facturapi"] || "").trim();
    const ticketUrl = String(reservacionRow["Ticket facturapi url"] || "").trim();
    const monto = reservacionRow["$ Monto facturado Total"] || reservacionRow["($) Monto Total pagado"] || "";
    const propiedad = String(reservacionRow["Propiedad"] || "").trim();
    const titleEs = "🧾 Ticket de factura emitido";
    const bodyEs = folio
      ? "Tu factura " + (propiedad ? "de " + propiedad + " " : "") + "(Folio " + folio + ") está lista. Toca para descargar."
      : "Tu factura está lista. Toca para descargar.";
    createInboxNotification_(cleanPhone, "ticket_issued", titleEs, bodyEs, {
      folio: folio,
      ticketUrl: ticketUrl,
      monto: monto,
      propiedad: propiedad,
      reservacionId: reservacionRow["ID"] || ""
    });
    // Encolar web push (categoría facturas)
    try {
      queueNotification_({
        target: cleanPhone,
        category: "facturas",
        title: titleEs,
        body: bodyEs,
        url: "./",
        tag: "ticket-" + (folio || Date.now()),
        badge: 1,
        source: "auto-folio-trigger"
      });
    } catch(_e){}
  } catch (err) {
    console.warn("notifyTicketIssued_ error:", err);
  }
}

// ═══ PUSH NOTIFICATIONS ════════════════════════════════════════════════════════
// Storage de Web Push subscriptions. El ENVÍO de push (que requiere firma ECDSA
// VAPID) no es práctico desde Apps Script; se hace desde un script externo
// (otros/push_sender.py) que lee esta hoja y manda los push con la clave privada.

function ensurePushSubsSheet_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  let sh = ss.getSheetByName(PUSH_SUBS_SHEET);
  if (!sh) {
    sh = ss.insertSheet(PUSH_SUBS_SHEET);
    sh.appendRow(PUSH_SUBS_HEADERS);
    return sh;
  }
  // Migración: agregar columnas faltantes al final si la hoja es antigua.
  const existing = sh.getRange(1, 1, 1, Math.max(1, sh.getLastColumn())).getValues()[0];
  PUSH_SUBS_HEADERS.forEach(h => {
    if (existing.indexOf(h) < 0) {
      sh.getRange(1, sh.getLastColumn() + 1).setValue(h);
    }
  });
  return sh;
}

// Registra (o actualiza) una suscripción Web Push para un phoneKey.
// data: { phoneKey, endpoint, p256dh, auth, ua? }
function registerPushSubscription_(data) {
  const phoneKey = safe_(data.phoneKey || data.phone_key || "").trim();
  const endpoint = safe_(data.endpoint || "").trim();
  const p256dh = safe_(data.p256dh || "").trim();
  const auth = safe_(data.auth || "").trim();
  const ua = safe_(data.ua || "").trim();
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  if (!endpoint || !p256dh || !auth) return { ok: false, error: "Faltan campos endpoint/p256dh/auth." };
  const sh = ensurePushSubsSheet_();
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const idxEndpoint = headers.indexOf("endpoint");
  const lastRow = sh.getLastRow();
  const now = nowIso_();
  // Buscar fila existente por endpoint (idempotente: un endpoint = un dispositivo)
  let foundRow = -1;
  if (lastRow >= 2 && idxEndpoint >= 0) {
    const col = sh.getRange(2, idxEndpoint + 1, lastRow - 1, 1).getValues();
    for (let i = 0; i < col.length; i++) {
      if (String(col[i][0]).trim() === endpoint) { foundRow = i + 2; break; }
    }
  }
  const row = {};
  PUSH_SUBS_HEADERS.forEach(h => row[h] = "");
  row.phoneKey = phoneKey;
  row.endpoint = endpoint;
  row.p256dh = p256dh;
  row.auth = auth;
  row.ua = ua;
  row.updated_at = now;
  row.badge_count = "";
  if (foundRow < 0) {
    row.created_at = now;
    sh.appendRow(PUSH_SUBS_HEADERS.map(h => row[h]));
    return { ok: true, action: "created", phoneKey, endpoint };
  } else {
    // Update: preserva created_at + badge_count + last_sent_at
    const existing = sh.getRange(foundRow, 1, 1, PUSH_SUBS_HEADERS.length).getValues()[0];
    row.created_at = existing[PUSH_SUBS_HEADERS.indexOf("created_at")] || now;
    row.badge_count = existing[PUSH_SUBS_HEADERS.indexOf("badge_count")] || "";
    row.last_sent_at = existing[PUSH_SUBS_HEADERS.indexOf("last_sent_at")] || "";
    sh.getRange(foundRow, 1, 1, PUSH_SUBS_HEADERS.length).setValues([PUSH_SUBS_HEADERS.map(h => row[h])]);
    return { ok: true, action: "updated", phoneKey, endpoint };
  }
}

// Elimina la suscripción por endpoint (cuando el usuario desinstala / revoca permiso)
function unregisterPushSubscription_(data) {
  const endpoint = safe_(data.endpoint || "").trim();
  if (!endpoint) return { ok: false, error: "Falta endpoint." };
  const sh = ensurePushSubsSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: true, deleted: 0 };
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const idxEndpoint = headers.indexOf("endpoint");
  if (idxEndpoint < 0) return { ok: false, error: "Hoja sin columna endpoint." };
  const col = sh.getRange(2, idxEndpoint + 1, lastRow - 1, 1).getValues();
  for (let i = col.length - 1; i >= 0; i--) {
    if (String(col[i][0]).trim() === endpoint) {
      sh.deleteRow(i + 2);
      return { ok: true, deleted: 1 };
    }
  }
  return { ok: true, deleted: 0 };
}

// Lista suscripciones (opcionalmente filtra por phoneKey). GET admin.
function listPushSubscriptions_(params) {
  const phoneKeyFilter = safe_(params.phoneKey || "").trim();
  const sh = ensurePushSubsSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: true, rows: [] };
  const data = sh.getRange(1, 1, lastRow, PUSH_SUBS_HEADERS.length).getValues();
  const headers = data[0];
  const rows = data.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = r[i]);
    return obj;
  });
  const filtered = phoneKeyFilter ? rows.filter(r => String(r.phoneKey).trim() === phoneKeyFilter) : rows;
  return { ok: true, rows: filtered, total: filtered.length };
}

// Stub: el envío real se hace desde Python (push_sender.py) con la VAPID privada.
// Esta función devuelve las suscripciones objetivo y un mensaje informativo.
function sendPushToUserFromAppsScript_(params) {
  return {
    ok: false,
    error: "El envío de push requiere firma ECDSA VAPID. Usa otros/push_sender.py.",
    hint: "Apps Script no soporta ECDSA. Lee la hoja " + PUSH_SUBS_SHEET + " y envía con web-push (Python o Node)."
  };
}

// Actualiza las categorías de aviso de un usuario (CSV, ej. "reservaciones,facturas").
function updatePushCategories_(data) {
  const phoneKey = String(safe_(data.phoneKey || "")).replace(/\D/g, "");
  const cats = safe_(data.categories || "").trim();
  if (!phoneKey) return { ok: false, error: "Falta phoneKey." };
  const sh = ensurePushSubsSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: true, updated: 0 };
  const data2 = sh.getRange(1, 1, lastRow, PUSH_SUBS_HEADERS.length).getValues();
  const headers = data2[0];
  const idxPhone = headers.indexOf("phoneKey");
  const idxCats = headers.indexOf("categories");
  if (idxPhone < 0 || idxCats < 0) return { ok: false, error: "Hoja sin columnas requeridas." };
  let updated = 0;
  for (let i = 1; i < data2.length; i++) {
    if (String(data2[i][idxPhone]).replace(/\D/g, "") === phoneKey) {
      sh.getRange(i + 1, idxCats + 1).setValue(cats);
      updated++;
    }
  }
  return { ok: true, updated };
}

// ═══ Cola de notificaciones ═══════════════════════════════════════════════════
function ensureQueueSheet_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  let sh = ss.getSheetByName(PUSH_QUEUE_SHEET);
  if (!sh) {
    sh = ss.insertSheet(PUSH_QUEUE_SHEET);
    sh.appendRow(PUSH_QUEUE_HEADERS);
  }
  return sh;
}

// Encola una notificación. target puede ser "ALL" o un phoneKey (dígitos).
// Args: { target, category, title, body, badge?, url?, tag?, source? }
function queueNotification_(data) {
  const target = safe_(data.target || "").replace(/\D/g, "") || "ALL";
  const category = safe_(data.category || "general");
  const title = safe_(data.title || "").trim();
  const body = safe_(data.body || "").trim();
  if (!title || !body) return { ok: false, error: "Falta title/body." };
  if (PUSH_CATEGORIES.indexOf(category) < 0) {
    return { ok: false, error: "Categoría inválida: " + category };
  }
  const sh = ensureQueueSheet_();
  const id = Utilities.getUuid();
  const row = {
    id,
    target: data.target === "ALL" ? "ALL" : target,
    category,
    title, body,
    badge: data.badge != null ? String(data.badge) : "",
    url: safe_(data.url || ""),
    tag: safe_(data.tag || ""),
    status: "pending",
    error: "",
    created_at: nowIso_(),
    processed_at: "",
    source: safe_(data.source || "manual")
  };
  sh.appendRow(PUSH_QUEUE_HEADERS.map(h => row[h] != null ? row[h] : ""));
  return { ok: true, id, queued: 1 };
}

// Lista notificaciones pendientes (las que el sender debe procesar).
function listPendingNotifications_(params) {
  const sh = ensureQueueSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: true, rows: [] };
  const data = sh.getRange(1, 1, lastRow, PUSH_QUEUE_HEADERS.length).getValues();
  const headers = data[0];
  const rows = data.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = r[i] == null ? "" : String(r[i]));
    return obj;
  }).filter(r => r.status === "pending");
  const limit = Math.min(parseInt(params.limit || "200", 10), 500);
  return { ok: true, rows: rows.slice(0, limit), total: rows.length };
}

// Marca una notificación como procesada (sent / failed).
function markNotificationProcessed_(data) {
  const id = safe_(data.id || "").trim();
  const status = safe_(data.status || "sent").trim();
  const error = safe_(data.error || "").trim();
  if (!id) return { ok: false, error: "Falta id." };
  const sh = ensureQueueSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: false, error: "Cola vacía." };
  const idxId = PUSH_QUEUE_HEADERS.indexOf("id");
  const idxStatus = PUSH_QUEUE_HEADERS.indexOf("status");
  const idxError = PUSH_QUEUE_HEADERS.indexOf("error");
  const idxProc = PUSH_QUEUE_HEADERS.indexOf("processed_at");
  const col = sh.getRange(2, idxId + 1, lastRow - 1, 1).getValues();
  for (let i = 0; i < col.length; i++) {
    if (String(col[i][0]).trim() === id) {
      const r = i + 2;
      sh.getRange(r, idxStatus + 1).setValue(status);
      sh.getRange(r, idxError + 1).setValue(error);
      sh.getRange(r, idxProc + 1).setValue(nowIso_());
      return { ok: true, updated: 1, id };
    }
  }
  return { ok: false, error: "id no encontrado.", id };
}

// ═══ Menú custom en la hoja para enviar avisos desde Sheets ═══════════════════
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("🔔 Avisos")
    .addItem("Enviar a un usuario…", "menuSendToUser_")
    .addItem("Enviar a TODOS los suscritos…", "menuSendToAll_")
    .addSeparator()
    .addItem("Ver pendientes en cola", "menuShowPending_")
    .addToUi();
}
function menuSendToUser_() {
  const ui = SpreadsheetApp.getUi();
  const r1 = ui.prompt("Destinatario", "phoneKey (solo dígitos, ej. 528115569120):", ui.ButtonSet.OK_CANCEL);
  if (r1.getSelectedButton() !== ui.Button.OK) return;
  const phone = r1.getResponseText().trim();
  if (!phone) return;
  const r2 = ui.prompt("Categoría", "reservaciones | facturas | recordatorios | general", ui.ButtonSet.OK_CANCEL);
  if (r2.getSelectedButton() !== ui.Button.OK) return;
  const category = r2.getResponseText().trim() || "general";
  const r3 = ui.prompt("Título", "Título corto de la notificación:", ui.ButtonSet.OK_CANCEL);
  if (r3.getSelectedButton() !== ui.Button.OK) return;
  const title = r3.getResponseText().trim();
  const r4 = ui.prompt("Cuerpo", "Mensaje:", ui.ButtonSet.OK_CANCEL);
  if (r4.getSelectedButton() !== ui.Button.OK) return;
  const body = r4.getResponseText().trim();
  const res = queueNotification_({ target: phone, category, title, body, source: "sheets-menu" });
  ui.alert(res.ok
    ? "✓ Encolada (id " + res.id + ").\nCorre 'python3 otros/push_sender.py --drain' para enviar."
    : "Error: " + (res.error || "desconocido"));
}
function menuSendToAll_() {
  const ui = SpreadsheetApp.getUi();
  const r2 = ui.prompt("Categoría", "reservaciones | facturas | recordatorios | general", ui.ButtonSet.OK_CANCEL);
  if (r2.getSelectedButton() !== ui.Button.OK) return;
  const category = r2.getResponseText().trim() || "general";
  const r3 = ui.prompt("Título", "Título corto:", ui.ButtonSet.OK_CANCEL);
  if (r3.getSelectedButton() !== ui.Button.OK) return;
  const title = r3.getResponseText().trim();
  const r4 = ui.prompt("Cuerpo", "Mensaje:", ui.ButtonSet.OK_CANCEL);
  if (r4.getSelectedButton() !== ui.Button.OK) return;
  const body = r4.getResponseText().trim();
  const res = queueNotification_({ target: "ALL", category, title, body, source: "sheets-menu" });
  ui.alert(res.ok
    ? "✓ Encolada para TODOS (id " + res.id + ").\nCorre 'python3 otros/push_sender.py --drain' para enviar."
    : "Error: " + (res.error || "desconocido"));
}
function menuShowPending_() {
  const list = listPendingNotifications_({ limit: "20" });
  SpreadsheetApp.getUi().alert("Pendientes: " + list.total + (list.total
    ? "\n\nEjecuta: python3 otros/push_sender.py --drain"
    : ""));
}

// ═══════════════════════════════════════════════════════════════════════════
// ─── MÓDULO: Reservas Lodgify (cache persistente en Google Sheets) ─────────
//
// La hoja "Reservas_Lodgify" guarda una fila por booking Id (agregando
// LineItems en una columna JSON). El frontend lee desde aquí (lodgify_list),
// y el sync (lodgify_sync, manual o time-driven) pulla desde el Cloud Run de
// Lodgify y upsertea.
// ═══════════════════════════════════════════════════════════════════════════
const LODGIFY_SHEET = "Reservas_Lodgify";
const LODGIFY_HIDDEN_SHEET = "Reservas_Lodgify_Hidden";
const LODGIFY_API_BASE = "https://checkinnreservas-1044570371371.northamerica-south1.run.app";
const LODGIFY_HEADERS = [
  "Id","Source","Status","DateArrival","DateDeparture","Nights",
  "HouseName","HouseId","RoomTypeNames","RoomTypeIds",
  "GuestName","GuestEmail","GuestPhone","GuestCountryCode",
  "NumberOfGuests","Adults","Children","Infants","Pets",
  "Currency","ConfirmationCode","ListingId","ThreadId","ChannelBooking","DateCancelled",
  "GrossTotal","NetTotal","VatTotal","LineItemsJSON",
  "first_synced_at","last_synced_at"
];

function ensureLodgifySheet_() {
  const ss = getSpreadsheet_();
  let sh = ss.getSheetByName(LODGIFY_SHEET);
  if (!sh) {
    sh = ss.insertSheet(LODGIFY_SHEET);
    sh.getRange(1, 1, 1, LODGIFY_HEADERS.length).setValues([LODGIFY_HEADERS]);
    sh.getRange(1, 1, 1, LODGIFY_HEADERS.length).setFontWeight("bold").setBackground("#f1f5f9");
    sh.setFrozenRows(1);
    sh.autoResizeColumns(1, LODGIFY_HEADERS.length);
  } else {
    // Si la hoja existe pero está vacía, escribe headers
    if (sh.getLastRow() === 0) {
      sh.getRange(1, 1, 1, LODGIFY_HEADERS.length).setValues([LODGIFY_HEADERS]);
    }
  }
  // CRÍTICO: forzar formato texto ("@") en las columnas de fecha para que
  // Sheets NO las auto-parsee usando el locale es-MX (que las invierte).
  // Lodgify envía MM/DD/YYYY; sin esto, Sheets las convierte a Date(DD/MM)
  // y al leerlas vienen como ISO "2026-05-08T..." con día y mes swapped.
  ['DateArrival','DateDeparture','DateCancelled'].forEach(colName => {
    const idx = LODGIFY_HEADERS.indexOf(colName);
    if (idx < 0) return;
    const rows = Math.max(1, sh.getMaxRows() - 1);
    sh.getRange(2, idx + 1, rows, 1).setNumberFormat('@');
  });
  return sh;
}

/** Agrupa los rows del Lodgify API (uno por LineItem) por booking Id. */
function aggregateLodgifyRows_(rows) {
  const map = new Map();
  (rows || []).forEach(r => {
    const id = r.Id;
    if (!id) return;
    let agg = map.get(id);
    if (!agg) {
      let meta = {};
      try { meta = JSON.parse(r.SourceText || "{}"); } catch(_) {}
      agg = {
        Id: id,
        Source: r.Source || "",
        Status: r.Status || "",
        DateArrival: r.DateArrival || "",
        DateDeparture: r.DateDeparture || "",
        Nights: Number(r.Nights) || 0,
        HouseName: r.HouseName || "",
        HouseId: r.HouseId || "",
        RoomTypeNames: r.RoomTypeNames || "",
        RoomTypeIds: r.RoomTypeIds || "",
        GuestName: r.GuestName || "",
        GuestEmail: r.GuestEmail || "",
        GuestPhone: r.GuestPhone || "",
        GuestCountryCode: r.GuestCountryCode || "",
        NumberOfGuests: Number(r.NumberOfGuests) || 0,
        Adults: Number(r.Adults) || 0,
        Children: Number(r.Children) || 0,
        Infants: Number(r.Infants) || 0,
        Pets: Number(r.Pets) || 0,
        Currency: r.Currency || "MXN",
        ConfirmationCode: meta.confirmationCode || "",
        ListingId: meta.listingId || "",
        ThreadId: meta.threadId || "",
        ChannelBooking: r.ChannelBooking || "",
        DateCancelled: r.DateCancelled || "",
        GrossTotal: 0, NetTotal: 0, VatTotal: 0,
        LineItems: [],
      };
      map.set(id, agg);
    }
    agg.GrossTotal += Number(r.GrossAmount) || 0;
    agg.NetTotal   += Number(r.NetAmount)   || 0;
    agg.VatTotal   += Number(r.VatAmount)   || 0;
    agg.LineItems.push({
      kind: r.LineItem || "",
      desc: r.LineItemDescription || "",
      gross: Number(r.GrossAmount) || 0,
      net: Number(r.NetAmount) || 0,
      vat: Number(r.VatAmount) || 0,
    });
  });
  return Array.from(map.values());
}

/** Convierte un booking agregado en row (array) en orden de LODGIFY_HEADERS. */
function lodgifyBookingToRow_(b, nowIso, prevFirstSync) {
  return [
    String(b.Id), b.Source, b.Status, b.DateArrival, b.DateDeparture, b.Nights,
    b.HouseName, String(b.HouseId||""), b.RoomTypeNames, b.RoomTypeIds,
    b.GuestName, b.GuestEmail, b.GuestPhone, b.GuestCountryCode,
    b.NumberOfGuests, b.Adults, b.Children, b.Infants, b.Pets,
    b.Currency, b.ConfirmationCode, b.ListingId, b.ThreadId, b.ChannelBooking, b.DateCancelled,
    Number(b.GrossTotal.toFixed(2)), Number(b.NetTotal.toFixed(2)), Number(b.VatTotal.toFixed(2)),
    JSON.stringify(b.LineItems || []),
    prevFirstSync || nowIso,
    nowIso
  ];
}

/** Fetch a Lodgify Cloud Run (que ya consulta y pagina la API). */
// Endpoint nuestro (Cloud Run ticket-vision) que consume Lodgify v2 directo.
// Incluye reservas SIN presupuesto que el OTC de Lodgify omitía.
var LODGIFY_BOOKINGS_URL = "https://ticket-vision-957627511957.northamerica-south1.run.app/lodgify-bookings-all";
function fetchLodgifyOTC_(fromDate, toDate) {
  // updatedSince: 30 días antes de fromDate para capturar reservas creadas/
  // modificadas que toquen el rango.
  var fd = new Date(fromDate + 'T00:00:00');
  fd.setDate(fd.getDate() - 30);
  var updatedSince = fd.toISOString().slice(0,10);
  var url = LODGIFY_BOOKINGS_URL + "?from=" + encodeURIComponent(fromDate) + "&to=" + encodeURIComponent(toDate) + "&updatedSince=" + encodeURIComponent(updatedSince);
  var resp = UrlFetchApp.fetch(url, {
    method: "get",
    muteHttpExceptions: true,
    headers: { Accept: "application/json" },
  });
  var text = resp.getContentText();
  if (resp.getResponseCode() !== 200) throw new Error("Lodgify HTTP " + resp.getResponseCode() + ": " + text.slice(0,200));
  var data = JSON.parse(text);
  if (!data.ok) throw new Error(data.error || "Lodgify returned ok=false");
  return data.rows || [];
}

/** ACCIÓN: lodgify_sync — pulla Lodgify y upsertea en la hoja.
 *  params:
 *    days_back  (default 60)  ventana hacia atrás desde hoy
 *    days_fwd   (default 365) ventana hacia adelante desde hoy
 *    full       (default false) si true: rango amplio (2 años atrás/adelante) */
function syncLodgifyReservations_(data) {
  data = data || {};
  const full = String(data.full || "").toLowerCase() === "true" || data.full === true;
  const daysBack = full ? 730 : (Number(data.days_back) || 60);
  const daysFwd  = full ? 730 : (Number(data.days_fwd)  || 365);
  const today = new Date();
  const from = new Date(today.getTime() - daysBack * 86400000).toISOString().slice(0,10);
  const to   = new Date(today.getTime() + daysFwd  * 86400000).toISOString().slice(0,10);

  const startMs = Date.now();
  const rows = fetchLodgifyOTC_(from, to);
  const bookings = aggregateLodgifyRows_(rows);

  const sh = ensureLodgifySheet_();
  const lastRow = sh.getLastRow();
  const headers = sh.getRange(1, 1, 1, LODGIFY_HEADERS.length).getValues()[0];
  const colId = headers.indexOf("Id");
  const colFirstSync = headers.indexOf("first_synced_at");

  // Mapeo Id → row_number actual. Si hay filas duplicadas con el mismo Id
  // (consecuencia de sincronizaciones previas con bug), marcamos las
  // duplicadas para borrar y conservamos sólo la primera. Sin esto, el
  // upsert sólo actualiza la última, dejando datos viejos en la otra y
  // confundiendo al frontend que las lee todas.
  const existing = {};
  let dupRowsToDelete = [];
  if (lastRow >= 2) {
    const ids = sh.getRange(2, colId + 1, lastRow - 1, 1).getValues();
    const firsts = sh.getRange(2, colFirstSync + 1, lastRow - 1, 1).getValues();
    for (let i = 0; i < ids.length; i++) {
      const idVal = String(ids[i][0] || "").trim();
      if (!idVal) continue;
      const rowIdx = i + 2;
      if (existing[idVal]) {
        dupRowsToDelete.push(rowIdx); // duplicado → borrar
      } else {
        existing[idVal] = { row: rowIdx, firstSync: firsts[i][0] || "" };
      }
    }
  }
  // Borrar duplicados de atrás hacia adelante para no shiftear índices
  const dupCount = dupRowsToDelete.length;
  if (dupCount > 0) {
    dupRowsToDelete.sort(function(a, b) { return b - a; });
    dupRowsToDelete.forEach(function(r) { sh.deleteRow(r); });
    // Después de borrar filas, los índices de los keepers (todos < a los
    // borrados) ya no son válidos. Reconstruimos el mapa existing leyendo
    // de nuevo desde el sheet actualizado.
    Object.keys(existing).forEach(function(k) { delete existing[k]; });
    const newLast = sh.getLastRow();
    if (newLast >= 2) {
      const ids2 = sh.getRange(2, colId + 1, newLast - 1, 1).getValues();
      const firsts2 = sh.getRange(2, colFirstSync + 1, newLast - 1, 1).getValues();
      for (let i = 0; i < ids2.length; i++) {
        const idVal = String(ids2[i][0] || "").trim();
        if (idVal) existing[idVal] = { row: i + 2, firstSync: firsts2[i][0] || "" };
      }
    }
  }

  const nowIso = new Date().toISOString();
  const toAppend = [];
  let updated = 0, inserted = 0;
  bookings.forEach(b => {
    const key = String(b.Id);
    const ex = existing[key];
    const arr = lodgifyBookingToRow_(b, nowIso, ex ? ex.firstSync : nowIso);
    if (ex) {
      sh.getRange(ex.row, 1, 1, LODGIFY_HEADERS.length).setValues([arr]);
      updated++;
    } else {
      toAppend.push(arr);
      inserted++;
    }
  });
  if (toAppend.length) {
    sh.getRange(sh.getLastRow() + 1, 1, toAppend.length, LODGIFY_HEADERS.length).setValues(toAppend);
  }

  // Guarda metadatos en Document Properties (rápido de leer)
  const props = PropertiesService.getDocumentProperties();
  props.setProperty("LODGIFY_LAST_SYNC", nowIso);
  props.setProperty("LODGIFY_LAST_SYNC_RANGE", from + "→" + to);

  // Propaga cada booking a las hojas canónicas (Perfiles / Reservaciones).
  // Perfiles: 1 fila por celular único (no se duplica si ya existe).
  // Reservaciones: 1 fila por booking de Lodgify (idempotente por "Lodgify Id").
  let canonical = { perfiles_inserted: 0, reservaciones_inserted: 0, reservaciones_skipped: 0 };
  try { canonical = propagateLodgifyToCanonical_(bookings); }
  catch (err) { canonical.error = err.message || String(err); }

  return {
    ok: true,
    from, to,
    rows_fetched: rows.length,
    bookings: bookings.length,
    inserted, updated,
    duplicates_removed: dupCount,
    total_in_sheet: sh.getLastRow() - 1,
    elapsed_ms: Date.now() - startMs,
    last_synced_at: nowIso,
    perfiles_inserted: canonical.perfiles_inserted,
    reservaciones_inserted: canonical.reservaciones_inserted,
    reservaciones_linked: canonical.reservaciones_linked,
    reservaciones_skipped: canonical.reservaciones_skipped,
    bookings_filtered_out: canonical.bookings_filtered_out,
    canonical_error: canonical.error,
  };
}

/** Devuelve los últimos 10 dígitos de un teléfono (para match cross-fuente). */
function lodgifyPhoneKey_(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.slice(-10);
}

/** Normaliza una fecha (Date u string en MM/DD/YYYY o YYYY-MM-DD) a "YYYYMMDD"
 *  para comparación robusta entre check-ins manuales y Lodgify. */
/** Wrapper PÚBLICO para correr el backfill desde el editor de Apps Script.
 *  Las funciones que terminan en "_" son privadas y no aparecen en el
 *  selector de funciones del editor → necesitamos un wrapper sin "_". */
function fixLodgifyDates() {
  const res = fixLodgifyDatesInReservaciones_();
  Logger.log(JSON.stringify(res, null, 2));
  return res;
}

/** Backfill one-shot: para cada fila de Reservaciones con "Lodgify Id" no
 *  vacío, busca el booking en Reservas_Lodgify y re-escribe sus
 *  "Fecha de ingreso" / "Fecha de salida" en ISO YYYY-MM-DD. Corrige el
 *  daño causado por escrituras anteriores que sufrieron el swap DD/MM
 *  por locale es-MX. Idempotente: si ya está en ISO correcto, no toca.
 *  Llámala vía el wrapper fixLodgifyDates() desde el editor. */
function fixLodgifyDatesInReservaciones_() {
  ensureNormalizedSheets_();
  const ss = getSpreadsheet_();
  const shR = ss.getSheetByName(RESERVACIONES_SHEET);
  const shL = ss.getSheetByName(LODGIFY_SHEET);
  if (!shR || !shL) return { ok:false, error:"Faltan hojas." };

  const headersR = shR.getRange(1, 1, 1, shR.getLastColumn()).getValues()[0];
  const idxLodId = headersR.indexOf("Lodgify Id");
  const idxFi    = headersR.indexOf("Fecha de ingreso");
  const idxFs    = headersR.indexOf("Fecha de salida");
  if (idxLodId < 0 || idxFi < 0 || idxFs < 0) {
    return { ok:false, error:"Faltan columnas (Lodgify Id / Fecha de ingreso / Fecha de salida)." };
  }

  // Mapa Lodgify Id → { arrival, departure } leyendo Reservas_Lodgify.
  const headersL = shL.getRange(1, 1, 1, shL.getLastColumn()).getValues()[0];
  const idxLId   = headersL.indexOf("Id");
  const idxLArr  = headersL.indexOf("DateArrival");
  const idxLDep  = headersL.indexOf("DateDeparture");
  const lastL    = shL.getLastRow();
  const lodgifyById = {};
  if (lastL >= 2) {
    const data = shL.getRange(2, 1, lastL - 1, headersL.length).getDisplayValues();
    for (let i = 0; i < data.length; i++) {
      const id = String(data[i][idxLId] || "").trim();
      if (!id) continue;
      lodgifyById[id] = {
        arrival:   lodgifyDateToIso_(data[i][idxLArr]),
        departure: lodgifyDateToIso_(data[i][idxLDep]),
      };
    }
  }

  const lastR = shR.getLastRow();
  if (lastR < 2) return { ok:true, scanned:0, fixed:0 };
  // Forzar formato @ en ambas columnas antes de escribir
  shR.getRange(2, idxFi + 1, lastR - 1, 1).setNumberFormat('@');
  shR.getRange(2, idxFs + 1, lastR - 1, 1).setNumberFormat('@');

  const data = shR.getRange(2, 1, lastR - 1, headersR.length).getDisplayValues();
  let scanned = 0, fixed = 0;
  for (let i = 0; i < data.length; i++) {
    const lodId = String(data[i][idxLodId] || "").trim();
    if (!lodId) continue;
    const expected = lodgifyById[lodId];
    if (!expected) continue;
    scanned++;
    const currArr = lodgifyDateToIso_(data[i][idxFi]);
    const currDep = lodgifyDateToIso_(data[i][idxFs]);
    if (currArr !== expected.arrival || currDep !== expected.departure) {
      const row = i + 2;
      shR.getRange(row, idxFi + 1).setValue(expected.arrival);
      shR.getRange(row, idxFs + 1).setValue(expected.departure);
      fixed++;
    }
  }
  SpreadsheetApp.flush();
  return { ok:true, scanned, fixed };
}

/** Convierte una fecha Lodgify (MM/DD/YYYY o ISO o Date) a "YYYY-MM-DD"
 *  para guardar en Reservaciones SIN ambigüedad de locale. Lodgify envía
 *  MM/DD/YYYY y Sheets en es-MX lo interpretaría como DD/MM/YYYY → swap
 *  de día y mes. ISO YYYY-MM-DD es locale-independiente. */
/** Carga el catálogo "alojamientos" como índices Map por HouseId y por
 *  HouseName normalizado, para resolver Propiedad/# Departamento canónicos
 *  durante la propagación de Lodgify → Reservaciones. */
function buildAlojamientosIndex_() {
  const ss = getSpreadsheet_();
  const idx = { byHouseId: {}, byHouseName: {} };
  let sh = ss.getSheetByName(ALOJAMIENTOS_SHEET);
  if (!sh) {
    // Tolerancia al case (ej. "Alojamientos" vs "alojamientos")
    const all = ss.getSheets();
    for (var k = 0; k < all.length; k++) {
      if (all[k].getName().toLowerCase() === ALOJAMIENTOS_SHEET.toLowerCase()) { sh = all[k]; break; }
    }
  }
  if (!sh) return idx;
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return idx;
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(v => String(v || '').trim());
  const colHId  = headers.indexOf("HouseId");
  const colHN   = headers.indexOf("HouseName");
  const colProp = headers.indexOf("Propiedad");
  const colDpt  = headers.indexOf("# Departamento");
  if (colProp < 0 || colDpt < 0) return idx;
  const data = sh.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();
  const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  for (let i = 0; i < data.length; i++) {
    const propiedad = String(data[i][colProp] || "").trim();
    const departamento = String(data[i][colDpt] || "").trim();
    if (!propiedad && !departamento) continue;
    const rec = { propiedad, departamento };
    if (colHId >= 0) {
      const hid = String(data[i][colHId] || "").trim();
      if (hid) idx.byHouseId[hid] = rec;
    }
    if (colHN >= 0) {
      const hn = norm(data[i][colHN]);
      if (hn) idx.byHouseName[hn] = rec;
    }
  }
  return idx;
}

/** Resuelve { propiedad, departamento } canónicos desde el catálogo
 *  alojamientos para un booking de Lodgify. Fallback a HouseName/RoomTypeNames
 *  si no hay match. */
function resolvePropiedadFromAloj_(idx, b) {
  if (!idx) return { propiedad: b.HouseName || "", departamento: b.RoomTypeNames || "" };
  const hid = String(b.HouseId || "").trim();
  if (hid && idx.byHouseId[hid]) return idx.byHouseId[hid];
  const norm = String(b.HouseName || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  if (norm && idx.byHouseName[norm]) return idx.byHouseName[norm];
  return { propiedad: b.HouseName || "", departamento: b.RoomTypeNames || "" };
}

function lodgifyDateToIso_(value) {
  if (!value) return "";
  if (Object.prototype.toString.call(value) === "[object Date]") {
    if (isNaN(value.getTime())) return "";
    const y = value.getFullYear();
    const m = String(value.getMonth() + 1).padStart(2, "0");
    const d = String(value.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }
  const s = String(value).trim();
  let m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return `${m[3]}-${m[1].padStart(2,"0")}-${m[2].padStart(2,"0")}`;
  m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  return s;
}

function lodgifyDateKey_(value) {
  if (!value) return "";
  if (Object.prototype.toString.call(value) === "[object Date]") {
    if (isNaN(value.getTime())) return "";
    const y = value.getFullYear();
    const m = String(value.getMonth() + 1).padStart(2, "0");
    const d = String(value.getDate()).padStart(2, "0");
    return y + m + d;
  }
  const s = String(value).trim();
  let m;
  if ((m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/))) {
    return m[3] + m[1].padStart(2, "0") + m[2].padStart(2, "0");
  }
  if ((m = s.match(/^(\d{4})-(\d{2})-(\d{2})/))) {
    return m[1] + m[2] + m[3];
  }
  return s.replace(/\D/g, "").slice(0, 8);
}

/** Propaga bookings de Lodgify a Perfiles y Reservaciones.
 *  Reglas:
 *    - Sólo Status === 'Booked' (descarta Declined/Tentative/Open).
 *    - Perfiles: inserta sólo si el celular (últimos 10 dígitos) no existe.
 *    - Reservaciones:
 *        a) Si el "Lodgify Id" ya está → skip.
 *        b) Si existe fila con mismo phone+fecha de ingreso → escribe el
 *           Lodgify Id en esa fila (enlace; no toca el resto de los datos).
 *        c) Si no hay match → inserta nueva fila. */
function propagateLodgifyToCanonical_(bookings) {
  ensureNormalizedSheets_();
  const ss = getSpreadsheet_();
  const shP = ss.getSheetByName(PERFILES_SHEET);
  const shR = ss.getSheetByName(RESERVACIONES_SHEET);
  if (!shP || !shR) {
    return { perfiles_inserted: 0, reservaciones_inserted: 0, reservaciones_linked: 0, reservaciones_skipped: 0, bookings_filtered_out: 0 };
  }

  // Catálogo "alojamientos" para homologar Propiedad/# Departamento.
  const alojIdx = buildAlojamientosIndex_();

  // Sólo bookings confirmados.
  const confirmed = bookings.filter(b => String(b.Status || "").toLowerCase() === "booked");
  const filteredOut = bookings.length - confirmed.length;

  // --- Index Perfiles por phoneKey ---
  const headersP = shP.getRange(1, 1, 1, shP.getLastColumn()).getValues()[0];
  const idxPCel  = headersP.indexOf("Cel/Whatsapp (principal)");
  const lastP    = shP.getLastRow();
  const perfilesPhones = new Set();
  if (lastP >= 2 && idxPCel >= 0) {
    const cels = shP.getRange(2, idxPCel + 1, lastP - 1, 1).getValues();
    for (let i = 0; i < cels.length; i++) {
      const k = lodgifyPhoneKey_(cels[i][0]);
      if (k) perfilesPhones.add(k);
    }
  }

  // --- Index Reservaciones por (Lodgify Id) y por (phoneKey + fechaIngreso) ---
  const headersR = shR.getRange(1, 1, 1, shR.getLastColumn()).getValues()[0];
  const idxRLod  = headersR.indexOf("Lodgify Id");
  const idxRCel  = headersR.indexOf("Cel/Whatsapp (principal)");
  const idxRFi   = headersR.indexOf("Fecha de ingreso");
  const lastR    = shR.getLastRow();
  const reservasLodIds = new Set();
  const reservasByPhoneDate = new Map(); // key → { row, hasLodId }
  if (lastR >= 2 && idxRLod >= 0 && idxRCel >= 0 && idxRFi >= 0) {
    const data = shR.getRange(2, 1, lastR - 1, headersR.length).getValues();
    for (let i = 0; i < data.length; i++) {
      const row = i + 2;
      const lodVal = String(data[i][idxRLod] || "").trim();
      if (lodVal) reservasLodIds.add(lodVal);
      const phoneK = lodgifyPhoneKey_(data[i][idxRCel]);
      const dateK  = lodgifyDateKey_(data[i][idxRFi]);
      if (phoneK && dateK) {
        const key = phoneK + "|" + dateK;
        if (!reservasByPhoneDate.has(key)) {
          reservasByPhoneDate.set(key, { row: row, hasLodId: !!lodVal });
        }
      }
    }
  }

  const now = nowIso_();
  const perfilesToAppend = [];
  const reservasToAppend = [];
  const linkUpdates = []; // [{row, lodId}]
  let resSkipped = 0;
  const batchPhones = new Set();

  confirmed.forEach(b => {
    const phoneKey = lodgifyPhoneKey_(b.GuestPhone);
    const name  = String(b.GuestName  || "").trim();
    const email = String(b.GuestEmail || "").trim();

    // PERFILES — sólo si hay teléfono y no existe ya.
    if (phoneKey && !perfilesPhones.has(phoneKey) && !batchPhones.has(phoneKey)) {
      const rowP = headersP.map(h => {
        switch (h) {
          case "ID_Perfil": return Utilities.getUuid();
          case "Cel/Whatsapp (principal)": return b.GuestPhone || "";
          case "Lada celular huésped": return b.GuestCountryCode || "";
          case "Nombre del huésped": return name;
          case "Correo electrónico para el envío de la factura": return email;
          case "Fecha creación": return now;
          case "Fecha actualización": return now;
          default: return "";
        }
      });
      perfilesToAppend.push(rowP);
      batchPhones.add(phoneKey);
    }

    // RESERVACIONES
    const lodId = String(b.Id || "").trim();
    if (!lodId) return;
    if (reservasLodIds.has(lodId)) { resSkipped++; return; }

    // ¿Hay fila manual existente con mismo phone + fecha de ingreso?
    const dateK = lodgifyDateKey_(b.DateArrival);
    const matchKey = (phoneKey && dateK) ? (phoneKey + "|" + dateK) : "";
    const match = matchKey ? reservasByPhoneDate.get(matchKey) : null;

    if (match && !match.hasLodId) {
      // Enlazar: escribir Lodgify Id en la fila manual existente.
      linkUpdates.push({ row: match.row, lodId: lodId });
      match.hasLodId = true;
      reservasLodIds.add(lodId);
      return;
    }
    if (match && match.hasLodId) {
      // Ya enlazado (a otra reserva Lodgify previa). Skip para no duplicar.
      resSkipped++;
      return;
    }

    // Sin match → insertar nueva fila.
    reservasLodIds.add(lodId);
    if (matchKey) reservasByPhoneDate.set(matchKey, { row: -1, hasLodId: true });
    // Homologa Propiedad/# Departamento desde el catálogo alojamientos.
    const aloj = resolvePropiedadFromAloj_(alojIdx, b);
    const rowR = headersR.map(h => {
      switch (h) {
        case "ID": return Utilities.getUuid();
        case "Cel/Whatsapp (principal)": return b.GuestPhone || "";
        case "Marca temporal": return now;
        case "Medio de reservación": return b.Source || "";
        case "Propiedad": return aloj.propiedad;
        case "# Departamento": return aloj.departamento;
        case "Fecha de ingreso": return lodgifyDateToIso_(b.DateArrival);
        case "Fecha de salida": return lodgifyDateToIso_(b.DateDeparture);
        case "# Noches": return Number(b.Nights) || 0;
        case "# Huéspedes": return Number(b.NumberOfGuests) || 0;
        case "Nombre de la persona que hizo la reservación": return name;
        case "Correo electrónico": return email;
        case "Lodgify Id": return lodId;
        default: return "";
      }
    });
    reservasToAppend.push(rowR);
  });

  if (perfilesToAppend.length) {
    shP.getRange(shP.getLastRow() + 1, 1, perfilesToAppend.length, headersP.length)
       .setValues(perfilesToAppend);
  }
  // Escribir links (Lodgify Id en filas manuales existentes).
  linkUpdates.forEach(u => {
    shR.getRange(u.row, idxRLod + 1).setValue(u.lodId);
  });
  if (reservasToAppend.length) {
    const startRow = shR.getLastRow() + 1;
    // Forzar formato texto en las columnas de fecha ANTES de escribir, para
    // que Sheets NO auto-parsee MM/DD/YYYY como DD/MM/YYYY (locale es-MX).
    // Usamos ISO YYYY-MM-DD desde lodgifyDateToIso_, pero el @ es seguro
    // extra en caso de cualquier formato edge.
    ['Fecha de ingreso','Fecha de salida'].forEach(colName => {
      const idx = headersR.indexOf(colName);
      if (idx < 0) return;
      shR.getRange(startRow, idx + 1, reservasToAppend.length, 1).setNumberFormat('@');
    });
    shR.getRange(startRow, 1, reservasToAppend.length, headersR.length)
       .setValues(reservasToAppend);
  }

  return {
    perfiles_inserted: perfilesToAppend.length,
    reservaciones_inserted: reservasToAppend.length,
    reservaciones_linked: linkUpdates.length,
    reservaciones_skipped: resSkipped,
    bookings_filtered_out: filteredOut,
  };
}

/** ACCIÓN: lodgify_list — devuelve todas las filas de la hoja (rápido).
 *  Filtro opcional: source, status, name_contains, limit, since (ISO). */
function getLodgifyReservations_(data) {
  data = data || {};
  const sh = ensureLodgifySheet_();
  const last = sh.getLastRow();
  if (last < 2) {
    return { ok: true, bookings: [], total: 0, last_synced_at: getLodgifyMeta_().last_synced_at };
  }
  const headers = sh.getRange(1, 1, 1, LODGIFY_HEADERS.length).getValues()[0];
  const values = sh.getRange(2, 1, last - 1, LODGIFY_HEADERS.length).getValues();
  let rows = values.map(function(r, idx) {
    var o = {};
    headers.forEach(function(h, i) { o[h] = r[i]; });
    o.__row_number = idx + 2;
    if (o.LineItemsJSON) {
      try { o.LineItems = JSON.parse(o.LineItemsJSON); } catch(_) { o.LineItems = []; }
    } else { o.LineItems = []; }
    return o;
  });

  // ─── DEDUP DEFINITIVO ─────────────────────────────────────────────────────
  // Si hay filas duplicadas por Id, conservamos la de last_synced_at MÁS
  // RECIENTE y BORRAMOS físicamente las otras. Esto resuelve el bug donde
  // tras un sync el monto se actualizaba en una fila pero la vieja seguía
  // siendo leída por el frontend.
  var byId = {};
  var rowsToDelete = [];
  rows.forEach(function(r) {
    var id = String(r.Id == null ? "" : r.Id).trim();
    if (!id) return;
    var lsa = String(r.last_synced_at || "");
    if (!byId[id]) {
      byId[id] = r;
    } else if (lsa > String(byId[id].last_synced_at || "")) {
      // Esta fila es MÁS reciente → la vieja sale del sheet.
      rowsToDelete.push(byId[id].__row_number);
      byId[id] = r;
    } else {
      // Esta fila es la duplicada/vieja → sale del sheet.
      rowsToDelete.push(r.__row_number);
    }
  });
  if (rowsToDelete.length) {
    // Borrar de atrás hacia adelante para no shiftear índices
    rowsToDelete.sort(function(a, b) { return b - a; });
    rowsToDelete.forEach(function(rn) { sh.deleteRow(rn); });
  }
  rows = Object.keys(byId).map(function(k) { return byId[k]; });

  // Ocultar bookings marcados como "eliminados" del frontend (sólo se ocultan,
  // no se borran del sheet maestro)
  const hiddenIds = getLodgifyHiddenIds_();
  if (hiddenIds.size) {
    rows = rows.filter(function(r){ return !hiddenIds.has(String(r.Id||"").trim()); });
  }

  // Filtros opcionales
  const src = String(data.source || "").trim().toLowerCase();
  const st  = String(data.status || "").trim().toLowerCase();
  const nm  = String(data.name_contains || "").trim().toLowerCase();
  if (src) rows = rows.filter(r => String(r.Source||"").toLowerCase() === src);
  if (st)  rows = rows.filter(r => String(r.Status||"").toLowerCase() === st);
  if (nm)  rows = rows.filter(r => String(r.GuestName||"").toLowerCase().indexOf(nm) >= 0);

  // Orden descendente por DateArrival (MM/DD/YYYY)
  rows.sort((a,b) => {
    const ma = String(a.DateArrival||"").match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    const mb = String(b.DateArrival||"").match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    const da = ma ? new Date(+ma[3], +ma[1]-1, +ma[2]).getTime() : 0;
    const db = mb ? new Date(+mb[3], +mb[1]-1, +mb[2]).getTime() : 0;
    return db - da;
  });

  const limit = Number(data.limit) || 0;
  if (limit > 0) rows = rows.slice(0, limit);

  return {
    ok: true,
    bookings: rows,
    total: rows.length,
    last_synced_at: getLodgifyMeta_().last_synced_at,
    last_synced_range: getLodgifyMeta_().last_synced_range,
  };
}

function getLodgifyMeta_() {
  const props = PropertiesService.getDocumentProperties();
  return {
    last_synced_at: props.getProperty("LODGIFY_LAST_SYNC") || "",
    last_synced_range: props.getProperty("LODGIFY_LAST_SYNC_RANGE") || "",
  };
}

// ─── Reservas ocultas del frontend (no se borran del sheet maestro) ─────────
function ensureLodgifyHiddenSheet_() {
  const ss = getSpreadsheet_();
  let sh = ss.getSheetByName(LODGIFY_HIDDEN_SHEET);
  if (!sh) {
    sh = ss.insertSheet(LODGIFY_HIDDEN_SHEET);
    sh.getRange(1, 1, 1, 3).setValues([["Id", "hidden_at", "hidden_by"]]);
    sh.getRange(1, 1, 1, 3).setFontWeight("bold").setBackground("#fef3c7");
    sh.setFrozenRows(1);
  }
  return sh;
}

function getLodgifyHiddenIds_() {
  const sh = ensureLodgifyHiddenSheet_();
  const last = sh.getLastRow();
  const set = new Set();
  if (last < 2) return set;
  const ids = sh.getRange(2, 1, last - 1, 1).getValues();
  ids.forEach(function(row){
    const id = String(row[0] || "").trim();
    if (id) set.add(id);
  });
  return set;
}

function hideLodgifyBooking_(data) {
  data = data || {};
  const id = String(data.id || "").trim();
  if (!id) return { ok: false, error: "id requerido" };
  const sh = ensureLodgifyHiddenSheet_();
  // Evitar duplicados
  const existing = getLodgifyHiddenIds_();
  if (existing.has(id)) return { ok: true, already_hidden: true, id: id };
  const now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  sh.appendRow([id, now, String(data.hidden_by || "")]);
  return { ok: true, id: id, hidden_at: now };
}

// ─── Reservaciones ocultas (post-unificación) ────────────────────────────────
const RESERVACIONES_HIDDEN_SHEET = "Reservaciones_Hidden";

function ensureReservacionesHiddenSheet_() {
  const ss = getSpreadsheet_();
  let sh = ss.getSheetByName(RESERVACIONES_HIDDEN_SHEET);
  if (!sh) {
    sh = ss.insertSheet(RESERVACIONES_HIDDEN_SHEET);
    sh.getRange(1, 1, 1, 4).setValues([["ID", "merged_into_ID", "hidden_at", "hidden_by"]]);
    sh.getRange(1, 1, 1, 4).setFontWeight("bold").setBackground("#fef3c7");
    sh.setFrozenRows(1);
  }
  return sh;
}

function getReservacionesHiddenSet_() {
  const sh = ensureReservacionesHiddenSheet_();
  const last = sh.getLastRow();
  const set = new Set();
  if (last < 2) return set;
  const ids = sh.getRange(2, 1, last - 1, 1).getValues();
  ids.forEach(r => {
    const v = String(r[0] || "").trim();
    if (v) set.add(v);
  });
  return set;
}

/** Unifica dos filas de Reservaciones: copia campos faltantes desde loser
 *  hacia winner y marca loser como oculta.
 *  Input: { winner_id, loser_id, fields: {Header: value, ...}, hidden_by }
 *  - winner_id: ID de la fila que sobrevive (la del registro manual)
 *  - loser_id:  ID de la fila que se oculta (la propagada por Lodgify)
 *  - fields:    pares header→valor a escribir en el winner (solo los que
 *               estaban vacíos). El cliente decide cuáles.
 */
function unifyReservacionesRows_(data) {
  data = data || {};
  const winnerId = String(data.winner_id || "").trim();
  const loserId  = String(data.loser_id  || "").trim();
  if (!winnerId || !loserId) return { ok: false, error: "winner_id y loser_id requeridos" };
  if (winnerId === loserId)   return { ok: false, error: "winner_id y loser_id no pueden ser iguales" };

  const sh = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sh);
  const winnerRow = findRowByValue_(sh, headers, "ID", winnerId);
  if (!winnerRow) return { ok: false, error: "winner_id no encontrado: " + winnerId };

  const fields = (data.fields && typeof data.fields === "object") ? data.fields : {};
  const writes = [];
  Object.keys(fields).forEach(h => {
    const colIdx = headers.indexOf(h);
    if (colIdx < 0) return; // header desconocido, ignorar
    const v = fields[h];
    if (v == null || v === "") return; // no sobrescribir con vacío
    sh.getRange(winnerRow, colIdx + 1).setValue(v);
    writes.push(h);
  });

  // Marcar loser como oculto
  const hSh = ensureReservacionesHiddenSheet_();
  const already = getReservacionesHiddenSet_();
  if (!already.has(loserId)) {
    const now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
    hSh.appendRow([loserId, winnerId, now, String(data.hidden_by || "")]);
  }

  return { ok: true, winner_id: winnerId, loser_id: loserId, fields_written: writes };
}

/** Oculta una reservación del frontend (no la borra del sheet maestro).
 *  Input: { id, hidden_by }
 *  Solo agrega el ID a Reservaciones_Hidden con merged_into_ID vacío.
 */
function hideReservacion_(data) {
  data = data || {};
  const id = String(data.id || "").trim();
  if (!id) return { ok: false, error: "id requerido" };
  const sh = ensureReservacionesHiddenSheet_();
  const existing = getReservacionesHiddenSet_();
  if (existing.has(id)) return { ok: true, already_hidden: true, id: id };
  const now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  sh.appendRow([id, "", now, String(data.hidden_by || "")]);
  return { ok: true, id: id, hidden_at: now };
}

/** Deshace una unificación: quita el ID de Reservaciones_Hidden para que
 *  la fila vuelva a verse en el frontend. NO restaura los campos del
 *  winner (la copia fue aditiva: solo llenó campos que estaban vacíos).
 *  Input: { id }  o  { loser_id }
 *  Si se pasa `merged_into`, valida que esa relación exista antes de borrar.
 */
function unhideReservacion_(data) {
  data = data || {};
  const targetId = String(data.id || data.loser_id || "").trim();
  if (!targetId) return { ok: false, error: "id requerido" };
  const sh = ensureReservacionesHiddenSheet_();
  const last = sh.getLastRow();
  if (last < 2) return { ok: false, error: "Reservaciones_Hidden vacío" };
  const values = sh.getRange(2, 1, last - 1, sh.getLastColumn()).getValues();
  const rowsToDelete = [];
  for (let i = 0; i < values.length; i++) {
    if (String(values[i][0] || "").trim() === targetId) rowsToDelete.push(i + 2);
  }
  if (!rowsToDelete.length) return { ok: false, error: "ID no encontrado en Reservaciones_Hidden: " + targetId };
  // Borrar de atrás hacia adelante para no shiftear índices
  rowsToDelete.sort((a, b) => b - a).forEach(rn => sh.deleteRow(rn));
  return { ok: true, id: targetId, rows_deleted: rowsToDelete.length };
}

/** OPCIONAL: Trigger time-driven. Para activarlo desde el editor:
 *    1. Apps Script editor → Triggers (ícono ⏰) → + Add Trigger
 *    2. Function: lodgifySyncCronJob_
 *    3. Event source: Time-driven
 *    4. Type: Hour timer, every 6 hours (por ejemplo).
 */
function lodgifySyncCronJob_() {
  try {
    const res = syncLodgifyReservations_({ days_back: 60, days_fwd: 365 });
    Logger.log("Lodgify sync OK: " + JSON.stringify(res));
  } catch (e) {
    Logger.log("Lodgify sync ERROR: " + (e && e.message));
  }
}

/** Elimina una reservación de la hoja "Reservaciones" (la fila completa)
 *  identificada por record_id (columna "ID") o por row_number. */
function deleteReservacion_(data) {
  const recordId   = safe_(data.record_id || data.id || data.row_id);
  const explicitRow = safe_(data.row_number || data.rowNumber);
  if (!recordId && !explicitRow) throw new Error("Falta record_id o row_number.");
  const sheet = getSheet_(RESERVACIONES_SHEET);
  const headers = getHeaders_(sheet);
  let row = null;
  if (explicitRow) row = findRowByRowNumber_(sheet, explicitRow);
  if (!row && recordId) {
    row = findRowByValue_(sheet, headers, "ID", recordId);
    if (!row) row = findRowByRowNumber_(sheet, recordId);
  }
  if (!row) throw new Error("No se encontró la reservación.");
  sheet.deleteRow(row);
  return { ok: true, deleted_row: row, record_id: recordId || "" };
}

// ════════════════════════════════════════════════════════════════════════
// ║  CARGA DE DATOS BANCARIOS — acciones de Apps Script                 ║
// ║                                                                      ║
// ║  Soporta la subsección "Carga de datos bancarios" del módulo        ║
// ║  Registros contables: lectura de cuentas_bancarias, dedupe contra   ║
// ║  BANCOS existente, e inserción por lotes desde el frontend.          ║
// ╚════════════════════════════════════════════════════════════════════════

/** Devuelve los registros de la hoja `cuentas_bancarias` para el frontend.
 *  Schema esperado: cuenta_nombre | cuenta_numero | cuenta_tag |
 *                   cuenta_tag_original | cuenta_tipo
 *  El frontend matchea el texto literal del marker en el Excel
 *  (ej. "Digital *2220") contra `cuenta_tag_original` y obtiene
 *  cuenta_nombre + cuenta_tag + cuenta_tipo.
 */
function bnCuentasBancariasList_() {
  const ss = getSpreadsheet_();
  // Detección case-insensitive del nombre (cuentas_bancarias o el viejo
  // cuentas_bancaria).
  const sheets = ss.getSheets();
  const sh = sheets.find(function(s){
    const n = String(s.getName() || "").trim().toLowerCase();
    return n === "cuentas_bancarias" || n === "cuentas_bancaria";
  });
  if (!sh) return { ok: false, error: "No se encontró la hoja 'cuentas_bancarias'." };
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return { ok: true, rows: [] };
  const headers = data[0].map(function(h){ return String(h || "").trim().toLowerCase(); });
  const idx = function(name){ return headers.indexOf(name); };
  const iNom = idx("cuenta_nombre"),
        iNum = idx("cuenta_numero"),
        iTag = idx("cuenta_tag"),
        iOri = idx("cuenta_tag_original"),
        iTip = idx("cuenta_tipo");
  const rows = [];
  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    const nombre = iNom >= 0 ? String(row[iNom] || "").trim() : "";
    if (!nombre) continue;
    rows.push({
      cuenta_nombre:       nombre,
      cuenta_numero:       iNum >= 0 ? String(row[iNum] || "").trim() : "",
      cuenta_tag:          iTag >= 0 ? String(row[iTag] || "").trim() : "",
      cuenta_tag_original: iOri >= 0 ? String(row[iOri] || "").trim() : "",
      cuenta_tipo:         iTip >= 0 ? String(row[iTip] || "").trim() : "",
    });
  }
  return { ok: true, rows: rows };
}

/** Devuelve el índice de keys de dedupe de la hoja BANCOS para que el
 *  frontend pueda detectar duplicados ANTES de pedir la inserción.
 *  Key = Día|Cuenta bancaria|Subcuenta|DESCRIPCION(norm)|CARGO|ABONO|#N
 *  El contador #N se asigna por orden de aparición dentro del mismo grupo
 *  (distingue cargos legítimos idénticos: 2 MERPAGO mismo día = #1 y #2).
 */
function bnBancosDedupeIndex_() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheets().find(function(s){
    return String(s.getName() || "").trim().toUpperCase() === "BANCOS";
  });
  if (!sh) return { ok: false, error: "No se encontró la hoja BANCOS." };
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return { ok: true, keys: [] };
  const headers = data[0].map(function(h){ return String(h || "").trim().toUpperCase(); });
  const pickIdx_ = function(opts){
    for (let i = 0; i < opts.length; i++) {
      const k = headers.indexOf(opts[i].toUpperCase());
      if (k >= 0) return k;
    }
    return -1;
  };
  const iDia = pickIdx_(["DÍA","DIA","FECHA"]);
  const iCta = pickIdx_(["CUENTA BANCARIA"]);
  // Lee de la nueva columna 'Subcuenta_bancaria'. La columna 'SUBCUENTA'
  // legacy se ignora intencionalmente (puede tener clasificación contable
  // distinta de la subcuenta bancaria — no la queremos en la dedupe key).
  const iSub = pickIdx_(["SUBCUENTA_BANCARIA","SUBCUENTA BANCARIA"]);
  const iDes = pickIdx_(["DESCRIPCION","DESCRIPCIÓN"]);
  const iCar = pickIdx_(["CARGO"]);
  const iAbo = pickIdx_(["ABONO"]);
  if (iDia < 0 || iDes < 0) {
    return { ok: false, error: "Faltan columnas DÍA o DESCRIPCION en BANCOS." };
  }
  // Día usar DisplayValues para evitar que celdas Date se serialicen con timezone.
  const displ = sh.getDataRange().getDisplayValues();
  // Normalización canónica de strings ROBUSTA: trim, lower, sin acentos,
  // NBSP→space, colapsa whitespace, uniformiza guiones. Debe ser idéntica
  // a bnUploadNormStr() del frontend.
  const norm_ = function(s){
    return String(s || "")
      .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
      .replace(/[\u00a0]/g, " ")
      .replace(/[–—]/g, "-")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ");
  };
  // Normalización canónica de números ROBUSTA: maneja "3000", "3,000.00",
  // "$3,000.00", "$ 3000", "(3000)" (parens negativo). Debe ser idéntica
  // a bnUploadNumStr() del frontend.
  const numStr_ = function(v){
    if (v === null || v === undefined || v === "") return "";
    if (typeof v === "number" && isFinite(v)) return String(Math.round(v * 100) / 100);
    let s = String(v).trim();
    if (!s) return "";
    const isParenNeg = /^\(.*\)$/.test(s);
    if (isParenNeg) s = "-" + s.slice(1, -1);
    s = s.replace(/[$\s,]/g, "");
    const n = Number(s);
    if (isFinite(n)) return String(Math.round(n * 100) / 100);
    return s.trim();
  };
  // Normaliza CUALQUIER formato de fecha a ISO YYYY-MM-DD para la key.
  // CRÍTICO: el frontend hace la misma normalización. Sin esto, filas viejas
  // guardadas como "2026-05-09" no matchean con uploads nuevos "9/5/2026".
  const diaToIso_ = function(s){
    if (!s) return "";
    const str = String(s).trim();
    let m = str.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
    if (m) return m[1] + "-" + String(m[2]).padStart(2,"0") + "-" + String(m[3]).padStart(2,"0");
    m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) return m[3] + "-" + String(m[2]).padStart(2,"0") + "-" + String(m[1]).padStart(2,"0");
    const d = new Date(str);
    if (!isNaN(d.getTime())) {
      return d.getFullYear() + "-" + String(d.getMonth()+1).padStart(2,"0") + "-" + String(d.getDate()).padStart(2,"0");
    }
    return str;
  };
  const seen = {};
  const keys = [];
  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    if (row.join("").toString().trim() === "") continue;
    const diaRaw = iDia >= 0 ? String(displ[r][iDia] || "").trim() : "";
    if (!diaRaw) continue;
    const diaIso = diaToIso_(diaRaw);
    const base = [
      diaIso,
      iCta >= 0 ? norm_(row[iCta]) : "",   // ← CRÍTICO: usa norm_ no solo trim
      iSub >= 0 ? norm_(row[iSub]) : "",   // ← CRÍTICO: usa norm_ no solo trim
      iDes >= 0 ? norm_(row[iDes]) : "",
      iCar >= 0 ? numStr_(row[iCar]) : "",
      iAbo >= 0 ? numStr_(row[iAbo]) : "",
    ].join("|");
    seen[base] = (seen[base] || 0) + 1;
    keys.push(base + "|#" + seen[base]);
  }
  return { ok: true, keys: keys };
}

/** Inserta filas validadas en BANCOS. El frontend manda un array de objetos
 *  con los campos ya calculados (Monto firmado, Subcuenta, Cuenta bancaria,
 *  Año, Mes, etc.). NO recalcula nada — solo escribe a la hoja respetando
 *  los headers actuales (mapeo por nombre de columna, no por posición).
 *  Usa LockService para serializar contra escrituras concurrentes.
 *  data.rows = [{ "Día":"YYYY-MM-DD", "DESCRIPCION":"…", "CARGO":… , "ABONO":… ,
 *                 "SALDO":…|texto, "Monto":…|"", "Cuenta bancaria":"…",
 *                 "Subcuenta":"…", "Año":"…", "Mes":"…", "COMENTARIOS":"…" }, …]
 */
/** Devuelve el subset de filas de BANCOS que tienen al menos UNA
 *  clasificación contable (CUENTA, SUBCUENTA, CATEGORIA, CONCEPTO).
 *  El frontend lo usa como base de "memoria" para auto-clasificar nuevas
 *  filas similares (por DESCRIPCION + MONTO).
 *  Output mínimo por fila: { descripcion, monto, cuenta, subcuenta,
 *                             categoria, concepto }
 *  Se omiten filas vacías o sin descripcion para optimizar payload. */
function bnBancosClassifiedHistory_() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheets().find(function(s){
    return String(s.getName() || "").trim().toUpperCase() === "BANCOS";
  });
  if (!sh) return { ok: false, error: "No se encontró la hoja BANCOS." };
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return { ok: true, rows: [] };
  const headers = data[0].map(function(h){ return String(h || "").trim().toUpperCase(); });
  const pickIdx = function(opts){
    for (var i = 0; i < opts.length; i++) {
      var k = headers.indexOf(opts[i].toUpperCase());
      if (k >= 0) return k;
    }
    return -1;
  };
  const iDes = pickIdx(["DESCRIPCION","DESCRIPCIÓN"]);
  const iMon = pickIdx(["MONTO"]);
  const iCue = pickIdx(["CUENTA"]);
  const iSub = pickIdx(["SUBCUENTA"]);
  const iCat = pickIdx(["CATEGORIA","CATEGORÍA"]);
  const iCon = pickIdx(["CONCEPTO"]);
  if (iDes < 0) return { ok: false, error: "BANCOS no tiene columna DESCRIPCION." };
  const rows = [];
  for (var r = 1; r < data.length; r++) {
    const row = data[r];
    const desc = iDes >= 0 ? String(row[iDes] || "").trim() : "";
    if (!desc) continue;
    const cuenta    = iCue >= 0 ? String(row[iCue] || "").trim() : "";
    const subcuenta = iSub >= 0 ? String(row[iSub] || "").trim() : "";
    const categoria = iCat >= 0 ? String(row[iCat] || "").trim() : "";
    const concepto  = iCon >= 0 ? String(row[iCon] || "").trim() : "";
    // Skip si no tiene NINGUNA clasificación
    if (!cuenta && !subcuenta && !categoria && !concepto) continue;
    const monto = iMon >= 0 ? Number(row[iMon]) : 0;
    rows.push({
      descripcion: desc,
      monto: isFinite(monto) ? monto : 0,
      cuenta: cuenta,
      subcuenta: subcuenta,
      categoria: categoria,
      concepto: concepto,
    });
  }
  return { ok: true, rows: rows };
}

function bnEnsureBancosColumns_(sh, requiredColumns) {
  function _norm(s) {
    return String(s || "").trim().toLowerCase()
      .normalize("NFD").replace(new RegExp("[̀-ͯ]", "g"), "")
      .replace(/[_\s]+/g, "_");
  }
  var headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0]
                  .map(function(h){ return String(h || "").trim(); });
  var existingNorm = {};
  headers.forEach(function(h){ existingNorm[_norm(h)] = true; });
  var missing = requiredColumns.filter(function(c){ return !existingNorm[_norm(c)]; });
  if (!missing.length) return;
  // Append missing columns to the right
  var startCol = sh.getLastColumn() + 1;
  sh.insertColumnsAfter(sh.getLastColumn(), missing.length);
  sh.getRange(1, startCol, 1, missing.length).setValues([missing])
    .setFontWeight("bold").setBackground("#fee2e2");
  SpreadsheetApp.flush();
}

function bnBancosInsertBulk_(data) {
  const rows = (data && data.rows) || [];
  if (!Array.isArray(rows) || !rows.length) {
    return { ok: true, inserted: 0, message: "Sin filas para insertar." };
  }
  const ss = getSpreadsheet_();
  const sh = ss.getSheets().find(function(s){
    return String(s.getName() || "").trim().toUpperCase() === "BANCOS";
  });
  if (!sh) return { ok: false, error: "No se encontró la hoja BANCOS." };
  const lock = LockService.getScriptLock();
  try { lock.waitLock(30000); }
  catch (e) { return { ok: false, error: "No se pudo adquirir lock (otra escritura en curso)." }; }
  try {
    // Auto-crear columnas ORIGEN/DESTINO si no existen (módulo Efectivo)
    bnEnsureBancosColumns_(sh, ["ORIGEN/DESTINO", "ORIGEN/DESTINO_comments"]);
    const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0]
                      .map(function(h){ return String(h || "").trim(); });
    // Match case/accent-insensitive: frontend manda keys "CARGO", "Día",
    // "DESCRIPCION", "# Cuenta" pero el sheet puede tener "Cargo", "Día",
    // "Descripción", "# cuenta". Normalizamos para que matchee siempre.
    function _norm(s) {
      // Lower + sin acentos + colapsa espacios/underscores como equivalentes.
      // Ej.: "Subcuenta_bancaria" == "Subcuenta bancaria" == "SUBCUENTA  BANCARIA"
      return String(s || "").trim().toLowerCase()
        .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
        .replace(/[_\s]+/g, "_");
    }
    // Headers que NUNCA se llenan desde el upload. CUENTA/SUBCUENTA/CATEGORIA/
    // CONCEPTO sí se pueden llenar (clasificación automática vía
    // bn_bancos_classified_history). Solo dejamos vacío lo que no es del
    // dominio bancario (nada por ahora — el set queda vacío).
    const SKIP_HEADERS = {};
    const matrix = rows.map(function(r){
      const rowNormMap = {};
      Object.keys(r || {}).forEach(function(k){
        rowNormMap[_norm(k)] = r[k];
      });
      return headers.map(function(h){
        const hn = _norm(h);
        if (SKIP_HEADERS[hn]) return ""; // ignora columnas de clasificación
        const v = rowNormMap[hn];
        if (v === undefined || v === null) return "";
        return v;
      });
    });
    const startRow = sh.getLastRow() + 1;
    sh.getRange(startRow, 1, matrix.length, headers.length).setValues(matrix);
    SpreadsheetApp.flush();
    return { ok: true, inserted: matrix.length, startRow: startRow };
  } finally {
    lock.releaseLock();
  }
}

// ════════════════════════════════════════════════════════════════════════
// ║  DRIVE FOLDER IMPORTER — lectura de carpeta + tracking de importados ║
// ╚════════════════════════════════════════════════════════════════════════

const BN_DRIVE_DEFAULT_FOLDER_ID = "1pnUUvWpt0yzYjIwtkiYM2F2T_iLN9lNZ";
const BN_IMPORTED_FILES_SHEET    = "Bancos_Imported_Files";
const BN_IMPORTED_FILES_HEADERS  = [
  "file_id","filename","folder_id","imported_at","imported_by","rows_inserted"
];

/** Lista archivos xlsx/xls/csv de una carpeta de Drive RECURSIVAMENTE
 *  (recorre todas las subcarpetas anidadas). La carpeta raíz debe estar
 *  compartida con el usuario que deployó el Apps Script.
 *  Devuelve [{id, name, path, mimeType, size, lastModified}] — 'path'
 *  incluye la ruta relativa "Subcarpeta/Subcarpeta2/archivo.xlsx".
 *  Opciones: max_depth (10), max_files (2000). */
function bnDriveListFiles_(data) {
  const folderId = (data && data.folder_id) || BN_DRIVE_DEFAULT_FOLDER_ID;
  const maxDepth = Math.max(1, Math.min((data && Number(data.max_depth)) || 10, 20));
  const maxFiles = Math.max(1, Math.min((data && Number(data.max_files)) || 2000, 5000));
  try {
    const root = DriveApp.getFolderById(folderId);
    const allFiles = [];
    const visited = {}; // anti-cycle (shortcuts, etc.)
    function walk(folder, depth, prefix) {
      if (depth > maxDepth || allFiles.length >= maxFiles) return;
      // 1) Archivos directos
      const fIter = folder.getFiles();
      while (fIter.hasNext() && allFiles.length < maxFiles) {
        const f = fIter.next();
        const id = f.getId();
        if (visited[id]) continue;
        visited[id] = true;
        const name = f.getName();
        const ext = String(name.split(".").pop() || "").toLowerCase();
        if (ext !== "xlsx" && ext !== "xls" && ext !== "csv") continue;
        allFiles.push({
          id: id,
          name: name,
          path: prefix ? (prefix + "/" + name) : name,
          mimeType: f.getMimeType(),
          size: f.getSize(),
          lastModified: f.getLastUpdated().toISOString(),
        });
      }
      // 2) Subcarpetas (recursivo)
      const dIter = folder.getFolders();
      while (dIter.hasNext() && allFiles.length < maxFiles) {
        const sub = dIter.next();
        const subId = sub.getId();
        if (visited[subId]) continue;
        visited[subId] = true;
        const subName = sub.getName();
        walk(sub, depth + 1, prefix ? (prefix + "/" + subName) : subName);
      }
    }
    walk(root, 0, "");
    allFiles.sort(function(a, b){ return b.lastModified.localeCompare(a.lastModified); });
    return {
      ok: true,
      folder_id: folderId,
      files: allFiles,
      truncated: allFiles.length >= maxFiles,
    };
  } catch (e) {
    return {
      ok: false,
      error: "No se pudo leer la carpeta de Drive. Verifica que esté compartida con el dueño del Apps Script. (" + (e.message || e) + ")"
    };
  }
}

/** Descarga el contenido de un archivo de Drive como base64. El frontend lo
 *  convierte a ArrayBuffer y lo pasa al parser de SheetJS. */
function bnDriveGetFile_(data) {
  const fileId = data && data.file_id;
  if (!fileId) return { ok: false, error: "Falta file_id" };
  try {
    const file = DriveApp.getFileById(fileId);
    const blob = file.getBlob();
    const bytes = blob.getBytes();
    return {
      ok: true,
      file_id: fileId,
      name: file.getName(),
      mimeType: blob.getContentType(),
      size: bytes.length,
      base64: Utilities.base64Encode(bytes),
    };
  } catch (e) {
    return { ok: false, error: "No se pudo leer el archivo: " + (e.message || e) };
  }
}

/** Devuelve la lista de archivos ya importados a BANCOS (file_id + meta). */
function bnImportedFilesList_() {
  const ss = getSpreadsheet_();
  const sh = ss.getSheetByName(BN_IMPORTED_FILES_SHEET);
  if (!sh) return { ok: true, files: [] };
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return { ok: true, files: [] };
  const headers = data[0].map(function(h){ return String(h || "").trim().toLowerCase(); });
  const idx = function(name){ return headers.indexOf(name); };
  const iId   = idx("file_id");
  const iName = idx("filename");
  const iAt   = idx("imported_at");
  const iBy   = idx("imported_by");
  const iRows = idx("rows_inserted");
  const files = [];
  for (var r = 1; r < data.length; r++) {
    const row = data[r];
    const id = iId >= 0 ? String(row[iId] || "").trim() : "";
    if (!id) continue;
    files.push({
      file_id:       id,
      filename:      iName >= 0 ? String(row[iName] || "") : "",
      imported_at:   iAt   >= 0 ? String(row[iAt]   || "") : "",
      imported_by:   iBy   >= 0 ? String(row[iBy]   || "") : "",
      rows_inserted: iRows >= 0 ? Number(row[iRows] || 0)  : 0,
    });
  }
  return { ok: true, files: files };
}

/** Registra un archivo como importado. Si ya existe (same file_id),
 *  agrega una nueva fila — para tener histórico de reimportaciones. */
function bnImportedFilesMark_(data) {
  if (!data || !data.file_id) return { ok: false, error: "Falta file_id" };
  const ss = getSpreadsheet_();
  let sh = ss.getSheetByName(BN_IMPORTED_FILES_SHEET);
  if (!sh) {
    sh = ss.insertSheet(BN_IMPORTED_FILES_SHEET);
    sh.getRange(1, 1, 1, BN_IMPORTED_FILES_HEADERS.length)
      .setValues([BN_IMPORTED_FILES_HEADERS])
      .setFontWeight("bold")
      .setBackground("#0d9488")
      .setFontColor("#ffffff");
    sh.setFrozenRows(1);
  }
  sh.appendRow([
    String(data.file_id),
    String(data.filename || ""),
    String(data.folder_id || ""),
    new Date().toISOString(),
    String(data.imported_by || ""),
    Number(data.rows_inserted) || 0,
  ]);
  return { ok: true };
}

// ════════════════════════════════════════════════════════════════════════════
// ─── TICKET VISION + BANCOS read/save (migrado desde apps_script_completo.gs)
// ════════════════════════════════════════════════════════════════════════════

const SHEET_ID = SPREADSHEET_ID; // alias para compatibilidad con código migrado
const MESES_ES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                  "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];

// ─── Subir imagen de ticket a Drive ──────────────────────────────────────────
function uploadTicketImage_(data) {
  var fileObj = data.file;
  if (!fileObj || !fileObj.base64) return { ok: false, error: "Sin base64" };
  var rawFecha = data.fecha || new Date().toISOString().slice(0, 10);
  var parts    = rawFecha.split("-");
  var anio     = parts[0] || String(new Date().getFullYear());
  var mes      = parseInt(parts[1] || "1", 10);
  var mesStr   = MESES_ES[mes - 1] || "Enero";
  var tienda   = (data.tienda || "sin_tienda").slice(0, 50).replace(/[\/\\:*?"<>|]/g, "_");
  var ts   = Utilities.formatDate(new Date(), "America/Monterrey", "yyyyMMdd_HHmmss");
  var ext  = (fileObj.fileName || ".jpg").split(".").pop().toLowerCase();
  var name = tienda.replace(/\s+/g, "_").slice(0, 30) + "_" + ts + "." + ext;
  var folder = DriveApp.getRootFolder();
  folder = getOrCreateTicketFolder_(folder, "Check Inn - Sistemas");
  folder = getOrCreateTicketFolder_(folder, "Ticket vision");
  folder = getOrCreateTicketFolder_(folder, "Codigo");
  folder = getOrCreateTicketFolder_(folder, "tickets_images");
  folder = getOrCreateTicketFolder_(folder, anio);
  folder = getOrCreateTicketFolder_(folder, mesStr);
  folder = getOrCreateTicketFolder_(folder, tienda);
  var bytes = Utilities.base64Decode(fileObj.base64);
  var blob  = Utilities.newBlob(bytes, fileObj.mimeType || "image/jpeg", name);
  var file  = folder.createFile(blob);
  file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
  return { ok: true, url: file.getUrl(), id: file.getId(), name: file.getName() };
}

function getOrCreateTicketFolder_(parent, name) {
  var iter = parent.getFoldersByName(name);
  if (iter.hasNext()) return iter.next();
  return parent.createFolder(name);
}

// ─── Agregar filas a Sheets ───────────────────────────────────────────────────
function appendRows_(data) {
  var ss = SpreadsheetApp.openById(SHEET_ID);
  if (data.productos && data.productos.length) appendToSheet_(ss, "Transcripcion",   data.productos);
  if (data.resumen   && data.resumen.length)   appendToSheet_(ss, "Resumen tickets", data.resumen);
  if (data.cruce     && data.cruce.length)     appendToSheet_(ss, "Cruce bancario",  data.cruce);
  return { ok: true };
}

function appendToSheet_(ss, sheetName, rows) {
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) sheet = ss.insertSheet(sheetName);
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(Object.keys(rows[0]));
  } else {
    var lastCol = sheet.getLastColumn();
    var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    Object.keys(rows[0]).forEach(function(k) {
      if (headers.indexOf(k) === -1) {
        lastCol++;
        sheet.getRange(1, lastCol).setValue(k);
        headers.push(k);
      }
    });
  }
  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  rows.forEach(function(row) {
    var values = headers.map(function(h) {
      var v = row[h];
      return (v === undefined || v === null) ? "" : v;
    });
    sheet.appendRow(values);
  });
}

// ─── Índice para detección de duplicados ─────────────────────────────────────
function getTicketsIndex_() {
  var ss    = SpreadsheetApp.openById(SHEET_ID);
  var sheet = ss.getSheetByName("Resumen tickets");
  if (!sheet || sheet.getLastRow() < 2) return { ok: true, tickets: [] };
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
  var rows    = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  var idx = {};
  ["tienda","fecha","total","folio","archivo_hash"].forEach(function(k) {
    idx[k] = headers.indexOf(k);
  });
  var tickets = rows.map(function(row) {
    var rawFecha = idx.fecha >= 0 ? row[idx.fecha] : "";
    var fecha = rawFecha instanceof Date
      ? Utilities.formatDate(rawFecha, "America/Monterrey", "yyyy-MM-dd")
      : String(rawFecha || "");
    return {
      tienda:       idx.tienda       >= 0 ? String(row[idx.tienda]       || "") : "",
      fecha:        fecha,
      total:        idx.total        >= 0 ? Number(row[idx.total]        || 0)  : 0,
      folio:        idx.folio        >= 0 ? String(row[idx.folio]        || "") : "",
      archivo_hash: idx.archivo_hash >= 0 ? String(row[idx.archivo_hash] || "") : "",
    };
  }).filter(function(t) { return t.tienda || t.fecha || t.archivo_hash; });
  return { ok: true, tickets: tickets };
}

// ─── Dashboard: todos los tickets ────────────────────────────────────────────
function getAllTickets_() {
  var ss           = SpreadsheetApp.openById(SHEET_ID);
  var resumenSheet = ss.getSheetByName("Resumen tickets");
  if (!resumenSheet || resumenSheet.getLastRow() < 2) return { ok: true, tickets: [] };

  var lastCol = resumenSheet.getLastColumn();
  var headers = resumenSheet.getRange(1, 1, 1, lastCol).getValues()[0];
  var rows    = resumenSheet.getRange(2, 1, resumenSheet.getLastRow() - 1, lastCol).getValues();

  var productsByTicket = {};
  var transcSheet = ss.getSheetByName("Transcripcion");
  if (transcSheet && transcSheet.getLastRow() > 1) {
    var tLastCol = transcSheet.getLastColumn();
    var tHeaders = transcSheet.getRange(1, 1, 1, tLastCol).getValues()[0];
    var tRows    = transcSheet.getRange(2, 1, transcSheet.getLastRow() - 1, tLastCol).getValues();
    var tidIdx   = tHeaders.indexOf("ticket_id");
    var lineaIdx = tHeaders.indexOf("linea_numero");
    var descIdx  = tHeaders.indexOf("descripcion");
    var cantIdx  = tHeaders.indexOf("cantidad");
    var puIdx    = tHeaders.indexOf("precio_unitario");
    var montoIdx = tHeaders.indexOf("monto");
    tRows.forEach(function(tr) {
      var tid = String(tr[tidIdx] || "");
      if (!tid) return;
      if (!productsByTicket[tid]) productsByTicket[tid] = [];
      productsByTicket[tid].push({
        linea_numero:    lineaIdx >= 0 ? tr[lineaIdx]               : "",
        descripcion:     descIdx  >= 0 ? String(tr[descIdx]  || "") : "",
        cantidad:        cantIdx  >= 0 ? tr[cantIdx]               : "",
        precio_unitario: puIdx    >= 0 ? tr[puIdx]                 : "",
        monto:           montoIdx >= 0 ? tr[montoIdx]              : ""
      });
    });
  }

  var tickets = rows.map(function(row) {
    var resumen = {};
    headers.forEach(function(h, j) {
      var v = row[j];
      if (v instanceof Date) {
        if (v.getFullYear() <= 1900) {
          v = Utilities.formatDate(v, "America/Monterrey", "HH:mm");
        } else {
          v = Utilities.formatDate(v, "America/Monterrey", "yyyy-MM-dd");
        }
      }
      resumen[h] = (v === null || v === undefined) ? "" : v;
    });
    var tid = String(resumen.ticket_id || "");
    return { ticket_id: tid, resumen: resumen, productos: productsByTicket[tid] || [] };
  }).filter(function(t) { return t.ticket_id; });

  return { ok: true, tickets: tickets };
}

// ─── Dashboard: actualizar clasificación de ticket existente ─────────────────
function updateTicketClassification_(data) {
  var ticketId = String(data.ticket_id || "");
  var clasif   = data.clasificacion      || {};
  var prodsEd  = data.productos_editados || [];
  if (!ticketId) return { ok: false, error: "ticket_id requerido" };

  var ss = SpreadsheetApp.openById(SHEET_ID);

  var sheetR = ss.getSheetByName("Resumen tickets");
  if (sheetR && sheetR.getLastRow() > 1) {
    var lastColR = sheetR.getLastColumn();
    var headR    = sheetR.getRange(1, 1, 1, lastColR).getValues()[0];
    var rowsR    = sheetR.getRange(2, 1, sheetR.getLastRow() - 1, lastColR).getValues();
    var idColR   = headR.indexOf("ticket_id");
    var CAMPOS_R = [
      "fecha","cuenta","subcuenta","categoria_gasto","concepto",
      "propiedad","departamento","comprador","deducible","reembolso",
      "reembolso_a","metodo_pago","detalles_operacion","comentarios",
      "tienda","rfc","hora","folio","tarjeta_ultimos4",
      "subtotal","iva","ieps","descuentos","total","clasificado_por"
    ];
    for (var r = 0; r < rowsR.length; r++) {
      if (String(rowsR[r][idColR]) === ticketId) {
        CAMPOS_R.forEach(function(campo) {
          if (clasif.hasOwnProperty(campo)) {
            var col = headR.indexOf(campo);
            if (col >= 0) sheetR.getRange(r + 2, col + 1).setValue(clasif[campo]);
          }
        });
        break;
      }
    }
  }

  var sheetP = ss.getSheetByName("Transcripcion");
  if (sheetP && sheetP.getLastRow() > 1) {
    var lastColP = sheetP.getLastColumn();
    var headP    = sheetP.getRange(1, 1, 1, lastColP).getValues()[0];
    var rowsP    = sheetP.getRange(2, 1, sheetP.getLastRow() - 1, lastColP).getValues();
    var idColP   = headP.indexOf("ticket_id");
    var lineaCol = headP.indexOf("linea_numero");
    var CAMPOS_P = ["cuenta","subcuenta","categoria_gasto","concepto",
                    "propiedad","departamento","comprador","comentarios"];
    var editMap = {};
    prodsEd.forEach(function(pe) {
      editMap[String(pe.linea_numero)] = pe;
    });
    for (var p = 0; p < rowsP.length; p++) {
      if (String(rowsP[p][idColP]) === ticketId) {
        CAMPOS_P.forEach(function(campo) {
          if (clasif.hasOwnProperty(campo)) {
            var col = headP.indexOf(campo);
            if (col >= 0) sheetP.getRange(p + 2, col + 1).setValue(clasif[campo]);
          }
        });
        if (lineaCol >= 0) {
          var lineaNum = String(rowsP[p][lineaCol] || "");
          var pe = editMap[lineaNum];
          if (pe) {
            ["descripcion","cantidad","precio_unitario","monto"].forEach(function(campo) {
              if (pe.hasOwnProperty(campo)) {
                var col = headP.indexOf(campo);
                if (col >= 0) sheetP.getRange(p + 2, col + 1).setValue(pe[campo]);
              }
            });
          }
        }
      }
    }
  }

  return { ok: true };
}

// ─── Eliminar un ticket de Sheets ─────────────────────────────────────────────
function deleteTicket_(data) {
  var ticketId = String(data.ticket_id || "");
  if (!ticketId) return { ok: false, error: "ticket_id requerido" };

  var ss         = SpreadsheetApp.openById(SHEET_ID);
  var sheetNames = ["Transcripcion", "Resumen tickets", "Cruce bancario"];

  sheetNames.forEach(function(name) {
    var sheet = ss.getSheetByName(name);
    if (!sheet) return;
    var values = sheet.getDataRange().getValues();
    if (values.length < 2) return;
    var col = values[0].indexOf("ticket_id");
    if (col === -1) return;
    for (var r = values.length - 1; r >= 1; r--) {
      if (String(values[r][col]) === ticketId) {
        sheet.deleteRow(r + 1);
      }
    }
  });

  return { ok: true };
}

// ─── Registros contables: leer BANCOS + Presupuesto_sys ───────────────────────
function getBancosData_(ss) {
  const norm = (s) => (s ?? "").toString()
    .replace(/[​-‍﻿]/g, "")
    .replace(/ /g, " ")
    .trim()
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/\s+/g, " ")
    .toUpperCase();

  const pickSheet = (names) => {
    const sheets = ss.getSheets();
    const wanted = names.map(norm);
    for (const sh of sheets) {
      const n = norm(sh.getName());
      if (wanted.includes(n)) return sh;
    }
    for (const sh of sheets) {
      const n = norm(sh.getName());
      if (wanted.some(w => n.includes(w) || w.includes(n))) return sh;
    }
    return null;
  };

  const pickIdx = (headers, names) => {
    const H = headers.map(norm);
    for (const n of names) {
      const i = H.indexOf(norm(n));
      if (i >= 0) return i;
    }
    for (const n of names) {
      const key = norm(n);
      const j = H.findIndex(h => h.includes(key));
      if (j >= 0) return j;
    }
    return -1;
  };

  const toNumber = (v) => {
    if (typeof v === "number") return Number.isFinite(v) ? v : 0;
    const s = (v ?? "").toString().trim();
    if (!s) return 0;
    const n = Number(s.replace(/[^0-9\-\.]/g, ""));
    return Number.isFinite(n) ? n : 0;
  };

  const TZ = Session.getScriptTimeZone();
  const fmtDate = (v) => {
    if (!v && v !== 0) return "";
    if (v instanceof Date) {
      return Utilities.formatDate(v, TZ, "yyyy-MM-dd");
    }
    const s = String(v).trim();
    if (!s) return "";
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.substring(0, 10);
    const ddmm = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (ddmm) return `${ddmm[3]}-${ddmm[2].padStart(2,"0")}-${ddmm[1].padStart(2,"0")}`;
    if (s.includes("GMT") || /^\w{3}\s\w{3}\s\d/.test(s)) {
      try { return Utilities.formatDate(new Date(s), TZ, "yyyy-MM-dd"); } catch(e) {}
    }
    return s;
  };

  const MESES_MIN = ["","enero","febrero","marzo","abril","mayo","junio",
                     "julio","agosto","septiembre","octubre","noviembre","diciembre"];
  const fmtMes = (v) => {
    if (!v) return "";
    if (v instanceof Date) return `${MESES_MIN[v.getMonth() + 1]} ${v.getFullYear()}`;
    return String(v).trim();
  };

  const shB = pickSheet(["BANCOS"]);
  const shP = pickSheet(["PRESUPUESTO_SYS", "PRESUPUESTO SYS", "PRESUPUESTO", "PRESUPUESTOS"]);

  if (!shB || !shP) {
    return {
      ok: false,
      error: "sheet_not_found",
      message: "No se encontraron las hojas BANCOS o Presupuesto_sys"
    };
  }

  (function migrateAndEnsureCols(){
    const lastCol = shB.getLastColumn();
    if (lastCol < 1) return;
    let headers = shB.getRange(1, 1, 1, shB.getLastColumn()).getValues()[0]
      .map(h => String(h ?? "").trim().toUpperCase());

    if (!headers.includes("DUDA") && headers.includes("VALIDADO")) {
      const idx = headers.indexOf("VALIDADO");
      shB.getRange(1, idx + 1).setValue("DUDA");
      headers[idx] = "DUDA";
    }
    if (!headers.includes("VALIDADO") && headers.includes("REVISADO")) {
      const idx = headers.indexOf("REVISADO");
      shB.getRange(1, idx + 1).setValue("VALIDADO");
      headers[idx] = "VALIDADO";
    }

    const REQ = ["CUENTA","SUBCUENTA","CATEGORIA","CONCEPTO","PROPIEDAD","DEPARTAMENTO",
                 "ENCARGADO","DEDUCIBLE","REEMBOLSO","REEMBOLSO_A","METODO_PAGO",
                 "CLASIFICADO_POR","FECHA_CLASIF","DUDA","VALIDADO"];
    REQ.forEach(name => {
      if (!headers.includes(name)) {
        const newCol = shB.getLastColumn() + 1;
        shB.getRange(1, newCol).setValue(name);
        headers.push(name);
      }
    });
  })();

  const bancos        = shB.getDataRange().getValues();
  const bancosDisplay = shB.getDataRange().getDisplayValues();
  const pres          = shP.getDataRange().getValues();
  const hB = bancos[0] || [];
  const hP = pres[0]   || [];

  const iAno    = pickIdx(hB, ["AÑO", "ANO", "ANIO", "YEAR"]);
  const iMes    = pickIdx(hB, ["MES"]);
  const iDia    = pickIdx(hB, ["DÍA", "DIA", "DIA DE OPERACION", "FECHA"]);
  const iCta    = pickIdx(hB, ["CUENTA BANCARIA"]);
  const iCuenta = pickIdx(hB, ["CUENTA"]);
  const iSub    = pickIdx(hB, ["SUBCUENTA", "SUB-CUENTA"]);
  const iCat    = pickIdx(hB, ["CATEGORIA", "CATEGORÍA"]);
  const iDes    = pickIdx(hB, ["DESCRIPCION", "DESCRIPCIÓN"]);
  const iConRef = pickIdx(hB, ["CONCEPTO / REFERENCIA", "CONCEPTO/REFERENCIA",
                                "CONCEPTO REFERENCIA", "REFERENCIA", "CONCEPTO"]);
  const iCon    = pickIdx(hB, ["CONCEPTO"]);
  const iFac    = pickIdx(hB, ["FACTURA"]);
  const iMon    = pickIdx(hB, ["MONTO"]);
  const iDud    = pickIdx(hB, ["DUDA"]);
  const iDudN   = pickIdx(hB, ["DUDA_NOTA", "DUDA NOTA", "NOTA_DUDA"]);
  const iVal    = pickIdx(hB, ["VALIDADO"]);
  const iComB   = pickIdx(hB, ["COMENTARIOS", "COMENTARIO"]);
  const iCuentaA = pickIdx(hB, ["CUENTA_AUTO", "CUENTA AUTO"]);
  const iSubA    = pickIdx(hB, ["SUBCUENTA_AUTO", "SUBCUENTA AUTO"]);
  const iCatA    = pickIdx(hB, ["CATEGORIA_AUTO", "CATEGORÍA_AUTO", "CATEGORIA AUTO"]);
  const iConA    = pickIdx(hB, ["CONCEPTO_AUTO", "CONCEPTO AUTO"]);
  const iProb    = pickIdx(hB, ["PROBABILIDAD_CLASIF", "PROBABILIDAD CLASIF", "PROBABILIDAD"]);
  const iArgs    = pickIdx(hB, ["ARGUMENTOS_CLASIF", "ARGUMENTOS CLASIF", "ARGUMENTOS"]);
  // Match Banco↔Ticket (persistido por bn_set_ticket_matches_bulk). Sin leerlas,
  // el filtro "Relacionados con tickets" del frontend no detecta los 'Sí'.
  const iTrel   = pickIdx(hB, ["TICKET_RELACIONADO", "TICKET RELACIONADO"]);
  const iTms    = pickIdx(hB, ["TICKET_MATCH_SCORE", "TICKET MATCH SCORE"]);
  const iTmt    = pickIdx(hB, ["TICKET_MATCH_TIENDA", "TICKET MATCH TIENDA"]);
  const iTmf    = pickIdx(hB, ["TICKET_MATCH_FECHA", "TICKET MATCH FECHA"]);
  const iTmfo   = pickIdx(hB, ["TICKET_MATCH_FOLIO", "TICKET MATCH FOLIO"]);
  const iTmto   = pickIdx(hB, ["TICKET_MATCH_TOTAL", "TICKET MATCH TOTAL"]);

  const records = bancos.slice(1)
    .map((r, i) => ({ r, rowNum: i + 2 }))
    .filter(({ r }) => r.join("").toString().trim() !== "")
    .map(({ r, rowNum }) => {
      const factura = (iFac >= 0 ? r[iFac] : "") || "";
      const diaDisplay = iDia >= 0 ? String(bancosDisplay[rowNum - 1][iDia]).trim() : "";
      return {
        Año:               iAno    >= 0 ? String(r[iAno]).trim()    : "",
        Mes:               iMes    >= 0 ? fmtMes(r[iMes])           : "",
        Día:               diaDisplay,
        "Cuenta bancaria": iCta    >= 0 ? String(r[iCta]).trim()    : "",
        CUENTA:            iCuenta >= 0 ? String(r[iCuenta]).trim() : "",
        SUBCUENTA:         iSub    >= 0 ? String(r[iSub]).trim()    : "",
        CATEGORIA:         iCat    >= 0 ? String(r[iCat]).trim()    : "",
        Concepto:          iConRef >= 0 ? String(r[iConRef]).trim() : "",
        CONCEPTO:          iCon    >= 0 ? String(r[iCon]).trim()    : "",
        DESCRIPCION:       iDes    >= 0 ? String(r[iDes]).trim()    : "",
        Factura:           String(factura).trim(),
        FacturaFlag:       String(factura).trim().length ? "Con factura" : "Sin factura",
        Monto:             toNumber(iMon >= 0 ? r[iMon] : 0),
        DUDA:              iDud    >= 0 ? String(r[iDud]).trim()    : "",
        DUDA_NOTA:         iDudN   >= 0 ? String(r[iDudN]).trim()   : "",
        VALIDADO:          iVal    >= 0 ? String(r[iVal]).trim()    : "",
        COMENTARIOS:       iComB   >= 0 ? String(r[iComB]).trim()   : "",
        CUENTA_auto:       iCuentaA>= 0 ? String(r[iCuentaA]).trim(): "",
        SUBCUENTA_auto:    iSubA   >= 0 ? String(r[iSubA]).trim()   : "",
        CATEGORIA_auto:    iCatA   >= 0 ? String(r[iCatA]).trim()   : "",
        CONCEPTO_auto:     iConA   >= 0 ? String(r[iConA]).trim()   : "",
        Probabilidad_clasif: iProb >= 0 ? toNumber(r[iProb])        : 0,
        Argumentos_clasif: iArgs   >= 0 ? String(r[iArgs]).trim()   : "",
        Ticket_relacionado:  iTrel >= 0 ? String(r[iTrel]).trim()  : "",
        Ticket_match_score:  iTms  >= 0 ? toNumber(r[iTms])        : 0,
        Ticket_match_tienda: iTmt  >= 0 ? String(r[iTmt]).trim()   : "",
        Ticket_match_fecha:  iTmf  >= 0 ? String(r[iTmf]).trim()   : "",
        Ticket_match_folio:  iTmfo >= 0 ? String(r[iTmfo]).trim()  : "",
        Ticket_match_total:  iTmto >= 0 ? toNumber(r[iTmto])       : 0,
        rowNum:            rowNum
      };
    });

  const iCuentaP = pickIdx(hP, ["CUENTA"]);
  const iTipoP   = pickIdx(hP, ["TIPO"]);
  const iPerP    = pickIdx(hP, ["PERIODICIDAD"]);
  const iNatP    = pickIdx(hP, ["NATURALEZA"]);
  const iSubP    = pickIdx(hP, ["SUBCUENTA", "SUB-CUENTA"]);
  const iCatP    = pickIdx(hP, ["CATEGORIA", "CATEGORÍA"]);
  const iConP    = pickIdx(hP, ["CONCEPTO"]);
  const iDesP    = pickIdx(hP, ["DESCRIPCION", "DESCRIPCIÓN"]);
  const iConcP   = pickIdx(hP, ["CONCATENADO"]);
  const iSemP    = pickIdx(hP, ["SEMANAL"]);
  const iMenP    = pickIdx(hP, ["MENSUAL", "PRESUPUESTO MENSUAL"]);
  const iBimP    = pickIdx(hP, ["BIMESTRAL"]);
  const iAnuP    = pickIdx(hP, ["ANUAL", "PRESUPUESTO ANUAL"]);

  const budget = pres.slice(1)
    .filter(r => r.join("").toString().trim() !== "")
    .filter(r => {
      const cuenta = norm(iCuentaP >= 0 ? r[iCuentaP] : "");
      const cat    = norm(iCatP    >= 0 ? r[iCatP]    : "");
      const con    = norm(iConP    >= 0 ? r[iConP]    : "");
      return !(cuenta.startsWith("SUBTOTAL") || cat.startsWith("SUBTOTAL") || con.startsWith("SUBTOTAL"));
    })
    .map(r => ({
      CUENTA:       iCuentaP >= 0 ? String(r[iCuentaP]).trim() : "",
      TIPO:         iTipoP   >= 0 ? String(r[iTipoP]).trim()   : "",
      PERIODICIDAD: iPerP    >= 0 ? String(r[iPerP]).trim()    : "",
      NATURALEZA:   iNatP    >= 0 ? String(r[iNatP]).trim()    : "",
      SUBCUENTA:    iSubP    >= 0 ? String(r[iSubP]).trim()    : "",
      CATEGORIA:    iCatP    >= 0 ? String(r[iCatP]).trim()    : "",
      CONCEPTO:     iConP    >= 0 ? String(r[iConP]).trim()    : "",
      DESCRIPCION:  iDesP    >= 0 ? String(r[iDesP]).trim()    : "",
      CONCATENADO:  iConcP   >= 0 ? String(r[iConcP]).trim()   : "",
      SEMANAL:      toNumber(iSemP >= 0 ? r[iSemP] : 0),
      MENSUAL:      toNumber(iMenP >= 0 ? r[iMenP] : 0),
      BIMESTRAL:    toNumber(iBimP >= 0 ? r[iBimP] : 0),
      ANUAL:        toNumber(iAnuP >= 0 ? r[iAnuP] : 0)
    }))
    .filter(r => r.CUENTA);

  return {
    ok: true,
    spreadsheetId:  ss.getId(),
    spreadsheetUrl: ss.getUrl(),
    sourceSheets: { bancos: shB.getName(), presupuesto: shP.getName() },
    counts:       { records: records.length, budget: budget.length },
    records,
    budget
  };
}

// ─── Guardar clasificación de un registro bancario en hoja BANCOS ────────────
function saveBancoClasificacion_(ss, data) {
  const norm = (s) => (s ?? "").toString()
    .replace(/[​-‍﻿]/g, "")
    .replace(/ /g, " ")
    .trim()
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/\s+/g, " ")
    .toUpperCase();

  const shB = ss.getSheets().find(sh => norm(sh.getName()) === "BANCOS") ||
              ss.getSheets().find(sh => norm(sh.getName()).includes("BANCOS"));
  if (!shB) {
    return { ok: false, error: "sheet_not_found", message: "No se encontró la hoja BANCOS" };
  }

  const rowNum = Number(data.rowNum);
  if (!rowNum || rowNum < 2) {
    return { ok: false, error: "invalid_row", message: "rowNum inválido: " + data.rowNum };
  }

  const CLASIF_COLS = [
    "CUENTA", "SUBCUENTA", "CATEGORIA", "CONCEPTO",
    "PROPIEDAD", "DEPARTAMENTO", "ENCARGADO",
    "DEDUCIBLE", "REEMBOLSO", "REEMBOLSO_A",
    "METODO_PAGO", "CLASIFICADO_POR", "FECHA_CLASIF",
    "DUDA", "DUDA_NOTA", "VALIDADO", "COMENTARIOS"
  ];

  const lastCol   = shB.getLastColumn();
  const headerRow = shB.getRange(1, 1, 1, lastCol).getValues()[0];

  const getOrCreateCol = (colName) => {
    const normName = norm(colName);
    let idx = headerRow.findIndex(h => norm(h) === normName);
    if (idx >= 0) return idx + 1;
    const newCol = shB.getLastColumn() + 1;
    shB.getRange(1, newCol).setValue(colName);
    headerRow.push(colName);
    return newCol;
  };

  const colMap = {};
  for (const col of CLASIF_COLS) {
    colMap[col] = getOrCreateCol(col);
  }

  const c   = data.clasificacion || {};
  const now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");

  const writeCell = (colName, value) => {
    const col = colMap[colName];
    if (col) shB.getRange(rowNum, col).setValue(value ?? "");
  };

  if (data.descripcion_edit) {
    const descCol = getOrCreateCol("DESCRIPCION");
    if (descCol) shB.getRange(rowNum, descCol).setValue(data.descripcion || "");
  }

  if (data.duda_edit) {
    const dudCol = getOrCreateCol("DUDA");
    if (dudCol) shB.getRange(rowNum, dudCol).setValue(data.duda || "");
    if (data.duda_nota !== undefined) {
      const ndCol = getOrCreateCol("DUDA_NOTA");
      if (ndCol) shB.getRange(rowNum, ndCol).setValue(data.duda_nota || "");
    }
    return { ok: true, rowNum: rowNum, duda: data.duda, duda_nota: data.duda_nota || "" };
  }

  if (data.duda_nota_edit) {
    const ndCol = getOrCreateCol("DUDA_NOTA");
    if (ndCol) shB.getRange(rowNum, ndCol).setValue(data.duda_nota || "");
    return { ok: true, rowNum: rowNum, duda_nota: data.duda_nota || "" };
  }

  if (data.validado_edit) {
    const valCol = getOrCreateCol("VALIDADO");
    if (valCol) shB.getRange(rowNum, valCol).setValue(data.validado || "");
    return { ok: true, rowNum: rowNum, validado: data.validado };
  }

  if (data.fecha_edit) {
    const headerRow2 = shB.getRange(1, 1, 1, shB.getLastColumn()).getValues()[0];
    let diaCol = -1;
    for (let i = 0; i < headerRow2.length; i++) {
      const n = norm(headerRow2[i]);
      if (n === "DIA" || n === "DÍA" || n === "FECHA") { diaCol = i + 1; break; }
    }
    if (diaCol > 0) {
      const newDate = data.dia || (data.clasificacion && data.clasificacion.dia) || "";
      shB.getRange(rowNum, diaCol).setValue(newDate);
    }
    return { ok: true, rowNum: rowNum, dia_updated: true };
  }

  writeCell("CUENTA",          c.cuenta          || "");
  writeCell("SUBCUENTA",       c.subcuenta        || "");
  writeCell("CATEGORIA",       c.categoria_gasto  || "");
  writeCell("CONCEPTO",        c.concepto         || "");
  writeCell("PROPIEDAD",       c.propiedad        || "");
  writeCell("DEPARTAMENTO",    c.departamento     || "");
  writeCell("ENCARGADO",       c.encargado        || "");
  writeCell("DEDUCIBLE",       c.deducible        || "");
  writeCell("REEMBOLSO",       c.reembolso        || "");
  writeCell("REEMBOLSO_A",     c.reembolso_a      || "");
  writeCell("METODO_PAGO",     c.metodo_pago      || "");
  writeCell("CLASIFICADO_POR", c.clasificado_por  || "");
  writeCell("FECHA_CLASIF",    now);
  if (c.duda     !== undefined) writeCell("DUDA",     c.duda     || "");
  if (c.duda_nota !== undefined) writeCell("DUDA_NOTA", c.duda_nota || "");
  if (c.validado !== undefined) writeCell("VALIDADO", c.validado || "");
  if (c.comentarios !== undefined) writeCell("COMENTARIOS", c.comentarios || "");

  return { ok: true, rowNum: rowNum, columnsWritten: CLASIF_COLS.length };
}

// Persiste el resultado del match Banco↔Ticket en columnas de BANCOS.
// data.updates = [{ rowNum, ticket_relacionado, ticket_match_score, ticket_match_tienda, ticket_match_fecha, ticket_match_folio, ticket_match_total }]
function bnSetTicketMatchesBulk_(ss, data) {
  try {
    var payload = data && data.payload ? (typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload) : data;
    var updates = (payload && payload.updates) || [];
    if (!Array.isArray(updates) || !updates.length) return { ok: false, error: 'updates vacío' };
    var norm = function (s) { return String(s || '').trim().toUpperCase(); };
    var shB = ss.getSheets().find(function (sh) { return norm(sh.getName()) === 'BANCOS'; }) ||
              ss.getSheets().find(function (sh) { return norm(sh.getName()).indexOf('BANCOS') >= 0; });
    if (!shB) return { ok: false, error: 'BANCOS no encontrada' };
    var lastCol = shB.getLastColumn();
    var header  = shB.getRange(1, 1, 1, lastCol).getValues()[0];
    function getOrCreateCol(name) {
      var n = norm(name);
      for (var i = 0; i < header.length; i++) if (norm(header[i]) === n) return i + 1;
      var c = shB.getLastColumn() + 1;
      shB.getRange(1, c).setValue(name);
      header.push(name);
      return c;
    }
    var col = {
      rel:    getOrCreateCol('Ticket_relacionado'),
      score:  getOrCreateCol('Ticket_match_score'),
      tienda: getOrCreateCol('Ticket_match_tienda'),
      fecha:  getOrCreateCol('Ticket_match_fecha'),
      folio:  getOrCreateCol('Ticket_match_folio'),
      total:  getOrCreateCol('Ticket_match_total'),
    };
    var written = 0;
    for (var i = 0; i < updates.length; i++) {
      var u = updates[i];
      var rn = Number(u.rowNum);
      if (!rn || rn < 2) continue;
      shB.getRange(rn, col.rel).setValue(u.ticket_relacionado || '');
      if (u.ticket_relacionado === 'Sí') {
        shB.getRange(rn, col.score).setValue(u.ticket_match_score != null ? u.ticket_match_score : '');
        shB.getRange(rn, col.tienda).setValue(u.ticket_match_tienda || '');
        shB.getRange(rn, col.fecha).setValue(u.ticket_match_fecha || '');
        shB.getRange(rn, col.folio).setValue(u.ticket_match_folio || '');
        shB.getRange(rn, col.total).setValue(u.ticket_match_total != null ? u.ticket_match_total : '');
      } else {
        shB.getRange(rn, col.score).setValue('');
        shB.getRange(rn, col.tienda).setValue('');
        shB.getRange(rn, col.fecha).setValue('');
        shB.getRange(rn, col.folio).setValue('');
        shB.getRange(rn, col.total).setValue('');
      }
      written++;
    }
    return { ok: true, written: written };
  } catch (err) {
    return { ok: false, error: String(err && err.message || err) };
  }
}

// ─── Guardar Presupuesto_sys (reescribe la hoja con las filas dadas) ─────────
function savePresupuesto_(ss, data) {
  const norm = (s) => (s ?? "").toString().trim().normalize("NFD").replace(/[̀-ͯ]/g,"").toUpperCase();
  const sh = ss.getSheets().find(s => norm(s.getName()).includes("PRESUPUESTO"));
  if (!sh) return { ok: false, error: "sheet_not_found", message: "No se encontró la hoja Presupuesto_sys" };

  const columns = Array.isArray(data.columns) ? data.columns : [];
  const rows    = Array.isArray(data.rows)    ? data.rows    : [];
  if (!columns.length) return { ok: false, error: "no_columns" };

  const lastCol = Math.max(sh.getLastColumn(), columns.length);
  const headerRow = sh.getRange(1, 1, 1, lastCol).getValues()[0]
    .map(h => String(h ?? "").trim());

  const colIdx = {};
  columns.forEach(name => {
    let idx = headerRow.findIndex(h => h.toUpperCase() === String(name).toUpperCase());
    if (idx < 0) {
      const newCol = sh.getLastColumn() + 1;
      sh.getRange(1, newCol).setValue(name);
      headerRow.push(name);
      idx = newCol - 1;
    }
    colIdx[name] = idx;
  });

  const totalLastRow = sh.getLastRow();
  if (totalLastRow >= 2) {
    sh.getRange(2, 1, totalLastRow - 1, Math.max(1, sh.getLastColumn())).clearContent();
  }

  if (rows.length) {
    const width = sh.getLastColumn();
    const matrix = rows.map(r => {
      const out = new Array(width).fill("");
      for (const col of columns) {
        const v = r[col];
        out[colIdx[col]] = (v == null) ? "" : v;
      }
      return out;
    });
    sh.getRange(2, 1, matrix.length, width).setValues(matrix);
  }

  return { ok: true, rowsWritten: rows.length };
}
