const BACKEND = "https://ticket-vision-957627511957.northamerica-south1.run.app";

// ─── Catálogo de clasificaciones ───────────────────────────────────────────
// CATALOG[cuenta][subcuenta] = Object{categoria:[conceptos]} | Array[conceptos]

let CATALOG = {
  "Egresos": {
    "Recursos Humanos": {
      "Limpieza":        ["Brenda", "Alma", "Gaby", "Juanita", "Damariz"],
      "Mantenimiento":   ["Francisco"],
      "Administración":  ["Auxiliar administrativo", "Andres", "Claudia", "Papa"]
    },
    "Gastos Administrativos": {
      "Oficina":       ["Impresiones / Señalítica", "Papelería", "Equipo de cómputo"],
      "Otros gastos":  []
    },
    "Gastos Operativos": {
      "Servicios":     ["Lavandería", "Agua", "Luz", "Gas", "Internet", "Pipas de agua", "Fumigación"],
      "Insumos":       ["Limpieza", "Blancos"],
      "Mantenimiento": ["Ferreteria", "Aparatos electrónicos y focos", "Limpieza de colchones sillones", "Gas de minisplit", "Hidro", "Lavadora / secadora"],
      "Autos":         ["Gasolina", "Seguros", "Mantenimiento"],
      "Otros Gastos":  []
    },
    "Tecnología y automatización": {
      "Plataformas":   ["Lodgify", "Leadsales", "Amazon prime", "Netflix", "GoogleOne", "Breezeway", "Kommo", "Microsoft"],
      "Otros gastos":  []
    },
    "Impuestos y obligaciones": {
      "SAT":      [],
      "IMSS":     [],
      "Tenencia": [],
      "Predial":  []
    }
  },
  "Ingresos": {
    "BBVA ACR Empresarial": ["Trato directo", "Stripe", "Airbnb", "Contratos"],
    "BBVA ACL Libreton":    ["Trato directo", "Stripe", "Airbnb", "Contratos"],
    "Efectivo":             ["Contratos", "Trato directo"]
  },
  "Pasivos": {
    "Devolución de depósitos en garantía": [],
    "Depósitos en garantía":               [],
    "Pago de tarjetas de crédito":         []
  },
  "Activos": {
    "CAPEX / Inversión": {
      "Equipamiento":  ["General", "Depa nuevo", "Decoración", "Reemplazo de minisplit", "Hidro nuevo", "Lavadora / secadora nueva", "Colchón", "Mesa", "Silla", "Sillón", "Refrigerador nuevo", "Abanico", "Smart tv", "Boiler nuevo"],
      "Herramienta":   [],
      "Construcción":  ["Material", "Mano de obra", "Eléctrico", "Plomería", "Otros gastos", "Pintura", "Herrería", "Carpintería", "Yesería", "Puertas y ventanas"]
    },
    "Inversiones financieras": []
  },
  "Capital": {
    "Utilidad":          [],
    "Gastos Familiares": ["Viajes", "Gatos casa", "Gastos médicos", "Otros gastos"]
  }
};

const CUENTA_EMOJIS = {
  "Egresos":  "💸",
  "Ingresos": "💰",
  "Pasivos":  "📋",
  "Activos":  "📈",
  "Capital":  "💼"
};

const CONCEPTO_EMOJIS = {
  // Personas
  "Brenda":"👩","Alma":"👩","Gaby":"👩","Juanita":"👩","Damariz":"👩",
  "Francisco":"👨","Auxiliar administrativo":"👔","Andres":"👨","Claudia":"👩","Papa":"👨‍👧",
  // Oficina
  "Impresiones / Señalítica":"🖨️","Papelería":"📄","Equipo de cómputo":"💻",
  // Servicios
  "Lavandería":"👕","Agua":"💧","Luz":"💡","Gas":"🔥","Internet":"🌐",
  "Pipas de agua":"🚛","Fumigación":"🪲",
  // Insumos
  "Limpieza":"🧹","Blancos":"🛏️",
  // Mantenimiento
  "Ferreteria":"🔧","Aparatos electrónicos y focos":"💡","Limpieza de colchones sillones":"🛋️",
  "Gas de minisplit":"❄️","Hidro":"🛁","Lavadora / secadora":"🫧",
  // Autos
  "Gasolina":"⛽","Seguros":"🛡️","Mantenimiento":"🔧",
  // Plataformas
  "Lodgify":"🏨","Leadsales":"📱","Amazon prime":"📦","Netflix":"🎬",
  "GoogleOne":"☁️","Breezeway":"🏠","Kommo":"💬","Microsoft":"🪟",
  // Ingresos
  "Trato directo":"🤝","Stripe":"💳","Airbnb":"🏡","Contratos":"📋",
  // Capital
  "Viajes":"✈️","Gatos casa":"🏠","Gastos médicos":"🏥","Otros gastos":"📦",
  // Activos CAPEX
  "General":"📦","Depa nuevo":"🏗️","Decoración":"🖼️","Reemplazo de minisplit":"❄️",
  "Hidro nuevo":"🛁","Lavadora / secadora nueva":"🫧","Colchón":"🛏️",
  "Mesa":"🪑","Silla":"🪑","Sillón":"🛋️","Refrigerador nuevo":"🧊",
  "Abanico":"💨","Smart tv":"📺","Boiler nuevo":"🚿",
  // Construcción
  "Material":"🧱","Mano de obra":"👷","Eléctrico":"⚡","Plomería":"🔩",
  "Pintura":"🎨","Herrería":"⚒️","Carpintería":"🪵","Yesería":"🏛️",
  "Puertas y ventanas":"🚪","Otros Gastos":"📦","Otros gastos":"📦",
};

const SUBCUENTA_EMOJIS = {
  "Recursos Humanos":                     "👥",
  "Gastos Administrativos":               "📁",
  "Gastos Operativos":                    "🏢",
  "Tecnología y automatización":          "💻",
  "Impuestos y obligaciones":             "🧾",
  "BBVA ACR Empresarial":                 "🏦",
  "BBVA ACL Libreton":                    "🏦",
  "Efectivo":                             "💵",
  "Devolución de depósitos en garantía":  "↩️",
  "Depósitos en garantía":                "🔒",
  "Pago de tarjetas de crédito":          "💳",
  "CAPEX / Inversión":                    "🏗",
  "Inversiones financieras":              "📊",
  "Utilidad":                             "✨",
  "Gastos Familiares":                    "🏠"
};

// ─── Search index ─────────────────────────────────────────────────────────

function buildSearchIndex() {
  const index = [];
  for (const cuenta of Object.keys(CATALOG)) {
    for (const subcuenta of Object.keys(CATALOG[cuenta])) {
      const sub = CATALOG[cuenta][subcuenta];
      if (Array.isArray(sub)) {
        if (sub.length === 0) {
          index.push({ cuenta, subcuenta, categoria: "", concepto: "" });
        } else {
          for (const concepto of sub) {
            index.push({ cuenta, subcuenta, categoria: "", concepto });
          }
        }
      } else {
        for (const categoria of Object.keys(sub)) {
          const conceptos = sub[categoria];
          if (conceptos.length === 0) {
            index.push({ cuenta, subcuenta, categoria, concepto: "" });
          } else {
            for (const concepto of conceptos) {
              index.push({ cuenta, subcuenta, categoria, concepto });
            }
          }
        }
      }
    }
  }
  return index;
}

let SEARCH_INDEX = buildSearchIndex();
let searchMatches = {};

// ─── State ─────────────────────────────────────────────────────────────────

let selectedFiles     = [];
let ticketResults     = [];
let dashboardTickets  = [];   // todos los tickets cargados desde Sheets
let dashboardFiltered = [];   // subset filtrado
let dbFiltersOpen     = true;
let dbPageSize        = 50;   // registros por página (25 | 50 | 100)
let dbCurrentPage     = 1;    // página actual
const DB_IDX          = 20000; // offset de índice para paneles de clasificar del dashboard
let classifyAutoPopulating = false; // suprime markClassifyDirty durante auto-relleno
let currentUser       = "";   // usuario activo (acl | ccl | admin)

// ─── Contraseñas válidas ────────────────────────────────────────────────────
const VALID_PASSWORDS = { acl: "acl", ccl: "ccl", admin: "admin" };
let lightboxBlobUrl  = null;
let lightboxZoomed   = false;
const LB_ZOOM        = 2.4;
let lbPanX = 0, lbPanY = 0;
let lbDragging = false, lbHasDragged = false, lbLx = 0, lbLy = 0;
let fileHashes    = [];   // SHA-256 por índice de selectedFiles
let ticketsIndex  = null; // null = no cargado; [] = cargado (vacío o con datos)
const pdfThumbCache = new Map(); // File → blob URL (página 1 rasterizada)

// ─── PDF helpers ───────────────────────────────────────────────────────────

function isPdf(file) {
  return !!(file && (file.type === "application/pdf" || file.name.toLowerCase().endsWith(".pdf")));
}

/** Renderiza una sola página de un PDF a Blob JPEG */
async function renderPdfPageToBlob(file, pageNum = 1, scale = 1.5) {
  const data  = await file.arrayBuffer();
  const pdf   = await pdfjsLib.getDocument({ data }).promise;
  const page  = await pdf.getPage(Math.min(pageNum, pdf.numPages));
  const vp    = page.getViewport({ scale });
  const cvs   = document.createElement("canvas");
  cvs.width   = vp.width;
  cvs.height  = vp.height;
  await page.render({ canvasContext: cvs.getContext("2d"), viewport: vp }).promise;
  return new Promise(res => cvs.toBlob(res, "image/jpeg", 0.92));
}

/** Renderiza todas las páginas de un PDF en un único canvas vertical → Blob JPEG */
async function renderPdfAllPagesToBlob(file, scale = 2.0) {
  const data  = await file.arrayBuffer();
  const pdf   = await pdfjsLib.getDocument({ data }).promise;
  const pages = [];
  for (let p = 1; p <= pdf.numPages; p++) {
    const page = await pdf.getPage(p);
    const vp   = page.getViewport({ scale });
    const cvs  = document.createElement("canvas");
    cvs.width  = vp.width; cvs.height = vp.height;
    await page.render({ canvasContext: cvs.getContext("2d"), viewport: vp }).promise;
    pages.push(cvs);
  }
  if (pages.length === 1) return new Promise(res => pages[0].toBlob(res, "image/jpeg", 0.92));
  const gap    = 18;
  const maxW   = Math.max(...pages.map(c => c.width));
  const totalH = pages.reduce((h, c, i) => h + c.height + (i > 0 ? gap : 0), 0);
  const out    = document.createElement("canvas");
  out.width    = maxW; out.height = totalH;
  const ctx    = out.getContext("2d");
  ctx.fillStyle = "#d1d5db"; ctx.fillRect(0, 0, maxW, totalH);
  let y = 0;
  for (const c of pages) {
    ctx.drawImage(c, Math.floor((maxW - c.width) / 2), y);
    y += c.height + gap;
  }
  return new Promise(res => out.toBlob(res, "image/jpeg", 0.92));
}

/** Thumbnail de página 1 con caché por File object */
async function getPdfThumbUrl(file) {
  if (pdfThumbCache.has(file)) return pdfThumbCache.get(file);
  const blob = await renderPdfPageToBlob(file, 1, 0.6);
  const url  = URL.createObjectURL(blob);
  pdfThumbCache.set(file, url);
  return url;
}

/** Abre el lightbox con todas las páginas del PDF renderizadas */
async function openPdfLightbox(file) {
  showLoading("Cargando PDF…", "");
  try {
    const blob = await renderPdfAllPagesToBlob(file);
    hideLoading();
    openImageLightbox(URL.createObjectURL(blob), true);
  } catch { hideLoading(); }
}

// ─── Detección de duplicados ────────────────────────────────────────────────

async function sha256(file) {
  const buf  = await file.arrayBuffer();
  const hash = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, "0")).join("");
}

async function ensureTicketsIndex() {
  if (ticketsIndex !== null) return ticketsIndex;
  try {
    const res  = await fetch(`${BACKEND}/tickets-index`);
    const data = await res.json();
    ticketsIndex = data.ok ? (data.tickets || []) : [];
  } catch (_) {
    ticketsIndex = [];
  }
  return ticketsIndex;
}

function dupByHash(hash) {
  if (!hash || !ticketsIndex?.length) return null;
  return ticketsIndex.find(t => t.archivo_hash && t.archivo_hash === hash) || null;
}

function dupByFields(resumen) {
  if (!ticketsIndex?.length) return null;
  const tienda = (resumen.tienda || "").toLowerCase().trim();
  const fecha  =  resumen.fecha  || "";
  const total  = Math.round(Number(resumen.total  || 0) * 100);
  const folio  = (resumen.folio  || "").trim();
  return ticketsIndex.find(t => {
    const tTotal  = Math.round(Number(t.total  || 0) * 100);
    const tFolio  = (t.folio  || "").trim();
    const tFecha  =  t.fecha  || "";
    const tTienda = (t.tienda || "").toLowerCase().trim();
    if (folio && tFolio && folio === tFolio && fecha === tFecha) return true;
    if (fecha && fecha === tFecha && total > 0 && total === tTotal && tienda && tTienda)
      return tienda === tTienda || tienda.includes(tTienda) || tTienda.includes(tienda);
    return false;
  }) || null;
}

// Computa hashes desde startIdx y re-renderiza el strip cuando termina
async function computeHashes(startIdx) {
  await ensureTicketsIndex();
  for (let i = startIdx; i < selectedFiles.length; i++) {
    if (!fileHashes[i]) fileHashes[i] = await sha256(selectedFiles[i]);
  }
  renderImageStrip();
}

// ─── Init ──────────────────────────────────────────────────────────────────

// ─── Login ─────────────────────────────────────────────────────────────────

function tryLogin() {
  const pw  = (document.getElementById("loginPassword")?.value || "").trim();
  const err = document.getElementById("loginError");
  const user = VALID_PASSWORDS[pw];
  if (!user) {
    if (err) { err.textContent = "Contraseña incorrecta."; err.classList.remove("hidden"); }
    return;
  }
  currentUser = user;
  document.getElementById("loginOverlay")?.classList.add("hidden");
  document.getElementById("app-root")?.classList.remove("hidden");
  const _ub = document.getElementById("current-user-badge"); if (_ub) _ub.textContent = currentUser.toUpperCase();
  // Módulo predeterminado: Registros contables
  switchModule("registros");
}

function handleLoginKey(e) {
  if (e.key === "Enter") tryLogin();
}

// ─── Modo nocturno ─────────────────────────────────────────────────────
function toggleDarkMode() {
  document.body.classList.toggle('dark');
  const on = document.body.classList.contains('dark');
  try { localStorage.setItem('bn-dark', on ? '1' : '0'); } catch(_) {}
  const btn = document.getElementById('btn-dark-mode');
  if (btn) btn.textContent = on ? '☀️' : '🌙';
}
// Restaurar preferencia al cargar
try {
  if (localStorage.getItem('bn-dark') === '1') {
    document.addEventListener('DOMContentLoaded', () => {
      document.body.classList.add('dark');
      const btn = document.getElementById('btn-dark-mode');
      if (btn) btn.textContent = '☀️';
    });
  }
} catch(_) {}

window.addEventListener("DOMContentLoaded", () => {
  // ── LOGIN — contraseñas válidas: acl, ccl, admin (ver VALID_PASSWORDS) ──
  const DEV_MODE = false;
  if (DEV_MODE) {
    currentUser = "admin";
    document.getElementById("loginOverlay")?.classList.add("hidden");
    document.getElementById("app-root")?.classList.remove("hidden");
    const ub = document.getElementById("current-user-badge"); if (ub) ub.textContent = "ADMIN (dev)";
    // Cargar automáticamente el módulo Registros contables al entrar
    setTimeout(() => { try { switchModule("registros"); } catch(_) {} }, 0);
  } else {
    document.getElementById("loginOverlay")?.classList.remove("hidden");
    document.getElementById("app-root")?.classList.add("hidden");
    document.getElementById("loginPassword")?.focus();
  }

  // Configurar PDF.js worker
  if (typeof pdfjsLib !== "undefined") {
    pdfjsLib.GlobalWorkerOptions.workerSrc =
      "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
  }

  ensureTicketsIndex();

  // ── Cerrar lightbox con Escape ─────────────────────────────────────────
  document.addEventListener("keydown", e => {
    if (e.key === "Escape" && !document.getElementById("imageLightbox").classList.contains("hidden")) {
      closeLightbox();
    }
  });

  // ── Drag/pan en el lightbox ────────────────────────────────────────────
  const lbImg = document.getElementById("lightboxImg");

  // Desktop — mouse
  lbImg.addEventListener("mousedown", e => {
    if (!lightboxZoomed) return;
    e.preventDefault();
    lbDragging = true; lbHasDragged = false;
    lbLx = e.clientX; lbLy = e.clientY;
    lbImg.style.cursor = "grabbing"; lbImg.style.transition = "none";
  });
  document.addEventListener("mousemove", e => {
    if (!lbDragging) return;
    const dx = e.clientX - lbLx, dy = e.clientY - lbLy;
    if (Math.abs(dx) > 3 || Math.abs(dy) > 3) lbHasDragged = true;
    lbPanX += dx; lbPanY += dy;
    lbLx = e.clientX; lbLy = e.clientY;
    lbImg.style.transform = `translate(${lbPanX}px,${lbPanY}px) scale(${LB_ZOOM})`;
  });
  document.addEventListener("mouseup", () => {
    if (!lbDragging) return;
    lbDragging = false;
    if (lightboxZoomed) { lbImg.style.cursor = "grab"; lbImg.style.transition = "transform .22s ease"; }
  });

  // Móvil — touch (un dedo para arrastrar, toque corto para zoom)
  let ltx = 0, lty = 0;
  lbImg.addEventListener("touchstart", e => {
    if (!lightboxZoomed || e.touches.length !== 1) return;
    ltx = e.touches[0].clientX; lty = e.touches[0].clientY;
    lbHasDragged = false;
  }, { passive: true });
  lbImg.addEventListener("touchmove", e => {
    if (!lightboxZoomed || e.touches.length !== 1) return;
    e.preventDefault();
    const dx = e.touches[0].clientX - ltx, dy = e.touches[0].clientY - lty;
    if (Math.abs(dx) > 3 || Math.abs(dy) > 3) lbHasDragged = true;
    lbPanX += dx; lbPanY += dy;
    ltx = e.touches[0].clientX; lty = e.touches[0].clientY;
    lbImg.style.transform = `translate(${lbPanX}px,${lbPanY}px) scale(${LB_ZOOM})`;
  }, { passive: false });

  ["files-camera","files-gallery","files-files",
   "sheet-camera","sheet-gallery","sheet-files"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener("change", handleFilesAdded);
  });
  // Actualizar hint según dispositivo
  if (!isMobile()) {
    const hint = document.getElementById("uploadHint");
    if (hint) hint.textContent = "Haz clic para seleccionar imágenes";
  }
});

function isMobile() {
  return "ontouchstart" in window || navigator.maxTouchPoints > 0;
}

function openUploadSheet() {
  if (isMobile()) {
    document.getElementById("uploadSheetOverlay").classList.remove("hidden");
    requestAnimationFrame(() => document.getElementById("uploadSheet").classList.add("open"));
  } else {
    // Desktop: abrir selector de archivos directamente
    document.getElementById("files-files").click();
  }
}
function closeUploadSheet() {
  document.getElementById("uploadSheet").classList.remove("open");
  setTimeout(() => document.getElementById("uploadSheetOverlay").classList.add("hidden"), 280);
}

// ─── Accordion panels ──────────────────────────────────────────────────────

function openPanel(n) {
  document.getElementById(`panel-body-${n}`).classList.remove("collapsed");
  document.getElementById(`panel-arrow-${n}`).classList.add("open");
}
function closePanel(n) {
  document.getElementById(`panel-body-${n}`).classList.add("collapsed");
  document.getElementById(`panel-arrow-${n}`).classList.remove("open");
}
function togglePanel(n) {
  const body = document.getElementById(`panel-body-${n}`);
  if (body.classList.contains("collapsed")) openPanel(n); else closePanel(n);
}

// ─── File management ───────────────────────────────────────────────────────

function handleFilesAdded(e) {
  addFiles(Array.from(e.target.files));
  e.target.value = "";
}

function addFiles(newFiles) {
  if (!newFiles.length) return;
  const startIdx = selectedFiles.length;
  selectedFiles  = [...selectedFiles, ...newFiles];
  renderImageStrip();
  document.getElementById("analyzeWrap").classList.remove("hidden");
  setStep(1);
  computeHashes(startIdx); // detectar duplicados por hash en segundo plano
}

function removeFile(idx) {
  selectedFiles.splice(idx, 1);
  if (!selectedFiles.length) {
    document.getElementById("analyzeWrap").classList.add("hidden");
    document.getElementById("limpiarWrap").classList.add("hidden");
  }
  renderImageStrip();
}

function limpiarTickets() {
  ticketResults.forEach(t => { if (t.imageUrl) URL.revokeObjectURL(t.imageUrl); });
  pdfThumbCache.forEach(url => URL.revokeObjectURL(url));
  pdfThumbCache.clear();
  selectedFiles = [];
  ticketResults = [];
  fileHashes    = [];
  const strip = document.getElementById("imagePreview");
  strip.innerHTML = "";
  strip.classList.add("hidden");
  document.getElementById("analyzeWrap").classList.add("hidden");
  document.getElementById("limpiarWrap").classList.add("hidden");
  document.getElementById("analyzeStatus").textContent = "";
  document.getElementById("ticketsContainer").innerHTML = "";
  document.getElementById("panel-3").classList.add("hidden");
  const sub = document.getElementById("panel-2-subtitle");
  if (sub) sub.textContent = "Sin tickets analizados";
  openPanel(1);
  closePanel(2);
}

function renderImageStrip() {
  const strip = document.getElementById("imagePreview");
  if (!selectedFiles.length) { strip.classList.add("hidden"); return; }
  strip.classList.remove("hidden");
  document.getElementById("limpiarWrap").classList.remove("hidden");
  strip.innerHTML = "";

  selectedFiles.forEach((file, i) => {
    const dup   = fileHashes[i] ? dupByHash(fileHashes[i]) : null;
    const thumb = document.createElement("div");
    thumb.className = "image-thumb" + (dup ? " thumb-has-dup" : "");

    const img = document.createElement("img");
    if (isPdf(file)) {
      img.src   = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='80' height='80'%3E%3Crect width='80' height='80' rx='8' fill='%23fee2e2'/%3E%3Ctext x='40' y='42' text-anchor='middle' font-size='28' font-family='sans-serif'%3E%F0%9F%93%84%3C/text%3E%3C/svg%3E";
      img.style.objectFit = "contain"; img.style.background = "#fee2e2";
      getPdfThumbUrl(file).then(url => { img.src = url; img.style.objectFit = ""; img.style.background = ""; });
      img.onclick = (e) => { e.stopPropagation(); openPdfLightbox(selectedFiles[i]); };
    } else {
      const previewUrl = URL.createObjectURL(file);
      img.src = previewUrl;
      img.onload = () => URL.revokeObjectURL(img.src);
      img.onclick = (e) => {
        e.stopPropagation();
        const url = URL.createObjectURL(selectedFiles[i]);
        openImageLightbox(url, true);
      };
    }

    const lbl = document.createElement("div");
    lbl.className   = "thumb-label";
    lbl.textContent = String(i + 1);

    const btn = document.createElement("button");
    btn.className   = "thumb-remove";
    btn.textContent = "×";
    btn.onclick     = (e) => { e.stopPropagation(); removeFile(i); };

    thumb.append(img, lbl, btn);

    if (dup) {
      const badge = document.createElement("div");
      badge.className = "thumb-dup-badge";
      badge.title     = `Ya existe: ${dup.tienda || ""}${dup.fecha ? " · " + dup.fecha : ""}`;
      badge.textContent = "⚠️";
      thumb.appendChild(badge);
    }

    strip.appendChild(thumb);
  });

  // Botón "+" para agregar más imágenes (abre el sheet)
  const addBtn = document.createElement("button");
  addBtn.className = "image-thumb thumb-add";
  addBtn.type      = "button";
  addBtn.innerHTML = `<span class="thumb-add-icon">＋</span>`;
  addBtn.onclick   = openUploadSheet;
  strip.appendChild(addBtn);
}

// ─── Loading ───────────────────────────────────────────────────────────────

function showLoading(title = "Analizando...", sub = "Extrayendo productos y datos") {
  document.getElementById("loadingTitle").textContent    = title;
  document.getElementById("loadingSubtitle").textContent = sub;
  document.getElementById("loadingOverlay").classList.remove("hidden");
}
function hideLoading() {
  document.getElementById("loadingOverlay").classList.add("hidden");
}

// ─── Step indicator ────────────────────────────────────────────────────────

function setStep(n) {
  if (n === 2) {
    closePanel(1);
    openPanel(2);
    const count = ticketResults.length;
    const sub = document.getElementById("panel-2-subtitle");
    if (sub) sub.textContent = `${count} ticket${count !== 1 ? "s" : ""} analizados`;
  } else {
    openPanel(1);
  }
}

// ─── Analyze ───────────────────────────────────────────────────────────────

async function analyzeTickets() {
  if (!selectedFiles.length) return;
  try {
    setStatus("analyzeStatus", "");
    await ensureTicketsIndex();

    // ── Separar: analizar vs omitir (duplicado por hash) ──────────────────
    const toAnalyze = [];
    const toSkip    = [];
    selectedFiles.forEach((file, i) => {
      const hash = fileHashes[i] || "";
      const dup  = dupByHash(hash);
      if (dup) toSkip.push({ file, fileHash: hash, imageUrl: URL.createObjectURL(file), duplicate: dup });
      else     toAnalyze.push({ file, fileHash: hash, originalIndex: i });
    });

    let analyzedResults = [];

    if (toAnalyze.length) {
      showLoading("Analizando…", `Procesando ${toAnalyze.length} ticket${toAnalyze.length > 1 ? "s" : ""}…`);
      const form = new FormData();
      toAnalyze.forEach(item => form.append("files", item.file));

      const res  = await fetch(`${BACKEND}/process-json`, { method: "POST", body: form });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.error || "No se pudo procesar.");

      analyzedResults = (data.resumen || []).map((r, i) => {
        const item = toAnalyze[i];
        const hash = item?.fileHash || "";
        return {
          file:      item?.file,
          fileHash:  hash,
          imageUrl:  item?.file ? URL.createObjectURL(item.file) : null,
          resumen:   r,
          productos: (data.productos      || []).filter(p => p.ticket_id === r.ticket_id),
          cruce:     (data.cruce_bancario || [])[i] || {},
          duplicate: dupByHash(hash) || dupByFields(r),
          skipped:   false,
        };
      });
    }

    // ── Resultados omitidos (sin tokens) ───────────────────────────────────
    const skippedResults = toSkip.map(item => ({
      file:      item.file,
      fileHash:  item.fileHash,
      imageUrl:  item.imageUrl,
      resumen:   null,
      productos: [],
      cruce:     {},
      duplicate: item.duplicate,
      skipped:   true,
    }));

    // ── Rasterizar thumbnails de PDFs (imageUrl debe ser imagen, no PDF blob) ─
    for (const result of [...analyzedResults, ...skippedResults]) {
      if (result.file && isPdf(result.file)) {
        if (result.imageUrl) URL.revokeObjectURL(result.imageUrl);
        const blob = await renderPdfPageToBlob(result.file, 1, 1.5);
        result.imageUrl = URL.createObjectURL(blob);
      }
    }

    ticketResults = [...analyzedResults, ...skippedResults];

    renderTicketCards();
    setStep(2);

    // Subtítulo con desglose
    const nA = analyzedResults.length;
    const nS = skippedResults.length;
    const sub = document.getElementById("panel-2-subtitle");
    if (sub) sub.textContent =
      (nA ? `${nA} analizado${nA !== 1 ? "s" : ""}` : "") +
      (nA && nS ? " · " : "") +
      (nS ? `${nS} omitido${nS !== 1 ? "s" : ""} (duplicado${nS !== 1 ? "s" : ""})` : "");

    setStatus("analyzeStatus", "");
    setTimeout(() => document.getElementById("panel-2").scrollIntoView({ behavior: "smooth" }), 200);
  } catch (err) {
    setStatus("analyzeStatus", "Error: " + err.message);
  } finally {
    hideLoading();
  }
}

// ─── Render ticket cards ────────────────────────────────────────────────────

// Mapeo de método detectado por Claude → valor en los recuadros de Clasificar
const METODO_A_CLASIF = {
  VISA:            "Tarjeta crédito",
  MASTERCARD:      "Tarjeta crédito",
  AMEX:            "Tarjeta crédito",
  TARJETA_CREDITO: "Tarjeta crédito",
  TARJETA_BANCO:   "Tarjeta crédito",
  TARJETA_DEBITO:  "Tarjeta débito",
  EFECTIVO:        "Efectivo",
  TRANSFERENCIA:   "Transfer.",
  RETIRO_SIN_TARJETA: "Retiro sin tarjeta",
  QR:              "Transfer.",
};

function autoSelectMetodoPago(i, rawMethod) {
  if (!rawMethod) return;
  const key      = normalizePaymentKey(rawMethod);
  const clasifVal = METODO_A_CLASIF[key];
  if (!clasifVal) return;
  const grid = document.getElementById(`metodo-grid-${i}`);
  if (!grid) return;
  const card = Array.from(grid.querySelectorAll(".cuenta-card"))
    .find(c => c.dataset.value === clasifVal);
  if (!card) return;
  grid.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  card.classList.add("active");
  document.getElementById(`metodo-clasif-${i}`).value = clasifVal;
}

function renderTicketCards() {
  const container = document.getElementById("ticketsContainer");
  container.innerHTML = ticketResults.map((t, i) => createTicketCard(t, i)).join("");
  const hasReal = ticketResults.some(t => !t.skipped);
  document.getElementById("panel-3").classList.toggle("hidden", !hasReal);
  // Auto-seleccionar método de pago y deducible detectados
  ticketResults.forEach((t, i) => {
    if (!t.skipped) {
      if (t.resumen?.metodo_pago) autoSelectMetodoPago(i, t.resumen.metodo_pago);
      autoSelectDeducible(i, t.productos);
    }
  });
}

/** Auto-selecciona el toggle Deducible según deducible_sugerido de los productos */
function autoSelectDeducible(i, productos) {
  if (!productos?.length) return;
  const siCount = productos.filter(p => p.deducible_sugerido === "Sí").length;
  // Marcar deducible si la mayoría de productos lo sugiere
  if (siCount === 0 || siCount < Math.ceil(productos.length / 2)) return;
  const inner  = document.getElementById(`deducible-${i}`);
  if (inner && !inner.checked) { inner.checked = true; updateDeducibleLabel(i, true); }
}

/** Actualiza el texto de ruta de clasificación debajo del buscador */
function updateClasiPath(i) {
  const el = document.getElementById(`clasif-path-${i}`);
  if (!el) return;
  const cuenta    = document.getElementById(`cuenta-${i}`)?.value    || "";
  const subcuenta = document.getElementById(`subcuenta-${i}`)?.value || "";
  const categoria = document.getElementById(`categoria-${i}`)?.value || "";
  const concepto  = document.getElementById(`concepto-${i}`)?.value  || "";
  const parts = [cuenta, subcuenta, categoria, concepto].filter(Boolean);
  if (!parts.length) { el.textContent = ""; return; }
  const emoji = CUENTA_EMOJIS[cuenta] || "";
  el.textContent = (emoji ? emoji + " " : "") + parts.join(" › ");
}

/** Despliega / contrae la sección Cuenta→Concepto */
function toggleClasiDetail(i) {
  const section = document.getElementById(`cuenta-section-${i}`);
  const btn     = document.getElementById(`clasif-toggle-${i}`);
  if (!section) return;
  const isHidden = section.classList.toggle("hidden");
  if (btn) btn.classList.toggle("active", !isHidden);
}

function removeTicket(i) {
  if (ticketResults[i]?.imageUrl) URL.revokeObjectURL(ticketResults[i].imageUrl);
  ticketResults.splice(i, 1);
  renderTicketCards();
  // Actualizar subtítulo
  const nA = ticketResults.filter(t => !t.skipped).length;
  const nS = ticketResults.filter(t =>  t.skipped).length;
  const sub = document.getElementById("panel-2-subtitle");
  if (sub) {
    if (!ticketResults.length) { sub.textContent = "Sin tickets analizados"; return; }
    sub.textContent =
      (nA ? `${nA} analizado${nA !== 1 ? "s" : ""}` : "") +
      (nA && nS ? " · " : "") +
      (nS ? `${nS} omitido${nS !== 1 ? "s" : ""}` : "");
  }
}

const PAYMENT_CHIP = {
  VISA:            { emoji: "💳", color: "#1a56db", bg: "#e8f0fe" },
  MASTERCARD:      { emoji: "💳", color: "#c0392b", bg: "#fde8e8" },
  AMEX:            { emoji: "💳", color: "#1f7a4c", bg: "#d1fae5" },
  TARJETA_DEBITO:  { emoji: "🏦", color: "#6d28d9", bg: "#ede9fe" },
  TARJETA_CREDITO: { emoji: "💳", color: "#b45309", bg: "#e2e8f0" },
  TARJETA_BANCO:   { emoji: "🏦", color: "#334155", bg: "#e2e8f0" },
  EFECTIVO:        { emoji: "💵", color: "#065f46", bg: "#d1fae5" },
  TRANSFERENCIA:   { emoji: "🔄", color: "#0e7490", bg: "#cffafe" },
  RETIRO_SIN_TARJETA: { emoji: "🏧", color: "#b45309", bg: "#e2e8f0" },
  QR:              { emoji: "📱", color: "#7c3aed", bg: "#f3e8ff" },
};

const PAYMENT_LABEL = {
  VISA:            "Visa",
  MASTERCARD:      "Mastercard",
  AMEX:            "Amex",
  TARJETA_DEBITO:  "Tarjeta débito",
  TARJETA_CREDITO: "Tarjeta crédito",
  TARJETA_BANCO:   "Tarjeta banco",
  EFECTIVO:        "Efectivo",
  TRANSFERENCIA:   "Transfer.",
  RETIRO_SIN_TARJETA: "Retiro s/tarjeta",
  QR:              "Pago QR",
};

const PAYMENT_ALIASES = {
  TARJETA_DE_DEBITO:  "TARJETA_DEBITO",
  TARJETA_DE_CREDITO: "TARJETA_CREDITO",
  DEBITO:             "TARJETA_DEBITO",
  CREDITO:            "TARJETA_CREDITO",
  DEBIT:              "TARJETA_DEBITO",
  CREDIT:             "TARJETA_CREDITO",
  CASH:               "EFECTIVO",
};

function normalizePaymentKey(method) {
  if (!method) return "";
  const clean = method.toUpperCase()
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return PAYMENT_ALIASES[clean] || clean;
}

function paymentChip(method, last4) {
  if (!method) return "";
  const key   = normalizePaymentKey(method);
  const p     = PAYMENT_CHIP[key] || { emoji: "💳", color: "#374151", bg: "#f3f4f6" };
  const label = (PAYMENT_LABEL[key] || key.replace(/_/g, " ")) + (last4 ? " *" + last4 : "");
  return `<span class="info-chip" style="color:${p.color};background:${p.bg}">${p.emoji} ${label}</span>`;
}

function createTicketCard(ticket, i) {
  // ── Tarjeta simplificada para duplicados omitidos ──────────────────────
  if (ticket.skipped) {
    const dup = ticket.duplicate || {};
    const info = [dup.tienda, dup.fecha, dup.total ? "$" + Number(dup.total).toLocaleString("es-MX") : ""].filter(Boolean).join(" · ");
    return `
      <div class="ticket-card ticket-card--skipped" id="ticket-${i}">
        <div class="ticket-card-header" style="cursor:default">
          <button class="btn-x" title="Eliminar" onclick="removeTicket(${i})">×</button>
          ${ticket.imageUrl ? `<img class="skipped-thumb" src="${esc(ticket.imageUrl)}"
              onclick="openTicketImageLightbox(${i})" alt="Ticket ${i+1}">` : ""}
          <div class="ticket-info" style="flex:1">
            <div class="dup-banner" style="margin:0">
              ⚠️ Omitido — ya existe en Sheets · <strong>${esc(dup.tienda || "")}</strong>${dup.fecha ? " · " + esc(dup.fecha) : ""}${dup.total ? " · $" + Number(dup.total).toLocaleString("es-MX") : ""}
            </div>
          </div>
        </div>
      </div>`;
  }

  const r = ticket.resumen;
  const metaParts = [r.fecha || "", r.hora || ""].filter(Boolean);

  const rawSummary = (ticket.productos || [])
    .map(p => p.descripcion || "").filter(Boolean).join(", ");
  const productSummary = rawSummary.length > 95 ? rawSummary.slice(0, 92) + "…" : rawSummary;

  const deptOptions = Array.from({length: 14}, (_, j) => `<option>${j + 1}</option>`).join("");

  return `
    <div class="ticket-card" id="ticket-${i}">
      <div class="ticket-card-header" onclick="toggleTable(${i})" id="header-${i}">
        <button class="btn-x" title="Eliminar" onclick="event.stopPropagation(); removeTicket(${i})">×</button>
        <div class="ticket-info">
          <div class="header-chips">
            <span class="info-chip hidden" id="cuenta-chip-${i}"></span>
            <span class="info-chip hidden" id="encargado-chip-${i}"></span>
            <span class="info-chip hidden" id="propiedad-chip-${i}"></span>
            <span class="info-chip hidden" id="dept-chip-${i}"></span>
            <span class="info-chip chip-reembolso hidden" id="reembolso-chip-${i}"></span>
          </div>
          <div class="ticket-store-row">
            <span class="ticket-store">${esc(r.tienda || "Ticket " + (i + 1))}</span>
            ${ticket.imageUrl ? `<button class="btn-ver-ticket" onclick="event.stopPropagation(); openTicketImageLightbox(${i})">Ver ticket</button>` : ""}
          </div>
          <div class="ticket-meta" id="ticket-meta-${i}">
            ${esc(metaParts.join(" · "))}${metaParts.length ? " · " : ""}🧾 ${r.num_productos || 0} producto${(r.num_productos || 0) !== 1 ? "s" : ""}
          </div>
          ${productSummary ? `<div class="product-summary">${esc(productSummary)}</div>` : ""}
        </div>
        <div class="ticket-header-right">
          <span id="payment-chip-${i}">${paymentChip(r.metodo_pago, r.tarjeta_ultimos4)}</span>
          <div class="ticket-total-badge" id="total-badge-${i}">
            <span class="total-main">${money(r.total)}</span>
            ${r.iva ? `<span class="total-iva">IVA ${money(r.iva)}</span>` : ""}
          </div>
          <div class="header-deducible" onclick="event.stopPropagation()">
            <label class="toggle-switch toggle-switch--dark">
              <input type="checkbox" id="deducible-header-${i}"
                     onchange="syncDeducible(${i}, this.checked)">
              <span class="toggle-slider"></span>
            </label>
            <span class="fhl" id="fhl-${i}">No deducible</span>
          </div>
        </div>
      </div>

      ${ticket.duplicate ? `<div class="dup-banner">
        ⚠️ Posible duplicado — <strong>${esc(ticket.duplicate.tienda || "")}</strong>${ticket.duplicate.fecha ? " · " + esc(ticket.duplicate.fecha) : ""}${ticket.duplicate.total ? " · $" + Number(ticket.duplicate.total).toLocaleString("es-MX") : ""}
      </div>` : ""}

      <div class="ticket-table-wrap hidden" id="table-${i}" data-tidx="${i}" data-tmod="analysis">
        <div class="ticket-tabs">
          <button class="ticket-tab active" onclick="showTicketTab(${i},'transcripcion',this)">Transcripción</button>
          <button class="ticket-tab" onclick="showTicketTab(${i},'resumen',this)">Resumen</button>
          <button class="ticket-tab" onclick="showTicketTab(${i},'cruce',this)">Cruce bancario</button>
        </div>
        <div id="tab-transcripcion-${i}" class="ticket-tab-content">
          ${buildProductTable(ticket.productos)}
          <button class="btn-add-row" onclick="addAnalysisProductRow(${i})">＋ Agregar producto</button>
        </div>
        <div id="tab-resumen-${i}" class="ticket-tab-content hidden">${buildResumenTable(ticket.resumen)}</div>
        <div id="tab-cruce-${i}" class="ticket-tab-content hidden">${buildCruceTable(ticket.cruce)}</div>
        <div class="table-actions-bar hidden" id="tact-analysis-${i}">
          <button class="btn-clasificar-ticket" style="padding:10px 28px;font-size:13px" onclick="saveTableChanges(${i})">💾 Guardar cambios</button>
          <button class="btn-limpiar-ticket" onclick="resetTableChanges(${i})">Limpiar</button>
        </div>
      </div>

      <div class="classify-tab" id="btn-classify-${i}" onclick="toggleClassify(${i})">
        <span class="classify-tab-arrow">›</span>
        <span class="classify-tab-label">Clasificar</span>
      </div>

      ${buildClassifyPanel(i, r.fecha, deptOptions, "✓ Clasificar", `clasificarTicket(${i})`, `limpiarClasificacion(${i})`)}
    </div>
  `;
}


// ─── Panel de clasificación reutilizable ───────────────────────────────────

function buildClassifyPanel(idx, fecha, deptOpts, saveLabel, saveOnclick, limpiarOnclick, fechaLabel) {
  return `
    <div class="classify-panel hidden" id="classify-${idx}">

      <div class="clasif-fecha-row">
        <label class="clasif-fecha-label">📅 ${fechaLabel || 'Fecha del ticket'}</label>
        <input type="date" id="fecha-clasif-${idx}" class="clasif-fecha-input"
          value="${esc(fecha || '')}" onchange="markClassifyDirty('${idx}')">
      </div>

      <div class="classify-search-wrap">
        <button class="btn-clasif-toggle" type="button"
                onclick="toggleClasiDetail('${idx}')"
                id="clasif-toggle-${idx}"
                title="Cuenta / Subcuenta / Categoría / Concepto">≡</button>
        <input type="text" id="search-${idx}" class="classify-search"
               placeholder="🔍 Buscar por cuenta, subcuenta, categoría o concepto..."
               oninput="onClassifySearch('${idx}', this.value)"
               onblur="setTimeout(()=>hideSearchResults('${idx}'), 180)">
        <div class="search-results hidden" id="search-results-${idx}"></div>
      </div>
      <div class="clasif-path-text" id="clasif-path-${idx}"></div>

      <div class="clasif-cuenta-section hidden" id="cuenta-section-${idx}">
        <div class="cuenta-field">
          <label>Cuenta</label>
          <div class="cuenta-grid" id="cuenta-grid-${idx}">
            <div class="cuenta-card active" data-value="" onclick="selectCuenta(this,'${idx}')">
              <div class="cuenta-icon">🏠</div><div class="cuenta-label">Sin cuenta</div><div class="cuenta-sub">General</div>
            </div>
            <div class="cuenta-card" data-value="Egresos" onclick="selectCuenta(this,'${idx}')">
              <div class="cuenta-icon">💸</div><div class="cuenta-label">Egresos</div><div class="cuenta-sub">Gastos y pagos</div>
            </div>
            <div class="cuenta-card" data-value="Ingresos" onclick="selectCuenta(this,'${idx}')">
              <div class="cuenta-icon">💰</div><div class="cuenta-label">Ingresos</div><div class="cuenta-sub">Cobros y entradas</div>
            </div>
            <div class="cuenta-card" data-value="Pasivos" onclick="selectCuenta(this,'${idx}')">
              <div class="cuenta-icon">📋</div><div class="cuenta-label">Pasivos</div><div class="cuenta-sub">Obligaciones</div>
            </div>
            <div class="cuenta-card" data-value="Activos" onclick="selectCuenta(this,'${idx}')">
              <div class="cuenta-icon">📈</div><div class="cuenta-label">Activos</div><div class="cuenta-sub">Inversión / CAPEX</div>
            </div>
            <div class="cuenta-card" data-value="Capital" onclick="selectCuenta(this,'${idx}')">
              <div class="cuenta-icon">💼</div><div class="cuenta-label">Capital</div><div class="cuenta-sub">Utilidad / Familiar</div>
            </div>
          </div>
          <input type="hidden" id="cuenta-${idx}" value="">
        </div>
        <div class="cuenta-field hidden" id="subcuenta-field-${idx}">
          <label>Subcuenta</label>
          <div class="cuenta-grid cuenta-grid--sub" id="subcuenta-grid-${idx}"></div>
          <input type="hidden" id="subcuenta-${idx}" value="">
        </div>
        <div class="cuenta-field hidden" id="categoria-field-${idx}">
          <label>Categoría</label>
          <div class="cuenta-grid cuenta-grid--cat" id="categoria-grid-${idx}"></div>
          <input type="hidden" id="categoria-${idx}" value="">
        </div>
        <div class="cuenta-field hidden" id="concepto-field-${idx}">
          <label>Concepto</label>
          <div class="cuenta-grid" id="concepto-grid-${idx}"></div>
          <input type="hidden" id="concepto-${idx}" value="">
        </div>
      </div><!-- /clasif-cuenta-section -->

      <div class="cuenta-field">
        <label>Encargado de operación</label>
        <div class="cuenta-grid cuenta-grid--compact" id="comprador-grid-${idx}">
          <div class="cuenta-card" data-value="Andrés"   onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👨</div><div class="cuenta-label">Andrés</div></div>
          <div class="cuenta-card" data-value="Claudia"  onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👩</div><div class="cuenta-label">Claudia</div></div>
          <div class="cuenta-card" data-value="Papá"     onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👨‍👧</div><div class="cuenta-label">Papá</div></div>
          <div class="cuenta-card" data-value="Francisco" onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👨</div><div class="cuenta-label">Francisco</div></div>
          <div class="cuenta-card" data-value="Brenda"   onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👩</div><div class="cuenta-label">Brenda</div></div>
          <div class="cuenta-card" data-value="Alma"     onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👩</div><div class="cuenta-label">Alma</div></div>
          <div class="cuenta-card" data-value="Gaby"     onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👩</div><div class="cuenta-label">Gaby</div></div>
          <div class="cuenta-card" data-value="Juanita"  onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👩</div><div class="cuenta-label">Juanita</div></div>
          <div class="cuenta-card" data-value="Damariz"  onclick="selectComprador(this,'${idx}')"><div class="cuenta-icon">👩</div><div class="cuenta-label">Damariz</div></div>
        </div>
        <input type="hidden" id="comprador-${idx}" value="">
      </div>

      <div class="cuenta-field">
        <label>Propiedad</label>
        <select id="propiedad-${idx}" class="field-select" onchange="togglePropiedadOtro('${idx}', this.value); markClassifyDirty('${idx}')">
          <option value="">— Seleccionar —</option>
          <option>Calle Cumbres</option>
          <option>Calle Baja California</option>
          <option>Calle Oaxaca</option>
          <option>Calle José Cárdenas</option>
          <option>Calle Matamoros</option>
          <option value="Otro">Otro</option>
        </select>
        <div class="hidden" id="propiedad-otro-wrap-${idx}" style="margin-top:8px">
          <input type="text" id="propiedad-otro-${idx}" class="field-select"
                 placeholder="Especificar propiedad..." style="appearance:none"
                 oninput="markClassifyDirty('${idx}')">
        </div>
      </div>

      <div class="cuenta-field">
        <label># Departamento</label>
        <select id="departamento-${idx}" class="field-select" onchange="markClassifyDirty('${idx}')">
          <option value="">— Seleccionar —</option>
          ${Array.from({length:14},(_,j)=>`<option>${j+1}</option>`).join('')}
        </select>
      </div>

      <div class="cuenta-field" id="deducible-field-${idx}">
        <label>Deducible</label>
        <div class="toggle-row">
          <label class="toggle-switch">
            <input type="checkbox" id="deducible-${idx}" onchange="updateDeducibleLabel('${idx}', this.checked); markClassifyDirty('${idx}')">
            <span class="toggle-slider"></span>
          </label>
          <span class="toggle-label-text" id="deducible-label-${idx}">No</span>
        </div>
      </div>

      <div class="cuenta-field">
        <label>Reembolso</label>
        <div class="toggle-row">
          <label class="toggle-switch">
            <input type="checkbox" id="reembolso-${idx}" onchange="toggleReembolso('${idx}', this.checked); markClassifyDirty('${idx}')">
            <span class="toggle-slider"></span>
          </label>
          <span class="toggle-label-text" id="reembolso-label-${idx}">No</span>
        </div>
        <div class="hidden" id="reembolso-a-field-${idx}" style="margin-top:10px">
          <label style="font-size:12px;font-weight:700;color:#374151;display:block;margin-bottom:6px">Reembolsar a:</label>
          <select id="reembolso-a-${idx}" class="field-select" onchange="toggleReembolsoOtro('${idx}', this.value); markClassifyDirty('${idx}')">
            <option value="">— Seleccionar —</option>
            <option>Andrés</option><option>Claudia</option><option>Papá</option>
            <option>Francisco</option><option>Brenda</option><option>Alma</option>
            <option>Gaby</option><option>Juanita</option><option>Damariz</option>
            <option value="Otro">Otro</option>
          </select>
          <div class="hidden" id="reembolso-otro-wrap-${idx}" style="margin-top:8px">
            <input type="text" id="reembolso-otro-${idx}" class="field-select"
                   placeholder="Especificar persona..." style="appearance:none" oninput="markClassifyDirty('${idx}')">
          </div>
          <div style="margin-top:12px">
            <label style="font-size:12px;font-weight:700;color:#374151;display:block;margin-bottom:6px">Detalles de la operación</label>
            <textarea id="detalles-${idx}" class="classify-textarea" rows="3"
                      placeholder="Descripción libre de la operación..." oninput="markClassifyDirty('${idx}')"></textarea>
          </div>
        </div>
      </div>

      <div class="cuenta-field">
        <label>Método de pago</label>
        <div class="cuenta-grid cuenta-grid--compact" id="metodo-grid-${idx}">
          <div class="cuenta-card" data-value="Tarjeta crédito"        onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">💳</div><div class="cuenta-label">Crédito</div></div>
          <div class="cuenta-card" data-value="Tarjeta débito"         onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">🏦</div><div class="cuenta-label">Débito</div></div>
          <div class="cuenta-card" data-value="Efectivo"               onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">💵</div><div class="cuenta-label">Efectivo</div></div>
          <div class="cuenta-card" data-value="Transferencia"          onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">🔄</div><div class="cuenta-label">Transfer.</div></div>
          <div class="cuenta-card" data-value="Retiro sin tarjeta"     onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">🏧</div><div class="cuenta-label">Retiro s/tarjeta</div></div>
          <div class="cuenta-card" data-value="Cheque"                 onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">📝</div><div class="cuenta-label">Cheque</div></div>
          <div class="cuenta-card" data-value="QR"                     onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">📱</div><div class="cuenta-label">QR</div></div>
          <div class="cuenta-card" data-value="BBVA ACR Empresarial"   onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">🏦</div><div class="cuenta-label">ACR Empresarial</div></div>
          <div class="cuenta-card" data-value="BBVA ACR TC Platino"    onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">💳</div><div class="cuenta-label">ACR TC Platino</div></div>
          <div class="cuenta-card" data-value="BBVA ACL TC Platino"    onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">💳</div><div class="cuenta-label">ACL TC Platino</div></div>
          <div class="cuenta-card" data-value="BBVA ACL TC Azul"       onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">💳</div><div class="cuenta-label">ACL TC Azul</div></div>
          <div class="cuenta-card" data-value="BBVA ACL Libreton"      onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">📖</div><div class="cuenta-label">ACL Libreton</div></div>
          <div class="cuenta-card" data-value="BBVA JJLC Empresarial"  onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">🏦</div><div class="cuenta-label">JJLC Empresarial</div></div>
          <div class="cuenta-card" data-value="BBVA ACR Libreton"      onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">📖</div><div class="cuenta-label">ACR Libreton</div></div>
          <div class="cuenta-card" data-value="CCL"                    onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">💵</div><div class="cuenta-label">CCL</div></div>
          <div class="cuenta-card" data-value="Otro"                   onclick="selectMetodoPago(this,'${idx}')"><div class="cuenta-icon">✏️</div><div class="cuenta-label">Otro</div></div>
        </div>
        <input type="hidden" id="metodo-clasif-${idx}" value="">
        <div class="hidden" id="metodo-otro-wrap-${idx}" style="margin-top:8px">
          <input type="text" id="metodo-otro-${idx}" class="field-select"
                 placeholder="Especificar método..." style="appearance:none"
                 oninput="bn_metodoOtroInput('${idx}', this); markClassifyDirty('${idx}')">
        </div>
      </div>

      <div class="cuenta-field">
        <label>Comentarios</label>
        <textarea id="comentarios-${idx}" class="classify-textarea"
                  placeholder="Notas adicionales sobre este ticket..."
                  oninput="bn_onComentariosTextareaInput('${idx}', this); markClassifyDirty('${idx}')"></textarea>
      </div>

      <div class="classify-actions hidden" id="classify-actions-${idx}">
        <button class="btn-clasificar-ticket" onclick="${saveOnclick}">${saveLabel}</button>
      </div>
      <div class="classify-actions-always" id="classify-actions-always-${idx}" style="display:flex;justify-content:flex-end;margin-top:8px">
        <button class="btn-limpiar-ticket" onclick="${limpiarOnclick}">✕ Limpiar</button>
      </div>
    </div>`;
}

// ─── Product table ─────────────────────────────────────────────────────────

function buildProductTable(productos) {
  if (!productos?.length) {
    return `<div class="empty-state"><div class="empty-icon">🧾</div><p>Sin productos detectados</p></div>`;
  }
  const cols = [
    { key: "linea_numero",            label: "#",         ro: true },
    { key: "descripcion",             label: "Descripción" },
    { key: "cantidad",                label: "Cant." },
    { key: "precio_unitario",         label: "P.Unit." },
    { key: "monto",                   label: "Monto" },
    { key: "categoria_operativa",     label: "Categoría" },
    { key: "deducible_sugerido",      label: "Deducible" },
    { key: "confianza_clasificacion", label: "Confianza" },
  ];
  return `<table>
    <thead><tr>${cols.map(c => `<th>${c.label}</th>`).join("")}<th></th></tr></thead>
    <tbody>${productos.map((r, rowIdx) =>
      `<tr data-row="${rowIdx}">${cols.map(c =>
        c.ro
          ? `<td>${esc(String(r[c.key] ?? ""))}</td>`
          : `<td contenteditable="true" spellcheck="false" data-field="${c.key}" oninput="onTableCellInput(this)">${esc(String(r[c.key] ?? ""))}</td>`
      ).join("")}<td class="btn-del-cell"><button class="btn-del-row" onclick="deleteProductRow(this)" title="Eliminar fila">✕</button></td></tr>`
    ).join("")}</tbody>
  </table>`;
}

// Campos manejados por el panel Clasificar — NO editables en tabla
const CLASIF_FIELDS = new Set(["fecha","metodo_pago","cuenta","subcuenta",
  "categoria_gasto","concepto","propiedad","departamento","fecha_captura","clasificado_por"]);

function buildResumenTable(r) {
  if (!r) return `<div class="empty-state"><p>Sin datos de resumen</p></div>`;
  const rows = [
    ["Tienda",            "tienda",            r.tienda],
    ["RFC",               "rfc",               r.rfc],
    ["Fecha",             "fecha",             r.fecha],
    ["Hora",              "hora",              formatHora(r.hora)],
    ["Folio",             "folio",             r.folio],
    ["Método de pago",    "metodo_pago",       r.metodo_pago],
    ["Últimos 4 dígitos", "tarjeta_ultimos4",  r.tarjeta_ultimos4],
    ["# Productos",       "num_productos",     r.num_productos],
    ["Subtotal",          "subtotal",          r.subtotal  || null],
    ["IVA",               "iva",               r.iva       || null],
    ["IEPS",              "ieps",              r.ieps      || null],
    ["Descuentos",        "descuentos",        r.descuentos|| null],
    ["Total",             "total",             r.total     || null],
    ["Cuenta",            "cuenta",            r.cuenta],
    ["Subcuenta",         "subcuenta",         r.subcuenta],
    ["Categoría",         "categoria_gasto",   r.categoria_gasto],
    ["Concepto",          "concepto",          r.concepto],
    ["Propiedad",         "propiedad",         r.propiedad],
    ["Departamento",      "departamento",      r.departamento],
    ["Clasificado por",   "clasificado_por",   r.clasificado_por],
    ["Fecha captura",     "fecha_captura",     r.fecha_captura],
  ].filter(([,, v]) => v !== "" && v != null);
  return `<table>
    <thead><tr><th>Campo</th><th>Valor</th></tr></thead>
    <tbody>${rows.map(([label, field, val]) => {
      const ro = CLASIF_FIELDS.has(field);
      const td = ro
        ? `<td>${esc(String(val))}</td>`
        : `<td contenteditable="true" spellcheck="false" data-field="${field}" oninput="onTableCellInput(this)">${esc(String(val))}</td>`;
      return `<tr data-field="${field}"><td class="resumen-key">${label}</td>${td}</tr>`;
    }).join("")}</tbody>
  </table>`;
}

function buildCruceTable(c) {
  if (!c || !Object.keys(c).length) return `<div class="empty-state"><div class="empty-icon">🏦</div><p>Sin datos de cruce bancario</p></div>`;
  const CRUCE_RO = new Set(["fecha","metodo_pago","cuenta","subcuenta","propiedad","departamento"]);
  const rows = [
    ["Fecha",             "fecha",            c.fecha],
    ["Hora",              "hora",             formatHora(c.hora)],
    ["Comercio",          "comercio",         c.comercio],
    ["RFC",               "rfc",              c.rfc],
    ["Folio",             "folio",            c.folio],
    ["Método de pago",    "metodo_pago",      c.metodo_pago],
    ["Últimos 4 dígitos", "tarjeta_ultimos4", c.tarjeta_ultimos4],
    ["Monto cruce",       "monto_cruce",      c.monto_cruce  || null],
    ["Total ticket",      "total_ticket",     c.total_ticket || null],
    ["Cuenta",            "cuenta",           c.cuenta],
    ["Subcuenta",         "subcuenta",        c.subcuenta],
    ["Propiedad",         "propiedad",        c.propiedad],
    ["Departamento",      "departamento",     c.departamento],
  ].filter(([,, v]) => v !== "" && v != null);
  return `<table>
    <thead><tr><th>Campo</th><th>Valor</th></tr></thead>
    <tbody>${rows.map(([label, field, val]) => {
      const ro = CRUCE_RO.has(field);
      const td = ro
        ? `<td>${esc(String(val))}</td>`
        : `<td contenteditable="true" spellcheck="false" data-field="${field}" oninput="onTableCellInput(this)">${esc(String(val))}</td>`;
      return `<tr data-field="${field}"><td class="resumen-key">${label}</td>${td}</tr>`;
    }).join("")}</tbody>
  </table>`;
}

function buildDashboardCruceTable(ticket) {
  const r = ticket.resumen || {};
  const s = k => String(r[k] || "");
  const CRUCE_RO = new Set(["fecha","metodo_pago","cuenta","subcuenta","propiedad","departamento"]);
  const rows = [
    ["Fecha",             "fecha",            s("fecha")],
    ["Hora",              "hora",             formatHora(s("hora"))],
    ["Comercio",          "comercio",         s("tienda")],
    ["RFC",               "rfc",              s("rfc")],
    ["Folio",             "folio",            s("folio")],
    ["Método de pago",    "metodo_pago",      s("metodo_pago")],
    ["Últimos 4 dígitos", "tarjeta_ultimos4", s("tarjeta_ultimos4")],
    ["Monto cruce",       "monto_cruce",      r.total  || null],
    ["Total ticket",      "total_ticket",     r.total  || null],
    ["Cuenta",            "cuenta",           s("cuenta")],
    ["Subcuenta",         "subcuenta",        s("subcuenta")],
    ["Propiedad",         "propiedad",        s("propiedad")],
    ["Departamento",      "departamento",     s("departamento")],
  ].filter(([,, v]) => v !== "" && v != null);
  if (!rows.length) return `<div class="empty-state"><div class="empty-icon">🏦</div><p>Sin datos de cruce bancario</p></div>`;
  return `<table>
    <thead><tr><th>Campo</th><th>Valor</th></tr></thead>
    <tbody>${rows.map(([label, field, val]) => {
      const ro = CRUCE_RO.has(field);
      const td = ro
        ? `<td>${esc(String(val))}</td>`
        : `<td contenteditable="true" spellcheck="false" data-field="${field}" oninput="onTableCellInput(this)">${esc(String(val))}</td>`;
      return `<tr data-field="${field}"><td class="resumen-key">${label}</td>${td}</tr>`;
    }).join("")}</tbody>
  </table>`;
}

function showTicketTab(i, tab, btn) {
  ["transcripcion", "resumen", "cruce"].forEach(t => {
    document.getElementById(`tab-${t}-${i}`).classList.toggle("hidden", t !== tab);
  });
  btn.closest(".ticket-tabs").querySelectorAll(".ticket-tab").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
}

// ─── Toggle table / classify ────────────────────────────────────────────────

function toggleTable(i) {
  const wrap   = document.getElementById(`table-${i}`);
  const header = document.getElementById(`header-${i}`);
  const hidden = wrap.classList.toggle("hidden");
  header.classList.toggle("collapsed", hidden);
}

function toggleClassify(i) {
  const panel = document.getElementById(`classify-${i}`);
  const tab   = document.getElementById(`btn-classify-${i}`);
  const open  = panel.classList.toggle("hidden");
  tab.classList.toggle("open", !open);
}

const CUENTA_COLOR_CLASS = {
  "Ingresos": "ci-ingresos", "Ingreso":  "ci-ingresos",
  "Egresos":  "ci-egresos",  "Egreso":   "ci-egresos",
  "Capital":  "ci-capital",
  "Activos":  "ci-activos",  "Activo":   "ci-activos",
  "Pasivos":  "ci-pasivos",  "Pasivo":   "ci-pasivos",
};

const ALL_CI = ["ci-ingresos","ci-egresos","ci-capital","ci-activos","ci-pasivos","ci-sincuenta"];


async function guardarResultados() {
  if (!ticketResults.length) return;

  const btn        = document.getElementById("btnGuardar");
  const statusEl   = document.getElementById("saveStatus");
  const subtitleEl = document.getElementById("saveSubtitle");

  try {
    if (btn) btn.style.pointerEvents = "none";
    if (statusEl) { statusEl.textContent = ""; statusEl.classList.add("hidden"); }

    showLoading("Guardando tickets…", "Subiendo imágenes y guardando en Sheets…");

    const productos = [];
    const resumen   = [];
    const cruce     = [];
    const metadata  = [];

    ticketResults.forEach((t, i) => {
      if (t.skipped) return; // omitir duplicados detectados por hash

      const c     = getClassify(i);
      const clasif = {
        cuenta:             c.cuenta,
        subcuenta:          c.subcuenta,
        categoria_gasto:    c.categoria,
        concepto:           c.concepto,
        propiedad:          c.propiedad,
        departamento:       c.departamento,
        comprador:          c.comprador,
        deducible:          c.deducible   ? "Sí" : "No",
        reembolso:          c.reembolso   ? "Sí" : "No",
        reembolso_a:        c.reembolso_a,
        metodo_pago_clasif: c.metodo_pago_clasif,
        detalles_operacion: c.detalles_operacion,
        comentarios:        c.comentarios,
      };

      const fechaFinal = c.fecha || t.resumen.fecha || "";
      metadata.push({ ticket_id: t.resumen.ticket_id, fecha: fechaFinal, tienda: t.resumen.tienda });

      // Leer estado actual de las tablas (ediciones, nuevos y eliminados)
      const resEdits   = readResumenEditsFromDOM(`#tab-resumen-${i}`);
      const prodsFinal = readAllProductsFromDOM(`#tab-transcripcion-${i}`, t.productos, t.resumen.ticket_id);

      prodsFinal.forEach(p => {
        productos.push({
          ticket_id:               p.ticket_id,
          tienda:                  resEdits.tienda ?? t.resumen.tienda,
          fecha:                   fechaFinal,
          linea_numero:            p.linea_numero,
          descripcion:             p.descripcion,
          cantidad:                p.cantidad,
          precio_unitario:         p.precio_unitario,
          monto:                   p.monto,
          categoria_operativa:     p.categoria_operativa,
          deducible_sugerido:      p.deducible_sugerido,
          confianza_clasificacion: p.confianza_clasificacion,
          ...clasif,
        });
      });

      resumen.push({
        ticket_id:        t.resumen.ticket_id,
        archivo:          t.resumen.archivo,
        tienda:           resEdits.tienda           ?? t.resumen.tienda,
        rfc:              resEdits.rfc              ?? t.resumen.rfc,
        fecha:            fechaFinal,
        hora:             resEdits.hora             ?? t.resumen.hora,
        folio:            resEdits.folio            ?? t.resumen.folio,
        metodo_pago:      t.resumen.metodo_pago,
        tarjeta_ultimos4: resEdits.tarjeta_ultimos4 ?? t.resumen.tarjeta_ultimos4,
        num_productos:    t.resumen.num_productos,
        subtotal:         numEdit(resEdits.subtotal,  t.resumen.subtotal),
        iva:              numEdit(resEdits.iva,        t.resumen.iva),
        ieps:             numEdit(resEdits.ieps,       t.resumen.ieps),
        descuentos:       numEdit(resEdits.descuentos, t.resumen.descuentos),
        total:            numEdit(resEdits.total,      t.resumen.total),
        ...clasif,
        clasificado_por:  currentUser,
        fecha_captura:    t.resumen.fecha_captura || new Date().toISOString(),
        imagen_nombre:    "",
        imagen_url:       "",
        archivo_hash:     t.fileHash || "",
      });

      cruce.push({
        fecha:            fechaFinal,
        hora:             t.cruce.hora,
        comercio:         t.cruce.comercio,
        rfc:              t.cruce.rfc,
        folio:            t.cruce.folio,
        metodo_pago:      t.cruce.metodo_pago,
        tarjeta_ultimos4: t.cruce.tarjeta_ultimos4,
        monto_cruce:      t.cruce.monto_cruce,
        total_ticket:     t.cruce.total_ticket,
        cuenta:           c.cuenta,
        subcuenta:        c.subcuenta,
        categoria_gasto:  c.categoria,
        concepto:         c.concepto,
        propiedad:        c.propiedad,
        departamento:     c.departamento,
      });
    });

    // Una sola llamada al backend — él sube imágenes a Drive y guarda en Sheets
    const form = new FormData();
    form.append("metadata",  JSON.stringify(metadata));
    form.append("productos", JSON.stringify(productos));
    form.append("resumen",   JSON.stringify(resumen));
    form.append("cruce",     JSON.stringify(cruce));
    ticketResults.forEach(t => { if (t.file) form.append("files", t.file); });

    const saveRes  = await fetch(`${BACKEND}/save-tickets`, { method: "POST", body: form });
    const saveData = await saveRes.json();
    if (!saveRes.ok || !saveData.ok) throw new Error(saveData.error || "Error al guardar");

    const dupCount = ticketResults.filter(t => t.duplicate).length;
    const dupNote  = dupCount ? ` · ⚠️ ${dupCount} posible${dupCount > 1 ? "s" : ""} duplicado${dupCount > 1 ? "s" : ""}` : "";
    const msg = `✅ ${saveData.tickets_saved} ticket${saveData.tickets_saved !== 1 ? "s" : ""} guardados · ${saveData.images_uploaded} imagen${saveData.images_uploaded !== 1 ? "es" : ""} subida${saveData.images_uploaded !== 1 ? "s" : ""} a Drive${dupNote}`;
    if (statusEl)   { statusEl.textContent = msg; statusEl.classList.remove("hidden"); statusEl.className = "save-status save-ok"; }
    if (subtitleEl) subtitleEl.textContent = "Enviado correctamente";
  } catch (err) {
    const msg = "❌ Error: " + err.message;
    if (statusEl) { statusEl.textContent = msg; statusEl.classList.remove("hidden"); statusEl.className = "save-status save-err"; }
  } finally {
    hideLoading();
    if (btn) btn.style.pointerEvents = "";
  }
}

/** Abre el lightbox para un ticket: imágenes directo, PDFs con todas las páginas */
async function openTicketImageLightbox(i) {
  const ticket = ticketResults[i];
  if (!ticket) return;
  if (ticket.file && isPdf(ticket.file)) {
    await openPdfLightbox(ticket.file);
  } else {
    openImageLightbox(ticket.imageUrl);
  }
}

function openImageLightbox(url, isTemporaryBlob = false) {
  if (!url) return;
  if (lightboxBlobUrl) { URL.revokeObjectURL(lightboxBlobUrl); lightboxBlobUrl = null; }
  if (isTemporaryBlob) lightboxBlobUrl = url;
  document.getElementById("lightboxImg").src = url;
  document.getElementById("imageLightbox").classList.remove("hidden");
  document.body.style.overflow = "hidden";
}

function lbApplyTransform() {
  const img = document.getElementById("lightboxImg");
  if (lightboxZoomed) {
    img.style.transform = `translate(${lbPanX}px,${lbPanY}px) scale(${LB_ZOOM})`;
    img.style.cursor    = "grab";
    img.style.transition = lbDragging ? "none" : "transform .22s ease";
  } else {
    img.style.transform = "";
    img.style.cursor    = "zoom-in";
    img.style.transition = "transform .22s ease";
  }
}

function toggleLightboxZoom(e) {
  e.stopPropagation();
  if (lbHasDragged) return; // fue un arrastre, no un clic
  lightboxZoomed = !lightboxZoomed;
  if (lightboxZoomed) { lbPanX = 0; lbPanY = 0; } // centrar al entrar
  lbApplyTransform();
}

function closeLightbox() {
  lightboxZoomed = false; lbDragging = false; lbHasDragged = false;
  lbPanX = 0; lbPanY = 0;
  const img = document.getElementById("lightboxImg");
  img.style.transform = ""; img.style.cursor = ""; img.style.transition = "";
  document.getElementById("imageLightbox").classList.add("hidden");
  img.src = "";
  if (lightboxBlobUrl) { URL.revokeObjectURL(lightboxBlobUrl); lightboxBlobUrl = null; }
  document.body.style.overflow = "";
}

function clasificarTicket(i) {
  markAsClassified(i);
  hideClassifyActions(i);
  // Cerrar el panel después de clasificar
  document.getElementById(`classify-${i}`).classList.add("hidden");
  document.getElementById(`btn-classify-${i}`).classList.remove("open");
}

function limpiarClasificacion(i) {
  // Buscador
  const search = document.getElementById(`search-${i}`);
  if (search) search.value = "";
  hideSearchResults(i);

  // Cuenta → volver a "Sin cuenta"
  const cuentaGrid = document.getElementById(`cuenta-grid-${i}`);
  if (cuentaGrid) {
    const cards = cuentaGrid.querySelectorAll(".cuenta-card");
    cards.forEach(c => c.classList.remove("active"));
    const sinCuenta = Array.from(cards).find(c => c.dataset.value === "");
    if (sinCuenta) sinCuenta.classList.add("active");
    document.getElementById(`cuenta-${i}`).value = "";
  }
  resetSubcuenta(i);

  // Ruta de clasificación
  updateClasiPath(i);

  // Propiedad
  const prop = document.getElementById(`propiedad-${i}`);
  if (prop) { prop.value = ""; togglePropiedadOtro(i, ""); }

  // Departamento
  const dept = document.getElementById(`departamento-${i}`);
  if (dept) dept.value = "";

  // Encargado de operación
  document.getElementById(`comprador-grid-${i}`)
    ?.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  const comprador = document.getElementById(`comprador-${i}`);
  if (comprador) comprador.value = "";

  // Deducible
  const ded = document.getElementById(`deducible-${i}`);
  if (ded && ded.checked) { ded.checked = false; updateDeducibleLabel(i, false); }

  // Reembolso
  const reem = document.getElementById(`reembolso-${i}`);
  if (reem && reem.checked) { reem.checked = false; toggleReembolso(i, false); }

  // Método de pago
  document.getElementById(`metodo-grid-${i}`)
    ?.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  const metodo = document.getElementById(`metodo-clasif-${i}`);
  if (metodo) metodo.value = "";

  // Campos de texto libres
  ["detalles", "comentarios"].forEach(id => {
    const el = document.getElementById(`${id}-${i}`);
    if (el) el.value = "";
  });

  // Fecha — restaurar a la detectada automáticamente
  const fechaInput = document.getElementById(`fecha-clasif-${i}`);
  if (fechaInput) fechaInput.value = ticketResults[i]?.resumen?.fecha || "";
  hideClassifyActions(i);
}

function markAsClassified(i) {
  const c        = getClassify(i);
  const header   = document.getElementById(`header-${i}`);
  const tab      = document.getElementById(`btn-classify-${i}`);
  const colorCls = CUENTA_COLOR_CLASS[c.cuenta] || "ci-sincuenta";

  // Header color
  ALL_CI.forEach(cls => header.classList.remove(cls));
  header.classList.add("classified", colorCls);

  // Tab color (same clase, CSS defines darker shade)
  ALL_CI.forEach(cls => tab.classList.remove(cls));
  tab.classList.add("classified", colorCls);

  // Badge color — same cuenta color as tab
  const badge = document.getElementById(`total-badge-${i}`);
  ALL_CI.forEach(cls => badge.classList.remove(cls));
  badge.classList.add("classified", colorCls);

  // 1. Cuenta chip
  const cuentaChipEl = document.getElementById(`cuenta-chip-${i}`);
  if (c.cuenta) {
    cuentaChipEl.textContent = (CUENTA_EMOJIS[c.cuenta] || "") + " " + c.cuenta;
    ALL_CI.forEach(cls => cuentaChipEl.classList.remove(cls));
    cuentaChipEl.classList.remove("hidden");
    cuentaChipEl.classList.add(colorCls);
  } else {
    cuentaChipEl.classList.add("hidden");
  }

  // 3. Encargado de operación chip
  const encargadoEl = document.getElementById(`encargado-chip-${i}`);
  if (c.comprador) {
    encargadoEl.textContent = "👤 " + c.comprador;
    encargadoEl.classList.remove("hidden");
  } else {
    encargadoEl.classList.add("hidden");
  }

  // 4. Propiedad chip
  const propiedadEl = document.getElementById(`propiedad-chip-${i}`);
  if (c.propiedad) {
    propiedadEl.textContent = "🏠 " + c.propiedad;
    propiedadEl.classList.remove("hidden");
  } else {
    propiedadEl.classList.add("hidden");
  }

  // 5. Departamento chip
  const deptEl = document.getElementById(`dept-chip-${i}`);
  if (c.departamento) {
    deptEl.textContent = "🚪 Depto. " + c.departamento;
    deptEl.classList.remove("hidden");
  } else {
    deptEl.classList.add("hidden");
  }

  // 6. Reembolso chip
  const reembolsoChipEl = document.getElementById(`reembolso-chip-${i}`);
  if (reembolsoChipEl) {
    if (c.reembolso && c.reembolso_a) {
      reembolsoChipEl.textContent = "↩ Reembolso a " + c.reembolso_a;
      reembolsoChipEl.classList.remove("hidden");
    } else {
      reembolsoChipEl.classList.add("hidden");
    }
  }

  // Path in tab label
  const parts    = [c.cuenta, c.subcuenta, c.categoria, c.concepto].filter(Boolean);
  const pathText = parts.join(" › ") || "Sin clasificar";
  const labelEl  = tab.querySelector(".classify-tab-label");
  labelEl.textContent = pathText;
  labelEl.style.cssText = "font-size:10px;text-transform:none;letter-spacing:.01em;font-weight:700;color:#fff;";

  // ── Sincronizar ticketResults en memoria ──────────────────────────────────
  const ticket = ticketResults[i];
  if (ticket) {
    // Resumen
    const r = ticket.resumen || {};
    if (c.fecha)            r.fecha         = c.fecha;
    if (c.metodo_pago_clasif) r.metodo_pago = c.metodo_pago_clasif;
    r.cuenta          = c.cuenta;
    r.subcuenta       = c.subcuenta;
    r.categoria_gasto = c.categoria;
    r.concepto        = c.concepto;
    r.propiedad       = c.propiedad;
    r.departamento    = c.departamento;
    // Actualizar fecha visible en el encabezado
    const metaEl = document.getElementById(`ticket-meta-${i}`);
    if (metaEl && c.fecha) {
      const parts = [c.fecha, r.hora || ""].filter(Boolean);
      metaEl.textContent = parts.join(" · ") + (parts.length ? " · " : "") +
        "🧾 " + (r.num_productos || 0) + " producto" + ((r.num_productos || 0) !== 1 ? "s" : "");
    }

    // Cruce bancario
    const cr = ticket.cruce || {};
    if (c.metodo_pago_clasif) cr.metodo_pago = c.metodo_pago_clasif;
    cr.cuenta       = c.cuenta;
    cr.subcuenta    = c.subcuenta;
    cr.propiedad    = c.propiedad;
    cr.departamento = c.departamento;

    // Rebuild tablas
    const resumenTab = document.getElementById(`tab-resumen-${i}`);
    if (resumenTab) resumenTab.innerHTML = buildResumenTable(r);
    const cruceTab = document.getElementById(`tab-cruce-${i}`);
    if (cruceTab) cruceTab.innerHTML = buildCruceTable(cr);

    // Actualizar chip de método de pago en encabezado
    const chipWrap = document.getElementById(`payment-chip-${i}`);
    if (chipWrap) {
      chipWrap.innerHTML = paymentChip(r.metodo_pago, r.tarjeta_ultimos4);
    }
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  MÓDULO: Registros contables  (Bancos + Presupuesto_sys)
// ══════════════════════════════════════════════════════════════════════════════

// ─── Estado global ───────────────────────────────────────────────────────────
let BN_RAW    = [];
let BN_BUDGET = [];
let BN_TIPO   = 'PC';  // Default: Por clasificar → Todos (sub-tipos: PC_I/PC_E/PC_AC/PC_PA/PC_CA)
let BN_LOADED = false;
// Estado de filtros (multi-select). Cada campo es un array; vacío = "Todos".
const bn_st   = {
  cuentaClasif: [], subcuenta: [], categoria: [], concepto: [],
  cuentaBan:    [], dia: [],
  propiedad:    [], departamento: [],
  deducible:    [], duda: [], validado: [], factura: [], metodoPago: [],
  reembolso:    [], encargado: [],
  anio:         [], mes: [], deducible: [],
  // Texto libre (no multi-select)
  q: '',
};

// Configuración de los multi-selects del panel desplegable
const BN_MSEL_FIELDS = [
  { key: 'cuentaClasif', label: 'Cuenta',          from: r => r._cuenta             || '' },
  { key: 'subcuenta',    label: 'Subcuenta',       from: r => r._subcuenta          || '' },
  { key: 'categoria',    label: 'Categoría',       from: r => r._categoria_gasto    || '' },
  { key: 'concepto',     label: 'Concepto',        from: r => r._concepto           || '' },
  { key: 'cuentaBan',    label: 'Cuenta bancaria', from: r => r['Cuenta bancaria']  || '' },
  { key: 'dia',          label: '📅 Día (fecha)',  from: r => {
      const iso = bn_formatDiaISO(r.Día || r.Dia || '');
      const m = iso.match(/^(\d{4}-\d{2}-\d{2})/);
      return m ? m[1] : '';
    } },
  { key: 'propiedad',    label: 'Propiedad',       from: r => r._propiedad          || '' },
  { key: 'departamento', label: '# Departamento',  from: r => (r._departamento !== undefined && r._departamento !== null && r._departamento !== '') ? String(r._departamento) : '' },
  { key: 'deducible',    label: 'Deducible',       from: r => r._deducible          || '' },
  { key: 'duda',         label: 'Duda',            from: r => r._duda               || '' },
  { key: 'validado',     label: 'Validado',        from: r => r._validado           || '' },
  { key: 'factura',      label: 'Factura',         from: r => r.FacturaFlag || (r.Factura ? 'Con factura' : 'Sin factura') },
  { key: 'metodoPago',   label: 'Método de pago',  from: r => r._metodo_pago        || '' },
  { key: 'reembolso',    label: 'Reembolso',       from: r => r._reembolso          || '' },
  { key: 'encargado',    label: 'Encargado de operación', from: r => r._encargado   || '' },
];
const BN_BCACHE = { E: null, I: null };
let BN_CATALOG = {};           // catálogo construido desde Presupuesto_sys
let _BN_ORIG_CATALOG = null;   // respaldo del CATALOG de tickets

// ─── Helpers ─────────────────────────────────────────────────────────────────
const bn_fmt$  = (x) => Number(x || 0).toLocaleString('es-MX', { style: 'currency', currency: 'MXN' });
const bn_fmtPct= (x) => isFinite(x) ? (x * 100).toFixed(1) + '%' : '—';
const bn_norm  = (s) => String(s ?? '').trim();
const bn_canon = (s) => bn_norm(s).normalize('NFD').replace(/[̀-ͯ]/g,'').replace(/\s+/g,' ').toLowerCase();
const bn_cc    = (s) => bn_canon(s).replace(/[^a-z0-9]+/g,'');
const bn_uniq  = (arr) => [...new Set(arr.map(bn_norm).filter(Boolean))].sort((a,b)=>a.localeCompare(b,'es'));

/** Devuelve el valor de Día formateado como YYYY-MM-DD (para <input type="date">). */
function bn_formatDiaISO(d) {
  const s = String(d || '').trim();
  if (!s) return '';
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.substring(0, 10);
  if (s.includes('GMT') || /^\w{3}\s+\w{3}\s+\d/.test(s)) {
    const dt = new Date(s);
    if (!isNaN(dt.getTime())) {
      const y  = dt.getFullYear();
      const m  = String(dt.getMonth() + 1).padStart(2, '0');
      const dy = String(dt.getDate()).padStart(2, '0');
      return `${y}-${m}-${dy}`;
    }
  }
  // d/m/AAAA o dd/mm/aaaa → YYYY-MM-DD
  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    const [, d2, mo, y] = m;
    return `${y}-${mo.padStart(2,'0')}-${d2.padStart(2,'0')}`;
  }
  return '';
}

/** Devuelve el valor de Día formateado como d/m/AAAA (ej. 2/1/2025).
 *  Acepta YYYY-MM-DD o Date.toString() de Apps Script. */
function bn_formatDia(d) {
  const s = String(d || '').trim();
  if (!s) return '';
  // Ya es YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
    const [y, m, dy] = s.substring(0, 10).split('-');
    return `${Number(dy)}/${Number(m)}/${y}`;
  }
  // "Wed Jan 01 2025 00:00:00 GMT-0600..." — Date.toString() de Apps Script
  if (s.includes('GMT') || /^\w{3}\s+\w{3}\s+\d/.test(s)) {
    const dt = new Date(s);
    if (!isNaN(dt.getTime())) {
      return `${dt.getDate()}/${dt.getMonth() + 1}/${dt.getFullYear()}`;
    }
  }
  return s;
}

/** Construye BN_CATALOG desde BN_BUDGET (Presupuesto_sys).
 *  Soporta las estructuras:
 *    4 niveles: cuenta > subcuenta > categoria > concepto
 *    3 niveles: cuenta > subcuenta > concepto   (sin categoria)
 *    3 niveles: cuenta > categoria > concepto   (sin subcuenta)
 */
function bn_buildBnCatalog() {
  const cat = {};
  for (const b of (BN_BUDGET || [])) {
    const cuenta = (b.CUENTA || b.TIPO || '').trim();
    const sub    = (b.SUBCUENTA || '').trim();
    const catg   = (b.CATEGORIA || '').trim();
    const con    = (b.CONCEPTO  || '').trim();
    if (!cuenta) continue;
    if (!cat[cuenta]) cat[cuenta] = {};

    if (sub && catg) {
      // 4-nivel: cuenta > sub > catg > concepto
      if (Array.isArray(cat[cuenta][sub])) {
        // estaba como array 3-nivel; promover a objeto
        const prev = cat[cuenta][sub];
        cat[cuenta][sub] = {};
        if (prev.length) cat[cuenta][sub][''] = prev;
      }
      if (!cat[cuenta][sub]) cat[cuenta][sub] = {};
      if (!cat[cuenta][sub][catg]) cat[cuenta][sub][catg] = [];
      if (con && !cat[cuenta][sub][catg].includes(con)) cat[cuenta][sub][catg].push(con);
    } else if (sub) {
      // 3-nivel: cuenta > sub > concepto (sin catg)
      if (!cat[cuenta][sub] || (typeof cat[cuenta][sub] === 'object' && !Array.isArray(cat[cuenta][sub]) && Object.keys(cat[cuenta][sub]).length === 0)) {
        cat[cuenta][sub] = [];
      }
      if (Array.isArray(cat[cuenta][sub]) && con && !cat[cuenta][sub].includes(con)) {
        cat[cuenta][sub].push(con);
      }
    } else if (catg) {
      // 3-nivel: cuenta > catg > concepto (sin sub)
      if (!cat[cuenta][catg]) cat[cuenta][catg] = [];
      if (Array.isArray(cat[cuenta][catg]) && con && !cat[cuenta][catg].includes(con)) {
        cat[cuenta][catg].push(con);
      }
    }
  }
  BN_CATALOG = cat;
}

/** Activa el catálogo de Presupuesto_sys para el panel Clasificar de Bancos. */
function bn_activateCatalog() {
  if (!_BN_ORIG_CATALOG) _BN_ORIG_CATALOG = CATALOG;
  CATALOG = BN_CATALOG;
  // Construir índice de búsqueda DIRECTO desde BN_BUDGET (lista plana).
  // Esto garantiza que TODOS los renglones de Presupuesto_sys sean buscables
  // independientemente de la estructura jerárquica del catálogo.
  SEARCH_INDEX = (BN_BUDGET || [])
    .filter(b => (b.CUENTA || b.TIPO))
    .map(b => ({
      cuenta:    (b.CUENTA    || b.TIPO || '').trim(),
      subcuenta: (b.SUBCUENTA || '').trim(),
      categoria: (b.CATEGORIA || '').trim(),
      concepto:  (b.CONCEPTO  || '').trim()
    }));
}
/** Restaura el catálogo original de tickets. */
function bn_deactivateCatalog() {
  if (_BN_ORIG_CATALOG) {
    CATALOG = _BN_ORIG_CATALOG;
    SEARCH_INDEX = buildSearchIndex(); // reconstruir índice con catálogo de Tickets
  }
}

/** Extrae solo el nombre del mes ("Enero 2025" → "Enero", "01/2025" → "Enero"). */
function bn_mesLabel(m) {
  const NAMES = ['','Enero','Febrero','Marzo','Abril','Mayo','Junio',
                 'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  const s = String(m || '').trim();
  // Número al inicio: "01/2025", "3-2026"
  const numPfx = s.match(/^(\d{1,2})\s*[.\-\/]/);
  if (numPfx) return NAMES[Number(numPfx[1])] || s;
  // Nombre de mes en español al inicio: "enero 2026", "Febrero"
  const MAP = {enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,
               julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12};
  const key = bn_canon(s).split(' ')[0];
  if (MAP[key]) return NAMES[MAP[key]];
  // Date string de Sheets: "Wed Jan 01 2026 00:00:00 GMT-0600..."
  const dt = new Date(s);
  if (!isNaN(dt.getTime()) && dt.getFullYear() > 1900) return NAMES[dt.getMonth() + 1];
  return s;
}

function bn_monthOrd(m) {
  const mm = String(m||'').match(/^\s*(\d{1,2})\s*[.\-\/]/);
  if (mm) return Number(mm[1]);
  const map = {enero:1,febrero:2,marzo:3,abril:4,mayo:5,junio:6,
    julio:7,agosto:8,septiembre:9,setiembre:9,octubre:10,noviembre:11,diciembre:12};
  return map[bn_canon(String(m||'')).split(' ')[0]] || 99;
}
function bn_yearOf(val) { const m=String(val||'').match(/\b(20\d{2})\b/); return m?m[1]:''; }

/**
 * Parsea el campo Día (ya normalizado a ISO) y devuelve { año, mes }.
 * Sin filtros de año/mes — función mantenida por compatibilidad.
 */
function bn_parseDia(dia) { return { año: '', mes: '' }; }

function bn_sevOver(av)  {
  if(!isFinite(av))   return {label:'—',       cls:'bn-pill'};
  if(av<=1.0)         return {label:'OK',       cls:'bn-pill bn-good'};
  if(av<=1.10)        return {label:'Alerta',   cls:'bn-pill bn-warn'};
                      return {label:'Crítico',  cls:'bn-pill bn-bad'};
}
function bn_sevUnder(av) {
  if(!isFinite(av))   return {label:'—',       cls:'bn-pill'};
  if(av>=1.0)         return {label:'OK',       cls:'bn-pill bn-good'};
  if(av>=0.90)        return {label:'Alerta',   cls:'bn-pill bn-warn'};
                      return {label:'Crítico',  cls:'bn-pill bn-bad'};
}

// ─── Budget index ─────────────────────────────────────────────────────────────
function bn_buildBIdx(tipo) {
  const m1=new Map(), m2=new Map();
  for (const b of (BN_BUDGET||[])) {
    const t = bn_canon(String(b.CUENTA??b.TIPO??''));
    const isE = t.includes('egr')||t.includes('gasto');
    const isI = t.includes('ing')||t.includes('venta');
    if (t && tipo==='E' && !isE) continue;
    if (t && tipo==='I' && !isI) continue;
    const cat=String(b.CATEGORIA??''), con=String(b.CONCEPTO??'');
    const val=Number(b.MENSUAL??0)||0;
    const k1=bn_canon(cat)+'||'+bn_canon(con);
    const k2=bn_cc(cat)+'||'+bn_cc(con);
    m1.set(k1,(m1.get(k1)||0)+val);
    m2.set(k2,(m2.get(k2)||0)+val);
  }
  return {m1,m2};
}

function bn_getBud(tipo,cat,con) {
  if (!BN_BCACHE[tipo]) BN_BCACHE[tipo]=bn_buildBIdx(tipo);
  const {m1,m2}=BN_BCACHE[tipo];
  const k1=bn_canon(cat)+'||'+bn_canon(con);
  const k2=bn_cc(cat)+'||'+bn_cc(con);
  return m1.get(k1)??m2.get(k2)??0;
}
function bn_resetBCache() { BN_BCACHE.E=null; BN_BCACHE.I=null; }
function bn_txt(id,v) { const el=document.getElementById(id); if(el) el.textContent=v; }

// ─── Load ─────────────────────────────────────────────────────────────────────
async function bn_loadData() {
  const lbl   = document.getElementById('bn-status-label');
  const empty = document.getElementById('bn-empty');
  const emMsg = document.getElementById('bn-empty-msg');
  if (lbl) lbl.textContent='Cargando…';
  if (empty) { empty.style.display='flex'; if(emMsg) emMsg.textContent='Cargando datos…'; }
  document.getElementById('bn-table-wrap')?.classList.add('hidden');
  document.getElementById('bn-indicadores')?.classList.add('hidden');
  document.getElementById('bn-kpi-row')?.classList.add('hidden');
  try { showLoading('Cargando datos…', 'Conectando con Google Sheets'); } catch(_) {}
  try {
    const res  = await fetch(BACKEND+'/get-bancos',{cache:'no-store'});
    const data = await res.json();
    if (!data.ok) throw new Error(data.error||'Error al obtener datos');
    BN_RAW=(data.records||[]).map((r,i)=>{
      const rec = r.rowNum ? {...r} : {...r, rowNum: i+2};
      // Inicializar campos _ desde valores ya clasificados en el sheet
      rec._cuenta          = rec.CUENTA    || '';
      rec._subcuenta       = rec.SUBCUENTA || '';
      rec._categoria_gasto = rec.CATEGORIA || '';
      rec._concepto        = rec.CONCEPTO  || '';
      rec._duda            = rec.DUDA      || '';
      rec._duda_nota       = rec.DUDA_NOTA || '';
      rec._validado        = rec.VALIDADO  || '';
      rec._comentarios     = rec.COMENTARIOS || rec.Comentarios || '';
      // _tipo: clasificación manual > TIPO del sheet > signo del monto
      // Normalizar a forma canónica para que CUENTA_COLOR_CLASS y filtros funcionen
      const monto = Number(rec.Monto) || 0;
      const _rawTipo = rec._cuenta || rec.TIPO || (monto < 0 ? 'Egresos' : monto > 0 ? 'Ingresos' : '');
      const _ct = bn_canon(_rawTipo);
      rec._tipo = _ct.includes('egr') ? 'Egresos' : _ct.includes('ing') ? 'Ingresos' :
                  _ct.includes('activ') ? 'Activos' : _ct.includes('pasiv') ? 'Pasivos' :
                  _ct.includes('capital') ? 'Capital' : _rawTipo;
      return rec;
    });
    BN_BUDGET=data.budget||[];
    BN_LOADED=true; bn_resetBCache();
    bn_buildBnCatalog();
    bn_activateCatalog(); // Usa el catálogo de Presupuesto_sys mientras el módulo está activo
    if (lbl) {
      const ssId  = data.spreadsheetId  || '1f_rdwQncSUXRNEvp5kM_kjyX1S-NnY7UE2z4HGvFL3Q';
      const ssUrl = data.spreadsheetUrl || `https://docs.google.com/spreadsheets/d/${ssId}/edit`;
      lbl.innerHTML = `Registros contables — ${BN_RAW.length.toLocaleString('es-MX')} movimientos` +
        `&nbsp;&nbsp;📄 <a href="${ssUrl}" target="_blank" style="font-size:11px;color:var(--text-soft);text-decoration:underline">Ver Spreadsheet fuente</a>`;
    }
    if (empty) empty.style.display='none';
    bn_populateFilters();
    bn_setDefaultFilters();
    // Inicializar categoría (renderiza chips de sub-opciones)
    bn_setCat(BN_TIPO_PARENT[BN_TIPO] || 'pc');
  } catch(e) {
    if (lbl) lbl.textContent='Error al cargar';
    if (empty) { empty.style.display='flex'; if(emMsg) emMsg.textContent='Error: '+e.message; }
  } finally {
    try { hideLoading(); } catch(_) {}
  }
}

// ─── Filters ─────────────────────────────────────────────────────────────────
// Devuelve {key:'YYYY-MM', label:'Enero 2026'} a partir del campo Día ('YYYY-MM-DD' o Date.toString())
function bn_diaToMesAnio(d) {
  const iso = bn_formatDiaISO(d || '');
  if (!/^\d{4}-\d{2}-\d{2}/.test(iso)) return null;
  const [y, m] = iso.split('-');
  const NOMBRES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
                   'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  return { key: `${y}-${m}`, label: `${NOMBRES[Number(m)-1]} ${y}` };
}

// Devuelve los registros que pertenecen a una pestaña (sin aplicar otros filtros).
// Usado para poblar los dropdowns en función del menú seleccionado.
/** ¿El tipo activo está bajo el menú 'Por clasificar'? */
function bn_isPC(tipo) {
  return tipo === 'PC' || (typeof tipo === 'string' && tipo.startsWith('PC_'));
}
/** Devuelve el sub-código de tipo (E, I, AC, PA, CA, T) para un PC_X. */
function bn_subOfPC(tipo) {
  if (tipo === 'PC' || tipo === 'PC_T') return 'T';
  if (typeof tipo === 'string' && tipo.startsWith('PC_')) return tipo.slice(3);
  return null;
}

// ─── Análisis por partida — agregado jerárquico con semáforo ───────────────
/** Construye un árbol agregado a partir de records y la lista de niveles. */
function bn_apBuildTree(records, levels) {
  const root = { name: '__root', children: new Map(), total: 0, count: 0 };
  for (const r of records) {
    const monto = Math.abs(Number(r.Monto) || 0);
    let node = root;
    node.total += monto; node.count += 1;
    for (const lvl of levels) {
      const key = String(r[lvl] || '').trim() || '(Sin asignar)';
      if (!node.children.has(key)) {
        node.children.set(key, { name: key, level: lvl, children: new Map(), total: 0, count: 0 });
      }
      node = node.children.get(key);
      node.total += monto; node.count += 1;
    }
  }
  return root;
}

/** Devuelve {value, cycle} para una entrada de Presupuesto.
 *  Usa PERIODICIDAD si el cycle corresponde tiene valor; sino prioridad
 *  Mensual → Bimestral → Semanal → Anual. */
function bn_apBudgetForRow(b) {
  const cycles = {
    Mensual:   Math.abs(Number(b.MENSUAL)   || 0),
    Bimestral: Math.abs(Number(b.BIMESTRAL) || 0),
    Semanal:   Math.abs(Number(b.SEMANAL)   || 0),
    Anual:     Math.abs(Number(b.ANUAL)     || 0),
  };
  const per = String(b.PERIODICIDAD || '').toLowerCase();
  if (per.includes('mens')  && cycles.Mensual   > 0) return { value: cycles.Mensual,   cycle: 'Mensual' };
  if (per.includes('bimes') && cycles.Bimestral > 0) return { value: cycles.Bimestral, cycle: 'Bimestral' };
  if (per.includes('seman') && cycles.Semanal   > 0) return { value: cycles.Semanal,   cycle: 'Semanal' };
  if (per.includes('anu')   && cycles.Anual     > 0) return { value: cycles.Anual,     cycle: 'Anual' };
  // Fallback: prioridad
  if (cycles.Mensual   > 0) return { value: cycles.Mensual,   cycle: 'Mensual' };
  if (cycles.Bimestral > 0) return { value: cycles.Bimestral, cycle: 'Bimestral' };
  if (cycles.Semanal   > 0) return { value: cycles.Semanal,   cycle: 'Semanal' };
  if (cycles.Anual     > 0) return { value: cycles.Anual,     cycle: 'Anual' };
  return { value: 0, cycle: '' };
}

/** Calcula presupuesto agregado de un nodo. Devuelve {value, cycle} donde
 *  cycle es 'Mensual'/'Bimestral'/'Semanal'/'Anual' o '' si no aplica.
 *  Si hay múltiples ciclos diferentes bajo el nodo, devuelve 'Mixto'. */
function bn_apComputeBudget(node, records, ancestors, allLevels) {
  // Filtra records que coinciden con todos los ancestros
  let matchingRecs = records;
  for (let i = 0; i < ancestors.length; i++) {
    const lvl = allLevels[i], key = ancestors[i];
    matchingRecs = matchingRecs.filter(r => {
      const v = String(r[lvl] || '').trim() || '(Sin asignar)';
      return v === key;
    });
  }
  // Sumar presupuesto único por (cat||con)
  const seen = new Set();
  let total = 0;
  const cyclesSeen = new Set();
  for (const r of matchingRecs) {
    const cat = bn_norm(r.CATEGORIA || r._categoria_gasto || '');
    const con = bn_norm(r.CONCEPTO  || r._concepto || '');
    const k = cat + '||' + con;
    if (seen.has(k)) continue;
    seen.add(k);
    // Buscar la entrada del presupuesto que coincide con (cat, con)
    const budRow = (BN_BUDGET || []).find(b =>
      bn_norm(b.CATEGORIA || '') === cat && bn_norm(b.CONCEPTO || '') === con);
    if (budRow) {
      const { value, cycle } = bn_apBudgetForRow(budRow);
      total += value;
      if (cycle) cyclesSeen.add(cycle);
    }
  }
  let cycleLabel = '';
  if (cyclesSeen.size === 1) cycleLabel = [...cyclesSeen][0];
  else if (cyclesSeen.size > 1) cycleLabel = 'Mixto';
  return { value: total, cycle: cycleLabel };
}

// ─── Presupuesto — editor de la hoja Presupuesto_sys ──────────────────────
const BN_PR_COLS = [
  { key: 'CUENTA',        label: 'Cuenta',        width: 130, chip: true },
  { key: 'SUBCUENTA',     label: 'Subcuenta',     width: 160 },
  { key: 'CATEGORIA',     label: 'Categoría',     width: 150 },
  { key: 'CONCEPTO',      label: 'Concepto',      width: 150 },
  { key: 'DESCRIPCION',   label: 'Descripción',   width: 200 },
  { key: 'TIPO',          label: 'Tipo',          width: 90  },
  { key: 'PERIODICIDAD',  label: 'Periodicidad',  width: 100 },
  { key: 'NATURALEZA',    label: 'Naturaleza',    width: 100 },
  { key: 'SEMANAL',       label: 'Semanal',       width: 110, numeric: true },
  { key: 'MENSUAL',       label: 'Mensual',       width: 110, numeric: true },
  { key: 'BIMESTRAL',     label: 'Bimestral',     width: 110, numeric: true },
  { key: 'ANUAL',         label: 'Anual',         width: 110, numeric: true },
];

let BN_PR_DRAFT = null; // copia local editable

let BN_PR_ORIGINAL = null; // snapshot original para comparar y detectar cambios

function bn_prSnapshot(arr) {
  // Devuelve un string canónico para comparar igualdad estructural
  return JSON.stringify((arr || []).map(r => {
    const o = {};
    BN_PR_COLS.forEach(c => o[c.key] = r[c.key] != null ? String(r[c.key]) : '');
    return o;
  }));
}

function bn_prHasChanges() {
  if (!BN_PR_DRAFT || !BN_PR_ORIGINAL) return false;
  return bn_prSnapshot(BN_PR_DRAFT) !== BN_PR_ORIGINAL;
}

function bn_renderPresupuesto() {
  const wrap = document.getElementById('bn-presupuesto');
  if (!wrap) return;

  // Inicializar draft desde BN_BUDGET la primera vez (o si se cancela)
  if (!BN_PR_DRAFT) {
    BN_PR_DRAFT = (BN_BUDGET || []).map(b => {
      const row = {};
      BN_PR_COLS.forEach(c => row[c.key] = b[c.key] !== undefined ? b[c.key] : '');
      return row;
    });
    BN_PR_ORIGINAL = bn_prSnapshot(BN_PR_DRAFT);
  }

  const headersHtml = BN_PR_COLS.map(c =>
    `<th style="padding:8px 10px;text-align:${c.numeric?'right':'left'};font-size:11px;text-transform:uppercase;letter-spacing:.04em;background:#475569;color:#fff;min-width:${c.width}px;white-space:nowrap">${esc(c.label)}</th>`
  ).join('') + `<th style="padding:8px 10px;background:#475569;color:#fff;text-align:center;width:90px;font-size:11px;text-transform:uppercase;letter-spacing:.04em">Eliminar fila</th>`;

  // "+" insertor de fila angosto — sobre la línea divisoria, sin ocupar alto
  const inserter = (insertAt) => `
    <tr class="bn-pr-inserter">
      <td colspan="${BN_PR_COLS.length + 1}" style="padding:0;height:2px;line-height:0;position:relative;background:#e2e8f0">
        <button onclick="bn_prInsertRow(${insertAt})"
                title="Insertar fila aquí"
                style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
                       width:20px;height:20px;border-radius:50%;border:1.5px solid #2563eb;
                       background:#fff;color:#2563eb;font-weight:900;font-size:14px;
                       line-height:1;cursor:pointer;padding:0;
                       display:inline-flex;align-items:center;justify-content:center;
                       opacity:.45;transition:opacity .15s,transform .15s;z-index:2"
                onmouseover="this.style.opacity='1';this.style.transform='translate(-50%,-50%) scale(1.15)'"
                onmouseout="this.style.opacity='.45';this.style.transform='translate(-50%,-50%)'">+</button>
      </td>
    </tr>`;

  // Colores por cuenta (mismos que registros bancarios)
  const PR_CUENTA_BG = { Egresos:'#fee2e2', Ingresos:'#dcfce7', Activos:'#dbeafe', Pasivos:'#ede9fe', Capital:'#fef3c7' };
  const PR_CUENTA_FG = { Egresos:'#991b1b', Ingresos:'#166534', Activos:'#1e40af', Pasivos:'#5b21b6', Capital:'#92400e' };
  const PR_CUENTA_EMOJI = { Egresos:'💸', Ingresos:'💰', Activos:'📈', Pasivos:'📋', Capital:'💼' };
  const dataRows = BN_PR_DRAFT.map((row, i) => {
    const cells = BN_PR_COLS.map(c => {
      const v = row[c.key] != null ? String(row[c.key]) : '';
      const ta = c.numeric ? 'text-align:right' : '';
      if (c.chip && c.key === 'CUENTA') {
        const bg = PR_CUENTA_BG[v] || '#f1f5f9';
        const fg = PR_CUENTA_FG[v] || '#334155';
        const em = PR_CUENTA_EMOJI[v] || '🏷️';
        return `<td contenteditable="true" spellcheck="false"
                    data-row="${i}" data-key="${c.key}"
                    oninput="bn_prCellEdit(this)"
                    style="padding:6px 9px;border-bottom:1px solid #e2e8f0;font-size:12px;min-width:${c.width}px;outline:none">
                  <span style="display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:999px;background:${bg};color:${fg};font-weight:700;font-size:11px">${em} ${esc(v)}</span>
                </td>`;
      }
      return `<td contenteditable="true" spellcheck="false"
                  data-row="${i}" data-key="${c.key}"
                  oninput="bn_prCellEdit(this)"
                  style="padding:7px 9px;border-bottom:1px solid #e2e8f0;font-size:12px;min-width:${c.width}px;${ta};outline:none"
                  onfocus="this.style.background='#f1f5f9'"
                  onblur="this.style.background=''">${esc(v)}</td>`;
    }).join('');
    return `<tr>${cells}<td style="padding:7px 9px;border-bottom:1px solid #e2e8f0;text-align:center">
              <button onclick="bn_prDeleteRow(${i})" title="Eliminar fila"
                      style="width:24px;height:24px;border:none;background:#fee2e2;color:#dc2626;border-radius:50%;font-weight:800;cursor:pointer">✕</button>
            </td></tr>`;
  });

  // Intercalar inserters: + entre cada par de filas, + al inicio, + al final
  const bodyParts = [];
  bodyParts.push(inserter(0));
  for (let i = 0; i < dataRows.length; i++) {
    bodyParts.push(dataRows[i]);
    bodyParts.push(inserter(i + 1));
  }
  const rowsHtml = bodyParts.join('');

  const hasChanges = bn_prHasChanges();
  const actionsHtml = hasChanges ? `
    <div style="display:flex;gap:8px;margin-left:auto">
      <button onclick="bn_prResetDraft()" style="padding:8px 14px;border:1.5px solid #cbd5e1;background:#fff;color:#475569;border-radius:8px;font-weight:600;font-size:12px;cursor:pointer">↺ Descartar cambios</button>
      <button onclick="bn_prSaveConfirm()" style="padding:8px 16px;border:none;background:#334155;color:#fff;border-radius:8px;font-weight:700;font-size:12px;cursor:pointer">💾 Guardar cambios</button>
    </div>` : '';

  wrap.innerHTML = `
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px">
      <h3 style="margin:0;font-size:16px;display:flex;align-items:center;gap:6px">💰 Presupuesto <span style="font-size:12px;color:var(--text-soft,#64748b);font-weight:400">— hoja Presupuesto_sys</span></h3>
      ${actionsHtml}
    </div>
    <div style="overflow-x:auto;border:1px solid #cbd5e1;border-radius:10px">
      <table style="width:100%;border-collapse:collapse;background:var(--surface,#fff);font-size:12px">
        <thead><tr>${headersHtml}</tr></thead>
        <tbody>${rowsHtml}</tbody>
      </table>
    </div>
  `;
}

function bn_prCellEdit(el) {
  const i = Number(el.dataset.row);
  const k = el.dataset.key;
  if (!BN_PR_DRAFT[i]) return;
  let v = el.textContent.trim();
  const col = BN_PR_COLS.find(c => c.key === k);
  if (col?.numeric) {
    if (v === '') v = '';
    else {
      const n = Number(v.replace(/[^0-9.\-]/g, ''));
      if (isFinite(n)) v = n;
    }
  }
  BN_PR_DRAFT[i][k] = v;
  // Si entró/salió del estado modificado, mostrar/ocultar acciones
  bn_prRefreshActions();
}

/** Re-renderiza sólo el contenedor de acciones si el estado dirty cambia. */
function bn_prRefreshActions() {
  // Para mantenerlo simple, re-renderiza todo cuando el estado dirty cambie
  // pero sin perder el foco actual: comprobamos si toca alternar
  const wrap = document.getElementById('bn-presupuesto');
  if (!wrap) return;
  const had = !!wrap.querySelector('button[onclick="bn_prSaveConfirm()"]');
  const now = bn_prHasChanges();
  if (had === now) return; // sin cambio en visibilidad
  // Re-render conservando el caret no es trivial; aceptamos un re-render rápido
  bn_renderPresupuesto();
}

function bn_prInsertRow(at) {
  const row = {};
  BN_PR_COLS.forEach(c => row[c.key] = '');
  BN_PR_DRAFT.splice(Math.max(0, Math.min(BN_PR_DRAFT.length, at)), 0, row);
  bn_renderPresupuesto();
}

function bn_prDeleteRow(i) {
  BN_PR_DRAFT.splice(i, 1);
  bn_renderPresupuesto();
}

function bn_prResetDraft() {
  if (!confirm('¿Descartar todos los cambios locales y volver al contenido original?')) return;
  BN_PR_DRAFT = null;
  BN_PR_ORIGINAL = null;
  bn_renderPresupuesto();
}

async function bn_prSaveConfirm() {
  if (!confirm(`¿Estás seguro de guardar los cambios?\n\n` +
               `Esto sobreescribirá la hoja Presupuesto_sys con ${BN_PR_DRAFT.length} fila(s).`)) return;
  if (!confirm('Última confirmación: ¿proceder con la sobreescritura del Presupuesto?')) return;
  try {
    showLoading?.('Guardando Presupuesto…', 'Actualizando Sheets…');
    const resp = await fetch(`${BACKEND}/save-presupuesto`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        columns: BN_PR_COLS.map(c => c.key),
        rows:    BN_PR_DRAFT,
      }),
    });
    const r = await resp.json();
    hideLoading?.();
    if (!r.ok) throw new Error(r.error || 'Error al guardar');
    alert(`Guardado correctamente. ${r.rowsWritten || BN_PR_DRAFT.length} filas escritas.`);
    BN_BUDGET = BN_PR_DRAFT.slice();
    bn_resetBCache();
    BN_PR_DRAFT = null;
    BN_PR_ORIGINAL = null;
    bn_renderPresupuesto();
  } catch (e) {
    hideLoading?.();
    alert('Error al guardar: ' + e.message);
  }
}

// ─── Chatbot financiero ─────────────────────────────────────────────────
const BN_CHAT_KEY = 'bn-chat-history-v1';
let BN_CHAT_HISTORY = [];
const BN_CHAT_SUGGESTIONS = [
  'Dame la utilidad de este mes',
  'Cuánto gasté en Egresos > Insumos en febrero',
  'Resumen de ingresos por método de pago',
  '¿Cuántos registros están sin clasificar?',
  'Tickets de "Sodimac" o "Home Depot"',
];

function bn_chatToggle() {
  const p = document.getElementById('bn-chat-panel');
  if (!p) return;
  const open = p.classList.toggle('hidden');
  if (!open) {
    bn_chatLoadHistory();
    bn_chatRenderSuggestions();
    setTimeout(() => document.getElementById('bn-chat-input')?.focus(), 50);
  }
}

function bn_chatRenderSuggestions() {
  const wrap = document.getElementById('bn-chat-suggestions');
  if (!wrap) return;
  if (BN_CHAT_HISTORY.length) { wrap.innerHTML = ''; return; }
  wrap.innerHTML = BN_CHAT_SUGGESTIONS.map(s =>
    `<button onclick="bn_chatPrefill(${JSON.stringify(s).replace(/"/g,'&quot;')})"
             style="padding:5px 10px;border:1px solid #cbd5e1;background:#fff;color:#475569;border-radius:999px;font-size:11px;cursor:pointer">
       ${esc(s)}
     </button>`).join('');
}

function bn_chatPrefill(text) {
  const inp = document.getElementById('bn-chat-input');
  if (inp) { inp.value = text; inp.focus(); }
}

function bn_chatLoadHistory() {
  try { BN_CHAT_HISTORY = JSON.parse(localStorage.getItem(BN_CHAT_KEY) || '[]'); }
  catch(_) { BN_CHAT_HISTORY = []; }
  const box = document.getElementById('bn-chat-messages');
  if (!box) return;
  if (!BN_CHAT_HISTORY.length) {
    box.innerHTML = `
      <div style="padding:14px;background:#fff;border:1px solid #e2e8f0;border-radius:12px;font-size:13px;color:#334155">
        👋 Hola, soy tu asistente financiero. Puedo responder sobre tus
        <b>registros contables</b>, <b>tickets</b>, <b>presupuesto</b> y
        análisis. Pregúntame lo que quieras.
      </div>`;
    return;
  }
  box.innerHTML = BN_CHAT_HISTORY.map(bn_chatBubble).join('');
  box.scrollTop = box.scrollHeight;
}

function bn_chatBubble(msg) {
  const isUser = msg.role === 'user';
  const align = isUser ? 'flex-end' : 'flex-start';
  const bg    = isUser ? '#475569' : '#fff';
  const color = isUser ? '#fff'    : '#1f2937';
  const border = isUser ? 'none'   : '1px solid #e2e8f0';
  return `<div style="align-self:${align};max-width:85%;padding:10px 12px;background:${bg};color:${color};border:${border};border-radius:12px;font-size:13px;white-space:pre-wrap;line-height:1.5;word-break:break-word">${esc(msg.text)}</div>`;
}

function bn_chatSave() {
  try { localStorage.setItem(BN_CHAT_KEY, JSON.stringify(BN_CHAT_HISTORY.slice(-30))); } catch(_) {}
}

function bn_chatClear() {
  if (!confirm('¿Limpiar toda la conversación?')) return;
  BN_CHAT_HISTORY = [];
  bn_chatSave();
  bn_chatLoadHistory();
  bn_chatRenderSuggestions();
}

function bn_chatPush(role, text) {
  BN_CHAT_HISTORY.push({ role, text, ts: Date.now() });
  const box = document.getElementById('bn-chat-messages');
  if (box) {
    box.insertAdjacentHTML('beforeend', bn_chatBubble({ role, text }));
    box.scrollTop = box.scrollHeight;
  }
  bn_chatSave();
  bn_chatRenderSuggestions();
}

/** Construye un contexto compacto con los datos disponibles para que el
 *  modelo responda con precisión. */
function bn_chatBuildContext() {
  // Construye AGREGADOS sobre TODO BN_RAW (sin slice). El bot no recibe
  // registros individuales — recibe sumatorias precomputadas que cubren
  // el 100% de los datos.
  const all = BN_RAW || [];
  const fechas = [];
  // map: clave → { ing, egr, n_ing, n_egr }
  const acc = (map, key, monto, tipo) => {
    if (!map[key]) map[key] = { ing: 0, egr: 0, n_ing: 0, n_egr: 0 };
    const e = map[key];
    if (tipo === 'I') { e.ing += monto; e.n_ing++; }
    else if (tipo === 'E') { e.egr += monto; e.n_egr++; }
  };
  const porMes = {};                 // YYYY-MM
  const porCuentaMes = {};           // 'Cuenta||YYYY-MM'
  const porSubMes = {};              // 'Cuenta||Sub||YYYY-MM'
  const porCatMes = {};              // 'Cuenta||Sub||Cat||YYYY-MM'
  const porConMes = {};              // 'Cuenta||Sub||Cat||Con||YYYY-MM'
  const porCtaBan = {};              // 'CuentaBancaria||YYYY-MM'
  const porMP    = {};               // 'MetodoPago||YYYY-MM'
  const porEnc   = {};               // 'Encargado||YYYY-MM'
  const porProp  = {};               // 'Propiedad||YYYY-MM'

  for (const r of all) {
    const iso = bn_formatDiaISO(r.Día || r.Dia || '');
    const m = iso.match(/^(\d{4})-(\d{2})/);
    if (!m) continue;
    const ym = m[1] + '-' + m[2];
    fechas.push(iso);
    const monto = Math.abs(Number(r.Monto) || 0);
    const tipoCanon = bn_canon(r._tipo || r._cuenta || '');
    let tipo = '';
    if (tipoCanon.includes('ing')) tipo = 'I';
    else if (tipoCanon.includes('egr')) tipo = 'E';
    else continue; // Activos/Pasivos/Capital no entran en Ingresos/Egresos
    const cuenta = r._cuenta || '';
    const sub    = r._subcuenta || '';
    const cat    = r._categoria_gasto || '';
    const con    = r._concepto || '';
    const ctaBan = r['Cuenta bancaria'] || '(sin cuenta)';
    const mp     = r._metodo_pago || '(sin método)';
    const enc    = r._encargado || '(sin encargado)';
    const prop   = r._propiedad || '(sin propiedad)';

    acc(porMes, ym, monto, tipo);
    if (cuenta) acc(porCuentaMes, `${cuenta}||${ym}`, monto, tipo);
    if (cuenta && sub) acc(porSubMes, `${cuenta}||${sub}||${ym}`, monto, tipo);
    if (cuenta && sub && cat) acc(porCatMes, `${cuenta}||${sub}||${cat}||${ym}`, monto, tipo);
    if (cuenta && sub && cat && con) acc(porConMes, `${cuenta}||${sub}||${cat}||${con}||${ym}`, monto, tipo);
    acc(porCtaBan, `${ctaBan}||${ym}`, monto, tipo);
    acc(porMP, `${mp}||${ym}`, monto, tipo);
    acc(porEnc, `${enc}||${ym}`, monto, tipo);
    acc(porProp, `${prop}||${ym}`, monto, tipo);
  }

  // Convierte mapas a arrays compactos
  const round2 = n => Math.round(n*100)/100;
  const toArr = (map, keys) => Object.entries(map).map(([k, v]) => {
    const parts = k.split('||');
    const row = {};
    keys.forEach((kn, i) => row[kn] = parts[i]);
    row.I = round2(v.ing); row.E = round2(v.egr);
    row.U = round2(v.ing - v.egr);
    row.nI = v.n_ing; row.nE = v.n_egr;
    return row;
  });

  const resumenMes = toArr(porMes, ['Mes']).sort((a,b) => a.Mes.localeCompare(b.Mes));
  const porCuenta_ = toArr(porCuentaMes, ['Cuenta','Mes']);
  const porSub_    = toArr(porSubMes,    ['Cuenta','Sub','Mes']);
  const porCat_    = toArr(porCatMes,    ['Cuenta','Sub','Cat','Mes']);
  const porCon_    = toArr(porConMes,    ['Cuenta','Sub','Cat','Con','Mes']);
  const porCtaBan_ = toArr(porCtaBan,    ['CtaBancaria','Mes']);
  const porMP_     = toArr(porMP,        ['MetodoPago','Mes']);
  const porEnc_    = toArr(porEnc,       ['Encargado','Mes']);
  const porProp_   = toArr(porProp,      ['Propiedad','Mes']);
  const budget = (BN_BUDGET || []).slice(0, 200).map(b => ({
    Cuenta: b.CUENTA || '', Sub: b.SUBCUENTA || '', Cat: b.CATEGORIA || '',
    Con: b.CONCEPTO || '', Per: b.PERIODICIDAD || '', Nat: b.NATURALEZA || '',
    Mensual: Number(b.MENSUAL) || 0, Anual: Number(b.ANUAL) || 0,
    Semanal: Number(b.SEMANAL) || 0, Bimestral: Number(b.BIMESTRAL) || 0,
  }));
  const tickets = (BN_TICKETS_CACHE || []).slice(0, 200).map(t => {
    const tk = bn_ticketFields(t);
    return {
      tienda: tk.tienda || '', fecha: tk.fecha || '',
      total: Number(tk.total) || 0, folio: tk.folio || '',
      rfc: tk.rfc || '',
    };
  });
  fechas.sort();
  const rango = fechas.length
    ? { desde: fechas[0], hasta: fechas[fechas.length - 1] }
    : null;
  return {
    fecha_hoy:    new Date().toISOString().slice(0,10),
    total_registros: all.length,
    rango_fechas: rango,
    tickets_cargados: tickets.length,
    // Agregados — el bot debe responder usando ÚNICAMENTE estos arrays.
    // Cada fila tiene: I (Ingresos), E (Egresos), U (Utilidad), nI/nE (conteos).
    agregados: {
      por_mes:               resumenMes,    // {Mes, I, E, U, nI, nE}
      por_cuenta_mes:        porCuenta_,    // +Cuenta
      por_subcuenta_mes:     porSub_,       // +Sub
      por_categoria_mes:     porCat_,       // +Cat
      por_concepto_mes:      porCon_,       // +Con
      por_cuenta_bancaria:   porCtaBan_,    // {CtaBancaria, Mes, ...}
      por_metodo_pago:       porMP_,        // {MetodoPago, Mes, ...}
      por_encargado:         porEnc_,       // {Encargado, Mes, ...}
      por_propiedad:         porProp_,      // {Propiedad, Mes, ...}
    },
    presupuesto: budget,
    tickets,
  };
}

async function bn_chatSend() {
  const inp = document.getElementById('bn-chat-input');
  const btn = document.getElementById('bn-chat-send');
  if (!inp || !btn) return;
  const text = inp.value.trim();
  if (!text) return;
  inp.value = '';
  bn_chatPush('user', text);

  // typing indicator
  const box = document.getElementById('bn-chat-messages');
  const typingId = 'bn-chat-typing-' + Date.now();
  if (box) {
    box.insertAdjacentHTML('beforeend',
      `<div id="${typingId}" style="align-self:flex-start;padding:10px 14px;background:#fff;border:1px solid #e2e8f0;border-radius:12px;font-size:13px;color:#94a3b8">⏳ Pensando…</div>`);
    box.scrollTop = box.scrollHeight;
  }
  btn.disabled = true;

  try {
    const ctx = bn_chatBuildContext();
    const history = BN_CHAT_HISTORY.slice(-10).map(m => ({ role: m.role, content: m.text }));
    const resp = await fetch(`${BACKEND}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, history, context: ctx }),
    });
    const data = await resp.json();
    document.getElementById(typingId)?.remove();
    if (!data.ok) throw new Error(data.error || 'Error en el chat');
    bn_chatPush('assistant', data.reply || '(sin respuesta)');
  } catch (e) {
    document.getElementById(typingId)?.remove();
    bn_chatPush('assistant', '⚠ Error: ' + e.message);
  } finally {
    btn.disabled = false;
    inp.focus();
  }
}

// ─── Match: relación entre registros bancarios y tickets ─────────────────
let BN_TICKETS_CACHE = null; // lista de tickets cargada vía /get-tickets

/** Parser robusto de monto: acepta números, "1,234.56", "$1,234.56",
 *  "-1234.5", etc. y devuelve siempre el valor absoluto en number. */
function bn_parseMonto(v) {
  if (v == null || v === '') return 0;
  if (typeof v === 'number' && isFinite(v)) return Math.abs(v);
  const s = String(v).replace(/[^0-9.\-]/g, '');
  const n = parseFloat(s);
  return isFinite(n) ? Math.abs(n) : 0;
}

/** Devuelve un objeto plano con los campos relevantes del ticket,
 *  aceptando tanto el formato 'plano' como el envuelto en .resumen. */
function bn_ticketFields(ticket) {
  const r = (ticket && ticket.resumen) ? ticket.resumen : (ticket || {});
  return {
    total:  r.total  ?? ticket.total  ?? '',
    fecha:  r.fecha  ?? ticket.fecha  ?? '',
    tienda: r.tienda ?? ticket.tienda ?? '',
    folio:  r.folio  ?? ticket.folio  ?? '',
    rfc:    r.rfc    ?? ticket.rfc    ?? '',
  };
}

/** Calcula la similitud (score 0..1) entre un registro bancario y un ticket.
 *  El monto se compara siempre en valor absoluto — robusto al signo
 *  (un ticket de compra es positivo en Tickets y negativo en Registros). */
function bn_matchScore(rec, ticket) {
  const tk = bn_ticketFields(ticket);
  const recMonto = bn_parseMonto(rec.Monto);
  const tkTotal  = bn_parseMonto(tk.total);
  if (!recMonto || !tkTotal) return 0;

  // ── Monto (0..0.5) — descarta si la diferencia es enorme ──
  const diff = Math.abs(recMonto - tkTotal) / Math.max(recMonto, tkTotal);
  let s = 0;
  if (diff < 0.01)       s += 0.50;
  else if (diff < 0.05)  s += 0.35;
  else if (diff < 0.15)  s += 0.15;
  else                   return 0;

  // ── Fecha (0..0.3) ──
  const recDate = bn_formatDiaISO(rec.Día || rec.Dia || '');
  const tkDate  = String(tk.fecha || '').slice(0,10);
  if (recDate && tkDate && /^\d{4}-\d{2}-\d{2}/.test(tkDate)) {
    if (recDate === tkDate) s += 0.30;
    else {
      const d1 = new Date(recDate), d2 = new Date(tkDate);
      const dd = Math.abs((d1 - d2) / 86400000);
      if (dd <= 1)      s += 0.22;
      else if (dd <= 3) s += 0.12;
      else if (dd <= 7) s += 0.05;
    }
  }

  // ── Descripción (0..0.2) — overlap de palabras significativas ──
  const norm = (x) => bn_canon(String(x||'')).replace(/[^a-z0-9\s]/g,' ').replace(/\s+/g,' ').trim();
  const recDesc = norm(rec.DESCRIPCION || '');
  const tkText  = norm((tk.tienda || '') + ' ' + (tk.folio || ''));
  if (recDesc && tkText) {
    const words = tkText.split(' ').filter(w => w.length >= 3);
    if (words.length) {
      let matches = 0;
      for (const w of words) if (recDesc.includes(w)) matches++;
      s += Math.min(matches / words.length, 1) * 0.20;
    }
  }
  return Math.min(s, 1);
}

/** Encuentra el ticket con mayor score para un registro bancario. */
function bn_findBestTicket(rec, tickets) {
  let best = null, bestScore = 0;
  for (const t of (tickets || [])) {
    const sc = bn_matchScore(rec, t);
    if (sc > bestScore) { bestScore = sc; best = t; }
  }
  return best ? { ticket: best, score: bestScore } : null;
}

/** HTML del chip "Ticket encontrado" con semáforo (debajo del Deducible). */
function bn_matchChipHtml(rec, idx) {
  const m = rec._matchedTicket;
  if (!m || !(m.score > 0)) return '';
  const color = m.score >= 0.7 ? '#16a34a' : m.score >= 0.45 ? '#f59e0b' : '#dc2626';
  const label = m.score >= 0.7 ? 'Alta' : m.score >= 0.45 ? 'Media' : 'Baja';
  const tienda = (m.tienda || '—').slice(0, 30);
  return `
    <div onclick="event.stopPropagation();bn_showMatchDetail(${idx})"
         title="${esc(tienda)} · ${(m.score*100).toFixed(0)}% match — clic para detalles"
         style="display:inline-flex;align-items:center;gap:6px;padding:5px 10px;margin-top:6px;
                background:#fff;border:1.5px solid ${color};color:${color};
                border-radius:999px;font-weight:600;font-size:11px;cursor:pointer">
      <span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${color}"></span>
      <span>🧾 Ticket encontrado · ${label}</span>
    </div>`;
}

function bn_showMatchDetail(idx) {
  const rec = BN_CUR_RECS[idx];
  const m = rec?._matchedTicket;
  if (!m) return;
  alert(`Posible coincidencia (${(m.score*100).toFixed(0)}%):\n\n` +
        `Tienda: ${m.tienda || '—'}\n` +
        `Fecha:  ${m.fecha  || '—'}\n` +
        `Total:  ${bn_fmt$(Number(m.total)||0)}\n` +
        (m.folio ? `Folio:  ${m.folio}\n` : '') +
        `\nMonto del registro bancario: ${bn_fmt$(Number(rec.Monto)||0)}`);
}

/** Función principal: descarga tickets si hace falta y calcula matches
 *  para todos los registros visibles. */
async function bn_relacionarConTickets() {
  const btn = document.getElementById('bn-btn-match-tickets');
  const origLabel = btn ? btn.innerHTML : '';
  if (btn) { btn.disabled = true; btn.innerHTML = '⏳ Buscando…'; }
  try {
    if (!BN_TICKETS_CACHE) {
      const res = await fetch(`${BACKEND}/get-tickets`, { cache: 'no-store' });
      const data = await res.json();
      if (!data.ok) throw new Error(data.error || 'Error al obtener tickets');
      BN_TICKETS_CACHE = data.tickets || [];
    }
    let matched = 0;
    for (const rec of BN_RAW) {
      const r = bn_findBestTicket(rec, BN_TICKETS_CACHE);
      if (r && r.score >= 0.4) {
        const tk = bn_ticketFields(r.ticket);
        rec._matchedTicket = {
          score:   r.score,
          tienda:  tk.tienda || '',
          fecha:   tk.fecha  || '',
          total:   tk.total  || 0,
          folio:   tk.folio  || '',
        };
        matched++;
      } else {
        rec._matchedTicket = null;
      }
    }
    bn_render();
    alert(`${matched} posible(s) coincidencia(s) encontrada(s) sobre ${BN_RAW.length} registros (umbral 40%).`);
  } catch (e) {
    alert('Error: ' + e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = origLabel; }
  }
}

// Totales del último render de Análisis por partida (compartidos con KPIs)
//   global    → todas las cuentas
//   operativo → cuenta Egresos
//   reinversion → cuenta Activos
//   retiro    → cuenta Capital
let BN_AP_TOTALS = {
  global:      { real: 0, bud: 0, av: NaN, cycle: '' },
  operativo:   { real: 0, bud: 0, av: NaN, cycle: '' },
  reinversion: { real: 0, bud: 0, av: NaN, cycle: '' },
  retiro:      { real: 0, bud: 0, av: NaN, cycle: '' },
};

/** Helper: ¿el registro pertenece a la cuenta (Egresos/Activos/Capital)? */
function bn_apMatchesCuenta(r, target) {
  const t = bn_canon(r._cuenta || r._tipo || '');
  if (target === 'Egresos')  return t.includes('egr');
  if (target === 'Activos')  return t.includes('activ');
  if (target === 'Capital')  return t.includes('capital');
  return false;
}

/** Agrega un grupo de records → { real, bud, av, cycle }. */
function bn_apAggregateGroup(records) {
  let real = 0;
  for (const r of records) real += Math.abs(Number(r.Monto) || 0);
  const seen = new Set();
  let bud = 0;
  const cyclesSeen = new Set();
  for (const r of records) {
    const cat = bn_norm(r.CATEGORIA || r._categoria_gasto || '');
    const con = bn_norm(r.CONCEPTO  || r._concepto || '');
    const k = cat + '||' + con;
    if (seen.has(k)) continue;
    seen.add(k);
    const budRow = (BN_BUDGET || []).find(b =>
      bn_norm(b.CATEGORIA || '') === cat && bn_norm(b.CONCEPTO || '') === con);
    if (budRow) {
      const { value, cycle } = bn_apBudgetForRow(budRow);
      bud += value;
      if (cycle) cyclesSeen.add(cycle);
    }
  }
  const av = bud > 0 ? real / bud : NaN;
  const cycle = cyclesSeen.size === 1 ? [...cyclesSeen][0] : (cyclesSeen.size > 1 ? 'Mixto' : '');
  return { real, bud, av, cycle };
}

/** Calcula totales globales y por cuenta (Egresos/Activos/Capital). */
function bn_apComputeTotalsOnly() {
  const records = bn_kpiRecs(null);
  BN_AP_TOTALS.global      = bn_apAggregateGroup(records);
  BN_AP_TOTALS.operativo   = bn_apAggregateGroup(records.filter(r => bn_apMatchesCuenta(r, 'Egresos')));
  BN_AP_TOTALS.reinversion = bn_apAggregateGroup(records.filter(r => bn_apMatchesCuenta(r, 'Activos')));
  BN_AP_TOTALS.retiro      = bn_apAggregateGroup(records.filter(r => bn_apMatchesCuenta(r, 'Capital')));
}

// Estado del Análisis por partida — siempre se construye con los 4 niveles;
// la profundidad visible la controlan: (a) BN_AP_LEVEL (1-4) y (b) overrides
// manuales por click en el triangulito.
let BN_AP_LEVEL = 4; // default: completamente desglosado a Concepto
const BN_AP_OVERRIDES = new Map(); // path → bool (true=expandido manual, false=colapsado manual)
const BN_AP_LEVELS = ['_cuenta','_subcuenta','_categoria_gasto','_concepto'];
let BN_AP_PCT_MODE = 'abs'; // 'abs' = % vs total de la cuenta raíz; 'rel' = % vs nivel inmediato superior
const BN_AP_COLLAPSED = { er: false, bg: false }; // estado plegado por sección
let BN_AP_DATA_MODE = 'all'; // 'all' = todos; 'validated' = sólo _validado='Sí'
function bn_apToggleDataMode() {
  BN_AP_DATA_MODE = (BN_AP_DATA_MODE === 'all') ? 'validated' : 'all';
  bn_apApplyDataModeStyle();
  bn_renderAP();
}
function bn_apApplyDataModeStyle() {
  const btn = document.getElementById('bn-ap-data-toggle');
  if (!btn) return;
  if (BN_AP_DATA_MODE === 'validated') {
    btn.textContent = '✓ Sólo validados';
    btn.style.cssText = 'padding:8px 14px;border:1px solid #16a34a;background:#16a34a;color:#fff;border-radius:6px;cursor:pointer;font-size:12px;font-weight:700';
  } else {
    btn.textContent = '📋 Todos los registros';
    btn.style.cssText = 'padding:8px 14px;border:1px solid #475569;background:#475569;color:#fff;border-radius:6px;cursor:pointer;font-size:12px;font-weight:700';
  }
}
function bn_apToggleSection(key) {
  BN_AP_COLLAPSED[key] = !BN_AP_COLLAPSED[key];
  bn_renderAP();
}

/** Popup con la sumatoria por Subcuenta de una cuenta raíz (Egresos/Ingresos/Capital).
 *  Muestra Monto, barra de magnitud relativa y % del total. Botón "Detalles" lleva
 *  a Análisis por partida con la cuenta pre-expandida. */
function bn_kpiOpenSubcuentasPopup(cuentaRaiz) {
  const records = (typeof bn_kpiRecs === 'function' ? bn_kpiRecs(null) : BN_RAW)
    .filter(r => (r._cuenta || '') === cuentaRaiz);
  // Agrupar por subcuenta
  const map = new Map();
  for (const r of records) {
    const sub = (r._subcuenta || '(Sin subcuenta)').trim() || '(Sin subcuenta)';
    const monto = Math.abs(Number(r.Monto) || 0);
    if (!map.has(sub)) map.set(sub, { sub, total: 0, count: 0 });
    const e = map.get(sub); e.total += monto; e.count++;
  }
  const list = [...map.values()].sort((a, b) => b.total - a.total);
  const grand = list.reduce((s, x) => s + x.total, 0);
  const maxV  = list.reduce((m, x) => Math.max(m, x.total), 0) || 1;
  // Estilo del overlay
  let ov = document.getElementById('bn-kpi-pop-overlay');
  if (!ov) {
    ov = document.createElement('div');
    ov.id = 'bn-kpi-pop-overlay';
    ov.style.cssText = 'position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:9998;display:flex;align-items:center;justify-content:center;padding:20px';
    ov.onclick = (e) => { if (e.target === ov) ov.remove(); };
    document.body.appendChild(ov);
  }
  const rows = list.map(it => {
    const pct = grand > 0 ? (it.total / grand) * 100 : 0;
    const w   = (it.total / maxV) * 100;
    const montoColor = cuentaRaiz==='Ingresos'?'#16a34a':cuentaRaiz==='Egresos'?'#dc2626':'#1f2937';
    return `<tr style="border-bottom:1px solid #e2e8f0">
      <td style="padding:8px 10px;font-size:12px;color:#1f2937">${esc(it.sub)}</td>
      <td style="padding:8px 10px;min-width:260px">
        <div style="display:flex;align-items:center;gap:8px">
          <span style="font-weight:700;color:${montoColor};white-space:nowrap;min-width:90px;text-align:right">${bn_fmt$(it.total)}</span>
          <div style="position:relative;flex:1;height:10px;background:#e5e7eb;border-radius:5px;overflow:hidden">
            <div style="height:100%;width:${w.toFixed(1)}%;background:${montoColor};opacity:.75"></div>
          </div>
        </div>
      </td>
      <td style="padding:8px 10px;text-align:right;font-size:11px;color:#475569;font-weight:600">${pct.toFixed(1)}%</td>
      <td style="padding:8px 10px;text-align:right;font-size:11px;color:#94a3b8">${it.count}</td>
    </tr>`;
  }).join('');
  ov.innerHTML = `
    <div style="background:#fff;border-radius:14px;width:100%;max-width:680px;max-height:80vh;overflow:auto;padding:20px;box-shadow:0 20px 60px rgba(15,23,42,.4)" onclick="event.stopPropagation()">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
        <h3 style="margin:0;font-size:16px;color:#1f2937">${esc(cuentaRaiz)} — Detalle por Subcuenta</h3>
        <button onclick="document.getElementById('bn-kpi-pop-overlay').remove()"
                style="width:32px;height:32px;border:none;background:#f1f5f9;border-radius:50%;font-size:16px;cursor:pointer">✕</button>
      </div>
      <div style="font-size:12px;color:#64748b;margin-bottom:14px">
        Total: <b style="color:#1f2937">${bn_fmt$(grand)}</b> · ${records.length} movimientos · ${list.length} subcuentas
      </div>
      ${list.length ? `
      <table style="width:100%;border-collapse:collapse;background:#fff">
        <thead><tr style="background:#475569;color:#fff">
          <th style="padding:8px 10px;text-align:left;font-size:11px;text-transform:uppercase">Subcuenta</th>
          <th style="padding:8px 10px;text-align:left;font-size:11px;text-transform:uppercase">$ Monto</th>
          <th style="padding:8px 10px;text-align:right;font-size:11px;text-transform:uppercase">% Total</th>
          <th style="padding:8px 10px;text-align:right;font-size:11px;text-transform:uppercase">#Mov</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>` : `<div style="padding:30px;text-align:center;color:#94a3b8;font-size:13px">Sin movimientos en esta cuenta con los filtros actuales</div>`}
      <div style="margin-top:14px;display:flex;justify-content:flex-end;gap:8px">
        <button onclick="bn_kpiGoToAP('${esc(cuentaRaiz)}')"
                style="padding:8px 16px;border:none;background:#334155;color:#fff;border-radius:8px;font-weight:700;font-size:12px;cursor:pointer">
          📊 Detalles — Análisis por partida
        </button>
      </div>
    </div>`;
}

/** Cierra el popup KPI y navega a Análisis por partida con la cuenta visible. */
function bn_kpiGoToAP(cuentaRaiz) {
  document.getElementById('bn-kpi-pop-overlay')?.remove();
  try { bn_setCat('pres'); bn_setTipo('AP'); } catch(_) {}
  // Pre-expandir la cuenta raíz en el AP (best-effort)
  setTimeout(() => {
    if (typeof BN_AP_OVERRIDES !== 'undefined' && BN_AP_OVERRIDES) {
      BN_AP_OVERRIDES.set(cuentaRaiz, true);
      try { bn_renderAP(); } catch(_) {}
    }
  }, 60);
}

function bn_toggleKpiSection() {
  const body = document.getElementById('bn-kpi-row');
  const chev = document.getElementById('bn-kpi-toggle-chev');
  if (!body) return;
  const willHide = body.style.display !== 'none';
  body.style.display = willHide ? 'none' : '';
  if (chev) chev.textContent = willHide ? '▸ Mostrar' : '▼ Ocultar';
}

function bn_apTogglePctMode() {
  BN_AP_PCT_MODE = (BN_AP_PCT_MODE === 'abs') ? 'rel' : 'abs';
  bn_apApplyPctToggleStyle();
  bn_renderAP();
}

function bn_apApplyPctToggleStyle() {
  const btn = document.getElementById('bn-ap-pct-toggle');
  if (!btn) return;
  if (BN_AP_PCT_MODE === 'abs') {
    btn.textContent = '% Absoluto';
    btn.style.cssText = 'padding:8px 14px;border:1px solid #2563eb;background:#2563eb;color:#fff;border-radius:6px;cursor:pointer;font-size:12px;font-weight:700';
  } else {
    btn.textContent = '% Relativo';
    btn.style.cssText = 'padding:8px 14px;border:1px solid #ea580c;background:#ea580c;color:#fff;border-radius:6px;cursor:pointer;font-size:12px;font-weight:700';
  }
}

/** Cambia el nivel de desglose; limpia overrides manuales. */
function bn_setApLevel(level) {
  BN_AP_LEVEL = Math.max(1, Math.min(4, Number(level) || 4));
  BN_AP_OVERRIDES.clear();
  bn_renderAP();
}

/** ¿Los hijos del nodo en 'path' (con depth dado) deben mostrarse? */
function bn_apIsExpanded(path, depth) {
  if (BN_AP_OVERRIDES.has(path)) return BN_AP_OVERRIDES.get(path);
  // depth = profundidad del nodo (0-indexed); hijos en depth+1.
  // Si BN_AP_LEVEL = 2 (Subcuenta), nivel 1 visible (depth 0).
  // El nodo en depth 0 expande hijos sólo si BN_AP_LEVEL > 1.
  return (depth + 1) < BN_AP_LEVEL;
}

/** Click en el triángulo — alterna expansión manual de un nodo. */
function bn_apToggleNode(pathEnc) {
  const path = decodeURIComponent(pathEnc);
  // Extraer depth del path: número de pipes
  const depth = path.split('|').length - 1;
  const wasExpanded = bn_apIsExpanded(path, depth);
  BN_AP_OVERRIDES.set(path, !wasExpanded);
  bn_renderAP();
}

/** Renderiza recursivamente las filas con indentación, respetando estado de expansión. */
function bn_apRenderNode(node, depth, records, ancestors, rows, rootTotal, parentTotal) {
  const path = ancestors.join('|');
  const pathEnc = encodeURIComponent(path);
  const hasChildren = node.children.size > 0;
  const expanded = hasChildren && bn_apIsExpanded(path, depth);

  // % Total: vs total de la cuenta raíz (jerárquicamente consistente)
  const pctTotalVal = (rootTotal && Math.abs(rootTotal) > 0) ? (node.total / rootTotal) : NaN;
  const pctTotalTxt = isFinite(pctTotalVal) ? (pctTotalVal*100).toFixed(1) + '%' : '—';
  // % Relativo: respecto al padre inmediato
  const pctRelVal = (parentTotal && Math.abs(parentTotal) > 0) ? (node.total / parentTotal) : NaN;
  let pctRelTxt;
  if (hasChildren && isFinite(pctRelVal)) {
    // Afuera de los paréntesis: % absoluto vs padre.
    // Dentro de los paréntesis: 100% (suma de hijos).
    pctRelTxt = `${(pctRelVal*100).toFixed(1)}% <span style="font-size:9px;color:#94a3b8;font-weight:500">(100%)</span>`;
  } else {
    pctRelTxt = isFinite(pctRelVal) ? (pctRelVal*100).toFixed(1) + '%' : '—';
  }

  const { value: bud, cycle: budCycle } = bn_apComputeBudget(node, records, ancestors, BN_AP_LEVELS);
  const av = bud > 0 ? node.total / bud : NaN;
  const color = !isFinite(av) ? '#9ca3af' : av > 1.10 ? '#dc2626' : av > 1.0 ? '#f59e0b' : '#16a34a';
  const semIcon = !isFinite(av) ? '—' : av > 1.10 ? '🔴' : av > 1.0 ? '🟡' : '🟢';
  const pctTxt  = isFinite(av) ? (av * 100).toFixed(1) + '%' : '—';
  const fillW   = isFinite(av) ? Math.min(av, 2) / 2 * 100 : 0;
  // Sombreado por jerarquía: cuanto más profundo, más claro. Texto contrasta.
  const bgRow   = depth === 0 ? '#475569' : depth === 1 ? '#94a3b8' : depth === 2 ? '#cbd5e1' : depth === 3 ? '#e2e8f0' : '#f8fafc';
  const txtRow  = depth === 0 ? '#fff'    : depth === 1 ? '#fff'    : depth === 2 ? '#1f2937' : '#1f2937';
  const mutedTx = depth <= 1 ? 'rgba(255,255,255,.85)' : '#475569';
  const wgt   = depth === 0 ? '800' : depth === 1 ? '700' : '600';
  const indent = depth * 22;
  const triangle = hasChildren
    ? `<span onclick="event.stopPropagation();bn_apToggleNode('${pathEnc}')"
            style="color:#334155;cursor:pointer;flex-shrink:0;width:14px;text-align:center;user-select:none">${expanded ? '▼' : '▸'}</span>`
    : `<span style="flex-shrink:0;width:14px"></span>`;

  const pathEncForClick = encodeURIComponent(JSON.stringify({ ancestors, levels: BN_AP_LEVELS.slice(0, ancestors.length), name: node.name }));
  // Fondo aplicado en cada <td> para que gane sobre cualquier CSS global
  const tdBg = `background:${bgRow};`;
  rows.push(`
    <tr class="bn-ap-row" data-depth="${depth}" style="background:${bgRow};cursor:pointer" onclick="bn_apOpenRecordsModal('${pathEncForClick}')"
        onmouseover="this.querySelectorAll('td').forEach(c => c.style.filter='brightness(.95)')" onmouseout="this.querySelectorAll('td').forEach(c => c.style.filter='')">
      <td style="${tdBg}padding:8px 10px;border-bottom:1px solid #f3f4f6;padding-left:${10+indent}px;font-weight:${wgt};font-size:${depth===0?'13':'12'}px;max-width:380px;color:${txtRow}">
        <div style="display:flex;align-items:center;gap:6px;min-width:0">
          ${triangle}
          <span title="${esc(node.name)}" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;min-width:0">${esc(node.name)}</span>
        </div>
      </td>
      <td style="${tdBg}padding:8px 10px;border-bottom:1px solid #f3f4f6;text-align:right;font-size:11px;color:${mutedTx}">${node.count}</td>
      <td style="${tdBg}padding:8px 10px;border-bottom:1px solid #f3f4f6;text-align:right;font-weight:${wgt};font-size:12px;color:${txtRow}">${bn_fmt$(node.total)}</td>
      <td style="${tdBg}padding:8px 10px;border-bottom:1px solid #f3f4f6;text-align:right;font-size:11px;font-weight:${wgt};color:${depth<=1?'#bfdbfe':'#2563eb'}" title="% del total de la cuenta raíz">${pctTotalTxt}</td>
      <td style="${tdBg}padding:8px 10px;border-bottom:1px solid #f3f4f6;text-align:right;font-size:11px;font-weight:${wgt};color:${depth<=1?'#fed7aa':'#ea580c'};white-space:nowrap" title="% del nivel superior inmediato">${pctRelTxt}</td>
      <td style="${tdBg}padding:8px 10px;border-bottom:1px solid #f3f4f6;text-align:right;font-size:11px;color:${mutedTx}">${bud > 0 ? bn_fmt$(bud) : '—'}</td>
      <td style="${tdBg}padding:8px 10px;border-bottom:1px solid #f3f4f6;text-align:center;font-size:11px;font-weight:600;color:${budCycle?(depth<=1?'#fff':'#334155'):mutedTx}">${budCycle || '—'}</td>
      <td style="padding:8px 10px;border-bottom:1px solid #f3f4f6;min-width:140px">
        ${bud > 0 ? `
          <div style="display:flex;align-items:center;gap:6px">
            <div style="position:relative;flex:1;height:6px;background:#e5e7eb;border-radius:3px;overflow:hidden">
              <div style="height:100%;width:${fillW.toFixed(1)}%;background:${color}"></div>
              <div style="position:absolute;top:-1px;bottom:-1px;left:50%;width:1.5px;background:#1f2937"></div>
            </div>
            <span style="font-size:11px;font-weight:700;color:${color};min-width:46px;text-align:right">${pctTxt}</span>
          </div>` : '<span style="font-size:11px;color:#9ca3af">—</span>'}
      </td>
      <td style="padding:8px 10px;border-bottom:1px solid #f3f4f6;text-align:center;font-size:14px">${semIcon}</td>
    </tr>`);

  if (expanded) {
    const kids = [...node.children.values()].sort((a,b) => b.total - a.total);
    // Para hijos: rootTotal sigue siendo el de la cuenta raíz; parentTotal pasa a ser node.total
    const newRoot = (depth === 0) ? node.total : rootTotal;
    for (const child of kids) {
      bn_apRenderNode(child, depth + 1, records, [...ancestors, child.name], rows, newRoot, node.total);
    }
  }
}

/** Abre el modal con los registros que coinciden con la partida (ancestros). */
function bn_apOpenRecordsModal(pathEnc) {
  let info;
  try { info = JSON.parse(decodeURIComponent(pathEnc)); } catch(_) { return; }
  const { ancestors, levels, name } = info;

  // Filtrar BN_RAW (vía kpiRecs para mantener Año/Mes/multi-selects)
  const all = bn_kpiRecs(null);
  const matching = all.filter(r => {
    for (let i = 0; i < ancestors.length; i++) {
      const lvl = levels[i];
      const v = String(r[lvl] || '').trim() || '(Sin asignar)';
      if (v !== ancestors[i]) return false;
    }
    return true;
  });

  // Orden desc por monto absoluto
  matching.sort((a,b) => Math.abs(Number(b.Monto)||0) - Math.abs(Number(a.Monto)||0));

  // Max absoluto para escalar la barra de magnitud
  const maxAbs = matching.reduce((m,r) => Math.max(m, Math.abs(Number(r.Monto)||0)), 0) || 1;

  // Encabezados de la tabla
  const cols = [
    { label: 'Cuenta',        w: 110 },
    { label: 'Subcuenta',     w: 140 },
    { label: 'Categoría',     w: 130 },
    { label: 'Concepto',      w: 130 },
    { label: 'Descripción',   w: 240 },
    { label: 'Día',           w: 90  },
    { label: 'Cuenta bancaria', w: 150 },
    { label: 'Monto',         w: 110, num: true },
    { label: 'Magnitud',      w: 160 },
    { label: 'Factura',       w: 100 },
    { label: 'Encargado',     w: 120 },
    { label: 'Reembolso',     w: 100 },
    { label: 'Método pago',   w: 130 },
    { label: 'Comentarios',   w: 200 },
    { label: 'Duda',          w: 70  },
  ];

  const thead = cols.map(c =>
    `<th style="padding:9px 10px;text-align:${c.num?'right':'left'};font-size:11px;text-transform:uppercase;letter-spacing:.04em;background:#475569;color:#fff;min-width:${c.w}px;white-space:nowrap">${esc(c.label)}</th>`
  ).join('');

  const totalMonto = matching.reduce((s,r) => s + Math.abs(Number(r.Monto)||0), 0);

  const tbody = matching.map(r => {
    const monto = Number(r.Monto) || 0;
    const absM = Math.abs(monto);
    const pct = (absM / maxAbs) * 100;
    const isNeg = monto < 0;
    const barColor = isNeg ? '#dc2626' : '#16a34a';
    const desc = r.DESCRIPCION || '';
    const dia  = bn_formatDia ? bn_formatDia(r.Día || r.Dia || '') : (r.Día || r.Dia || '');
    const cta  = r['Cuenta bancaria'] || '';
    const fac  = r.FacturaFlag || r.Factura || '';
    const enc  = r._encargado || '';
    const reem = r._reembolso || '';
    const mp   = r._metodo_pago || '';
    const com  = r._comentarios || '';
    const duda = r._duda === 'Sí' ? '<span title="Marcado: Duda" style="display:inline-block;width:22px;height:22px;border-radius:50%;background:#fef3c7;color:#b45309;font-weight:900;line-height:22px;text-align:center">?</span>' : '';
    const cu = r._cuenta || '', su = r._subcuenta || '', ca = r._categoria_gasto || '', co = r._concepto || '';
    return `<tr>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#475569;max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(cu)}">${esc(cu)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#475569;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(su)}">${esc(su)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#475569;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(ca)}">${esc(ca)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#475569;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(co)}">${esc(co)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(desc)}">${esc(desc)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#64748b">${esc(dia)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#64748b">${esc(cta)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;text-align:right;font-weight:700;color:${isNeg?'#dc2626':'#16a34a'}">${bn_fmt$(monto)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0">
        <div style="position:relative;width:140px;height:8px;background:#f1f5f9;border-radius:4px;overflow:hidden">
          <div style="height:100%;width:${pct.toFixed(1)}%;background:${barColor};transition:width .3s"></div>
        </div>
      </td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:11px;color:${fac && fac !== 'Sin factura' ? '#16a34a':'#94a3b8'}">${esc(fac || '—')}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569">${esc(enc)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569">${esc(reem)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569">${esc(mp)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#475569;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(com)}">${esc(com)}</td>
      <td style="padding:7px 10px;border-bottom:1px solid #e2e8f0;text-align:center">${duda}</td>
    </tr>`;
  }).join('');

  document.getElementById('bn-ap-records-title').textContent =
    `📋 Registros — ${name}`;
  document.getElementById('bn-ap-records-sub').textContent =
    `${matching.length} movimiento${matching.length===1?'':'s'} · Total ${bn_fmt$(totalMonto)} · Ruta: ${ancestors.join(' › ')}`;
  document.getElementById('bn-ap-records-body').innerHTML = matching.length
    ? `<table style="width:100%;border-collapse:collapse;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(15,23,42,.05);font-size:12px">
         <thead><tr>${thead}</tr></thead>
         <tbody>${tbody}</tbody>
       </table>`
    : `<div style="padding:30px;text-align:center;color:#94a3b8;font-size:13px">Sin registros bajo esta partida con los filtros actuales</div>`;

  document.getElementById('bn-ap-records-overlay').classList.remove('hidden');
  document.body.style.overflow = 'hidden';
}

function bn_closeApRecordsModal() {
  document.getElementById('bn-ap-records-overlay')?.classList.add('hidden');
  document.body.style.overflow = '';
}

function bn_apRecordsOverlayClick(e) {
  if (e.target && e.target.id === 'bn-ap-records-overlay') bn_closeApRecordsModal();
}

// Esc cierra el modal de registros AP
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    const ov = document.getElementById('bn-ap-records-overlay');
    if (ov && !ov.classList.contains('hidden')) bn_closeApRecordsModal();
  }
});

/** Renderiza un KPI tipo 'AP' con monto real, barra/semáforo y sub-texto
 *  'Real $X / $Y  🟢/🟡/🔴'. Usa los IDs de la KPI card pasados como argumentos. */
function bn_renderApKpi(valId, barId, subId, totals) {
  const { real = 0, bud = 0, av = NaN } = totals || {};
  const color = !isFinite(av) ? '#9ca3af' : av > 1.10 ? '#dc2626' : av > 1.0 ? '#f59e0b' : '#16a34a';
  const sem   = !isFinite(av) ? '' : av > 1.10 ? '🔴' : av > 1.0 ? '🟡' : '🟢';
  const fillW = isFinite(av) ? Math.min(av, 2) / 2 * 100 : 0;
  bn_txt(valId, bn_fmt$(real));
  bn_txt(subId, 'Real ' + bn_fmt$(real) + ' / ' + bn_fmt$(bud) + (sem ? '  ' + sem : ''));
  const barEl = document.getElementById(barId);
  if (barEl) {
    if (bud > 0) {
      const pctTxt = isFinite(av) ? bn_fmtPct(av) : '—';
      barEl.innerHTML = `
        <div style="display:flex;align-items:center;gap:6px">
          <div style="position:relative;flex:1;height:7px;background:#e5e7eb;border-radius:4px;overflow:hidden">
            <div style="height:100%;width:${fillW.toFixed(1)}%;background:${color};transition:width .3s"></div>
            <div style="position:absolute;top:-1px;bottom:-1px;left:50%;width:1.5px;background:#1f2937"></div>
          </div>
          <span style="font-size:11px;font-weight:700;color:${color};min-width:46px;text-align:right">${pctTxt}</span>
        </div>`;
    } else {
      barEl.innerHTML = '';
    }
  }
  // Color tipo 'val-good/warn/bad' en el valor monto
  const v = document.getElementById(valId);
  if (v) {
    v.className = 'bn-kpi-value';
    if (isFinite(av)) {
      if (av > 1.10) v.classList.add('bn-val-bad');
      else if (av > 1.0) v.classList.add('bn-val-warn');
      else v.classList.add('bn-val-good');
    }
  }
}

/** Construye una fila de pie con: nombre, count, monto, %, bud, cycle, barra y sem. */
function bn_apFooterRow(name, count, real, bud, cycle, av, bg, sharePct) {
  const color = !isFinite(av) ? '#9ca3af' : av > 1.10 ? '#dc2626' : av > 1.0 ? '#f59e0b' : '#16a34a';
  const sem   = !isFinite(av) ? '—' : av > 1.10 ? '🔴' : av > 1.0 ? '🟡' : '🟢';
  const pct   = isFinite(av) ? (av*100).toFixed(1) + '%' : '—';
  const fillW = isFinite(av) ? Math.min(av, 2) / 2 * 100 : 0;
  const shareTxt = (sharePct == null) ? '100.0%' : (isFinite(sharePct) ? (sharePct*100).toFixed(1) + '%' : '—');
  return `<tr style="background:${bg};color:#fff;font-weight:800"
    onmouseover="this.style.filter='brightness(1.25)'" onmouseout="this.style.filter=''">
    <td style="padding:10px">${esc(name)}</td>
    <td style="padding:10px;text-align:right">${count != null ? count : ''}</td>
    <td style="padding:10px;text-align:right">${bn_fmt$(real || 0)}</td>
    <td style="padding:10px;text-align:right;color:#e2e8f0">${shareTxt}</td>
    <td style="padding:10px;text-align:right;color:#e2e8f0">100%</td>
    <td style="padding:10px;text-align:right">${bud > 0 ? bn_fmt$(bud) : '—'}</td>
    <td style="padding:10px;text-align:center;font-size:11px;color:${cycle?'#bfdbfe':'#cbd5e1'}">${cycle || '—'}</td>
    <td style="padding:10px">
      ${bud > 0 ? `
        <div style="display:flex;align-items:center;gap:6px">
          <div style="position:relative;flex:1;height:7px;background:rgba(255,255,255,.18);border-radius:4px;overflow:hidden">
            <div style="height:100%;width:${fillW.toFixed(1)}%;background:${color}"></div>
            <div style="position:absolute;top:-1px;bottom:-1px;left:50%;width:1.5px;background:#fff"></div>
          </div>
          <span style="font-size:11px;font-weight:700;color:${color};min-width:50px;text-align:right">${pct}</span>
        </div>` : '—'}
    </td>
    <td style="padding:10px;text-align:center;font-size:14px">${sem}</td>
  </tr>`;
}

function bn_apFooterRowGroup(name, totals, bg, globalTotal) {
  const share = (globalTotal && Math.abs(globalTotal) > 0) ? (totals.real / globalTotal) : NaN;
  return bn_apFooterRow(name, null, totals.real, totals.bud, totals.cycle, totals.av, bg, share);
}

/** Renderiza la tabla de Análisis por partida. */
function bn_renderAP() {
  const wrap = document.getElementById('bn-ap-table');
  if (!wrap) return;

  // Sincronizar el dropdown con el nivel activo
  const lvlSel = document.getElementById('bn-ap-level-select');
  if (lvlSel) lvlSel.value = String(BN_AP_LEVEL);
  bn_apApplyDataModeStyle();

  // Usar los registros del período; si está activo "Sólo validados", filtra _validado='Sí'
  let records = bn_kpiRecs(null);
  if (BN_AP_DATA_MODE === 'validated') {
    records = records.filter(r => r._validado === 'Sí');
  }
  if (!records.length) {
    wrap.innerHTML = `<div style="padding:30px;text-align:center;color:#9ca3af;font-size:13px">
      Sin registros para los filtros seleccionados
    </div>`;
    return;
  }

  // Árbol con los 4 niveles siempre; la visibilidad se controla por expansión
  const tree = bn_apBuildTree(records, BN_AP_LEVELS);

  const ER_NAMES = new Set(['Egresos', 'Ingresos']);
  const BG_NAMES = new Set(['Activos', 'Pasivos', 'Capital']);
  const top = [...tree.children.values()].sort((a,b) => b.total - a.total);
  const erNodes = top.filter(n => ER_NAMES.has(n.name));
  const bgNodes = top.filter(n => BG_NAMES.has(n.name));
  const otherNodes = top.filter(n => !ER_NAMES.has(n.name) && !BG_NAMES.has(n.name));

  // Total global: suma de presupuesto único por (cat, con) entre TODOS los registros (igual que antes)
  const seen = new Set();
  let budTotal = 0;
  const cyclesSeen = new Set();
  for (const r of records) {
    const cat = bn_norm(r.CATEGORIA || r._categoria_gasto || '');
    const con = bn_norm(r.CONCEPTO  || r._concepto || '');
    const k = cat + '||' + con;
    if (seen.has(k)) continue;
    seen.add(k);
    const budRow = (BN_BUDGET || []).find(b =>
      bn_norm(b.CATEGORIA || '') === cat && bn_norm(b.CONCEPTO || '') === con);
    if (budRow) {
      const { value, cycle } = bn_apBudgetForRow(budRow);
      budTotal += value;
      if (cycle) cyclesSeen.add(cycle);
    }
  }
  const totalCycleLabel = cyclesSeen.size === 1 ? [...cyclesSeen][0] : (cyclesSeen.size > 1 ? 'Mixto' : '');

  bn_apComputeTotalsOnly();

  const header = `
    <thead>
      <tr style="background:#374151;color:#fff">
        <th style="padding:10px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.04em">Partida</th>
        <th style="padding:10px;text-align:right;font-size:11px;text-transform:uppercase;letter-spacing:.04em">#Mov</th>
        <th style="padding:10px;text-align:right;font-size:11px;text-transform:uppercase;letter-spacing:.04em">Monto</th>
        <th style="padding:10px;text-align:right;font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:#2563eb;background:#dbeafe" title="% del total de la cuenta raíz">% Total</th>
        <th style="padding:10px;text-align:right;font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:#ea580c;background:#fed7aa" title="% del nivel superior inmediato">% Relativo</th>
        <th style="padding:10px;text-align:right;font-size:11px;text-transform:uppercase;letter-spacing:.04em">Presupuesto</th>
        <th style="padding:10px;text-align:center;font-size:11px;text-transform:uppercase;letter-spacing:.04em">Ciclo</th>
        <th style="padding:10px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.04em">Avance</th>
        <th style="padding:10px;text-align:center;font-size:11px;text-transform:uppercase;letter-spacing:.04em">Sem.</th>
      </tr>
    </thead>`;

  // Renderiza un grupo plegable (drop-down) con su propia tabla.
  function renderGroup(nodes, title, opts) {
    if (!nodes.length) return '';
    const key = opts.key;
    const collapsed = !!BN_AP_COLLAPSED[key];
    const rows = [];
    let groupTotal = 0, groupCount = 0;
    for (const node of nodes) {
      bn_apRenderNode(node, 0, records, [node.name], rows, node.total, tree.total);
      groupTotal += node.total;
      groupCount += node.count;
    }
    let footerHtml = '';
    if (opts.showTotal) {
      const totalAv = budTotal > 0 ? groupTotal / budTotal : NaN;
      footerHtml = `<tfoot>${bn_apFooterRow('TOTAL GLOBAL', groupCount, groupTotal, budTotal, totalCycleLabel, totalAv, '#1f2937', null)}</tfoot>`;
    }
    const tableHtml = collapsed ? '' : `
      <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;background:var(--surface,#fff);border-radius:10px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);font-size:12px;min-width:780px">
          ${header}
          <tbody>${rows.join('')}</tbody>
          ${footerHtml}
        </table>
      </div>`;
    const hdrStyles = {
      er:    { bg:'linear-gradient(180deg,#1e3a8a,#1e40af)', color:'#fff', sub:'#bfdbfe' },
      bg:    { bg:'linear-gradient(180deg,#065f46,#047857)', color:'#fff', sub:'#bbf7d0' },
      other: { bg:'#f1f5f9', color:'#334155', sub:'#64748b' },
    };
    const hdr = hdrStyles[key] || hdrStyles.other;
    return `
      <div style="margin-bottom:18px;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;background:#fff">
        <div onclick="bn_apToggleSection('${key}')"
             style="display:flex;align-items:center;gap:8px;padding:10px 14px;background:${hdr.bg};cursor:pointer;user-select:none">
          <span style="font-size:13px;font-weight:800;color:${hdr.color};flex:1">${title}</span>
          <span style="font-size:11px;color:${hdr.sub}">${collapsed ? '▸ Mostrar' : '▼ Ocultar'}</span>
        </div>
        ${collapsed ? '' : `<div style="padding:10px">${tableHtml}</div>`}
      </div>`;
  }

  wrap.innerHTML =
    renderGroup(erNodes,    '📈 Estado de Resultados', { showTotal: true,  key: 'er' }) +
    renderGroup(bgNodes,    '📊 Balance General',      { showTotal: false, key: 'bg' }) +
    (otherNodes.length ? renderGroup(otherNodes, '🗂️ Otras cuentas', { showTotal: false, key: 'other' }) : '');
}

/** Panel de estado de revisión — barra stacked + notas, sólo en tabs Por Clasificar. */
function bn_renderReviewPanel() {
  const panel = document.getElementById('bn-review-panel');
  if (!panel) return;
  if (!bn_isPC(BN_TIPO)) { panel.classList.add('hidden'); return; }

  // Universo: kpiRecs (mismo Año/Mes/multi-selects/búsqueda) filtrado por sub-tipo si aplica
  const sub = bn_subOfPC(BN_TIPO);
  const universe = (sub && sub !== 'T') ? bn_kpiRecs(sub) : bn_kpiRecs(null);
  const total = universe.length;
  const noRev = universe.filter(r => r._validado !== 'Sí').length;
  const rev   = total - noRev;
  const pctNoRev = total > 0 ? (noRev / total * 100) : 0;
  const pctRev   = total > 0 ? (rev   / total * 100) : 0;

  panel.classList.remove('hidden');
  panel.innerHTML = `
    <div style="display:flex;align-items:center;gap:10px;padding:8px 12px;background:#f8fafc;border:1px solid #cbd5e1;border-radius:10px">
      <div style="display:flex;align-items:center;gap:6px;font-size:12px;font-weight:700;color:#14532d;white-space:nowrap">
        <span style="width:9px;height:9px;border-radius:50%;background:#16a34a;display:inline-block"></span>
        ${rev} (${pctRev.toFixed(0)}%) Validados
      </div>
      <div style="flex:1;display:flex;height:12px;border-radius:999px;overflow:hidden;background:#e5e7eb">
        <div title="Validados: ${rev}" style="width:${pctRev.toFixed(2)}%;background:linear-gradient(90deg,#16a34a,#86efac);transition:width .3s"></div>
        <div title="Pendientes: ${noRev}" style="width:${pctNoRev.toFixed(2)}%;background:linear-gradient(90deg,#fca5a5,#dc2626);transition:width .3s"></div>
      </div>
      <div style="display:flex;align-items:center;gap:6px;font-size:12px;font-weight:700;color:#7f1d1d;white-space:nowrap">
        Pendientes ${noRev} (${pctNoRev.toFixed(0)}%)
        <span style="width:9px;height:9px;border-radius:50%;background:#dc2626;display:inline-block"></span>
      </div>
    </div>`;
}

/** Registros para KPIs del período: aplica Año/Mes/búsqueda/multi-selects
 *  pero NO restringe por revisado ni por tipo-tab. */
function bn_kpiRecs(subtype) {
  const s = bn_st;
  const q = (s.q || '').toLowerCase().trim();
  return BN_RAW.filter(r => {
    const t = bn_canon(r._tipo || '');
    if (subtype === 'E'  && !t.includes('egr'))     return false;
    if (subtype === 'I'  && !t.includes('ing'))     return false;
    if (subtype === 'AC' && !t.includes('activ'))   return false;
    if (subtype === 'PA' && !t.includes('pasiv'))   return false;
    if (subtype === 'CA' && !t.includes('capital')) return false;
    // Multi-selects del hamburguesa (excepto 'dia' que ahora es rango)
    for (const f of BN_MSEL_FIELDS) {
      if (f.key === 'dia') continue;
      const arr = s[f.key];
      if (arr?.length) {
        const val = (f.from(r) || '').toString().trim();
        if (!arr.includes(val)) return false;
      }
    }
    // Rango de fechas
    if (s.dia_desde || s.dia_hasta) {
      const isoR = bn_formatDiaISO(r.Día || r.Dia || '').slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(isoR)) return false;
      if (s.dia_desde && isoR < s.dia_desde) return false;
      if (s.dia_hasta && isoR > s.dia_hasta) return false;
    }
    // Año / Mes
    if ((s.anio?.length) || (s.mes?.length)) {
      const iso = bn_formatDiaISO(r.Día || r.Dia || '');
      const m   = iso.match(/^(\d{4})-(\d{2})/);
      if (!m) return false;
      if (s.anio?.length && !s.anio.includes(m[1])) return false;
      if (s.mes?.length  && !s.mes .includes(m[2])) return false;
    }
    if (q) {
      const words = q.split(/\s+/).filter(Boolean);
      const h = [bn_norm(r.SUBCUENTA||''), bn_norm(r.CATEGORIA||''), bn_norm(r.CONCEPTO||''),
                 bn_norm(r.DESCRIPCION||''), bn_norm(r['Cuenta bancaria']||''),
                 bn_norm(r._cuenta||''), bn_norm(r._encargado||''),
                 bn_norm(r._propiedad||''), bn_norm(r._metodo_pago||'')].join(' ').toLowerCase();
      if (!words.every(w => h.includes(w))) return false;
    }
    return true;
  });
}

function bn_recsForTipo(tipo) {
  const isPC = bn_isPC(tipo);
  const sub  = isPC ? bn_subOfPC(tipo) : null;
  return BN_RAW.filter(r => {
    const t = bn_canon(r._tipo || '');
    const revisado = r._validado === 'Sí';
    if (isPC) {
      if (revisado) return false;
      if (sub === 'E'  && !t.includes('egr'))     return false;
      if (sub === 'I'  && !t.includes('ing'))     return false;
      if (sub === 'AC' && !t.includes('activ'))   return false;
      if (sub === 'PA' && !t.includes('pasiv'))   return false;
      if (sub === 'CA' && !t.includes('capital')) return false;
      return true;
    }
    if (['T','E','I','AC','PA','CA'].includes(tipo)) {
      if (!revisado) return false;
    }
    if (tipo === 'E'  && !t.includes('egr'))     return false;
    if (tipo === 'I'  && !t.includes('ing'))     return false;
    if (tipo === 'AC' && !t.includes('activ'))   return false;
    if (tipo === 'PA' && !t.includes('pasiv'))   return false;
    if (tipo === 'CA' && !t.includes('capital')) return false;
    return true;
  });
}

// Construye / re-construye los multi-selects del panel + Año/Mes.
function bn_populateFilters() {
  // Fuente de datos: registros del tipo activo
  const recs = (typeof BN_TIPO !== 'undefined' && BN_TIPO) ? bn_recsForTipo(BN_TIPO) : BN_RAW;

  // Construir grid de multi-selects del panel
  const grid = document.getElementById('bn-msel-grid');
  if (grid && !grid.dataset.built) {
    grid.innerHTML = BN_MSEL_FIELDS.map(f => `
      <div class="bn-msel" data-field="${f.key}" style="position:relative">
        <label style="display:block;font-size:11px;font-weight:700;color:var(--text-soft);text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px">${esc(f.label)}</label>
        <button class="bn-msel-trigger" onclick="bn_mselToggle('${f.key}',event)" type="button"
                style="width:100%;padding:8px 10px;border:1px solid var(--border,#e5e7eb);border-radius:6px;background:#fff;cursor:pointer;text-align:left;font-size:12px;display:flex;justify-content:space-between;align-items:center">
          <span class="bn-msel-label" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">Todos</span>
          <span style="font-size:10px;color:var(--text-soft);margin-left:6px">▼</span>
        </button>
        <div class="bn-msel-panel hidden" id="bn-msel-panel-${f.key}"
             style="position:absolute;top:calc(100% + 2px);left:0;right:0;background:#fff;border:1px solid var(--border,#e5e7eb);border-radius:6px;box-shadow:0 6px 20px rgba(0,0,0,.12);max-height:260px;overflow-y:auto;z-index:90;padding:6px;min-width:170px"></div>
      </div>`).join('');
    grid.dataset.built = '1';
  }

  // Poblar opciones de cada multi-select del panel
  for (const f of BN_MSEL_FIELDS) {
    let values = [...new Set(recs.map(r => (f.from(r) || '').toString().trim()).filter(v => v))];
    // Orden: numérico para Día y Departamento, alfabético para el resto
    if (f.key === 'dia' || f.key === 'departamento') {
      values.sort((a,b) => Number(a) - Number(b));
    } else {
      values.sort((a,b) => a.localeCompare(b, 'es'));
    }
    bn_mselFillOptions(f.key, values);
  }

  // Año y Mes son GLOBALES — derivan de BN_RAW (no del tipo activo) para que
  // los defaults (año/mes en curso) siempre estén disponibles como opción.
  // Año (multi)
  {
    const years = new Set();
    for (const r of BN_RAW) {
      const iso = bn_formatDiaISO(r.Día || r.Dia || '');
      if (/^\d{4}-/.test(iso)) years.add(iso.substring(0, 4));
    }
    // Asegurar año en curso aunque no haya registros aún
    years.add(String(new Date().getFullYear()));
    const sorted = [...years].sort((a,b) => b.localeCompare(a));
    bn_mselFillOptions('anio', sorted);
  }

  // Mes (multi) — claves "01".."12", label nombre español
  {
    const NOMBRES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
                     'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
    const months = new Set();
    for (const r of BN_RAW) {
      const iso = bn_formatDiaISO(r.Día || r.Dia || '');
      const m = iso.match(/^\d{4}-(\d{2})/);
      if (m) months.add(m[1]);
    }
    // Mostrar siempre los 12 meses para que el mes en curso esté disponible
    for (let i = 1; i <= 12; i++) months.add(String(i).padStart(2,'0'));
    const sorted = [...months].sort();
    bn_mselFillOptions('mes', sorted, mm => NOMBRES[Number(mm)-1]);
  }
}

/** Rellena las opciones de un multi-select con diseño simple tipo lista de checks. */
function bn_mselFillOptions(field, options, labelFn) {
  const panel = document.getElementById(`bn-msel-panel-${field}`);
  if (!panel) return;

  // Caso especial: 'dia' usa un selector de rango de fechas (calendario)
  if (field === 'dia') {
    return bn_mselRenderDiaCalendar(panel);
  }

  const sel = bn_st[field] || [];
  // Filtrar selecciones que ya no estén disponibles en el contexto actual
  bn_st[field] = sel.filter(v => options.includes(v));

  if (!options.length) {
    panel.innerHTML = `<div style="padding:10px;font-size:12px;color:#9ca3af;text-align:center">Sin opciones</div>`;
    panel.dataset.labels = '{}';
    bn_mselUpdateTrigger(field);
    return;
  }

  const isNumeric = (field === 'dia' || field === 'departamento' || field === 'anio' || field === 'ind_anio');
  const header = `
    <div style="padding:8px;border-bottom:1px solid #e5e7eb;background:#f9fafb;border-radius:6px 6px 0 0">
      <div style="display:flex;gap:6px;margin-bottom:${isNumeric ? '0' : '6px'}">
        <button type="button" onclick="bn_mselSelectAll('${field}')"
                style="flex:1;padding:6px 10px;border:none;background:#334155;color:#fff;font-weight:600;font-size:12px;border-radius:5px;cursor:pointer">
          ✓ Todos
        </button>
        <button type="button" onclick="bn_mselClear('${field}')"
                style="flex:1;padding:6px 10px;border:1px solid #d1d5db;background:#fff;color:#374151;font-weight:600;font-size:12px;border-radius:5px;cursor:pointer">
          ✕ Limpiar
        </button>
      </div>
      ${isNumeric ? '' : `<input type="text" placeholder="🔍 Buscar opciones..."
             oninput="bn_mselFilterOpts('${field}', this.value)"
             onclick="event.stopPropagation()"
             style="width:100%;padding:6px 10px;border:1px solid #d1d5db;border-radius:5px;font-size:12px;box-sizing:border-box">`}
    </div>`;

  const items = options.map(opt => {
    const lbl = labelFn ? labelFn(opt) : opt;
    const isSel = bn_st[field].includes(opt);
    return `<div onclick="bn_mselToggleOpt('${field}', this)"
                 data-value="${esc(opt)}"
                 data-selected="${isSel}"
                 style="display:flex;align-items:center;gap:10px;padding:8px 12px;cursor:pointer;font-size:13px;color:${isSel?'#334155':'#374151'};background:${isSel?'#f1f5f9':'transparent'};font-weight:${isSel?'600':'400'};border-bottom:1px solid #f3f4f6"
                 onmouseover="if(this.dataset.selected!=='true')this.style.background='#f3f4f6'"
                 onmouseout="if(this.dataset.selected!=='true')this.style.background='transparent'">
              <span style="display:inline-flex;align-items:center;justify-content:center;width:18px;height:18px;border-radius:4px;border:1.5px solid ${isSel?'#334155':'#d1d5db'};background:${isSel?'#334155':'#fff'};color:#fff;font-size:12px;font-weight:900;line-height:1;flex-shrink:0">${isSel?'✓':''}</span>
              <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(lbl)}</span>
            </div>`;
  }).join('');

  panel.innerHTML = header + `<div>${items}</div>`;
  panel.dataset.labels = JSON.stringify(options.reduce((acc, o) => {
    acc[o] = labelFn ? labelFn(o) : o;
    return acc;
  }, {}));
  bn_mselUpdateTrigger(field);
}

/** Renderiza el panel de "Día" como un selector de rango de fechas (calendario). */
function bn_mselRenderDiaCalendar(panel) {
  const desde = bn_st.dia_desde || '';
  const hasta = bn_st.dia_hasta || '';
  panel.innerHTML = `
    <div style="padding:10px;display:flex;flex-direction:column;gap:8px;min-width:240px">
      <div style="display:flex;gap:6px;align-items:center">
        <label style="font-size:11px;font-weight:700;color:#475569;min-width:46px">Desde</label>
        <input type="date" id="bn-dia-desde" value="${esc(desde)}"
               onchange="bn_setDiaRange('desde', this.value)"
               style="flex:1;padding:6px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:12px;background:#fff;color:#334155">
      </div>
      <div style="display:flex;gap:6px;align-items:center">
        <label style="font-size:11px;font-weight:700;color:#475569;min-width:46px">Hasta</label>
        <input type="date" id="bn-dia-hasta" value="${esc(hasta)}"
               onchange="bn_setDiaRange('hasta', this.value)"
               style="flex:1;padding:6px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:12px;background:#fff;color:#334155">
      </div>
      <div style="display:flex;gap:6px;margin-top:2px">
        <button type="button" onclick="bn_setDiaPreset('hoy')"
                style="flex:1;padding:5px 8px;border:1px solid #d1d5db;background:#f1f5f9;color:#334155;font-weight:600;font-size:11px;border-radius:5px;cursor:pointer">Hoy</button>
        <button type="button" onclick="bn_setDiaPreset('semana')"
                style="flex:1;padding:5px 8px;border:1px solid #d1d5db;background:#f1f5f9;color:#334155;font-weight:600;font-size:11px;border-radius:5px;cursor:pointer">Semana</button>
        <button type="button" onclick="bn_setDiaPreset('mes')"
                style="flex:1;padding:5px 8px;border:1px solid #d1d5db;background:#f1f5f9;color:#334155;font-weight:600;font-size:11px;border-radius:5px;cursor:pointer">Mes</button>
      </div>
      <button type="button" onclick="bn_clearDiaRange()"
              style="padding:6px 8px;border:1px solid #d1d5db;background:#fff;color:#475569;font-weight:600;font-size:12px;border-radius:5px;cursor:pointer">
        ✕ Limpiar rango
      </button>
    </div>`;
  panel.dataset.labels = '{}';
  bn_mselUpdateTrigger('dia');
}

function bn_setDiaRange(which, val) {
  if (which === 'desde') bn_st.dia_desde = val || '';
  if (which === 'hasta') bn_st.dia_hasta = val || '';
  bn_mselUpdateTrigger('dia');
  bn_render();
}

function bn_setDiaPreset(preset) {
  const today = new Date();
  const iso = d => d.toISOString().slice(0, 10);
  if (preset === 'hoy') {
    bn_st.dia_desde = iso(today);
    bn_st.dia_hasta = iso(today);
  } else if (preset === 'semana') {
    const dow = today.getDay() || 7;
    const monday = new Date(today); monday.setDate(today.getDate() - (dow - 1));
    const sunday = new Date(monday); sunday.setDate(monday.getDate() + 6);
    bn_st.dia_desde = iso(monday);
    bn_st.dia_hasta = iso(sunday);
  } else if (preset === 'mes') {
    bn_st.dia_desde = iso(new Date(today.getFullYear(), today.getMonth(), 1));
    bn_st.dia_hasta = iso(new Date(today.getFullYear(), today.getMonth()+1, 0));
  }
  const panel = document.getElementById('bn-msel-panel-dia');
  if (panel) bn_mselRenderDiaCalendar(panel);
  bn_render();
}

function bn_clearDiaRange() {
  bn_st.dia_desde = ''; bn_st.dia_hasta = '';
  const panel = document.getElementById('bn-msel-panel-dia');
  if (panel) bn_mselRenderDiaCalendar(panel);
  bn_render();
}

/** Etiqueta visible del trigger 'dia' usando el rango. */
function bn_diaTriggerLabel() {
  const d = bn_st.dia_desde || '', h = bn_st.dia_hasta || '';
  if (!d && !h) return 'Todos';
  if (d && h && d === h) return d;
  if (d && h) return `${d} → ${h}`;
  if (d) return `Desde ${d}`;
  return `Hasta ${h}`;
}

/** Filtra dinámicamente las opciones visibles del multi-select por texto. */
function bn_mselFilterOpts(field, q) {
  const panel = document.getElementById(`bn-msel-panel-${field}`);
  if (!panel) return;
  const qN = (q || '').toLowerCase().trim();
  panel.querySelectorAll('[data-value]').forEach(row => {
    if (!qN) { row.style.display = ''; return; }
    const text = (row.textContent || '').toLowerCase();
    row.style.display = text.includes(qN) ? '' : 'none';
  });
}

/** Selecciona qué renderizador llamar según el contexto del campo. */
function bn_mselRerender(field) {
  if (field === 'ind_anio' || field === 'ind_mes') bn_renderInd();
  else bn_render();
}

/** Click en una opción del multi-select — alterna selección. */
function bn_mselToggleOpt(field, rowEl) {
  if (!Array.isArray(bn_st[field])) bn_st[field] = [];
  const v = rowEl.dataset.value;
  const i = bn_st[field].indexOf(v);
  const willSelect = (i < 0);
  if (willSelect) bn_st[field].push(v);
  else            bn_st[field].splice(i, 1);
  // Sync con BN_IND_STATE para que bn_renderInd no sobreescriba la selección
  if (typeof BN_IND_STATE !== 'undefined') {
    if (field === 'ind_anio') BN_IND_STATE.anio = bn_st[field].slice();
    if (field === 'ind_mes')  BN_IND_STATE.mes  = bn_st[field].slice();
  }
  // Actualizar visual de la fila
  rowEl.dataset.selected = willSelect ? 'true' : 'false';
  rowEl.style.background = willSelect ? '#f1f5f9' : 'transparent';
  rowEl.style.color      = willSelect ? '#334155' : '#374151';
  rowEl.style.fontWeight = willSelect ? '600' : '400';
  const check = rowEl.firstElementChild;
  if (check) {
    check.style.borderColor = willSelect ? '#334155' : '#d1d5db';
    check.style.background  = willSelect ? '#334155' : '#fff';
    check.textContent       = willSelect ? '✓' : '';
  }
  bn_mselUpdateTrigger(field);
  bn_mselRerender(field);
}

/** Actualiza el texto del trigger según selección. */
function bn_mselUpdateTrigger(field) {
  const wrap = document.querySelector(`.bn-msel[data-field="${field}"]`);
  if (!wrap) return;
  const lblEl = wrap.querySelector('.bn-msel-label');
  if (!lblEl) return;
  // Caso especial: 'dia' usa rango de fechas, no array de selecciones
  if (field === 'dia') {
    const txt = bn_diaTriggerLabel();
    lblEl.textContent = txt;
    const active = txt !== 'Todos';
    lblEl.style.color = active ? '#334155' : '';
    lblEl.style.fontWeight = active ? '700' : '';
    return;
  }
  const sel = bn_st[field] || [];
  const panel = document.getElementById(`bn-msel-panel-${field}`);
  let map = {};
  try { map = JSON.parse(panel?.dataset.labels || '{}'); } catch(_) {}
  if (sel.length === 0)      lblEl.textContent = 'Todos';
  else if (sel.length === 1) lblEl.textContent = map[sel[0]] || sel[0];
  else                       lblEl.textContent = `${sel.length} seleccionados`;
  lblEl.style.color = sel.length ? '#334155' : '';
  lblEl.style.fontWeight = sel.length ? '700' : '';
}

/** Abre/cierra un panel de multi-select. Cuando abre, lo posiciona como fixed
 *  para que se despliegue por encima de cualquier contenedor (sin recorte). */
function bn_mselToggle(field, ev) {
  if (ev) ev.stopPropagation();
  const panel = document.getElementById(`bn-msel-panel-${field}`);
  if (!panel) return;
  document.querySelectorAll('.bn-msel-panel').forEach(p => {
    if (p !== panel) { p.classList.add('hidden'); p.style.position=''; p.style.top=''; p.style.left=''; p.style.width=''; }
  });
  const willOpen = panel.classList.contains('hidden');
  if (willOpen) {
    const trigger = document.querySelector(`.bn-msel[data-field="${field}"] .bn-msel-trigger`);
    if (trigger) {
      const r = trigger.getBoundingClientRect();
      panel.style.position = 'fixed';
      panel.style.top   = (r.bottom + 4) + 'px';
      panel.style.left  = r.left + 'px';
      panel.style.width = r.width + 'px';
      panel.style.minWidth = '180px';
      panel.style.maxHeight = Math.min(360, window.innerHeight - r.bottom - 20) + 'px';
      panel.style.right = 'auto';
    }
    panel.classList.remove('hidden');
  } else {
    panel.classList.add('hidden');
  }
}

/** Cambio en un checkbox del multi-select: actualiza estado y re-renderiza. */
function bn_mselChange(field, cb) {
  if (!Array.isArray(bn_st[field])) bn_st[field] = [];
  const v = cb.value;
  const i = bn_st[field].indexOf(v);
  if (cb.checked && i < 0) bn_st[field].push(v);
  else if (!cb.checked && i >= 0) bn_st[field].splice(i, 1);
  bn_mselUpdateTrigger(field);
  bn_mselRerender(field);
}

/** Botón "Todos" — marca todas las opciones disponibles. */
function bn_mselSelectAll(field) {
  const panel = document.getElementById(`bn-msel-panel-${field}`);
  if (!panel) return;
  const all = Array.from(panel.querySelectorAll('[data-value]')).map(el => el.dataset.value);
  bn_st[field] = all.slice();
  // Mantener sincronización con BN_IND_STATE
  if (field === 'ind_anio') BN_IND_STATE.anio = bn_st[field];
  if (field === 'ind_mes')  BN_IND_STATE.mes  = bn_st[field];
  panel.querySelectorAll('[data-value]').forEach(el => {
    el.dataset.selected = 'true';
    el.style.background = '#f1f5f9';
    el.style.color      = '#334155';
    el.style.fontWeight = '600';
    const check = el.firstElementChild;
    if (check) {
      check.style.borderColor = '#334155';
      check.style.background  = '#334155';
      check.textContent       = '✓';
    }
  });
  bn_mselUpdateTrigger(field);
  bn_mselRerender(field);
}

/** Botón "Limpiar" — desmarca todo. */
function bn_mselClear(field) {
  const panel = document.getElementById(`bn-msel-panel-${field}`);
  bn_st[field] = [];
  if (field === 'ind_anio') BN_IND_STATE.anio = bn_st[field];
  if (field === 'ind_mes')  BN_IND_STATE.mes  = bn_st[field];
  if (panel) {
    panel.querySelectorAll('[data-value]').forEach(el => {
      el.dataset.selected = 'false';
      el.style.background = 'transparent';
      el.style.color      = '#374151';
      el.style.fontWeight = '400';
      const check = el.firstElementChild;
      if (check) {
        check.style.borderColor = '#d1d5db';
        check.style.background  = '#fff';
        check.textContent       = '';
      }
    });
  }
  bn_mselUpdateTrigger(field);
  bn_mselRerender(field);
}

// Cerrar paneles al hacer clic fuera — usa mousedown en captura para reaccionar
// incluso si algún hijo hace stopPropagation y aún cuando el panel está
// position:fixed (sigue dentro del .bn-msel en el DOM)
document.addEventListener('mousedown', (e) => {
  const inMsel = e.target.closest && e.target.closest('.bn-msel');
  if (!inMsel) {
    document.querySelectorAll('.bn-msel-panel').forEach(p => p.classList.add('hidden'));
  }
}, true);
// Escape cierra todos los paneles multi-select
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    document.querySelectorAll('.bn-msel-panel').forEach(p => p.classList.add('hidden'));
  }
});

function bn_setDefaultFilters() {
  // Año y Mes en curso por defecto (si están disponibles en los datos)
  const now = new Date();
  const curY = String(now.getFullYear());
  const curM = String(now.getMonth() + 1).padStart(2, '0');

  const hasOpt = (field, val) => {
    const panel = document.getElementById(`bn-msel-panel-${field}`);
    return panel ? Array.from(panel.querySelectorAll('[data-value]')).some(el => el.dataset.value === val) : false;
  };
  bn_st.anio = hasOpt('anio', curY) ? [curY] : [];
  bn_st.mes  = hasOpt('mes',  curM) ? [curM] : [];

  // Limpiar todos los demás multi-selects
  for (const f of BN_MSEL_FIELDS) bn_st[f.key] = [];
  bn_st.q = '';

  // Reflejar en UI re-renderizando opciones de cada panel
  bn_populateFilters();
}

function bn_onFilterChange() {
  bn_st.q = (document.getElementById('bn-f-text')?.value || '').toLowerCase().trim();
  bn_render();
}

function bn_clearFilters() {
  for (const f of [...BN_MSEL_FIELDS.map(x => x.key), 'anio', 'mes']) {
    bn_st[f] = [];
  }
  bn_st.q = '';
  bn_st.dia_desde = '';
  bn_st.dia_hasta = '';
  const txt = document.getElementById('bn-f-text');
  if (txt) txt.value = '';
  // Re-render opciones para reflejar el estado limpio
  bn_populateFilters();
  bn_render();
}

function bn_toggleFilters() {
  const body=document.getElementById('bn-filter-body');
  const btn=document.getElementById('bn-btn-filter-toggle');
  if(!body) return;
  const hidden=body.classList.toggle('hidden');
  btn?.classList.toggle('active',!hidden);
}

// ─── Filtered records ─────────────────────────────────────────────────────────
function bn_filteredRecs(tipo) {
  const s=bn_st;
  const q=(s.q || (document.getElementById('bn-f-text')?.value||'').toLowerCase().trim());
  // Helper: array vacío = sin filtro
  const inArr = (arr, v) => !arr || arr.length === 0 || arr.includes(v);
  return BN_RAW.filter(r=>{
    const t = bn_canon(r._tipo || '');

    const isPCTab = bn_isPC(tipo);
    // Revisado: PC/PC_X muestra solo NO revisados; T/E/I/AC/PA/CA solo revisados.
    // Bypass cuando se renderizan Indicadores (vista independiente).
    if (!BN_BYPASS_REVISADO) {
      const revisado = r._validado === 'Sí';
      if (isPCTab) {
        if (revisado) return false;
      } else if (['T','E','I','AC','PA','CA'].includes(tipo)) {
        if (!revisado) return false;
      }
    }

    // Filtros por sub-tipo (E/I/AC/PA/CA) tanto en Registros contables como en Por clasificar
    const sub = isPCTab ? bn_subOfPC(tipo) : tipo;
    if(sub==='E'  && !t.includes('egr'))     return false;
    if(sub==='I'  && !t.includes('ing'))     return false;
    if(sub==='AC' && !t.includes('activ'))   return false;
    if(sub==='PA' && !t.includes('pasiv'))   return false;
    if(sub==='CA' && !t.includes('capital')) return false;

    // Multi-selects: cada campo es array; vacío = todos.
    // 'dia' se omite — ahora usa rango de fechas (dia_desde / dia_hasta).
    for (const f of BN_MSEL_FIELDS) {
      if (f.key === 'dia') continue;
      const arr = s[f.key];
      if (arr && arr.length) {
        const val = (f.from(r) || '').toString().trim();
        if (!arr.includes(val)) return false;
      }
    }
    // Rango de fechas para 'dia'
    if (s.dia_desde || s.dia_hasta) {
      const iso = bn_formatDiaISO(r.Día || r.Dia || '').slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return false;
      if (s.dia_desde && iso < s.dia_desde) return false;
      if (s.dia_hasta && iso > s.dia_hasta) return false;
    }

    // Año y Mes (multi)
    if ((s.anio && s.anio.length) || (s.mes && s.mes.length)) {
      const iso = bn_formatDiaISO(r.Día || r.Dia || '');
      const m   = iso.match(/^(\d{4})-(\d{2})/);
      if(!m) return false;
      if (s.anio && s.anio.length && !s.anio.includes(m[1])) return false;
      if (s.mes  && s.mes.length  && !s.mes .includes(m[2])) return false;
    }


    // Búsqueda libre: match parcial, multi-palabra (todas las palabras deben aparecer)
    if(q){
      const words = q.split(/\s+/).filter(Boolean);
      const h = [bn_norm(r.SUBCUENTA||''), bn_norm(r.CATEGORIA||''), bn_norm(r.CONCEPTO||''),
                 bn_norm(r.DESCRIPCION||''), bn_norm(r['Cuenta bancaria']||''),
                 bn_norm(r._cuenta||''), bn_norm(r._encargado||''),
                 bn_norm(r._propiedad||''), bn_norm(r._metodo_pago||'')].join(' ').toLowerCase();
      if(!words.every(w => h.includes(w))) return false;
    }
    return true;
  });
}

// ─── Aggregation ─────────────────────────────────────────────────────────────
function bn_aggregate(tipo) {
  const recs=bn_filteredRecs(tipo);
  const g=new Map();
  for(const r of recs){
    const mes=bn_norm(r.Mes), cat=bn_norm(r.CATEGORIA||''), con=bn_norm(r.CONCEPTO||'');
    const key=cat+'||'+con+'||'+mes;
    const bud=bn_getBud(tipo,cat,con);
    if(!g.has(key)) g.set(key,{key,cat,con,mes,real:0,bud});
    g.get(key).real+=Number(r.Monto||0);
  }
  const rows=[...g.values()].map(row=>{
    const realAbs=Math.abs(row.real), budAbs=Math.abs(Number(row.bud||0));
    const av=budAbs>0?(realAbs/budAbs):NaN;
    const sev=(tipo==='E')?bn_sevOver(av):bn_sevUnder(av);
    return{...row,realAbs,budAbs,av,sev};
  });
  rows.sort((a,b)=>{
    const c=a.cat.localeCompare(b.cat,'es'); if(c) return c;
    const d=a.con.localeCompare(b.con,'es'); if(d) return d;
    return a.mes.localeCompare(b.mes,'es');
  });
  const realM=recs.reduce((s,r)=>s+Math.abs(Number(r.Monto||0)),0);
  const seen=new Set(); let budM=0;
  for(const r of recs){
    const k=bn_canon(r.CATEGORIA||'')+'||'+bn_canon(r.CONCEPTO||'')+'||'+bn_norm(r.Mes);
    if(seen.has(k)) continue; seen.add(k);
    budM+=Math.abs(bn_getBud(tipo,bn_norm(r.CATEGORIA||''),bn_norm(r.CONCEPTO||'')));
  }
  const avM=budM>0?(realM/budM):NaN;
  const alerts=tipo==='E'?rows.filter(r=>r.budAbs>0&&r.av>1.0).length:0;
  return{rows,realM,budM,avM,alerts};
}

// ─── Monthly series ────────────────────────────────────────────────────────────
function bn_monthly() {
  const eR=bn_filteredRecs('E'), iR=bn_filteredRecs('I');
  const sum=(recs)=>{ const m=new Map(); for(const r of recs){ const mes=bn_norm(r.Mes); m.set(mes,(m.get(mes)||0)+Math.abs(Number(r.Monto||0))); } return m; };
  const mE=sum(eR), mI=sum(iR);
  const months=[...new Set([...mE.keys(),...mI.keys()])];
  months.sort((a,b)=>{
    const ya=bn_yearOf(a),yb=bn_yearOf(b); if(ya!==yb) return ya.localeCompare(yb,'es');
    return bn_monthOrd(a)-bn_monthOrd(b);
  });
  const rows=months.map(m=>{const I=mI.get(m)||0,E=mE.get(m)||0,U=I-E,M=I>0?(U/I):NaN;return{Mes:m,I,E,U,M};});
  const ytdI=rows.reduce((s,r)=>s+r.I,0), ytdE=rows.reduce((s,r)=>s+r.E,0);
  const ytdU=ytdI-ytdE, ytdM=ytdI>0?(ytdU/ytdI):NaN;
  const n=rows.filter(r=>r.I>0||r.E>0).length||0;
  return{rows,ytdI,ytdE,ytdU,ytdM,n,avgI:n?ytdI/n:0,avgE:n?ytdE/n:0,avgU:n?ytdU/n:0};
}

// ─── Tab switching ────────────────────────────────────────────────────────────
// Mapa tipo → menú padre (para resaltar el menú correspondiente)
const BN_TIPO_PARENT = {
  T:'reg', I:'reg', E:'reg', AC:'reg', PA:'reg', CA:'reg',
  PC:'pc', PC_I:'pc', PC_E:'pc', PC_AC:'pc', PC_PA:'pc', PC_CA:'pc',
  A:'pres', AP:'pres', PR:'pres',
  F:'ind',
};

// Sub-tabs por categoría — renderizadas como chips horizontales en row 2
const BN_CAT_SUBS = {
  pc: [
    { id: 'PC',     label: '📋 Todos' },
    { id: 'PC_I',   label: '💰 Ingresos' },
    { id: 'PC_E',   label: '💸 Egresos' },
    { id: 'PC_AC',  label: '📈 Activos' },
    { id: 'PC_PA',  label: '📋 Pasivos' },
    { id: 'PC_CA',  label: '💼 Capital' },
  ],
  reg: [
    { id: 'T',  label: '📋 Todos' },
    { id: 'I',  label: '💰 Ingresos' },
    { id: 'E',  label: '💸 Egresos' },
    { id: 'AC', label: '📈 Activos' },
    { id: 'PA', label: '📋 Pasivos' },
    { id: 'CA', label: '💼 Capital' },
  ],
  pres: [
    { id: 'A',  label: '⚠️ Alertas' },
    { id: 'AP', label: '🧮 Análisis por partida' },
    { id: 'PR', label: '💰 Presupuesto' },
  ],
  ind: [
    { id: 'F',  label: '📊 Indicadores' },
  ],
};

/** Cambia la categoría activa (Por clasificar / Registros / Control presup.).
 *  Renderiza los chips de sub-opciones en la fila inferior y selecciona la
 *  primera por defecto (o conserva la actual si pertenece a la categoría). */
// Paleta grises ejecutiva — mismo tono para las 4 categorías.
// Padre activo: gris oscuro. Hijo activo: gris medio. Resto: gris claro.
const BN_GREY_PALETTE = {
  parent:'#334155',         // gris oscuro para padre seleccionado
  child:'#f1f5f9',          // gris claro para inactivos
  childActive:'#64748b',    // gris medio para hijo seleccionado
  childText:'#475569',      // texto en inactivos
  childActiveText:'#fff',
  parentTextInactive:'#475569',
};
const BN_CAT_PALETTE = {
  pc:   BN_GREY_PALETTE,
  reg:  BN_GREY_PALETTE,
  pres: BN_GREY_PALETTE,
  ind:  BN_GREY_PALETTE,
};

function bn_setCat(cat) {
  const pal = BN_CAT_PALETTE[cat] || BN_CAT_PALETTE.pc;
  // Resaltar el botón de categoría activo con el tono de su paleta
  ['pc','reg','pres','ind'].forEach(k => {
    const btn = document.getElementById('bn-cat-' + k);
    if (!btn) return;
    const active = (k === cat);
    const p = BN_CAT_PALETTE[k] || BN_CAT_PALETTE.pc;
    btn.style.background = active ? p.parent : p.child;
    btn.style.color      = active ? '#fff'   : p.parentTextInactive;
    btn.style.boxShadow  = active ? 'inset 0 -3px 0 rgba(0,0,0,.18)' : 'none';
    btn.style.fontWeight = active ? '800' : '600';
  });
  // Renderizar sub-opciones como cuadros contiguos full-width en tono claro
  const row = document.getElementById('bn-subcat-row');
  if (row) {
    const subs = BN_CAT_SUBS[cat] || [];
    if (subs.length) {
      row.style.gridTemplateColumns = `repeat(${subs.length}, 1fr)`;
      row.innerHTML = subs.map((s, i) => `
        <button class="bn-sub-chip" id="bn-tab-${s.id}" onclick="bn_setTipo('${s.id}')"
                style="padding:10px 8px;border:none;${i < subs.length-1 ? `border-right:1px solid ${pal.parent}22;` : ''}background:${pal.child};color:${pal.childText};font-weight:600;font-size:12px;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">
          ${esc(s.label)}
        </button>`).join('');
    } else {
      row.innerHTML = '';
    }
  }
  // Si el tipo actual no pertenece a esta categoría, seleccionar la primera
  if (BN_TIPO_PARENT[BN_TIPO] !== cat) {
    const first = (BN_CAT_SUBS[cat] || [])[0];
    if (first) bn_setTipo(first.id);
  } else {
    bn_setTipo(BN_TIPO);
  }
}

function bn_setTipo(t) {
  BN_TIPO=t;
  // Ocultar/mostrar el selector 'Vista' (sólo válido en Por clasificar y Cuentas)
  const viewWrap = document.getElementById('bn-view-cards')?.closest('div')?.parentElement;
  const parent = BN_TIPO_PARENT[t];
  if (viewWrap) {
    viewWrap.style.display = (parent === 'pc' || parent === 'reg') ? '' : 'none';
  }
  // En Planeación e Indicadores: ocultar Seleccionar/Relacionar/Mostrar por página
  const isPresOrInd = (parent === 'pres' || parent === 'ind');
  const selBtn = document.getElementById('bn-btn-sel-mode');
  if (selBtn) selBtn.style.display = isPresOrInd ? 'none' : '';
  const matchBtn = document.getElementById('bn-btn-match-tickets');
  if (matchBtn) matchBtn.style.display = isPresOrInd ? 'none' : '';
  const pageSize = document.getElementById('bn-page-size');
  const pageSizeWrap = pageSize?.parentElement;
  if (pageSizeWrap) pageSizeWrap.style.display = isPresOrInd ? 'none' : '';
  const allTabs = ['T','E','I','AC','PA','CA','PC','PC_I','PC_E','PC_AC','PC_PA','PC_CA','A','F','AP','PR'];
  const pal = BN_CAT_PALETTE[parent] || BN_CAT_PALETTE.pc;
  // Resaltar el chip activo (hijo activo en tono medio; resto en tono claro)
  allTabs.forEach(x => {
    const el = document.getElementById('bn-tab-'+x);
    if (!el) return;
    const active = (x === t);
    el.classList.toggle('active', active);
    if (el.classList.contains('bn-sub-chip')) {
      el.style.background = active ? pal.childActive : pal.child;
      el.style.color      = active ? pal.childActiveText : pal.childText;
      el.style.fontWeight = active ? '800' : '600';
    }
  });
  // Sincronizar resaltado del botón de CATEGORÍA padre
  if (parent) {
    ['pc','reg','pres','ind'].forEach(k => {
      const btn = document.getElementById('bn-cat-' + k);
      if (!btn) return;
      const a = (k === parent);
      const p = BN_CAT_PALETTE[k] || BN_CAT_PALETTE.pc;
      btn.style.background = a ? p.parent : p.child;
      btn.style.color      = a ? '#fff'   : p.parentTextInactive;
      btn.style.boxShadow  = a ? 'inset 0 -3px 0 rgba(0,0,0,.18)' : 'none';
      btn.style.fontWeight = a ? '800' : '600';
    });
  }

  // Mostrar el botón hamburguesa solo cuando hay un tab seleccionado de los 3 menús
  // (Indicadores y la sección Análisis por partida no requieren filtros)
  const ham      = document.getElementById('bn-btn-filter-toggle');
  const filterBd = document.getElementById('bn-filter-body');
  const hideFilters = (t === 'F' || t === 'AP');
  if (ham) ham.classList.toggle('hidden', hideFilters);
  if (hideFilters && filterBd) filterBd.classList.add('hidden');

  // Repoblar dropdowns en función del tipo activo (contexto)
  bn_populateFilters();
  bn_render();
}

// Abre/cierra un submenú (Registros contables o Control presupuestario)
function bn_toggleMenu(name) {
  const sm = document.getElementById('bn-submenu-' + name);
  if (!sm) return;
  const wasHidden = sm.classList.contains('hidden');
  document.querySelectorAll('.bn-submenu').forEach(el => el.classList.add('hidden'));
  if (wasHidden) sm.classList.remove('hidden');
}

// Cerrar submenús al clic fuera
document.addEventListener('click', (e) => {
  if (!e.target.closest('.bn-menu-dropdown')) {
    document.querySelectorAll('.bn-submenu').forEach(el => el.classList.add('hidden'));
  }
});

// ─── Main render ──────────────────────────────────────────────────────────────
function bn_render() {
  if(!BN_LOADED) return;
  const sE=bn_aggregate('E'), sI=bn_aggregate('I');

  // KPIs del período: usan bn_kpiRecs que NO restringe por revisado ni tipo-tab
  const kpiE = bn_kpiRecs('E');
  const kpiI = bn_kpiRecs('I');
  const realE = kpiE.reduce((s,r)=>s+Math.abs(Number(r.Monto||0)),0);
  const realI = kpiI.reduce((s,r)=>s+Math.abs(Number(r.Monto||0)),0);

  bn_txt('bn-k-egr-real',       bn_fmt$(realE));
  const egrPctIng = realI > 0 ? (realE/realI) : NaN;
  bn_txt('bn-k-egr-sub',        kpiE.length+' mov · '+(isFinite(egrPctIng)?bn_fmtPct(egrPctIng):'—')+' de Ingresos');
  bn_txt('bn-k-ing-real',       bn_fmt$(realI));
  bn_txt('bn-k-ing-sub',        kpiI.length+' mov');
  // Egresos siempre rojo, Ingresos siempre verde
  const kE = document.getElementById('bn-k-egr-real');
  if (kE) { kE.style.color = '#dc2626'; }
  const kI = document.getElementById('bn-k-ing-real');
  if (kI) { kI.style.color = '#16a34a'; }

  // KPIs Presupuesto Operativo / Reinversión / Retiro de utilidades
  // (consistentes con los 3 renglones nuevos del pie de Análisis por partida)
  try { bn_apComputeTotalsOnly(); } catch(_) {}
  bn_renderApKpi('bn-k-avance-egr', 'bn-k-avance-bar', 'bn-k-avance-egr-sub', BN_AP_TOTALS.operativo);
  bn_renderApKpi('bn-k-reinv',      'bn-k-reinv-bar',  'bn-k-reinv-sub',      BN_AP_TOTALS.reinversion);
  bn_renderApKpi('bn-k-retiro',     'bn-k-retiro-bar', 'bn-k-retiro-sub',     BN_AP_TOTALS.retiro);
  // Reinversión: rojo siempre (es egreso); muestra % del Ingreso total
  const kR = document.getElementById('bn-k-reinv');
  if (kR) { kR.style.color = '#dc2626'; }
  const reinvReal = BN_AP_TOTALS.reinversion?.real || 0;
  const reinvPct = realI > 0 ? reinvReal/realI : NaN;
  bn_txt('bn-k-reinv-sub', bn_fmt$(reinvReal)+' · '+(isFinite(reinvPct)?bn_fmtPct(reinvPct):'—')+' del Ingreso');
  // Retiro: verde si positivo, rojo si negativo; muestra % del Ingreso total
  const retReal = BN_AP_TOTALS.retiro?.real || 0;
  const kT = document.getElementById('bn-k-retiro');
  if (kT) { kT.style.color = retReal >= 0 ? '#16a34a' : '#dc2626'; }
  const retPct = realI > 0 ? Math.abs(retReal)/realI : NaN;
  bn_txt('bn-k-retiro-sub', bn_fmt$(retReal)+' · '+(isFinite(retPct)?bn_fmtPct(retPct):'—')+' del Ingreso');

  const util=realI-realE;
  bn_txt('bn-k-utilidad',       bn_fmt$(util));
  const marg=realI>0?(util/realI):NaN;
  bn_txt('bn-k-utilidad-sub',   isFinite(marg)?'Margen '+bn_fmtPct(marg):'Sin ingresos');
  const kU=document.getElementById('bn-k-utilidad');
  if(kU) { kU.className='bn-kpi-value'; kU.style.color = util >= 0 ? '#16a34a' : '#dc2626'; }

  document.getElementById('bn-kpi-row')?.classList.remove('hidden');
  // Tarea 13: réplica de los botones Guardar/Clasificar/Limpiar arriba (mismo flujo
  // que el bulk-bar inferior). Sólo aparece cuando hay registros seleccionados.
  (function buildTopActions(){
    const wrap = document.getElementById('bn-card-actions-top'); if (!wrap) return;
    if (!BN_SEL_MODE || BN_SEL.size === 0) {
      wrap.classList.add('hidden'); wrap.innerHTML = ''; return;
    }
    wrap.classList.remove('hidden');
    wrap.innerHTML = `
      <div style="font-weight:800;color:#334155;font-size:11px;padding:6px 10px;border-right:1.5px solid #cbd5e1;margin-right:4px;white-space:nowrap">${BN_SEL.size} seleccionado(s)</div>
      <button onclick="bn_bulkClasificar()"      style="padding:7px 12px;border:none;background:#334155;color:#fff;border-radius:8px;font-weight:600;font-size:12px;cursor:pointer">🏷️ Clasificar</button>
      <button onclick="bn_bulkToggleValidar()"   style="padding:7px 12px;border:none;background:#16a34a;color:#fff;border-radius:8px;font-weight:600;font-size:12px;cursor:pointer">✓ Validar</button>
      <button onclick="bn_selClear()"            style="padding:7px 12px;border:1.5px solid #cbd5e1;background:#fff;color:#475569;border-radius:8px;font-weight:600;font-size:12px;cursor:pointer">✕ Limpiar</button>`;
  })();
  // Resumen del período (Año + Mes) considerado en los KPIs
  (function updatePeriodSummary(){
    const el = document.getElementById('bn-kpi-period-summary'); if (!el) return;
    const NMES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
    const a = (bn_st.anio || []).slice().sort();
    const m = (bn_st.mes  || []).slice().sort();
    const aTxt = a.length ? a.join(', ') : 'Todos los años';
    const mTxt = m.length ? m.map(x => NMES[Number(x)-1] || x).join(', ') : 'Todos los meses';
    let rango = '';
    if (bn_st.dia_desde || bn_st.dia_hasta) {
      rango = ` · 📅 ${bn_st.dia_desde||'…'} → ${bn_st.dia_hasta||'…'}`;
    }
    el.textContent = `· ${aTxt} · ${mTxt}${rango}`;
  })();

  // Panel de estado de revisión (solo en tabs Por Clasificar)
  bn_renderReviewPanel();

  // Subtitle — total filtrado de la pestaña actual
  const visLen = bn_filteredRecs(BN_TIPO).length;
  bn_txt('bn-filter-subtitle', visLen.toLocaleString('es-MX')+' de '+BN_RAW.length.toLocaleString('es-MX')+' movimientos');

  const tw=document.getElementById('bn-table-wrap');
  const cw=document.getElementById('bn-cards-container');
  const pw=document.getElementById('bn-card-pagination');
  const iw=document.getElementById('bn-indicadores');
  const iov=document.getElementById('bn-indicadores-overlay');
  const ap=document.getElementById('bn-analisis-partida');

  if(BN_TIPO==='F'){
    // Indicadores como sección inline aislada: ocultar todo lo demás
    tw?.classList.add('hidden'); cw?.classList.add('hidden'); pw?.classList.add('hidden');
    ap?.classList.add('hidden');
    document.getElementById('bn-presupuesto')?.classList.add('hidden');
    document.getElementById('bn-kpi-row')?.classList.add('hidden');
    document.getElementById('bn-actions-row')?.classList.add('hidden');
    document.getElementById('bn-review-panel')?.classList.add('hidden');
    document.getElementById('bn-filter-body')?.classList.add('hidden');
    const mesAnio = document.getElementById('bn-mes-anio-row');
    if (mesAnio) mesAnio.style.display = 'none';
    // Ocultar barra de búsqueda + botón hamburguesa de filtros
    const searchBar = document.getElementById('bn-f-text')?.parentElement;
    if (searchBar) searchBar.style.display = 'none';
    if (iov) {
      iov.classList.remove('hidden');
      iov.style.position = 'static';
      iov.style.background = 'transparent';
      iov.style.padding = '0';
      iov.style.display = 'block';
      iov.style.zIndex = '';
    }
    const md = document.getElementById('bn-indicadores-modal');
    if (md) {
      md.style.maxWidth = '100%';
      md.style.maxHeight = 'none';
      md.style.boxShadow = 'none';
      md.style.padding = '0';
      md.style.background = 'transparent';
      // Ocultar el botón ✕ y el h2 cuando es inline
      md.querySelectorAll('button').forEach(b => {
        if (b.textContent.trim() === '✕') b.style.display = 'none';
      });
      const h2 = md.querySelector('h2'); if (h2) h2.style.display = 'none';
    }
    iw?.classList.remove('hidden');
    document.body.style.overflow = '';
    bn_renderInd();
    return;
  }
  // Restaurar estilos de overlay (cuando salimos de F)
  if (iov && !iov.classList.contains('hidden')) {
    iov.style.position = ''; iov.style.background = '';
    iov.style.padding = ''; iov.style.display = ''; iov.style.zIndex = '';
  }
  const _md = document.getElementById('bn-indicadores-modal');
  if (_md) {
    _md.style.maxWidth = ''; _md.style.maxHeight = '';
    _md.style.boxShadow = ''; _md.style.padding = ''; _md.style.background = '';
    _md.querySelectorAll('button').forEach(b => { if (b.textContent.trim() === '✕') b.style.display = ''; });
    const h2 = _md.querySelector('h2'); if (h2) h2.style.display = '';
  }
  iw?.classList.add('hidden');
  iov?.classList.add('hidden');
  // Restaurar elementos ocultados al salir de F
  document.getElementById('bn-actions-row')?.classList.remove('hidden');
  const mesAnio2 = document.getElementById('bn-mes-anio-row');
  if (mesAnio2) mesAnio2.style.display = '';
  const searchBar2 = document.getElementById('bn-f-text')?.parentElement;
  if (searchBar2) searchBar2.style.display = '';

  if(BN_TIPO==='AP'){
    tw?.classList.add('hidden'); cw?.classList.add('hidden'); pw?.classList.add('hidden');
    ap?.classList.remove('hidden');
    document.getElementById('bn-presupuesto')?.classList.add('hidden');
    bn_renderAP();
    return;
  }
  ap?.classList.add('hidden');

  if(BN_TIPO==='PR'){
    tw?.classList.add('hidden'); cw?.classList.add('hidden'); pw?.classList.add('hidden');
    const pr = document.getElementById('bn-presupuesto');
    pr?.classList.remove('hidden');
    bn_renderPresupuesto();
    return;
  }
  document.getElementById('bn-presupuesto')?.classList.add('hidden');

  if(BN_TIPO==='A'){
    tw?.classList.remove('hidden'); cw?.classList.add('hidden'); pw?.classList.add('hidden');
    bn_renderTable('A',sE,sI); return;
  }

  // E o I → tarjetas individuales
  tw?.classList.add('hidden'); cw?.classList.remove('hidden'); pw?.classList.remove('hidden');
  BN_CUR_RECS = bn_filteredRecs(BN_TIPO);
  // Sort aplicado sobre TODO el universo filtrado (no sólo sobre la página).
  if (BN_TBL_SORT && BN_TBL_SORT.key) {
    BN_CUR_RECS = bn_tblApplySort(BN_CUR_RECS);
  }
  BN_CARD_PAGE = 1;
  bn_renderCards();
}

// ─── Table render ─────────────────────────────────────────────────────────────
function bn_renderTable(tipo,sE,sI) {
  const thead=document.getElementById('bn-thead-row');
  const tbody=document.getElementById('bn-tbody');
  const title=document.getElementById('bn-table-title');
  const sub=document.getElementById('bn-table-sub');
  if(!thead||!tbody) return;

  if(tipo==='E'){
    title.textContent='💸 Egresos — Presupuesto vs Real';
    sub.textContent='Click en una fila para ver los movimientos detallados';
    thead.innerHTML='<th>Categoría</th><th>Concepto</th><th>Mes</th><th class="num">Real</th><th class="num">Presupuesto</th><th>Avance</th><th>Estado</th>';
  } else if(tipo==='I'){
    title.textContent='💰 Ingresos';
    sub.textContent='Click en una fila para ver movimientos detallados';
    thead.innerHTML='<th>Categoría</th><th>Concepto</th><th>Mes</th><th class="num">Real</th>';
  } else {
    title.textContent='⚠️ Alertas — Partidas fuera de umbral';
    sub.textContent='Egresos > 100% presupuesto · Ingresos < 90% esperado';
    thead.innerHTML='<th>Tipo</th><th>Categoría</th><th>Concepto</th><th>Mes</th><th class="num">%</th><th>Severidad</th>';
  }
  tbody.innerHTML='';

  let rows;
  if(tipo==='A'){
    rows=[];
    sE.rows.forEach(r=>{ if(r.budAbs>0&&r.av>1.0) rows.push({...r,tipoSrc:'E',sev:bn_sevOver(r.av)}); });
    sI.rows.forEach(r=>{ if(r.budAbs>0&&r.av<1.0) rows.push({...r,tipoSrc:'I',sev:bn_sevUnder(r.av)}); });
    const ord={'bn-pill bn-bad':0,'bn-pill bn-warn':1,'bn-pill bn-good':2,'bn-pill':3};
    rows.sort((a,b)=>(ord[a.sev.cls]||3)-(ord[b.sev.cls]||3));
  } else {
    rows=tipo==='E'?sE.rows:sI.rows;
  }

  if(!rows.length){
    tbody.innerHTML=`<tr><td colspan="8" style="color:var(--text-soft);padding:20px;text-align:center">Sin movimientos para los filtros seleccionados</td></tr>`;
    return;
  }

  for(const r of rows){
    const tr=document.createElement('tr');
    tr.style.cursor='pointer';
    if(tipo==='E'){
      const pct=isFinite(r.av)?Math.min(Math.max(r.av,0),2):0;
      const barCls=r.av>1.10?'bn-bar-bad':r.av>1.0?'bn-bar-warn':'bn-bar-good';
      tr.innerHTML=`
        <td>${esc(r.cat)}</td>
        <td>${esc(r.con)||'<span style="color:var(--text-soft)">(sin concepto)</span>'}</td>
        <td>${esc(r.mes)}</td>
        <td class="num">${bn_fmt$(r.realAbs)}</td>
        <td class="num">${r.budAbs?bn_fmt$(r.budAbs):'<span style="color:var(--text-soft)">—</span>'}</td>
        <td>
          <div class="bn-avance-wrap">
            <div class="bn-avance-bar ${barCls}">
              <div class="bn-avance-fill" style="width:${(pct*100).toFixed(1)}%"></div>
              <div class="bn-avance-marker"></div>
            </div>
            <span class="bn-avance-pct">${isFinite(r.av)?bn_fmtPct(r.av):'—'}</span>
          </div>
        </td>
        <td><span class="${r.sev.cls}">${r.sev.label}</span></td>`;
    } else if(tipo==='I'){
      tr.innerHTML=`
        <td>${esc(r.cat)}</td>
        <td>${esc(r.con)||'<span style="color:var(--text-soft)">(sin concepto)</span>'}</td>
        <td>${esc(r.mes)}</td>
        <td class="num">${bn_fmt$(r.realAbs)}</td>`;
    } else {
      const badge=r.tipoSrc==='E'?'<span class="bn-pill bn-bad">Egreso</span>':'<span class="bn-pill bn-warn">Ingreso</span>';
      tr.innerHTML=`
        <td>${badge}</td>
        <td>${esc(r.cat)}</td>
        <td>${esc(r.con)||'(sin concepto)'}</td>
        <td>${esc(r.mes)}</td>
        <td class="num">${isFinite(r.av)?bn_fmtPct(r.av):'—'}</td>
        <td><span class="${r.sev.cls}">${r.sev.label}</span></td>`;
    }
    tr.addEventListener('click',()=>bn_showDetail(r.cat,r.con,r.mes,(tipo==='A'?r.tipoSrc:tipo)));
    tbody.appendChild(tr);
  }
}

// ─── Detail modal ─────────────────────────────────────────────────────────────
function bn_showDetail(cat,con,mes,tipo) {
  const overlay=document.getElementById('bn-modal-overlay');
  const tbody=document.getElementById('bn-modal-tbody');
  if(!overlay||!tbody) return;
  document.getElementById('bn-modal-title').textContent=(tipo==='E'?'Egreso':'Ingreso')+' — '+cat+(con?' › '+con:'');
  document.getElementById('bn-modal-sub').textContent=mes;
  const rows=BN_RAW.filter(r=>{
    const tip=bn_canon(r.CUENTA||r.TIPO||'');
    if(tipo==='E'&&!tip.includes('egr')) return false;
    if(tipo==='I'&&!tip.includes('ing')) return false;
    return bn_norm(r.CATEGORIA||'')===cat && bn_norm(r.CONCEPTO||'')===con && bn_norm(r.Mes)===mes;
  });
  tbody.innerHTML='';
  if(!rows.length){
    tbody.innerHTML='<tr><td colspan="8" style="color:var(--text-soft);padding:16px;text-align:center">Sin movimientos</td></tr>';
  } else {
    for(const r of rows){
      const fac=bn_norm(r.Factura||'');
      const tr=document.createElement('tr');
      tr.innerHTML=`
        <td>${esc(bn_norm(r.Mes))}</td>
        <td>${esc(bn_norm(r['Cuenta bancaria']||''))}</td>
        <td><span class="bn-pill">${esc(bn_norm(r.CUENTA||''))}</span></td>
        <td>${esc(bn_norm(r.SUBCUENTA||''))}</td>
        <td>${esc(bn_norm(r.CATEGORIA||''))}</td>
        <td>${esc(bn_norm(r.CONCEPTO||''))}</td>
        <td><span class="bn-pill ${fac?'bn-good':'bn-warn'}">${fac||'Sin factura'}</span></td>
        <td class="num">${bn_fmt$(Number(r.Monto||0))}</td>`;
      tbody.appendChild(tr);
    }
  }
  overlay.classList.remove('hidden');
  document.body.style.overflow='hidden';
}

function bn_closeDetail() {
  document.getElementById('bn-modal-overlay')?.classList.add('hidden');
  document.body.style.overflow='';
}

// ─── Indicadores — estado y filtros independientes ─────────────────────────
const BN_IND_STATE = { anio: [], mes: [], initialized: false, chartType: 'line' };

/** Inicializa los dropdowns multi-select Año/Mes del modal Indicadores
 *  reutilizando la lógica de bn_mselFillOptions. */
function bn_indInitFilters() {
  const NOMBRES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
                   'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  const now = new Date();
  const curY = String(now.getFullYear());
  const curM = String(now.getMonth()+1).padStart(2,'0');

  const years = new Set();
  for (const r of BN_RAW) {
    const iso = bn_formatDiaISO(r.Día || r.Dia || '');
    if (/^\d{4}-/.test(iso)) years.add(iso.substring(0, 4));
  }
  years.add(curY);
  const ys = [...years].sort((a,b) => b.localeCompare(a));

  if (!BN_IND_STATE.initialized) {
    BN_IND_STATE.anio = ys.includes(curY) ? [curY] : [];
    BN_IND_STATE.mes  = [curM];
    BN_IND_STATE.initialized = true;
  }

  // bn_mselFillOptions usa bn_st[field] como estado; necesitamos exponer
  // BN_IND_STATE.anio/mes bajo claves ind_anio/ind_mes en bn_st temporalmente.
  bn_st.ind_anio = BN_IND_STATE.anio;
  bn_st.ind_mes  = BN_IND_STATE.mes;
  bn_mselFillOptions('ind_anio', ys);
  bn_mselFillOptions('ind_mes',
    Array.from({length:12},(_,i)=>String(i+1).padStart(2,'0')),
    mm => NOMBRES[Number(mm)-1]);

  // Actualiza el label del botón de chart type
  const btn = document.getElementById('bn-ind-chart-type-btn');
  if (btn) btn.textContent = BN_IND_STATE.chartType === 'line' ? '📊 Cambiar a Barras' : '📈 Cambiar a Líneas';
}

function bn_indToggleChartType() {
  BN_IND_STATE.chartType = BN_IND_STATE.chartType === 'line' ? 'bar' : 'line';
  bn_renderInd();
}

// ─── Indicadores ──────────────────────────────────────────────────────────────
// Flag: cuando true, bn_filteredRecs ignora la restricción de revisado
let BN_BYPASS_REVISADO = false;

function bn_renderInd() {
  // Inicializar selectores la primera vez que se abre
  bn_indInitFilters();
  // Swap temporal de filtros: vista de indicadores es INDEPENDIENTE de la principal
  const saved = JSON.parse(JSON.stringify({
    anio: bn_st.anio, mes: bn_st.mes,
    cuentaClasif: bn_st.cuentaClasif, subcuenta: bn_st.subcuenta,
    categoria: bn_st.categoria, concepto: bn_st.concepto,
    cuentaBan: bn_st.cuentaBan, dia: bn_st.dia,
    propiedad: bn_st.propiedad, departamento: bn_st.departamento,
    deducible: bn_st.deducible, reembolso: bn_st.reembolso,
    encargado: bn_st.encargado, q: bn_st.q,
  }));
  // Aplicar SOLO Año/Mes del modal de Indicadores
  bn_st.cuentaClasif = []; bn_st.subcuenta = []; bn_st.categoria = []; bn_st.concepto = [];
  bn_st.cuentaBan = []; bn_st.dia = []; bn_st.propiedad = []; bn_st.departamento = [];
  bn_st.deducible = []; bn_st.reembolso = []; bn_st.encargado = []; bn_st.q = '';
  bn_st.anio = BN_IND_STATE.anio.slice();
  bn_st.mes  = BN_IND_STATE.mes.slice();
  BN_BYPASS_REVISADO = true;
  try {
    bn_renderIndInner();
  } finally {
    BN_BYPASS_REVISADO = false;
    Object.assign(bn_st, saved);
  }
}

function bn_renderIndInner() {
  const s=bn_monthly();
  bn_txt('bn-k-egr-real',     bn_fmt$(s.ytdE));
  bn_txt('bn-k-egr-sub',      'Prom. mensual: '+bn_fmt$(s.avgE)+' ('+s.n+' meses)');
  bn_txt('bn-k-ing-real',     bn_fmt$(s.ytdI));
  bn_txt('bn-k-ing-sub',      'Prom. mensual: '+bn_fmt$(s.avgI));
  bn_txt('bn-k-avance-egr',   bn_fmt$(s.ytdU));
  bn_txt('bn-k-avance-egr-sub','Utilidad YTD');
  bn_txt('bn-k-utilidad',     isFinite(s.ytdM)?bn_fmtPct(s.ytdM):'—');
  bn_txt('bn-k-utilidad-sub', 'Margen neto YTD');
  try{ bn_drawTrend('bn-trend-canvas','bn-trend-tip',s); }catch(e){}
  try{ bn_drawMargin('bn-margin-canvas','bn-margin-tip',s); }catch(e){}

  const list=document.getElementById('bn-alerts-list');
  if(!list) return; list.innerHTML='';
  const add=(lvl,title,det)=>{
    const d=document.createElement('div');
    d.className='bn-fin-alert '+lvl;
    d.innerHTML=`<div class="bn-alert-title">${esc(title)}</div><div class="bn-alert-detail">${esc(det)}</div>`;
    list.appendChild(d);
  };
  if(isFinite(s.ytdM)){
    if(s.ytdM<0.20) add('bad','Margen neto bajo (< 20%)','Margen actual: '+bn_fmtPct(s.ytdM)+'. Revisa costos.');
    else if(s.ytdM<0.30) add('warn','Margen moderado (< 30%)','Margen actual: '+bn_fmtPct(s.ytdM)+'.');
    else add('good','Margen saludable (≥ 30%)','Margen actual: '+bn_fmtPct(s.ytdM)+'.');
  } else { add('warn','Margen no calculable','Sin ingresos suficientes en el período.'); }
  const rows=s.rows||[];
  if(rows.length>=2){
    const a=rows[rows.length-2],b=rows[rows.length-1];
    const gE=a.E>0?(b.E-a.E)/a.E:(b.E>0?1:0);
    const gI=a.I>0?(b.I-a.I)/a.I:(b.I>0?1:0);
    if(gE>gI+0.05&&b.E>0)
      add('bad','Egresos creciendo más rápido que ingresos',
        b.Mes+': Egresos '+bn_fmt$(b.E)+' vs Ingresos '+bn_fmt$(b.I));
    else add('good','Crecimiento de egresos controlado','Cambio último mes: E '+bn_fmtPct(gE)+' vs I '+bn_fmtPct(gI));
  }
  if(s.ytdI>0&&s.ytdE>s.ytdI)
    add('bad','Egresos superan ingresos YTD','E '+bn_fmt$(s.ytdE)+' > I '+bn_fmt$(s.ytdI));
}

// ─── Chart: Trend line ────────────────────────────────────────────────────────
function bn_drawTrend(canvasId,tipId,series) {
  const host=document.getElementById(canvasId), tip=document.getElementById(tipId);
  if(!host) return;
  const chartType = (typeof BN_IND_STATE !== 'undefined' && BN_IND_STATE.chartType) || 'line';
  const rows=(series&&series.rows)?series.rows:[];
  const DPR=Math.max(1,Math.min(2,window.devicePixelRatio||1));
  const W=Math.max(320,host.parentElement?.clientWidth||600);
  const Hcss=240;
  host.width=Math.round(W*DPR); host.height=Math.round(Hcss*DPR);
  host.style.height=Hcss+'px';
  const ctx=host.getContext('2d');
  ctx.setTransform(DPR,0,0,DPR,0,0); ctx.clearRect(0,0,W,Hcss);
  const pL=56,pR=16,pT=14,pB=44,plotW=W-pL-pR,plotH=Hcss-pT-pB;
  if(!rows.length){ ctx.font='12px system-ui'; ctx.fillStyle='rgba(0,0,0,.4)'; ctx.fillText('Sin datos.',pL,pT+20); return; }
  // En modo barras stacked usar I+E como max (porque están apilados)
  const maxV = chartType === 'bar'
    ? Math.max(1, ...rows.map(r => (Math.abs(r.I||0) + Math.abs(r.E||0))))
    : Math.max(1, ...rows.map(r => Math.max(r.I||0, r.E||0, Math.abs(r.U||0))));
  const xAt=(i)=>pL+(rows.length===1?plotW/2:i*(plotW/(rows.length-1)));
  const yAt=(v)=>pT+(1-(v/maxV))*plotH;

  const drawAxes=()=>{
    ctx.lineWidth=1; ctx.strokeStyle='rgba(0,0,0,.10)';
    ctx.beginPath(); ctx.moveTo(pL,pT); ctx.lineTo(pL,pT+plotH); ctx.lineTo(pL+plotW,pT+plotH); ctx.stroke();
    ctx.font='9px system-ui'; ctx.fillStyle='rgba(0,0,0,.40)'; ctx.textAlign='right';
    [0,.25,.5,.75,1].forEach(f=>{
      const y=pT+(1-f)*plotH;
      const lbl=maxV*f>=1000?(bn_fmt$(maxV*f)).replace('MX$','$'):bn_fmt$(maxV*f).replace('MX$','$');
      ctx.fillText(lbl,pL-3,y+3);
      ctx.strokeStyle='rgba(0,0,0,.05)'; ctx.beginPath(); ctx.moveTo(pL,y); ctx.lineTo(pL+plotW,y); ctx.stroke();
    });
    ctx.textAlign='center'; ctx.fillStyle='rgba(0,0,0,.50)';
    rows.forEach((r,i)=>{
      if(rows.length>12&&i%2===1) return;
      const lab=String(r.Mes||'').replace(/^\s*\d{1,2}\s*[.\-\/]\s*/,'').slice(0,10);
      ctx.fillText(lab,xAt(i),pT+plotH+18);
    });
  };
  const drawSeries=(key,color)=>{
    ctx.lineWidth=2.5; ctx.strokeStyle=color;
    ctx.beginPath();
    rows.forEach((r,i)=>{ const v=Math.max(0,Number(r[key]||0)); i===0?ctx.moveTo(xAt(i),yAt(v)):ctx.lineTo(xAt(i),yAt(v)); });
    ctx.stroke();
    ctx.fillStyle=color;
    rows.forEach((r,i)=>{ ctx.beginPath(); ctx.arc(xAt(i),yAt(Math.max(0,r[key]||0)),3,0,Math.PI*2); ctx.fill(); });
  };
  // Bar mode: stacked I (verde) + E (amber) — Utilidad como línea encima
  const drawBars = () => {
    const n = rows.length;
    const colW = Math.max(8, plotW / n * 0.62);
    rows.forEach((r,i) => {
      const cx = xAt(i);
      const I = Math.abs(r.I||0), E = Math.abs(r.E||0);
      const baseY = pT + plotH;
      const hI = (I/maxV)*plotH;
      const hE = (E/maxV)*plotH;
      // Ingresos abajo
      ctx.fillStyle='#10b981';
      ctx.fillRect(cx - colW/2, baseY - hI, colW, hI);
      // Egresos encima
      ctx.fillStyle='#f59e0b';
      ctx.fillRect(cx - colW/2, baseY - hI - hE, colW, hE);
    });
    // Línea de Utilidad
    ctx.lineWidth=2.5; ctx.strokeStyle='#6366f1';
    ctx.beginPath();
    rows.forEach((r,i) => {
      const v = Math.max(0, Number(r.U||0));
      i===0 ? ctx.moveTo(xAt(i), yAt(v)) : ctx.lineTo(xAt(i), yAt(v));
    });
    ctx.stroke();
    ctx.fillStyle='#6366f1';
    rows.forEach((r,i) => { ctx.beginPath(); ctx.arc(xAt(i), yAt(Math.max(0,r.U||0)), 3.5, 0, Math.PI*2); ctx.fill(); });
  };
  const redraw=()=>{
    ctx.clearRect(0,0,W,Hcss);
    drawAxes();
    if (chartType === 'bar') drawBars();
    else { drawSeries('I','#10b981'); drawSeries('E','#f59e0b'); drawSeries('U','#6366f1'); }
  };
  redraw();

  const near=(cx)=>{ const rect=host.getBoundingClientRect(),px=cx-rect.left; let b=0,bd=Infinity; for(let i=0;i<rows.length;i++){const d=Math.abs(px-xAt(i));if(d<bd){bd=d;b=i;}} return{idx:b,rect}; };
  const showAt=(idx,rect,cx,cy)=>{
    redraw();
    ctx.strokeStyle='rgba(0,0,0,.18)'; ctx.lineWidth=1; ctx.beginPath(); ctx.moveTo(xAt(idx),pT); ctx.lineTo(xAt(idx),pT+plotH); ctx.stroke();
    [['I','#10b981'],['E','#f59e0b'],['U','#6366f1']].forEach(([key,col])=>{ ctx.fillStyle=col; ctx.beginPath(); ctx.arc(xAt(idx),yAt(Math.max(0,rows[idx][key]||0)),5,0,Math.PI*2); ctx.fill(); });
    if(tip){ const r=rows[idx]; tip.innerHTML=`<div class="bn-tip-title">${esc(r.Mes)}</div><div>Ingresos: <b>${bn_fmt$(r.I)}</b></div><div>Egresos: <b>${bn_fmt$(r.E)}</b></div><div>Utilidad: <b>${bn_fmt$(r.U)}</b></div>`; tip.style.display='block'; const bx=Math.min(Math.max(8,cx-rect.left+12),rect.width-200),by=Math.min(Math.max(8,cy-rect.top-10),rect.height-100); tip.style.left=bx+'px'; tip.style.top=by+'px'; }
  };
  const hide=()=>{ if(tip) tip.style.display='none'; redraw(); };
  host.onmousemove=(ev)=>{ const {idx,rect}=near(ev.clientX); showAt(idx,rect,ev.clientX,ev.clientY); };
  host.onmouseleave=hide;
  host.addEventListener('touchstart',(ev)=>{ const t=ev.touches[0]; if(!t) return; const {idx,rect}=near(t.clientX); showAt(idx,rect,t.clientX,t.clientY); },{passive:true});
  host.addEventListener('touchmove',(ev)=>{ const t=ev.touches[0]; if(!t) return; const {idx,rect}=near(t.clientX); showAt(idx,rect,t.clientX,t.clientY); },{passive:true});
  host.addEventListener('touchend',hide,{passive:true});
}

// ─── Chart: Margin bars ───────────────────────────────────────────────────────
function bn_drawMargin(canvasId,tipId,series) {
  const host=document.getElementById(canvasId), tip=document.getElementById(tipId);
  if(!host) return;
  const rows=(series&&series.rows)?series.rows:[];
  const DPR=Math.max(1,Math.min(2,window.devicePixelRatio||1));
  const W=Math.max(320,host.parentElement?.clientWidth||600), Hcss=220;
  host.width=Math.round(W*DPR); host.height=Math.round(Hcss*DPR); host.style.height=Hcss+'px';
  const ctx=host.getContext('2d');
  ctx.setTransform(DPR,0,0,DPR,0,0); ctx.clearRect(0,0,W,Hcss);
  const pL=56,pR=16,pT=14,pB=44,plotW=W-pL-pR,plotH=Hcss-pT-pB,baseY=pT+plotH/2;
  ctx.lineWidth=1; ctx.strokeStyle='rgba(0,0,0,.10)';
  ctx.beginPath(); ctx.moveTo(pL,baseY); ctx.lineTo(pL+plotW,baseY); ctx.stroke();
  ctx.font='9px system-ui'; ctx.fillStyle='rgba(0,0,0,.40)'; ctx.textAlign='right';
  ctx.fillText('+100%',pL-3,pT+12); ctx.fillText('0%',pL-3,baseY+4); ctx.fillText('-100%',pL-3,pT+plotH+4);
  if(!rows.length) return;
  const clamp=(v)=>Math.max(-1,Math.min(1,v));
  const band=plotW/rows.length, barW=Math.max(8,band*0.55);
  rows.forEach((r,i)=>{
    const m=clamp(isFinite(r.M)?r.M:0);
    const x0=pL+i*band+(band-barW)/2, h=Math.abs(m)*(plotH/2), y0=m>=0?(baseY-h):baseY;
    ctx.fillStyle=m>=0?'rgba(16,185,129,.65)':'rgba(245,158,11,.75)';
    ctx.beginPath(); bn_rrect(ctx,x0,y0,barW,h||1,4); ctx.fill();
    if(rows.length<=12||i%2===0){
      const lab=String(r.Mes||'').replace(/^\s*\d{1,2}\s*[.\-\/]\s*/,'').slice(0,10);
      ctx.font='9px system-ui'; ctx.fillStyle='rgba(0,0,0,.50)'; ctx.textAlign='center';
      ctx.fillText(lab,pL+i*band+band/2,pT+plotH+18);
    }
  });
  const showAt=(cx,cy)=>{
    const rect=host.getBoundingClientRect(), px=cx-rect.left-pL;
    const idx=Math.max(0,Math.min(rows.length-1,Math.floor(px/band)));
    const r=rows[idx];
    if(tip&&r){ tip.innerHTML=`<div class="bn-tip-title">${esc(r.Mes)}</div><div>Margen: <b>${isFinite(r.M)?bn_fmtPct(r.M):'—'}</b></div><div>Ingresos: <b>${bn_fmt$(r.I)}</b></div><div>Egresos: <b>${bn_fmt$(r.E)}</b></div><div>Utilidad: <b>${bn_fmt$(r.U)}</b></div>`; tip.style.display='block'; const bx=Math.min(Math.max(8,cx-rect.left+12),rect.width-200),by=Math.min(Math.max(8,cy-rect.top-10),rect.height-120); tip.style.left=bx+'px'; tip.style.top=by+'px'; }
  };
  const hide=()=>{ if(tip) tip.style.display='none'; };
  host.onmousemove=(ev)=>showAt(ev.clientX,ev.clientY);
  host.onmouseleave=hide;
  host.addEventListener('touchstart',(ev)=>{ const t=ev.touches[0]; if(t) showAt(t.clientX,t.clientY); },{passive:true});
  host.addEventListener('touchend',hide,{passive:true});
}

function bn_rrect(ctx,x,y,w,h,r) {
  const rr=Math.min(r,w/2,h/2);
  ctx.beginPath(); ctx.moveTo(x+rr,y); ctx.lineTo(x+w-rr,y); ctx.quadraticCurveTo(x+w,y,x+w,y+rr);
  ctx.lineTo(x+w,y+h-rr); ctx.quadraticCurveTo(x+w,y+h,x+w-rr,y+h);
  ctx.lineTo(x+rr,y+h); ctx.quadraticCurveTo(x,y+h,x,y+h-rr);
  ctx.lineTo(x,y+rr); ctx.quadraticCurveTo(x,y,x+rr,y); ctx.closePath();
}

// ─── Bancos: Card-based view ──────────────────────────────────────────────────
let BN_CUR_RECS  = [];
let BN_CARD_PAGE = 1;
let BN_CARD_SIZE = 50;
let BN_VIEW_MODE = 'cards'; // 'cards' | 'table'

/** Columnas exportadas (todas las disponibles, no sólo las visibles). */
const BN_EXPORT_COLS = [
  { k: 'Día',          get: r => bn_formatDia(r.Día || r.Dia || '') },
  { k: 'Cuenta bancaria', get: r => r['Cuenta bancaria'] || '' },
  { k: 'Descripción',  get: r => r.DESCRIPCION || '' },
  { k: 'Monto',        get: r => Number(r.Monto) || 0 },
  { k: 'Factura',      get: r => r.FacturaFlag || r.Factura || '' },
  { k: 'Cuenta',       get: r => r._cuenta || '' },
  { k: 'Subcuenta',    get: r => r._subcuenta || '' },
  { k: 'Categoría',    get: r => r._categoria_gasto || '' },
  { k: 'Concepto',     get: r => r._concepto || '' },
  { k: 'Propiedad',    get: r => r._propiedad || '' },
  { k: 'Departamento', get: r => r._departamento || '' },
  { k: 'Encargado',    get: r => r._encargado || '' },
  { k: 'Deducible',    get: r => r._deducible || '' },
  { k: 'Reembolso',    get: r => r._reembolso || '' },
  { k: 'Reembolso a',  get: r => r._reembolso_a || '' },
  { k: 'Método pago',  get: r => r._metodo_pago || '' },
  { k: 'Comentarios',  get: r => r._comentarios || '' },
  { k: 'Duda',         get: r => r._duda || '' },
  { k: 'Validado',     get: r => r._validado || '' },
  { k: 'Clasificado por', get: r => r._clasificado_por || '' },
];

/** Devuelve {title, cols, rows} con los datos a exportar según la sección actual. */
function bn_currentDownloadDataset() {
  const tipo = BN_TIPO;
  // Sección Presupuesto: exporta BN_BUDGET con sus columnas
  if (tipo === 'PR') {
    const cols = (typeof BN_PR_COLS !== 'undefined' ? BN_PR_COLS : []).map(c => ({ k: c.label || c.key, get: r => r[c.key] }));
    const data = (BN_PR_DRAFT || BN_BUDGET || []).map(r => r);
    return { title: 'Presupuesto', cols, rows: data };
  }
  // Sección Análisis por partida: exporta el árbol agregado
  if (tipo === 'AP') {
    return bn_buildAPDownloadDataset();
  }
  // Sección Indicadores: exporta los datos mensuales utilizados
  if (tipo === 'F') {
    return bn_buildIndDownloadDataset();
  }
  // Default (Por clasificar / Cuentas): BN_CUR_RECS con BN_EXPORT_COLS
  return { title: 'Registros', cols: BN_EXPORT_COLS, rows: BN_CUR_RECS };
}

function bn_buildAPDownloadDataset() {
  const records = bn_kpiRecs(null);
  const cols = [
    { k: 'Nivel',       get: r => r._nivel || '' },
    { k: 'Cuenta',      get: r => r.cuenta || '' },
    { k: 'Subcuenta',   get: r => r.sub || '' },
    { k: 'Categoría',   get: r => r.cat || '' },
    { k: 'Concepto',    get: r => r.con || '' },
    { k: 'Movimientos', get: r => r.count || 0 },
    { k: 'Monto',       get: r => r.total || 0 },
    { k: '%',           get: r => isFinite(r.pct) ? (r.pct*100).toFixed(2) + '%' : '' },
    { k: 'Presupuesto', get: r => r.bud || 0 },
    { k: 'Ciclo',       get: r => r.cycle || '' },
    { k: 'Avance %',    get: r => r.bud > 0 ? ((r.total/r.bud)*100).toFixed(2) + '%' : '' },
  ];
  const tree = bn_apBuildTree(records, BN_AP_LEVELS);
  const rows = [];
  function walk(node, depth, ancestors, rootTotal, parentTotal) {
    if (depth > 0) {
      const { value: bud, cycle } = bn_apComputeBudget(node, records, ancestors, BN_AP_LEVELS);
      const denom = BN_AP_PCT_MODE === 'rel' ? parentTotal : rootTotal;
      const pct = (denom && Math.abs(denom) > 0) ? (node.total / denom) : NaN;
      rows.push({
        _nivel: ['Cuenta','Subcuenta','Categoría','Concepto'][depth-1] || '',
        cuenta: ancestors[0] || '', sub: ancestors[1] || '',
        cat:    ancestors[2] || '', con: ancestors[3] || '',
        count: node.count, total: node.total, bud, cycle, pct,
      });
    }
    const kids = [...node.children.values()].sort((a,b) => b.total - a.total);
    for (const child of kids) {
      // Para hijos de la raíz (cuenta): rootTotal = child.total (cuenta misma)
      const childRoot = (depth === 0) ? child.total : rootTotal;
      walk(child, depth + 1, [...ancestors, child.name], childRoot, node.total);
    }
  }
  walk(tree, 0, [], 0, tree.total);
  return { title: 'Análisis por partida', cols, rows };
}

function bn_buildIndDownloadDataset() {
  // Aplicar swap igual que bn_renderInd
  const saved = JSON.parse(JSON.stringify({
    anio: bn_st.anio, mes: bn_st.mes,
    cuentaClasif: bn_st.cuentaClasif, subcuenta: bn_st.subcuenta,
    categoria: bn_st.categoria, concepto: bn_st.concepto,
    cuentaBan: bn_st.cuentaBan, dia: bn_st.dia,
    propiedad: bn_st.propiedad, departamento: bn_st.departamento,
    deducible: bn_st.deducible, reembolso: bn_st.reembolso,
    encargado: bn_st.encargado, q: bn_st.q,
  }));
  bn_st.cuentaClasif = []; bn_st.subcuenta = []; bn_st.categoria = []; bn_st.concepto = [];
  bn_st.cuentaBan = []; bn_st.dia = []; bn_st.propiedad = []; bn_st.departamento = [];
  bn_st.deducible = []; bn_st.reembolso = []; bn_st.encargado = []; bn_st.q = '';
  bn_st.anio = BN_IND_STATE.anio.slice();
  bn_st.mes  = BN_IND_STATE.mes.slice();
  BN_BYPASS_REVISADO = true;
  let monthly;
  try {
    monthly = bn_monthly();
  } finally {
    BN_BYPASS_REVISADO = false;
    Object.assign(bn_st, saved);
  }
  const cols = [
    { k: 'Mes',       get: r => r.Mes || '' },
    { k: 'Ingresos',  get: r => r.I || 0 },
    { k: 'Egresos',   get: r => r.E || 0 },
    { k: 'Utilidad',  get: r => r.U || 0 },
    { k: 'Margen %',  get: r => isFinite(r.M) ? (r.M*100).toFixed(2) + '%' : '' },
  ];
  return { title: 'Indicadores', cols, rows: monthly.rows || [] };
}

function bn_showDownloadPopup() {
  let ov = document.getElementById('bn-download-overlay');
  if (!ov) {
    ov = document.createElement('div');
    ov.id = 'bn-download-overlay';
    ov.style.cssText = 'position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px';
    ov.onclick = (e) => { if (e.target === ov) ov.remove(); };
    document.body.appendChild(ov);
  }
  const n = BN_CUR_RECS.length;
  ov.innerHTML = `
    <div style="background:#fff;border-radius:14px;width:100%;max-width:460px;padding:22px;box-shadow:0 20px 60px rgba(15,23,42,.4);position:relative" onclick="event.stopPropagation()">
      <button onclick="document.getElementById('bn-download-overlay').remove()"
              style="position:absolute;top:10px;right:10px;width:32px;height:32px;border:none;background:#f1f5f9;border-radius:50%;font-size:18px;cursor:pointer">✕</button>
      <h3 style="margin:0 0 6px;font-size:16px">⬇️ Descargar datos</h3>
      <p style="margin:0 0 16px;font-size:12px;color:#64748b">
        Se exportarán <b>${n}</b> registro${n===1?'':'s'} con los filtros actuales y todos los campos disponibles.
      </p>
      <div style="display:flex;gap:10px;flex-wrap:wrap">
        <button onclick="bn_downloadExcel();document.getElementById('bn-download-overlay').remove()"
                style="flex:1;min-width:140px;padding:14px;border:1.5px solid #16a34a;background:#f0fdf4;color:#15803d;border-radius:10px;font-weight:700;font-size:13px;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:6px">
          <span style="font-size:24px">📊</span>
          <span>Excel / CSV</span>
          <span style="font-size:10px;font-weight:400;color:#16a34a">.csv (abre en Excel)</span>
        </button>
        <button onclick="bn_downloadPDF();document.getElementById('bn-download-overlay').remove()"
                style="flex:1;min-width:140px;padding:14px;border:1.5px solid #dc2626;background:#fef2f2;color:#991b1b;border-radius:10px;font-weight:700;font-size:13px;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:6px">
          <span style="font-size:24px">📄</span>
          <span>PDF</span>
          <span style="font-size:10px;font-weight:400;color:#dc2626">imprimir → guardar</span>
        </button>
      </div>
    </div>`;
}

function bn_csvEscape(v) {
  const s = String(v == null ? '' : v);
  if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function bn_downloadExcel() {
  const ds = bn_currentDownloadDataset();
  const cols = ds.cols;
  const head = cols.map(c => bn_csvEscape(c.k)).join(',');
  const rows = ds.rows.map(r => cols.map(c => bn_csvEscape(c.get(r))).join(','));
  const csv = '﻿' + head + '\n' + rows.join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const slug = (ds.title || 'datos').toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_|_$/g,'');
  a.download = `${slug}_${new Date().toISOString().slice(0,10)}.csv`;
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
}

function bn_downloadPDF() {
  const ds = bn_currentDownloadDataset();
  const cols = ds.cols;
  const w = window.open('', '_blank');
  if (!w) { alert('Permite ventanas emergentes para imprimir a PDF'); return; }
  const rows = ds.rows.map(r => `<tr>${
    cols.map(c => {
      const v = c.get(r);
      const isNum = typeof v === 'number';
      const txt = isNum ? bn_fmt$(Number(v)||0) : String(v==null?'':v);
      return `<td style="${isNum?'text-align:right':''}">${esc(txt)}</td>`;
    }).join('')
  }</tr>`).join('');
  w.document.write(`
    <!DOCTYPE html><html><head><meta charset="utf-8"><title>${esc(ds.title)} — Exportar</title>
    <style>
      * { box-sizing: border-box; }
      body { font-family: Arial, sans-serif; padding: 20px; color: #1f2937; }
      h1 { margin: 0 0 6px; font-size: 18px; }
      .meta { color: #64748b; font-size: 11px; margin-bottom: 14px; }
      table { width: 100%; border-collapse: collapse; font-size: 9px; }
      th { background: #475569; color: #fff; padding: 6px 5px; text-align: left; font-size: 9px; text-transform: uppercase; }
      td { padding: 5px; border-bottom: 1px solid #e2e8f0; vertical-align: top; }
      tr:nth-child(even) td { background: #f8fafc; }
      @page { size: landscape; margin: 14mm; }
      @media print { .noprint { display: none; } }
    </style></head><body>
    <button class="noprint" onclick="window.print()" style="padding:8px 16px;background:#475569;color:#fff;border:none;border-radius:6px;cursor:pointer;margin-bottom:10px">🖨 Imprimir / Guardar PDF</button>
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
      <img src="https://drive.google.com/thumbnail?id=1We0TuuoTb0fDa5xLVMzDqzMZZWwgkb0v&sz=w200"
           alt="Check Inn Saltillo"
           style="height:40px;width:auto;object-fit:contain"
           onerror="this.style.display='none'">
      <h1 style="margin:0">${esc(ds.title)} — Sistema Financiero</h1>
    </div>
    <div class="meta">Generado: ${new Date().toLocaleString('es-MX')} · ${ds.rows.length} registro(s)</div>
    <table>
      <thead><tr>${cols.map(c => `<th>${esc(c.k)}</th>`).join('')}</tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <script>setTimeout(()=>window.print(),300);<\/script>
    </body></html>`);
  w.document.close();
}
// Orden de columnas (drag-drop) — claves canónicas que coinciden con headers/body
const BN_TBL_DEFAULT_ORDER = [
  'validado','duda','dudaNota','deducible','dia','buscarClasif','cuenta',
  'subcuenta','categoria','concepto','monto','descripcion','comentarios',
  'cuentaBanc','factura'
];
let BN_TBL_COL_ORDER = (() => {
  try {
    const saved = JSON.parse(localStorage.getItem('BN_TBL_COL_ORDER') || '[]');
    if (Array.isArray(saved) && saved.length === BN_TBL_DEFAULT_ORDER.length &&
        BN_TBL_DEFAULT_ORDER.every(k => saved.includes(k))) return saved;
  } catch(_) {}
  return BN_TBL_DEFAULT_ORDER.slice();
})();
function bn_tblSaveColOrder() {
  try { localStorage.setItem('BN_TBL_COL_ORDER', JSON.stringify(BN_TBL_COL_ORDER)); } catch(_) {}
}
let BN_TBL_DRAG_FROM = null;
function bn_tblColDragStart(ev, key) {
  BN_TBL_DRAG_FROM = key;
  ev.dataTransfer.effectAllowed = 'move';
  try { ev.dataTransfer.setData('text/plain', key); } catch(_) {}
}
function bn_tblColDragOver(ev) { ev.preventDefault(); ev.dataTransfer.dropEffect = 'move'; }
function bn_tblColDrop(ev, key) {
  ev.preventDefault();
  const from = BN_TBL_DRAG_FROM; BN_TBL_DRAG_FROM = null;
  if (!from || from === key) return;
  const i = BN_TBL_COL_ORDER.indexOf(from);
  const j = BN_TBL_COL_ORDER.indexOf(key);
  if (i < 0 || j < 0) return;
  BN_TBL_COL_ORDER.splice(i, 1);
  BN_TBL_COL_ORDER.splice(j, 0, from);
  bn_tblSaveColOrder();
  bn_renderCards();
}

// Ordenamiento en la vista Tabla — { key, dir: 'asc' | 'desc' }
let BN_TBL_SORT = { key: '', dir: 'asc' };
const BN_TBL_SORT_DEFS = {
  dia:        { kind: 'date',   get: r => bn_formatDiaISO(r.Día || r.Dia || '') },
  cuenta:     { kind: 'text',   get: r => r._cuenta || '' },
  subcuenta:  { kind: 'text',   get: r => r._subcuenta || '' },
  categoria:  { kind: 'text',   get: r => r._categoria_gasto || '' },
  concepto:   { kind: 'text',   get: r => r._concepto || '' },
  descripcion:{ kind: 'text',   get: r => r.DESCRIPCION || '' },
  comentarios:{ kind: 'text',   get: r => r._comentarios || '' },
  dudaNota:   { kind: 'text',   get: r => r._duda_nota || '' },
  monto:      { kind: 'number', get: r => Number(r.Monto) || 0 },
  cuentaBanc: { kind: 'text',   get: r => r['Cuenta bancaria'] || '' },
  factura:    { kind: 'text',   get: r => r.FacturaFlag || '' },
  deducible:  { kind: 'text',   get: r => r._deducible || '' },
  duda:       { kind: 'text',   get: r => r._duda || '' },
  validado:   { kind: 'text',   get: r => r._validado || '' },
};

function bn_tblSort(key) {
  if (BN_TBL_SORT.key === key) {
    BN_TBL_SORT.dir = (BN_TBL_SORT.dir === 'asc') ? 'desc' : 'asc';
  } else {
    BN_TBL_SORT = { key, dir: 'asc' };
  }
  bn_renderCards();
}

function bn_tblApplySort(recs) {
  const def = BN_TBL_SORT_DEFS[BN_TBL_SORT.key];
  if (!def) return recs;
  const dir = BN_TBL_SORT.dir === 'desc' ? -1 : 1;
  return [...recs].sort((a, b) => {
    let va = def.get(a), vb = def.get(b);
    if (def.kind === 'number') return ((Number(va)||0) - (Number(vb)||0)) * dir;
    va = String(va || ''); vb = String(vb || '');
    return va.localeCompare(vb, 'es', { numeric: true, sensitivity: 'base' }) * dir;
  });
}

function bn_tblSortIcon(key) {
  if (BN_TBL_SORT.key !== key) return '<span style="opacity:.4;font-size:10px;margin-left:4px">⇅</span>';
  return BN_TBL_SORT.dir === 'asc'
    ? '<span style="font-size:10px;margin-left:4px;color:#cbd5e1">▲</span>'
    : '<span style="font-size:10px;margin-left:4px;color:#cbd5e1">▼</span>';
}

let BN_TBL_EDIT_MODE = false; // modo edición de la columna Descripción
const BN_TBL_DESC_CHANGES = new Map(); // rowNum → newDesc
let BN_TBL_DESC_WIDTH = 260; // ancho px de la columna Descripción
let BN_TBL_COMENT_EDIT_MODE = false; // modo edición de la columna Comentarios
const BN_TBL_COMENT_CHANGES = new Map(); // rowNum → newComentarios
let BN_TBL_DUDANT_EDIT_MODE = false; // modo edición de la columna Dudas texto

function bn_tblToggleDudaNotaEditMode() {
  BN_TBL_DUDANT_EDIT_MODE = !BN_TBL_DUDANT_EDIT_MODE;
  bn_renderCards();
}
function bn_tblExitDudaNotaEditMode() {
  BN_TBL_DUDANT_EDIT_MODE = false;
  bn_renderCards();
}
async function bn_tblTrackDudaNotaChange(idx, cellEl) {
  const rec = BN_CUR_RECS[idx]; if (!rec || !rec.rowNum) return;
  const newVal = (cellEl.textContent || '').trim();
  if ((rec._duda_nota || '') === newVal) return;
  rec._duda_nota = newVal;
  cellEl.style.outline = '2px solid #64748b';
  try {
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rowNum: rec.rowNum, duda_nota_edit: true, duda_nota: newVal }),
    });
    const j = await resp.json();
    cellEl.style.outline = j.ok ? '2px solid #16a34a' : '2px solid #dc2626';
    setTimeout(() => { cellEl.style.outline = ''; }, 1200);
  } catch (_) {
    cellEl.style.outline = '2px solid #dc2626';
    setTimeout(() => { cellEl.style.outline = ''; }, 1500);
  }
}

function bn_tblToggleComentEditMode() {
  if (!BN_TBL_COMENT_EDIT_MODE) {
    BN_TBL_COMENT_EDIT_MODE = true;
    BN_TBL_COMENT_CHANGES.clear();
    bn_renderCards();
  } else {
    bn_tblShowSaveComentConfirm();
  }
}

function bn_tblExitComentEditMode() {
  const n = BN_TBL_COMENT_CHANGES.size;
  if (n > 0) {
    if (!confirm(`¿Salir sin guardar los ${n} cambio${n===1?'':'s'} pendiente${n===1?'':'s'} en Comentarios?`)) return;
  }
  BN_TBL_COMENT_EDIT_MODE = false;
  BN_TBL_COMENT_CHANGES.clear();
  bn_renderCards();
}

function bn_tblTrackComentChange(idx, cellEl) {
  const rec = BN_CUR_RECS[idx];
  if (!rec || !rec.rowNum) return;
  const newVal = (cellEl.textContent || '').trim();
  const origVal = (cellEl.dataset.origComent || '').trim();
  if (newVal === origVal) return;
  rec._comentarios = newVal;
  cellEl.dataset.origComent = newVal;
  cellEl.title = newVal;
  cellEl.style.outline = '2px solid #64748b';
  (async () => {
    try {
      const payload = bn_buildSavePayload(rec, {});
      payload.clasificacion.comentarios = newVal;
      const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const j = await resp.json();
      cellEl.style.outline = j.ok ? '2px solid #16a34a' : '2px solid #dc2626';
      setTimeout(() => { cellEl.style.outline = ''; }, 1200);
    } catch (_) {
      cellEl.style.outline = '2px solid #dc2626';
      setTimeout(() => { cellEl.style.outline = ''; }, 1500);
    }
  })();
}

function bn_tblShowSaveComentConfirm() {
  const n = BN_TBL_COMENT_CHANGES.size;
  // Aplicación inmediata via bn_tblTrackComentChange (autoguarda al blur);
  // este botón sólo cierra el modo de edición.
  BN_TBL_COMENT_EDIT_MODE = false;
  BN_TBL_COMENT_CHANGES.clear();
  bn_renderCards();
}

function bn_tblStartDescResize(ev) {
  ev.preventDefault(); ev.stopPropagation();
  const startX = ev.clientX;
  const startW = BN_TBL_DESC_WIDTH;
  const onMove = (e) => {
    BN_TBL_DESC_WIDTH = Math.max(120, Math.min(800, startW + (e.clientX - startX)));
    document.querySelectorAll('[data-bn-tbl-col="desc"]').forEach(el => {
      el.style.width = BN_TBL_DESC_WIDTH + 'px';
      el.style.maxWidth = BN_TBL_DESC_WIDTH + 'px';
    });
  };
  const onUp = () => {
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
  };
  document.addEventListener('mousemove', onMove);
  document.addEventListener('mouseup', onUp);
}

function bn_tblExitEditMode() {
  const n = BN_TBL_DESC_CHANGES.size;
  if (n > 0) {
    if (!confirm(`¿Salir sin guardar los ${n} cambio${n===1?'':'s'} pendiente${n===1?'':'s'}?`)) return;
  }
  BN_TBL_EDIT_MODE = false;
  BN_TBL_DESC_CHANGES.clear();
  bn_renderCards();
}

function bn_tblToggleEditMode() {
  if (!BN_TBL_EDIT_MODE) {
    BN_TBL_EDIT_MODE = true;
    BN_TBL_DESC_CHANGES.clear();
    bn_renderCards();
  } else {
    // Pedir confirmación; si acepta, guardar todos los cambios
    bn_tblShowSaveConfirm();
  }
}

function bn_tblTrackDescChange(idx, cellEl) {
  const rec = BN_CUR_RECS[idx];
  if (!rec || !rec.rowNum) return;
  const newVal = (cellEl.textContent || '').trim();
  const origVal = (cellEl.dataset.origDesc || '').trim();
  if (newVal === origVal) return;
  // Auto-guardar inmediatamente (sin esperar al botón Guardar)
  rec.DESCRIPCION = newVal;
  cellEl.dataset.origDesc = newVal;
  cellEl.title = newVal;
  cellEl.style.outline = '2px solid #64748b';
  (async () => {
    try {
      const payload = bn_buildSavePayload(rec, { descripcion_edit: true });
      payload.descripcion = newVal;
      const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const j = await resp.json();
      cellEl.style.outline = j.ok ? '2px solid #16a34a' : '2px solid #dc2626';
      setTimeout(() => { cellEl.style.outline = ''; }, 1200);
    } catch (_) {
      cellEl.style.outline = '2px solid #dc2626';
      setTimeout(() => { cellEl.style.outline = ''; }, 1500);
    }
  })();
}

function bn_tblShowSaveConfirm() {
  const n = BN_TBL_DESC_CHANGES.size;
  let overlay = document.getElementById('bn-tbl-save-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'bn-tbl-save-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px';
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  overlay.innerHTML = `
    <div style="background:#fff;border-radius:14px;width:100%;max-width:420px;padding:20px;position:relative;box-shadow:0 20px 60px rgba(15,23,42,.4)" onclick="event.stopPropagation()">
      <h3 style="margin:0 0 6px;font-size:16px">💾 Guardar cambios</h3>
      <p style="margin:0 0 14px;font-size:13px;color:#475569">
        Se guardarán <b>${n}</b> cambio${n===1?'':'s'} a la columna Descripción.
        ${n === 0 ? '<br><span style="color:#94a3b8">(No hay cambios pendientes)</span>' : ''}
      </p>
      <div style="display:flex;justify-content:flex-end;gap:8px">
        <button onclick="document.getElementById('bn-tbl-save-overlay').remove()"
                style="padding:8px 14px;border:1.5px solid #cbd5e1;background:#fff;color:#475569;border-radius:8px;font-weight:600;font-size:12px;cursor:pointer">Cancelar</button>
        <button onclick="bn_tblSaveAllDescChanges()"
                style="padding:8px 16px;border:none;background:#475569;color:#fff;border-radius:8px;font-weight:700;font-size:12px;cursor:pointer"${n===0?' disabled':''}>
          Guardar cambios
        </button>
      </div>
    </div>`;
}

async function bn_tblSaveAllDescChanges() {
  document.getElementById('bn-tbl-save-overlay')?.remove();
  const entries = [...BN_TBL_DESC_CHANGES.entries()];
  if (entries.length) {
    showLoading?.('Guardando ' + entries.length + ' cambios…', '');
    const { ok, fail } = await bn_runInBatches(entries, 6, async ([rowNum, newDesc]) => {
      // Localizar el rec por rowNum y actualizar en memoria
      const rec = BN_RAW.find(r => r.rowNum === rowNum);
      if (rec) rec.DESCRIPCION = newDesc;
      try {
        const payload = bn_buildSavePayload(rec || { rowNum }, { descripcion_edit: true });
        payload.descripcion = newDesc;
        const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const j = await resp.json();
        return !!j.ok;
      } catch (_) { return false; }
    });
    hideLoading?.();
    if (fail) alert(`Guardados: ${ok} · Fallidos: ${fail}`);
  }
  BN_TBL_EDIT_MODE = false;
  BN_TBL_DESC_CHANGES.clear();
  bn_renderCards();
}

/** Alterna entre vista de tarjetas y vista de tabla. */
function bn_setView(mode) {
  if (mode !== 'cards' && mode !== 'table') return;
  if (mode === BN_VIEW_MODE) return;
  BN_VIEW_MODE = mode;
  const cardsBtn = document.getElementById('bn-view-cards');
  const tableBtn = document.getElementById('bn-view-table');
  if (cardsBtn && tableBtn) {
    const onCss = 'background:#475569;color:#fff';
    const offCss = 'background:#fff;color:#374151';
    cardsBtn.style.cssText = (mode === 'cards')
      ? `padding:7px 12px;border:none;${onCss};font-weight:600;font-size:12px;cursor:pointer`
      : `padding:7px 12px;border:none;${offCss};font-weight:600;font-size:12px;cursor:pointer`;
    tableBtn.style.cssText = (mode === 'table')
      ? `padding:7px 12px;border:none;${onCss};font-weight:600;font-size:12px;cursor:pointer;border-left:1.5px solid var(--border,#d1d5db)`
      : `padding:7px 12px;border:none;${offCss};font-weight:600;font-size:12px;cursor:pointer;border-left:1.5px solid var(--border,#d1d5db)`;
  }
  BN_CARD_PAGE = 1;
  bn_renderCards();
}

/** Cambia el tamaño de página (registros visibles por página). */
function bn_setPageSize(v) {
  const n = Number(v) || 25;
  if (n === BN_CARD_SIZE) return;
  BN_CARD_SIZE = n;
  BN_CARD_PAGE = 1;
  bn_renderCards();
}

/** Tabla Resumen para un registro bancario. CONCEPTO es editable. */
function bn_buildBnResumenTable(r, idx) {
  // Orden y campos fijos — siempre se muestran todos, con o sin valor
  // Descripción es el único campo editable de texto libre
  const rows = [
    ['Descripción',      'DESC',    r.DESCRIPCION,                                               true ],
    ['Comentarios',      'COMENT',  r._comentarios  || '',                                       true ],
    ['Día',              'DÍA',     bn_formatDia(r.Día || r.Dia || '') || r.Mes || r.Año || '', false],
    ['Cuenta bancaria',  'CUENTA_B', r['Cuenta bancaria'],                                      false],
    ['Monto',            'MONTO',   r.Monto != null ? bn_fmt$(Number(r.Monto || 0)) : '',       false],
    ['Factura',          'FACTURA', r.FacturaFlag || '',                                         false],
    ['Deducible',        '_ded',    r._deducible    || '',                                       false],
    ['Reembolso',        '_reem',   r._reembolso    || '',                                       false],
    ['Reembolso a',      '_reema',  r._reembolso_a  || '',                                       false],
    ['Cuenta',           '_cuenta', r._cuenta       || '',                                       false],
    ['Subcuenta',        '_sub',    r._subcuenta     || '',                                       false],
    ['Categoría gasto',  '_cat',    r._categoria_gasto || '',                                    false],
    ['Concepto clasif.', '_con',    r._concepto     || '',                                       false],
    ['Propiedad',        '_prop',   r._propiedad    || '',                                       false],
    ['Departamento',     '_depto',  r._departamento !== undefined && r._departamento !== '' ? String(r._departamento) : '', false],
    ['Encargado',        '_enc',    r._encargado    || '',                                       false],
    ['Clasificado por',  '_clasif', r._clasificado_por || '',                                    false],
    ['Duda',             '_duda',   r._duda     === 'Sí' ? 'Sí' : '',                            false],
    ['Dudas texto',      'DUDANT',  r._duda_nota || '',                                          true ],
    ['Validado',         '_valid',  r._validado === 'Sí' ? 'Sí' : '',                            false],
  ];

  return `<table style="width:100%;table-layout:fixed;border-collapse:collapse;font-size:12px">
    <thead><tr><th style="width:38%;text-align:left;padding:6px 8px;font-size:11px">Campo</th><th style="text-align:left;padding:6px 8px;font-size:11px">Valor</th></tr></thead>
    <tbody>${rows
      .filter(([label, field, val, editable]) => {
        if (editable) return true;
        const display = val != null ? String(val).trim() : '';
        return display.length > 0;
      })
      .map(([label, field, val, editable]) => {
      const display = val != null ? String(val) : '';
      const tdCommon = 'padding:6px 8px;word-break:break-word;overflow-wrap:anywhere';
      const isComent  = (field === 'COMENT');
      const isDudaNt  = (field === 'DUDANT');
      const handlerInput =
        isComent ? `bn_onResumenComentEdit(${idx}, this)` :
        isDudaNt ? `bn_onResumenDudaNotaEdit(${idx}, this)` :
                   `bn_onResumenEdit(${idx}, this)`;
      const handlerBlur =
        isComent ? `bn_autoSaveResumenComent(${idx}, this)` :
        isDudaNt ? `bn_autoSaveResumenDudaNota(${idx}, this)` :
                   `bn_autoSaveResumenDesc(${idx}, this)`;
      const td = editable
        ? `<td contenteditable="true" spellcheck="false" data-field="${field}"
              style="${tdCommon}"
              oninput="${handlerInput}"
              onblur="${handlerBlur}">${esc(display)}</td>`
        : `<td style="${tdCommon}">${esc(display)}</td>`;
      return `<tr><td class="resumen-key" style="${tdCommon}">${label}</td>${td}</tr>`;
    }).join('')}</tbody>
  </table>`;
}

/** Crea el HTML de una tarjeta individual de registro bancario. */
function bn_createCard(rec, idx) {
  const montoN      = Number(rec.Monto || 0);
  const cuentaClasif = rec._cuenta || '';
  // _tipo siempre tiene un valor (cuenta clasificada o E/I por signo del monto)
  const tipoEfectivo = rec._tipo || (montoN < 0 ? 'Egresos' : montoN > 0 ? 'Ingresos' : '');
  const colorCls = CUENTA_COLOR_CLASS[tipoEfectivo] || '';
  // En "Por clasificar" no aplicar color tenue al header ni al monto;
  // el chip de CUENTA y la pestaña Clasificar conservan su color.
  const inPC     = (typeof BN_TIPO !== 'undefined') && bn_isPC && bn_isPC(BN_TIPO);
  const clsCls   = (colorCls && !inPC) ? `classified ${colorCls}` : '';
  const tip      = bn_canon(tipoEfectivo);
  const isE      = tip.includes('egr');
  const isI      = tip.includes('ing');
  const ci       = 'bn' + idx;

  const name     = bn_norm(rec.DESCRIPCION || rec.Concepto || rec['Cuenta bancaria'] || 'Movimiento');
  const diaFmt   = bn_formatDia(rec.Día || rec.Dia || '') || bn_norm(rec.Mes || rec.Año || '');
  const desc     = bn_norm(rec.DESCRIPCION || '');
  const dedChecked = rec._deducible === 'Sí';
  const monto  = Number(rec.Monto || 0);
  const cat    = bn_norm(rec.CATEGORIA || '');
  const con    = bn_norm(rec.CONCEPTO  || '');
  const cuenta = bn_norm(rec['Cuenta bancaria'] || '');
  const fac    = bn_norm(rec.Factura || '');
  const tipoLbl = cuentaClasif || (montoN < 0 ? 'Egreso' : montoN > 0 ? 'Ingreso' : '—');

  // Presupuesto y avance — usar valores absolutos para mostrar progreso positivo
  const tipo4bud = isE ? 'E' : 'I';
  const bud      = bn_getBud(tipo4bud, cat, con);
  const absMonto = Math.abs(monto);
  const absBud   = Math.abs(bud);
  const av       = absBud > 0 ? absMonto / absBud : NaN;
  const sev      = isE ? bn_sevOver(av) : bn_sevUnder(av);

  // Chips de encabezado — emoji por cuenta clasificada
  const CUENTA_EMOJI = {'Egresos':'💸','Ingresos':'💰','Activos':'📈','Pasivos':'📋','Capital':'💼'};
  const tipoEmoji  = CUENTA_EMOJI[cuentaClasif] || '🏦';
  const tipoChip   = `<span class="info-chip ${colorCls}">${tipoEmoji} ${esc(tipoLbl)}</span>`;
  // Chips informativos. Regla: arriba del Monto va sólo UNO:
  //   - Ingreso → Cuenta bancaria
  //   - Egreso → Método de pago
  // Ese mismo chip se OMITE del header-chips (no duplicar).
  const cuentaBancChipHtml = cuenta ? `<span class="info-chip" style="background:#f1f5f9;color:#334155">🏦 ${esc(cuenta)}</span>` : '';
  const mpChipHtml = rec._metodo_pago
    ? `<span class="info-chip" style="background:#dbeafe;color:#1e40af">💳 ${esc(rec._metodo_pago)}</span>`
    : `<span class="info-chip" style="background:#f1f5f9;color:#94a3b8;font-style:italic">Sin método</span>`;
  // Chip que va sobre el Monto y chips que van en el header
  const topChip = isI ? cuentaBancChipHtml : mpChipHtml;
  const cuentaBancChip = isI ? '' : cuentaBancChipHtml; // omitir si ya está arriba
  const mpChip        = isI ? mpChipHtml : '';          // sólo mostrar si NO está arriba
  const encChip = rec._encargado ? `<span class="info-chip" style="background:#e2e8f0;color:#334155">👤 ${esc(rec._encargado)}</span>` : '';
  const propChip = rec._propiedad ? `<span class="info-chip" style="background:#e0e7ff;color:#3730a3">🏠 ${esc(rec._propiedad)}</span>` : '';
  const deptChip = (rec._departamento !== undefined && rec._departamento !== '') ? `<span class="info-chip" style="background:#e0f2fe;color:#075985">🏢 Depto ${esc(String(rec._departamento))}</span>` : '';
  const facChip = fac
    ? `<span class="info-chip" style="background:#dcfce7;color:#166534">🧾 ${esc(fac)}</span>`
    : `<span class="info-chip" style="background:#f1f5f9;color:#94a3b8;font-style:italic">Sin factura</span>`;
  const reemChip = (rec._reembolso === 'Sí')
    ? `<span class="info-chip" style="background:#ede9fe;color:#5b21b6">↩️ Reembolso${rec._reembolso_a ? ' · ' + esc(rec._reembolso_a) : ''}</span>`
    : '';
  const isDuda = rec._duda === 'Sí';
  const isValidado = rec._validado === 'Sí';

  // Tab Clasificar
  const isClasif  = !!rec._cuenta;
  const pathParts = [rec._cuenta, rec._subcuenta, rec._categoria_gasto, rec._concepto].filter(Boolean);
  const pathText  = pathParts.join(' › ');
  // En "Por clasificar" la pestaña inferior tampoco lleva color (sólo el chip de CUENTA lo conserva)
  const clasifColorCls = (CUENTA_COLOR_CLASS[rec._cuenta] && !inPC) ? CUENTA_COLOR_CLASS[rec._cuenta] : '';
  // Color del texto de la pestaña: blanco cuando hay color de fondo (clasificado fuera de PC);
  // texto oscuro cuando la pestaña queda sin color (no clasificado o en Por clasificar).
  const tabTxtColor = clasifColorCls ? '#fff' : 'var(--text,#111827)';
  const tabHtml = isClasif
    ? `<div class="classify-tab classified ${clasifColorCls}" id="bn-btn-classify-${idx}" onclick="bn_toggleBnClassify(${idx})">
         <span class="classify-tab-arrow" style="color:${tabTxtColor}">›</span>
         <span class="classify-tab-label" style="font-size:10px;text-transform:none;letter-spacing:.01em;font-weight:700;color:${tabTxtColor};">${esc(pathText)}</span>
       </div>`
    : `<div class="classify-tab" id="bn-btn-classify-${idx}" onclick="bn_toggleBnClassify(${idx})">
         <span class="classify-tab-arrow">›</span>
         <span class="classify-tab-label">Clasificar</span>
       </div>`;

  const deptOptions = Array.from({length:14},(_,j)=>`<option>${j+1}</option>`).join('');

  // Mini barra semáforo de avance en encabezado (valores absolutos, % positivo).
  // 100% del presupuesto marcado al 50% del ancho de la barra (rango visual 0–200%).
  const avPct   = isFinite(av) ? Math.min(Math.max(av, 0), 2) : 0;
  const fillW   = (avPct / 2 * 100).toFixed(1); // 100% bud = 50% barra
  const fillCol = av > 1.10 ? '#dc2626' : av > 1.0 ? '#f59e0b' : '#16a34a';
  const avanceHtml = absBud > 0 ? `
    <div class="bn-hdr-avance" style="display:flex;flex-direction:column;align-items:flex-end;gap:3px;min-width:110px;margin-top:4px">
      <div style="position:relative;width:110px;height:7px;background:#e5e7eb;border-radius:4px;overflow:hidden">
        <div style="height:100%;width:${fillW}%;background:${fillCol};transition:width .3s"></div>
        <div title="100% del presupuesto" style="position:absolute;top:-2px;bottom:-2px;left:50%;width:2px;background:#1f2937;border-radius:1px;transform:translateX(-1px)"></div>
      </div>
      <span style="font-size:10px;font-weight:700;color:${fillCol};line-height:1">${isFinite(av) ? bn_fmtPct(av) : '—'} del presup.</span>
      <span style="font-size:9px;color:var(--text-soft);line-height:1">Pres: ${bn_fmt$(absBud)}</span>
    </div>` : '';

  return `
    <div class="ticket-card" id="bn-card-${idx}" style="position:relative;margin-bottom:6px${(BN_SEL_MODE && rec.rowNum && BN_SEL.has(rec.rowNum)) ? ';box-shadow:0 0 0 3px #334155,0 4px 12px rgba(234,88,12,.25);border-radius:12px' : ''}">
      ${BN_SEL_MODE ? `
      <!-- Checkbox de selección múltiple en esquina superior izquierda -->
      <label onclick="event.stopPropagation()"
             style="position:absolute;top:8px;left:8px;z-index:4;display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:6px;border:1.5px solid ${BN_SEL.has(rec.rowNum)?'#334155':'#cbd5e1'};background:${BN_SEL.has(rec.rowNum)?'#334155':'#fff'};color:#fff;cursor:pointer;font-size:14px;font-weight:900;line-height:1">
        <input type="checkbox" ${BN_SEL.has(rec.rowNum)?'checked':''}
               onchange="bn_selToggle(${rec.rowNum||0}, this.checked); bn_renderCards()"
               style="display:none">
        ${BN_SEL.has(rec.rowNum)?'✓':''}
      </label>` : ''}
      <!-- Nota de duda (arriba del botón ?) — clic para editar; sólo se renderiza si Duda está activa -->
      ${isDuda ? `
      <div id="bn-duda-note-${idx}"
           onclick="event.stopPropagation();bn_openDudaNotaEditor(${idx})"
           title="${esc(rec._duda_nota || 'Sin nota — clic para agregar')}"
           style="position:absolute;top:8px;right:42px;max-width:200px;padding:3px 8px;border-radius:6px;background:#fef3c7;color:#92400e;font-size:10px;font-weight:600;line-height:1.4;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;z-index:3;border:1px solid #f59e0b;cursor:pointer">📝 ${esc(rec._duda_nota || '(sin nota)')}</div>` : ''}
      <!-- Botón "Duda" (?) en esquina superior derecha. Marca registros con duda para revisar después. -->
      <button id="bn-check-${ci}" type="button"
              onclick="event.stopPropagation();bn_syncDuda(${idx}, !(this.dataset.checked==='true'))"
              data-checked="${isDuda}"
              title="${isDuda ? (rec._duda_nota || 'Marcado: Duda') : 'Duda'}"
              style="position:absolute;top:8px;right:8px;width:26px;height:26px;display:inline-flex;align-items:center;justify-content:center;border-radius:50%;border:1.5px solid ${isDuda ? '#f59e0b' : '#e5e7eb'};background:${isDuda ? '#fef3c7' : '#f9fafb'};color:${isDuda ? '#b45309' : '#d1d5db'};font-size:14px;font-weight:900;line-height:1;cursor:pointer;z-index:3;padding:0">?</button>
      <!-- Botón Validado (✓) debajo del Duda. Activo → registro sale de 'Por clasificar' -->
      <button id="bn-revisado-${ci}" type="button"
              onclick="event.stopPropagation();bn_syncValidado(${idx}, !(this.dataset.checked==='true'))"
              data-checked="${isValidado}"
              title="${isValidado ? 'Validado — disponible en Registros contables' : 'Marcar como Validado (sale de Por clasificar)'}"
              style="position:absolute;top:40px;right:8px;width:26px;height:26px;display:inline-flex;align-items:center;justify-content:center;border-radius:50%;border:1.5px solid ${isValidado ? '#16a34a' : '#e5e7eb'};background:${isValidado ? '#16a34a' : '#f9fafb'};color:${isValidado ? '#fff' : '#d1d5db'};font-size:14px;font-weight:900;line-height:1;cursor:pointer;z-index:3;padding:0">✓</button>
      <div class="ticket-card-header ${clsCls}" id="bn-hdr-${idx}" onclick="bn_toggleBnCard(${idx})" style="padding-right:46px">
        <div class="ticket-info">
          <div class="header-chips">${tipoChip}${cuentaBancChip}${encChip}${propChip}${deptChip}${facChip}${reemChip}</div>
          <div class="ticket-store-row">
            <span class="ticket-store ${colorCls}"
                  style="display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;text-overflow:ellipsis;white-space:normal;word-break:break-word;line-height:1.25">${esc(name)}</span>
          </div>
          <div class="ticket-meta">${esc(diaFmt)}</div>
          ${desc ? `<div class="product-summary">${esc(desc.length > 120 ? desc.slice(0, 117) + '…' : desc)}</div>` : ''}
        </div>
        <div class="ticket-header-right">
          <div style="margin-bottom:4px;display:flex;justify-content:flex-end">${topChip}</div>
          <div class="ticket-total-badge ${clsCls}">
            <span class="total-main">${bn_fmt$(monto)}</span>
          </div>
          ${avanceHtml}
          <div class="header-deducible" onclick="event.stopPropagation()">
            <label class="toggle-switch toggle-switch--dark">
              <input type="checkbox" id="deducible-header-${ci}"
                     ${dedChecked ? 'checked' : ''}
                     onchange="bn_syncDeducible(${idx}, this.checked)">
              <span class="toggle-slider"></span>
            </label>
            <span class="fhl${dedChecked ? ' on' : ''}" id="fhl-${ci}">${dedChecked ? 'Deducible' : 'No deducible'}</span>
          </div>
          ${bn_matchChipHtml(rec, idx)}
        </div>
      </div>

      <div class="ticket-table-wrap hidden" id="bn-tbl-${idx}" data-tidx="${idx}" data-tmod="bnr">
        <div id="bn-tab-resumen-${idx}" class="ticket-tab-content" style="overflow-x:auto">${bn_buildBnResumenTable(rec, idx)}</div>
        <div class="table-actions-bar hidden" id="tact-bnr-${idx}">
          <button class="btn-clasificar-ticket" style="padding:10px 28px;font-size:13px"
                  onclick="bn_saveBnResumenEdit(${idx})">💾 Guardar cambios</button>
          <button class="btn-limpiar-ticket" onclick="bn_resetBnResumenEdit(${idx})">Limpiar</button>
        </div>
      </div>

      ${tabHtml}
    </div>`;
}

function bn_toggleBnCard(idx) {
  const wrap   = document.getElementById(`bn-tbl-${idx}`);
  const header = document.getElementById(`bn-hdr-${idx}`);
  if (!wrap) return;
  const hidden = wrap.classList.toggle('hidden');
  header.classList.toggle('collapsed', hidden);
}

function bn_showBnTab(idx, tab, btn) {
  ['resumen'].forEach(t =>
    document.getElementById(`bn-tab-${t}-${idx}`)?.classList.toggle('hidden', t !== tab)
  );
  btn.closest('.ticket-tabs').querySelectorAll('.ticket-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
}

/** Abre el modal Clasificar (popup) con la tabla Resumen + panel Clasificar. */
function bn_toggleBnClassify(idx) {
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  const ci  = 'bn' + idx;

  // Construir HTML del panel Clasificar (sin la clase 'hidden' por defecto)
  const deptOptions = Array.from({length:14},(_,j)=>`<option>${j+1}</option>`).join('');
  const isClasif   = !!rec._cuenta;
  const classifyHtml = buildClassifyPanel(
    ci,
    bn_formatDiaISO(rec.Día || rec.Dia || ''),
    deptOptions,
    isClasif ? '💾 Guardar cambios' : '✓ Clasificar',
    `bn_saveBnClassification(${idx})`,
    `bn_limpiarBnClassification(${idx})`,
    'Fecha del registro'
  ).replace('class="classify-panel hidden"', 'class="classify-panel"');

  // Inyectar Resumen + Clasificar en el modal. Botones estilo encabezado.
  const valOn = rec._duda === 'Sí';
  const revOn = rec._validado === 'Sí';
  const validarBlock = `
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
      <button id="validado-panel-${ci}" type="button" data-checked="${valOn}"
              onclick="bn_syncDuda(${idx}, !(this.dataset.checked==='true'))"
              title="${valOn ? 'Marcado: Duda' : 'Duda'}"
              style="width:32px;height:32px;display:inline-flex;align-items:center;justify-content:center;border-radius:50%;border:1.5px solid ${valOn ? '#f59e0b' : '#e5e7eb'};background:${valOn ? '#fef3c7' : '#f9fafb'};color:${valOn ? '#b45309' : '#d1d5db'};font-size:17px;font-weight:900;line-height:1;cursor:pointer;padding:0;flex-shrink:0">?</button>
      <span style="font-size:12px;color:#6b7280">Marca este registro si tienes <b>dudas</b> y quieres revisarlo después</span>
    </div>
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
      <button id="revisado-panel-${ci}" type="button" data-checked="${revOn}"
              onclick="bn_syncValidado(${idx}, !(this.dataset.checked==='true'))"
              title="${revOn ? 'Validado — disponible en Registros contables' : 'Marcar como Validado'}"
              style="width:32px;height:32px;display:inline-flex;align-items:center;justify-content:center;border-radius:50%;border:1.5px solid ${revOn ? '#16a34a' : '#e5e7eb'};background:${revOn ? '#16a34a' : '#f9fafb'};color:${revOn ? '#fff' : '#d1d5db'};font-size:17px;font-weight:900;line-height:1;cursor:pointer;padding:0;flex-shrink:0">✓</button>
      <span style="font-size:12px;color:#6b7280">Marca como <b>Validado</b> para que el registro salga de <i>Por clasificar</i> y aparezca en su sección de Registros contables</span>
    </div>`;

  document.getElementById('bn-classify-modal-resumen').innerHTML =
    bn_buildBnResumenTable(rec, idx);
  document.getElementById('bn-classify-modal-body').innerHTML = classifyHtml;

  // Inyectar botones Duda/Validado en una fila SIEMPRE VISIBLE arriba de
  // classify-actions; el classify-actions sólo aparece cuando hay cambios.
  setTimeout(() => {
    const actions = document.getElementById(`classify-actions-${ci}`);
    if (actions) {
      // classify-actions queda hidden por defecto; markClassifyDirty lo muestra
      const dudaBtn = validarBlock.match(/<button id="validado-panel[\s\S]*?<\/button>/)?.[0] || '';
      const valBtn  = validarBlock.match(/<button id="revisado-panel[\s\S]*?<\/button>/)?.[0]  || '';
      const wrap = document.createElement('div');
      wrap.style.cssText = 'display:flex;align-items:center;gap:10px;margin:10px 0 0';
      wrap.innerHTML = dudaBtn + valBtn;
      actions.parentNode.insertBefore(wrap, actions);
      // Snapshot de la clasificación inicial para detectar 'dirty' real
      bn_classifyInitialSnapshot = bn_classifySnapshot(ci);
    }
  }, 0);

  // Mostrar modal
  document.getElementById('bn-classify-overlay').classList.remove('hidden');
  document.body.style.overflow = 'hidden';

  // Renderizar grid de Cuentas dinámicamente (igual que antes)
  bn_activateCatalog();
  const cuentaGrid = document.getElementById(`cuenta-grid-${ci}`);
  if (cuentaGrid) {
    const CUENTA_ICONS = { Egresos:'💸', Ingresos:'💰', Pasivos:'📋', Activos:'📈', Capital:'💼' };
    const CUENTA_SUBS  = { Egresos:'Gastos y pagos', Ingresos:'Cobros y entradas',
                           Pasivos:'Obligaciones',   Activos:'Inversión / CAPEX',  Capital:'Utilidad / Familiar' };
    const cuentas = Object.keys(BN_CATALOG);
    cuentaGrid.innerHTML = [
      `<div class="cuenta-card" data-value="" onclick="selectCuenta(this,'${ci}')">` +
        `<div class="cuenta-icon">🏠</div><div class="cuenta-label">Sin cuenta</div>` +
        `<div class="cuenta-sub">General</div></div>`,
      ...cuentas.map(c =>
        `<div class="cuenta-card" data-value="${esc(c)}" onclick="selectCuenta(this,'${ci}')">` +
        `<div class="cuenta-icon">${CUENTA_ICONS[c] || '📁'}</div>` +
        `<div class="cuenta-label">${esc(c)}</div>` +
        (CUENTA_SUBS[c] ? `<div class="cuenta-sub">${CUENTA_SUBS[c]}</div>` : '') +
        `</div>`)
    ].join('');
  }

  bn_autoPopulateBnClassify(ci, rec);

  // Pre-seleccionar Cuenta por tipo / signo si aún no está clasificado
  if (!rec._cuenta) {
    const tip = bn_canon(rec.CUENTA || rec.TIPO || '');
    const mN  = Number(rec.Monto || 0);
    const autoAcc = tip.includes('egr') ? 'Egresos'
                  : tip.includes('ing') ? 'Ingresos'
                  : mN < 0 ? 'Egresos' : 'Ingresos';
    const grid = document.getElementById(`cuenta-grid-${ci}`);
    const card = Array.from(grid?.querySelectorAll('.cuenta-card') || [])
      .find(c => c.dataset.value === autoAcc);
    if (card) selectCuenta(card, ci);
  }
}

/** Guarda solo la Descripción editada en el modal a Sheets (sin tocar clasificación). */
async function bn_saveBnDescFromModal(idx) {
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  const wrap   = document.getElementById('bn-classify-modal-resumen');
  const descEl = wrap?.querySelector('[data-field="DESC"]');
  if (!descEl) return;
  rec.DESCRIPCION = descEl.textContent.trim();

  // Re-render tarjeta (encabezado refleja nueva Descripción)
  const card = document.getElementById(`bn-card-${idx}`);
  if (card) card.outerHTML = bn_createCard(rec, idx);

  if (!rec.rowNum) return;
  try {
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rowNum:           rec.rowNum,
        descripcion:      bn_norm(rec.DESCRIPCION || ''),
        descripcion_edit: true,
        clasificacion: {
          cuenta:          rec._cuenta          || '',
          subcuenta:       rec._subcuenta       || '',
          categoria_gasto: rec._categoria_gasto || '',
          concepto:        rec._concepto        || '',
          propiedad:       rec._propiedad       || '',
          departamento:    rec._departamento    || '',
          encargado:       rec._encargado       || '',
          deducible:       rec._deducible       || 'No',
          reembolso:       rec._reembolso       || 'No',
          reembolso_a:     rec._reembolso_a     || '',
          metodo_pago:     rec._metodo_pago     || '',
          clasificado_por: currentUser || '',
          duda:            rec._duda || '',
        }
      })
    });
    const result = await resp.json();
    if (!result.ok) throw new Error(result.error || 'Error');
  } catch (e) {
    console.warn('Error guardando Descripción:', e.message);
    alert('Error al guardar Descripción: ' + e.message);
  }
}

/** Cierra el modal de Indicadores. */
function bn_closeIndicadoresModal() {
  const iov = document.getElementById('bn-indicadores-overlay');
  if (!iov) return;
  iov.classList.add('hidden');
  document.body.style.overflow = '';
  // Volver al último tab no-F para no quedar en F y reaparecer al re-renderizar
  if (BN_TIPO === 'F') {
    BN_TIPO = 'T';
    bn_setTipo('T');
  }
}

/** Click en backdrop del modal Indicadores → cerrar. */
function bn_indicadoresOverlayClick(e) {
  if (e.target && e.target.id === 'bn-indicadores-overlay') bn_closeIndicadoresModal();
}

// Esc cierra el modal Indicadores
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    const ov = document.getElementById('bn-indicadores-overlay');
    if (ov && !ov.classList.contains('hidden')) bn_closeIndicadoresModal();
  }
});

/** Cierra el modal Clasificar y limpia su contenido. */
function bn_closeClassifyModal() {
  const ov = document.getElementById('bn-classify-overlay');
  if (!ov) return;
  ov.classList.add('hidden');
  document.body.style.overflow = '';
  document.getElementById('bn-classify-modal-resumen').innerHTML = '';
  document.getElementById('bn-classify-modal-body').innerHTML    = '';
}

/** Click en el backdrop del modal — cerrar (clic fuera del cuadro). */
function bn_classifyOverlayClick(e) {
  if (e.target && e.target.id === 'bn-classify-overlay') bn_closeClassifyModal();
}

// Tecla Esc cierra el modal Clasificar (desktop)
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    const ov = document.getElementById('bn-classify-overlay');
    if (ov && !ov.classList.contains('hidden')) bn_closeClassifyModal();
  }
});

/** Guarda las ediciones del campo Concepto de la tabla Resumen en memoria. */
async function bn_saveBnResumenEdit(idx) {
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  const tabEl = document.getElementById(`bn-tab-resumen-${idx}`);
  const descEl = tabEl?.querySelector('[data-field="DESC"]');
  if (descEl) rec.DESCRIPCION = descEl.textContent.trim();
  hideTableActions('bnr', idx);
  // Re-render para reflejar la nueva descripción en el encabezado
  const card = document.getElementById(`bn-card-${idx}`);
  if (card) {
    card.outerHTML = bn_createCard(rec, idx);
    document.getElementById(`bn-tbl-${idx}`)?.classList.remove('hidden');
  }
  // Persistir a Google Sheets
  if (!rec.rowNum) return;
  try {
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rowNum:          rec.rowNum,
        mes:             bn_norm(rec.Mes),
        cuenta_bancaria: bn_norm(rec['Cuenta bancaria'] || ''),
        cuenta:          bn_norm(rec.CUENTA    || ''),
        subcuenta:       bn_norm(rec.SUBCUENTA || ''),
        categoria:       bn_norm(rec.CATEGORIA || ''),
        concepto:        bn_norm(rec.CONCEPTO  || ''),
        descripcion:     bn_norm(rec.DESCRIPCION || ''),
        descripcion_edit: true,
        monto:           Number(rec.Monto || 0),
        clasificacion: {
          cuenta:          rec._cuenta          || '',
          subcuenta:       rec._subcuenta       || '',
          categoria_gasto: rec._categoria_gasto || '',
          concepto:        rec._concepto        || '',
          propiedad:       rec._propiedad       || '',
          departamento:    rec._departamento    || '',
          encargado:       rec._encargado       || '',
          deducible:       rec._deducible       || 'No',
          reembolso:       rec._reembolso       || 'No',
          reembolso_a:     rec._reembolso_a     || '',
          metodo_pago:     rec._metodo_pago     || '',
          clasificado_por: currentUser || '',
        }
      }),
    });
    const result = await resp.json();
    if (!result.ok) throw new Error(result.error || 'Error');
  } catch (e) {
    console.warn('Error guardando Descripción:', e.message);
    alert('Error al guardar Descripción: ' + e.message);
  }
}

/** Revierte las ediciones sin guardar de la Descripción. */
function bn_resetBnResumenEdit(idx) {
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  const tabEl = document.getElementById(`bn-tab-resumen-${idx}`);
  const descEl = tabEl?.querySelector('[data-field="DESC"]');
  if (descEl) descEl.textContent = rec.DESCRIPCION || '';
  hideTableActions('bnr', idx);
}

/** Rellena el panel Clasificar con los datos guardados del registro. */
function bn_autoPopulateBnClassify(ci, rec) {
  if (!rec) return;
  classifyAutoPopulating = true;

  // Helpers de match laxo: normaliza Cuenta a forma canónica (plural)
  // y usa bn_canon (case/acentos insensitive) para sub/cat/concepto.
  const canonCta = (s) => {
    const c = bn_canon(s || '');
    if (c.startsWith('egr'))     return 'egresos';
    if (c.startsWith('ing'))     return 'ingresos';
    if (c.startsWith('activ'))   return 'activos';
    if (c.startsWith('pasiv'))   return 'pasivos';
    if (c.startsWith('capital')) return 'capital';
    return c;
  };
  const findCardByVal = (gridEl, val, normFn = bn_canon) => {
    if (!gridEl || !val) return null;
    const target = normFn(val);
    return Array.from(gridEl.querySelectorAll('.cuenta-card'))
      .find(c => normFn(c.dataset.value || '') === target);
  };

  const cuentaVal = rec._cuenta || '';
  if (cuentaVal) {
    const grid = document.getElementById(`cuenta-grid-${ci}`);
    const card = findCardByVal(grid, cuentaVal, canonCta);
    if (card) {
      selectCuenta(card, ci);
      const subVal = rec._subcuenta || '';
      if (subVal) {
        const subCard = findCardByVal(document.getElementById(`subcuenta-grid-${ci}`), subVal);
        if (subCard) {
          selectSubcuenta(subCard, ci);
          const catVal = rec._categoria_gasto || '';
          if (catVal) {
            const catCard = findCardByVal(document.getElementById(`categoria-grid-${ci}`), catVal);
            if (catCard) {
              selectCategoria(catCard, ci);
              const conVal = rec._concepto || '';
              if (conVal) {
                const conCard = findCardByVal(document.getElementById(`concepto-grid-${ci}`), conVal);
                if (conCard) selectConcepto(conCard, ci);
              }
            }
          }
        }
      }
    }
  }

  // Propiedad
  const propSel = document.getElementById(`propiedad-${ci}`);
  if (propSel && rec._propiedad) {
    const exists = Array.from(propSel.options).some(o => o.value === rec._propiedad);
    if (exists) propSel.value = rec._propiedad;
    else { propSel.value = 'Otro'; togglePropiedadOtro(ci, 'Otro');
      const oEl = document.getElementById(`propiedad-otro-${ci}`); if (oEl) oEl.value = rec._propiedad; }
  }

  // Departamento
  const dEl = document.getElementById(`departamento-${ci}`);
  if (dEl && rec._departamento) dEl.value = rec._departamento;

  // Encargado (comprador)
  if (rec._encargado) {
    const compGrid = document.getElementById(`comprador-grid-${ci}`);
    const compCard = Array.from(compGrid?.querySelectorAll('.cuenta-card') || [])
      .find(c => c.dataset.value === rec._encargado);
    if (compCard) selectComprador(compCard, ci);
  }

  // Deducible
  const dedEl = document.getElementById(`deducible-${ci}`);
  const dedChecked = rec._deducible === 'Sí';
  if (dedEl) { dedEl.checked = dedChecked; updateDeducibleLabel(ci, dedChecked); }

  // Reembolso
  if (rec._reembolso === 'Sí') {
    const reemEl = document.getElementById(`reembolso-${ci}`);
    if (reemEl) { reemEl.checked = true; toggleReembolso(ci, true); }
    if (rec._reembolso_a) {
      const reemSel = document.getElementById(`reembolso-a-${ci}`);
      if (reemSel) {
        const exists = Array.from(reemSel.options).some(o => o.value === rec._reembolso_a);
        if (exists) reemSel.value = rec._reembolso_a;
        else { reemSel.value = 'Otro'; toggleReembolsoOtro(ci, 'Otro');
          const oEl = document.getElementById(`reembolso-otro-${ci}`); if (oEl) oEl.value = rec._reembolso_a; }
      }
    }
  }

  // Método de pago
  if (rec._metodo_pago) {
    const metGrid = document.getElementById(`metodo-grid-${ci}`);
    const metCard = Array.from(metGrid?.querySelectorAll('.cuenta-card') || [])
      .find(c => c.dataset.value === rec._metodo_pago);
    if (metCard) {
      selectMetodoPago(metCard, ci);
    } else {
      // Valor libre — activa la opción "Otro" y rellena el input
      const otroCard = metGrid?.querySelector('.cuenta-card[data-value="Otro"]');
      if (otroCard) {
        selectMetodoPago(otroCard, ci);
        const inp = document.getElementById(`metodo-otro-${ci}`);
        if (inp) { inp.value = rec._metodo_pago; bn_metodoOtroInput(ci, inp); }
      }
    }
  }

  // Comentarios — restaurar el valor guardado al re-abrir el popup
  const comEl = document.getElementById(`comentarios-${ci}`);
  if (comEl) comEl.value = rec._comentarios || '';

  updateClasiPath(ci);
  classifyAutoPopulating = false;
}

/** Guarda la clasificación del panel en el registro en memoria y en el backend. */
async function bn_saveBnClassification(idx) {
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  if (!rec.rowNum) {
    alert(`Error: el registro no tiene número de fila (rowNum=${rec.rowNum}).\nPor favor recarga los datos con el botón ↺ y vuelve a intentarlo.`);
    return;
  }
  const ci = 'bn' + idx;
  const c  = getClassify(ci);

  // Si el modal está abierto, capturar la Descripción editada en la tabla Resumen
  let descEdited = false;
  const modalResumen = document.getElementById('bn-classify-modal-resumen');
  const ov = document.getElementById('bn-classify-overlay');
  if (modalResumen && ov && !ov.classList.contains('hidden')) {
    const descEl = modalResumen.querySelector('[data-field="DESC"]');
    if (descEl) {
      const nuevaDesc = descEl.textContent.trim();
      if (nuevaDesc !== (rec.DESCRIPCION || '')) {
        rec.DESCRIPCION = nuevaDesc;
        descEdited = true;
      }
    }
  }

  rec._cuenta          = c.cuenta;
  rec._subcuenta       = c.subcuenta;
  rec._categoria_gasto = c.categoria;
  rec._concepto        = c.concepto;
  rec._propiedad       = c.propiedad;
  rec._departamento    = c.departamento;
  rec._encargado       = c.comprador;
  rec._deducible       = c.deducible  ? 'Sí' : 'No';
  rec._reembolso       = c.reembolso  ? 'Sí' : 'No';
  rec._reembolso_a     = c.reembolso_a;
  rec._metodo_pago     = c.metodo_pago_clasif;
  rec._comentarios     = c.comentarios || '';
  rec._clasificado_por = currentUser || '';
  // Sincronizar campos raw y _tipo para que filtros y colores sean correctos
  rec.CUENTA    = c.cuenta;
  rec.SUBCUENTA = c.subcuenta;
  rec.CATEGORIA = c.categoria;
  rec.CONCEPTO  = c.concepto;
  const monto   = Number(rec.Monto) || 0;
  const _rawT2  = rec._cuenta || rec.TIPO || (monto < 0 ? 'Egresos' : monto > 0 ? 'Ingresos' : '');
  const _ct2    = bn_canon(_rawT2);
  rec._tipo     = _ct2.includes('egr') ? 'Egresos' : _ct2.includes('ing') ? 'Ingresos' :
                  _ct2.includes('activ') ? 'Activos' : _ct2.includes('pasiv') ? 'Pasivos' :
                  _ct2.includes('capital') ? 'Capital' : _rawT2;

  // Re-render: actualizar SIEMPRE la vista activa (cards o tabla)
  // UI instantánea: cierra modal + re-renderiza ANTES del fetch
  bn_closeClassifyModal();
  bn_renderCards();

  // Persistencia en background (sin bloquear ni mostrar spinner global)
  // Si falla, avisamos pero la UI ya quedó actualizada.
  fetch(`${BACKEND}/save-banco-clasificacion`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      rowNum:          rec.rowNum,
      mes:             bn_norm(rec.Mes),
      cuenta_bancaria: bn_norm(rec['Cuenta bancaria'] || ''),
      cuenta:          bn_norm(rec.CUENTA     || ''),
      subcuenta:       bn_norm(rec.SUBCUENTA  || ''),
      categoria:       bn_norm(rec.CATEGORIA  || ''),
      concepto:        bn_norm(rec.CONCEPTO   || ''),
      descripcion:     bn_norm(rec.DESCRIPCION|| ''),
      descripcion_edit: descEdited,
      monto:           Number(rec.Monto || 0),
      clasificacion: {
        cuenta:          c.cuenta,
        subcuenta:       c.subcuenta,
        categoria_gasto: c.categoria,
        concepto:        c.concepto,
        propiedad:       c.propiedad,
        departamento:    c.departamento,
        encargado:       c.comprador,
        deducible:       c.deducible ? 'Sí' : 'No',
        reembolso:       c.reembolso ? 'Sí' : 'No',
        reembolso_a:     c.reembolso_a,
        metodo_pago:     c.metodo_pago_clasif,
        clasificado_por: currentUser || '',
        duda:            rec._duda || '',
        validado:        rec._validado || '',
        comentarios:     c.comentarios || '',
      }
    }),
  })
    .then(r => r.json())
    .then(result => { if (!result.ok) throw new Error(result.error || 'Error desconocido'); })
    .catch(e => {
      console.warn('Error guardando clasificación:', e.message);
      alert('⚠ Error al guardar en Sheets (la UI ya está actualizada): ' + e.message);
    });
}

/** Limpia la clasificación del registro y re-renderiza la tarjeta. */
async function bn_limpiarBnClassification(idx) {
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  // Limpiar memoria local y columnas raw
  ['_cuenta','_subcuenta','_categoria_gasto','_concepto','_propiedad',
   '_departamento','_encargado','_deducible','_reembolso','_reembolso_a',
   '_metodo_pago','_clasificado_por'].forEach(k => delete rec[k]);
  rec.CUENTA = ''; rec.SUBCUENTA = ''; rec.CATEGORIA = ''; rec.CONCEPTO = '';
  // Recalcular _tipo desde TIPO/signo de monto
  const monto = Number(rec.Monto) || 0;
  const _raw  = rec.TIPO || (monto < 0 ? 'Egresos' : monto > 0 ? 'Ingresos' : '');
  const _c    = bn_canon(_raw);
  rec._tipo = _c.includes('egr') ? 'Egresos' : _c.includes('ing') ? 'Ingresos' : _raw;
  // Re-render tarjeta
  const card = document.getElementById(`bn-card-${idx}`);
  if (card) card.outerHTML = bn_createCard(rec, idx);

  // Persistir limpieza en Sheets (envía valores vacíos)
  if (!rec.rowNum) return;
  try {
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rowNum:        rec.rowNum,
        clasificacion: {
          cuenta:'', subcuenta:'', categoria_gasto:'', concepto:'',
          propiedad:'', departamento:'', encargado:'',
          deducible:'', reembolso:'', reembolso_a:'',
          metodo_pago:'', clasificado_por:'',
          duda: rec._duda || '',
        }
      }),
    });
    const result = await resp.json();
    if (!result.ok) throw new Error(result.error || 'Error');
  } catch (e) {
    console.warn('Error limpiando clasificación:', e.message);
    alert('Error al limpiar en Sheets: ' + e.message);
  }
}

/** Renderiza las tarjetas de la página actual. */
// Vista cronológica activa por defecto (siempre on)
let BN_TIMELINE = true;

// ─── Selección múltiple y acciones masivas ──────────────────────────────────
let BN_SEL_MODE = false;
const BN_SEL = new Set(); // contiene rowNum's seleccionados (no idx, que cambian al re-render)

/** Toggle del modo de selección múltiple. */
function bn_toggleSelMode() {
  BN_SEL_MODE = !BN_SEL_MODE;
  const btn  = document.getElementById('bn-btn-sel-mode');
  const btnA = document.getElementById('bn-btn-sel-all');
  const btnC = document.getElementById('bn-btn-sel-clear');
  const cnt  = document.getElementById('bn-sel-count');
  const bar  = document.getElementById('bn-bulk-bar');
  if (BN_SEL_MODE) {
    btn.style.background = '#334155'; btn.style.color = '#fff'; btn.style.borderColor = '#334155';
    btn.innerHTML = '☑️ Seleccionando…';
    btnA?.classList.remove('hidden'); btnC?.classList.remove('hidden'); cnt?.classList.remove('hidden');
  } else {
    btn.style.background = '#fff'; btn.style.color = '#374151'; btn.style.borderColor = '#d1d5db';
    btn.innerHTML = '☑️ Seleccionar';
    btnA?.classList.add('hidden'); btnC?.classList.add('hidden'); cnt?.classList.add('hidden');
    BN_SEL.clear();
    bar?.classList.add('hidden');
  }
  bn_renderCards();
  bn_updateSelUI();
}

/** Marca/desmarca un registro por su rowNum. */
function bn_selToggle(rowNum, checked) {
  if (checked) BN_SEL.add(rowNum); else BN_SEL.delete(rowNum);
  bn_updateSelUI();
}

/** Selecciona todos los registros visibles en BN_CUR_RECS. */
function bn_selAllVisible() {
  for (const r of BN_CUR_RECS) if (r.rowNum) BN_SEL.add(r.rowNum);
  bn_renderCards();
  bn_updateSelUI();
}

/** Limpia la selección. */
function bn_selClear() {
  BN_SEL.clear();
  bn_renderCards();
  bn_updateSelUI();
}

/** Actualiza la barra flotante y el contador. */
function bn_updateSelUI() {
  const cnt = document.getElementById('bn-sel-count');
  const bar = document.getElementById('bn-bulk-bar');
  const bulkCnt = document.getElementById('bn-bulk-count');
  const n = BN_SEL.size;
  if (cnt) cnt.textContent = n ? `${n} seleccionado${n>1?'s':''}` : '';
  if (bulkCnt) bulkCnt.textContent = `${n} reg.`;
  if (bar) bar.classList.toggle('hidden', !(BN_SEL_MODE && n > 0));
}

/** Devuelve los registros seleccionados (con rowNum válido). */
function bn_selectedRecs() {
  return BN_RAW.filter(r => r.rowNum && BN_SEL.has(r.rowNum));
}

/** Construye payload base para guardar clasificación de un registro. */
function bn_buildSavePayload(rec, extras = {}) {
  // Incluye campos top-level que el Apps Script lee directamente:
  // data.validado, data.duda, data.descripcion, data.dia
  return {
    rowNum:          rec.rowNum,
    // Top-level (los flags *_edit del Apps Script los leen desde aquí)
    validado:        rec._validado    || '',
    duda:            rec._duda        || '',
    descripcion:     rec.DESCRIPCION  || '',
    dia:             bn_formatDiaISO(rec.Día || rec.Dia || '') || '',
    clasificacion: {
      cuenta:          rec._cuenta          || '',
      subcuenta:       rec._subcuenta       || '',
      categoria_gasto: rec._categoria_gasto || '',
      concepto:        rec._concepto        || '',
      propiedad:       rec._propiedad       || '',
      departamento:    rec._departamento    || '',
      encargado:       rec._encargado       || '',
      deducible:       rec._deducible       || 'No',
      reembolso:       rec._reembolso       || 'No',
      reembolso_a:     rec._reembolso_a     || '',
      metodo_pago:     rec._metodo_pago     || '',
      clasificado_por: currentUser || '',
      duda:            rec._duda     || '',
      duda_nota:       rec._duda_nota || '',
      validado:        rec._validado || '',
      comentarios:     rec._comentarios || '',
    },
    ...extras
  };
}

/** Limita la concurrencia de promesas a 'limit' simultáneas. */
async function bn_runInBatches(items, limit, worker) {
  let ok = 0, fail = 0;
  for (let i = 0; i < items.length; i += limit) {
    const batch = items.slice(i, i + limit);
    const results = await Promise.allSettled(batch.map(worker));
    for (const r of results) {
      if (r.status === 'fulfilled' && r.value === true) ok++;
      else fail++;
    }
  }
  return { ok, fail };
}

async function bn_bulkSaveAll(updater, payloadKey) {
  const recs = bn_selectedRecs();
  if (!recs.length) { alert('No hay registros seleccionados'); return; }
  showLoading?.('Aplicando a ' + recs.length + ' registros…', 'Actualizando Sheets…');
  // Aplicar updater a TODOS en memoria primero (para que el payload de cada uno
  // refleje los cambios correctamente al construirse).
  for (const rec of recs) { try { updater(rec); } catch(_) {} }

  // Llamadas en paralelo (lotes de 6) para acelerar significativamente.
  const { ok, fail } = await bn_runInBatches(recs, 6, async (rec) => {
    const payload = bn_buildSavePayload(rec, payloadKey ? { [payloadKey]: true } : {});
    try {
      const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const j = await resp.json();
      return !!j.ok;
    } catch (_) { return false; }
  });
  hideLoading?.();
  bn_render();
  alert(`Guardados: ${ok}${fail ? ' · Fallidos: ' + fail : ''}`);
}

async function bn_bulkToggleDuda() {
  const recs = bn_selectedRecs();
  if (!recs.length) return;
  // Decide nuevo estado: si TODOS están validados → desactivar; sino → activar
  const allSi = recs.every(r => r._duda === 'Sí');
  const target = allSi ? 'No' : 'Sí';
  await bn_bulkSaveAll(rec => { rec._duda = target; }, 'duda_edit');
}

async function bn_bulkToggleValidar() {
  const recs = bn_selectedRecs();
  if (!recs.length) return;
  const allSi = recs.every(r => r._validado === 'Sí');
  const target = allSi ? 'No' : 'Sí';
  await bn_bulkSaveAll(rec => { rec._validado = target; }, 'validado_edit');
}

async function bn_bulkToggleDeducible() {
  const recs = bn_selectedRecs();
  if (!recs.length) return;
  const allSi = recs.every(r => r._deducible === 'Sí');
  const target = allSi ? 'No' : 'Sí';
  await bn_bulkSaveAll(rec => { rec._deducible = target; });
}

// ─── Mini-modal genérico para elegir una opción en bulk actions ─────────────
function bn_bulkShowChoice(opts) {
  // opts = { title, items:[{value,label,icon?}], onPick }
  let overlay = document.getElementById('bn-bulk-choice-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'bn-bulk-choice-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px';
    overlay.onclick = (e) => { if (e.target === overlay) bn_bulkChoiceClose(); };
    document.body.appendChild(overlay);
  }
  const itemsHtml = opts.items.map(it => `
    <button onclick="bn_bulkChoicePick(${JSON.stringify(it.value).replace(/"/g,'&quot;')})"
            style="display:flex;flex-direction:column;align-items:center;gap:6px;padding:14px 10px;border:1.5px solid #e5e7eb;background:#fff;border-radius:10px;cursor:pointer;font-size:12px;font-weight:600;color:#374151;transition:all .15s"
            onmouseover="this.style.borderColor='#334155';this.style.background='#f1f5f9'"
            onmouseout="this.style.borderColor='#e5e7eb';this.style.background='#fff'">
      ${it.icon ? `<span style="font-size:24px;line-height:1">${it.icon}</span>` : ''}
      <span style="text-align:center">${esc(it.label)}</span>
    </button>`).join('');
  overlay.innerHTML = `
    <div style="background:#fff;border-radius:14px;width:100%;max-width:520px;max-height:90vh;overflow-y:auto;padding:20px;position:relative;box-shadow:0 20px 60px rgba(0,0,0,.4)" onclick="event.stopPropagation()">
      <button onclick="bn_bulkChoiceClose()" style="position:absolute;top:10px;right:10px;width:32px;height:32px;border:none;background:#f3f4f6;border-radius:50%;font-size:18px;cursor:pointer;color:#374151">✕</button>
      <h3 style="margin:0 0 6px;font-size:16px">${esc(opts.title)}</h3>
      <p style="margin:0 0 14px;font-size:12px;color:#6b7280">Aplicar a <b style="color:#334155">${BN_SEL.size}</b> registros seleccionados</p>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px">${itemsHtml}</div>
    </div>`;
  // Guardar callback en el overlay
  overlay._onPick = opts.onPick;
}

function bn_bulkChoiceClose() {
  const ov = document.getElementById('bn-bulk-choice-overlay');
  if (ov) ov.remove();
}

function bn_bulkChoicePick(val) {
  const ov = document.getElementById('bn-bulk-choice-overlay');
  const cb = ov?._onPick;
  bn_bulkChoiceClose();
  if (typeof cb === 'function') cb(val);
}

// Mini-modal para elegir fecha con calendario nativo
function bn_bulkShowDate(title, onPick) {
  let overlay = document.getElementById('bn-bulk-date-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'bn-bulk-date-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px';
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }
  const today = new Date();
  const iso = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
  overlay.innerHTML = `
    <div style="background:#fff;border-radius:14px;width:100%;max-width:380px;padding:20px;position:relative;box-shadow:0 20px 60px rgba(0,0,0,.4)" onclick="event.stopPropagation()">
      <button onclick="document.getElementById('bn-bulk-date-overlay').remove()" style="position:absolute;top:10px;right:10px;width:32px;height:32px;border:none;background:#f3f4f6;border-radius:50%;font-size:18px;cursor:pointer;color:#374151">✕</button>
      <h3 style="margin:0 0 6px;font-size:16px">${esc(title)}</h3>
      <p style="margin:0 0 14px;font-size:12px;color:#6b7280">Aplicar a <b style="color:#334155">${BN_SEL.size}</b> registros</p>
      <input type="date" id="bn-bulk-date-input" value="${iso}"
             style="width:100%;padding:10px;border:1.5px solid #d1d5db;border-radius:8px;font-size:14px;margin-bottom:14px">
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button onclick="document.getElementById('bn-bulk-date-overlay').remove()" style="padding:8px 14px;border:1.5px solid #d1d5db;background:#fff;color:#374151;border-radius:8px;font-weight:600;font-size:12px;cursor:pointer">Cancelar</button>
        <button onclick="bn_bulkDatePick()" style="padding:8px 14px;border:none;background:#334155;color:#fff;border-radius:8px;font-weight:600;font-size:12px;cursor:pointer">Aplicar</button>
      </div>
    </div>`;
  overlay._onPick = onPick;
}

function bn_bulkDatePick() {
  const ov = document.getElementById('bn-bulk-date-overlay');
  const v  = document.getElementById('bn-bulk-date-input')?.value;
  const cb = ov?._onPick;
  if (!v) { alert('Selecciona una fecha'); return; }
  ov?.remove();
  if (typeof cb === 'function') cb(v);
}

// Opciones (mismas que en classify panel)
const BN_METODOS_PAGO = [
  { value: 'Tarjeta crédito',       label: 'Crédito',           icon: '💳' },
  { value: 'Tarjeta débito',        label: 'Débito',            icon: '🏦' },
  { value: 'Efectivo',              label: 'Efectivo',          icon: '💵' },
  { value: 'Transferencia',         label: 'Transfer.',         icon: '🔄' },
  { value: 'Retiro sin tarjeta',    label: 'Retiro s/tarjeta',  icon: '🏧' },
  { value: 'Cheque',                label: 'Cheque',            icon: '📝' },
  { value: 'QR',                    label: 'QR',                icon: '📱' },
  { value: 'BBVA ACR Empresarial',  label: 'ACR Empresarial',   icon: '🏦' },
  { value: 'BBVA ACR TC Platino',   label: 'ACR TC Platino',    icon: '💳' },
  { value: 'BBVA ACL TC Platino',   label: 'ACL TC Platino',    icon: '💳' },
  { value: 'BBVA ACL TC Azul',      label: 'ACL TC Azul',       icon: '💳' },
  { value: 'BBVA ACL Libreton',     label: 'ACL Libreton',      icon: '📖' },
  { value: 'BBVA JJLC Empresarial', label: 'JJLC Empresarial',  icon: '🏦' },
  { value: 'BBVA ACR Libreton',     label: 'ACR Libreton',      icon: '📖' },
  { value: 'CCL',                   label: 'CCL',               icon: '💵' },
  { value: 'Otro',                  label: 'Otro',              icon: '✏️' },
];

const BN_ENCARGADOS = [
  { value: 'Andrés',                 label: 'Andrés',                 icon: '👨' },
  { value: 'Claudia',                label: 'Claudia',                icon: '👩' },
  { value: 'Papá',                   label: 'Papá',                   icon: '👨‍👧' },
  { value: 'Francisco',              label: 'Francisco',              icon: '👨' },
  { value: 'Brenda',                 label: 'Brenda',                 icon: '👩' },
  { value: 'Alma',                   label: 'Alma',                   icon: '👩' },
  { value: 'Gaby',                   label: 'Gaby',                   icon: '👩' },
  { value: 'Juanita',                label: 'Juanita',                icon: '👩' },
  { value: 'Damariz',                label: 'Damariz',                icon: '👩' },
  { value: 'Auxiliar administrativo', label: 'Aux. admin.',           icon: '👔' },
];

function bn_bulkPromptMetodo() {
  if (!BN_SEL.size) { alert('No hay registros seleccionados'); return; }
  bn_bulkShowChoice({
    title: '💳 Método de pago',
    items: BN_METODOS_PAGO,
    onPick: (val) => bn_bulkSaveAll(rec => { rec._metodo_pago = val; })
  });
}

function bn_bulkPromptEncargado() {
  if (!BN_SEL.size) { alert('No hay registros seleccionados'); return; }
  bn_bulkShowChoice({
    title: '👤 Encargado de operación',
    items: BN_ENCARGADOS,
    onPick: (val) => bn_bulkSaveAll(rec => { rec._encargado = val; })
  });
}

function bn_bulkPromptFecha() {
  if (!BN_SEL.size) { alert('No hay registros seleccionados'); return; }
  bn_bulkShowDate('📅 Fecha del registro', async (v) => {
    const recs = bn_selectedRecs();
    showLoading?.('Aplicando fecha a ' + recs.length + ' registros…', '');
    let ok = 0, fail = 0;
    for (const rec of recs) {
      try {
        rec.Día = v;
        const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ rowNum: rec.rowNum, fecha_edit: true, dia: v }),
        });
        const j = await resp.json();
        if (j.ok) ok++; else fail++;
      } catch (_) { fail++; }
    }
    hideLoading?.();
    bn_render();
    if (fail) alert(`Guardados: ${ok} · Fallidos: ${fail}`);
  });
}

/** Abre el popup Clasificar en modo masivo (sin Resumen ni Validar/Revisar). */
function bn_bulkClasificar() {
  if (!BN_SEL.size) { alert('No hay registros seleccionados'); return; }
  const firstRec = bn_selectedRecs()[0];
  if (!firstRec) return;

  // Usar idx virtual 'bulk' que no choca con índices reales
  const ci = 'bnbulk';
  const deptOptions = Array.from({length:14},(_,j)=>`<option>${j+1}</option>`).join('');

  const classifyHtml = buildClassifyPanel(
    ci, '', deptOptions,
    `💾 Aplicar a ${BN_SEL.size} reg.`,
    `bn_bulkApplyClasificar()`,
    `bn_bulkClearClasificarPanel()`,
    'Fecha del registro'
  ).replace('class="classify-panel hidden"', 'class="classify-panel"');

  document.getElementById('bn-classify-modal-resumen').innerHTML =
    `<div style="padding:14px;background:#f1f5f9;border:1px solid #cbd5e1;border-radius:10px;margin-bottom:8px">
       <div style="font-weight:700;color:#334155;font-size:14px;margin-bottom:4px">🏷️ Clasificación masiva</div>
       <div style="font-size:12px;color:#475569">Se aplicará la clasificación que selecciones a <b>${BN_SEL.size}</b> registros marcados. La tabla de Resumen individual se omite por ser múltiples.</div>
     </div>`;
  document.getElementById('bn-classify-modal-body').innerHTML = classifyHtml;
  document.getElementById('bn-classify-overlay').classList.remove('hidden');
  document.body.style.overflow = 'hidden';

  // Renderizar grid de Cuentas como en bn_toggleBnClassify
  bn_activateCatalog();
  const cuentaGrid = document.getElementById(`cuenta-grid-${ci}`);
  if (cuentaGrid) {
    const CUENTA_ICONS = { Egresos:'💸', Ingresos:'💰', Pasivos:'📋', Activos:'📈', Capital:'💼' };
    const CUENTA_SUBS  = { Egresos:'Gastos y pagos', Ingresos:'Cobros y entradas',
                           Pasivos:'Obligaciones', Activos:'Inversión / CAPEX', Capital:'Utilidad / Familiar' };
    const cuentas = Object.keys(BN_CATALOG);
    cuentaGrid.innerHTML = [
      `<div class="cuenta-card" data-value="" onclick="selectCuenta(this,'${ci}')">` +
        `<div class="cuenta-icon">🏠</div><div class="cuenta-label">Sin cuenta</div>` +
        `<div class="cuenta-sub">General</div></div>`,
      ...cuentas.map(c =>
        `<div class="cuenta-card" data-value="${esc(c)}" onclick="selectCuenta(this,'${ci}')">` +
        `<div class="cuenta-icon">${CUENTA_ICONS[c] || '📁'}</div>` +
        `<div class="cuenta-label">${esc(c)}</div>` +
        (CUENTA_SUBS[c] ? `<div class="cuenta-sub">${CUENTA_SUBS[c]}</div>` : '') +
        `</div>`)
    ].join('');
  }
}

function bn_bulkClearClasificarPanel() {
  // Limpia las selecciones del panel virtual 'bulk' (botón Limpiar)
  const ci = 'bnbulk';
  const grid = document.getElementById(`cuenta-grid-${ci}`);
  grid?.querySelectorAll('.cuenta-card').forEach(c => c.classList.remove('active'));
  const sinCta = grid?.querySelector('.cuenta-card[data-value=""]');
  sinCta?.classList.add('active');
  ['subcuenta','categoria','concepto'].forEach(k => {
    const g = document.getElementById(`${k}-grid-${ci}`);
    if (g) g.innerHTML = '';
  });
}

async function bn_bulkApplyClasificar() {
  const ci = 'bnbulk';
  const c  = getClassify(ci);
  if (!c.cuenta) { alert('Selecciona al menos una Cuenta'); return; }
  await bn_bulkSaveAll(rec => {
    rec._cuenta          = c.cuenta;
    rec._subcuenta       = c.subcuenta;
    rec._categoria_gasto = c.categoria;
    rec._concepto        = c.concepto;
    rec.CUENTA = c.cuenta; rec.SUBCUENTA = c.subcuenta;
    rec.CATEGORIA = c.categoria; rec.CONCEPTO = c.concepto;
    const monto = Number(rec.Monto) || 0;
    const _raw = rec._cuenta || rec.TIPO || (monto < 0 ? 'Egresos' : monto > 0 ? 'Ingresos' : '');
    const _c   = bn_canon(_raw);
    rec._tipo = _c.includes('egr') ? 'Egresos' : _c.includes('ing') ? 'Ingresos' :
                _c.includes('activ') ? 'Activos' : _c.includes('pasiv') ? 'Pasivos' :
                _c.includes('capital') ? 'Capital' : _raw;
  });
  bn_closeClassifyModal();
}

/** Toggle handler para la vista cronológica. */
function bn_toggleTimeline(checked) {
  BN_TIMELINE = !!checked;
  BN_CARD_PAGE = 1;
  bn_renderCards();
}

/** Devuelve la fecha ISO YYYY-MM-DD de un registro o '' si no es válida. */
function bn_recDateISO(r) {
  return bn_formatDiaISO(r.Día || r.Dia || '');
}

/** Devuelve etiqueta humana para una fecha ISO YYYY-MM-DD ("Hoy", "Ayer", "5 de enero, 2025"). */
function bn_dateHeaderLabel(iso) {
  if (!iso) return 'Sin fecha';
  const NOMBRES = ['enero','febrero','marzo','abril','mayo','junio',
                   'julio','agosto','septiembre','octubre','noviembre','diciembre'];
  const [y, m, d] = iso.split('-').map(Number);
  const today = new Date(); today.setHours(0,0,0,0);
  const recDate = new Date(y, m-1, d);
  const diffDays = Math.round((today - recDate) / 86400000);
  if (diffDays === 0)  return 'Hoy';
  if (diffDays === 1)  return 'Ayer';
  return `${d} de ${NOMBRES[m-1]}, ${y}`;
}

/** Renderiza los registros como una tabla.
 *  - Si BN_SEL_MODE: agrega checkbox al inicio y la fila toggle selecciona.
 *  - Si NO: click en fila abre el modal Clasificar.
 *  - Columna 'Buscar clasificación' permite autocompletar Cuenta/Sub/Cat/Concepto. */
function bn_renderRecordsTable(recs, startIdx) {
  if (!recs.length) return '';
  // (Ya viene ordenado globalmente desde bn_render; no re-ordenar la página.)
  const CUENTA_EMOJI = { Egresos:'💸', Ingresos:'💰', Activos:'📈', Pasivos:'📋', Capital:'💼' };
  const editBtnsDesc = BN_TBL_EDIT_MODE ? `
    <button onclick="event.stopPropagation();bn_tblToggleEditMode()"
            style="padding:4px 10px;border:none;background:#475569;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">💾 Guardar</button>
    <button onclick="event.stopPropagation();bn_tblExitEditMode()"
            style="padding:4px 10px;border:none;background:#dc2626;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">✕ Salir</button>` : `
    <button onclick="event.stopPropagation();bn_tblToggleEditMode()"
            style="padding:4px 10px;border:none;background:#475569;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">✏️ Editar</button>`;
  const editBtnsCom = BN_TBL_COMENT_EDIT_MODE ? `
    <button onclick="event.stopPropagation();bn_tblToggleComentEditMode()"
            style="padding:4px 10px;border:none;background:#475569;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">💾 Guardar</button>
    <button onclick="event.stopPropagation();bn_tblExitComentEditMode()"
            style="padding:4px 10px;border:none;background:#dc2626;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">✕ Salir</button>` : `
    <button onclick="event.stopPropagation();bn_tblToggleComentEditMode()"
            style="padding:4px 10px;border:none;background:#475569;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">✏️ Editar</button>`;
  const editBtnsDudaNota = BN_TBL_DUDANT_EDIT_MODE ? `
    <button onclick="event.stopPropagation();bn_tblToggleDudaNotaEditMode()"
            style="padding:4px 10px;border:none;background:#475569;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">💾 Guardar</button>
    <button onclick="event.stopPropagation();bn_tblExitDudaNotaEditMode()"
            style="padding:4px 10px;border:none;background:#dc2626;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">✕ Salir</button>` : `
    <button onclick="event.stopPropagation();bn_tblToggleDudaNotaEditMode()"
            style="padding:4px 10px;border:none;background:#475569;color:#fff;border-radius:6px;font-size:10px;font-weight:800;cursor:pointer;text-transform:none;letter-spacing:0;box-shadow:0 1px 3px rgba(0,0,0,.15)">✏️ Editar</button>`;
  // Helper: th draggable, ordenable, con etiqueta
  const dragAttrs = (key) => `draggable="true" data-col-key="${key}" ondragstart="bn_tblColDragStart(event,'${key}')" ondragover="bn_tblColDragOver(event)" ondrop="bn_tblColDrop(event,'${key}')"`;
  const mkTh = (key, label, align='left', extra='') =>
    `<th ${dragAttrs(key)} onclick="bn_tblSort('${key}')" style="padding:9px 10px;text-align:${align};font-size:11px;text-transform:uppercase;cursor:grab;user-select:none;${extra}" title="Ordenar por ${label} · arrastra para reordenar">${label}${bn_tblSortIcon(key)}</th>`;
  // Definiciones por clave: header HTML y constructor del td
  const COLS = {
    validado: { th: mkTh('validado','Val.','center'),
                td: (rec, idx, ctx) => `<td onclick="event.stopPropagation();bn_syncValidado(${idx}, ${!ctx.isVal});bn_renderCards()" title="${ctx.isVal?'Validado — clic para quitar':'Marcar como Validado'}" style="padding:6px;text-align:center;cursor:pointer"><span style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:6px;border:1.5px solid ${ctx.isVal?'#16a34a':'#e2e8f0'};background:${ctx.isVal?'#dcfce7':'#fff'};color:${ctx.isVal?'#16a34a':'#cbd5e1'};font-weight:900;font-size:13px">✓</span></td>` },
    duda: { th: mkTh('duda','Duda','center'),
            td: (rec, idx, ctx) => `<td onclick="event.stopPropagation();bn_syncDuda(${idx}, ${!ctx.isDuda});bn_renderCards()" title="${ctx.isDuda?(rec._duda_nota||'Marcado: Duda — clic para quitar'):'Marcar como Duda'}" style="padding:6px;text-align:center;cursor:pointer"><span style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:6px;border:1.5px solid ${ctx.isDuda?'#f59e0b':'#e2e8f0'};background:${ctx.isDuda?'#fef3c7':'#fff'};color:${ctx.isDuda?'#b45309':'#cbd5e1'};font-weight:900;font-size:13px">?</span></td>` },
    dudaNota: { th: `<th ${dragAttrs('dudaNota')} onclick="bn_tblSort('dudaNota')" style="padding:9px 10px;text-align:left;font-size:11px;text-transform:uppercase;cursor:grab;user-select:none" title="Dudas texto · arrastra para reordenar"><div style="display:inline-flex;align-items:center;gap:6px"><span>Dudas texto${bn_tblSortIcon('dudaNota')}</span>${editBtnsDudaNota}</div></th>`,
                td: (rec, idx, ctx) => `<td ${BN_TBL_DUDANT_EDIT_MODE ? 'contenteditable="true"' : ''} spellcheck="false" data-row-idx="${idx}" data-orig-dudant="${esc(ctx.dudaNt)}" onclick="event.stopPropagation()" onfocus="this.style.overflow='auto';this.style.textOverflow='clip';this.style.background='#fff'" onblur="this.style.overflow='hidden';this.style.textOverflow='ellipsis';this.style.background='${ctx.dudaNtBg}';bn_tblTrackDudaNotaChange(${idx}, this)" onkeydown="if(event.key==='Enter'){event.preventDefault();this.blur()}" style="padding:8px 10px;font-size:11px;color:#92400e;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;outline:none;cursor:${BN_TBL_DUDANT_EDIT_MODE?'text':'default'};${ctx.dudaNtBg?'background:'+ctx.dudaNtBg+';':''}${BN_TBL_DUDANT_EDIT_MODE?'border-left:2px solid #f59e0b':''}" title="${esc(ctx.dudaNt)}">${esc(ctx.dudaNt)}</td>` },
    deducible: { th: mkTh('deducible','Ded.','center'),
                 td: (rec, idx, ctx) => `<td onclick="event.stopPropagation();bn_syncDeducible(${idx}, ${!ctx.dedOn});bn_renderCards()" title="${ctx.dedOn?'Deducible':'No deducible'}" style="padding:6px;text-align:center;cursor:pointer"><span style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:6px;border:1.5px solid ${ctx.dedOn?'#0e7490':'#e2e8f0'};background:${ctx.dedOn?'#cffafe':'#fff'};font-size:13px;opacity:${ctx.dedOn?'1':'.5'}">💰</span></td>` },
    dia: { th: mkTh('dia','Día'),
           td: (rec, idx, ctx) => `<td style="padding:8px 10px;font-size:11px;color:#2563eb;font-weight:700;white-space:nowrap">● ${esc(ctx.dia)}</td>` },
    buscarClasif: { th: `<th ${dragAttrs('buscarClasif')} style="padding:9px 10px;text-align:left;font-size:11px;text-transform:uppercase;cursor:grab" title="Arrastra para reordenar">Buscar clasificación</th>`,
                    td: (rec, idx, ctx) => `<td style="padding:6px 8px;min-width:200px" onclick="event.stopPropagation()"><input type="text" id="bn-tbl-search-${idx}" placeholder="🔍 Buscar..." autocomplete="off" oninput="bn_tblSearchInput(${idx}, this)" onfocus="bn_tblSearchInput(${idx}, this)" onblur="setTimeout(()=>bn_tblSearchHide(),200)" style="width:100%;padding:6px 10px;border:1px solid #cbd5e1;border-radius:6px;font-size:12px;outline:none"></td>` },
    cuenta: { th: mkTh('cuenta','Cuenta','left','letter-spacing:.04em'),
              td: (rec, idx, ctx) => `<td style="padding:8px 10px">${ctx.chip}</td>` },
    subcuenta: { th: mkTh('subcuenta','Subcuenta'),
                 td: (rec, idx, ctx) => `<td style="padding:8px 10px;font-size:11px;color:#475569;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(rec._subcuenta||'')}">${esc(rec._subcuenta||'')}</td>` },
    categoria: { th: mkTh('categoria','Categoría'),
                 td: (rec, idx, ctx) => `<td style="padding:8px 10px;font-size:11px;color:#475569;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(rec._categoria_gasto||'')}">${esc(rec._categoria_gasto||'')}</td>` },
    concepto: { th: mkTh('concepto','Concepto'),
                td: (rec, idx, ctx) => `<td style="padding:8px 10px;font-size:11px;color:#475569;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(rec._concepto||'')}">${esc(rec._concepto||'')}</td>` },
    monto: { th: mkTh('monto','Monto','right'),
             td: (rec, idx, ctx) => `<td style="padding:8px 10px;font-size:12px;text-align:right;font-weight:700;color:${ctx.montoN<0?'#dc2626':'#16a34a'};white-space:nowrap">${bn_fmt$(ctx.montoN)}</td>` },
    descripcion: { th: `<th ${dragAttrs('descripcion')} data-bn-tbl-col="desc" onclick="bn_tblSort('descripcion')" style="padding:9px 10px;text-align:left;font-size:11px;text-transform:uppercase;position:relative;width:${BN_TBL_DESC_WIDTH}px;max-width:${BN_TBL_DESC_WIDTH}px;cursor:grab;user-select:none" title="Descripción · arrastra para reordenar"><div style="display:inline-flex;align-items:center;gap:6px"><span>Descripción${bn_tblSortIcon('descripcion')}</span>${editBtnsDesc}</div><span class="bn-tbl-desc-resizer" onclick="event.stopPropagation()" onmousedown="event.stopPropagation();bn_tblStartDescResize(event)" title="Arrastrar para redimensionar">⇿</span></th>`,
                   td: (rec, idx, ctx) => `<td data-bn-tbl-col="desc" ${BN_TBL_EDIT_MODE ? 'contenteditable="true"' : ''} spellcheck="false" data-row-idx="${idx}" data-orig-desc="${esc(ctx.desc)}" onclick="event.stopPropagation()" onfocus="this.style.overflow='auto';this.style.textOverflow='clip';this.style.background='#fff'" onblur="this.style.overflow='hidden';this.style.textOverflow='ellipsis';this.style.background='${ctx.descBg}';bn_tblTrackDescChange(${idx}, this)" onkeydown="if(event.key==='Enter'){event.preventDefault();this.blur()}" style="padding:8px 10px;font-size:12px;width:${BN_TBL_DESC_WIDTH}px;max-width:${BN_TBL_DESC_WIDTH}px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;outline:none;cursor:${BN_TBL_EDIT_MODE?'text':'default'};${ctx.descBg?'background:'+ctx.descBg+';':''}${BN_TBL_EDIT_MODE?'border-left:2px solid #2563eb':''}" title="${esc(ctx.desc)}">${esc(ctx.desc)}</td>` },
    comentarios: { th: `<th ${dragAttrs('comentarios')} onclick="bn_tblSort('comentarios')" style="padding:9px 10px;text-align:left;font-size:11px;text-transform:uppercase;cursor:grab;user-select:none" title="Comentarios · arrastra para reordenar"><div style="display:inline-flex;align-items:center;gap:6px"><span>Comentarios${bn_tblSortIcon('comentarios')}</span>${editBtnsCom}</div></th>`,
                   td: (rec, idx, ctx) => `<td ${BN_TBL_COMENT_EDIT_MODE ? 'contenteditable="true"' : ''} spellcheck="false" data-row-idx="${idx}" data-orig-coment="${esc(ctx.com)}" onclick="event.stopPropagation()" onfocus="this.style.overflow='auto';this.style.textOverflow='clip';this.style.background='#fff'" onblur="this.style.overflow='hidden';this.style.textOverflow='ellipsis';this.style.background='${ctx.comBg}';bn_tblTrackComentChange(${idx}, this)" onkeydown="if(event.key==='Enter'){event.preventDefault();this.blur()}" style="padding:8px 10px;font-size:11px;color:#475569;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;outline:none;cursor:${BN_TBL_COMENT_EDIT_MODE?'text':'default'};${ctx.comBg?'background:'+ctx.comBg+';':''}${BN_TBL_COMENT_EDIT_MODE?'border-left:2px solid #2563eb':''}" title="${esc(ctx.com)}">${esc(ctx.com)}</td>` },
    cuentaBanc: { th: mkTh('cuentaBanc','Cuenta banc.'),
                  td: (rec, idx, ctx) => `<td style="padding:8px 10px;font-size:11px;color:#64748b;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(ctx.cb)}">${esc(ctx.cb)}</td>` },
    factura: { th: mkTh('factura','Fact.','center'),
               td: (rec, idx, ctx) => `<td style="padding:8px 10px;text-align:center;font-size:14px">${ctx.fac ? '🧾' : ''}</td>` },
  };
  const headerCells = BN_TBL_COL_ORDER.map(k => COLS[k]?.th || '').join('');
  const head = `
    <thead>
      <tr style="background:#475569;color:#fff">
        ${BN_SEL_MODE ? '<th style="padding:9px 10px;width:36px"></th>' : ''}
        ${headerCells}
      </tr>
    </thead>`;

  const body = recs.map((rec, j) => {
    const idx = startIdx + j;
    const montoN = Number(rec.Monto || 0);
    const cuentaClasif = rec._cuenta || '';
    const tipoEfectivo = rec._tipo || (montoN < 0 ? 'Egresos' : montoN > 0 ? 'Ingresos' : '');
    const colorCls = CUENTA_COLOR_CLASS[tipoEfectivo] || '';
    const tipoLbl  = cuentaClasif || (montoN < 0 ? 'Egreso' : montoN > 0 ? 'Ingreso' : '—');
    const tipoEmoji = CUENTA_EMOJI[cuentaClasif] || '🏦';
    const chip = `<span class="info-chip ${colorCls}" style="font-size:11px;padding:3px 8px">${tipoEmoji} ${esc(tipoLbl)}</span>`;
    const dia  = bn_formatDia(rec.Día || rec.Dia || '') || '';
    const desc = (rec.DESCRIPCION || '').slice(0, 60);
    const cb   = rec['Cuenta bancaria'] || '';
    const isDuda = rec._duda     === 'Sí';
    const isVal  = rec._validado === 'Sí';
    const dedOn  = rec._deducible === 'Sí';
    const fac    = rec.FacturaFlag === 'Con factura' || (rec.Factura && String(rec.Factura).trim());
    const isSel  = !!(rec.rowNum && BN_SEL.has(rec.rowNum));
    const rowClick = BN_SEL_MODE
      ? `bn_selToggle(${rec.rowNum||0}, ${!isSel}); bn_renderCards()`
      : (BN_TBL_EDIT_MODE ? '' : `bn_toggleBnClassify(${idx})`);
    const rowBg = isSel ? 'background:#fff7ed' : '';
    const selCell = BN_SEL_MODE
      ? `<td style="padding:6px 10px;text-align:center" onclick="event.stopPropagation();bn_selToggle(${rec.rowNum||0}, ${!isSel}); bn_renderCards()">
           <span style="display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;border-radius:5px;border:1.5px solid ${isSel?'#ea580c':'#cbd5e1'};background:${isSel?'#ea580c':'#fff'};color:#fff;font-weight:800;font-size:13px;cursor:pointer">${isSel?'✓':''}</span>
         </td>`
      : '';
    const com  = (rec._comentarios || '').slice(0, 200);
    const dudaNt = (rec._duda_nota || '').slice(0, 200);
    // Cuando NO se edita la columna, no fijar background inline para permitir
    // que el striping zebra (tr:nth-child(even)) se aplique como en el resto.
    const descBg    = BN_TBL_EDIT_MODE        ? '#fff' : '';
    const comBg     = BN_TBL_COMENT_EDIT_MODE ? '#fff' : '';
    const dudaNtBg  = BN_TBL_DUDANT_EDIT_MODE ? '#fff' : '';
    const ctx = { isVal, isDuda, dedOn, dudaNt, dudaNtBg, dia, chip, montoN, desc, descBg, com, comBg, cb, fac };
    const bodyCells = BN_TBL_COL_ORDER.map(k => COLS[k]?.td(rec, idx, ctx) || '').join('');
    return `
      <tr ${rowClick ? `onclick="${rowClick}"` : ''}
          style="cursor:${rowClick?'pointer':'default'};border-bottom:1px solid #e2e8f0;${rowBg}"
          onmouseover="this.style.filter='brightness(.97)'"
          onmouseout="this.style.filter=''">
        ${selCell}
        ${bodyCells}
      </tr>`;
  }).join('');

  return `
    <div style="overflow-x:auto;border:1px solid #e2e8f0;border-radius:10px;background:#fff">
      <table style="width:100%;border-collapse:collapse;font-size:12px;min-width:1200px">
        ${head}
        <tbody>${body}</tbody>
      </table>
    </div>
    <div id="bn-tbl-search-dropdown" class="hidden"
         style="position:fixed;background:#fff;border:1px solid #cbd5e1;border-radius:8px;box-shadow:0 8px 22px rgba(15,23,42,.15);max-height:260px;overflow-y:auto;z-index:8500;min-width:280px"></div>`;
}

// (Antiguo bn_tblEditDescripcion removido; ahora la edición se hace en lote
//  vía el botón Editar/Guardar de la cabecera. Ver bn_tblTrackDescChange y
//  bn_tblSaveAllDescChanges arriba.)

/** Filtra SEARCH_INDEX y muestra dropdown. */
function bn_tblSearchInput(idx, input) {
  const q = (input.value || '').trim().toLowerCase();
  const dd = document.getElementById('bn-tbl-search-dropdown');
  if (!dd) return;
  if (!q || q.length < 2) {
    dd.classList.add('hidden');
    return;
  }
  // Filtra SEARCH_INDEX (catálogo de combinaciones cuenta/sub/cat/concepto)
  const idx_ = (typeof SEARCH_INDEX !== 'undefined' && SEARCH_INDEX) ? SEARCH_INDEX : [];
  const matches = idx_
    .filter(o => {
      const t = ((o.cuenta||'') + ' ' + (o.subcuenta||'') + ' ' + (o.categoria||'') + ' ' + (o.concepto||'')).toLowerCase();
      return q.split(/\s+/).every(w => t.includes(w));
    })
    .slice(0, 12);
  if (!matches.length) {
    dd.innerHTML = `<div style="padding:10px;font-size:12px;color:#9ca3af">Sin coincidencias</div>`;
    bn_tblSearchPositionAt(input);
    dd.classList.remove('hidden');
    return;
  }
  dd.innerHTML = matches.map(o => {
    const path = [o.cuenta, o.subcuenta, o.categoria, o.concepto].filter(Boolean).join(' › ');
    const enc = encodeURIComponent(JSON.stringify(o));
    return `<div onmousedown="event.preventDefault();bn_tblSearchPick(${idx}, '${enc}')"
                 onmouseover="this.style.background='#f1f5f9'"
                 onmouseout="this.style.background=''"
                 style="padding:8px 10px;border-bottom:1px solid #f1f5f9;font-size:12px;cursor:pointer;color:#1f2937">
              ${esc(path)}
            </div>`;
  }).join('');
  bn_tblSearchPositionAt(input);
  dd.classList.remove('hidden');
}

function bn_tblSearchPositionAt(input) {
  const dd = document.getElementById('bn-tbl-search-dropdown');
  if (!dd || !input) return;
  const r = input.getBoundingClientRect();
  dd.style.top   = (r.bottom + 4) + 'px';
  dd.style.left  = r.left + 'px';
  dd.style.width = Math.max(280, r.width) + 'px';
}

function bn_tblSearchHide() {
  document.getElementById('bn-tbl-search-dropdown')?.classList.add('hidden');
}

/** Selecciona una clasificación del catálogo y la persiste en el registro. */
async function bn_tblSearchPick(idx, enc) {
  let opt;
  try { opt = JSON.parse(decodeURIComponent(enc)); } catch(_) { return; }
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;

  rec._cuenta          = opt.cuenta    || '';
  rec._subcuenta       = opt.subcuenta || '';
  rec._categoria_gasto = opt.categoria || '';
  rec._concepto        = opt.concepto  || '';
  rec.CUENTA    = rec._cuenta;
  rec.SUBCUENTA = rec._subcuenta;
  rec.CATEGORIA = rec._categoria_gasto;
  rec.CONCEPTO  = rec._concepto;
  // Recalcular _tipo
  const monto = Number(rec.Monto) || 0;
  const _raw = rec._cuenta || rec.TIPO || (monto < 0 ? 'Egresos' : monto > 0 ? 'Ingresos' : '');
  const _c   = bn_canon(_raw);
  rec._tipo = _c.includes('egr') ? 'Egresos' : _c.includes('ing') ? 'Ingresos' :
              _c.includes('activ') ? 'Activos' : _c.includes('pasiv') ? 'Pasivos' :
              _c.includes('capital') ? 'Capital' : _raw;

  bn_tblSearchHide();
  bn_renderCards();

  if (!rec.rowNum) return;
  try {
    const payload = bn_buildSavePayload(rec);
    await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (e) { console.warn('Error guardando clasificación:', e.message); }
}

function bn_renderCards() {
  const container = document.getElementById('bn-cards-container');
  const pagEl     = document.getElementById('bn-card-pagination');
  if (!container) return;

  let recs  = BN_CUR_RECS;
  const total = recs.length;

  if (!total) {
    container.innerHTML = `<div class="empty-state" style="padding:50px 20px">
      <div class="empty-icon">📭</div>
      <p style="color:var(--text-soft);margin:0">Sin movimientos para los filtros seleccionados</p>
    </div>`;
    if (pagEl) pagEl.innerHTML = '';
    return;
  }

  // Vista cronológica: ordena por fecha desc y agrupa por día con encabezados
  if (BN_TIMELINE) {
    recs = recs.slice().sort((a, b) => {
      const da = bn_recDateISO(a), db = bn_recDateISO(b);
      if (!da && !db) return 0;
      if (!da) return 1;
      if (!db) return -1;
      return db.localeCompare(da); // descendente
    });
    BN_CUR_RECS = recs; // mantener para clics, paginación etc.
  }

  const totalPages = Math.max(1, Math.ceil(total / BN_CARD_SIZE));
  BN_CARD_PAGE     = Math.min(BN_CARD_PAGE, totalPages);
  const start      = (BN_CARD_PAGE - 1) * BN_CARD_SIZE;
  const pageRecs   = recs.slice(start, start + BN_CARD_SIZE);

  // Modo Tabla — cada registro en una sola fila
  if (BN_VIEW_MODE === 'table') {
    container.style.position = '';
    container.style.paddingLeft = '';
    container.innerHTML = bn_renderRecordsTable(pageRecs, start);
  } else if (BN_TIMELINE) {
    // Cards con encabezados de fecha
    container.style.position = '';
    container.style.paddingLeft = '';
    let lastKey = null;
    const parts = [];
    pageRecs.forEach((rec, j) => {
      const iso = bn_recDateISO(rec);
      const key = iso || '__sin_fecha__';
      if (key !== lastKey) {
        const label = iso ? bn_dateHeaderLabel(iso) : 'Sin fecha';
        parts.push(
          `<div style="margin:14px 0 8px;display:flex;align-items:center;gap:10px">
             <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#64748b;flex-shrink:0"></span>
             <span style="font-weight:700;font-size:13px;color:#64748b;text-transform:uppercase;letter-spacing:.04em">${esc(label)}</span>
             <span style="flex:1;height:1px;background:linear-gradient(90deg,#cbd5e1,transparent)"></span>
           </div>`
        );
        lastKey = key;
      }
      parts.push(bn_createCard(rec, start + j));
    });
    container.innerHTML = parts.join('');
  } else {
    container.style.position = '';
    container.style.paddingLeft = '';
    container.innerHTML = pageRecs.map((rec, j) => bn_createCard(rec, start + j)).join('');
  }

  // Paginación
  if (!pagEl) return;
  if (totalPages <= 1) { pagEl.innerHTML = ''; return; }
  let btns = '';
  for (let p = Math.max(1, BN_CARD_PAGE - 2); p <= Math.min(totalPages, BN_CARD_PAGE + 2); p++) {
    btns += `<button class="pag-btn${p === BN_CARD_PAGE ? ' active' : ''}" onclick="bn_goToCardPage(${p})">${p}</button>`;
  }
  pagEl.innerHTML = `
    <div class="pagination-row" style="margin:12px 0 24px">
      <button class="pag-btn" onclick="bn_goToCardPage(${BN_CARD_PAGE - 1})" ${BN_CARD_PAGE <= 1 ? 'disabled' : ''}>‹</button>
      ${btns}
      <button class="pag-btn" onclick="bn_goToCardPage(${BN_CARD_PAGE + 1})" ${BN_CARD_PAGE >= totalPages ? 'disabled' : ''}>›</button>
      <span style="color:var(--text-soft);font-size:12px;margin-left:10px">
        ${total.toLocaleString('es-MX')} movimientos · Pág. ${BN_CARD_PAGE}/${totalPages}
      </span>
    </div>`;
}

function bn_goToCardPage(p) {
  const totalPages = Math.max(1, Math.ceil(BN_CUR_RECS.length / BN_CARD_SIZE));
  BN_CARD_PAGE = Math.max(1, Math.min(p, totalPages));
  bn_renderCards();
  document.getElementById('bn-cards-container')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ─── Auto-load when switching to Registros contables ─────────────────────────
(function(){
  const _orig=window.switchModule;
  window.switchModule=function(mod){
    if (mod !== 'registros') bn_deactivateCatalog(); // restaurar catálogo de tickets
    _orig(mod);
    if (mod === 'registros') {
      if (!BN_LOADED) bn_loadData();
      else bn_activateCatalog(); // ya cargado: reactivar catálogo Bancos
    }
  };
})();

// ─── Emojis por categoría ──────────────────────────────────────────────────

const CATEGORIA_EMOJIS = {
  "Limpieza":        "🧹",
  "Mantenimiento":   "🔧",
  "Administración":  "📝",
  "Oficina":         "🖊️",
  "Otros gastos":    "📦",
  "Otros Gastos":    "📦",
  "Servicios":       "⚡",
  "Insumos":         "🛍️",
  "Autos":           "🚗",
  "Plataformas":     "💻",
  "SAT":             "🏛️",
  "IMSS":            "🏥",
  "Tenencia":        "📄",
  "Predial":         "🏘️",
  "Equipamiento":    "🛋️",
  "Herramienta":     "🔨",
  "Construcción":    "🏗️",
};

// ─── Card HTML helper ──────────────────────────────────────────────────────

function makeCard(name, emoji, onclick) {
  return `<div class="cuenta-card" data-value="${esc(name)}" onclick="${onclick}">` +
    `<div class="cuenta-icon">${emoji}</div>` +
    `<div class="cuenta-label">${esc(name)}</div>` +
    `</div>`;
}

// Desplaza el card seleccionado al borde izquierdo del carrusel
function scrollCardIntoView(el) {
  if (!el) return;
  el.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "start" });
}

// ─── Cuenta / Subcuenta / Categoría / Concepto (cascading cards) ───────────

const FACTURABLE_CUENTAS = new Set(["", "Egresos", "Activos", "Pasivos"]);

function selectCuenta(el, i) {
  // Click sobre la card ya activa → deseleccionar (vuelve a 'Sin cuenta')
  if (el.classList.contains('active') && el.dataset.value !== '') {
    el.classList.remove('active');
    const sinCuenta = el.closest('.cuenta-grid')?.querySelector('.cuenta-card[data-value=""]');
    if (sinCuenta) sinCuenta.classList.add('active');
    document.getElementById(`cuenta-${i}`).value = '';
    resetSubcuenta(i);
    try { markClassifyDirty(i); } catch(_) {}
    return;
  }
  el.closest(".cuenta-grid").querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  el.classList.add("active");
  scrollCardIntoView(el);
  const cuenta = el.dataset.value;
  document.getElementById(`cuenta-${i}`).value = cuenta;
  resetSubcuenta(i);

  const deducibleField = document.getElementById(`deducible-field-${i}`);
  if (deducibleField) deducibleField.classList.toggle("hidden", !FACTURABLE_CUENTAS.has(cuenta));

  const subs = cuenta && CATALOG[cuenta] ? Object.keys(CATALOG[cuenta]) : [];
  if (!subs.length) return;

  document.getElementById(`subcuenta-grid-${i}`).innerHTML =
    subs.map(n => makeCard(n, SUBCUENTA_EMOJIS[n] || "📌", `selectSubcuenta(this,'${i}')`)).join("");
  document.getElementById(`subcuenta-field-${i}`).classList.remove("hidden");
  updateClasiPath(i);
  markClassifyDirty(i);
}

function resetSubcuenta(i) {
  document.getElementById(`subcuenta-${i}`).value = "";
  document.getElementById(`subcuenta-field-${i}`).classList.add("hidden");
  document.getElementById(`subcuenta-grid-${i}`).innerHTML = "";
  resetCategoria(i);
}

function selectSubcuenta(el, i) {
  if (el.classList.contains('active')) {
    el.classList.remove('active');
    document.getElementById(`subcuenta-${i}`).value = '';
    resetCategoria(i);
    try { markClassifyDirty(i); } catch(_) {}
    return;
  }
  el.closest(".cuenta-grid").querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  el.classList.add("active");
  scrollCardIntoView(el);
  const subcuenta = el.dataset.value;
  document.getElementById(`subcuenta-${i}`).value = subcuenta;
  resetCategoria(i);

  const cuenta = document.getElementById(`cuenta-${i}`).value;
  const sub    = CATALOG[cuenta]?.[subcuenta];

  if (Array.isArray(sub)) {
    // Ingresos / Capital: sin nivel Categoría, concepto directo
    if (sub.length) renderConceptos(sub, i);
  } else if (sub) {
    const cats = Object.keys(sub);
    if (!cats.length) return;
    document.getElementById(`categoria-grid-${i}`).innerHTML =
      cats.map(n => makeCard(n, CATEGORIA_EMOJIS[n] || "📂", `selectCategoria(this,'${i}')`)).join("");
    document.getElementById(`categoria-field-${i}`).classList.remove("hidden");
  }
  updateClasiPath(i);
}

function resetCategoria(i) {
  document.getElementById(`categoria-${i}`).value = "";
  document.getElementById(`categoria-field-${i}`).classList.add("hidden");
  document.getElementById(`categoria-grid-${i}`).innerHTML = "";
  resetConcepto(i);
}

function resetConcepto(i) {
  document.getElementById(`concepto-${i}`).value = "";
  document.getElementById(`concepto-field-${i}`).classList.add("hidden");
  document.getElementById(`concepto-grid-${i}`).innerHTML = "";
}

function renderConceptos(conceptos, i) {
  document.getElementById(`concepto-grid-${i}`).innerHTML =
    conceptos.map(n => makeCard(n, CONCEPTO_EMOJIS[n] || "🔹", `selectConcepto(this,'${i}')`)).join("");
  document.getElementById(`concepto-field-${i}`).classList.remove("hidden");
}

function selectConcepto(el, i) {
  if (el.classList.contains('active')) {
    el.classList.remove('active');
    document.getElementById(`concepto-${i}`).value = '';
    updateClasiPath(i);
    markClassifyDirty(i);
    return;
  }
  el.closest(".cuenta-grid").querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  el.classList.add("active");
  scrollCardIntoView(el);
  document.getElementById(`concepto-${i}`).value = el.dataset.value;
  updateClasiPath(i);
  markClassifyDirty(i);
}

function selectCategoria(el, i) {
  if (el.classList.contains('active')) {
    el.classList.remove('active');
    document.getElementById(`categoria-${i}`).value = '';
    resetConcepto(i);
    updateClasiPath(i);
    markClassifyDirty(i);
    return;
  }
  el.closest(".cuenta-grid").querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  el.classList.add("active");
  scrollCardIntoView(el);
  const categoria = el.dataset.value;
  document.getElementById(`categoria-${i}`).value = categoria;
  resetConcepto(i);

  const cuenta    = document.getElementById(`cuenta-${i}`).value;
  const subcuenta = document.getElementById(`subcuenta-${i}`).value;
  const conceptos = CATALOG[cuenta]?.[subcuenta]?.[categoria] || [];
  if (conceptos.length) renderConceptos(conceptos, i);
  updateClasiPath(i);
  markClassifyDirty(i);
}

function selectMetodoPago(el, i) {
  const otroWrap = document.getElementById(`metodo-otro-wrap-${i}`);
  if (el.classList.contains('active')) {
    el.classList.remove('active');
    document.getElementById(`metodo-clasif-${i}`).value = '';
    if (otroWrap) otroWrap.classList.add('hidden');
    markClassifyDirty(i);
    return;
  }
  el.closest(".cuenta-grid").querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  el.classList.add("active");
  scrollCardIntoView(el);
  const val = el.dataset.value;
  document.getElementById(`metodo-clasif-${i}`).value = val;
  if (otroWrap) {
    if (val === 'Otro') {
      otroWrap.classList.remove('hidden');
      setTimeout(() => document.getElementById(`metodo-otro-${i}`)?.focus(), 30);
    } else {
      otroWrap.classList.add('hidden');
      const inp = document.getElementById(`metodo-otro-${i}`);
      if (inp) inp.value = '';
    }
  }
  markClassifyDirty(i);
}

function bn_metodoOtroInput(i, inputEl) {
  const v = (inputEl.value || '').trim();
  document.getElementById(`metodo-clasif-${i}`).value = v ? v : 'Otro';
}

function toggleReembolso(i, checked) {
  const field = document.getElementById(`reembolso-a-field-${i}`);
  const label = document.getElementById(`reembolso-label-${i}`);
  field.classList.toggle("hidden", !checked);
  label.textContent = checked ? "Sí" : "No";
  label.classList.toggle("on", checked);
  if (checked) {
    const encargado = document.getElementById(`comprador-${i}`)?.value || "";
    const sel = document.getElementById(`reembolso-a-${i}`);
    if (sel && encargado) sel.value = encargado;
  }
}

function togglePropiedadOtro(i, value) {
  const wrap = document.getElementById(`propiedad-otro-wrap-${i}`);
  if (!wrap) return;
  wrap.classList.toggle("hidden", value !== "Otro");
  if (value !== "Otro") document.getElementById(`propiedad-otro-${i}`).value = "";
}

function toggleReembolsoOtro(i, value) {
  const wrap = document.getElementById(`reembolso-otro-wrap-${i}`);
  if (!wrap) return;
  wrap.classList.toggle("hidden", value !== "Otro");
  if (value !== "Otro") document.getElementById(`reembolso-otro-${i}`).value = "";
}

function selectComprador(el, i) {
  if (el.classList.contains('active')) {
    el.classList.remove('active');
    document.getElementById(`comprador-${i}`).value = '';
    markClassifyDirty(i);
    return;
  }
  el.closest(".cuenta-grid").querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  el.classList.add("active");
  scrollCardIntoView(el);
  document.getElementById(`comprador-${i}`).value = el.dataset.value;
  markClassifyDirty(i);
}

// Llamado desde el toggle dentro del panel "Clasificar"
function updateDeducibleLabel(i, checked) {
  const lbl = document.getElementById(`deducible-label-${i}`);
  if (lbl) { lbl.textContent = checked ? "Sí" : "No"; lbl.classList.toggle("on", checked); }
  // Sincronizar el toggle del encabezado (mantiene texto "Deducible"/"No deducible")
  const hCheck = document.getElementById(`deducible-header-${i}`);
  if (hCheck) hCheck.checked = checked;
  const hLbl = document.getElementById(`fhl-${i}`);
  if (hLbl) { hLbl.textContent = checked ? "Deducible" : "No deducible"; hLbl.classList.toggle("on", checked); }
}

// Llamado desde el toggle del encabezado (módulo tickets)
function syncDeducible(i, checked) {
  const inner = document.getElementById(`deducible-${i}`);
  if (inner) inner.checked = checked;
  updateDeducibleLabel(i, checked);
}

// Llamado desde el toggle Validado del encabezado bancario — guarda
// en memoria, actualiza chip de estado y persiste a Sheets en tiempo real.
async function bn_syncDuda(idx, checked) {
  const ci  = 'bn' + idx;
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  rec._duda = checked ? 'Sí' : 'No';
  // Al ACTIVAR: la nota se edita inline (no prompt nativo). Al desactivar: limpiar.
  if (!checked) rec._duda_nota = '';
  // Re-render la card para que aparezca/desaparezca el badge de nota inmediatamente
  const card = document.getElementById(`bn-card-${idx}`);
  if (card) card.outerHTML = bn_createCard(rec, idx);
  // Si acabamos de activar, abre el editor inline al instante (sin nota previa o con la previa)
  if (checked) setTimeout(() => bn_openDudaNotaEditor(idx), 30);

  // Botón Duda (?) en esquina superior derecha
  const btn = document.getElementById(`bn-check-${ci}`);
  if (btn) {
    btn.dataset.checked   = checked ? 'true' : 'false';
    btn.style.borderColor = checked ? '#f59e0b' : '#e5e7eb';
    btn.style.background  = checked ? '#e2e8f0' : '#f9fafb';
    btn.style.color       = checked ? '#b45309' : '#d1d5db';
    btn.title             = checked ? 'Marcado: Duda' : 'Duda';
    btn.textContent       = '?';
  }
  // Sincronizar el botón ? dentro del panel Clasificar
  const panelBtn = document.getElementById(`validado-panel-${ci}`);
  if (panelBtn) {
    panelBtn.dataset.checked   = checked ? 'true' : 'false';
    panelBtn.style.borderColor = checked ? '#f59e0b' : '#e5e7eb';
    panelBtn.style.background  = checked ? '#e2e8f0' : '#f9fafb';
    panelBtn.style.color       = checked ? '#b45309' : '#d1d5db';
    panelBtn.title             = checked ? 'Marcado: Duda' : 'Duda';
    panelBtn.textContent       = '?';
  }
  // Sync de la tabla Resumen del modal si está abierto
  bn_modalLiveSync(idx);

  if (!rec.rowNum) return;

  try {
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rowNum:    rec.rowNum,
        duda_edit: true,
        duda:      rec._duda,
        duda_nota: rec._duda_nota || '',
        clasificacion: {
          cuenta:          rec._cuenta          || '',
          subcuenta:       rec._subcuenta       || '',
          categoria_gasto: rec._categoria_gasto || '',
          concepto:        rec._concepto        || '',
          propiedad:       rec._propiedad       || '',
          departamento:    rec._departamento    || '',
          encargado:       rec._encargado       || '',
          deducible:       rec._deducible       || 'No',
          reembolso:       rec._reembolso       || 'No',
          reembolso_a:     rec._reembolso_a     || '',
          metodo_pago:     rec._metodo_pago     || '',
          clasificado_por: currentUser || '',
          duda:            rec._duda,
          duda_nota:       rec._duda_nota || '',
        }
      }),
    });
    const result = await resp.json();
    if (!result.ok) throw new Error(result.error || 'Error');
  } catch (e) {
    console.warn('Error guardando Duda:', e.message);
  }
}

/** Abre un editor inline para la nota de duda — pequeño recuadro anclado al
 *  badge 📝, con el mismo aspecto. Enter o blur guarda; Esc cancela. */
function bn_openDudaNotaEditor(idx) {
  const rec = BN_CUR_RECS[idx]; if (!rec) return;
  const card = document.getElementById(`bn-card-${idx}`);
  const anchor = document.getElementById(`bn-duda-note-${idx}`) ||
                 document.getElementById(`bn-check-bn${idx}`);
  if (!card || !anchor) return;
  // Evitar duplicar editor
  if (document.getElementById(`bn-duda-editor-${idx}`)) {
    document.getElementById(`bn-duda-editor-${idx}`).querySelector('input')?.focus();
    return;
  }
  const r = anchor.getBoundingClientRect();
  const wrap = document.createElement('div');
  wrap.id = `bn-duda-editor-${idx}`;
  wrap.style.cssText = `position:fixed;top:${r.bottom + 4}px;left:${Math.max(8, r.right - 260)}px;width:260px;z-index:9000;background:#fef3c7;border:1.5px solid #f59e0b;border-radius:8px;padding:6px;box-shadow:0 8px 22px rgba(180,83,9,.25);display:flex;gap:6px;align-items:center`;
  wrap.innerHTML = `
    <span style="font-size:13px">📝</span>
    <input type="text" placeholder="Describe la duda…" value="${esc(rec._duda_nota || '')}"
           style="flex:1;padding:5px 8px;border:1px solid #f59e0b;background:#fffbeb;color:#92400e;border-radius:6px;font-size:11px;font-weight:600;outline:none">
    <button type="button" title="Guardar"
            style="padding:4px 8px;border:none;background:#ea580c;color:#fff;border-radius:4px;font-size:11px;font-weight:700;cursor:pointer">✓</button>
    <button type="button" title="Cancelar"
            style="padding:4px 8px;border:none;background:#fde68a;color:#92400e;border-radius:4px;font-size:11px;font-weight:700;cursor:pointer">✕</button>`;
  document.body.appendChild(wrap);
  const input = wrap.querySelector('input');
  const [btnSave, btnCancel] = wrap.querySelectorAll('button');
  input.focus(); input.select();

  const save = () => {
    const newVal = (input.value || '').trim();
    rec._duda_nota = newVal;
    wrap.remove();
    // Re-render card para reflejar la nota en el badge
    const c2 = document.getElementById(`bn-card-${idx}`);
    if (c2) c2.outerHTML = bn_createCard(rec, idx);
    // Persistir en background
    if (rec.rowNum) {
      fetch(`${BACKEND}/save-banco-clasificacion`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rowNum: rec.rowNum, duda_nota_edit: true, duda_nota: newVal }),
      }).catch(e => console.warn('Error guardando nota:', e.message));
    }
  };
  const cancel = () => wrap.remove();

  btnSave.onclick = (ev) => { ev.stopPropagation(); save(); };
  btnCancel.onclick = (ev) => { ev.stopPropagation(); cancel(); };
  input.onkeydown = (ev) => {
    if (ev.key === 'Enter') { ev.preventDefault(); save(); }
    else if (ev.key === 'Escape') { ev.preventDefault(); cancel(); }
  };
  // Cerrar al hacer clic fuera
  const docHandler = (ev) => {
    if (!wrap.contains(ev.target)) { wrap.remove(); document.removeEventListener('mousedown', docHandler, true); }
  };
  setTimeout(() => document.addEventListener('mousedown', docHandler, true), 50);
}

// Snapshot inicial de la clasificación al abrir el modal Clasificar, usado
// para decidir si el botón 'Guardar cambios' debe aparecer.
let bn_classifyInitialSnapshot = '';

/** Devuelve un string canónico del estado actual de la clasificación. */
function bn_classifySnapshot(ci) {
  let c = {};
  try { c = getClassify(ci) || {}; } catch(_) {}
  const fecha = document.getElementById(`fecha-clasif-${ci}`)?.value || '';
  const dudaOn = document.getElementById(`validado-panel-${ci}`)?.dataset.checked || 'false';
  const valOn  = document.getElementById(`revisado-panel-${ci}`)?.dataset.checked || 'false';
  // Capturar el texto Descripción si el modal está abierto
  const descEl = document.querySelector('#bn-classify-modal-resumen [data-field="DESC"]');
  const desc   = descEl ? descEl.textContent.trim() : '';
  return JSON.stringify({
    cuenta: c.cuenta||'', sub: c.subcuenta||'', cat: c.categoria||'', con: c.concepto||'',
    prop: c.propiedad||'', dep: c.departamento||'', enc: c.comprador||'',
    ded: !!c.deducible, reem: !!c.reembolso, reema: c.reembolso_a||'',
    mp: c.metodo_pago_clasif||'',
    coment: c.comentarios||'', detalles: c.detalles||'',
    fecha, dudaOn, valOn, desc,
  });
}

/** Actualiza visibilidad del botón Guardar cambios según diferencia con snapshot. */
function bn_classifyRefreshActions(ci) {
  const actions = document.getElementById(`classify-actions-${ci}`);
  if (!actions) return;
  const now = bn_classifySnapshot(ci);
  if (now !== bn_classifyInitialSnapshot) {
    actions.classList.remove('hidden');
  } else {
    actions.classList.add('hidden');
  }
}

/** Sincroniza la tabla Resumen del modal con la selección actual del panel
 *  Clasificar (Cuenta, Subcuenta, Categoría, Concepto, Fecha, Duda, Validado),
 *  sin guardar todavía a Sheets — sólo refleja en vivo lo que el usuario ve. */
function bn_modalLiveSync(idx) {
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  const ci = 'bn' + idx;
  // CRÍTICO: si el modal Clasificar NO está abierto, NO sincronizar campos del panel.
  // getClassify() devolvería '' para todos los inputs inexistentes, borrando la
  // clasificación del registro.
  const ov = document.getElementById('bn-classify-overlay');
  if (!ov || ov.classList.contains('hidden')) {
    bn_classifyRefreshActions(ci);
    return;
  }
  const c = (function(){ try { return getClassify(ci); } catch(_) { return {}; } })();
  // Actualizar campos en memoria desde la selección del panel
  if (c.cuenta    !== undefined) rec._cuenta          = c.cuenta;
  if (c.subcuenta !== undefined) rec._subcuenta       = c.subcuenta;
  if (c.categoria !== undefined) rec._categoria_gasto = c.categoria;
  if (c.concepto  !== undefined) rec._concepto        = c.concepto;
  if (c.propiedad     !== undefined) rec._propiedad     = c.propiedad;
  if (c.departamento  !== undefined) rec._departamento  = c.departamento;
  if (c.comprador     !== undefined) rec._encargado     = c.comprador;
  const fechaEl = document.getElementById(`fecha-clasif-${ci}`);
  if (fechaEl && fechaEl.value) rec.Día = fechaEl.value;
  const dudaBtn = document.getElementById(`validado-panel-${ci}`);
  if (dudaBtn) rec._duda = (dudaBtn.dataset.checked === 'true') ? 'Sí' : 'No';
  const valBtn = document.getElementById(`revisado-panel-${ci}`);
  if (valBtn) rec._validado = (valBtn.dataset.checked === 'true') ? 'Sí' : 'No';
  // Re-render la tabla Resumen del modal SÓLO si el foco no está adentro
  // (evita destruir el caret cuando el usuario está editando Descripción)
  const active = document.activeElement;
  const editingInside = active && active.closest && active.closest('#bn-classify-modal-resumen');
  if (!editingInside) {
    const resumenWrap = document.getElementById('bn-classify-modal-resumen');
    if (resumenWrap) resumenWrap.innerHTML = bn_buildBnResumenTable(rec, idx);
  }
  // Actualizar visibilidad del botón 'Guardar cambios' según dirty real
  bn_classifyRefreshActions(ci);
}

/** Llamado al editar la celda Descripción de la tabla Resumen. Muestra el
 *  action-bar de la tabla; si la celda está dentro del modal Clasificar,
 *  también activa el botón 'Guardar cambios' del panel Clasificar. */
async function bn_autoSaveResumenDesc(idx, cell) {
  const rec = BN_CUR_RECS[idx];
  if (!rec || !rec.rowNum) return;
  const newVal = (cell.textContent || '').trim();
  if ((rec.DESCRIPCION || '') === newVal) return;
  rec.DESCRIPCION = newVal;
  cell.style.outline = '2px solid #64748b';
  try {
    const payload = bn_buildSavePayload(rec, { descripcion_edit: true });
    payload.descripcion = newVal;
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const j = await resp.json();
    cell.style.outline = j.ok ? '2px solid #16a34a' : '2px solid #dc2626';
    setTimeout(() => { cell.style.outline = ''; }, 1200);
    if (j.ok) {
      const card = document.getElementById(`bn-card-${idx}`);
      if (card && !cell.closest('#bn-classify-modal-resumen')) {
        card.outerHTML = bn_createCard(rec, idx);
      }
    }
  } catch (_) {
    cell.style.outline = '2px solid #dc2626';
    setTimeout(() => { cell.style.outline = ''; }, 1500);
  }
}

function bn_onResumenEdit(idx, cell) {
  try { showTableActions('bnr', idx); } catch(_) {}
  if (cell && cell.closest('#bn-classify-modal-resumen')) {
    try { markClassifyDirty('bn' + idx); } catch(_) {}
  }
}

/** Edición en vivo de Comentarios en la tabla Resumen del modal:
 *  refleja al textarea inferior y al estado rec en tiempo real. */
/** Llamado al editar el textarea Comentarios inferior del panel Clasificar:
 *  refleja en la celda Comentarios de la tabla de detalles sin destruir caret. */
function bn_onComentariosTextareaInput(ci, textarea) {
  const m = /^bn(\d+)$/.exec(ci);
  if (!m) return;
  const idx = parseInt(m[1], 10);
  const rec = BN_CUR_RECS[idx]; if (!rec) return;
  rec._comentarios = textarea.value;
  // Actualizar celda Comentarios en la tabla Resumen si el foco no está dentro
  const cell = document.querySelector('#bn-classify-modal-resumen [data-field="COMENT"]');
  if (cell && document.activeElement !== cell) cell.textContent = textarea.value;
}

function bn_onResumenComentEdit(idx, cell) {
  const rec = BN_CUR_RECS[idx]; if (!rec) return;
  const newVal = (cell.textContent || '').trim();
  rec._comentarios = newVal;
  // Sincroniza con el textarea Comentarios del panel inferior si está visible
  const ci = 'bn' + idx;
  const textarea = document.getElementById(`comentarios-${ci}`);
  if (textarea && textarea.value !== newVal) textarea.value = newVal;
  try { showTableActions('bnr', idx); } catch(_) {}
  if (cell.closest('#bn-classify-modal-resumen')) {
    try { markClassifyDirty('bn' + idx); } catch(_) {}
  }
}

/** Edición en vivo de "Dudas texto" en la tabla resumen: refleja al rec y la card. */
function bn_onResumenDudaNotaEdit(idx, cell) {
  const rec = BN_CUR_RECS[idx]; if (!rec) return;
  rec._duda_nota = (cell.textContent || '').trim();
  // Actualiza el subtítulo de la nota debajo del botón ? del card si existe
  const noteEl = document.getElementById(`bn-duda-note-${idx}`);
  if (noteEl) {
    noteEl.textContent = rec._duda_nota;
    noteEl.style.display = rec._duda_nota ? '' : 'none';
  }
  try { showTableActions('bnr', idx); } catch(_) {}
  if (cell.closest('#bn-classify-modal-resumen')) {
    try { markClassifyDirty('bn' + idx); } catch(_) {}
  }
}

/** Persiste "Dudas texto" al blur con indicador visual. */
async function bn_autoSaveResumenDudaNota(idx, cell) {
  const rec = BN_CUR_RECS[idx];
  if (!rec || !rec.rowNum) return;
  const newVal = (cell.textContent || '').trim();
  rec._duda_nota = newVal;
  cell.style.outline = '2px solid #64748b';
  try {
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rowNum: rec.rowNum, duda_nota_edit: true, duda_nota: newVal }),
    });
    const j = await resp.json();
    cell.style.outline = j.ok ? '2px solid #16a34a' : '2px solid #dc2626';
    setTimeout(() => { cell.style.outline = ''; }, 1200);
  } catch (_) {
    cell.style.outline = '2px solid #dc2626';
    setTimeout(() => { cell.style.outline = ''; }, 1500);
  }
}

/** Persiste Comentarios al blur con indicador visual. */
async function bn_autoSaveResumenComent(idx, cell) {
  const rec = BN_CUR_RECS[idx];
  if (!rec || !rec.rowNum) return;
  const newVal = (cell.textContent || '').trim();
  if ((rec._comentarios || '') === newVal && cell.dataset.savedOnce) return;
  rec._comentarios = newVal;
  cell.dataset.savedOnce = '1';
  cell.style.outline = '2px solid #64748b';
  try {
    const payload = bn_buildSavePayload(rec, {});
    payload.clasificacion.comentarios = newVal;
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const j = await resp.json();
    cell.style.outline = j.ok ? '2px solid #16a34a' : '2px solid #dc2626';
    setTimeout(() => { cell.style.outline = ''; }, 1200);
  } catch (_) {
    cell.style.outline = '2px solid #dc2626';
    setTimeout(() => { cell.style.outline = ''; }, 1500);
  }
}

// Toggle 'Validado' (✓) — al activar, el registro sale de 'Por clasificar'
// y aparece en su sección de Registros contables. Persiste a Sheets.
async function bn_syncValidado(idx, checked) {
  const ci  = 'bn' + idx;
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  rec._validado = checked ? 'Sí' : 'No';

  // Actualizar botón en encabezado
  const btn = document.getElementById(`bn-revisado-${ci}`);
  if (btn) {
    btn.dataset.checked   = checked ? 'true' : 'false';
    btn.style.borderColor = checked ? '#16a34a' : '#e5e7eb';
    btn.style.background  = checked ? '#16a34a' : '#f9fafb';
    btn.style.color       = checked ? '#fff'    : '#d1d5db';
    btn.title             = checked ? 'Validado — disponible en Registros contables' : 'Marcar como Validado (sale de Por clasificar)';
  }
  // Sincronizar botón dentro del panel Clasificar si está abierto
  const panelBtn = document.getElementById(`revisado-panel-${ci}`);
  if (panelBtn) {
    panelBtn.dataset.checked   = checked ? 'true' : 'false';
    panelBtn.style.borderColor = checked ? '#16a34a' : '#e5e7eb';
    panelBtn.style.background  = checked ? '#16a34a' : '#f9fafb';
    panelBtn.style.color       = checked ? '#fff'    : '#d1d5db';
    panelBtn.title             = checked ? 'Validado' : 'Marcar como Validado';
  }
  // Sync de la tabla Resumen del modal si está abierto
  bn_modalLiveSync(idx);

  if (!rec.rowNum) return;
  try {
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rowNum:        rec.rowNum,
        validado_edit: true,
        validado:      rec._validado,
        clasificacion: {
          cuenta:          rec._cuenta          || '',
          subcuenta:       rec._subcuenta       || '',
          categoria_gasto: rec._categoria_gasto || '',
          concepto:        rec._concepto        || '',
          propiedad:       rec._propiedad       || '',
          departamento:    rec._departamento    || '',
          encargado:       rec._encargado       || '',
          deducible:       rec._deducible       || 'No',
          reembolso:       rec._reembolso       || 'No',
          reembolso_a:     rec._reembolso_a     || '',
          metodo_pago:     rec._metodo_pago     || '',
          clasificado_por: currentUser || '',
          duda:            rec._duda     || '',
          validado:        rec._validado || '',
        }
      }),
    });
    const result = await resp.json();
    if (!result.ok) throw new Error(result.error || 'Error');
  } catch (e) {
    console.warn('Error guardando Revisado:', e.message);
  }
}

// Llamado desde el toggle del encabezado (módulo bancos) — actualiza
// memoria + panel Clasificar + Sheets en tiempo real (sin presionar Guardar).
async function bn_syncDeducible(idx, checked) {
  const ci    = 'bn' + idx;
  const inner = document.getElementById(`deducible-${ci}`);
  if (inner) inner.checked = checked;
  updateDeducibleLabel(ci, checked);
  const rec = BN_CUR_RECS[idx];
  if (!rec) return;
  rec._deducible = checked ? 'Sí' : 'No';

  // Indicador visual sutil mientras guarda
  const lbl = document.getElementById(`fhl-${ci}`);
  const prevText = lbl?.textContent;
  if (lbl) lbl.textContent = (checked ? 'Deducible' : 'No deducible') + ' …';

  if (!rec.rowNum) { if (lbl) lbl.textContent = prevText; return; }

  try {
    const resp = await fetch(`${BACKEND}/save-banco-clasificacion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rowNum:          rec.rowNum,
        mes:             bn_norm(rec.Mes),
        cuenta_bancaria: bn_norm(rec['Cuenta bancaria'] || ''),
        cuenta:          bn_norm(rec.CUENTA    || ''),
        subcuenta:       bn_norm(rec.SUBCUENTA || ''),
        categoria:       bn_norm(rec.CATEGORIA || ''),
        concepto:        bn_norm(rec.CONCEPTO  || ''),
        descripcion:     bn_norm(rec.DESCRIPCION || ''),
        monto:           Number(rec.Monto || 0),
        clasificacion: {
          cuenta:          rec._cuenta          || '',
          subcuenta:       rec._subcuenta       || '',
          categoria_gasto: rec._categoria_gasto || '',
          concepto:        rec._concepto        || '',
          propiedad:       rec._propiedad       || '',
          departamento:    rec._departamento    || '',
          encargado:       rec._encargado       || '',
          deducible:       rec._deducible       || 'No',
          reembolso:       rec._reembolso       || 'No',
          reembolso_a:     rec._reembolso_a     || '',
          metodo_pago:     rec._metodo_pago     || '',
          clasificado_por: currentUser || '',
        }
      }),
    });
    const result = await resp.json();
    if (!result.ok) throw new Error(result.error || 'Error');
    if (lbl) lbl.textContent = checked ? 'Deducible' : 'No deducible';
  } catch (e) {
    console.warn('Error guardando Deducible:', e.message);
    if (lbl) lbl.textContent = (checked ? 'Deducible' : 'No deducible') + ' ⚠';
  }
}

// ─── Buscador ──────────────────────────────────────────────────────────────

function onClassifySearch(i, q) {
  const resEl = document.getElementById(`search-results-${i}`);
  if (!q.trim()) { resEl.classList.add("hidden"); return; }

  const lower   = q.toLowerCase();
  const matches = SEARCH_INDEX.filter(e =>
    [e.cuenta, e.subcuenta, e.categoria, e.concepto].some(f => f.toLowerCase().includes(lower))
  ).slice(0, 12);

  searchMatches[i] = matches;

  if (!matches.length) {
    resEl.innerHTML = `<div class="search-no-results">Sin resultados para "${esc(q)}"</div>`;
    resEl.classList.remove("hidden");
    return;
  }

  resEl.innerHTML = matches.map((m, idx) => {
    const parts = [m.cuenta, m.subcuenta, m.categoria, m.concepto].filter(Boolean);
    const html  = parts.map(p => `<span>${esc(p)}</span>`).join(`<span class="search-sep"> › </span>`);
    return `<div class="search-result-item" onmousedown="applySearchResult('${i}',${idx})">${html}</div>`;
  }).join("");
  resEl.classList.remove("hidden");
}

function hideSearchResults(i) {
  document.getElementById(`search-results-${i}`)?.classList.add("hidden");
}

function applySearchResult(i, idx) {
  const m = (searchMatches[i] || [])[idx];
  if (!m) return;

  // Fuerza una activación limpia: quita 'active' del grid antes para que
  // selectCuenta no tome el atajo de "click sobre activa → deseleccionar".
  const cuentaGrid = document.getElementById(`cuenta-grid-${i}`);
  cuentaGrid?.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  const cuentaCard = Array.from(cuentaGrid?.querySelectorAll(".cuenta-card") || [])
    .find(c => c.dataset.value === m.cuenta);
  if (cuentaCard) selectCuenta(cuentaCard, i);

  // Seleccionar tarjeta de Subcuenta (ya renderizada por selectCuenta)
  if (m.subcuenta) {
    const subGrid = document.getElementById(`subcuenta-grid-${i}`);
    subGrid?.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
    const subCard = Array.from(subGrid?.querySelectorAll(".cuenta-card") || [])
      .find(c => c.dataset.value === m.subcuenta);
    if (subCard) selectSubcuenta(subCard, i);
  }

  // Seleccionar tarjeta de Categoría (ya renderizada por selectSubcuenta)
  if (m.categoria) {
    const catGrid = document.getElementById(`categoria-grid-${i}`);
    catGrid?.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
    const catCard = Array.from(catGrid?.querySelectorAll(".cuenta-card") || [])
      .find(c => c.dataset.value === m.categoria);
    if (catCard) selectCategoria(catCard, i);
  }

  // Seleccionar tarjeta de Concepto (ya renderizada por selectCategoria / selectSubcuenta)
  if (m.concepto) {
    const conGrid = document.getElementById(`concepto-grid-${i}`);
    conGrid?.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
    const conCard = Array.from(conGrid?.querySelectorAll(".cuenta-card") || [])
      .find(c => c.dataset.value === m.concepto);
    if (conCard) selectConcepto(conCard, i);
  }

  // Actualizar texto de ruta de clasificación bajo la barra de búsqueda
  updateClasiPath(i);

  // Limpiar buscador
  document.getElementById(`search-${i}`).value = "";
  hideSearchResults(i);
  markClassifyDirty(i);
}

// ─── Get classification values ──────────────────────────────────────────────

function getClassify(i) {
  return {
    fecha:             document.getElementById(`fecha-clasif-${i}`)?.value   || "",
    cuenta:            document.getElementById(`cuenta-${i}`)?.value         || "",
    subcuenta:         document.getElementById(`subcuenta-${i}`)?.value      || "",
    categoria:         document.getElementById(`categoria-${i}`)?.value      || "",
    concepto:          document.getElementById(`concepto-${i}`)?.value       || "",
    propiedad:         (() => {
      const sel = document.getElementById(`propiedad-${i}`);
      if (!sel) return "";
      if (sel.value === "Otro") return document.getElementById(`propiedad-otro-${i}`)?.value?.trim() || "Otro";
      return sel.value;
    })(),
    departamento:      document.getElementById(`departamento-${i}`)?.value   || "",
    comprador:         document.getElementById(`comprador-${i}`)?.value      || "",
    deducible:         document.getElementById(`deducible-${i}`)?.checked    || false,
    reembolso:         document.getElementById(`reembolso-${i}`)?.checked    || false,
    reembolso_a:       (() => {
      const sel = document.getElementById(`reembolso-a-${i}`);
      if (!sel) return "";
      if (sel.value === "Otro") return document.getElementById(`reembolso-otro-${i}`)?.value?.trim() || "Otro";
      return sel.value;
    })(),
    metodo_pago_clasif:document.getElementById(`metodo-clasif-${i}`)?.value  || "",
    detalles_operacion:document.getElementById(`detalles-${i}`)?.value?.trim()|| "",
    comentarios:       document.getElementById(`comentarios-${i}`)?.value?.trim() || "",
  };
}

// ─── Per-ticket Excel ──────────────────────────────────────────────────────

async function downloadExcelForTicket(i) {
  try {
    showLoading("Generando Excel...", "Preparando archivo...");
    setStatus(`status-${i}`, "");

    const c    = getClassify(i);
    const form = new FormData();
    form.append("files",        ticketResults[i].file);
    form.append("cuenta",       c.cuenta);
    form.append("subcuenta",    c.subcuenta);
    form.append("categoria",    c.categoria);
    form.append("concepto",     c.concepto);
    form.append("propiedad",    c.propiedad);
    form.append("departamento", c.departamento);
    form.append("comprador",    c.comprador);
    form.append("comentarios",  c.comentarios);

    const res = await fetch(`${BACKEND}/process`, { method: "POST", body: form });
    if (!res.ok) {
      let msg = "Error al generar archivo.";
      try { const d = await res.json(); msg = d.error || msg; } catch (_) {}
      throw new Error(msg);
    }

    const blob     = await res.blob();
    const tienda   = ticketResults[i].resumen.tienda || `ticket_${i + 1}`;
    const filename = `${tienda.replace(/[^a-z0-9]/gi, "_")}_transcripcion.xlsx`;
    const a = Object.assign(document.createElement("a"), {
      href: URL.createObjectURL(blob), download: filename
    });
    a.click();
    URL.revokeObjectURL(a.href);
    setStatus(`status-${i}`, "Excel descargado correctamente.");
  } catch (err) {
    setStatus(`status-${i}`, "Error: " + err.message);
  } finally {
    hideLoading();
  }
}

// ─── Per-ticket Sheets ─────────────────────────────────────────────────────

async function saveToSheetsForTicket(i) {
  try {
    showLoading("Guardando en Sheets...", "Enviando datos...");
    setStatus(`status-${i}`, "");

    const c    = getClassify(i);
    const form = new FormData();
    form.append("files",        ticketResults[i].file);
    form.append("saveToSheets", "true");
    form.append("cuenta",       c.cuenta);
    form.append("subcuenta",    c.subcuenta);
    form.append("categoria",    c.categoria);
    form.append("concepto",     c.concepto);
    form.append("propiedad",    c.propiedad);
    form.append("departamento", c.departamento);
    form.append("comprador",    c.comprador);
    form.append("comentarios",  c.comentarios);

    const res  = await fetch(`${BACKEND}/process-json`, { method: "POST", body: form });
    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || "No se pudo guardar.");

    markAsClassified(i);
    setStatus(`status-${i}`, "Guardado correctamente.");
  } catch (err) {
    setStatus(`status-${i}`, "Error: " + err.message);
  } finally {
    hideLoading();
  }
}

// ─── Formato de hora ───────────────────────────────────────────────────────

/** Convierte valores de Sheets (1899-12-30...) a HH:mm legible */
function formatHora(val) {
  if (!val) return "";
  const s = String(val).trim();
  // Fecha-tiempo 1899-12-30T16:30... → extraer sólo la hora
  const dtMatch = s.match(/^1899-12-30[T ](\d{2}:\d{2})/);
  if (dtMatch) return dtMatch[1];
  // Fecha sin tiempo 1899-12-30 → dato perdido, mostrar vacío
  if (/^1899-12-30/.test(s)) return "";
  // Cualquier ISO con T → extraer hora
  const isoMatch = s.match(/T(\d{2}:\d{2})/);
  if (isoMatch) return isoMatch[1];
  return s; // ya es HH:mm u otro formato legible
}

// ─── Leer ediciones de tablas desde el DOM ─────────────────────────────────

/**
 * Lee el estado actual de la tabla de productos desde el DOM.
 * Maneja filas editadas, eliminadas y nuevas.
 * Devuelve array de objetos producto listos para guardar.
 */
function readAllProductsFromDOM(selector, originalProds, ticketId) {
  const tbody = document.querySelector(`${selector} tbody`);
  if (!tbody) return (originalProds || []);
  return [...tbody.querySelectorAll("tr")].map((tr, domIdx) => {
    const origIdx = parseInt(tr.dataset.row ?? "", 10);
    const orig    = isNaN(origIdx) ? {} : ((originalProds || [])[origIdx] || {});
    const fields  = {};
    tr.querySelectorAll("td[data-field]").forEach(td => {
      fields[td.dataset.field] = td.innerText.trim();
    });
    // Ignorar filas completamente vacías (nuevas sin datos)
    if (!orig.descripcion && !fields.descripcion && !fields.monto) return null;
    return {
      ticket_id:               orig.ticket_id    || ticketId || "",
      linea_numero:            domIdx + 1,
      descripcion:             fields.descripcion             ?? orig.descripcion             ?? "",
      cantidad:                numEdit(fields.cantidad,        orig.cantidad),
      precio_unitario:         numEdit(fields.precio_unitario, orig.precio_unitario),
      monto:                   numEdit(fields.monto,           orig.monto),
      categoria_operativa:     fields.categoria_operativa     ?? orig.categoria_operativa     ?? "",
      deducible_sugerido:      fields.deducible_sugerido      ?? orig.deducible_sugerido      ?? "",
      confianza_clasificacion: fields.confianza_clasificacion ?? orig.confianza_clasificacion ?? "",
    };
  }).filter(Boolean);
}

/** Devuelve {field: value} de las filas editables de la tabla de resumen/cruce */
function readResumenEditsFromDOM(selector) {
  const tbody = document.querySelector(`${selector} tbody`);
  if (!tbody) return {};
  const obj = {};
  tbody.querySelectorAll("tr[data-field]").forEach(tr => {
    const td = tr.querySelector("td[contenteditable]");
    if (td) obj[tr.dataset.field] = td.innerText.trim();
  });
  return obj;
}

// ─── Agregar / eliminar renglones en tabla Transcripción ──────────────────

function addAnalysisProductRow(i) {
  addProductRowToTable(`#tab-transcripcion-${i}`, true);
}
function addDashboardProductRow(i) {
  addProductRowToTable(`#db-tab-transcripcion-${i}`, false);
}

function addProductRowToTable(selector, isAnalysis) {
  const tbody = document.querySelector(`${selector} tbody`);
  if (!tbody) return;
  const rowIdx = tbody.querySelectorAll("tr").length;
  const editCols = isAnalysis
    ? ["descripcion","cantidad","precio_unitario","monto","categoria_operativa","deducible_sugerido","confianza_clasificacion"]
    : ["descripcion","cantidad","precio_unitario","monto"];
  const tr = document.createElement("tr");
  tr.dataset.row = "new";
  let html = isAnalysis ? `<td style="background:#f1f5f9;color:#94a3b8">${rowIdx + 1}</td>` : "";
  html += editCols.map(k =>
    `<td contenteditable="true" spellcheck="false" data-field="${k}" oninput="onTableCellInput(this)"></td>`
  ).join("");
  html += `<td class="btn-del-cell"><button class="btn-del-row" onclick="deleteProductRow(this)" title="Eliminar fila">✕</button></td>`;
  tr.innerHTML = html;
  tbody.appendChild(tr);
  tr.querySelector("[contenteditable]")?.focus();
  // Marcar dirty
  const wrap = tbody.closest(".ticket-table-wrap");
  if (wrap) showTableActions(wrap.dataset.tmod, parseInt(wrap.dataset.tidx));
}

function deleteProductRow(btn) {
  const wrap = btn.closest(".ticket-table-wrap");
  btn.closest("tr")?.remove();
  if (wrap) showTableActions(wrap.dataset.tmod, parseInt(wrap.dataset.tidx));
}

// ─── Custom confirm dialog ─────────────────────────────────────────────────

function showConfirm({ icon = "🗑", title, msg, okLabel = "Eliminar" }) {
  return new Promise(resolve => {
    document.getElementById("confirmModalIcon").textContent  = icon;
    document.getElementById("confirmModalTitle").textContent = title;
    document.getElementById("confirmModalMsg").textContent   = msg;
    document.getElementById("confirmModalOk").textContent    = okLabel;
    document.getElementById("confirmModal").classList.remove("hidden");

    const ok = document.getElementById("confirmModalOk");
    const cancel = document.getElementById("confirmModalCancel");
    const close = result => {
      document.getElementById("confirmModal").classList.add("hidden");
      ok.onclick = cancel.onclick = null;
      resolve(result);
    };
    ok.onclick     = () => close(true);
    cancel.onclick = () => close(false);
  });
}
function confirmModalOverlayClick(e) {
  if (e.target === e.currentTarget) {
    document.getElementById("confirmModal").classList.add("hidden");
  }
}

// ─── Classify dirty tracking ───────────────────────────────────────────────

function markClassifyDirty(idx) {
  if (classifyAutoPopulating) return;
  // Para registros bancarios usamos comparación contra el snapshot inicial:
  // sólo mostramos 'Guardar cambios' si hay diferencia real con el estado al abrir.
  if (typeof idx === 'string' && /^bn\d+$/.test(idx)) {
    const n = parseInt(idx.slice(2), 10);
    if (!isNaN(n)) bn_modalLiveSync(n);   // ya llama bn_classifyRefreshActions
    return;
  }
  // Otros módulos (tickets) usan el comportamiento clásico
  document.getElementById(`classify-actions-${idx}`)?.classList.remove("hidden");
}
function hideClassifyActions(idx) {
  document.getElementById(`classify-actions-${idx}`)?.classList.add("hidden");
}

// ─── Dirty tracking de tablas ──────────────────────────────────────────────

/** Llamado por oninput en cualquier celda editable de una tabla */
function onTableCellInput(cell) {
  const wrap = cell.closest(".ticket-table-wrap");
  if (!wrap) return;
  showTableActions(wrap.dataset.tmod, parseInt(wrap.dataset.tidx));
}

function showTableActions(mod, i) {
  document.getElementById(`tact-${mod}-${i}`)?.classList.remove("hidden");
}
function hideTableActions(mod, i) {
  document.getElementById(`tact-${mod}-${i}`)?.classList.add("hidden");
}

// ── Análisis: guardar cambios de tabla en memoria (sin llamada al backend) ──
function saveTableChanges(i) {
  const ticket = ticketResults[i];
  if (!ticket) return;
  const prodsFinal = readAllProductsFromDOM(`#tab-transcripcion-${i}`, ticket.productos, ticket.resumen.ticket_id);
  ticket.productos = prodsFinal;
  const resEdits = readResumenEditsFromDOM(`#tab-resumen-${i}`);
  Object.assign(ticket.resumen, resEdits);
  hideTableActions("analysis", i);
}

// ── Análisis: revertir tablas al estado en memoria ──────────────────────────
function resetTableChanges(i) {
  const ticket = ticketResults[i];
  if (!ticket) return;
  const tDiv = document.getElementById(`tab-transcripcion-${i}`);
  if (tDiv) tDiv.innerHTML = buildProductTable(ticket.productos) +
    `<button class="btn-add-row" onclick="addAnalysisProductRow(${i})">＋ Agregar producto</button>`;
  const rDiv = document.getElementById(`tab-resumen-${i}`);
  if (rDiv) rDiv.innerHTML = buildResumenTable(ticket.resumen);
  const cDiv = document.getElementById(`tab-cruce-${i}`);
  if (cDiv) cDiv.innerHTML = buildCruceTable(ticket.cruce);
  hideTableActions("analysis", i);
}

// ── Dashboard: guardar cambios de tabla en Sheets ───────────────────────────
async function saveDbTableChanges(i) {
  const ticket = dashboardFiltered[i];
  if (!ticket) return;
  const r = ticket.resumen || {};

  const resEdits = readResumenEditsFromDOM(`#db-tab-resumen-${i}`);
  const productosEditados = readAllProductsFromDOM(
    `#db-tab-transcripcion-${i}`, ticket.productos, ticket.ticket_id
  ).map(p => ({
    linea_numero:    p.linea_numero,
    descripcion:     p.descripcion,
    cantidad:        p.cantidad,
    precio_unitario: p.precio_unitario,
    monto:           p.monto,
  }));

  // Preservar clasificación existente en memoria, solo sobreescribir campos de tabla
  const clasificacion = {
    fecha:              r.fecha              || "",
    cuenta:             r.cuenta             || "",
    subcuenta:          r.subcuenta          || "",
    categoria_gasto:    r.categoria_gasto    || "",
    concepto:           r.concepto           || "",
    propiedad:          r.propiedad          || "",
    departamento:       r.departamento       || "",
    comprador:          r.comprador          || "",
    deducible:          r.deducible          || "No",
    reembolso:          r.reembolso          || "No",
    reembolso_a:        r.reembolso_a        || "",
    metodo_pago:        r.metodo_pago        || "",
    detalles_operacion: r.detalles_operacion || "",
    comentarios:        r.comentarios        || "",
    clasificado_por:    currentUser,
    tienda:           resEdits.tienda           ?? r.tienda           ?? "",
    rfc:              resEdits.rfc              ?? r.rfc              ?? "",
    hora:             resEdits.hora             ?? r.hora             ?? "",
    folio:            resEdits.folio            ?? r.folio            ?? "",
    tarjeta_ultimos4: resEdits.tarjeta_ultimos4 ?? r.tarjeta_ultimos4 ?? "",
    subtotal:  numEdit(resEdits.subtotal,   r.subtotal   ?? 0),
    iva:       numEdit(resEdits.iva,        r.iva        ?? 0),
    ieps:      numEdit(resEdits.ieps,       r.ieps       ?? 0),
    descuentos:numEdit(resEdits.descuentos, r.descuentos ?? 0),
    total:     numEdit(resEdits.total,      r.total      ?? 0),
  };

  try {
    showLoading("Guardando cambios…", "Actualizando tablas en Sheets…");
    const res  = await fetch(`${BACKEND}/update-ticket`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ ticket_id: ticket.ticket_id, clasificacion, productos_editados: productosEditados }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Error al guardar");
    Object.assign(ticket.resumen, clasificacion);
    ticket.productos = productosEditados;
    hideTableActions("db", i);
  } catch (err) {
    alert("Error al guardar: " + err.message);
  } finally {
    hideLoading();
  }
}

// ── Dashboard: revertir tablas al estado en memoria ─────────────────────────
function resetDbTableChanges(i) {
  const ticket = dashboardFiltered[i];
  if (!ticket) return;
  const tDiv = document.getElementById(`db-tab-transcripcion-${i}`);
  if (tDiv) tDiv.innerHTML = buildDashboardProductTable(ticket.productos || []) +
    `<button class="btn-add-row" onclick="addDashboardProductRow(${i})">＋ Agregar producto</button>`;
  const rDiv = document.getElementById(`db-tab-resumen-${i}`);
  if (rDiv) rDiv.innerHTML = buildResumenTable(ticket.resumen);
  const cDiv = document.getElementById(`db-tab-cruce-${i}`);
  if (cDiv) cDiv.innerHTML = buildDashboardCruceTable(ticket);
  hideTableActions("db", i);
}

/** Convierte un valor editado a número; si falla devuelve el fallback */
function numEdit(val, fallback) {
  if (val === undefined || val === "") return fallback;
  const n = Number(val);
  return isNaN(n) ? fallback : n;
}

// ─── Utilities ─────────────────────────────────────────────────────────────

function populateSelect(selectEl, options, placeholder) {
  if (!selectEl) return;
  selectEl.innerHTML = `<option value="">${placeholder}</option>`;
  options.forEach(opt => {
    const o = document.createElement("option");
    o.value = o.textContent = opt;
    selectEl.appendChild(o);
  });
}

function setStatus(id, msg) {
  const el = document.getElementById(id);
  if (el) el.textContent = msg;
}

/** Convierte cualquier representación de número a float.
 *  Maneja: números, strings con $ o comas ("2,796.89"), y formato español ("2.796,89"). */
function parseTotal(v) {
  if (v === null || v === undefined || v === "" || v === false) return 0;
  if (typeof v === "number") return isNaN(v) ? 0 : v;
  let s = String(v).replace(/[$\s%]/g, "");
  // Formato español: punto como miles, coma como decimal  →  "2.796,89"
  if (/^\d{1,3}(\.\d{3})+(,\d+)?$/.test(s)) {
    s = s.replace(/\./g, "").replace(",", ".");
  } else {
    // Formato anglosajón: coma como miles, punto como decimal  →  "2,796.89"
    s = s.replace(/,/g, "");
  }
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

/** Total definitivo de un ticket: intenta resumen.total, luego cruce.total_ticket */
function ticketTotal(t) {
  const a = parseTotal(t.resumen?.total);
  if (a > 0) return a;
  return parseTotal(t.cruce?.total_ticket) || parseTotal(t.cruce?.monto_cruce) || 0;
}

function money(value) {
  const n = parseTotal(value);
  if (!Number.isFinite(n) || n === 0) return "";
  return esc(n.toLocaleString("es-MX", { style: "currency", currency: "MXN" }));
}

function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g, s =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[s])
  );
}

// ─── Sección switcher ──────────────────────────────────────────────────────

/** Cambia entre módulos de nivel superior */
function switchModule(mod) {
  ["tickets", "registros", "huespedes", "lodgify"].forEach(m => {
    document.getElementById(`module-${m}`)?.classList.toggle("hidden", m !== mod);
    document.getElementById(`tab-module-${m}`)?.classList.toggle("active", m === mod);
    document.getElementById(`nav-item-${m}`)?.classList.toggle("active", m === mod);
  });
  if (mod === "huespedes") {
    if (!HU_STATE.loaded && !HU_STATE.loading) huespedesLoad(true);
    else huespedesRender();
  }
  if (mod === "lodgify") {
    if (!LG_STATE.loaded && !LG_STATE.loading) lodgifyLoad(true);
    else lodgifyRender();
  }
}

// ─── Hamburger nav menu ───────────────────────────────────────────────────────
function toggleNavMenu() {
  const sidebar = document.getElementById('nav-sidebar');
  const overlay = document.getElementById('nav-overlay');
  const open = sidebar.classList.toggle('open');
  overlay.classList.toggle('hidden', !open);
}
function closeNavMenu() {
  document.getElementById('nav-sidebar')?.classList.remove('open');
  document.getElementById('nav-overlay')?.classList.add('hidden');
}

/** Cambia entre sub-secciones dentro del módulo Tickets */
function switchSection(sec) {
  ["capturados", "captura"].forEach(s => {
    document.getElementById(`section-${s}`)?.classList.toggle("hidden", s !== sec);
    document.getElementById(`subtab-${s}`)?.classList.toggle("active", s === sec);
  });
  if (sec === "capturados") loadDashboard();
}

// ─── Dashboard: carga desde Sheets ────────────────────────────────────────

async function loadDashboard() {
  const container = document.getElementById("dbContainer");
  const countBar  = document.getElementById("db-count-bar");
  container.innerHTML = `
    <div class="empty-state" style="padding:40px 20px">
      <div class="thinking-dots" style="margin:0 auto 16px">
        <div class="dot"></div><div class="dot"></div><div class="dot"></div>
      </div>
      <p>Cargando tickets desde Sheets…</p>
    </div>`;
  countBar.textContent = "";
  try {
    const res  = await fetch(`${BACKEND}/get-tickets`);
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Error al cargar tickets");
    dashboardTickets = data.tickets || [];
    populateDashboardFilters();
    // Siempre resetear al cargar: limpiar inputs y encender el switch
    clearDashboardFiltersInternal();
    const todasEl = document.getElementById("db-f-todas");
    if (todasEl) todasEl.checked = true;
    dbCurrentPage = 1;
    applyDashboardFilters();
  } catch (err) {
    container.innerHTML = `
      <div class="empty-state" style="padding:40px 20px">
        <div class="empty-icon">⚠️</div>
        <p>Error: ${esc(err.message)}</p>
        <button class="btn-analizar" style="margin-top:14px;width:auto;padding:12px 28px"
          onclick="dashboardTickets=[];loadDashboard()">Reintentar</button>
      </div>`;
  }
}

// ─── Dashboard: filtros ────────────────────────────────────────────────────

function populateDashboardFilters() {
  const unique = key =>
    [...new Set(dashboardTickets.map(t => String(t.resumen[key] || "")).filter(Boolean))].sort();

  const fillSelect = (id, values) => {
    const sel = document.getElementById(id);
    if (!sel) return;
    const current = sel.value;
    while (sel.options.length > 1) sel.remove(1);
    values.forEach(v => {
      const o = document.createElement("option");
      o.value = o.textContent = v;
      sel.appendChild(o);
    });
    if (values.includes(current)) sel.value = current;
  };

  fillSelect("db-f-metodo",          unique("metodo_pago"));
  fillSelect("db-f-propiedad",       unique("propiedad"));
  fillSelect("db-f-departamento",    unique("departamento"));
  fillSelect("db-f-comprador",       unique("comprador"));
  fillSelect("db-f-tienda",          unique("tienda"));
  fillSelect("db-f-clasificado-por", unique("clasificado_por"));

  // Descripciones únicas de productos (todos los tickets)
  const descs = [...new Set(
    dashboardTickets.flatMap(t => (t.productos || []).map(p => String(p.descripcion || "").trim()))
    .filter(Boolean)
  )].sort();
  fillSelect("db-f-descripcion", descs);

  // Ajustar el máximo del slider según el "Total ticket" más alto (resumen.total con fallback a cruce)
  const maxTotal   = dashboardTickets.reduce((mx, t) => Math.max(mx, ticketTotal(t)), 0);
  const sliderMax  = Math.max(1000, Math.ceil(maxTotal / 500) * 500);  // múltiplo de 500
  // Step: ~1% del máximo, redondeado a múltiplo de 100, mínimo 100  (da ~100 posiciones)
  const sliderStep = Math.max(100, Math.round(sliderMax * 0.01 / 100) * 100);
  const loEl = document.getElementById("db-f-total-min");
  const hiEl = document.getElementById("db-f-total-max");
  if (loEl) { loEl.max = sliderMax; loEl.step = sliderStep; loEl.value = 0; }
  if (hiEl) { hiEl.max = sliderMax; hiEl.step = sliderStep; hiEl.value = sliderMax; }
  updateTotalRangeFill();
}

function getDashboardFilters() {
  const v  = id => document.getElementById(id)?.value || "";
  // Range slider: null when at boundary (no filter effect)
  const getRangeMin = () => {
    const el = document.getElementById("db-f-total-min");
    if (!el) return null;
    const val = Number(el.value), min = Number(el.min) || 0;
    return val <= min ? null : val;
  };
  const getRangeMax = () => {
    const el = document.getElementById("db-f-total-max");
    if (!el) return null;
    const val = Number(el.value), max = Number(el.max) || 50000;
    return val >= max ? null : val;
  };
  return {
    text:           v("db-f-text").trim().toLowerCase(),
    fechaDesde:     v("db-f-desde"),
    fechaHasta:     v("db-f-hasta"),
    cuenta:         v("db-f-cuenta"),
    metodoPago:     v("db-f-metodo"),
    propiedad:      v("db-f-propiedad"),
    departamento:   v("db-f-departamento"),
    comprador:      v("db-f-comprador"),
    deducible:      v("db-f-deducible"),
    reembolso:      v("db-f-reembolso"),
    tienda:         v("db-f-tienda"),
    clasificadoPor: v("db-f-clasificado-por"),
    descripcion:    v("db-f-descripcion"),
    totalMin:       getRangeMin(),
    totalMax:       getRangeMax(),
  };
}

/** Activa/desactiva el switch "Todos los tickets" */
function syncTodasSwitch(checked) {
  if (checked) {
    // Activar = limpiar todos los filtros y mostrar todos
    clearDashboardFiltersInternal();
    const el = document.getElementById("db-f-todas");
    if (el) el.checked = true;
    applyDashboardFilters();
  } else {
    // Desactivar = abrir panel de filtros como pista al usuario (nada más)
    if (!dbFiltersOpen) toggleDbFilters();
  }
}

/** Devuelve true si hay algún filtro activo */
function hasActiveFilters() {
  const f = getDashboardFilters();
  return !!(f.text || f.fechaDesde || f.fechaHasta || f.cuenta || f.metodoPago ||
            f.propiedad || f.departamento || f.comprador || f.deducible || f.reembolso ||
            f.tienda || f.clasificadoPor || f.descripcion || f.totalMin !== null || f.totalMax !== null);
}

/** Llamado desde cada input/select de filtro: desactiva el switch y filtra */
function onFilterChange() {
  const todasEl = document.getElementById("db-f-todas");
  if (todasEl) todasEl.checked = false;
  dbCurrentPage = 1;
  applyDashboardFilters();
}

function applyDashboardFilters() {
  // applyDashboardFilters NO toca el switch — ese estado lo manejan
  // onFilterChange / clearDashboardFilters / syncTodasSwitch / loadDashboard
  const active = hasActiveFilters();

  // Sin filtros activos — mostrar todos
  if (!active) {
    dashboardFiltered = [...dashboardTickets];
    const n = dashboardFiltered.length;
    const sub = document.getElementById("db-filter-subtitle");
    if (sub) sub.textContent = `${n} ticket${n !== 1 ? "s" : ""} (todos)`;
    renderDashboardCards();
    return;
  }

  const f = getDashboardFilters();
  dashboardFiltered = dashboardTickets.filter(t => {
    const r = t.resumen || {};
    const str = k => String(r[k] || "");
    if (f.text) {
      const hay = [r.tienda, r.concepto, r.comprador, r.cuenta,
                   r.subcuenta, r.categoria_gasto].join(" ").toLowerCase();
      if (!hay.includes(f.text)) return false;
    }
    if (f.fechaDesde && str("fecha") < f.fechaDesde) return false;
    if (f.fechaHasta && str("fecha") > f.fechaHasta) return false;
    if (f.cuenta      && str("cuenta")      !== f.cuenta)      return false;
    if (f.metodoPago  && str("metodo_pago") !== f.metodoPago)  return false;
    if (f.propiedad   && str("propiedad")   !== f.propiedad)   return false;
    if (f.departamento&& str("departamento")!== f.departamento) return false;
    if (f.comprador   && str("comprador")   !== f.comprador)   return false;
    if (f.deducible === "si" && str("deducible") !== "Sí") return false;
    if (f.deducible === "no" && str("deducible") === "Sí") return false;
    if (f.reembolso === "si" && str("reembolso") !== "Sí") return false;
    if (f.reembolso === "no" && str("reembolso") === "Sí") return false;
    if (f.tienda         && str("tienda")          !== f.tienda)         return false;
    if (f.clasificadoPor && str("clasificado_por") !== f.clasificadoPor) return false;
    if (f.descripcion) {
      const hasDesc = (t.productos || []).some(p =>
        String(p.descripcion || "").trim() === f.descripcion
      );
      if (!hasDesc) return false;
    }
    const total = ticketTotal(t);
    if (f.totalMin !== null && total < f.totalMin) return false;
    if (f.totalMax !== null && total > f.totalMax) return false;
    return true;
  });

  // Actualizar subtítulo del filtro
  const n = dashboardFiltered.length;
  const t = dashboardTickets.length;
  const sub = document.getElementById("db-filter-subtitle");
  if (sub) sub.textContent = t ? (n === t ? `${t} ticket${t!==1?"s":""}` : `${n} de ${t} tickets`) : "Filtra por cualquier campo";

  renderDashboardCards();
}

function resetRangeSliders() {
  const loEl = document.getElementById("db-f-total-min");
  const hiEl = document.getElementById("db-f-total-max");
  if (loEl) { loEl.value = loEl.min || 0; loEl.style.zIndex = 4; }
  if (hiEl) { hiEl.value = hiEl.max || 50000; hiEl.style.zIndex = 5; }
  updateTotalRangeFill();
}

/** Limpia solo los inputs (sin tocar el switch ni re-renderizar) */
function clearDashboardFiltersInternal() {
  ["db-f-text","db-f-desde","db-f-hasta"].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = "";
  });
  ["db-f-cuenta","db-f-metodo","db-f-propiedad","db-f-departamento","db-f-comprador",
   "db-f-deducible","db-f-reembolso","db-f-tienda","db-f-clasificado-por","db-f-descripcion"].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = "";
  });
  resetRangeSliders();
}

/** Botón "✕ Limpiar": limpia filtros, activa switch y muestra todos */
function clearDashboardFilters() {
  clearDashboardFiltersInternal();
  const el = document.getElementById("db-f-todas");
  if (el) el.checked = true;
  dbCurrentPage = 1;
  applyDashboardFilters();
}

function toggleDbFilters() {
  dbFiltersOpen = !dbFiltersOpen;
  const panel = document.getElementById("db-filter-body");
  const btn   = document.getElementById("btn-filter-toggle");
  panel?.classList.toggle("hidden", !dbFiltersOpen);
  btn?.classList.toggle("active", dbFiltersOpen);
}

/** Actualiza el fill visual y etiquetas del dual-range slider */
function updateTotalRangeFill() {
  const loEl   = document.getElementById("db-f-total-min");
  const hiEl   = document.getElementById("db-f-total-max");
  const fill   = document.getElementById("dual-range-fill");
  const minLbl = document.getElementById("range-total-min-lbl");
  const maxLbl = document.getElementById("range-total-max-lbl");
  if (!loEl || !hiEl || !fill) return;

  const min = Number(loEl.min) || 0;
  const max = Number(loEl.max) || 50000;
  const lo  = Number(loEl.value);
  const hi  = Number(hiEl.value);

  const pLo = ((lo - min) / (max - min)) * 100;
  const pHi = ((hi - min) / (max - min)) * 100;
  fill.style.left  = pLo + "%";
  fill.style.width = (pHi - pLo) + "%";

  const fmt = n => n >= 1000 ? "$" + (n / 1000).toFixed(n % 1000 === 0 ? 0 : 1) + "k" : "$" + n.toLocaleString("es-MX");
  if (minLbl) minLbl.textContent = lo === min ? "$0" : fmt(lo);
  if (maxLbl) maxLbl.textContent = hi >= max ? "Máx." : fmt(hi);
}

function onTotalRangeInput(which) {
  const loEl = document.getElementById("db-f-total-min");
  const hiEl = document.getElementById("db-f-total-max");
  if (!loEl || !hiEl) return;

  // Evitar cruce
  if (which === 'lo' && Number(loEl.value) > Number(hiEl.value)) loEl.value = hiEl.value;
  if (which === 'hi' && Number(hiEl.value) < Number(loEl.value)) hiEl.value = loEl.value;

  // Z-index dinámico: cuando el thumb lo está muy a la derecha, ponerlo encima
  const range = (Number(loEl.max) || 50000) - (Number(loEl.min) || 0);
  const loFrac = range > 0 ? (Number(loEl.value) - (Number(loEl.min) || 0)) / range : 0;
  loEl.style.zIndex = loFrac >= 0.9 ? 5 : 4;
  hiEl.style.zIndex = loFrac >= 0.9 ? 4 : 5;

  updateTotalRangeFill();
  onFilterChange();
}

// ─── Dashboard: render ─────────────────────────────────────────────────────

function renderDashboardCards() {
  const container = document.getElementById("dbContainer");
  const countBar  = document.getElementById("db-count-bar");

  if (!dashboardFiltered.length) {
    container.innerHTML = `
      <div class="empty-state" style="padding:40px 20px">
        <div class="empty-icon">${dashboardTickets.length ? "🔍" : "📋"}</div>
        <p>${dashboardTickets.length ? "Sin resultados para los filtros aplicados" : "No hay tickets guardados aún"}</p>
      </div>`;
    countBar.innerHTML = "";
    renderPagination(0, 0);
    return;
  }

  // Ordenar: sin clasificar primero
  const sinClasif = dashboardFiltered.filter(t => !String(t.resumen?.cuenta || "").trim());
  const clasif    = dashboardFiltered.filter(t =>  String(t.resumen?.cuenta || "").trim());
  const sorted    = [...sinClasif, ...clasif];

  // Paginación
  const totalItems  = sorted.length;
  const totalPages  = Math.max(1, Math.ceil(totalItems / dbPageSize));
  dbCurrentPage     = Math.min(dbCurrentPage, totalPages);
  const start       = (dbCurrentPage - 1) * dbPageSize;
  const pageItems   = sorted.slice(start, start + dbPageSize);

  // Barra de conteo
  const nSin = sinClasif.length;
  countBar.innerHTML = nSin
    ? `<span class="db-count-warn">⚠️ ${nSin} sin clasificar</span> · ${totalItems} total`
    : `<span class="db-count-ok">✅ ${totalItems} clasificado${totalItems !== 1 ? "s" : ""}</span>`;

  container.innerHTML = pageItems.map((t, i) => createDashboardCard(t, start + i)).join("");
  renderPagination(totalItems, totalPages);
}

function renderPagination(totalItems, totalPages) {
  const bar = document.getElementById("db-pagination");
  if (!bar) return;
  if (totalPages <= 1 && totalItems <= 25) { bar.innerHTML = ""; return; }

  const sizes = [25, 50, 100];
  const sizeOpts = sizes.map(s =>
    `<option value="${s}"${s === dbPageSize ? " selected" : ""}>${s}</option>`
  ).join("");

  // Páginas visibles: siempre primera, última, y ±2 alrededor de la actual
  const pages = new Set([1, totalPages]);
  for (let p = Math.max(1, dbCurrentPage - 2); p <= Math.min(totalPages, dbCurrentPage + 2); p++) pages.add(p);
  const pageArr = [...pages].sort((a, b) => a - b);

  let pageBtns = "";
  let prev = 0;
  for (const p of pageArr) {
    if (p - prev > 1) pageBtns += `<span class="pag-ellipsis">…</span>`;
    pageBtns += `<button class="pag-btn${p === dbCurrentPage ? " active" : ""}" onclick="goToPage(${p})">${p}</button>`;
    prev = p;
  }

  bar.innerHTML = `
    <div class="pag-left">
      <select class="pag-size-sel" onchange="changePageSize(Number(this.value))">${sizeOpts}</select>
      <span class="pag-info">por página</span>
    </div>
    <div class="pag-right">
      <button class="pag-btn" onclick="goToPage(${dbCurrentPage - 1})" ${dbCurrentPage <= 1 ? "disabled" : ""}>‹</button>
      ${pageBtns}
      <button class="pag-btn" onclick="goToPage(${dbCurrentPage + 1})" ${dbCurrentPage >= totalPages ? "disabled" : ""}>›</button>
    </div>`;
}

function goToPage(p) {
  const totalPages = Math.max(1, Math.ceil(dashboardFiltered.length / dbPageSize));
  dbCurrentPage = Math.max(1, Math.min(p, totalPages));
  renderDashboardCards();
  // Scroll suave al top de la lista
  document.getElementById("db-count-bar")?.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function changePageSize(size) {
  dbPageSize    = size;
  dbCurrentPage = 1;
  renderDashboardCards();
}

function createDashboardCard(ticket, i) {
  const r        = ticket.resumen || {};
  const str      = k => String(r[k] || "");
  const isClasif = !!str("cuenta").trim();
  const colorCls = CUENTA_COLOR_CLASS[str("cuenta")] || "";
  const clsCls   = isClasif ? `classified ${colorCls}` : "";
  const ci       = DB_IDX + i; // índice para el panel de clasificar

  const metaParts   = [str("fecha"), formatHora(str("hora"))].filter(Boolean);
  const numProd     = Number(r.num_productos) || (ticket.productos || []).length || 0;
  const rawSummary  = (ticket.productos || []).map(p => p.descripcion || "").filter(Boolean).join(", ");
  const prodSummary = rawSummary.length > 95 ? rawSummary.slice(0, 92) + "…" : rawSummary;
  const deptOptions = Array.from({length: 14}, (_, j) => `<option>${j + 1}</option>`).join("");

  // Chips
  const sinClasifChip = !isClasif
    ? `<span class="info-chip chip-sin-clasif">⚠️ Sin clasificar</span>` : "";
  const cuentaChip    = isClasif
    ? `<span class="info-chip ${colorCls}">${CUENTA_EMOJIS[str("cuenta")] || ""} ${esc(str("cuenta"))}</span>` : "";
  const compradorChip = str("comprador")  ? `<span class="info-chip">👤 ${esc(str("comprador"))}</span>` : "";
  const propiedadChip = str("propiedad")  ? `<span class="info-chip">🏠 ${esc(str("propiedad"))}</span>` : "";
  const deptChip      = str("departamento") ? `<span class="info-chip">🚪 Depto. ${esc(str("departamento"))}</span>` : "";
  const reembolsoChip = (str("reembolso") === "Sí" && str("reembolso_a"))
    ? `<span class="info-chip chip-reembolso">↩ Reembolso a ${esc(str("reembolso_a"))}</span>` : "";

  const verTicket   = str("imagen_url")
    ? `<a class="btn-ver-ticket" href="${esc(str("imagen_url"))}" target="_blank" rel="noopener" onclick="event.stopPropagation()">Ver ticket</a>` : "";
  const isDeducible = str("deducible") === "Sí";
  const deducibleHtml = `
    <div class="header-deducible" onclick="event.stopPropagation()">
      <label class="toggle-switch toggle-switch--dark">
        <input type="checkbox" id="db-deducible-header-${i}"
               ${isDeducible ? "checked" : ""}
               onchange="syncDbDeducible(${i}, this.checked)">
        <span class="toggle-slider"></span>
      </label>
      <span class="fhl${isDeducible ? " on" : ""}" id="db-fhl-${i}">${isDeducible ? "Deducible" : "No deducible"}</span>
    </div>`;

  // Pestaña inferior
  const pathParts = [str("cuenta"), str("subcuenta"), str("categoria_gasto"), str("concepto")].filter(Boolean);
  const pathText  = pathParts.join(" › ");
  const tabHtml   = isClasif
    ? `<div class="classify-tab classified ${colorCls}" id="db-btn-classify-${i}" onclick="toggleDbClassify(${i})">
         <span class="classify-tab-arrow" style="color:#fff">›</span>
         <span class="classify-tab-label" style="font-size:10px;text-transform:none;letter-spacing:.01em;font-weight:700;color:#fff;">${esc(pathText)}</span>
       </div>`
    : `<div class="classify-tab" id="db-btn-classify-${i}" onclick="toggleDbClassify(${i})">
         <span class="classify-tab-arrow">›</span>
         <span class="classify-tab-label">Clasificar</span>
       </div>`;

  return `
    <div class="ticket-card" id="db-ticket-${i}">
      <div class="ticket-card-header ${clsCls}" id="db-header-${i}" onclick="toggleDbTable(${i})">
        <button class="btn-x" title="Eliminar registro" onclick="event.stopPropagation(); deleteDbTicket(${i})">×</button>
        <div class="ticket-info">
          <div class="header-chips">${sinClasifChip}${cuentaChip}${compradorChip}${propiedadChip}${deptChip}${reembolsoChip}</div>
          <div class="ticket-store-row">
            <span class="ticket-store">${esc(str("tienda") || "Ticket " + (i + 1))}</span>
            ${verTicket}
          </div>
          <div class="ticket-meta">
            ${esc(metaParts.join(" · "))}${metaParts.length ? " · " : ""}🧾 ${numProd} producto${numProd !== 1 ? "s" : ""}
          </div>
          ${prodSummary ? `<div class="product-summary">${esc(prodSummary)}</div>` : ""}
        </div>
        <div class="ticket-header-right">
          <span>${paymentChip(str("metodo_pago"), str("tarjeta_ultimos4"))}</span>
          <div class="ticket-total-badge ${clsCls}">
            <span class="total-main">${money(r.total)}</span>
            ${r.iva ? `<span class="total-iva">IVA ${money(r.iva)}</span>` : ""}
          </div>
          ${deducibleHtml}
        </div>
      </div>

      <div class="ticket-table-wrap hidden" id="db-table-${i}" data-tidx="${i}" data-tmod="db">
        <div class="ticket-tabs">
          <button class="ticket-tab active" onclick="showDbTab(${i},'transcripcion',this)">Transcripción</button>
          <button class="ticket-tab" onclick="showDbTab(${i},'resumen',this)">Resumen</button>
          <button class="ticket-tab" onclick="showDbTab(${i},'cruce',this)">Cruce bancario</button>
        </div>
        <div id="db-tab-transcripcion-${i}" class="ticket-tab-content">
          ${buildDashboardProductTable(ticket.productos || [])}
          <button class="btn-add-row" onclick="addDashboardProductRow(${i})">＋ Agregar producto</button>
        </div>
        <div id="db-tab-resumen-${i}" class="ticket-tab-content hidden">${buildResumenTable(r)}</div>
        <div id="db-tab-cruce-${i}" class="ticket-tab-content hidden">${buildDashboardCruceTable(ticket)}</div>
        <div class="table-actions-bar hidden" id="tact-db-${i}">
          <button class="btn-clasificar-ticket" style="padding:10px 28px;font-size:13px" onclick="saveDbTableChanges(${i})">💾 Guardar cambios</button>
          <button class="btn-limpiar-ticket" onclick="resetDbTableChanges(${i})">Limpiar</button>
        </div>
      </div>

      ${tabHtml}
      ${buildClassifyPanel(ci, str("fecha"), deptOptions,
          isClasif ? "💾 Guardar cambios" : "✓ Clasificar",
          `saveDbClassification(${i})`,
          `limpiarDbClassificacion(${i})`)}
    </div>`;
}

function buildDashboardProductTable(productos) {
  if (!productos?.length)
    return `<div class="empty-state"><div class="empty-icon">🧾</div><p>Sin productos registrados</p></div>`;
  const cols = [
    { key: "descripcion",     label: "Descripción" },
    { key: "cantidad",        label: "Cant." },
    { key: "precio_unitario", label: "P.Unit." },
    { key: "monto",           label: "Monto" },
  ];
  return `<table>
    <thead><tr>${cols.map(c => `<th>${c.label}</th>`).join("")}<th></th></tr></thead>
    <tbody>${productos.map((p, rowIdx) =>
      `<tr data-row="${rowIdx}">${cols.map(c =>
        `<td contenteditable="true" spellcheck="false" data-field="${c.key}" oninput="onTableCellInput(this)">${esc(String(p[c.key] ?? ""))}</td>`
      ).join("")}<td class="btn-del-cell"><button class="btn-del-row" onclick="deleteProductRow(this)" title="Eliminar fila">✕</button></td></tr>`
    ).join("")}</tbody>
  </table>`;
}

function toggleDbTable(i) {
  const wrap   = document.getElementById(`db-table-${i}`);
  const header = document.getElementById(`db-header-${i}`);
  const hidden = wrap.classList.toggle("hidden");
  header.classList.toggle("collapsed", hidden);
}

function showDbTab(i, tab, btn) {
  ["transcripcion", "resumen", "cruce"].forEach(t => {
    document.getElementById(`db-tab-${t}-${i}`)?.classList.toggle("hidden", t !== tab);
  });
  btn.closest(".ticket-tabs").querySelectorAll(".ticket-tab")
    .forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
}

// ─── Dashboard: panel clasificar ──────────────────────────────────────────

/** Sincroniza el toggle Deducible del encabezado con el panel de Clasificar */
async function syncDbDeducible(i, checked) {
  const ci     = DB_IDX + i;
  const ticket = dashboardFiltered[i];
  if (!ticket) return;

  // 1. Actualizar UI inmediatamente (optimista)
  const inner = document.getElementById(`deducible-${ci}`);
  if (inner) { inner.checked = checked; updateDeducibleLabel(ci, checked); }
  const lbl = document.getElementById(`db-fhl-${i}`);
  if (lbl) { lbl.textContent = checked ? "Deducible" : "No deducible"; lbl.classList.toggle("on", checked); }

  // 2. Actualizar en memoria
  const prevDeducible = ticket.resumen.deducible;
  ticket.resumen.deducible = checked ? "Sí" : "No";
  const orig = dashboardTickets.find(t => t.ticket_id === ticket.ticket_id);
  if (orig) orig.resumen.deducible = ticket.resumen.deducible;

  // 3. Guardar en Sheets inmediatamente usando clasificación existente en memoria
  const r = ticket.resumen;
  const clasificacion = {
    fecha:              r.fecha              || "",
    cuenta:             r.cuenta             || "",
    subcuenta:          r.subcuenta          || "",
    categoria_gasto:    r.categoria_gasto    || "",
    concepto:           r.concepto           || "",
    propiedad:          r.propiedad          || "",
    departamento:       r.departamento       || "",
    comprador:          r.comprador          || "",
    deducible:          checked ? "Sí" : "No",
    reembolso:          r.reembolso          || "No",
    reembolso_a:        r.reembolso_a        || "",
    metodo_pago:        r.metodo_pago        || "",
    detalles_operacion: r.detalles_operacion || "",
    comentarios:        r.comentarios        || "",
    clasificado_por:    currentUser          || r.clasificado_por || "",
    tienda:             r.tienda             || "",
    rfc:                r.rfc                || "",
    hora:               r.hora               || "",
    folio:              r.folio              || "",
    tarjeta_ultimos4:   r.tarjeta_ultimos4   || "",
    subtotal:           r.subtotal  ?? 0,
    iva:                r.iva       ?? 0,
    ieps:               r.ieps      ?? 0,
    descuentos:         r.descuentos ?? 0,
    total:              r.total     ?? 0,
  };

  try {
    const res  = await fetch(`${BACKEND}/update-ticket`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ ticket_id: ticket.ticket_id, clasificacion, productos_editados: [] }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Error");
  } catch (err) {
    // Revertir en caso de error
    ticket.resumen.deducible = prevDeducible;
    if (orig) orig.resumen.deducible = prevDeducible;
    const prevChecked = prevDeducible === "Sí";
    const headerToggle = document.getElementById(`db-deducible-header-${i}`);
    if (headerToggle) headerToggle.checked = prevChecked;
    if (inner) { inner.checked = prevChecked; updateDeducibleLabel(ci, prevChecked); }
    if (lbl) { lbl.textContent = prevChecked ? "Deducible" : "No deducible"; lbl.classList.toggle("on", prevChecked); }
    console.error("deducible_save_error", err.message);
  }
}

/** Elimina un ticket del dashboard y de Google Sheets */
async function deleteDbTicket(i) {
  const ticket = dashboardFiltered[i];
  if (!ticket) return;
  const tienda = ticket.resumen?.tienda || `Ticket ${i + 1}`;
  const confirmed = await showConfirm({
    icon:    "🗑",
    title:   `¿Eliminar "${tienda}"?`,
    msg:     "Esta acción borrará el ticket de Sheets (Transcripción, Resumen y Cruce bancario). No se puede deshacer.",
    okLabel: "Sí, eliminar",
  });
  if (!confirmed) return;
  try {
    showLoading("Eliminando ticket…", "Borrando registros de Sheets…");
    const res  = await fetch(`${BACKEND}/delete-ticket`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ ticket_id: ticket.ticket_id }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Error al eliminar");
    // Quitar de ambos arrays en memoria
    const tid = ticket.ticket_id;
    dashboardTickets  = dashboardTickets.filter(t => t.ticket_id !== tid);
    dashboardFiltered = dashboardFiltered.filter(t => t.ticket_id !== tid);
    renderDashboardCards();
  } catch (err) {
    alert("Error al eliminar: " + err.message);
  } finally {
    hideLoading();
  }
}

function toggleDbClassify(i) {
  const panel = document.getElementById(`classify-${DB_IDX + i}`);
  const tab   = document.getElementById(`db-btn-classify-${i}`);
  if (!panel) return;
  const isHidden = panel.classList.toggle("hidden");
  tab?.classList.toggle("open", !isHidden);
  if (!isHidden) autoPopulateDbClassify(DB_IDX + i, dashboardFiltered[i]);
}

/** Rellena el panel de clasificar del dashboard con los datos guardados */
function autoPopulateDbClassify(ci, ticket) {
  if (!ticket) return;
  classifyAutoPopulating = true;
  const r  = ticket.resumen || {};
  const s  = k => String(r[k] || "");

  // Fecha
  const fEl = document.getElementById(`fecha-clasif-${ci}`);
  if (fEl) fEl.value = s("fecha");

  // Cuenta → subcuenta → categoría → concepto (cascade)
  const cuentaVal = s("cuenta");
  if (cuentaVal) {
    const grid = document.getElementById(`cuenta-grid-${ci}`);
    const card = Array.from(grid?.querySelectorAll(".cuenta-card") || [])
      .find(c => c.dataset.value === cuentaVal);
    if (card) {
      selectCuenta(card, ci);
      const subVal = s("subcuenta");
      if (subVal) {
        const subGrid = document.getElementById(`subcuenta-grid-${ci}`);
        const subCard = Array.from(subGrid?.querySelectorAll(".cuenta-card") || [])
          .find(c => c.dataset.value === subVal);
        if (subCard) {
          selectSubcuenta(subCard, ci);
          const catVal = s("categoria_gasto");
          if (catVal) {
            const catGrid = document.getElementById(`categoria-grid-${ci}`);
            const catCard = Array.from(catGrid?.querySelectorAll(".cuenta-card") || [])
              .find(c => c.dataset.value === catVal);
            if (catCard) {
              selectCategoria(catCard, ci);
              const concVal = s("concepto");
              if (concVal) {
                const concGrid = document.getElementById(`concepto-grid-${ci}`);
                const concCard = Array.from(concGrid?.querySelectorAll(".cuenta-card") || [])
                  .find(c => c.dataset.value === concVal);
                if (concCard) selectConcepto(concCard, ci);
              }
            }
          }
        }
      }
    }
  }

  // Propiedad
  const propSel = document.getElementById(`propiedad-${ci}`);
  if (propSel) {
    const pv = s("propiedad");
    const exists = Array.from(propSel.options).some(o => o.value === pv);
    if (exists && pv) { propSel.value = pv; }
    else if (pv) {
      propSel.value = "Otro"; togglePropiedadOtro(ci, "Otro");
      const oEl = document.getElementById(`propiedad-otro-${ci}`); if (oEl) oEl.value = pv;
    }
  }

  // Departamento
  const dEl = document.getElementById(`departamento-${ci}`);
  if (dEl) dEl.value = s("departamento");

  // Comprador
  const compVal = s("comprador");
  if (compVal) {
    const compGrid = document.getElementById(`comprador-grid-${ci}`);
    const compCard = Array.from(compGrid?.querySelectorAll(".cuenta-card") || [])
      .find(c => c.dataset.value === compVal);
    if (compCard) selectComprador(compCard, ci);
  }

  // Deducible
  const dedEl = document.getElementById(`deducible-${ci}`);
  const dedChecked = s("deducible") === "Sí";
  if (dedEl) { dedEl.checked = dedChecked; updateDeducibleLabel(ci, dedChecked); }
  // Sincronizar toggle del encabezado
  const dbI = ci - DB_IDX;
  const headerToggle = document.getElementById(`db-deducible-header-${dbI}`);
  if (headerToggle) {
    headerToggle.checked = dedChecked;
    const hLbl = document.getElementById(`db-fhl-${dbI}`);
    if (hLbl) { hLbl.textContent = dedChecked ? "Deducible" : "No deducible"; hLbl.classList.toggle("on", dedChecked); }
  }

  // Reembolso
  if (s("reembolso") === "Sí") {
    const reemEl = document.getElementById(`reembolso-${ci}`);
    if (reemEl) { reemEl.checked = true; toggleReembolso(ci, true); }
    const reemSel = document.getElementById(`reembolso-a-${ci}`);
    if (reemSel) {
      const rv = s("reembolso_a");
      const exists = Array.from(reemSel.options).some(o => o.value === rv);
      if (exists && rv) { reemSel.value = rv; }
      else if (rv) {
        reemSel.value = "Otro"; toggleReembolsoOtro(ci, "Otro");
        const oEl = document.getElementById(`reembolso-otro-${ci}`); if (oEl) oEl.value = rv;
      }
    }
  }

  // Método de pago
  autoSelectMetodoPago(ci, s("metodo_pago_clasif") || s("metodo_pago"));

  // Comentarios
  const comEl = document.getElementById(`comentarios-${ci}`);
  if (comEl) comEl.value = s("comentarios");

  updateClasiPath(ci);
  classifyAutoPopulating = false;
}

function limpiarDbClassificacion(i) {
  const ci = DB_IDX + i;
  // Buscador y cuenta
  const search = document.getElementById(`search-${ci}`); if (search) search.value = "";
  hideSearchResults(ci);
  const grid = document.getElementById(`cuenta-grid-${ci}`);
  if (grid) {
    grid.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
    const sin = grid.querySelector('[data-value=""]'); if (sin) sin.classList.add("active");
  }
  const cuentaH = document.getElementById(`cuenta-${ci}`); if (cuentaH) cuentaH.value = "";
  resetSubcuenta(ci); updateClasiPath(ci);
  // Propiedad / Depto
  const p = document.getElementById(`propiedad-${ci}`); if (p) p.value = "";
  const d = document.getElementById(`departamento-${ci}`); if (d) d.value = "";
  // Comprador
  document.getElementById(`comprador-grid-${ci}`)
    ?.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  const comp = document.getElementById(`comprador-${ci}`); if (comp) comp.value = "";
  // Deducible
  const ded = document.getElementById(`deducible-${ci}`);
  if (ded && ded.checked) { ded.checked = false; updateDeducibleLabel(ci, false); }
  // Reembolso
  const reem = document.getElementById(`reembolso-${ci}`);
  if (reem && reem.checked) { reem.checked = false; toggleReembolso(ci, false); }
  // Método de pago
  document.getElementById(`metodo-grid-${ci}`)
    ?.querySelectorAll(".cuenta-card").forEach(c => c.classList.remove("active"));
  const metodo = document.getElementById(`metodo-clasif-${ci}`); if (metodo) metodo.value = "";
  // Textos
  ["detalles", "comentarios"].forEach(id => {
    const el = document.getElementById(`${id}-${ci}`); if (el) el.value = "";
  });
  // Restaurar fecha detectada
  const fechaEl = document.getElementById(`fecha-clasif-${ci}`);
  if (fechaEl) fechaEl.value = dashboardFiltered[i]?.resumen?.fecha || "";
  hideClassifyActions(ci);
}

async function saveDbClassification(i) {
  const ci     = DB_IDX + i;
  const ticket = dashboardFiltered[i];
  if (!ticket) return;
  const c = getClassify(ci);

  // Leer ediciones del usuario en tablas del dashboard
  const resEdits = readResumenEditsFromDOM(`#db-tab-resumen-${i}`);

  const clasificacion = {
    fecha:              c.fecha,
    cuenta:             c.cuenta,
    subcuenta:          c.subcuenta,
    categoria_gasto:    c.categoria,
    concepto:           c.concepto,
    propiedad:          c.propiedad,
    departamento:       c.departamento,
    comprador:          c.comprador,
    deducible:          c.deducible  ? "Sí" : "No",
    reembolso:          c.reembolso  ? "Sí" : "No",
    reembolso_a:        c.reembolso_a,
    metodo_pago_clasif: c.metodo_pago_clasif,
    metodo_pago:        c.metodo_pago_clasif || ticket.resumen.metodo_pago || "",
    detalles_operacion: c.detalles_operacion,
    comentarios:        c.comentarios,
    clasificado_por:    currentUser,
    // Campos de resumen editables por el usuario
    tienda:           resEdits.tienda           ?? ticket.resumen.tienda           ?? "",
    rfc:              resEdits.rfc              ?? ticket.resumen.rfc              ?? "",
    hora:             resEdits.hora             ?? ticket.resumen.hora             ?? "",
    folio:            resEdits.folio            ?? ticket.resumen.folio            ?? "",
    tarjeta_ultimos4: resEdits.tarjeta_ultimos4 ?? ticket.resumen.tarjeta_ultimos4 ?? "",
    subtotal:  numEdit(resEdits.subtotal,  ticket.resumen.subtotal  ?? 0),
    iva:       numEdit(resEdits.iva,       ticket.resumen.iva       ?? 0),
    ieps:      numEdit(resEdits.ieps,      ticket.resumen.ieps      ?? 0),
    descuentos:numEdit(resEdits.descuentos,ticket.resumen.descuentos ?? 0),
    total:     numEdit(resEdits.total,     ticket.resumen.total     ?? 0),
  };

  // Productos: estado final desde DOM (incluye nuevos y elimina borrados)
  const productosEditados = readAllProductsFromDOM(
    `#db-tab-transcripcion-${i}`, ticket.productos, ticket.ticket_id
  ).map(p => ({
    linea_numero:    p.linea_numero,
    descripcion:     p.descripcion,
    cantidad:        p.cantidad,
    precio_unitario: p.precio_unitario,
    monto:           p.monto,
  }));

  try {
    showLoading("Guardando clasificación…", "Actualizando registro en Sheets…");
    const res  = await fetch(`${BACKEND}/update-ticket`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ ticket_id: ticket.ticket_id, clasificacion, productos_editados: productosEditados }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Error al guardar");

    // Actualizar en memoria
    Object.assign(ticket.resumen, clasificacion);
    const orig = dashboardTickets.find(t => t.ticket_id === ticket.ticket_id);
    if (orig) Object.assign(orig.resumen, clasificacion);

    hideClassifyActions(DB_IDX + i);
    // Re-renderizar para reflejar nuevo estado (sort + chips + pestaña)
    renderDashboardCards();
  } catch (err) {
    alert("Error al guardar: " + err.message);
  } finally {
    hideLoading();
  }
}

// ════════════════════════════════════════════════════════════════════════════
//  MÓDULO: Información de huéspedes  (proxy al check-in: list_records)
// ════════════════════════════════════════════════════════════════════════════

const HU_STATE = {
  loaded: false,
  loading: false,
  filterOptions: null,
  rows: [],            // todas las filas devueltas (page_size=10000)
  filteredRows: [],    // tras aplicar filtro mes client-side
  page: 1,
  pageSize: 25,
  view: 'lista',       // 'lista' | 'calendario'
  // Counters server-side
  serverTotal: 0,
  totalConFactura: 0,
  totalSinFactura: 0,
  totalMediosUnicos: 0,
};

const HU_FILTERS = {
  nombre_reservacion: '',
  medio_reservacion:  '',
  celular_principal:  '',
  requiere_factura:   '',
  razon_social:       '',
  forma_pago:         '',
  correo:             '',
};

/** Auto-selecciona el mes en curso si está vacío y carga datos. */
async function huespedesLoad(forceRefetch) {
  const empty = document.getElementById('hu-empty');
  const wrap  = document.getElementById('hu-table-wrap');
  const lbl   = document.getElementById('hu-status-label');

  // El populate del select y la auto-selección del mes actual ocurren después
  // de cargar las filas (ver más abajo, justo antes de huespedesRender).

  if (!HU_STATE.filterOptions) await huespedesLoadFilterOptions();

  HU_STATE.loading = true;
  if (empty) { empty.classList.remove('hidden'); empty.textContent = 'Cargando reservaciones…'; }
  wrap?.classList.add('hidden');
  if (lbl) lbl.textContent = 'Cargando…';

  try {
    const qs = new URLSearchParams({
      page: '1',
      page_size: '10000',
      nombre_reservacion: HU_FILTERS.nombre_reservacion,
      medio_reservacion:  HU_FILTERS.medio_reservacion,
      celular_principal:  HU_FILTERS.celular_principal,
      requiere_factura:   HU_FILTERS.requiere_factura,
      razon_social:       HU_FILTERS.razon_social,
      forma_pago:         HU_FILTERS.forma_pago,
      correo:             HU_FILTERS.correo,
    });
    const res = await fetch(`${BACKEND}/huespedes-list?${qs.toString()}`, { cache: 'no-store' });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || data.message || 'Error al obtener registros');
    HU_STATE.rows = data.rows || [];
    HU_STATE.serverTotal = data.total || 0;
    HU_STATE.totalConFactura = data.total_con_factura || 0;
    HU_STATE.totalSinFactura = data.total_sin_factura || 0;
    HU_STATE.totalMediosUnicos = data.total_medios_unicos || 0;
    HU_STATE.loaded = true;
    if (lbl) lbl.textContent = `${data.total || 0} reservaciones`;
    huPopulateMesOptions();
    huespedesRender();
  } catch (e) {
    if (lbl) lbl.textContent = 'Error: ' + e.message;
    if (empty) { empty.textContent = '⚠ ' + e.message; empty.classList.remove('hidden'); }
  } finally {
    HU_STATE.loading = false;
  }
}

/** Trae los valores únicos para llenar los selects de filtros. */
async function huespedesLoadFilterOptions() {
  try {
    const res = await fetch(`${BACKEND}/huespedes-filter-options`, { cache: 'no-store' });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || 'Error filter options');
    HU_STATE.filterOptions = data.options || {};
    huespedesBuildFilters();
  } catch (e) {
    console.warn('Error filter options:', e.message);
  }
}

/** Render de los selects de filtros. */
function huespedesBuildFilters() {
  const cont = document.getElementById('hu-filters');
  if (!cont) return;
  const opts = HU_STATE.filterOptions || {};
  const mkSelect = (key, label, list) => {
    const items = (list || []).map(v => `<option value="${esc(v)}" ${HU_FILTERS[key]===v?'selected':''}>${esc(v)}</option>`).join('');
    return `<div>
      <label style="display:block;font-size:11px;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px">${label}</label>
      <select onchange="HU_FILTERS['${key}']=this.value;huespedesLoad(true)"
              style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;background:#fff">
        <option value="">Todos</option>${items}
      </select>
    </div>`;
  };
  const facturaSelect = `<div>
    <label style="display:block;font-size:11px;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px">¿Requiere factura?</label>
    <select onchange="HU_FILTERS.requiere_factura=this.value;huespedesLoad(true)"
            style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;background:#fff">
      <option value="">Todos</option>
      <option value="Sí" ${HU_FILTERS.requiere_factura==='Sí'?'selected':''}>Sí</option>
      <option value="No" ${HU_FILTERS.requiere_factura==='No'?'selected':''}>No</option>
    </select></div>`;
  cont.innerHTML =
    mkSelect('nombre_reservacion', 'Nombre reservación', opts.nombres_reservacion) +
    mkSelect('medio_reservacion',  'Medio de reservación', opts.medios_reservacion) +
    mkSelect('celular_principal',  'Cel/Whatsapp', opts.celulares_principales) +
    facturaSelect +
    mkSelect('razon_social',       'Razón social', opts.razones_sociales) +
    mkSelect('forma_pago',         'Forma de pago', opts.formas_pago) +
    mkSelect('correo',             'Correo electrónico', opts.correos);
}

function huespedesClearFilters() {
  Object.keys(HU_FILTERS).forEach(k => HU_FILTERS[k] = '');
  const mesEl = document.getElementById('hu-filtro-mes'); if (mesEl) mesEl.value = '';
  huespedesBuildFilters();
  huespedesLoad(true);
}

/** Parser flexible de fecha (acepta YYYY-MM-DD, ISO, DD/MM/YYYY). */
function huParseDate(v) {
  if (!v) return null;
  const s = String(v).trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return new Date(s.slice(0, 10) + 'T00:00:00');
  const ddmm = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (ddmm) return new Date(`${ddmm[3]}-${String(ddmm[2]).padStart(2,'0')}-${String(ddmm[1]).padStart(2,'0')}T00:00:00`);
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

/** Asigna una insignia (tier) al huésped según el score de lealtad 0-100.
 *  Umbrales:
 *    Oro       ≥ 70
 *    Plata     ≥ 45
 *    Bronce    ≥ 25
 *    Recurrente≥ 10
 *    null      < 10
 *  Devuelve { label, icon, bg, fg, border, shadow, tooltip, score } o null. */
function huGuestTier(score, stats) {
  const s = Math.round(Number(score)||0);
  const tip = stats
    ? `Score ${s}/100 · ${stats.totalNoches} noches · ${stats.visitas} visitas · ${huFmtMonto(stats.montoGlobal)}`
    : `Score ${s}/100`;
  if (s >= 70) {
    return { score:s, label:'Oro', icon:'🏆',
      bg:'linear-gradient(135deg,#fef3c7,#fde68a 60%,#facc15)', fg:'#78350f',
      border:'#f59e0b', shadow:'rgba(234,179,8,.5)', tooltip:tip };
  }
  if (s >= 45) {
    return { score:s, label:'Plata', icon:'🥈',
      bg:'linear-gradient(135deg,#f1f5f9,#e2e8f0 60%,#cbd5e1)', fg:'#334155',
      border:'#94a3b8', shadow:'rgba(100,116,139,.4)', tooltip:tip };
  }
  if (s >= 25) {
    return { score:s, label:'Bronce', icon:'🥉',
      bg:'linear-gradient(135deg,#fed7aa,#fdba74 60%,#f97316)', fg:'#7c2d12',
      border:'#ea580c', shadow:'rgba(234,88,12,.4)', tooltip:tip };
  }
  if (s >= 10) {
    return { score:s, label:'Recurrente', icon:'⭐',
      bg:'linear-gradient(135deg,#e0e7ff,#c7d2fe 60%,#a5b4fc)', fg:'#3730a3',
      border:'#6366f1', shadow:'rgba(99,102,241,.4)', tooltip:tip };
  }
  return null;
}

/** Estado semaforizado de la estancia (4 estados):
 *  - "concluida"  → salida < hoy           (gris)
 *  - "activa"     → en curso, NO sale hoy  (verde)
 *  - "salida_hoy" → la salida es hoy       (rojo)
 *  - "proxima"    → ingreso es mañana+     (amarillo)
 *  - ""           → sin fechas */
function huGetStayState(ingreso, salida) {
  const di = huParseDate(ingreso); const ds = huParseDate(salida);
  if (!di && !ds) return '';
  const today = new Date(); today.setHours(0,0,0,0);
  const start = di || ds;
  const end   = ds || di;
  // Comparamos por día (sin hora)
  const startDay = new Date(start); startDay.setHours(0,0,0,0);
  const endDay   = new Date(end);   endDay.setHours(0,0,0,0);
  if (endDay < today) return 'concluida';
  if (endDay.getTime() === today.getTime()) return 'salida_hoy';
  if (startDay > today) return 'proxima';
  return 'activa';
}
/** Mapa estado → color + leyenda. */
const HU_STAY_DOT = {
  concluida:  { color:'#94a3b8', label:'Concluida' },
  activa:     { color:'#16a34a', label:'Activa' },
  salida_hoy: { color:'#dc2626', label:'Salida hoy' },
  proxima:    { color:'#f59e0b', label:'Próxima' },
  '':         { color:'#cbd5e1', label:'—' },
};

/** Llena el <select id="hu-filtro-mes"> con los meses únicos presentes en
 *  HU_STATE.rows. Texto legible: "Mayo 2026". Valor: "2026-05".
 *  Si el usuario aún no seleccionó nada, auto-selecciona el mes actual si
 *  existe entre las opciones; si no, deja "Todos los meses". */
function huPopulateMesOptions() {
  const sel = document.getElementById('hu-filtro-mes');
  if (!sel) return;
  const MES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  const set = new Set();
  (HU_STATE.rows || []).forEach(r => {
    const d = huParseDate(huValueFlexible(r, ['Fecha de ingreso']));
    if (d && !isNaN(d.getTime())) set.add(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`);
  });
  const sorted = Array.from(set).sort().reverse(); // más reciente primero
  const prevVal = sel.value;
  const opts = ['<option value="">Todos los meses</option>'].concat(
    sorted.map(v => {
      const [y, m] = v.split('-');
      return `<option value="${v}">${MES[Number(m)-1]} ${y}</option>`;
    })
  );
  sel.innerHTML = opts.join('');
  if (prevVal && sorted.includes(prevVal)) {
    sel.value = prevVal;
  } else if (!prevVal) {
    const now = new Date();
    const cur = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    if (sorted.includes(cur)) sel.value = cur;
  }
}

/** Aplica filtro Mes (estancia toca al menos un día del mes seleccionado). */
function huApplyMonthFilter(rows) {
  const mesEl = document.getElementById('hu-filtro-mes');
  const mesVal = mesEl?.value || '';
  if (!mesVal) return rows;
  const m = mesVal.match(/^(\d{4})-(\d{2})/);
  if (!m) return rows;
  const startDate = new Date(`${m[1]}-${m[2]}-01T00:00:00`);
  const endDate   = new Date(startDate.getFullYear(), startDate.getMonth()+1, 0, 23, 59, 59, 999);
  return rows.filter(r => {
    const entrada = huParseDate(r['Fecha de ingreso']);
    const salida  = huParseDate(r['Fecha de salida']);
    if (!entrada && !salida) return false;
    const stayStart = entrada || salida;
    const stayEnd   = salida  || entrada;
    return stayStart <= endDate && stayEnd >= startDate;
  });
}

/** Formatea una hora a "HH:MM" 24h limpio. Acepta "9:00 a. m.", "21:30", Date, etc. */
function huFmtHoraSimple(v) {
  const raw = String(v || '').trim();
  if (!raw) return '';
  // Si ya es HH:MM 24h
  const m24 = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (m24) return `${String(m24[1]).padStart(2,'0')}:${m24[2]}`;
  // Si trae am/pm
  const m12 = raw.match(/(\d{1,2}):?(\d{2})?\s*(a\.?\s*m\.?|p\.?\s*m\.?|am|pm)/i);
  if (m12) {
    let h = Number(m12[1]); const mm = m12[2] || '00';
    const pm = /p/i.test(m12[3]);
    if (pm && h < 12) h += 12;
    if (!pm && h === 12) h = 0;
    return `${String(h).padStart(2,'0')}:${mm}`;
  }
  // Fallback: si Date parseable
  const d = new Date(raw);
  if (!isNaN(d.getTime())) {
    return `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
  }
  return raw;
}

// ─── Constantes y helpers de cálculo Airbnb (portados de check-in) ──────────
const HU_AIRBNB_COMISION = 0.155;
function huParseMontoAirbnb(value) {
  const clean = String(value || '').replace(/,/g, '').replace(/[^0-9.\-]/g, '');
  const n = Number(clean);
  return Number.isFinite(n) ? n : 0;
}
function huCalcComisionAirbnb(total) {
  return Math.max(0, huParseMontoAirbnb(total) * HU_AIRBNB_COMISION);
}
function huCalcMontoFacturadoAirbnb(total) {
  const t = huParseMontoAirbnb(total);
  return Math.max(0, t - huCalcComisionAirbnb(t));
}
/** Recalcula y refleja en los inputs hijos + en el header del card. */
function huRecalcAirbnb(inputAirbnb) {
  const box = inputAirbnb.closest('[data-hu-airbnb-box="1"]'); if (!box) return;
  const comInp = box.querySelector('[data-hu-airbnb-comision="1"]');
  const facInp = box.querySelector('[data-hu-airbnb-facturado="1"]');
  const com = huCalcComisionAirbnb(inputAirbnb.value);
  const fac = huCalcMontoFacturadoAirbnb(inputAirbnb.value);
  if (comInp) comInp.value = com ? com.toFixed(2) : '';
  if (facInp) facInp.value = fac ? fac.toFixed(2) : '';
  const card = inputAirbnb.closest('.hu-record');
  const hdrAmt = card?.querySelector('[data-hu-header-amount="1"]');
  if (hdrAmt) hdrAmt.textContent = fac ? huFmtMonto(fac) : '—';
  // Persistir (debounced) en cuanto el valor cambia.
  huMaybePersistCardMonto(card, fac ? fac.toFixed(2) : '');
}

/** Sincroniza el campo (+) Monto facturado Total del card con la columna
 *  "$ Monto facturado Total" del sheet. Sin debounce, sin skips: si el
 *  input tiene un valor numérico > 0, lo guarda. */
async function huPersistCardMonto(cardEl) {
  if (!cardEl) { console.warn('[HU] persist: no card'); return; }
  // Si el cardEl es un wrapper de reservación seleccionada (3-col layout),
  // usa su record-id en lugar del de la details outer.
  const recordId = cardEl.dataset.huResvId || cardEl.dataset.recordId || '';
  if (!recordId) { console.warn('[HU] persist: no recordId'); return; }
  const facInp = cardEl.querySelector('[data-hu-airbnb-facturado="1"]');
  if (!facInp) { console.warn('[HU] persist: no input facturado en card', recordId); return; }
  const monto = String(facInp.value || '').trim();
  if (!monto || isNaN(Number(monto)) || Number(monto) <= 0) {
    console.info('[HU] persist: monto inválido o cero', { recordId, monto });
    return;
  }
  // Detectar si es Airbnb para mandar también comisión y total Airbnb.
  const r = (HU_STATE.rows||[]).find(x => String(x['ID']||x['row_number']||'') === String(recordId));
  const medio = r ? huValueFlexible(r, ['Medio de reservación','Medio de reservacion']) : '';
  const esAirbnb = String(medio||'').toLowerCase().includes('airbnb');
  let comisionAirbnb = '';
  let totalAirbnb    = '';
  if (esAirbnb) {
    const comInp = cardEl.querySelector('[data-hu-airbnb-comision="1"]');
    const airBox = cardEl.querySelector('[data-hu-airbnb-box="1"] input[type="number"]'); // primer input = total Airbnb
    comisionAirbnb = String(comInp?.value || '').trim();
    totalAirbnb    = String(airBox?.value || '').trim();
  }
  // Indicador visual en el header
  const hdrAmt = cardEl.querySelector('[data-hu-header-amount="1"]');
  if (hdrAmt) hdrAmt.style.color = '#a16207';
  console.info('[HU] POST /huespedes-save-monto', { recordId, monto, comisionAirbnb, totalAirbnb });
  try {
    const res = await fetch(`${BACKEND}/huespedes-save-monto`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        record_id: recordId,
        monto_facturado_total: monto,
        comision_airbnb: comisionAirbnb,
        monto_total_airbnb: totalAirbnb,
      }),
    });
    const json = await res.json();
    console.info('[HU] response:', json);
    if (!json.ok) throw new Error(json.error || json.message || 'save failed');
    // Refleja en cache local para que un render posterior muestre el valor
    const r = (HU_STATE.rows||[]).find(x => String(x['ID']||x['row_number']||'') === String(recordId));
    if (r) r['$ Monto facturado Total'] = monto;
    if (hdrAmt) {
      hdrAmt.style.color = '#16a34a';
      setTimeout(() => { if (hdrAmt) hdrAmt.style.color = '#111827'; }, 1500);
    }
  } catch (e) {
    console.error('[HU] save error:', e);
    if (hdrAmt) {
      hdrAmt.style.color = '#dc2626';
      hdrAmt.title = 'Error: ' + e.message;
      setTimeout(() => { if (hdrAmt) { hdrAmt.style.color = '#111827'; hdrAmt.title = ''; } }, 2500);
    }
  }
}

/** Elimina la reservación del huésped (fila de "Reservaciones"). Usa el
 *  mismo modal showConfirm que las cards de "Tickets capturados". */
window.deleteHuespedRecord = async function(recordId, nombre) {
  if (!recordId) { alert('No se pudo identificar la reservación.'); return; }
  const label = nombre && nombre.trim() ? `"${nombre.trim()}"` : 'esta reservación';
  const confirmed = await showConfirm({
    icon:    "🗑",
    title:   `¿Eliminar ${label}?`,
    msg:     "Esta acción borrará la reservación de la hoja \"Reservaciones\" del sheets. No se puede deshacer.",
    okLabel: "Sí, eliminar",
  });
  if (!confirmed) return;
  try {
    if (typeof showLoading === 'function') showLoading("Eliminando reservación…", "Borrando registro de Sheets…");
    const res = await fetch(`${BACKEND}/huespedes-delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ record_id: recordId }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || data.message || 'Error al eliminar');
    // Quitar de cache local
    HU_STATE.rows = (HU_STATE.rows || []).filter(x => String(x['ID']||x['row_number']||'') !== String(recordId));
    if (typeof huespedesRender === 'function') huespedesRender();
  } catch (err) {
    alert("Error al eliminar: " + err.message);
  } finally {
    if (typeof hideLoading === 'function') hideLoading();
  }
};

/** Llamado al abrir o cerrar el <details>. Si se abrió, dispara el save
 *  y enriquece el row con los campos completos (incluidas URLs de fotos
 *  INE/identificación/vehículo) que NO vienen en /huespedes-list. */
window.huOnDetailsToggle = function(detailsEl) {
  console.info('[HU] toggle', { open: detailsEl?.open, recordId: detailsEl?.dataset?.recordId });
  if (!detailsEl?.open) return;
  setTimeout(() => huPersistCardMonto(detailsEl), 100);
  huEnrichRowAndRerenderIdCard(detailsEl);
};

/** Actualiza in-place los spans del header (summary) con los valores que SÍ
 *  vienen en /huespedes-detail pero NO en /huespedes-list. */
function huUpdateHeaderFromMerged(detailsEl, r) {
  if (!detailsEl || !r) return;
  const horaIng = huValueFlexible(r, ['Hora estimada de llegada','Hora de llegada']);
  const horaSal = huValueFlexible(r, ['Hora estimada de salida','Hora de salida']);
  const hI = detailsEl.querySelector('[data-hu-hdr-horaing]');
  if (hI) hI.textContent = horaIng ? huFmtHoraSimple(horaIng) : '—';
  const hS = detailsEl.querySelector('[data-hu-hdr-horasal]');
  if (hS) hS.textContent = horaSal ? huFmtHoraSimple(horaSal) : '—';
  // Huéspedes y noches sí vienen en list pero re-renderizar por seguridad
  const hu = huValueFlexible(r, ['# Huéspedes']);
  const chip = detailsEl.querySelector('[data-hu-hdr-huespedes]');
  if (chip && hu) {
    chip.innerHTML = `👥 ${esc(hu)} huésped${String(hu)==='1'?'':'es'}`;
    chip.style.cssText = 'display:inline-flex;align-items:center;gap:5px;padding:4px 11px;border-radius:999px;background:#fff;color:#1f2937;font-weight:800;font-size:11px;border:1px solid #e2e8f0;letter-spacing:.02em;box-shadow:0 1px 2px rgba(15,23,42,.05)';
  }
}

/** Trae el detalle completo del registro (incluye links de fotos) y re-renderiza
 *  la columna del ID Card. Idempotente: si ya enriquecimos antes, no repite. */
async function huEnrichRowAndRerenderIdCard(detailsEl) {
  try {
    if (!detailsEl || detailsEl.dataset.huEnriched === '1') return;
    const recordId = detailsEl.dataset.recordId;
    if (!recordId) return;
    const rows = HU_STATE.rows || [];
    const idx = rows.findIndex(r => String(r['ID']||r['row_number']||'') === String(recordId));
    if (idx < 0) return;
    detailsEl.dataset.huEnriched = '1'; // marcar antes de await para evitar dobles
    const res = await fetch(`${BACKEND}/huespedes-detail?record_id=${encodeURIComponent(recordId)}`);
    const json = await res.json();
    if (!json?.ok || !json.record) { detailsEl.dataset.huEnriched = ''; return; }
    // Merge: el detail tiene prioridad para los campos que faltan en list
    const merged = { ...rows[idx], ...json.record };
    HU_STATE.rows[idx] = merged;
    const profileCol = detailsEl.querySelector('.hu-col-profile');
    if (profileCol) profileCol.innerHTML = huBuildIdCard(merged);
    const detailCol = detailsEl.querySelector('.hu-col-detail');
    if (detailCol && typeof huBuildReservationDetail === 'function') {
      detailCol.innerHTML = huBuildReservationDetail(merged);
    }
    // También actualizar los campos del header (Hora estimada llegada/salida,
    // # Huéspedes, # Noches) que NO venían en /huespedes-list.
    huUpdateHeaderFromMerged(detailsEl, merged);
  } catch (e) {
    console.warn('[HU] enrich fail', e);
    if (detailsEl) detailsEl.dataset.huEnriched = '';
  }
}

/** Alias para mantener compat con huRecalcAirbnb (un toque debounce). */
let HU_RECALC_TIMER = null;
function huMaybePersistCardMonto(cardEl) {
  if (HU_RECALC_TIMER) clearTimeout(HU_RECALC_TIMER);
  HU_RECALC_TIMER = setTimeout(() => huPersistCardMonto(cardEl), 600);
}
/** Mensaje canónico para "consultar ticket emitido". */
function huBuildTicketConsultaMsg(url) {
  const clean = String(url||'').trim();
  if (!/^https?:\/\//i.test(clean)) return '';
  return `Hemos enviado el TICKET para AUTO-FACTURACIÓN al correo proporcionado.\n\nURL de la factura:\n${clean}\n\nRecuerda que sólo estará vigente dentro del mes fiscal en curso.`;
}
/** Copia texto al portapapeles (soporta navegadores sin clipboard API). */
async function huCopyTextUniversal(text) {
  const v = String(text||'');
  if (!v) return false;
  if (navigator.clipboard && window.isSecureContext) {
    try { await navigator.clipboard.writeText(v); return true; } catch(_) {}
  }
  const ta = document.createElement('textarea');
  ta.value = v; ta.setAttribute('readonly','');
  ta.style.cssText = 'position:fixed;top:0;left:-9999px;opacity:0';
  document.body.appendChild(ta); ta.focus(); ta.select();
  let ok = false;
  try { ok = document.execCommand('copy'); } catch(_) { ok = false; }
  document.body.removeChild(ta);
  return ok;
}
window.huespedesCopiarMsgConsulta = async function(btn, encoded) {
  const msg = decodeURIComponent(encoded || '');
  const original = btn.textContent;
  const ok = await huCopyTextUniversal(msg);
  btn.textContent = ok ? '✓ Mensaje copiado' : '⚠ No se pudo copiar';
  btn.style.background = ok ? '#16a34a' : '#dc2626';
  setTimeout(() => {
    btn.textContent = original;
    btn.style.background = '#475569';
  }, 1800);
};

// ─── Helpers de status de factura (portados de check-in) ────────────────────
function huValueFlexible(row, candidates) {
  if (!row || typeof row !== 'object') return '';
  const normK = (k) => String(k||'').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g,'').replace(/[^a-z0-9]+/g,' ').trim();
  for (const c of candidates) {
    if (row[c] != null && String(row[c]).trim() !== '') return String(row[c]).trim();
  }
  const entries = Object.entries(row);
  for (const c of candidates) {
    const nc = normK(c);
    const found = entries.find(([k, v]) => normK(k) === nc && v != null && String(v).trim() !== '');
    if (found) return String(found[1]).trim();
  }
  return '';
}
function huIsFacturaYes(value) {
  const t = String(value || '').trim().toLowerCase();
  return t === 'si' || t === 'sí' || t === 'yes';
}
function huGetFacturaStatus(row) {
  const factura = huValueFlexible(row, ['¿Requiere factura?','Requiere factura']).toLowerCase();
  const folio   = huValueFlexible(row, ['Folio facturapi','Folio Facturapi','folio facturapi','Folio']);
  if (folio) return 'emitida';
  if (factura === 'si' || factura === 'sí' || factura === 'yes') return 'pendiente';
  return 'no-requiere';
}
function huExtractTicketUrl(row) {
  if (!row) return '';
  const direct = [row['Ticket facturapi url'], row['ticket facturapi url'], row['Ticket Facturapi Url'], row['ticket_url'], row['url']];
  for (const v of direct) {
    const t = String(v||'').trim();
    if (/^https?:\/\//i.test(t)) return t;
  }
  return '';
}
function huBuildWhatsAppUrl(v) {
  const digits = String(v||'').replace(/[^\d]/g, '');
  if (!digits) return '';
  const n = digits.length === 10 ? '52' + digits : digits;
  return `https://wa.me/${n}`;
}
function huMedioBadge(medio) {
  const m = String(medio||'').trim();
  if (!m) return '';
  const lc = m.toLowerCase();
  // Estilo del badge según medio
  let dotBg, dotColor, pillBg, pillColor, pillBorder, initial;
  if (lc.includes('airbnb')) {
    dotBg = '#be123c'; dotColor = '#fff'; pillBg = '#ffe4e6'; pillColor = '#9f1239'; pillBorder = '#fbcfe8'; initial = 'A';
  } else if (lc.includes('booking')) {
    dotBg = '#1e40af'; dotColor = '#fff'; pillBg = '#dbeafe'; pillColor = '#1e3a8a'; pillBorder = '#bfdbfe'; initial = 'B';
  } else if (lc.includes('directo') || lc.includes('check')) {
    // Ej. "Check-inn-Saltillo.com" → globo 🌐 azul claro
    return `<span style="display:inline-flex;align-items:center;gap:6px;padding:4px 12px 4px 8px;border-radius:999px;background:#dbeafe;color:#1e40af;font-weight:700;font-size:12px;border:1px solid #bfdbfe">
      <span style="font-size:13px">🌐</span><span>${esc(m)}</span>
    </span>`;
  } else {
    dotBg = '#475569'; dotColor = '#fff'; pillBg = '#f1f5f9'; pillColor = '#334155'; pillBorder = '#cbd5e1'; initial = m[0].toUpperCase();
  }
  return `<span style="display:inline-flex;align-items:center;gap:6px;padding:4px 12px 4px 4px;border-radius:999px;background:${pillBg};color:${pillColor};font-weight:700;font-size:12px;border:1px solid ${pillBorder}">
    <span style="display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;border-radius:50%;background:${dotBg};color:${dotColor};font-size:10px;font-weight:900">${initial}</span>
    <span>${esc(m)}</span>
  </span>`;
}
function huFacturaHeaderBadge(row) {
  const status = huGetFacturaStatus(row);
  if (status === 'emitida') {
    const folio = huValueFlexible(row, ['Folio facturapi','Folio Facturapi','Folio']);
    const url   = huExtractTicketUrl(row);
    const label = folio ? `🧾 Ticket emitido — Folio #${esc(folio)}` : '🧾 Ticket emitido';
    const inner = `<span style="display:inline-block;padding:3px 10px;border-radius:999px;font-size:10px;font-weight:700;background:#dcfce7;color:#166534;border:1px solid #86efac">${label}</span>`;
    return url ? `<a href="${esc(url)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="text-decoration:none">${inner}</a>` : inner;
  }
  if (status === 'pendiente') {
    return `<span style="display:inline-block;padding:3px 10px;border-radius:999px;font-size:10px;font-weight:700;background:#fef3c7;color:#92400e;border:1px solid #fcd34d">🧾 Ticket pendiente</span>`;
  }
  return '';
}

/** Render principal del dashboard de huéspedes. */
function huespedesRender() {
  const empty   = document.getElementById('hu-empty');
  const records = document.getElementById('hu-records-wrap');
  const calend  = document.getElementById('hu-calendar-wrap');
  const pager   = document.getElementById('hu-pager');

  // 1) Filtro mes (estancia toca al menos un día del mes)
  HU_STATE.filteredRows = huApplyMonthFilter(HU_STATE.rows);

  // 2) Calcular conteos sobre el universo filtrado
  const allRows  = HU_STATE.filteredRows;
  const conFact  = allRows.filter(r => huIsFacturaYes(huValueFlexible(r, ['¿Requiere factura?','Requiere factura']))).length;
  const sinFact  = allRows.length - conFact;
  const pendiente = allRows.filter(r => huGetFacturaStatus(r) === 'pendiente').length;
  const emitida   = allRows.filter(r => huGetFacturaStatus(r) === 'emitida').length;
  const pctEmit   = conFact > 0 ? Math.round((emitida / conFact) * 100) : 0;
  const pctPend   = conFact > 0 ? Math.round((pendiente / conFact) * 100) : 0;

  // 3) Pintar KPIs simples
  document.getElementById('hu-kpi-total')       && (document.getElementById('hu-kpi-total').textContent = String(allRows.length));
  document.getElementById('hu-kpi-con-factura') && (document.getElementById('hu-kpi-con-factura').textContent = String(conFact));
  document.getElementById('hu-kpi-sin-factura') && (document.getElementById('hu-kpi-sin-factura').textContent = String(sinFact));

  // 4) Pintar tarjeta Estado de tickets
  document.getElementById('hu-tickets-total-requieren') && (document.getElementById('hu-tickets-total-requieren').textContent = `${conFact} tickets requieren factura`);
  document.getElementById('hu-bar-emitido')   && (document.getElementById('hu-bar-emitido').style.width   = `${pctEmit}%`);
  document.getElementById('hu-bar-pendiente') && (document.getElementById('hu-bar-pendiente').style.width = `${pctPend}%`);
  document.getElementById('hu-emitidos-num')  && (document.getElementById('hu-emitidos-num').textContent  = String(emitida));
  document.getElementById('hu-pendientes-num')&& (document.getElementById('hu-pendientes-num').textContent= String(pendiente));
  document.getElementById('hu-pct-emitidos')  && (document.getElementById('hu-pct-emitidos').textContent  = `${pctEmit}%`);
  document.getElementById('hu-pct-pendientes')&& (document.getElementById('hu-pct-pendientes').textContent= `${pctPend}%`);

  // 5) Alertas (rojo pendientes, verde emitidos)
  const alertsEl = document.getElementById('hu-alerts');
  if (alertsEl) {
    if (!conFact) {
      alertsEl.innerHTML = `<div style="padding:10px 14px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;color:#64748b;font-size:13px">No hay registros que requieran factura en esta consulta.</div>`;
    } else {
      const parts = [];
      if (pendiente > 0) {
        parts.push(`<div style="padding:10px 14px;background:#fef2f2;border:1.5px solid #fecaca;border-radius:8px;color:#7f1d1d;font-size:13px;font-weight:600;display:flex;align-items:center;gap:10px">
          <span style="display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:50%;background:#dc2626;color:#fff;font-weight:900;font-size:13px;flex-shrink:0">${pendiente}</span>
          <span>Ticket(s) pendiente(s) requieren atención. Se muestran primero en la lista.</span>
        </div>`);
      }
      if (emitida > 0) {
        parts.push(`<div style="padding:10px 14px;background:#f0fdf4;border:1.5px solid #bbf7d0;border-radius:8px;color:#14532d;font-size:13px;font-weight:600">✅ <strong>${emitida}</strong> ticket(s) emitido(s).</div>`);
      }
      alertsEl.innerHTML = parts.join('');
    }
  }

  // 6) Ordenar: pendientes primero, después emitidas, después no-requiere, dentro por ID desc
  const rankFactura = (r) => {
    const s = huGetFacturaStatus(r);
    return s === 'pendiente' ? 0 : s === 'emitida' ? 1 : 2;
  };
  const sorted = allRows.slice().sort((a, b) => {
    const d = rankFactura(a) - rankFactura(b); if (d !== 0) return d;
    const aId = Number(String(a['ID']||a['row_number']||'').replace(/[^0-9.-]/g,'')) || 0;
    const bId = Number(String(b['ID']||b['row_number']||'').replace(/[^0-9.-]/g,'')) || 0;
    return bId - aId;
  });

  // 7) Vista: Lista o Calendario
  if (HU_STATE.view === 'calendario') {
    records?.classList.add('hidden');
    calend?.classList.remove('hidden');
    empty?.classList.add('hidden');
    if (calend) calend.innerHTML = huRenderCalendar(sorted);
    if (pager) pager.innerHTML = '';
    huUpdateMeta(allRows.length, 0, 0);
    return;
  }
  calend?.classList.add('hidden');

  // 8) Paginación + render de cards expandibles (vista Lista)
  const totalPages = Math.max(1, Math.ceil(sorted.length / HU_STATE.pageSize));
  if (HU_STATE.page > totalPages) HU_STATE.page = totalPages;
  if (HU_STATE.page < 1) HU_STATE.page = 1;
  const start = (HU_STATE.page - 1) * HU_STATE.pageSize;
  const pageRows = sorted.slice(start, start + HU_STATE.pageSize);

  if (!sorted.length) {
    if (empty) {
      empty.textContent = HU_STATE.rows.length === 0
        ? 'Sin reservaciones con los filtros actuales.'
        : 'Sin reservaciones que toquen el mes seleccionado.';
      empty.classList.remove('hidden');
    }
    records?.classList.add('hidden');
    if (pager) pager.innerHTML = '';
    huUpdateMeta(0, 0, 0);
    return;
  }
  empty?.classList.add('hidden');
  records?.classList.remove('hidden');

  records.innerHTML = pageRows.map(r => huBuildRecordCard(r)).join('');

  // 9) Meta + paginador
  huUpdateMeta(sorted.length, start + 1, Math.min(sorted.length, start + pageRows.length));
  if (pager) {
    const psSel = `<select onchange="HU_STATE.pageSize=Number(this.value);HU_STATE.page=1;huespedesRender()"
            style="padding:6px 10px;border:1px solid #cbd5e1;border-radius:6px;font-size:12px">
      ${[10,25,50,100,200].map(n => `<option value="${n}" ${HU_STATE.pageSize===n?'selected':''}>${n}/pág</option>`).join('')}
    </select>`;
    pager.innerHTML = totalPages > 1 ? `
      <button ${HU_STATE.page<=1?'disabled':''} onclick="HU_STATE.page--;huespedesRender()" style="padding:6px 12px;border:1px solid #cbd5e1;background:#fff;border-radius:6px;font-size:12px;cursor:pointer">‹ Anterior</button>
      <span style="padding:6px 12px;font-size:12px;color:#475569;font-weight:600">Página ${HU_STATE.page} de ${totalPages}</span>
      <button ${HU_STATE.page>=totalPages?'disabled':''} onclick="HU_STATE.page++;huespedesRender()" style="padding:6px 12px;border:1px solid #cbd5e1;background:#fff;border-radius:6px;font-size:12px;cursor:pointer">Siguiente ›</button>
      ${psSel}
    ` : psSel;
  }
}

function huUpdateMeta(total, ini, fin) {
  const m = document.getElementById('hu-meta'); if (!m) return;
  if (!total) { m.textContent = '0 registros encontrados.'; return; }
  m.textContent = `${total} registros encontrados según filtros. Mostrando ${ini}-${fin}. Registros que Sí requieren factura siempre primero.`;
}

/** Formatea YYYY-MM-DD a "DD de mes" en español. */
const HU_MESES_ES = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
function huFmtFecha(iso) {
  if (!iso) return '—';
  const m = String(iso).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return iso;
  const d = parseInt(m[3], 10);
  const mes = HU_MESES_ES[parseInt(m[2], 10) - 1] || '';
  return `${d} de ${mes}`;
}

/** Badge "Sí/No" para ¿Requiere factura?. */
function huReqFacturaBadge(v) {
  const t = String(v||'').trim().toLowerCase();
  if (t === 'sí' || t === 'si' || t === 'yes') {
    return `<span style="display:inline-block;padding:5px 18px;border-radius:999px;background:#fce7f3;color:#9f1239;font-weight:700;font-size:13px;border:1px solid #fbcfe8">Sí</span>`;
  }
  if (t === 'no') {
    return `<span style="display:inline-block;padding:5px 18px;border-radius:999px;background:#f1f5f9;color:#475569;font-weight:700;font-size:13px;border:1px solid #cbd5e1">No</span>`;
  }
  return esc(v || '—');
}

/** Formatea un monto crudo a "$ 1,234.56" o "—". */
function huFmtMonto(raw) {
  if (raw == null || String(raw).trim() === '') return '—';
  let s = String(raw).trim();
  // Caso patológico: el Apps Script a veces serializa un número como ISO date
  // (p. ej. "2534-08-01T06:00:00.000Z" en vez de "2534.8"). Si detectamos
  // ese patrón, reconstruimos el número original a partir del año + mes/día.
  // En el ejemplo "2534-08-01" → 2534 año, 08 mes, 01 día → "2534.8".
  const iso = s.match(/^(-?\d+)-(\d{2})-(\d{2})T/);
  if (iso) {
    const ent = iso[1];
    // El "mes" suele ser el decimal real (08 → .8). El "día" suele ser 01 → fin.
    const decMonth = String(parseInt(iso[2], 10)); // "08" → "8"
    s = `${ent}.${decMonth}`;
  }
  // Soporta coma decimal (es-MX): "2534,8" → "2534.8"
  if (/,\d{1,2}$/.test(s) && !/\./.test(s)) s = s.replace(',', '.');
  const n = Number(s.replace(/[^0-9.\-]/g, ''));
  if (!isFinite(n)) return String(raw);
  return '$ ' + n.toLocaleString('es-MX', { minimumFractionDigits: n % 1 === 0 ? 0 : 2, maximumFractionDigits: 2 });
}

/** Card expandible de una reservación (vista Lista) — diseño que coincide con el original. */
// ─── Cálculos de historial (noches totales + nº de visitas) ────────────────
// Una "visita" agrupa reservaciones consecutivas (sin días-hueco entre salida
// y siguiente ingreso). Reservaciones separadas por ≥1 día son visitas distintas.
function huComputeGuestStats(currentRow, allRows) {
  const cel = huValueFlexible(currentRow, ['Cel/Whatsapp (principal)']);
  const list = (allRows || HU_STATE.rows || [])
    .filter(x => huValueFlexible(x, ['Cel/Whatsapp (principal)']) === cel && cel);
  // Parseo + ordenamiento por fecha de ingreso ascendente
  const parsed = list.map(x => {
    const ing = huParseDate(huValueFlexible(x, ['Fecha de ingreso']));
    const sal = huParseDate(huValueFlexible(x, ['Fecha de salida']));
    const noches = Number(huValueFlexible(x, ['# Noches'])) || 0;
    return { ing, sal, noches };
  }).filter(s => s.ing && s.sal).sort((a, b) => a.ing - b.ing);

  let totalNoches = 0;
  let visitas    = 0;
  let lastEnd    = null;
  for (const s of parsed) {
    totalNoches += s.noches > 0 ? s.noches : Math.max(0, Math.round((s.sal - s.ing) / 86400000));
    if (lastEnd === null) {
      visitas = 1;
    } else {
      const diffDays = Math.round((s.ing - lastEnd) / 86400000);
      if (diffDays > 0) visitas += 1;
    }
    if (lastEnd === null || s.sal > lastEnd) lastEnd = s.sal;
  }
  // Monto global = suma de "$ Monto facturado Total" en todas las reservas
  // del mismo Cel. Usa el mismo sanitizador que huFmtMonto para tolerar
  // valores ISO-date corruptos y coma decimal es-MX.
  const montoGlobal = list.reduce((acc, x) => {
    const raw = huValueFlexible(x, ['$ Monto facturado Total','Monto facturado Total']);
    return acc + huParseMontoRobust(raw);
  }, 0);
  return { totalNoches, visitas, reservaciones: parsed.length, montoGlobal };
}

/** Parser numérico tolerante: maneja "2534,8", "$25,348.00",
 *  "2534-08-01T06:00:00.000Z" (corrupción de fecha) y devuelve 0 si no se
 *  puede inferir. Espejo de la lógica de huFmtMonto. */
function huParseMontoRobust(raw) {
  if (raw == null) return 0;
  let s = String(raw).trim();
  if (!s) return 0;
  const iso = s.match(/^(-?\d+)-(\d{2})-(\d{2})T/);
  if (iso) s = `${iso[1]}.${parseInt(iso[2],10)}`;
  if (/,\d{1,2}$/.test(s) && !/\./.test(s)) s = s.replace(',', '.');
  const n = Number(s.replace(/[^0-9.\-]/g, ''));
  return isFinite(n) ? n : 0;
}

// ─── Índice de Lealtad: score 0-100 con ponderaciones editables ──────────
const HU_LOYALTY_DEFAULTS = { w_noches:30, w_visitas:20, w_monto:50 };
// Valores de referencia con los que cada KPI llega a 100 puntos. Calibrados
// a partir de la metodología solicitada (Oro = ≥20 noches y ≥8 visitas):
const HU_LOYALTY_REF = { ref_noches:30, ref_visitas:12, ref_monto:100000 };

function huGetLoyaltyWeights() {
  const wN = Number(document.getElementById('hu-w-noches')?.value);
  const wV = Number(document.getElementById('hu-w-visitas')?.value);
  const wM = Number(document.getElementById('hu-w-monto')?.value);
  const def = HU_LOYALTY_DEFAULTS;
  return {
    w_noches:  isFinite(wN) ? wN : def.w_noches,
    w_visitas: isFinite(wV) ? wV : def.w_visitas,
    w_monto:   isFinite(wM) ? wM : def.w_monto,
  };
}

/** Calcula el score 0-100 (saturado) a partir de los KPIs globales. */
function huComputeLoyaltyScore(stats) {
  const w = huGetLoyaltyWeights();
  const sum = (w.w_noches + w.w_visitas + w.w_monto) || 1;
  const nNoches  = Math.min(1, (Number(stats.totalNoches) || 0) / HU_LOYALTY_REF.ref_noches);
  const nVisitas = Math.min(1, (Number(stats.visitas)     || 0) / HU_LOYALTY_REF.ref_visitas);
  const nMonto   = Math.min(1, (Number(stats.montoGlobal) || 0) / HU_LOYALTY_REF.ref_monto);
  const score = (w.w_noches*nNoches + w.w_visitas*nVisitas + w.w_monto*nMonto) * (100/sum);
  return Math.round(Math.max(0, Math.min(100, score)));
}

/** Reset de ponderaciones a los valores por defecto + re-render. */
window.huResetLoyaltyWeights = function() {
  document.getElementById('hu-w-noches').value  = HU_LOYALTY_DEFAULTS.w_noches;
  document.getElementById('hu-w-visitas').value = HU_LOYALTY_DEFAULTS.w_visitas;
  document.getElementById('hu-w-monto').value   = HU_LOYALTY_DEFAULTS.w_monto;
  huUpdateLoyaltySumNote();
  if (typeof huespedesRender === 'function') huespedesRender();
};
/** Cambio en cualquiera de los inputs de ponderación: re-render debounced. */
let HU_W_TIMER = null;
window.huOnLoyaltyWeightChange = function() {
  huUpdateLoyaltySumNote();
  if (HU_W_TIMER) clearTimeout(HU_W_TIMER);
  HU_W_TIMER = setTimeout(() => {
    if (typeof huespedesRender === 'function') huespedesRender();
  }, 300);
};
function huUpdateLoyaltySumNote() {
  const w = huGetLoyaltyWeights();
  const sum = w.w_noches + w.w_visitas + w.w_monto;
  const note = document.getElementById('hu-w-sum-note');
  if (!note) return;
  const color = sum === 100 ? '#16a34a' : '#dc2626';
  note.innerHTML = `Suma = <b style="color:${color}">${sum}</b> ${sum===100?'✓':'(idealmente 100)'} · Oro ≥70 · Plata ≥45 · Bronce ≥25 · Recurrente ≥10.`;
}

// ─── Lightbox para fotos (Esc + X + clic fuera para cerrar) ─────────────────
function huImageZoom(url, title) {
  if (!url) return;
  let ov = document.getElementById('hu-img-zoom-overlay');
  if (!ov) {
    ov = document.createElement('div');
    ov.id = 'hu-img-zoom-overlay';
    ov.style.cssText = 'position:fixed;inset:0;background:rgba(15,23,42,.92);z-index:10000;display:none;align-items:center;justify-content:center;padding:20px;cursor:zoom-out;animation:hu-modal-back-in 200ms ease-out';
    ov.onclick = (e) => { if (e.target === ov) huImageZoomClose(); };
    document.body.appendChild(ov);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && ov.style.display === 'flex') huImageZoomClose();
    });
  }
  ov.innerHTML = `
    <div style="position:relative;max-width:96vw;max-height:96vh;display:flex;flex-direction:column;align-items:center;animation:hu-modal-card-in 280ms cubic-bezier(.34,1.56,.64,1)" onclick="event.stopPropagation()">
      ${title ? `<div style="color:#fff;font-size:13px;font-weight:600;margin-bottom:10px;text-shadow:0 1px 3px rgba(0,0,0,.5);padding:6px 14px;background:rgba(0,0,0,.45);border-radius:999px">${esc(title)}</div>` : ''}
      <img src="${esc(url)}" alt="${esc(title||'foto')}" referrerpolicy="no-referrer"
           style="max-width:96vw;max-height:88vh;object-fit:contain;border-radius:10px;box-shadow:0 20px 60px rgba(0,0,0,.6);background:#fff" />
      <button onclick="huImageZoomClose()" aria-label="Cerrar"
              style="position:absolute;top:-12px;right:-12px;width:42px;height:42px;border-radius:50%;border:none;background:#fff;color:#1f2937;font-size:20px;font-weight:900;cursor:pointer;box-shadow:0 4px 14px rgba(0,0,0,.5);display:flex;align-items:center;justify-content:center">✕</button>
    </div>`;
  ov.style.display = 'flex';
  document.body.style.overflow = 'hidden';
}
function huImageZoomClose() {
  const ov = document.getElementById('hu-img-zoom-overlay');
  if (ov) { ov.style.display = 'none'; ov.innerHTML = ''; }
  document.body.style.overflow = '';
}

/** Convierte un Drive URL en thumbnail directo (mismo enfoque que check-in
 *  /public/registro, que sí muestra las fotos correctamente). Acepta
 *  /file/d/{ID}/view, /d/{ID}, ?id=ID o ID puro. El size acepta "w1200" o 1200. */
function huDriveThumb(url, size) {
  if (!url) return '';
  const s = String(url).trim();
  const sz = String(size || 'w800').replace(/^w/i, '');
  let id = '';
  let m = s.match(/\/file\/d\/([a-zA-Z0-9_-]+)/);  if (m) id = m[1];
  if (!id) { m = s.match(/\/d\/([a-zA-Z0-9_-]+)/);       if (m) id = m[1]; }
  if (!id) { m = s.match(/[?&]id=([a-zA-Z0-9_-]+)/);     if (m) id = m[1]; }
  if (!id && /^[a-zA-Z0-9_-]{20,}$/.test(s)) id = s;
  if (!id) return s; // último recurso: la url tal cual
  return `https://drive.google.com/thumbnail?id=${id}&sz=w${sz}`;
}

/** Placeholder elegante cuando no hay foto (en lugar de aspect-ratio:1 vacío). */
function huPhotoPlaceholder(label, icon, height) {
  return `
    <div style="width:100%;height:${height||'120px'};border:1.5px dashed #cbd5e1;border-radius:10px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;background:linear-gradient(135deg,#f8fafc,#f1f5f9);color:#94a3b8;padding:10px;text-align:center">
      <div style="font-size:22px;opacity:.5">${icon || '📷'}</div>
      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em">${esc(label)}</div>
      <div style="font-size:9px;opacity:.7;font-style:italic">No hay foto</div>
    </div>`;
}

/** Overlay de "Cargando…" superpuesto en el recuadro mientras el <img> baja. */
function huPhotoLoadingOverlay() {
  return `<div class="hu-photo-loading" style="position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;background:linear-gradient(135deg,#f8fafc,#eef2f7);color:#64748b;pointer-events:none;animation:hu-pulse 1.2s ease-in-out infinite">
    <div style="width:22px;height:22px;border:2.5px solid #cbd5e1;border-top-color:#0d9488;border-radius:50%;animation:hu-spin 0.8s linear infinite"></div>
    <div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.06em">Cargando…</div>
  </div>`;
}
/** Handler global del <img> en ID Card: quita el overlay "Cargando…". Se
 *  expone en window porque los handlers inline no pueden usar querySelector
 *  con comillas dobles sin romper el atributo HTML. */
window.huPhotoImgLoaded = function(img) {
  try {
    const parent = img && img.parentElement;
    if (!parent) return;
    const ov = parent.getElementsByClassName('hu-photo-loading')[0];
    if (ov) ov.remove();
    img.style.opacity = '1';
  } catch (_) {}
};

/** Construye una foto-thumb con click→zoom. Si no hay url, devuelve placeholder. */
function huPhotoBox(url, label, options) {
  const opt = options || {};
  const size = opt.size || 'w400';
  const icon = opt.icon || '📷';
  const aspectRatio = opt.aspect || '1';
  const height = opt.height;
  const thumb = url ? huDriveThumb(url, size) : '';
  const full  = url ? huDriveThumb(url, 'w1600') : '';
  if (!thumb) return huPhotoPlaceholder(label, icon, height || '110px');
  // Fallback chain: thumbnail → lh3 → uc?export=view → placeholder
  let driveId = '';
  const sRaw = String(url).trim();
  let mm = sRaw.match(/\/file\/d\/([a-zA-Z0-9_-]+)/);  if (mm) driveId = mm[1];
  if (!driveId) { mm = sRaw.match(/\/d\/([a-zA-Z0-9_-]+)/);    if (mm) driveId = mm[1]; }
  if (!driveId) { mm = sRaw.match(/[?&]id=([a-zA-Z0-9_-]+)/);  if (mm) driveId = mm[1]; }
  if (!driveId && /^[a-zA-Z0-9_-]{20,}$/.test(sRaw)) driveId = sRaw;
  const fb1 = driveId ? `https://lh3.googleusercontent.com/d/${driveId}=w800` : '';
  const fb2 = driveId ? `https://drive.google.com/uc?export=view&id=${driveId}` : '';
  const sizeStyle = height ? `height:${height}` : `aspect-ratio:${aspectRatio}`;
  const phHtml = huPhotoPlaceholder(label, icon, '100%').replace(/"/g, '&quot;');
  const onerrFn = `(function(img){var t=img.dataset.tryStep||'0';if(t==='0'&&'${esc(fb1)}'){img.dataset.tryStep='1';img.src='${esc(fb1)}';return;}if((t==='0'||t==='1')&&'${esc(fb2)}'){img.dataset.tryStep='2';img.src='${esc(fb2)}';return;}img.parentElement.innerHTML='${phHtml}';img.parentElement.style.cursor='default';img.parentElement.onclick=null;})(this)`;
  return `
    <div style="position:relative;cursor:zoom-in;border-radius:10px;overflow:hidden;border:1.5px solid #e2e8f0;background:#f8fafc;${sizeStyle};transition:transform 180ms cubic-bezier(.34,1.56,.64,1),box-shadow 180ms"
         onmouseover="this.style.transform='translateY(-2px)';this.style.boxShadow='0 8px 24px rgba(15,23,42,.18)'"
         onmouseout="this.style.transform='';this.style.boxShadow=''"
         onclick="event.stopPropagation();huImageZoom('${esc(full)}','${esc(label)}')">
      ${huPhotoLoadingOverlay()}
      <img src="${esc(thumb)}" alt="${esc(label)}" loading="lazy" referrerpolicy="no-referrer"
           style="width:100%;height:100%;object-fit:cover;display:block;background:#f8fafc;opacity:0;transition:opacity 200ms ease-out"
           onload="huPhotoImgLoaded(this)"
           onerror="${onerrFn}">
      <div style="position:absolute;bottom:0;left:0;right:0;padding:6px 8px;background:linear-gradient(180deg,transparent,rgba(0,0,0,.7));color:#fff;font-size:10px;font-weight:700;text-shadow:0 1px 2px rgba(0,0,0,.5);text-transform:uppercase;letter-spacing:.04em">${esc(label)}</div>
      <div style="position:absolute;top:6px;right:6px;width:24px;height:24px;border-radius:50%;background:rgba(15,23,42,.7);color:#fff;display:flex;align-items:center;justify-content:center;font-size:11px">🔍</div>
    </div>`;
}

/** Construye la "ID Card" del perfil del huésped (columna izquierda). Tema claro. */
function huBuildIdCard(r) {
  const nombre   = huValueFlexible(r, ['Nombre del huésped','Nombre de la persona que hizo la reservación']);
  const cel      = huValueFlexible(r, ['Cel/Whatsapp (principal)']);
  const celEmer  = huValueFlexible(r, ['Cel/Whatsapp (contacto de emergencia)']);
  const correoP  = huValueFlexible(r, ['Correo electrónico para el envío de la factura','Correo electrónico']);
  const tipoId   = huValueFlexible(r, ['Tipo de identificación']);
  const razon    = huValueFlexible(r, ['Razón social']);
  const rfc      = huValueFlexible(r, ['RFC']);
  const regimen  = huValueFlexible(r, ['Régimen fiscal']);
  const cp       = huValueFlexible(r, ['Código Postal']);
  const reqFact  = huValueFlexible(r, ['¿Requiere factura?']);
  // Fotos (intentamos todas las variantes habituales del Apps Script)
  const ineFront = huValueFlexible(r, ['Link INE frontal','INE frontal','Link foto INE frontal']);
  const ineBack  = huValueFlexible(r, ['Link INE trasero','INE trasero','Link foto INE trasero']);
  const idUnica  = huValueFlexible(r, ['Link identificación única','Identificación única','Link otra identificación','Identificación otro']);
  // Vehículo
  const tieneVeh = huValueFlexible(r, ['¿Cuenta con vehículo?']);
  const marca    = huValueFlexible(r, ['Marca vehículo']);
  const marcaOtro= huValueFlexible(r, ['Marca vehículo otro']);
  const modelo   = huValueFlexible(r, ['Modelo vehículo']);
  const modeloOtr= huValueFlexible(r, ['Modelo vehículo otro']);
  const colorV   = huValueFlexible(r, ['Color vehículo']);
  const placas   = huValueFlexible(r, ['Placas']);
  const horaSal  = huValueFlexible(r, ['Hora habitual de salida']);
  const fotoVeh  = huValueFlexible(r, ['Link foto vehículo','Foto vehículo']);
  const marcaTxt = (marca && marca.toLowerCase() === 'otro') ? (marcaOtro || marca) : marca;
  const modeloTxt= (modelo && modelo.toLowerCase() === 'otro') ? (modeloOtr || modelo) : modelo;

  const wa = huBuildWhatsAppUrl(cel);
  const waEmer = huBuildWhatsAppUrl(celEmer);

  // Sección Vehículo: mostrar si CUALQUIER campo tiene contenido
  const tieneVehInfo = !!(huIsFacturaYes(tieneVeh) || marcaTxt || modeloTxt || colorV || placas || fotoVeh || horaSal);

  return `
    <div class="hu-id-card" style="background:#fff;border-radius:16px;padding:0;color:#0f172a;box-shadow:0 4px 16px rgba(15,23,42,.08);position:relative;overflow:hidden;border:1.5px solid #e2e8f0">
      <!-- accent bar superior -->
      <div style="height:4px;background:linear-gradient(90deg,#0ea5e9,#a855f7,#ec4899)"></div>

      <div style="padding:14px 16px">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
          <div style="font-size:9px;letter-spacing:.18em;color:#64748b;font-weight:800">PERFIL DEL HUÉSPED</div>
          <div style="font-size:9px;color:#94a3b8;text-transform:uppercase;font-weight:700;padding:2px 8px;background:#f1f5f9;border-radius:999px">ID Card</div>
        </div>

        <!-- Nombre y tipo -->
        <div style="margin-bottom:14px;padding-bottom:12px;border-bottom:1.5px solid #f1f5f9">
          <div style="font-size:17px;font-weight:800;color:#0f172a;line-height:1.25;margin-bottom:4px">${esc(nombre || '—')}</div>
          ${tipoId ? `<div style="font-size:10px;color:#64748b;letter-spacing:.04em;text-transform:uppercase;font-weight:600">${esc(tipoId)}</div>` : ''}
        </div>

        <!-- Fotos INE -->
        <div style="margin-bottom:14px">
          <div style="font-size:9px;letter-spacing:.12em;color:#475569;font-weight:800;margin-bottom:8px">📇 IDENTIFICACIÓN</div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
            ${huPhotoBox(ineFront, 'INE frontal', { icon: '🪪', height: '140px' })}
            ${huPhotoBox(ineBack,  'INE trasero', { icon: '🪪', height: '140px' })}
          </div>
          ${idUnica ? `<div style="margin-top:8px">${huPhotoBox(idUnica, 'Identificación única', { icon: '🆔', height: '120px' })}</div>` : ''}
        </div>

        <!-- Contacto -->
        <div style="margin-bottom:14px;padding:10px 12px;background:linear-gradient(180deg,#f0fdfa,#fff);border:1px solid #99f6e4;border-radius:10px">
          <div style="font-size:9px;letter-spacing:.12em;color:#0f766e;font-weight:800;margin-bottom:8px">📞 CONTACTO</div>
          <div style="display:flex;flex-direction:column;gap:6px">
            ${cel ? `<div style="display:flex;align-items:center;gap:8px;font-size:12px">
              <span>📱</span>
              ${wa ? `<a href="${esc(wa)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:#0d9488;font-weight:800;text-decoration:none">${esc(cel)}</a>` : `<span style="color:#0f172a;font-weight:700">${esc(cel)}</span>`}
              <span style="font-size:9px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.04em">Principal</span>
            </div>` : ''}
            ${correoP ? `<div style="display:flex;align-items:center;gap:8px;font-size:11px;color:#1f2937;word-break:break-all">
              <span>📧</span><span>${esc(correoP)}</span>
            </div>` : ''}
          </div>
        </div>

        <!-- Contacto de emergencia (cuando exista) -->
        ${celEmer ? `
        <div style="margin-bottom:14px;padding:10px 12px;background:linear-gradient(180deg,#fef2f2,#fff);border:1px solid #fecaca;border-radius:10px">
          <div style="font-size:9px;letter-spacing:.12em;color:#b91c1c;font-weight:800;margin-bottom:8px">🚨 CONTACTO DE EMERGENCIA</div>
          <div style="display:flex;align-items:center;gap:8px;font-size:12px">
            <span>📱</span>
            ${waEmer ? `<a href="${esc(waEmer)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:#b91c1c;font-weight:800;text-decoration:none">${esc(celEmer)}</a>` : `<span style="color:#7f1d1d;font-weight:700">${esc(celEmer)}</span>`}
          </div>
        </div>` : ''}

        <!-- Facturación si aplica -->
        ${(razon || rfc || regimen) ? `
        <div style="margin-bottom:14px;padding:10px 12px;background:linear-gradient(180deg,#eff6ff,#fff);border:1px solid #bfdbfe;border-radius:10px">
          <div style="font-size:9px;letter-spacing:.12em;color:#1e40af;font-weight:800;margin-bottom:6px">🧾 DATOS FISCALES ${huIsFacturaYes(reqFact)?'<span style="color:#15803d;background:#dcfce7;padding:1px 7px;border-radius:999px;margin-left:4px;font-weight:700">Requiere factura</span>':''}</div>
          ${razon   ? `<div style="font-size:12px;color:#0f172a;font-weight:700;margin-bottom:3px">${esc(razon)}</div>` : ''}
          ${rfc     ? `<div style="font-size:11px;color:#1f2937;font-family:'SFMono-Regular',Consolas,monospace;letter-spacing:.04em;background:#dbeafe;padding:2px 6px;border-radius:4px;display:inline-block;margin-bottom:3px">RFC: <b>${esc(rfc)}</b></div>` : ''}
          ${regimen ? `<div style="font-size:10px;color:#475569;margin-top:3px">${esc(regimen)}</div>` : ''}
          ${cp      ? `<div style="font-size:10px;color:#475569;margin-top:3px">📮 C.P. ${esc(cp)}</div>` : ''}
        </div>` : ''}

        <!-- Vehículo si aplica -->
        ${tieneVehInfo ? `
        <div style="padding:10px 12px;background:linear-gradient(180deg,#f5f3ff,#fff);border:1px solid #ddd6fe;border-radius:10px">
          <div style="font-size:9px;letter-spacing:.12em;color:#6d28d9;font-weight:800;margin-bottom:8px">🚗 VEHÍCULO</div>
          ${fotoVeh ? `<div style="margin-bottom:10px">${huPhotoBox(fotoVeh, 'Foto vehículo', { icon: '🚗', height: '140px' })}</div>` : ''}
          ${(marcaTxt||modeloTxt) ? `<div style="font-size:13px;color:#0f172a;font-weight:700;margin-bottom:4px">${esc(marcaTxt||'')} ${esc(modeloTxt||'')}</div>` : ''}
          <div style="display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin-top:4px">
            ${colorV ? `<span style="font-size:10px;color:#475569;background:#f1f5f9;padding:2px 8px;border-radius:999px">🎨 ${esc(colorV)}</span>` : ''}
            ${placas ? `<span style="font-size:11px;color:#0f172a;font-family:'SFMono-Regular',Consolas,monospace;font-weight:800;background:#fef3c7;padding:3px 10px;border-radius:4px;border:1.5px solid #fcd34d;letter-spacing:.08em">${esc(placas)}</span>` : ''}
          </div>
          ${horaSal ? `<div style="font-size:10px;color:#6b7280;margin-top:8px;display:flex;align-items:center;gap:5px">⏰ <span>Salida habitual: <b style="color:#1f2937">${esc(horaSal)}</b></span></div>` : ''}
        </div>` : ''}
      </div>
    </div>`;
}

/** Lista historial de reservaciones del mismo celular. */
function huBuildHistoryList(currentR, allRows, selectedRecId, outerCardRecId) {
  const cel = huValueFlexible(currentR, ['Cel/Whatsapp (principal)']);
  const list = (allRows || HU_STATE.rows || [])
    .filter(x => huValueFlexible(x, ['Cel/Whatsapp (principal)']) === cel && cel)
    .sort((a, b) => {
      const da = huValueFlexible(a, ['Fecha de ingreso']) || '';
      const db = huValueFlexible(b, ['Fecha de ingreso']) || '';
      return db.localeCompare(da);
    });
  const items = list.map(x => {
    const xid     = String(x['ID'] || x['row_number'] || '');
    const ingreso = huValueFlexible(x, ['Fecha de ingreso']);
    const salida  = huValueFlexible(x, ['Fecha de salida']);
    const prop    = huValueFlexible(x, ['Propiedad']);
    const depto   = huValueFlexible(x, ['# Departamento']);
    let   noches  = huValueFlexible(x, ['# Noches']);
    if (!noches || Number(noches) <= 0) {
      const di = huParseDate(ingreso); const ds = huParseDate(salida);
      if (di && ds) {
        const n = Math.max(0, Math.round((ds - di) / 86400000));
        if (n > 0) noches = String(n);
      }
    }
    const monto   = huValueFlexible(x, ['$ Monto facturado Total','($) Monto Total pagado']);
    const status  = huGetFacturaStatus(x);
    const isSel   = xid === selectedRecId;
    // El punto representa estado de la estancia (no de la factura):
    // verde = en curso, gris = concluida, azul = próxima.
    const stayState = huGetStayState(ingreso, salida);
    const dotMeta   = HU_STAY_DOT[stayState] || HU_STAY_DOT[''];
    const dotColor  = dotMeta.color;
    const dotTitle  = dotMeta.label;
    // Verde "Activa" pulsa suave; rojo "Salida hoy" pulsa más fuerte para llamar la atención.
    const dotPulse  = stayState === 'activa'     ? 'animation:hu-dot-pulse 1.4s ease-in-out infinite;'
                    : stayState === 'salida_hoy' ? 'animation:hu-dot-pulse-strong 1s ease-in-out infinite;'
                    : '';
    return `
      <div onclick="event.stopPropagation();huSelectReservation('${esc(outerCardRecId)}','${esc(xid)}')"
           data-hu-history-id="${esc(xid)}"
           class="hu-history-item ${isSel?'hu-history-active':''}"
           style="padding:11px 13px;border:none;border-radius:10px;cursor:pointer;background:#fff;transition:all 180ms cubic-bezier(.16,1,.3,1);${isSel?'box-shadow:0 4px 14px rgba(15,23,42,.14);transform:translateX(3px)':'box-shadow:0 1px 2px rgba(15,23,42,.06)'}"
           onmouseover="if(!this.classList.contains('hu-history-active')){this.style.boxShadow='0 4px 10px rgba(15,23,42,.10)';this.style.transform='translateX(2px)'}"
           onmouseout="if(!this.classList.contains('hu-history-active')){this.style.boxShadow='0 1px 2px rgba(15,23,42,.06)';this.style.transform=''}">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">
          <div style="font-size:11px;font-weight:800;color:#1f2937;letter-spacing:.02em">${huFmtFecha(ingreso)} → ${huFmtFecha(salida)}</div>
          <span style="display:inline-flex;align-items:center;gap:5px" title="${dotTitle}">
            <span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${dotColor};box-shadow:0 0 0 2px rgba(255,255,255,.7);${dotPulse}"></span>
            <span style="font-size:9px;font-weight:700;color:${dotColor};text-transform:uppercase;letter-spacing:.04em">${esc(dotTitle)}</span>
          </span>
        </div>
        <div style="font-size:11px;color:#64748b;font-weight:600">${esc(prop || '—')}${depto?' · # '+esc(depto):''}</div>
        <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">
          <span style="font-size:10px;color:#94a3b8">${esc(noches||'—')} noche${String(noches)==='1'?'':'s'}</span>
          ${monto ? `<span style="font-size:11px;font-weight:700;color:#0f766e">${huFmtMonto(monto)}</span>` : ''}
        </div>
      </div>`;
  }).join('');

  return `
    <div class="hu-col-card" style="background:#fff;border-radius:16px;padding:0;color:#0f172a;box-shadow:0 4px 16px rgba(15,23,42,.08);position:relative;overflow:hidden;border:1.5px solid #e2e8f0">
      <div style="height:4px;background:linear-gradient(90deg,#10b981,#06b6d4,#3b82f6)"></div>
      <div style="padding:14px 16px">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
          <div style="font-size:9px;letter-spacing:.18em;color:#64748b;font-weight:800">📚 HISTORIAL DE RESERVACIONES</div>
          <div style="font-size:9px;color:#0d9488;text-transform:uppercase;font-weight:700;padding:2px 8px;background:#ccfbf1;border-radius:999px">${list.length} ${list.length===1?'reserva':'reservas'}</div>
        </div>
        <div style="display:flex;flex-direction:column;gap:6px;overflow-y:auto;max-height:680px;padding:8px;background:#f8fafc;border-radius:12px;border:1px solid #e2e8f0">
          ${items || '<div style="padding:20px;text-align:center;color:#94a3b8;font-size:12px;font-style:italic">Sin historial.</div>'}
        </div>
      </div>
    </div>`;
}

/** Construye el contenido de la columna derecha (detalle de la reservación seleccionada).
 *  Incluye la caja de auto-facturación con sus inputs editables/calculados. */
function huBuildReservationDetail(r) {
  const status     = huGetFacturaStatus(r);
  const recId      = String(r['ID'] || r['row_number'] || '');
  const ingreso    = huValueFlexible(r, ['Fecha de ingreso']);
  const salida     = huValueFlexible(r, ['Fecha de salida']);
  const horaIng    = huValueFlexible(r, ['Hora estimada de llegada','Hora de llegada']);
  const horaSal    = huValueFlexible(r, ['Hora estimada de salida','Hora de salida']);
  let noches       = huValueFlexible(r, ['# Noches']);
  // Si no viene en el row, calcúlalo a partir de fecha de ingreso/salida.
  if (!noches || Number(noches) <= 0) {
    const di = huParseDate(ingreso); const ds = huParseDate(salida);
    if (di && ds) {
      const n = Math.max(0, Math.round((ds - di) / 86400000));
      if (n > 0) noches = String(n);
    }
  }
  const huespedes  = huValueFlexible(r, ['# Huéspedes']);
  const propiedad  = huValueFlexible(r, ['Propiedad']);
  const departamento = huValueFlexible(r, ['# Departamento']);
  const motivo     = huValueFlexible(r, ['Motivo de tu hospedaje','Motivo hospedaje','Motivo']);
  const motivoOtro = huValueFlexible(r, ['Motivo otro']);
  const motivoTxt  = (motivo && motivo.toLowerCase() === 'otro') ? (motivoOtro || motivo) : motivo;
  const comentarios= huValueFlexible(r, ['Notas','Envía tus comentarios','Comentarios']);
  const comentFact = huValueFlexible(r, ['Envía tus comentarios con relación a la factura']);
  const nombresHues= huValueFlexible(r, ['Nombres de TODOS los huéspedes (separados por comas)']);
  const medio      = huValueFlexible(r, ['Medio de reservación']);
  const formaPago  = huValueFlexible(r, ['Forma de pago']);
  const montoFact  = huValueFlexible(r, ['$ Monto facturado Total']);
  const montoAirbnb= huValueFlexible(r, ['$ Monto total Airbnb']);
  const reqFactura = huValueFlexible(r, ['¿Requiere factura?']);
  const folio      = huValueFlexible(r, ['Folio facturapi','Folio Facturapi','Folio']);
  const ticketUrl  = huExtractTicketUrl(r);
  const esAirbnb   = String(medio||'').toLowerCase().includes('airbnb');
  const medioBadge = huMedioBadge(medio);

  // Pre-cálculo Airbnb
  const airbnbVal    = esAirbnb ? huParseMontoAirbnb(montoAirbnb) : 0;
  const comisionPre  = esAirbnb && airbnbVal ? huCalcComisionAirbnb(airbnbVal).toFixed(2)        : '';
  const facturadoPre = esAirbnb && airbnbVal ? huCalcMontoFacturadoAirbnb(airbnbVal).toFixed(2) : montoFact;

  const mensajeConsulta = ticketUrl ? huBuildTicketConsultaMsg(ticketUrl) : '';
  const btnVerTicket = (status === 'emitida' && ticketUrl) ? `
    <a href="${esc(ticketUrl)}" target="_blank" rel="noopener" onclick="event.stopPropagation()"
       style="display:inline-block;padding:7px 14px;border:none;background:#16a34a;color:#fff;border-radius:8px;font-weight:700;font-size:12px;cursor:pointer;text-decoration:none;margin-right:6px;box-shadow:0 2px 4px rgba(22,163,74,.3)">
      🧾 Ver ticket${folio ? ' - Folio #' + esc(folio) : ''}
    </a>` : '';
  const btnCopiarMsg = (status === 'emitida' && mensajeConsulta) ? `
    <button type="button" onclick="event.stopPropagation();huespedesCopiarMsgConsulta(this,'${encodeURIComponent(mensajeConsulta)}')"
            style="padding:7px 14px;border:none;background:#475569;color:#fff;border-radius:8px;font-weight:700;font-size:12px;cursor:pointer">
      📋 Copiar mensaje para consultar ticket emitido
    </button>` : '';
  const btnGenerar = (status === 'pendiente') ? `
    <button onclick="event.stopPropagation();huespedesGenerarTicket('${esc(recId)}')"
            style="padding:8px 22px;border:none;background:linear-gradient(180deg,#ef4444 0%,#b91c1c 100%);color:#fff;border-radius:8px;font-weight:700;font-size:13px;cursor:pointer;box-shadow:0 2px 6px rgba(185,28,28,.4)">
      Generar Ticket
    </button>` : '';

  const fldRow = (label, value) => `
    <div style="display:grid;grid-template-columns:140px 1fr;gap:10px;padding:8px 0;border-bottom:1px solid #f1f5f9">
      <div style="font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#a16207;font-weight:700;align-self:center">${esc(label)}</div>
      <div style="font-size:13px;color:#1f2937">${value || '—'}</div>
    </div>`;

  const airbnbBox = `
    <div data-hu-airbnb-box="1" style="border:1.5px solid #c4b5fd;border-radius:12px;padding:12px;background:linear-gradient(180deg,#faf5ff,#fff);margin-top:12px">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#7c3aed;font-weight:800;margin-bottom:10px;display:flex;align-items:center;gap:6px">🧾 Ticket para auto-facturación</div>
      ${esAirbnb ? `
        <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:10px;margin-bottom:8px">
          <div style="font-size:11px;color:#1e40af;font-weight:700;margin-bottom:6px">(=) $ Monto total Airbnb</div>
          <input type="number" step="0.01" min="0" value="${esc(montoAirbnb)}" placeholder="0.00"
                 onclick="event.stopPropagation()"
                 oninput="huRecalcAirbnb(this)"
                 onblur="huMaybePersistCardMonto(this.closest('.hu-resv-detail'))"
                 onkeydown="if(event.key==='Enter'){event.preventDefault();this.blur()}"
                 style="width:100%;padding:7px 10px;border:1px solid #bfdbfe;border-radius:6px;background:#fff;font-size:13px;color:#1e40af;font-weight:700;outline:none">
          <div style="font-size:10px;color:#3b82f6;margin-top:4px">Base editable.</div>
        </div>
        <div style="background:#fde68a;border:1px solid #f59e0b;border-radius:8px;padding:10px;margin-bottom:8px">
          <div style="font-size:11px;color:#78350f;font-weight:700;margin-bottom:6px">(-) $ Comisión Airbnb</div>
          <input type="number" step="0.01" data-hu-airbnb-comision="1" value="${esc(comisionPre)}" readonly
                 onclick="event.stopPropagation()"
                 style="width:100%;padding:7px 10px;border:1px solid #f59e0b;border-radius:6px;background:#fef3c7;font-size:13px;color:#78350f;font-weight:700;cursor:not-allowed;outline:none">
          <div style="font-size:10px;color:#92400e;margin-top:4px;font-weight:600">🔒 Airbnb × 15.5%</div>
        </div>` : ''}
      <div style="background:${esAirbnb?'#ddd6fe':'#ede9fe'};border:1px solid #a78bfa;border-radius:8px;padding:10px;margin-bottom:10px">
        <div style="font-size:11px;color:#4c1d95;font-weight:700;margin-bottom:6px">(+) $ Monto facturado Total</div>
        <input type="number" step="0.01" data-hu-airbnb-facturado="1" value="${esc(facturadoPre)}" ${esAirbnb?'readonly':''}
               onclick="event.stopPropagation()"
               style="width:100%;padding:7px 10px;border:1px solid #a78bfa;border-radius:6px;background:${esAirbnb?'#c4b5fd':'#fff'};font-size:13px;color:#4c1d95;font-weight:700;outline:none">
        ${esAirbnb?'<div style="font-size:10px;color:#6d28d9;margin-top:4px;font-weight:600">🔒 Airbnb − Comisión</div>':''}
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:4px">${btnVerTicket}${btnCopiarMsg}${btnGenerar}</div>
    </div>`;

  // Pill de estado
  const statusPill = status === 'emitida'
    ? `<span style="display:inline-block;padding:4px 12px;border-radius:999px;background:#dcfce7;color:#166534;font-weight:700;font-size:10px;border:1px solid #86efac">EMITIDA${folio?' · '+esc(folio):''}</span>`
    : status === 'pendiente'
    ? `<span style="display:inline-block;padding:4px 12px;border-radius:999px;background:#fef3c7;color:#92400e;font-weight:700;font-size:10px;border:1px solid #fcd34d">PENDIENTE</span>`
    : `<span style="display:inline-block;padding:4px 12px;border-radius:999px;background:#f1f5f9;color:#475569;font-weight:700;font-size:10px;border:1px solid #cbd5e1">SIN FACTURA</span>`;

  return `
    <div class="hu-col-card hu-resv-detail" data-hu-resv-id="${esc(recId)}"
         style="background:#fff;border-radius:16px;padding:0;color:#0f172a;box-shadow:0 4px 16px rgba(15,23,42,.08);position:relative;overflow:hidden;border:1.5px solid #e2e8f0">
      <div style="height:4px;background:linear-gradient(90deg,#f59e0b,#ef4444,#a855f7)"></div>
      <div style="padding:14px 16px">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;padding-bottom:10px;border-bottom:1.5px solid #f1f5f9">
          <div>
            <div style="font-size:9px;letter-spacing:.18em;color:#64748b;font-weight:800;margin-bottom:4px">📑 DETALLE DE RESERVACIÓN</div>
            <div style="font-size:14px;font-weight:800;color:#0f172a">${huFmtFecha(ingreso)} → ${huFmtFecha(salida)}</div>
          </div>
          ${statusPill}
        </div>

        <div style="margin-bottom:8px">${medioBadge}</div>

      <div style="display:flex;flex-direction:column">
        ${fldRow('Propiedad', esc(propiedad))}
        ${fldRow('# Departamento', esc(departamento))}
        ${fldRow('Llegada estimada', horaIng ? esc(huFmtHoraSimple(horaIng)) : '—')}
        ${fldRow('Salida estimada', horaSal ? esc(huFmtHoraSimple(horaSal)) : '—')}
        ${fldRow('# Noches', (noches && Number(noches)>0) ? `<b>${esc(noches)}</b>` : '—')}
        ${fldRow('# Huéspedes', esc(huespedes))}
        ${nombresHues ? fldRow('Nombres de huéspedes', `<span style="font-size:12px;line-height:1.5">${esc(nombresHues)}</span>`) : ''}
        ${motivoTxt ? fldRow('Motivo del hospedaje', `<span style="display:inline-block;padding:3px 10px;border-radius:999px;background:#ede9fe;color:#5b21b6;font-weight:700;font-size:11px;border:1px solid #c4b5fd">${esc(motivoTxt)}</span>`) : ''}
        ${fldRow('Forma de pago', esc(formaPago))}
        ${fldRow('Monto Facturado', montoFact ? `<span data-hu-header-amount="1" style="font-weight:800;color:#0f172a">${huFmtMonto(montoFact)}</span>` : '<span data-hu-header-amount="1">—</span>')}
        ${fldRow('¿Requiere factura?', huReqFacturaBadge(reqFactura))}
        ${comentarios ? fldRow('Comentarios', `<div style="font-size:12px;line-height:1.5;background:#f8fafc;padding:8px 10px;border-radius:6px;border-left:3px solid #94a3b8;font-style:italic;color:#334155">${esc(comentarios)}</div>`) : ''}
        ${comentFact ? fldRow('Comentarios sobre factura', `<div style="font-size:12px;line-height:1.5;background:#fef3c7;padding:8px 10px;border-radius:6px;border-left:3px solid #f59e0b;font-style:italic;color:#78350f">${esc(comentFact)}</div>`) : ''}
      </div>

      ${airbnbBox}

        <div style="margin-top:14px;display:flex;justify-content:flex-end">
          <button onclick="event.stopPropagation();huespedesOpenDetail('${esc(recId)}')"
                  style="padding:7px 16px;border:1.5px solid #cbd5e1;background:#fff;color:#475569;border-radius:8px;font-weight:600;font-size:11px;cursor:pointer">
            Ver todos los campos
          </button>
        </div>
      </div>
    </div>`;
}

/** Click en una entrada del historial: re-renderiza la columna derecha y
 *  marca el nuevo seleccionado. outerRecId es el id del details outer. */
window.huSelectReservation = function(outerRecId, selectedRecId) {
  const r = (HU_STATE.rows||[]).find(x => String(x['ID']||x['row_number']||'') === String(selectedRecId));
  if (!r) return;
  const card = document.querySelector(`.hu-record[data-record-id="${outerRecId}"]`);
  if (!card) return;
  const detailCol = card.querySelector('.hu-col-detail');
  const historyCol = card.querySelector('.hu-col-history');
  if (detailCol) {
    detailCol.innerHTML = huBuildReservationDetail(r);
    detailCol.style.animation = 'hu-fade-in 280ms cubic-bezier(.16,1,.3,1)';
    setTimeout(() => { detailCol.style.animation = ''; }, 300);
  }
  if (historyCol) {
    // Re-renderizar history con nuevo selected
    historyCol.innerHTML = huBuildHistoryList(r, HU_STATE.rows, selectedRecId, outerRecId);
  }
};

function huBuildRecordCard(r) {
  const status     = huGetFacturaStatus(r);
  const recId      = String(r['ID'] || r['row_number'] || '');
  const nombre     = huValueFlexible(r, ['Nombre de la persona que hizo la reservación','mbre de la persona que hizo la reservación']);
  const ingreso    = huValueFlexible(r, ['Fecha de ingreso','Fecha de entrada']);
  const salida     = huValueFlexible(r, ['Fecha de salida']);
  let   noches     = huValueFlexible(r, ['# Noches']);
  if (!noches || Number(noches) <= 0) {
    const di = huParseDate(ingreso); const ds = huParseDate(salida);
    if (di && ds) {
      const n = Math.max(0, Math.round((ds - di) / 86400000));
      if (n > 0) noches = String(n);
    }
  }
  const horaIng    = huValueFlexible(r, ['Hora estimada de llegada','Hora de llegada']);
  const horaSal    = huValueFlexible(r, ['Hora estimada de salida','Hora de salida']);
  const huespedes  = huValueFlexible(r, ['# Huéspedes']);
  const propiedad  = huValueFlexible(r, ['Propiedad']);
  const departamento = huValueFlexible(r, ['# Departamento']);
  const medio      = huValueFlexible(r, ['Medio de reservación','Medio de reservacion']);
  const formaPago  = huValueFlexible(r, ['Forma de pago']);
  const montoPag   = huValueFlexible(r, ['($) Monto Total pagado','$ Monto Total pagado','Monto Total pagado']);
  const montoFact  = huValueFlexible(r, ['$ Monto facturado Total','Monto facturado Total']);
  const montoAirbnb= huValueFlexible(r, ['$ Monto total Airbnb','Monto total Airbnb','Monto Airbnb']);
  const reqFactura = huValueFlexible(r, ['¿Requiere factura?']);
  const razon      = huValueFlexible(r, ['Razón social']);
  const regimen    = huValueFlexible(r, ['Régimen fiscal','Regimen fiscal']);
  const cel        = huValueFlexible(r, ['Cel/Whatsapp (principal)']);
  const folio      = huValueFlexible(r, ['Folio facturapi','Folio Facturapi','Folio']);
  const ticketUrl  = huExtractTicketUrl(r);
  const esAirbnb   = String(medio||'').toLowerCase().includes('airbnb');

  // Paleta semafórica (basada en el screenshot)
  const palette = status === 'pendiente'
      ? { border:'#fecaca', bg:'#fef2f2', leftAcc:'#dc2626' }
      : status === 'emitida'
      ? { border:'#bbf7d0', bg:'#f0fdf4', leftAcc:'#16a34a' }
      : { border:'#cbd5e1', bg:'#f8fafc', leftAcc:'#94a3b8' };

  // Header chips: medio + factura + huéspedes
  const medioBadge = huMedioBadge(medio);
  const huespedesChip = huespedes
    ? `<span data-hu-hdr-huespedes style="display:inline-flex;align-items:center;gap:5px;padding:4px 11px;border-radius:999px;background:#fff;color:#1f2937;font-weight:800;font-size:11px;border:1px solid #e2e8f0;letter-spacing:.02em;box-shadow:0 1px 2px rgba(15,23,42,.05)">👥 ${esc(huespedes)} huésped${String(huespedes)==='1'?'':'es'}</span>`
    : '<span data-hu-hdr-huespedes></span>';
  const facBadge   = status === 'emitida'
    ? (ticketUrl
        ? `<a href="${esc(ticketUrl)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="text-decoration:none">
             <span style="display:inline-block;padding:5px 12px;border-radius:999px;background:#dcfce7;color:#166534;font-weight:700;font-size:11px;border:1px solid #86efac">🧾 Ticket emitido${folio?' - Folio #'+esc(folio):''}</span>
           </a>`
        : `<span style="display:inline-block;padding:5px 12px;border-radius:999px;background:#dcfce7;color:#166534;font-weight:700;font-size:11px;border:1px solid #86efac">🧾 Ticket emitido${folio?' - Folio #'+esc(folio):''}</span>`)
    : status === 'pendiente'
    ? `<span style="display:inline-block;padding:5px 12px;border-radius:999px;background:#fff7ed;color:#c2410c;font-weight:700;font-size:11px;border:1.5px solid #fdba74">🧾 Ticket pendiente</span>`
    : '';

  // KPIs del huésped (sumando su historial completo de reservaciones)
  const stats = huComputeGuestStats(r, HU_STATE.rows);
  const score = huComputeLoyaltyScore(stats);
  const tier  = huGuestTier(score, stats);
  const tierBadge = tier ? `
    <div title="${esc(tier.tooltip)}"
         style="display:flex;align-items:center;gap:6px;padding:6px 12px;border-radius:999px;background:${tier.bg};color:${tier.fg};font-weight:800;font-size:11px;letter-spacing:.04em;text-transform:uppercase;border:1.5px solid ${tier.border};box-shadow:0 2px 8px ${tier.shadow};animation:hu-badge-glow 2.6s ease-in-out infinite">
      <span style="font-size:14px">${tier.icon}</span>
      <span>${tier.label}</span>
      <span style="display:inline-flex;align-items:center;justify-content:center;min-width:28px;height:18px;padding:0 6px;border-radius:9px;background:rgba(255,255,255,.7);color:${tier.fg};font-size:10px;font-weight:800;letter-spacing:0">${tier.score}</span>
    </div>` : '';
  const kpisHtml = `
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      <div style="background:rgba(255,255,255,.75);border:1.5px solid ${palette.border};border-radius:10px;padding:6px 12px;min-width:84px;text-align:center;backdrop-filter:blur(4px)" title="Suma global de noches en todas las reservaciones del huésped">
        <div style="font-size:9px;color:#64748b;font-weight:800;letter-spacing:.05em;text-transform:uppercase">🌙 Noches globales</div>
        <div style="font-size:18px;font-weight:800;color:#0f172a;line-height:1.1">${stats.totalNoches}</div>
      </div>
      <div style="background:rgba(255,255,255,.75);border:1.5px solid ${palette.border};border-radius:10px;padding:6px 12px;min-width:84px;text-align:center;backdrop-filter:blur(4px)" title="Visitas globales distintas (reservaciones consecutivas cuentan como una sola visita)">
        <div style="font-size:9px;color:#64748b;font-weight:800;letter-spacing:.05em;text-transform:uppercase">🧳 Visitas globales</div>
        <div style="font-size:18px;font-weight:800;color:#0f172a;line-height:1.1">${stats.visitas}</div>
      </div>
      <div style="background:rgba(255,255,255,.75);border:1.5px solid ${palette.border};border-radius:10px;padding:6px 12px;min-width:104px;text-align:center;backdrop-filter:blur(4px)" title="Suma de $ Monto facturado Total de todas las reservaciones del huésped">
        <div style="font-size:9px;color:#64748b;font-weight:800;letter-spacing:.05em;text-transform:uppercase">💰 Monto global</div>
        <div style="font-size:16px;font-weight:800;color:#0f172a;line-height:1.1">${stats.montoGlobal > 0 ? huFmtMonto(stats.montoGlobal) : '—'}</div>
      </div>
      ${tierBadge}
    </div>`;

  // Header SUMMARY — clic abre/cierra (sin marker nativo)
  const summary = `
    <summary style="cursor:pointer;list-style:none;padding:16px 18px;background:${palette.bg};display:grid;grid-template-columns:1fr auto auto auto;gap:14px;align-items:center">
      <div>
        <div style="display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin-bottom:10px">${medioBadge}${huespedesChip}</div>
        <div style="font-size:18px;font-weight:800;color:#111827;line-height:1.25;margin-bottom:6px">${esc(nombre || '—')}</div>
        <div style="font-size:13px;color:#64748b;font-weight:500">${esc(propiedad || '—')}${departamento ? ' · # ' + esc(departamento) : ''}</div>
        <div style="font-size:13px;color:#64748b;font-weight:500;margin-top:2px">${huFmtFecha(ingreso)} → ${huFmtFecha(salida)}</div>
        <div style="font-size:12px;color:#475569;font-weight:600;margin-top:3px">🌙 <span data-hu-hdr-noches>${esc(noches && Number(noches)>0 ? noches : '—')}</span> noche${String(noches)==='1'?'':'s'}</div>
        <div style="display:flex;flex-wrap:wrap;gap:14px;margin-top:6px;font-size:11px;color:#64748b">
          <span><span style="color:#94a3b8;font-weight:600">Llegada estimada:</span> <b data-hu-hdr-horaing style="color:#1f2937">${esc(horaIng ? huFmtHoraSimple(horaIng) : '—')}</b></span>
          <span><span style="color:#94a3b8;font-weight:600">Salida estimada:</span> <b data-hu-hdr-horasal style="color:#1f2937">${esc(horaSal ? huFmtHoraSimple(horaSal) : '—')}</b></span>
        </div>
      </div>
      ${kpisHtml}
      <div style="display:flex;flex-direction:column;align-items:flex-end;gap:8px;min-width:160px">
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#a16207;font-weight:700">Monto facturado</div>
        <div data-hu-header-amount="1" style="font-size:22px;font-weight:800;color:#111827">${montoFact ? huFmtMonto(montoFact) : '—'}</div>
        <div>${facBadge}</div>
      </div>
      <div style="display:flex;align-items:center;justify-content:center;width:38px;height:38px;border-radius:50%;border:1.5px solid ${palette.border};background:#fff;color:#475569;font-size:14px;flex-shrink:0" class="hu-record-chev">▾</div>
    </summary>`;

  // Cuerpo expandido — grid de tarjetas-campo
  const fieldCard = (label, valueHtml) => `
    <div style="border:1.5px solid #e2e8f0;border-radius:12px;padding:11px 14px;background:#fff">
      <div style="font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#a16207;font-weight:700;margin-bottom:6px">${esc(label)}</div>
      <div style="font-size:13.5px;color:#1f2937">${valueHtml || '—'}</div>
    </div>`;

  // Pre-cálculo Airbnb cuando aplique (para que el primer render ya muestre los valores)
  const airbnbVal = esAirbnb ? huParseMontoAirbnb(montoAirbnb) : 0;
  const comisionPre  = esAirbnb && airbnbVal ? huCalcComisionAirbnb(airbnbVal).toFixed(2)        : '';
  const facturadoPre = esAirbnb && airbnbVal ? huCalcMontoFacturadoAirbnb(airbnbVal).toFixed(2) : montoFact;
  // El auto-save ahora se dispara desde el ontoggle del <details> cuando se
  // abre la card. Esto es más confiable que un setTimeout dependiente del DOM.

  // Botones cuando el ticket está emitido
  const mensajeConsulta = ticketUrl ? huBuildTicketConsultaMsg(ticketUrl) : '';
  const btnVerTicket = (status === 'emitida' && ticketUrl) ? `
    <a href="${esc(ticketUrl)}" target="_blank" rel="noopener" onclick="event.stopPropagation()"
       style="display:inline-block;padding:7px 14px;border:none;background:#16a34a;color:#fff;border-radius:8px;font-weight:700;font-size:12px;cursor:pointer;text-decoration:none;margin-right:6px;box-shadow:0 2px 4px rgba(22,163,74,.3)">
      🧾 Ver ticket${folio ? ' - Folio #' + esc(folio) : ''}
    </a>` : '';
  const btnCopiarMsg = (status === 'emitida' && mensajeConsulta) ? `
    <button type="button" onclick="event.stopPropagation();huespedesCopiarMsgConsulta(this,'${encodeURIComponent(mensajeConsulta)}')"
            style="padding:7px 14px;border:none;background:#475569;color:#fff;border-radius:8px;font-weight:700;font-size:12px;cursor:pointer;box-shadow:0 2px 4px rgba(71,85,105,.3)">
      📋 Copiar mensaje para consultar ticket emitido
    </button>` : '';
  const btnGenerar = (status === 'pendiente') ? `
    <button onclick="event.stopPropagation();huespedesGenerarTicket('${esc(recId)}')"
            style="padding:8px 22px;border:none;background:linear-gradient(180deg,#ef4444 0%,#b91c1c 100%);color:#fff;border-radius:8px;font-weight:700;font-size:13px;cursor:pointer;box-shadow:0 2px 6px rgba(185,28,28,.4)">
      Generar Ticket
    </button>` : '';

  // Bloque Airbnb auto-facturación
  const airbnbBox = `
    <div data-hu-airbnb-box="1" style="border:1.5px solid #e2e8f0;border-radius:12px;padding:11px 14px;background:#fff">
      <div style="font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#a16207;font-weight:700;margin-bottom:10px">Ticket para auto-facturación</div>
      ${esAirbnb ? `
        <!-- (=) Monto total Airbnb — editable -->
        <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:10px;margin-bottom:8px">
          <div style="font-size:11px;color:#1e40af;font-weight:700;margin-bottom:6px">(=) $ Monto total Airbnb</div>
          <input type="number" step="0.01" min="0" value="${esc(montoAirbnb)}" placeholder="0.00"
                 onclick="event.stopPropagation()"
                 oninput="huRecalcAirbnb(this)"
                 onblur="huMaybePersistCardMonto(this.closest('.hu-record'))"
                 onkeydown="if(event.key==='Enter'){event.preventDefault();this.blur()}"
                 style="width:100%;padding:7px 10px;border:1px solid #bfdbfe;border-radius:6px;background:#fff;font-size:13px;color:#1e40af;font-weight:700;outline:none">
          <div style="font-size:10px;color:#3b82f6;margin-top:4px">Base editable para calcular comisión y monto facturado.</div>
        </div>
        <!-- (-) Comisión Airbnb — calculado, fondo más sombreado -->
        <div style="background:#fde68a;border:1px solid #f59e0b;border-radius:8px;padding:10px;margin-bottom:8px">
          <div style="font-size:11px;color:#78350f;font-weight:700;margin-bottom:6px">(-) $ Comisión Airbnb</div>
          <input type="number" step="0.01" data-hu-airbnb-comision="1" value="${esc(comisionPre)}" readonly aria-readonly="true"
                 onclick="event.stopPropagation()"
                 style="width:100%;padding:7px 10px;border:1px solid #f59e0b;border-radius:6px;background:#fef3c7;font-size:13px;color:#78350f;font-weight:700;cursor:not-allowed;outline:none">
          <div style="font-size:10px;color:#92400e;margin-top:4px;font-weight:600">🔒 Calculado automáticamente: Airbnb × 15.5%</div>
        </div>` : ''}
      <!-- (+) Monto facturado Total — calculado/editable según medio -->
      <div style="background:${esAirbnb?'#ddd6fe':'#ede9fe'};border:1px solid #a78bfa;border-radius:8px;padding:10px;margin-bottom:10px">
        <div style="font-size:11px;color:#4c1d95;font-weight:700;margin-bottom:6px">(+) $ Monto facturado Total</div>
        <input type="number" step="0.01" data-hu-airbnb-facturado="1" value="${esc(facturadoPre)}" ${esAirbnb?'readonly aria-readonly="true"':''} onclick="event.stopPropagation()"
               style="width:100%;padding:7px 10px;border:1px solid #a78bfa;border-radius:6px;background:${esAirbnb?'#c4b5fd':'#fff'};font-size:13px;color:#4c1d95;font-weight:700;${esAirbnb?'cursor:not-allowed;':''}outline:none">
        ${esAirbnb?'<div style="font-size:10px;color:#6d28d9;margin-top:4px;font-weight:600">🔒 Calculado automáticamente: $ Monto total Airbnb − $ Comisión Airbnb</div>':''}
      </div>
      <!-- Botones: Ver ticket + Copiar mensaje (cuando emitida) o Generar Ticket (cuando pendiente) -->
      <div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:4px">${btnVerTicket}${btnCopiarMsg}${btnGenerar}</div>
    </div>`;

  // Cel con link
  const wa = huBuildWhatsAppUrl(cel);
  const phoneHtml = wa
    ? `<a href="${esc(wa)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:#0d9488;text-decoration:none;font-weight:800;font-size:15px">${esc(cel)}</a>`
    : `<span style="font-weight:700;color:#1f2937">${esc(cel || '—')}</span>`;

  return `
    <details class="hu-record" data-record-id="${esc(recId)}" ontoggle="huOnDetailsToggle(this)" style="position:relative;border:1.5px solid ${palette.border};border-radius:14px;background:#fff;overflow:hidden;box-shadow:0 1px 3px rgba(15,23,42,.04)">
      <button class="btn-x" title="Eliminar reservación"
              onclick="event.stopPropagation(); event.preventDefault(); deleteHuespedRecord('${esc(recId)}', '${esc(nombre || '').replace(/'/g, "\\'")}')"
              style="position:absolute;top:10px;left:10px;z-index:5">×</button>
      ${summary}
      <div class="hu-record-body" style="padding:16px;background:linear-gradient(180deg,#f8fafc,#fff);display:grid;grid-template-columns:minmax(260px,1fr) minmax(220px,1fr) minmax(320px,1.4fr);gap:14px;align-items:start">
        <div class="hu-col-profile">${huBuildIdCard(r)}</div>
        <div class="hu-col-history">${huBuildHistoryList(r, HU_STATE.rows, recId, recId)}</div>
        <div class="hu-col-detail">${huBuildReservationDetail(r)}</div>
      </div>
    </details>`;
}

// ─── Integración Facturapi ──────────────────────────────────────────────────
// URL base del facturapi del check-in (Cloud Run us-central1).
// Se puede sobreescribir guardando otra en localStorage.HU_FACTURAPI_URL.
const HU_FACTURAPI_DEFAULT = 'https://checkin-app-957627511957.us-central1.run.app/facturapi';
function huGetFacturapiUrl() {
  try {
    const override = localStorage.getItem('HU_FACTURAPI_URL');
    if (override && override.trim()) return override.trim();
  } catch(_) {}
  return HU_FACTURAPI_DEFAULT;
}

const HU_CHECKIN_WEBAPP_URL = 'https://script.google.com/macros/s/AKfycbwqMfC6tITLXlhEwYzQ5mKzw-KD6-nV7XVKIuekj6pK4Po50oRfVKClZeHcr-si3ppB/exec';

function huNormalizeCurrencyFromSheet(v) {
  const raw = String(v||'').trim().toUpperCase();
  if (!raw) return 'MXN';
  if (raw.includes('USD') || raw.includes('DÓLAR') || raw.includes('DOLAR')) return 'USD';
  if (raw.includes('EUR') || raw.includes('EURO')) return 'EUR';
  if (raw.includes('GBP') || raw.includes('LIBRA')) return 'GBP';
  if (raw.includes('BRL') || raw.includes('REAL')) return 'BRL';
  return 'MXN';
}

function huResolveConceptoPorRegimen(regimenFiscal) {
  const norm = String(regimenFiscal || '').trim().toLowerCase();
  return norm === 'general de ley personas morales'
    ? '1. Arrendamiento en Saltillo, Coah.'
    : '2. Arrendamiento en Saltillo, Coah.';
}

function huNormalizePhoneWA(phone) {
  let clean = String(phone || '').replace(/\D/g, '');
  if (!clean) return '';
  if (clean.length === 10) clean = '52' + clean;
  return clean;
}

function huFmtMontoEditable(v) {
  const raw = String(v||'').trim();
  if (!raw) return '';
  const limpio = raw.replace(/[^0-9.,-]/g, '').replace(/,/g, '');
  return limpio;
}

/** Construye la URL completa de facturapi con todos los params (igual al original).
 *  cantidadOverride: si se pasa, sobrescribe el valor de "quantity" (típicamente
 *  proveniente del input calculado del card). */
function huBuildFacturapiUrlFromRow(row, baseUrl, cantidadOverride) {
  const p = new URLSearchParams();
  p.set('source', 'control_huespedes');
  const cantidad = (cantidadOverride != null && String(cantidadOverride).trim() !== '')
    ? huFmtMontoEditable(cantidadOverride)
    : huFmtMontoEditable(huValueFlexible(row, ['$ Monto facturado Total','Monto facturado Total']));
  p.set('quantity', cantidad || '1');
  p.set('currency', huNormalizeCurrencyFromSheet(huValueFlexible(row, ['Divisa monto pagado','Moneda','Currency'])));
  const email     = huValueFlexible(row, ['Correo electrónico','Correo electronico','Email']);
  const whatsapp  = huNormalizePhoneWA(huValueFlexible(row, ['Cel/Whatsapp (principal)','Celular principal','Whatsapp principal']));
  const taxRegime = huValueFlexible(row, ['Régimen fiscal','Regimen fiscal','Tax regime']);
  const concepto  = huResolveConceptoPorRegimen(taxRegime);
  const recordId  = String(row['ID'] || row['row_number'] || '').trim();
  const rowNumber = String(row['row_number'] || '').trim();
  if (email) p.set('email', email);
  if (concepto) { p.set('concepto', concepto); p.set('descripcion', concepto); }
  if (whatsapp) p.set('whatsapp', whatsapp);
  if (taxRegime) p.set('taxRegime', taxRegime);
  if (recordId) {
    p.set('externalId', 'CHECKIN-' + recordId);
    p.set('recordId', recordId);
    p.set('rowNumber', rowNumber);
  }
  p.set('checkinWebAppUrl', HU_CHECKIN_WEBAPP_URL);
  return `${baseUrl}?${p.toString()}`;
}

/** Genera ticket facturapi para un registro. */
async function huespedesGenerarTicket(recordId) {
  if (!recordId) { alert('No se pudo identificar el registro.'); return; }

  // Buscar row en cache local
  let row = (HU_STATE.rows || []).find(r => String(r['ID']||r['row_number']||'') === String(recordId));
  if (!row) { alert('No se encontró la información del registro seleccionado.'); return; }

  // Si falta régimen fiscal, lo traemos del detalle (igual que el original)
  const missingRegime = !huValueFlexible(row, ['Régimen fiscal','Regimen fiscal','Tax regime']);
  if (missingRegime) {
    try {
      const res = await fetch(`${BACKEND}/huespedes-detail?record_id=${encodeURIComponent(recordId)}`);
      const json = await res.json();
      if (json?.ok && json.record) row = { ...row, ...json.record };
    } catch (_) {}
  }

  // Leer el monto facturado actual del input calculado del card (el valor
  // ya considera el cálculo Airbnb − comisión cuando aplica).
  let cantidad = '';
  const card = document.querySelector(`.hu-record[data-record-id="${recordId}"]`);
  const facInput = card?.querySelector('[data-hu-airbnb-facturado="1"]');
  if (facInput && String(facInput.value).trim() !== '') cantidad = facInput.value;

  // URL base de facturapi (hardcoded al Cloud Run del check-in, override
  // posible vía localStorage.HU_FACTURAPI_URL)
  const base = huGetFacturapiUrl();
  const url = huBuildFacturapiUrlFromRow(row, base, cantidad);
  if (!url) { alert('No se pudo construir la URL de Facturapi.'); return; }

  // Abrir en modal con iframe embebido (en lugar de pestaña independiente)
  huespedesOpenFacturapi(url);
}

/** Abre el modal de Facturapi con el iframe apuntando a la URL dada. */
function huespedesOpenFacturapi(url) {
  const ov   = document.getElementById('hu-facturapi-overlay');
  const ifr  = document.getElementById('hu-facturapi-frame');
  const ext  = document.getElementById('hu-facturapi-external');
  if (!ov || !ifr) { window.open(url, '_blank', 'noopener'); return; }
  if (ext) ext.href = url;
  ifr.src = url;
  ov.classList.remove('hidden');
  document.body.style.overflow = 'hidden';
}

/** Cierra el modal de Facturapi. */
function huespedesCloseFacturapi() {
  const ov  = document.getElementById('hu-facturapi-overlay');
  const ifr = document.getElementById('hu-facturapi-frame');
  if (ifr) ifr.src = 'about:blank';
  if (ov) ov.classList.add('hidden');
  document.body.style.overflow = '';
  // Refresca para reflejar folio + monto facturado guardados por Facturapi.
  // Pequeño delay para que el Apps Script termine de escribir/flush
  // antes de re-leer (evita race condition entre cierre del modal y refresh).
  if (HU_STATE.loaded) {
    const lbl = document.getElementById('hu-status-label');
    if (lbl) lbl.textContent = 'Actualizando…';
    setTimeout(() => huespedesLoad(true), 600);
  }
}

// Esc cierra el modal de Facturapi
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    const ov = document.getElementById('hu-facturapi-overlay');
    if (ov && !ov.classList.contains('hidden')) huespedesCloseFacturapi();
  }
});

/** Permite resetear la URL base si quedó mal configurada. */
window.huespedesResetFacturapiUrl = function() {
  try { localStorage.removeItem('HU_FACTURAPI_URL'); } catch(_) {}
  alert('URL de Facturapi reseteada. La próxima vez que generes un ticket te pedirá la URL nueva.');
};

/** Cambia entre vista Lista y Calendario. */
function huespedesSetView(v) {
  HU_STATE.view = v;
  document.getElementById('hu-view-lista').style.background       = v === 'lista'      ? '#334155' : '#fff';
  document.getElementById('hu-view-lista').style.color            = v === 'lista'      ? '#fff'    : '#475569';
  document.getElementById('hu-view-calendario').style.background  = v === 'calendario' ? '#334155' : '#fff';
  document.getElementById('hu-view-calendario').style.color       = v === 'calendario' ? '#fff'    : '#475569';
  huespedesRender();
}

/** Vista Calendario simplificada: agrupa por propiedad → lista días ocupados. */
function huRenderCalendar(rows) {
  if (!rows.length) return `<div class="empty-state" style="padding:30px;text-align:center;color:#94a3b8;font-size:13px">Sin reservaciones para el período seleccionado.</div>`;
  // Agrupar por propiedad
  const byProp = {};
  for (const r of rows) {
    const p = huValueFlexible(r, ['Propiedad']) || '(Sin propiedad)';
    if (!byProp[p]) byProp[p] = [];
    byProp[p].push(r);
  }
  const propsHtml = Object.entries(byProp).sort((a, b) => a[0].localeCompare(b[0])).map(([prop, recs]) => {
    const rowsHtml = recs.map(r => {
      const ingreso = huValueFlexible(r, ['Fecha de ingreso']);
      const salida  = huValueFlexible(r, ['Fecha de salida']);
      const noches  = huValueFlexible(r, ['# Noches']);
      const nombre  = huValueFlexible(r, ['Nombre de la persona que hizo la reservación']);
      const status  = huGetFacturaStatus(r);
      const color   = status === 'pendiente' ? '#dc2626' : status === 'emitida' ? '#16a34a' : '#64748b';
      return `<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;border-bottom:1px solid #f3f4f6">
        <span style="width:8px;height:8px;border-radius:50%;background:${color};flex-shrink:0"></span>
        <div style="flex:1;font-size:12px;color:#1f2937"><b>${esc(nombre)}</b> · ${esc(ingreso)} → ${esc(salida)} (${esc(noches)} noche${noches==='1'?'':'s'})</div>
      </div>`;
    }).join('');
    return `<div style="margin-bottom:14px">
      <div style="background:#f1f5f9;padding:8px 12px;border-radius:8px 8px 0 0;font-weight:800;color:#334155;font-size:13px">🏠 ${esc(prop)} <span style="color:#64748b;font-weight:600">(${recs.length})</span></div>
      <div style="border:1px solid #e2e8f0;border-radius:0 0 8px 8px">${rowsHtml}</div>
    </div>`;
  }).join('');
  return propsHtml;
}

/** Abre el modal de detalle de una reservación. */
async function huespedesOpenDetail(recordId) {
  const ov = document.getElementById('hu-detail-overlay');
  const body = document.getElementById('hu-detail-body');
  const title = document.getElementById('hu-detail-title');
  if (!ov || !body) return;
  ov.classList.remove('hidden');
  body.innerHTML = '<div style="padding:30px;text-align:center;color:#94a3b8">Cargando detalle…</div>';
  if (title) title.textContent = 'Detalle de reservación';
  try {
    const res = await fetch(`${BACKEND}/huespedes-detail?record_id=${encodeURIComponent(recordId)}`);
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || 'Error al obtener detalle');
    const r = data.record || {};
    if (title) title.textContent = `${r['Nombre de la persona que hizo la reservación'] || 'Reservación'} · ${r['Fecha de ingreso'] || ''}`;
    const rows = Object.entries(r)
      .filter(([k, v]) => k !== '__row_number' && v != null && String(v).trim() !== '')
      .map(([k, v]) => `<tr style="border-bottom:1px solid #f3f4f6">
        <td style="padding:6px 10px;font-weight:700;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:.04em;background:#f8fafc;width:38%">${esc(k)}</td>
        <td style="padding:6px 10px;color:#1f2937;font-size:12px;word-break:break-word">${esc(String(v))}</td>
      </tr>`).join('');
    body.innerHTML = `<table style="width:100%;border-collapse:collapse">${rows}</table>`;
  } catch (e) {
    body.innerHTML = `<div style="padding:20px;color:#dc2626">⚠ ${esc(e.message)}</div>`;
  }
}

/** Exporta los registros filtrados a CSV. */
function huespedesExportCsv() {
  if (!HU_STATE.filteredRows.length) {
    alert('No hay registros para exportar.');
    return;
  }
  const headers = [
    '¿Requiere factura?','$ Monto facturado Total','Fecha de ingreso','Fecha de salida',
    'Nombre de la persona que hizo la reservación','Medio de reservación',
    'Cel/Whatsapp (principal)','Razón social','Forma de pago','Correo electrónico',
    'Propiedad','# Departamento','# Huéspedes'
  ];
  const esc2 = s => `"${String(s ?? '').replace(/"/g, '""')}"`;
  const lines = [headers.join(','), ...HU_STATE.filteredRows.map(r => headers.map(h => esc2(r[h])).join(','))];
  const blob = new Blob(['﻿' + lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = `huespedes_${new Date().toISOString().slice(0,10)}.csv`;
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
}

// ═══════════════════════════════════════════════════════════════════════════
// ─── MÓDULO: Reservas Lodgify ──────────────────────────────────────────────
// Consume el backend de Lodgify (mismo que usa el archivo "otc_FIXED_…html")
// que ya está deployado en Cloud Run. Aggrega los rows por booking Id porque
// la API devuelve un row por LineItem (RoomRate + Fee + …).
// ═══════════════════════════════════════════════════════════════════════════

const LG_API_BASE = "https://checkinnreservas-1044570371371.northamerica-south1.run.app";
const LG_STATE = {
  raw: [],          // rows tal cual del backend (uno por line item)
  bookings: [],     // agregados por Id
  loaded: false,
  loading: false,
  from: '',
  to: '',
};

/** MM/DD/YYYY → Date (UTC). */
function lgParseMMDD(s) {
  if (!s) return null;
  const m = String(s).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (!m) return null;
  return new Date(Date.UTC(+m[3], +m[1]-1, +m[2]));
}

/** Devuelve "DD de mes" en es-MX a partir de MM/DD/YYYY. */
function lgFmtFecha(mmdd) {
  const d = lgParseMMDD(mmdd);
  if (!d) return '—';
  const mes = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'][d.getUTCMonth()];
  return `${d.getUTCDate()} de ${mes}`;
}

/** Trunca un texto largo de HouseName a algo legible como "Baja California #4". */
function lgFmtPropiedad(houseName) {
  const s = String(houseName || '').trim();
  if (!s) return '—';
  // Toma todo hasta el "(" si existe
  const m = s.match(/^([^(]+)/);
  return (m ? m[1] : s).trim();
}

/** Badge del Source — colores por canal. */
function lgSourceBadge(src) {
  const s = String(src || '').trim();
  if (!s) return '<span style="display:inline-block;padding:4px 10px;border-radius:999px;background:#f1f5f9;color:#475569;font-weight:700;font-size:11px;border:1px solid #cbd5e1">Sin canal</span>';
  const lc = s.toLowerCase();
  let bg='#f1f5f9', fg='#475569', bd='#cbd5e1', ico='🌐';
  if (lc.includes('airbnb'))       { bg='#fef2f2'; fg='#dc2626'; bd='#fecaca'; ico='🅰'; }
  else if (lc.includes('booking')) { bg='#dbeafe'; fg='#1e40af'; bd='#93c5fd'; ico='🅱'; }
  else if (lc.includes('expedia')) { bg='#fef3c7'; fg='#92400e'; bd='#fcd34d'; ico='Ⓔ'; }
  else if (lc.includes('vrbo'))    { bg='#dcfce7'; fg='#166534'; bd='#86efac'; ico='Ⓥ'; }
  else if (lc.includes('manual'))  { bg='#ede9fe'; fg='#5b21b6'; bd='#c4b5fd'; ico='✋'; }
  return `<span style="display:inline-flex;align-items:center;gap:5px;padding:4px 10px;border-radius:999px;background:${bg};color:${fg};font-weight:800;font-size:11px;border:1px solid ${bd};letter-spacing:.02em">${ico} ${esc(s)}</span>`;
}

/** Badge del Status (Open/Booked/Tentative/Declined). */
function lgStatusBadge(st) {
  const s = String(st || '').trim();
  if (!s) return '';
  const lc = s.toLowerCase();
  let bg='#f1f5f9', fg='#475569', bd='#cbd5e1';
  if (lc === 'booked')        { bg='#dcfce7'; fg='#166534'; bd='#86efac'; }
  else if (lc === 'open')     { bg='#dbeafe'; fg='#1e40af'; bd='#93c5fd'; }
  else if (lc === 'tentative'){ bg='#fef3c7'; fg='#92400e'; bd='#fcd34d'; }
  else if (lc === 'declined' || lc === 'cancelled') { bg='#fee2e2'; fg='#991b1b'; bd='#fca5a5'; }
  return `<span style="display:inline-block;padding:4px 12px;border-radius:999px;background:${bg};color:${fg};font-weight:700;font-size:10px;border:1px solid ${bd};text-transform:uppercase;letter-spacing:.04em">${esc(s)}</span>`;
}

/** Formato monetario tolerante (igual que huFmtMonto pero sin recuperación ISO). */
function lgFmtMoney(n, currency) {
  const v = Number(n) || 0;
  const cur = (currency || 'MXN').toUpperCase();
  return v.toLocaleString('es-MX', { style:'currency', currency: cur, minimumFractionDigits: v % 1 === 0 ? 0 : 2, maximumFractionDigits: 2 });
}

/** Construye el rango desde/hasta a partir del select de días. */
function lgGetRange() {
  const days = Number(document.getElementById('lg-filtro-rango')?.value) || 30;
  const today = new Date();
  const to = today.toISOString().slice(0,10);
  const fromDate = new Date(today.getTime() - days * 86400000);
  const from = fromDate.toISOString().slice(0,10);
  return { from, to, days };
}

/** Agrega los rows (uno por line item) en un objeto por booking Id. */
function lgAggregateBookings(rows) {
  const byId = new Map();
  for (const r of rows || []) {
    const id = r.Id;
    if (!id) continue;
    let agg = byId.get(id);
    if (!agg) {
      agg = {
        Id: id,
        Source: r.Source || '',
        SourceTextRaw: r.SourceText || '',
        Status: r.Status || '',
        DateArrival: r.DateArrival || '',
        DateDeparture: r.DateDeparture || '',
        DateCancelled: r.DateCancelled || '',
        Nights: Number(r.Nights) || 0,
        HouseName: r.HouseName || '',
        HouseId: r.HouseId || '',
        RoomTypeNames: r.RoomTypeNames || '',
        GuestName: r.GuestName || '',
        GuestEmail: r.GuestEmail || '',
        GuestPhone: r.GuestPhone || '',
        GuestCountryCode: r.GuestCountryCode || '',
        NumberOfGuests: Number(r.NumberOfGuests) || 0,
        Adults: Number(r.Adults) || 0,
        Children: Number(r.Children) || 0,
        Infants: Number(r.Infants) || 0,
        Pets: Number(r.Pets) || 0,
        Currency: r.Currency || 'MXN',
        ChannelBooking: r.ChannelBooking || '',
        Gross: 0,
        Net: 0,
        Vat: 0,
        LineItems: [],
      };
      byId.set(id, agg);
    }
    agg.Gross += Number(r.GrossAmount) || 0;
    agg.Net   += Number(r.NetAmount)   || 0;
    agg.Vat   += Number(r.VatAmount)   || 0;
    agg.LineItems.push({
      kind: r.LineItem || '',
      desc: r.LineItemDescription || '',
      gross: Number(r.GrossAmount) || 0,
      net:   Number(r.NetAmount)   || 0,
      vat:   Number(r.VatAmount)   || 0,
    });
  }
  // Parseo del SourceText (JSON) para extraer confirmationCode etc.
  for (const b of byId.values()) {
    try {
      const meta = JSON.parse(b.SourceTextRaw || '{}');
      b.ConfirmationCode = meta.confirmationCode || '';
      b.ListingId = meta.listingId || '';
      b.ThreadId  = meta.threadId  || '';
    } catch(_) { b.ConfirmationCode=''; b.ListingId=''; b.ThreadId=''; }
  }
  // Ordenar por DateArrival descendente (más reciente primero).
  return Array.from(byId.values()).sort((a,b) => {
    const da = lgParseMMDD(a.DateArrival)?.getTime() || 0;
    const db = lgParseMMDD(b.DateArrival)?.getTime() || 0;
    return db - da;
  });
}

/** Carga los datos del backend de Lodgify. */
async function lodgifyLoad(force) {
  const lbl = document.getElementById('lg-status-label');
  const empty = document.getElementById('lg-empty');
  const cont = document.getElementById('lg-cards');
  if (LG_STATE.loading) return;
  LG_STATE.loading = true;
  if (lbl) lbl.textContent = 'Cargando…';
  if (empty) { empty.textContent = 'Cargando reservaciones de Lodgify…'; empty.classList.remove('hidden'); }
  if (cont) cont.innerHTML = '';
  try {
    const { from, to, days } = lgGetRange();
    LG_STATE.from = from; LG_STATE.to = to;
    const url = `${LG_API_BASE}/api/otc?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`;
    const res = await fetch(url, { headers: { 'Accept':'application/json' }, cache:'no-store' });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || `HTTP ${res.status}`);
    LG_STATE.raw = data.rows || [];
    LG_STATE.bookings = lgAggregateBookings(LG_STATE.raw);
    LG_STATE.loaded = true;
    if (lbl) lbl.textContent = `${LG_STATE.bookings.length} reservaciones (${days}d)`;
    lgRebuildFilterOptions();
    lodgifyRender();
  } catch (e) {
    if (lbl) lbl.textContent = 'Error: ' + e.message;
    if (empty) { empty.textContent = '⚠ ' + e.message; empty.classList.remove('hidden'); }
  } finally {
    LG_STATE.loading = false;
  }
}

/** Llena los selects de Source y Status con los valores únicos. */
function lgRebuildFilterOptions() {
  const sources = [...new Set(LG_STATE.bookings.map(b => b.Source).filter(Boolean))].sort();
  const selSrc = document.getElementById('lg-filtro-source');
  if (selSrc) {
    const prev = selSrc.value;
    selSrc.innerHTML = '<option value="">Todas</option>' +
      sources.map(s => `<option value="${esc(s)}">${esc(s)}</option>`).join('');
    if (sources.includes(prev)) selSrc.value = prev;
  }
  const statuses = [...new Set(LG_STATE.bookings.map(b => b.Status).filter(Boolean))].sort();
  const selSt = document.getElementById('lg-filtro-status');
  if (selSt) {
    const prev = selSt.value;
    selSt.innerHTML = '<option value="">Todos</option>' +
      statuses.map(s => `<option value="${esc(s)}">${esc(s)}</option>`).join('');
    if (statuses.includes(prev)) selSt.value = prev;
  }
}

/** Aplica filtros locales y devuelve los bookings a mostrar. */
function lgGetFiltered() {
  const src = (document.getElementById('lg-filtro-source')?.value || '').toLowerCase();
  const st  = (document.getElementById('lg-filtro-status')?.value || '').toLowerCase();
  const nb  = (document.getElementById('lg-filtro-nombre')?.value || '').toLowerCase().trim();
  return LG_STATE.bookings.filter(b => {
    if (src && String(b.Source||'').toLowerCase() !== src) return false;
    if (st  && String(b.Status||'').toLowerCase() !== st)  return false;
    if (nb  && !String(b.GuestName||'').toLowerCase().includes(nb)) return false;
    return true;
  });
}

/** Renderiza KPIs + lista de cards. */
function lodgifyRender() {
  const list = lgGetFiltered();
  // KPIs
  const nights = list.reduce((a,b) => a + (b.Nights || 0), 0);
  const gross  = list.reduce((a,b) => a + (b.Gross  || 0), 0);
  const cur    = list[0]?.Currency || 'MXN';
  const setT = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  setT('lg-kpi-count', String(list.length));
  setT('lg-kpi-nights', String(nights));
  setT('lg-kpi-gross', gross > 0 ? lgFmtMoney(gross, cur) : '—');
  setT('lg-kpi-avg',   nights > 0 ? lgFmtMoney(gross/nights, cur) : '—');
  // Cards
  const cont = document.getElementById('lg-cards');
  const empty = document.getElementById('lg-empty');
  if (!list.length) {
    cont.innerHTML = '';
    if (empty) { empty.textContent = 'Sin reservaciones según los filtros.'; empty.classList.remove('hidden'); }
    return;
  }
  if (empty) empty.classList.add('hidden');
  cont.innerHTML = list.map(lgBuildCard).join('');
}

/** Card de una reservación de Lodgify — diseño idéntico al de huéspedes. */
function lgBuildCard(b) {
  const nombre = b.GuestName || 'Sin nombre';
  const prop   = lgFmtPropiedad(b.HouseName);
  const ingreso = lgFmtFecha(b.DateArrival);
  const salida  = lgFmtFecha(b.DateDeparture);
  const noches  = b.Nights || 0;
  const stayState = lgGetStayState(b.DateArrival, b.DateDeparture);
  const palette = stayState === 'concluida' ? { border:'#cbd5e1', bg:'#f8fafc' }
                : stayState === 'salida_hoy'? { border:'#fecaca', bg:'#fef2f2' }
                : stayState === 'activa'    ? { border:'#bbf7d0', bg:'#f0fdf4' }
                :                              { border:'#fde68a', bg:'#fffbeb' };

  const huespedesChip = b.NumberOfGuests
    ? `<span style="display:inline-flex;align-items:center;gap:5px;padding:4px 11px;border-radius:999px;background:#fff;color:#1f2937;font-weight:800;font-size:11px;border:1px solid #e2e8f0;letter-spacing:.02em;box-shadow:0 1px 2px rgba(15,23,42,.05)">👥 ${b.NumberOfGuests} huésped${b.NumberOfGuests===1?'':'es'}</span>`
    : '';

  // Header summary
  const summary = `
    <summary style="cursor:pointer;list-style:none;padding:16px 18px;background:${palette.bg};display:grid;grid-template-columns:1fr auto auto;gap:14px;align-items:center">
      <div>
        <div style="display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin-bottom:10px">${lgSourceBadge(b.Source)}${huespedesChip}</div>
        <div style="font-size:18px;font-weight:800;color:#111827;line-height:1.25;margin-bottom:6px">${esc(nombre)}</div>
        <div style="font-size:13px;color:#64748b;font-weight:500">${esc(prop)}</div>
        <div style="font-size:13px;color:#64748b;font-weight:500;margin-top:2px">${ingreso} → ${salida}</div>
        <div style="font-size:12px;color:#475569;font-weight:600;margin-top:3px">🌙 ${noches} noche${noches===1?'':'s'}</div>
        ${b.GuestPhone || b.GuestEmail ? `
        <div style="display:flex;flex-wrap:wrap;gap:14px;margin-top:6px;font-size:11px;color:#64748b">
          ${b.GuestPhone ? `<span><span style="color:#94a3b8;font-weight:600">📱</span> <b style="color:#1f2937">${esc(b.GuestPhone)}</b></span>` : ''}
          ${b.GuestEmail ? `<span><span style="color:#94a3b8;font-weight:600">✉️</span> <b style="color:#1f2937">${esc(b.GuestEmail)}</b></span>` : ''}
        </div>` : ''}
      </div>
      <div style="display:flex;flex-direction:column;align-items:flex-end;gap:8px;min-width:160px">
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#a16207;font-weight:700">Ingreso bruto</div>
        <div style="font-size:22px;font-weight:800;color:#111827">${b.Gross > 0 ? lgFmtMoney(b.Gross, b.Currency) : '—'}</div>
        <div>${lgStatusBadge(b.Status)}</div>
      </div>
      <div style="display:flex;align-items:center;justify-content:center;width:38px;height:38px;border-radius:50%;border:1.5px solid ${palette.border};background:#fff;color:#475569;font-size:14px;flex-shrink:0" class="hu-record-chev">▾</div>
    </summary>`;

  // Detalle expandido
  const fldRow = (label, value) => `
    <div style="display:grid;grid-template-columns:170px 1fr;gap:10px;padding:8px 0;border-bottom:1px solid #f1f5f9">
      <div style="font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#a16207;font-weight:700;align-self:center">${esc(label)}</div>
      <div style="font-size:13px;color:#1f2937">${value || '—'}</div>
    </div>`;

  const lineItemsHtml = (b.LineItems || []).map(li => `
    <div style="display:grid;grid-template-columns:1fr auto;gap:10px;padding:6px 0;border-bottom:1px dashed #e2e8f0;font-size:12px">
      <div><b style="color:#0f172a">${esc(li.kind || '—')}</b>${li.desc ? `<span style="color:#64748b"> · ${esc(li.desc)}</span>` : ''}</div>
      <div style="font-weight:700;color:#0f766e">${lgFmtMoney(li.gross, b.Currency)}</div>
    </div>`).join('');

  const detalleBody = `
    <div style="padding:16px;background:linear-gradient(180deg,#f8fafc,#fff)">
      <div style="background:#fff;border-radius:14px;padding:14px 16px;box-shadow:0 4px 16px rgba(15,23,42,.06);border:1.5px solid #e2e8f0">
        <div style="font-size:11px;letter-spacing:.18em;color:#64748b;font-weight:800;margin-bottom:10px">📑 DETALLE DE RESERVACIÓN</div>
        ${fldRow('ID booking', `<code style="background:#f1f5f9;padding:2px 6px;border-radius:4px;font-size:11px">${esc(b.Id)}</code>`)}
        ${b.ConfirmationCode ? fldRow('Confirmation code', `<code style="background:#f1f5f9;padding:2px 6px;border-radius:4px;font-size:11px">${esc(b.ConfirmationCode)}</code>`) : ''}
        ${fldRow('Fuente', lgSourceBadge(b.Source))}
        ${fldRow('Estado', lgStatusBadge(b.Status))}
        ${fldRow('Propiedad', esc(b.HouseName))}
        ${b.RoomTypeNames ? fldRow('Tipo de habitación', esc(b.RoomTypeNames)) : ''}
        ${fldRow('Llegada', esc(b.DateArrival))}
        ${fldRow('Salida', esc(b.DateDeparture))}
        ${fldRow('# Noches', `<b>${b.Nights}</b>`)}
        ${b.DateCancelled ? fldRow('Fecha cancelación', esc(b.DateCancelled)) : ''}
        ${fldRow('Nombre del huésped', esc(b.GuestName))}
        ${b.GuestEmail ? fldRow('Correo', `<a href="mailto:${esc(b.GuestEmail)}" style="color:#0d9488">${esc(b.GuestEmail)}</a>`) : ''}
        ${b.GuestPhone ? fldRow('Teléfono', `<a href="https://wa.me/${esc(String(b.GuestPhone).replace(/\D/g,''))}" target="_blank" rel="noopener" style="color:#0d9488">${esc(b.GuestPhone)}</a>`) : ''}
        ${b.GuestCountryCode ? fldRow('País', esc(b.GuestCountryCode)) : ''}
        ${fldRow('Personas', `👥 ${b.NumberOfGuests} (Adultos: ${b.Adults}, Niños: ${b.Children}${b.Infants?`, Infantes: ${b.Infants}`:''}${b.Pets?`, Mascotas: ${b.Pets}`:''})`)}
        ${fldRow('Currency', esc(b.Currency))}
        ${fldRow('Gross / Net / VAT', `${lgFmtMoney(b.Gross, b.Currency)} / ${lgFmtMoney(b.Net, b.Currency)} / ${lgFmtMoney(b.Vat, b.Currency)}`)}
        ${b.ChannelBooking ? fldRow('Channel booking', esc(b.ChannelBooking)) : ''}
        ${b.ListingId ? fldRow('Listing ID', esc(b.ListingId)) : ''}
      </div>
      ${lineItemsHtml ? `
      <div style="background:#fff;border-radius:14px;padding:14px 16px;box-shadow:0 4px 16px rgba(15,23,42,.06);border:1.5px solid #e2e8f0;margin-top:12px">
        <div style="font-size:11px;letter-spacing:.18em;color:#64748b;font-weight:800;margin-bottom:10px">💰 LÍNEAS DE COBRO</div>
        ${lineItemsHtml}
        <div style="display:grid;grid-template-columns:1fr auto;gap:10px;padding:10px 0 0;font-size:13px;border-top:2px solid #e2e8f0;margin-top:6px">
          <div style="font-weight:800;color:#0f172a">Total</div>
          <div style="font-weight:800;color:#0f766e">${lgFmtMoney(b.Gross, b.Currency)}</div>
        </div>
      </div>` : ''}
    </div>`;

  return `
    <details class="hu-record" data-lg-id="${esc(b.Id)}" style="border:1.5px solid ${palette.border};border-radius:14px;background:#fff;overflow:hidden;box-shadow:0 1px 3px rgba(15,23,42,.04)">
      ${summary}
      ${detalleBody}
    </details>`;
}

/** Estado de estancia de un booking (mismo semáforo que huéspedes). */
function lgGetStayState(arrivalMMDD, departureMMDD) {
  const di = lgParseMMDD(arrivalMMDD);
  const ds = lgParseMMDD(departureMMDD);
  if (!di && !ds) return '';
  const today = new Date(); today.setUTCHours(0,0,0,0);
  const start = di || ds;
  const end   = ds || di;
  if (end < today) return 'concluida';
  if (end.getTime() === today.getTime()) return 'salida_hoy';
  if (start > today) return 'proxima';
  return 'activa';
}
