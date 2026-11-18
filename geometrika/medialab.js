/* Media Lab · monitor masivo de noticias.
   A) Crear proyecto (asistente): título y objetivo, términos (miles), zona, periodo y volumen, periodicidad y primera extracción.
   B) Tablero: menú izquierdo con proyectos y filtros (categoría, subcategoría, lugar, medio, fechas, entidad); a la derecha tarjetas de noticias o mapa;
      al elegir una noticia se abre su detalle en la columna lateral derecha. Se inicia con window.medialabInit(). */
(function () {
  "use strict";
  const EMB = (() => { try { return new URLSearchParams(location.search).get("emb") === "1" && window.parent !== window; } catch (_) { return false; } })();
  const MOCK = (() => { try { return new URLSearchParams(location.search).get("mock") === "1"; } catch (_) { return false; } })();
  const API = "https://api.check-inn.mx/geometrika/medialab";
  const $ = (s, r) => (r || document).querySelector(s), $$ = (s, r) => [...(r || document).querySelectorAll(s)];
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const norm = s => String(s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
  const nf = n => Number(n || 0).toLocaleString("es-MX");
  const MES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];
  const fecha = iso => { const d = new Date(iso); return isNaN(d) ? "" : d.getDate() + " " + MES[d.getMonth()] + " " + d.getFullYear() + " · " + String(d.getHours()).padStart(2, "0") + ":" + String(d.getMinutes()).padStart(2, "0"); };
  const fechaC = iso => { const d = new Date(iso); return isNaN(d) ? "" : d.getDate() + " " + MES[d.getMonth()] + " " + d.getFullYear(); };
  const hora = iso => { const d = new Date(iso); return isNaN(d) ? "" : fechaC(iso) + " · " + String(d.getHours()).padStart(2, "0") + ":" + String(d.getMinutes()).padStart(2, "0"); };
  const faltan = iso => { const ms = new Date(iso).getTime() - Date.now(); if (isNaN(ms)) return ""; if (ms <= 0) return "pendiente: se ejecutará en el próximo ciclo"; const h = ms / 36e5; return h < 24 ? "en " + Math.max(1, Math.round(h)) + " h" : "en " + Math.round(ms / 864e5) + (Math.round(ms / 864e5) === 1 ? " día" : " días"); };
  const datosProy = p => { const e = p.estado || {}, c = p.config || {}, r = e.rango;
    return [["👤", "Creado por", p.creador ? esc(p.creador) : "<i>sin registrar</i>"], ["📅", "Fecha de creación", p.creado ? esc(hora(p.creado)) : "—"], ["📰", "Noticias", nf(e.total) + " noticias"],
      ["🗓️", "Rango de las noticias", r ? esc(fechaC(r[0])) + " → " + esc(fechaC(r[1])) : "—"], ["⏱️", "Última extracción", e.ultima ? esc(hora(e.ultima)) : "aún no se ha extraído"],
      ["⏭️", "Próxima extracción", e.proxima && c.periodicidad !== "manual" ? esc(hora(e.proxima)) + " · <b>" + esc(faltan(e.proxima)) + "</b>" : "sin programar (solo manual)"]]; };
  const enCurso = p => { const w = p.estado && p.estado.trabajo; return !!((w && w.fase !== "fin") || (S.extrayendo && S.extrayendo.has(p.id))); };
  const FASES = { extraccion: "Descargando noticias", texto: "Descargando el texto de las notas", enriquecimiento: "Enriqueciendo con IA (categoría, lugar, personas, síntesis)" };
  const chipEn = p => { const w = p.estado && p.estado.trabajo; return w && w.fase !== "fin" ? `⏳ ${w.fase === "texto" ? "descargando texto" : w.fase === "enriquecimiento" ? "enriqueciendo" : "extrayendo"} ${w.fase === "enriquecimiento" && w.ia ? nf(w.ia.hecho) + "/" + nf(w.ia.hecho + (w.ia.pend || 0)) : nf(w.hecho) + "/" + nf(w.total)} · ${nf(w.nuevas)} nuevas` : "⏳ iniciando extracción…"; };
  const PER = { manual: "Solo manual", horaria: "Cada hora", "6h": "Cada 6 horas", diaria: "Diaria", semanal: "Semanal" };
  const S = { proy: [], sel: null, modo: "proyectos", pv: "card", pf: "", pq: "", vista: "lista", f: {}, data: null, det: null, prov: null, W: null, job: null, ctx: null, mapa: null, cargando: false };
  let host, tipEl;

  /* ───────── comunicación con la maqueta (columna + ventana) ───────── */
  const mlWin = o => { if (!EMB) return; const a = $("#mlApp"); if (!a) return; a.classList.toggle("ml-open", o); try { parent.postMessage({ gl: "win", open: o }, location.origin); } catch (_) { } };
  if (EMB) addEventListener("message", e => {
    if (e.origin !== location.origin || !e.data) return; const d = e.data;
    if (d.gl === "col") { document.documentElement.style.setProperty("--col", d.w + "px"); if (d.h != null) document.documentElement.style.setProperty("--tth", d.h + "px"); }
    else if (d.gl === "tema") { const a = $("#mlApp"); if (a) { a.classList.toggle("ml-light", d.v !== "osc"); temaPinta(); } else window.__mlTema = d.v; }
    else if (d.gl === "ctx") { S.ctx = d; }
  });
  const temaPinta = () => { const a = $("#mlApp"), b = $("#mlTema"); if (a && b) b.textContent = a.classList.contains("ml-light") ? "🌙 Oscuro" : "☀️ Claro"; };

  /* ───────── API ───────── */
  const MOCKDB = { n: 0 };
  const mockProy = () => MOCKDB.proy || (MOCKDB.proy = (() => { const a0 = { id: "demo1", titulo: "Agua y sequía en el noreste", objetivo: "Monitorear cobertura", creador: "Andrés Carreón", creado: "2026-11-10T16:20:00Z", config: { nTerminos: 1250, periodicidad: "diaria", proveedor: "gdelt", maxNoticias: 5000 }, estado: { total: 120, ultima: new Date().toISOString(), proxima: new Date(Date.now() + 3 * 864e5).toISOString(), rango: ["2026-08-20T05:00:00Z", "2026-11-17T22:00:00Z"], trabajo: null } }; return [Object.assign(a0, { situacion: "activo", costo: { total: 3.42, mes: 1.2 }, presupuesto: 20, visibilidad: "publico", vigencia: "2026-12-31" }), Object.assign({}, a0, { id: "demo2", titulo: "Inseguridad en Nuevo León", objetivo: "Seguimiento de seguridad y justicia", situacion: "activo", costo: { total: 8.9, mes: 4 }, presupuesto: 10, visibilidad: "privado", vigencia: "", config: Object.assign({}, a0.config, { nTerminos: 9, periodicidad: "manual", zona: { estadosN: ["Nuevo León"] } }), estado: Object.assign({}, a0.estado, { total: 391, proxima: null }) }), Object.assign({}, a0, { id: "demo3", titulo: "Reforma fiscal 2027", objetivo: "Impacto económico y reacciones", situacion: "suspendido", costo: { total: 1.1, mes: 0 }, presupuesto: 0, visibilidad: "usuarios", usuarios: [{ id: "ana@ejemplo.mx", nombre: "ana", rol: "editor" }, { id: "Lucía Ramos", nombre: "Lucía Ramos", rol: "lector" }], vigencia: "2027-03-15", creador: "Lucía Ramos", estado: Object.assign({}, a0.estado, { total: 58, proxima: null }) }), Object.assign({}, a0, { id: "demo4", titulo: "Elecciones 2024 (archivo)", objetivo: "Proyecto concluido", situacion: "inactivo", costo: { total: 14.2, mes: 0 }, presupuesto: 15, visibilidad: "privado", vigencia: "2026-11-01", estado: Object.assign({}, a0.estado, { total: 2210, proxima: null }) })]; })());
  function mockApi(p, o) {
    const j = x => Promise.resolve(Object.assign({ ok: true }, x)), m = (o && o.method) || "GET";
    const CATS = [{ c: "Política", subs: ["Gobierno"] }, { c: "Seguridad y justicia", subs: ["Delincuencia"] }, { c: "Economía y negocios", subs: ["Finanzas", "Empleo"] }, { c: "Deportes", subs: ["Futbol"] }];
    if (!MOCKDB.items) { const med = ["Milenio", "El Heraldo de México", "Imagen", "MVS Noticias", "Noticieros Televisa", "Vanguardia"], lug = [{ nivel: "municipio", est: "05", estN: "Coahuila", mun: "05030", munN: "Saltillo", lat: 25.425, lng: -101 }, { nivel: "municipio", est: "19", estN: "Nuevo León", mun: "19039", munN: "Monterrey", lat: 25.67, lng: -100.3 }, { nivel: "estado", est: "14", estN: "Jalisco", lat: 20.6, lng: -103.3 }, null];
      const IMG = i => { const g = [["#0ea5e9", "#6366f1"], ["#14b8a6", "#0ea5e9"], ["#f59e0b", "#ef4444"], ["#64748b", "#1e293b"], ["#8b5cf6", "#ec4899"]][i % 5]; return "data:image/svg+xml;utf8," + encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="320" height="200"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="${g[0]}"/><stop offset="1" stop-color="${g[1]}"/></linearGradient></defs><rect width="320" height="200" fill="url(#g)"/><g fill="none" stroke="#fff" stroke-opacity=".55" stroke-width="3"><rect x="110" y="60" width="100" height="80" rx="8"/><path d="M126 84h68M126 102h68M126 120h44"/></g></svg>`); };
      MOCKDB.items = Array.from({ length: 120 }, (_, i) => { const c = CATS[i % 4]; return { id: "n" + i, t: ["Anuncian inversión en parque industrial", "Detienen a presunto responsable de robo", "Rayados vencen en casa", "Alcalde presenta plan de agua", "Inflación se modera en octubre"][i % 5] + " #" + (i + 1), med: med[i % 6], f: new Date(Date.now() - i * 36e5 * 5).toISOString(), img: i % 4 === 3 ? "" : IMG(i), res: "Resumen de ejemplo de la nota " + (i + 1) + ": se informó sobre el tema y se detallaron las acciones anunciadas por las autoridades.", cat: c.c, sub: c.subs[i % c.subs.length], lug: lug[i % 4], ent: { pers: i % 3 ? ["Claudia Pérez"] : [], inst: ["Gobierno de Coahuila"], top: ["agua"] }, ia: 1, tx: i % 3, an: i % 2 ? { s: 4 + i % 5, r: 20 + (i * 7) % 70, e: 30 + i % 40, h: 40 + (i * 3) % 60 } : null }; }); }
    if (/^\/costos\/tarifas/.test(p)) return j({});
    if (/^\/costos/.test(p)) return j({ tarifas: { ia: { "claude-haiku-5-5": { in: 1, out: 5 }, "claude-sonnet-5": { in: 3, out: 15 }, def: { in: 3, out: 15 } }, places: 0.032, gdelt: 0, grss: 0, newsdata: 0, gnews: 0, texto: 0, gnres: 0 }, servicios: { ia: "IA · Anthropic (Claude)", places: "Google Places (búsqueda de lugares)", gdelt: "GDELT (noticias)", grss: "Google Noticias (RSS)", newsdata: "NewsData.io", gnews: "GNews", texto: "Descarga de páginas de medios", gnres: "Resolución de enlaces de Google Noticias", notas: "Notas nuevas guardadas" }, porSvc: { ia: { n: 42, in: 210000, out: 96000, k: 420, costo: 0.69 }, places: { n: 55, costo: 1.76, in: 0, out: 0, k: 0 }, grss: { n: 120, costo: 0, in: 0, out: 0, k: 0 }, texto: { n: 380, costo: 0, in: 0, out: 0, k: 0 } }, porMes: { "2026-10": { costo: 1.1, llamadas: 300, notas: 210 }, "2026-11": { costo: 2.45, llamadas: 597, notas: 462 } }, porDia: { "2026-11-16": { costo: 0.4, llamadas: 90 }, "2026-11-17": { costo: 1.2, llamadas: 300 }, "2026-11-18": { costo: 0.85, llamadas: 207 } }, porProy: { demo1: { titulo: "Agua y sequía en el noreste", costo: 2.45, llamadas: 597, notas: 462 } }, total: { costo: 2.45, llamadas: 597, in: 210000, out: 96000, notas: 462, ia_notas: 420 } });
    if (p === "/proveedores") return j({ proveedores: { gdelt: { n: "GDELT (gratis)", ok: true, nota: "Cobertura masiva sin clave." }, grss: { n: "Google Noticias (RSS)", ok: true, nota: "Gratis, 100 por consulta." }, newsdata: { n: "NewsData.io", ok: false, clave: "NEWSDATA_API_KEY", nota: "De pago." }, gnews: { n: "GNews", ok: false, clave: "GNEWS_API_KEY", nota: "De pago." } }, ia: true, geocodificar: true, categorias: CATS, estados: { "05": "Coahuila", "19": "Nuevo León", "14": "Jalisco", "09": "Ciudad de México" } });
    if (/\/situacion$/.test(p) && m === "POST") { const q0 = mockProy().find(x => x.id === p.split("/")[2]); if (q0) q0.situacion = JSON.parse((o && o.body) || "{}").situacion; return j({ proyecto: q0 }); }
    if (/\/gestion$/.test(p) && m === "POST") { const q0 = mockProy().find(x => x.id === p.split("/")[2]), b0 = JSON.parse((o && o.body) || "{}"); if (q0) Object.assign(q0, { situacion: b0.situacion, visibilidad: b0.visibilidad, usuarios: b0.usuarios, vigencia: b0.vigencia, presupuesto: b0.presupuesto }); return j({ proyecto: q0 }); }
    if (p === "/medios") return j({ medios: [{ d: "milenio.com", nom: "Milenio", n: 214 }, { d: "elheraldodemexico.com", nom: "El Heraldo de México", n: 163 }, { d: "imagen.com.mx", nom: "Imagen", n: 97 }, { d: "mvsnoticias.com", nom: "MVS Noticias", n: 64 }, { d: "noticieros.televisa.com", nom: "Noticieros Televisa", n: 51 }, { d: "elnorte.com", nom: "El Norte", n: 33 }, { d: "elsiglodetorreon.com.mx", nom: "El Siglo de Torreón", n: 12 }] });
    if (p === "/proyectos" && m === "GET") return j({ proyectos: mockProy() });
    if (p === "/estimar") return j({ terminos: 1250, excluidos: 3, grupos: 94, consultas: 94, minutos: 9, muestra: [{ q: "(agua OR sequía…)", n: 250, saturado: true }], notasPorGrupo: 180, notasEstimadas: 10000, saturado: true, advertencias: ["Se subdivide por fechas cuando una consulta se satura."] });
    if (p === "/proyectos" && m === "POST") return j({ id: "demo2", nTerminos: 10 });
    if (/\/extraer$|\/avanzar$/.test(p)) { MOCKDB.n++; const fin = MOCKDB.n % 3 === 0; return j({ avance: { fase: fin ? "fin" : "extraccion", hecho: Math.min(MOCKDB.n * 30, 94), total: 94, nuevas: MOCKDB.n * 40, errores: 0 }, proyecto: { id: "demo2", estado: { trabajo: { fase: fin ? "fin" : "extraccion", hecho: Math.min(MOCKDB.n * 30, 94), total: 94, nuevas: MOCKDB.n * 40, errores: 0 } } } }); }
    if (/\/entidad\?/.test(p)) { const nm = decodeURIComponent(p.split("n=")[1] || ""), nombres = [nm, "Gobierno de Coahuila", "Claudia Pérez", "agua", "SIPSE CANCUN", "Frente Democrático Francisco Villa", "Autopista Siglo", "Adrian Castrejón", "Hector Llorente", "Torre Chiapas", "Monumento", "Decenas", "Protegidos", "Vamos"], nodos = nombres.map((n, i) => ({ id: n.toLowerCase(), n, t: ["pers", "inst", "top"][i % 3], f: 8 - (i % 6), g: 3 + (i % 4), gp: 10 + i * 2, c: Math.round((14 - i) * 10) / 10 })), enlaces = []; nodos.forEach((a, i) => { if (i) enlaces.push({ a: nodos[0].id, b: a.id, w: 1 + (i % 4) }); if (i > 2 && i % 2) enlaces.push({ a: nodos[i - 2].id, b: a.id, w: 1 + (i % 3) }); }); return j({ entidad: { id: nodos[0].id, n: nm, t: "inst", notas: 11 }, paso: "semana", serie: [["2026-09-01", 1], ["2026-09-08", 3], ["2026-09-15", 2], ["2026-09-22", 5], ["2026-10-06", 4], ["2026-10-20", 7], ["2026-11-03", 3]].map(([f, n]) => ({ f, n })), nodos, enlaces, recientes: MOCKDB.items.slice(0, 3).map(n => ({ id: n.id, t: n.t, med: n.med, f: n.f })) }); }
    if (/\/noticia\//.test(p)) { const id = p.split("/").pop(), n = MOCKDB.items.find(x => x.id === id); return j({ noticia: Object.assign({}, n, { ind: { afe: { esc: 5, escN: "Ciudad o municipio", rango: "100 mil a 1 millón", tipo: "gremial", tipoN: "Gremio o sindicato", grupo: "Maestros de Coahuila", rol: 4, rolN: "Servicio o sector clave", act: 4, actN: "Nacional · alta influencia", actn: "SNTE", rel: 78, relN: "Alta" }, sent: 6, pol: 3, pob: 235, alc: "municipal", expo: 57.7, haz: 100, risk: 57.7, grav: { pct: 150, filas: [{ nom: "Lesiones", w: 3, c: 3, cw: 9 }, { nom: "Armas/obj. punzocortantes", w: 2, c: 0, cw: 0 }, { nom: "Infantes involucrados", w: 1, c: 0, cw: 0 }] }, viol: { pct: 106.7, filas: [{ nom: "Pérdidas humanas", w: 5, c: 0, cw: 0 }, { nom: "Lesionados con armas de fuego", w: 4, c: 4, cw: 16 }, { nom: "Lesionados con armas blancas", w: 3, c: 0, cw: 0 }, { nom: "Agresiones físicas leves", w: 2, c: 0, cw: 0 }, { nom: "Agresiones verbales", w: 1, c: 0, cw: 0 }] }, scian: [{ c: "22", n: "Generación, transmisión y distribución de energía eléctrica; suministro de agua y de gas por ductos al consumidor final" }, { c: "23", n: "Construcción" }], riesgos: ["físico", "legal"] },  url: "https://example.com/nota", texto: "Texto de ejemplo de la nota.\n\nSegundo párrafo con más detalles de lo ocurrido en la zona." }) }); }
    if (/\/noticias/.test(p)) { const q = new URLSearchParams(p.split("?")[1] || ""); let L = MOCKDB.items.filter(n => (!q.get("cat") || n.cat === q.get("cat")) && (!q.get("est") || (n.lug && n.lug.est === q.get("est"))) && (!q.get("q") || norm(n.t).includes(norm(q.get("q")))) && (q.get("geo") !== "1" || (n.lug && n.lug.lat != null)));
      if (q.get("geo") === "1") return j({ total: L.length, puntos: L.map(n => ({ id: n.id, t: n.t, lat: n.lug.lat, lng: n.lug.lng, niv: n.lug.nivel, lug: (n.lug.munN ? n.lug.munN + ", " : "") + n.lug.estN, med: n.med, f: n.f, cat: n.cat, r: (n.an && n.an.r) != null ? n.an.r : (n.t.length * 7) % 100 })) });
      const cu = f => { const m = new Map(); MOCKDB.items.forEach(n => { const v = f(n); (Array.isArray(v) ? v : [v]).forEach(x => x && m.set(x, (m.get(x) || 0) + 1)); }); return [...m].map(([v, n]) => ({ v, n })); };
      return j({ total: L.length, de: 120, items: L.slice(0, 40), facetas: { cat: cu(n => n.cat), sub: cu(n => n.sub), est: cu(n => n.lug && n.lug.est).map(x => ({ ...x, nom: { "05": "Coahuila", "19": "Nuevo León", "14": "Jalisco" }[x.v] })), mun: [{ v: "05030", n: 20, nom: "Saltillo, Coahuila" }, { v: "19039", n: 20, nom: "Monterrey, Nuevo León" }], med: cu(n => n.med), pers: cu(n => n.ent.pers), inst: cu(n => n.ent.inst), top: cu(n => n.ent.top) } }); }
    return j({});
  }
  async function api(p, o) {
    if (MOCK) return mockApi(p, o);
    const r = await fetch(API + p, Object.assign({ headers: { "Content-Type": "application/json" } }, o || {})), j = await r.json().catch(() => ({ ok: false, error: "Respuesta no válida del servidor (" + r.status + ")" }));
    if (!j.ok) throw new Error(j.error || "Error " + r.status); return j;
  }
  const post = (p, b) => api(p, { method: "POST", body: JSON.stringify(b || {}) });
  const toast = (t, err) => { let e = $("#mlToast"); if (!e) { e = document.createElement("div"); e.id = "mlToast"; e.style.cssText = "position:fixed;left:50%;bottom:24px;transform:translateX(-50%);z-index:99;padding:10px 16px;border-radius:12px;font:600 13px system-ui;box-shadow:0 8px 24px rgba(0,0,0,.4);max-width:90vw"; document.body.appendChild(e); } e.style.background = err ? "#be123c" : "#0f766e"; e.style.color = "#fff"; e.textContent = t; e.style.display = ""; clearTimeout(toast.t); toast.t = setTimeout(() => { e.style.display = "none"; }, err ? 7000 : 3500); };

  /* ───────── estructura ───────── */
  function shell() {
    host.innerHTML = `<div class="ml-app${EMB ? " ml-emb" : ""}${window.__mlTema === "osc" ? "" : " ml-light"}" id="mlApp">
      <header class="ml-top"><button class="ml-ham" id="mlHam" type="button" aria-label="Mostrar u ocultar el menú" title="Mostrar u ocultar el menú"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M4 12h16M4 17h16"/></svg></button><span class="ml-sp"></span><button class="ml-theme" id="mlTema" type="button" title="Cambiar tema (aplica a GeoLab, EconoLab, DataLab y Media Lab)">🌙 Oscuro</button></header>
      <div class="ml-body"><aside class="ml-side" id="mlSide"></aside>
        <div class="ml-wbar"><b id="mlWT">Media Lab</b><button class="ml-wx" id="mlWX" type="button" aria-label="Cerrar ventana" title="Cerrar">✕</button></div>
        <main class="ml-main" id="mlMain"></main></div></div>`;
    const app = $("#mlApp"); try { if (localStorage.getItem("ml_side") === "0") app.classList.add("ml-sidehide"); } catch (_) { } if (window.__mlTema) app.classList.toggle("ml-light", window.__mlTema !== "osc"); if (!EMB && !window.__mlTema) { let g = null; try { g = localStorage.getItem("ml_tema"); } catch (_) { } app.classList.toggle("ml-light", g !== "osc"); }
    temaPinta();
    $("#mlTema").onclick = () => { const claro = app.classList.contains("ml-light"); if (EMB) { try { parent.postMessage({ gl: "tema", v: claro ? "osc" : "claro" }, location.origin); } catch (_) { } } else { app.classList.toggle("ml-light"); try { localStorage.setItem("ml_tema", app.classList.contains("ml-light") ? "claro" : "osc"); } catch (_) { } temaPinta(); } };
    $("#mlWX").onclick = () => mlWin(false); document.addEventListener("keydown", e => { if (e.key === "Escape") { if (S.det) cierraDet(); else mlWin(false); } });
    tipEl = null; host.onclick = clic; host.oninput = entrada; host.onchange = cambio;
  }
  const titulo = t => { const w = $("#mlWT"); if (w) w.textContent = t; };

  /* ───────── menú izquierdo ───────── */
  /* ───────── sección Proyectos: alta, listado a pantalla completa y situación (activo / suspendido / inactivo) ───────── */
  const SIT = { activo: ["Activo", "#16a34a", "Extracción programada y manual"], suspendido: ["Suspendido", "#d97706", "Sin extracción programada; se puede extraer a mano"], inactivo: ["Inactivo", "#64748b", "Archivado: solo consulta, sin extracciones"] };
  const sitDe = p => SIT[p.situacion] ? p.situacion : "activo";
  const sitBadge = p => { const k = sitDe(p); return `<span class="ml-sit s-${k}" title="${esc(SIT[k][2])}"><i></i>${SIT[k][0]}</span>`; };
  const navHtml = () => `<div class="ml-seg ml-nav"><button type="button" data-go="proyectos" class="${S.modo === "proyectos" ? "on" : ""}">📁 Proyectos</button><button type="button" data-go="tablero" class="${S.modo === "tablero" ? "on" : ""}">📰 Tablero</button><button type="button" data-go="costos" class="${S.modo === "costos" ? "on" : ""}">💰 Costos</button></div>`;
  function proyectosVista() {
    const m = $("#mlMain"); if (!m) return; const L = S.proy, q = (S.pq || "").trim().toLowerCase(), ak = document.activeElement && document.activeElement.id === "mlPQ" ? document.activeElement.selectionStart : null;
    const cnt = k => L.filter(p => sitDe(p) === k).length, arr = L.filter(p => (!S.pf || sitDe(p) === S.pf) && (!q || (p.titulo + " " + (p.objetivo || "") + " " + (p.creador || "")).toLowerCase().includes(q)));
    const zona = p => { const z = p.config && p.config.zona; return z && z.estadosN && z.estadosN.length ? z.estadosN.join(", ") : "Todo México"; };
    const per = p => PER[p.config && p.config.periodicidad] || "Manual", puede = p => sitDe(p) !== "inactivo" && !enCurso(p);
    const segSit = p => `<div class="ml-sseg" title="Situación del proyecto">${["activo", "suspendido", "inactivo"].map(k => `<button type="button" data-sit="${esc(p.id)}|${k}" class="s-${k}${sitDe(p) === k ? " on" : ""}" title="${esc(SIT[k][2])}">${SIT[k][0]}</button>`).join("")}</div>`;
    const kpi = [["", "Todos", L.length, "#2563eb"], ["activo", "Activos", cnt("activo"), SIT.activo[1]], ["suspendido", "Suspendidos", cnt("suspendido"), SIT.suspendido[1]], ["inactivo", "Inactivos", cnt("inactivo"), SIT.inactivo[1]]];
    let h = `<div class="ml-pv"><div class="ml-pvh"><span class="ml-sp"></span><button class="ml-btn" type="button" data-go="crear">➕ Nuevo proyecto</button></div>
      <div class="ml-pvt"><h2>Proyectos de monitoreo</h2><p>Da de alta los proyectos, consulta todos en un solo lugar y define su situación: <b>activo</b> (se extrae solo y a mano), <b>suspendido</b> (solo a mano) o <b>inactivo</b> (archivado, solo consulta).</p></div>
      <div class="ml-pvk">${kpi.map(([k, t, n, c]) => `<button type="button" data-pf="${k}" class="${(S.pf || "") === k ? "on" : ""}" style="--c:${c}"><b>${nf(n)}</b><span>${t}</span></button>`).join("")}</div>
      <div class="ml-pvb"><div class="ml-f" style="margin:0;flex:1;min-width:200px"><input type="search" id="mlPQ" placeholder="Buscar proyecto, objetivo o creador…" value="${esc(S.pq || "")}"></div><div class="ml-seg" style="margin:0;width:200px"><button type="button" data-pv="card" class="${S.pv !== "tabla" ? "on" : ""}">▦ Tarjetas</button><button type="button" data-pv="tabla" class="${S.pv === "tabla" ? "on" : ""}">☰ Tabla</button></div></div>`;
    if (!arr.length) h += `<div class="ml-vacio">${L.length ? "Ningún proyecto coincide con el filtro." : "Aún no hay proyectos.<br>Crea el primero con «Nuevo proyecto»."}</div>`;
    else if (S.pv === "tabla") h += `<div class="ml-pt"><table><thead><tr><th>Proyecto</th><th>Situación</th><th>Acceso</th><th>Noticias</th><th>Términos</th><th>Periodicidad</th><th>Ámbito</th><th>Última extracción</th><th>Próxima</th><th>Vigencia</th><th>Costo / presupuesto</th><th>Creado por</th><th></th></tr></thead><tbody>${arr.map(p => `<tr class="s-${sitDe(p)}"><td><a href="#" data-abrir="${esc(p.id)}"><b>${esc(p.titulo)}</b></a><small>${esc(p.objetivo || "")}</small></td><td><select data-sitsel="${esc(p.id)}" class="s-${sitDe(p)}">${["activo", "suspendido", "inactivo"].map(k => `<option value="${k}"${sitDe(p) === k ? " selected" : ""}>${SIT[k][0]}</option>`).join("")}</select></td><td>${visBadge(p)}</td><td class="n">${nf(p.estado && p.estado.total)}</td><td class="n">${nf(p.config && p.config.nTerminos)}</td><td>${esc(per(p))}</td><td>${esc(zona(p))}</td><td>${p.estado && p.estado.ultima ? esc(fechaC(p.estado.ultima)) : "—"}</td><td>${p.estado && p.estado.proxima ? esc(fechaC(p.estado.proxima)) : "—"}</td><td>${vigChip(p)}</td><td>${costoHtml(p)}</td><td>${p.creador ? esc(p.creador) : "—"}</td><td class="ac"><button type="button" data-abrir="${esc(p.id)}" title="Abrir tablero">📰</button><button type="button" data-gest="${esc(p.id)}" title="Gestionar">⚙️</button><button type="button" data-ed="${esc(p.id)}" title="Editar">✏️</button>${puede(p) ? `<button type="button" data-ex="${esc(p.id)}" title="Extraer lo nuevo">↻</button>` : ""}<button type="button" data-del="${esc(p.id)}" title="Eliminar">🗑</button></td></tr>`).join("")}</tbody></table></div>`;
    else h += `<div class="ml-pg">${arr.map(p => `<div class="ml-pc s-${sitDe(p)}"><div class="hd">${sitBadge(p)}${visBadge(p)}<span class="pe">${esc(per(p))}${enCurso(p) ? " · ⏳ extrayendo" : ""}</span></div><h3><a href="#" data-abrir="${esc(p.id)}">${esc(p.titulo)}</a></h3><p class="ob">${esc(p.objetivo || "Sin objetivo definido")}</p>
      <div class="mx"><div><b>${nf(p.estado && p.estado.total)}</b><span>noticias</span></div><div><b>${nf(p.config && p.config.nTerminos)}</b><span>términos</span></div><div><b>${p.estado && p.estado.ultima ? esc(fechaC(p.estado.ultima)) : "—"}</b><span>última extracción</span></div></div>
      <div class="mt"><span>📍 ${esc(zona(p))}</span><span>👤 ${p.creador ? esc(p.creador) : "sin registrar"}</span><span>📅 creado ${p.creado ? esc(fechaC(p.creado)) : "—"}</span><span>⏭ próxima ${p.estado && p.estado.proxima ? esc(fechaC(p.estado.proxima)) : "—"}</span></div>
      ${costoHtml(p)}${segSit(p)}<div class="ac"><button class="ml-btn" type="button" data-abrir="${esc(p.id)}">📰 Abrir tablero</button><button class="ml-btn2" type="button" data-gest="${esc(p.id)}">⚙️ Gestionar</button><button class="ml-btn2" type="button" data-ed="${esc(p.id)}">✏️ Editar</button>${puede(p) ? `<button class="ml-btn2" type="button" data-ex="${esc(p.id)}" title="Trae solo lo nuevo">↻ Extraer</button>` : ""}<button class="ml-btn2" type="button" data-del="${esc(p.id)}" title="Eliminar">🗑</button></div></div>`).join("")}</div>`;
    m.innerHTML = h + `</div>`;
    if (ak !== null) { const q2 = $("#mlPQ"); if (q2) { q2.focus(); try { q2.setSelectionRange(ak, ak); } catch (_) { } } }
  }
  /* ───────── gestión del proyecto: situación, visibilidad y usuarios, vigencia, presupuesto y costo ───────── */
  const VIS = { publico: ["🌐 Público", "Cualquier persona con acceso a Media Lab puede consultarlo."], privado: ["🔒 Privado", "Solo el creador y los administradores."], usuarios: ["👥 Usuarios", "Solo las personas que elijas, según su rol."] };
  const ROLES = { lector: ["Lector", "Consulta noticias y tablero"], editor: ["Editor", "Consulta y extrae noticias"], administrador: ["Administrador", "Además gestiona el proyecto"] };
  const visDe = p => VIS[p.visibilidad] ? p.visibilidad : "publico";
  const visBadge = p => { const k = visDe(p); return `<span class="ml-vis v-${k}" title="${esc(VIS[k][1])}">${VIS[k][0]}${k === "usuarios" ? " (" + ((p.usuarios || []).length) + ")" : ""}</span>`; };
  const diasA = f => { const d = new Date(f + "T12:00:00"), h = new Date(); h.setHours(12, 0, 0, 0); return Math.round((d - h) / 864e5); };
  const vigChip = p => { if (!p.vigencia) return `<span class="ml-vg">♾ Sin vigencia</span>`; const d = diasA(p.vigencia); return d < 0 ? `<span class="ml-vg bad" title="El proyecto pasó a inactivo por vigencia">⌛ Venció el ${esc(fechaC(p.vigencia + "T12:00:00"))}</span>` : `<span class="ml-vg${d <= 15 ? " warn" : ""}">⏳ ${d === 0 ? "Vence hoy" : "Vigente hasta " + esc(fechaC(p.vigencia + "T12:00:00")) + " · " + d + " d"}</span>`; };
  const costoHtml = p => { const c = (p.costo && p.costo.total) || 0, pr = +p.presupuesto || 0, pc = pr ? Math.round(c / pr * 100) : null;
    return `<div class="ml-cst"><div class="r"><span>💵 Costo <b>${USD(c)}</b>${p.costo && p.costo.mes ? ` <em>· este mes ${USD(p.costo.mes)}</em>` : ""}</span><span>${pr ? `Presupuesto <b>${USD(pr)}</b>` : "<em>Sin presupuesto</em>"}</span></div>${pr ? `<div class="bar${pc >= 100 ? " x" : pc >= 80 ? " w" : ""}"><i style="width:${Math.min(100, pc)}%"></i></div><small>${pc} % del presupuesto${pc >= 100 ? " · excedido" : pc >= 80 ? " · cerca del límite" : ""}</small>` : ""}${vigChip(p)}</div>`; };
  function gxCaptura() { const g = S.gx; if (!g) return; const v = $("#mlGxV"), pr = $("#mlGxP"); if (v) g.vigencia = v.value; if (pr) g.presupuesto = pr.value; }
  function abreGestion(id) { const p = S.proy.find(x => x.id === id); if (!p) return; S.gx = { id, sit: sitDe(p), vis: visDe(p), usuarios: (p.usuarios || []).map(u => Object.assign({}, u)), vigencia: p.vigencia || "", presupuesto: p.presupuesto || "" }; pintaGestion(); }
  function pintaGestion() {
    const g = S.gx, a = $("#mlApp"); if (!g || !a) return; const p = S.proy.find(x => x.id === g.id); if (!p) return; let m = $("#mlGx"); if (!m) { m = document.createElement("div"); m.id = "mlGx"; m.className = "ml-modal"; a.appendChild(m); }
    const c = (p.costo && p.costo.total) || 0, pr = +g.presupuesto || 0, pc = pr ? Math.round(c / pr * 100) : null;
    m.innerHTML = `<div class="ml-mb" data-gx="fuera"><div class="ml-mc"><header><div><h3>⚙️ Gestionar proyecto</h3><small>${esc(p.titulo)}</small></div><button type="button" data-gx="cancel" aria-label="Cerrar">✕</button></header>
      <div class="ml-mbody">
      <section><h4>Situación</h4><div class="ml-sseg">${["activo", "suspendido", "inactivo"].map(k => `<button type="button" data-gx="sit|${k}" class="s-${k}${g.sit === k ? " on" : ""}">${SIT[k][0]}</button>`).join("")}</div><p class="h">${esc(SIT[g.sit][2])}.</p></section>
      <section><h4>Visibilidad y acceso</h4><div class="ml-vseg">${Object.keys(VIS).map(k => `<button type="button" data-gx="vis|${k}" class="${g.vis === k ? "on" : ""}">${VIS[k][0]}</button>`).join("")}</div><p class="h">${esc(VIS[g.vis][1])}</p>
      ${g.vis === "usuarios" ? `<div class="ml-gu">${g.usuarios.length ? g.usuarios.map((u, i) => `<div class="u"><span class="av">${esc((u.nombre || u.id || "?").charAt(0).toUpperCase())}</span><div><b>${esc(u.nombre || u.id)}</b>${u.nombre && u.nombre !== u.id ? `<small>${esc(u.id)}</small>` : ""}</div><select data-gxrol="${i}">${Object.keys(ROLES).map(k => `<option value="${k}"${u.rol === k ? " selected" : ""}>${ROLES[k][0]}</option>`).join("")}</select><button type="button" data-gx="del|${i}" title="Quitar">✕</button></div>`).join("") : `<div class="ml-vacio" style="padding:8px">Aún no hay usuarios con acceso.</div>`}
        <div class="add"><input type="text" id="mlGxU" placeholder="Nombre o correo de la persona"><select id="mlGxR">${Object.keys(ROLES).map(k => `<option value="${k}">${ROLES[k][0]}</option>`).join("")}</select><button class="ml-btn2" type="button" data-gx="add">➕ Agregar</button></div>
        <p class="h">${Object.keys(ROLES).map(k => `<b>${ROLES[k][0]}</b>: ${ROLES[k][1].toLowerCase()}`).join(" · ")}</p></div>` : ""}
      <p class="h aviso">ℹ️ Se guarda quién puede ver cada proyecto. Hoy Media Lab entra con contraseña compartida y no distingue personas: el filtrado por usuario se aplicará cuando el acceso sea individual.</p></section>
      <section><h4>Vigencia</h4><div class="ml-gr"><input type="date" id="mlGxV" value="${esc(g.vigencia || "")}"><button class="ml-btn2" type="button" data-gx="sinvig">Sin vigencia</button></div><p class="h">A partir de esta fecha el proyecto pasa solo a <b>inactivo</b> y deja de extraer noticias.</p></section>
      <section><h4>Presupuesto y costos</h4><div class="ml-gr"><span class="pre">US$</span><input type="number" min="0" step="0.5" id="mlGxP" placeholder="0.00 (sin presupuesto)" value="${esc(g.presupuesto === "" ? "" : g.presupuesto)}"></div>
        <div class="ml-gk"><div><b>${USD(c)}</b><span>costo incurrido</span></div><div><b>${USD((p.costo && p.costo.mes) || 0)}</b><span>este mes</span></div><div><b>${pr ? pc + " %" : "—"}</b><span>del presupuesto</span></div></div>
        ${pr ? `<div class="ml-cst"><div class="bar${pc >= 100 ? " x" : pc >= 80 ? " w" : ""}"><i style="width:${Math.min(100, pc)}%"></i></div></div>` : ""}<p class="h">El costo suma IA, búsquedas de lugares y demás servicios de pago registrados para este proyecto.</p></section>
      </div><footer><button class="ml-btn2" type="button" data-gx="cancel">Cancelar</button><button class="ml-btn" type="button" data-gx="save">Guardar cambios</button></footer></div></div>`;
  }
  function cierraGestion() { const m = $("#mlGx"); if (m) m.remove(); S.gx = null; }
  async function guardaGestion() {
    gxCaptura(); const g = S.gx; if (!g) return; const p = S.proy.find(x => x.id === g.id);
    if (g.sit === "inactivo" && sitDe(p) !== "inactivo" && !confirm("¿Pasar el proyecto a inactivo? Se detiene cualquier extracción en curso y no se podrá extraer hasta reactivarlo.")) return;
    try { await post(`/proyectos/${g.id}/gestion`, { situacion: g.sit, visibilidad: g.vis, usuarios: g.usuarios, vigencia: g.vigencia || "", presupuesto: g.presupuesto === "" ? 0 : +g.presupuesto }); await cargaProy(); cierraGestion(); toast("Proyecto actualizado"); lado(); principal(); } catch (e) { toast(e.message, true); }
  }
  function gxAccion(k) {
    const g = S.gx; if (!g) return; gxCaptura(); const [a, v] = k.split("|");
    if (a === "cancel") { cierraGestion(); return; } if (a === "fuera") return; if (a === "save") { guardaGestion(); return; }
    if (a === "sit") g.sit = v; else if (a === "vis") g.vis = v; else if (a === "sinvig") g.vigencia = ""; else if (a === "del") g.usuarios.splice(+v, 1);
    else if (a === "add") { const i = $("#mlGxU"), r = $("#mlGxR"), t = (i && i.value || "").trim(); if (!t) { toast("Escribe el nombre o correo de la persona", true); return; } if (g.usuarios.some(u => u.id.toLowerCase() === t.toLowerCase())) { toast("Esa persona ya está en la lista", true); return; } g.usuarios.push({ id: t, nombre: t.includes("@") ? t.split("@")[0] : t, rol: r ? r.value : "lector" }); }
    pintaGestion();
  }
  async function cambiaSit(id, v) {
    const p = S.proy.find(x => x.id === id); if (!p || sitDe(p) === v) { proyectosVista(); return; }
    if (v === "inactivo" && !confirm("¿Pasar «" + p.titulo + "» a inactivo? Se detiene cualquier extracción en curso y no se podrá extraer hasta reactivarlo; las noticias se conservan.")) { proyectosVista(); return; }
    try { await post(`/proyectos/${id}/situacion`, { situacion: v }); await cargaProy(); toast("«" + p.titulo + "»: " + SIT[v][0].toLowerCase()); } catch (e) { toast(e.message, true); }
    lado(); principal();
  }
  /* menú lateral jerárquico: secciones y subsecciones (se oculta y muestra con el botón ☰) */
  function pjCard(p) {
    const w = p.estado && p.estado.trabajo, en = enCurso(p);
    return `<div class="ml-pj on" data-pj="${p.id}"><small>${nf(p.config && p.config.nTerminos)} términos · ${esc(PER[p.config && p.config.periodicidad] || "")}</small><div class="ml-fp ml-fpc"><div><span>📰 Noticias</span><b>${nf(p.estado && p.estado.total)}</b></div><div><span>⏱ Última extracción</span><b>${p.estado && p.estado.ultima ? esc(fechaC(p.estado.ultima)) : "—"}</b></div>${p.vigencia ? `<div><span>⏳ Vigencia</span><b>${esc(fechaC(p.vigencia + "T12:00:00"))}</b></div>` : ""}</div>${en ? `<span class="ml-chip run" data-chip="${p.id}">${chipEn(p)}</span>` : p.estado && p.estado.ultima ? `<span class="ml-chip ok">✓ ${esc(fechaC(p.estado.ultima))}</span>` : `<span class="ml-chip">sin extraer</span>`}${w && w.ultimoError && !en ? `<span class="ml-chip err" title="${esc(w.ultimoError)}">avisos</span>` : ""}
        <div class="ml-acc"><button type="button" data-gest="${p.id}" title="Situación, acceso, vigencia y presupuesto">⚙️</button><button type="button" data-ed="${p.id}">✏️ Editar</button>${en || sitDe(p) === "inactivo" ? "" : `<button type="button" data-ex="${p.id}" title="Trae solo lo nuevo desde la última extracción (o todo el periodo si el proyecto está vacío)">↻ Extraer</button><button type="button" data-exall="${p.id}" title="Vuelve a recorrer todo el periodo configurado">⟲ Todo</button>`}</div></div>`;
  }
  function treeHtml() {
    const L = S.proy, C = S.navC || {}, cnt = k => L.filter(p => sitDe(p) === k).length;
    const it = (nav, ico, lab, extra, on, cls) => `<a href="#" class="ml-ni${on ? " on" : ""}${cls ? " " + cls : ""}" data-nav="${esc(nav)}"><span class="i">${ico}</span><span class="t">${lab}</span>${extra !== null && extra !== undefined ? `<em>${extra}</em>` : ""}</a>`;
    const grp = (k, ico, lab, on, body) => `<div class="ml-ng${C[k] ? " c" : ""}"><div class="ml-nh${on ? " on" : ""}"><a href="#" class="ml-nhl" data-nav="g|${k}"><span class="i">${ico}</span><span class="t">${lab}</span></a><button type="button" class="ml-nc" data-navc="${k}" aria-label="Plegar o desplegar">▾</button></div><div class="ml-nb">${body}</div></div>`;
    const pm = S.modo === "proyectos" || S.modo === "crear", f = S.pf || "";
    const dot = k => `<span class="dot s-${k}"></span>`;
    const bProy = it("proy|", "▦", "Todos los proyectos", L.length, S.modo === "proyectos" && !f) + ["activo", "suspendido", "inactivo"].map(k => it("proy|" + k, dot(k), SIT[k][0] + "s", cnt(k), S.modo === "proyectos" && f === k, "sub")).join("") + it("nuevo", "➕", "Nuevo proyecto", null, S.modo === "crear", "nuevo");
    const bTab = L.length ? L.map(p => { const sel = p.id === S.sel, on = sel && S.modo === "tablero";
      return it("tab|" + p.id + "|" + (S.vista || "lista"), dot(sitDe(p)), esc(p.titulo), null, on && false, "pj" + (sel ? " sel" : "") + (sitDe(p) === "inactivo" ? " ina" : "")) + (sel ? `<div class="ml-nsub">${it("tab|" + p.id + "|lista", "📰", "Noticias", null, on && S.vista !== "mapa", "sub")}${it("tab|" + p.id + "|mapa", "🗺️", "Mapa", null, on && S.vista === "mapa", "sub")}${pjCard(p)}</div>` : ""); }).join("") : `<div class="ml-nv">Aún no hay proyectos.</div>`;
    const co = S.modo === "costos", bCos = it("cos|top", "📊", "Resumen de gasto", null, co, "sub") + it("cos|mlCosLim", "🎯", "Límite mensual", null, false, "sub") + it("cos|mlCosProy", "🗂️", "Gasto por proyecto", null, false, "sub") + it("cos|mlCosTar", "🏷️", "Tarifas", null, false, "sub");
    return `<nav class="ml-tree" aria-label="Secciones de Media Lab">${grp("proy", "📁", "Proyectos", pm, bProy)}${grp("tab", "📰", "Tablero", S.modo === "tablero", bTab)}${grp("cos", "💰", "Costos", co, bCos)}</nav>`;
  }
  function navTo(v) {
    const [a, b, c] = v.split("|");
    if (a === "g") { if (b === "proy") navTo("proy|" + (S.pf || "")); else if (b === "tab") { const id = S.sel || (S.proy[0] && S.proy[0].id); if (id) navTo("tab|" + id + "|" + (S.vista || "lista")); else navTo("proy|"); } else navTo("cos|top"); return; }
    if (a === "proy") { S.pf = b || ""; S.modo = "proyectos"; S.det = null; titulo("Proyectos"); lado(); principal(); }
    else if (a === "nuevo") nuevo();
    else if (a === "tab") { S.vista = c || "lista"; if (S.sel !== b) selecciona(b); else { S.modo = "tablero"; S.det = null; titulo((S.proy.find(x => x.id === b) || {}).titulo || "Media Lab"); lado(); principal(); consulta(); } }
    else if (a === "cos") { const go2 = () => { const el = b === "top" ? $("#mlMain") : $("#" + b); if (el) el.scrollIntoView({ behavior: "smooth", block: "start" }); }; if (S.modo !== "costos") abreCostos().then(() => setTimeout(go2, 200)); else go2(); }
  }
  function lado() {
    const side = $("#mlSide"); if (!side) return;
    const sc = side.scrollTop; side.innerHTML = treeHtml(); side.scrollTop = sc;
  }
  const opt = (arr, val, vf, tf) => `<option value="">Todas</option>` + (arr || []).map(x => `<option value="${esc(vf ? vf(x) : x.v)}"${String(vf ? vf(x) : x.v) === String(val || "") ? " selected" : ""}>${esc(tf ? tf(x) : x.v)} (${nf(x.n)})</option>`).join("");
  function filtros() {
    const F = S.data.facetas || {}, f = S.f, ents = [["Personas", "pers", F.pers], ["Instituciones", "inst", F.inst], ["Tópicos", "top", F.top]];
    const eo = ents.map(([t, k, L]) => (L && L.length) ? `<optgroup label="${t}">` + L.map(x => `<option value="${k}:${esc(x.v)}"${f.ent === k + ":" + x.v ? " selected" : ""}>${esc(x.v)} (${nf(x.n)})</option>`).join("") + `</optgroup>` : "").join("");
    const act = Object.keys(f).filter(k => f[k]).length;
    return `<details class="ml-ff"${S.ffC ? "" : " open"}><summary><span>${ic("bar")}Filtros</span><em>${nf(S.data.total)} de ${nf(S.data.de)} noticias${act ? ` · ${act} activo${act > 1 ? "s" : ""}` : ""}</em><button class="ml-btn2" type="button" data-go="limpiar">Limpiar filtros</button></summary><div class="ml-ffg">
      <div class="ml-f"><label>Buscar en título y resumen</label><input type="search" id="mlQ" placeholder="Palabra o frase…" value="${esc(f.q || "")}"></div>
      <div class="ml-f"><label>Categoría</label><select data-f="cat">${opt(F.cat, f.cat)}</select></div>
      <div class="ml-f"><label>Subcategoría</label><select data-f="sub">${opt(F.sub, f.sub)}</select></div>
      <div class="ml-f"><label>Lugar · zona del país</label><select data-f="reg">${opt(F.reg, f.reg)}</select></div>
      <div class="ml-f"><label>Lugar · estado</label><select data-f="est">${opt(F.est, f.est, null, x => x.nom || x.v)}</select></div>
      <div class="ml-f"><label>Lugar · ciudad o municipio</label><select data-f="mun">${opt(F.mun, f.mun, null, x => x.nom || x.v)}</select></div>
      ${(F.zona && F.zona.length) ? `<div class="ml-f"><label>Lugar · colonia o zona</label><select data-f="zona">${opt(F.zona, f.zona)}</select></div>` : ""}
      <div class="ml-f"><label>Medio</label><select data-f="med">${opt(F.med, f.med)}</select></div>
      <div class="ml-f"><label>Texto de la nota</label><select data-f="tx"><option value="">Todas</option>${[["1", "📄 Con texto"], ["2", "🔒 Sin texto (no accesible)"], ["0", "⏳ Texto pendiente"]].map(([v, t]) => `<option value="${v}"${f.tx === v ? " selected" : ""}>${t} (${nf(((F.tx || []).find(x => x.v === v) || {}).n || 0)})</option>`).join("")}</select></div>
      <div class="ml-f"><label>Fecha desde</label><input type="date" data-f="desde" value="${esc(f.desde || "")}"></div><div class="ml-f"><label>Fecha hasta</label><input type="date" data-f="hasta" value="${esc(f.hasta || "")}"></div>
      <div class="ml-f"><label>Entidad (personas, instituciones, tópicos)</label><select data-f="ent"><option value="">Todas</option>${eo}</select></div>
    </div></details>`;
  }

  /* ───────── tablero ───────── */
  const qs = () => { const f = S.f, p = new URLSearchParams(); Object.keys(f).forEach(k => { if (f[k]) p.set(k, f[k]); }); return p; };
  async function consulta(mas) {
    if (!S.sel) return; if (S.cargando) return; S.cargando = true;
    try {
      const p = qs(); p.set("n", "40"); p.set("pag", mas ? String(S.pag + 1) : "0"); const rg = S.vista === "mapa" && !mas ? mapaDatos() : null, r = await api(`/proyectos/${S.sel}/noticias?${p}`);
      if (mas && S.data) { S.pag++; S.data.items = S.data.items.concat(r.items); S.data.total = r.total; } else { S.pag = 0; S.data = r; } if (rg) await rg;
    } catch (e) { toast(e.message, true); } finally { S.cargando = false; }
    lado(); principal();
  }
  async function selecciona(id) {
    S.sel = id; S.modo = "tablero"; S.f = {}; S.data = null; S.det = null; S.pag = 0; const p = S.proy.find(x => x.id === id); titulo(p ? p.titulo : "Media Lab"); lado(); mlWin(true); principal(); await consulta();
  }
  /* carga automática: al llegar al final de la lista se pide el siguiente grupo de noticias */
  let ioMasObs = null;
  let scMas = 0;
  function chequeaMas() { if (scMas) return; scMas = setTimeout(() => { scMas = 0; const el = $(".ml-more"); if (el && !S.cargando && el.getBoundingClientRect().top < innerHeight + 500) consulta(true); }, 80); }
  addEventListener("scroll", chequeaMas, { capture: true, passive: true }); addEventListener("resize", chequeaMas);
  function ioMas() {
    if (ioMasObs) { ioMasObs.disconnect(); ioMasObs = null; }
    const el = $(".ml-more"); if (!el || !("IntersectionObserver" in window)) return;
    ioMasObs = new IntersectionObserver(es => { if (es.some(x => x.isIntersecting) && !S.cargando) { ioMasObs.disconnect(); consulta(true); } }, { rootMargin: "500px 0px" });
    ioMasObs.observe(el); chequeaMas();
  }
  function principal() {
    const m = $("#mlMain"); if (!m) return; S.mapa = null; const qa = document.activeElement && document.activeElement.id === "mlQ" ? document.activeElement.selectionStart : null;
    if (S.modo === "proyectos") { proyectosVista(); return; }
    if (S.modo === "crear") { wizard(); return; }
    if (S.modo === "costos") { costos(); return; }
    const p = S.proy.find(x => x.id === S.sel);
    if (!p) { m.innerHTML = `<div class="ml-vacio"><h3>Bienvenido a Media Lab</h3>Monitor masivo de noticias. Crea un proyecto con los términos, la zona y el periodo que te interesan, o elige uno de la lista.<br><br><button class="ml-btn" type="button" data-go="proyectos">📁 Ir a Proyectos</button></div>`; return; }
    const d = S.data, en = enCurso(p);
    const tot = (p.estado && p.estado.total) || 0, fil = d ? d.total : 0, pc = tot ? Math.min(100, Math.round(fil / tot * 100)) : 0, ult = p.estado && p.estado.ultima, hace = ult ? Math.max(0, Math.round((Date.now() - new Date(ult).getTime()) / 864e5)) : null;
    const ring = (v, c) => { const r = 24, L = 2 * Math.PI * r; return `<svg class="rg" viewBox="0 0 60 60" aria-label="${v} %"><circle cx="30" cy="30" r="${r}" fill="none" stroke="rgba(148,163,184,.25)" stroke-width="7"/><circle cx="30" cy="30" r="${r}" fill="none" stroke="${c}" stroke-width="7" stroke-linecap="round" stroke-dasharray="${(L * v / 100).toFixed(1)} ${L.toFixed(1)}" transform="rotate(-90 30 30)"/><text x="30" y="34" text-anchor="middle">${v}%</text></svg>`; };
    const KP = [["news", "Noticias con estos filtros", nf(fil), `<span class="kbar"><i style="width:${pc}%"></i></span><span class="pill">${pc} % del proyecto</span>`, "k1", ""], ["stack", "Noticias en el proyecto", nf(tot), `<span class="pill">${ult ? "última carga " + esc(fechaC(ult)) : "sin extracciones"}</span>`, "k2", ""], ["tag", "Términos de interés", nf(p.config && p.config.nTerminos), `<span class="pill">${esc(PER[p.config && p.config.periodicidad] || "Manual")}</span>`, "k3", ""], ["clock", "Última extracción", ult ? esc(fechaC(ult)) : "—", `<span class="pill">${hace === null ? "sin datos" : hace === 0 ? "hoy" : "hace " + hace + (hace === 1 ? " día" : " días")} · ${p.estado && p.estado.proxima ? "próxima " + esc(fechaC(p.estado.proxima)) : "sin programar"}</span>`, "k4", ""]];
    let h = `<div class="ml-k4w">${KP.map(([i, t, v, sub, c, extra]) => `<div class="k ${c}"><span class="ico">${ic2(i)}</span><div class="tx"><span class="lb">${t}</span><b>${v}</b>${sub}</div>${extra}</div>`).join("")}</div>`;
    h += `<div class="ml-meta">${datosProy(p).filter(x => !/^(Noticias|Última extracción|Próxima extracción)$/.test(x[1])).map(([i, l, v]) => `<span title="${esc(l)}">${i} <em>${esc(l)}</em> <b>${v}</b></span>`).join("")}</div>`;
    if (d && d.facetas) h += filtros();
    h += `<div class="ml-tb"><div class="ml-seg" style="margin:0;width:260px"><button type="button" data-v="lista" class="${S.vista === "lista" ? "on" : ""}">📰 Noticias</button><button type="button" data-v="mapa" class="${S.vista === "mapa" ? "on" : ""}">🗺️ Mapa</button></div><span class="ml-sp"></span>${en ? `<span class="ml-chip run" data-chip="${p.id}">${chipEn(p)}</span>` : ""}</div><div id="mlProg"></div>`;
    if (p.objetivo) h += `<div class="ml-sem"><span>🎯 ${esc(p.objetivo)}</span></div>`;
    const cuerpo = !d ? `<div class="ml-vacio">Cargando noticias…</div>` : !d.items.length ? `<div class="ml-vacio">${p.estado && p.estado.total ? "Ninguna noticia coincide con los filtros." : "Este proyecto aún no tiene noticias. Pulsa «Extraer ahora»."}</div>` : `<div class="ml-cards">${d.items.map(card).join("")}</div>${d.items.length < d.total ? `<div class="ml-more" data-go="mas"><span class="ml-spin"></span>Cargando más noticias… (${nf(d.items.length)} de ${nf(d.total)})</div>` : ""}`;
    if (S.vista === "mapa") h += `<div class="ml-split"><div class="ml-mapcol"><div class="ml-map" id="mlMap"></div><div class="ml-sem" id="mlMapN"></div></div><div class="ml-cardcol">${cuerpo}</div></div>`;
    else if (!d) h += `<div class="ml-vacio">Cargando noticias…</div>`;
    else if (!d.items.length) h += `<div class="ml-vacio">${p.estado && p.estado.total ? "Ninguna noticia coincide con los filtros." : "Este proyecto aún no tiene noticias. Pulsa «Extraer ahora»."}</div>`;
    else h += `<div class="ml-cards">${d.items.map(card).join("")}</div>${d.items.length < d.total ? `<div class="ml-more" data-go="mas"><span class="ml-spin"></span>Cargando más noticias… (${nf(d.items.length)} de ${nf(d.total)})</div>` : ""}`;
    m.innerHTML = h + `<aside class="ml-det" id="mlDet" hidden></aside>`; pintaProg(); layDet(!!S.det); if (S.det) pintaDet(); if (S.vista === "mapa") mapa(); ioMas(); if (qa !== null) { const q = $("#mlQ"); if (q) { q.focus(); try { q.setSelectionRange(qa, qa); } catch (_) { } } }
  }
  const TXC = { 1: ["ok", "📄 Con texto", "Se descargó el cuerpo de la nota"], 2: ["", "🔒 Sin texto", "El medio no permitió descargar el cuerpo (bloqueo o suscripción)"], 0: ["run", "⏳ Texto pendiente", "Aún no se intenta descargar el cuerpo"] };
  const MET = { regla: ["🔎 Texto", "Detectado en el título o la descripción con el catálogo de municipios del INEGI"], ia: ["🤖 IA", "La IA interpretó el lugar del texto de la nota y se ubicó con el catálogo de municipios del INEGI"], places: ["🗺️ Google Places", "Ubicado con la API de Google Places y verificado dentro del estado de la nota"] };
  const metChip = L => L ? `<span class="ml-chip" title="${esc((MET[L.met || "regla"] || MET.regla)[1])}">${(MET[L.met || "regla"] || MET.regla)[0]}</span>` : "";
  const card = n => `<div class="ml-card${S.det && S.det.id === n.id ? " on" : ""}" data-n="${n.id}">${n.img ? `<div class="im" style="background-image:url('${esc(n.img)}')"></div>` : ""}<div class="bd"><div class="mt"><b>${esc(n.med)}</b><span>${esc(fecha(n.f))}</span></div><h4>${esc(n.t)}</h4><p>${esc(n.res || "")}</p>${anCard(n)}<div><span class="ml-chip ${TXC[n.tx || 0][0]}" title="${TXC[n.tx || 0][2]}">${TXC[n.tx || 0][1]}</span><span class="ml-chip">${esc(n.cat)}</span><span class="ml-chip">${esc(n.sub)}</span>${n.lug ? `<span class="ml-chip">📍 ${esc(n.lug.munN ? n.lug.munN + ", " + n.lug.estN : n.lug.estN)}</span>${metChip(n.lug)}` : ""}</div></div></div>`;

  function pintaProg() {
    const el = $("#mlProg"), p = S.proy.find(x => x.id === S.sel); if (!el || !p) return; if (!enCurso(p)) { el.innerHTML = ""; return; }
    const w = p.estado && p.estado.trabajo, act = w && w.fase !== "fin", ia = act && w.fase === "enriquecimiento" && w.ia ? { h: w.ia.hecho || 0, t: (w.ia.hecho || 0) + (w.ia.pend || 0) } : null, pct = ia ? (ia.t ? Math.min(100, Math.round(ia.h / ia.t * 100)) : 0) : act && w.total ? Math.min(100, Math.round(w.hecho / w.total * 100)) : 0;
    el.innerHTML = `<div class="ml-est"><b>⏳ ${act ? (FASES[w.fase] || "Procesando") : "Iniciando la extracción…"}</b><div class="ml-bar${act && w.total ? "" : " ind"}"><i style="width:${act && w.total ? pct : 35}%"></i></div>${ia ? `Notas analizadas con IA: <b>${nf(ia.h)} de ${nf(ia.t)}</b> (${pct} %) · faltan ${nf(ia.t - ia.h)} · ${nf(w.errores)} avisos` : act ? `Consultas <b>${nf(w.hecho)}/${nf(w.total)}</b> (${pct} %) · <b>${nf(w.nuevas)}</b> noticias nuevas · ${nf(w.dup)} repetidas omitidas · ${nf(w.errores)} avisos${w.prov ? " · servicio: " + esc(w.prov === "grss" ? "Google Noticias" : w.prov) : ""}` : `La primera consulta puede tardar hasta un minuto; el avance se actualiza solo.`}${act && w.desde ? `<div class="ml-sem">Periodo consultado: ${esc(fechaC(w.desde))} → ${esc(fechaC(w.hasta))} · notas vistas: ${nf(w.vistas)}</div>` : ""}${w && w.aviso ? `<div class="ml-warn">ℹ️ ${esc(w.aviso)}</div>` : ""}${act && w.ultimoError && w.errores ? `<div class="ml-sem">Último aviso: ${esc(w.ultimoError)}</div>` : ""}${w && w.act ? (() => { const seg = Math.max(0, Math.round((Date.now() - (w.act.t || 0)) / 1000)), viva = seg < 45; return `<div class="ml-act"><span class="ml-dot${viva ? "" : " off"}"></span><b>Ahora:</b> ${esc(w.act.m)} <small>· ${viva ? "hace " + seg + " s" : "sin señal reciente (" + seg + " s); el servidor sigue intentando"}</small></div>`; })() : ""}${w && w.inicio ? `<div class="ml-sem">Tiempo transcurrido: ${(() => { const t = Math.max(0, Math.round((Date.now() - new Date(w.inicio).getTime()) / 1000)); return Math.floor(t / 60) + ":" + String(t % 60).padStart(2, "0"); })()}</div>` : ""}<br><small style="color:var(--ml-mut)">Puedes seguir usando el tablero o cerrar la ventana: la extracción continúa en el servidor.</small></div>`;
    $$(`[data-chip="${p.id}"]`).forEach(c => { c.textContent = chipEn(p); });
  }

  /* ───────── detalle (columna lateral derecha) ───────── */
  async function abreDet(id, forzarIA) {
    S.entV = null; S.det = { id, cargando: true }; pintaDet(); $$(".ml-card").forEach(c => c.classList.toggle("on", c.dataset.n === id));
    try { const r = await api(`/proyectos/${S.sel}/noticia/${id}${forzarIA ? "?ia=1" : ""}`); S.det = { id, n: r.noticia }; } catch (e) { S.det = { id, error: e.message }; } pintaDet();
  }
  function layDet(on) { const a = $("#mlApp"); if (!a) return; const act = !!on && S.vista === "mapa"; a.classList.toggle("ml-det-on", act); if (act) { if (!a.classList.contains("ml-sidehide")) { a.classList.add("ml-sidehide"); S.sideAuto = true; } } else if (S.sideAuto) { a.classList.remove("ml-sidehide"); S.sideAuto = false; } setTimeout(() => { if (S.mapa) S.mapa.invalidateSize(); }, 160); }
  function cierraDet() { layDet(false); S.det = null; S.entV = null; const d = $("#mlDet"); if (d) d.hidden = true; $$(".ml-card.on").forEach(c => c.classList.remove("on")); }
  function pintaDet() {
    const d = $("#mlDet"); if (!d || !S.det) return; d.hidden = false; layDet(true); const x = S.det; if (S.entV) { pintaEnt(d); return; }
    if (x.lista) { d.innerHTML = `<button class="x" type="button" data-go="cierra">✕</button><h3>📍 ${esc(x.lista.nombre)}</h3><div class="ml-sem"><span>${x.lista.items.length} noticias en este lugar</span></div>` + x.lista.items.map(n => `<div class="ml-card" data-n="${n.id}" style="margin:0 0 10px"><div class="bd"><div class="mt"><b>${esc(n.med)}</b><span>${esc(fecha(n.f))}</span></div><h4>${esc(n.t)}</h4><div><span class="ml-chip">${esc(n.cat)}</span></div></div></div>`).join(""); return; }
    if (x.cargando) { d.innerHTML = `<button class="x" type="button" data-go="cierra">✕</button><div class="ml-vacio">Cargando detalle…</div>`; return; }
    if (x.error) { d.innerHTML = `<button class="x" type="button" data-go="cierra">✕</button><div class="ml-warn">${esc(x.error)}</div>`; return; }
    const n = x.n, L = n.lug, ent = n.ent || {}, I = n.ind;
    const lista = [...(ent.pers || []).map(v => ["pers", v]), ...(ent.inst || []).map(v => ["inst", v]), ...(ent.top || []).map(v => ["top", v])];
    const tabla = (tt, T) => `<div class="ml-bl"><h5>${tt}</h5><table class="ml-tb2"><thead><tr><th>Componente</th><th>Ponderador</th><th>Calificación</th><th>Calif. ponderada</th></tr></thead><tbody>${T.filas.map(f => `<tr><td>${esc(f.nom)}</td><td><i>${f.w}</i></td><td><i>${f.c}</i></td><td><i>${f.cw}</i></td></tr>`).join("")}</tbody></table></div>`;
    const sb = I && I.sent != null ? SB(I.sent) : null;
    d.classList.remove("wide");
    d.innerHTML = `<button class="x" type="button" data-go="cierra">✕</button><div class="mt" style="font-size:12px;color:var(--ml-mut)"><b style="color:var(--ml-ac2)">${esc(n.med)}</b> · ${esc(fecha(n.f))}</div><h3>${esc(n.t)}</h3>
      ${n.img ? `<img class="g" src="${esc(n.img)}" alt="" onerror="this.remove()">` : ""}
      <div class="ml-k3">
        <div><small>${ic("smile")}Sentimiento</small><b class="v">${I && I.sent != null ? EMO(I.sent) + " " + I.sent + ` <em class="${sb[1]}">${sb[0]}</em>` : "—"}</b>${I && I.sent != null ? `<div class="ml-sb" title="Sentimiento ${I.sent} de 9"><span></span><i style="left:${((I.sent - 1) / 8 * 100).toFixed(1)}%"></i></div><div class="sub">Escala 1 a 9${I.pol != null ? ` · polarización ${I.pol}/10` : ""}</div>` : ""}</div>
        <div><small>${ic("net")}Entidades detectadas</small><b class="v">${lista.length}</b><div class="ml-eb">${[["pers", "personas"], ["inst", "instituciones"], ["top", "tópicos"]].map(([t, nm]) => { const c = lista.filter(x => x[0] === t).length; return c ? `<span><i style="background:${TCOL[t] || "#22d3ee"}"></i>${c} ${nm}</span>` : ""; }).join("")}</div></div>
        <div class="ml-kpop"><small>${ic("users")}Población afectada</small>${I && I.afe ? `<div class="pp"><span class="pi" style="--pc:${ESCCOL[I.afe.esc - 1] || "#64748b"}" title="Escala ${I.afe.esc} de 8">${ic("e" + I.afe.esc)}</span><div><b>${esc(I.afe.escN)}</b><span>${esc(I.afe.rango)}</span></div></div>` : `<b class="v pb">${I ? nf(I.pob) : "—"}</b>`}</div>
      </div>${I ? afeHtml(I) : ""}
      ${I ? "" : `<div class="ml-warn" style="margin:8px 0">Esta nota aún no tiene el análisis ampliado (sentimiento, riesgo, SCIAN, índices). <button class="ml-btn2" type="button" data-go="resumen" style="margin-left:6px">✨ Analizar con IA</button></div>`}
      <div class="ml-bl"><h5>${ic("pin")}Contexto</h5>
        <div>${L ? [L.estN, L.munN, L.zona, L.sitio].filter(Boolean).map(v => `<span class="ml-chip">${esc(v)}</span>`).join("") + `${metChip(L)}` : `<span class="ml-chip">Sin ubicación detectada</span>`}<span class="ml-chip">${esc(n.cat)}</span><span class="ml-chip">${esc(n.sub)}</span></div>
        <div class="ml-fa"><span>Fuente:</span> <b>${esc(n.med)}</b> <span>· Autor:</span> <b>${esc(n.autor || "ND")}</b> <span>· Fecha:</span> <b>${esc(fechaC(n.f))}</b></div>
        <div class="ml-fa"><span>${ic("bld")}SCIAN:</span></div>${I && I.scian.length ? `<div class="ml-sc">${I.scian.map(c => `<div><b>${esc(c.c)}</b> · ${esc(c.n)}</div>`).join("")}</div>` : `<div class="ml-fa">${I ? "Sin relación clara con un sector SCIAN." : "—"}</div>`}
      </div>
      ${n.res ? `<div class="ml-bl"><h5>${ic("doc")}Resumen</h5><div>${esc(n.res)}</div></div>` : (n.ia === 0 ? `<button class="ml-btn2" type="button" data-go="resumen" style="margin:6px 0">✨ Generar síntesis con IA</button>` : "")}
      ${I ? riesgoHtml(I) : ""}
      <div class="ml-bl"><h5>${ic("net")}Entidades mencionadas</h5>${lista.length ? lista.map(([t, v]) => `<a href="#" class="ml-ent ${t}" data-ver="${esc(v)}" title="Ver recurrencia y red de relaciones">${esc(v)}</a>`).join("") : `<div class="ml-fa">Sin entidades detectadas.</div>`}</div>
      ${n.texto ? `<div class="tx">${esc(n.texto)}</div>` : (n.d ? `<div class="tx">${esc(n.d)}</div>` : "")}
      <dl><dt>Texto</dt><dd>${n.texto ? `<span class="ml-chip ok">📄 Descargado</span> ${nf(n.texto.length)} caracteres` : n.txErr ? `<span class="ml-chip">🔒 No accesible</span> el medio no permitió descargarlo` : `<span class="ml-chip run">⏳ Pendiente</span>`}</dd>
      <dt>Lugar</dt><dd>${L ? esc([L.sitio, L.zona, L.munN, L.estN].filter(Boolean).join(", ")) + ` <small style="color:var(--ml-mut)">(nivel: ${esc(L.nivel)})</small><br>${metChip(L)} <small style="color:var(--ml-mut)">${esc((MET[L.met || "regla"] || MET.regla)[1])}</small>` : "Sin ubicación detectada"}</dd>
      <dt>Idioma</dt><dd>${esc(n.idi || "—")}${n.pais ? " · " + esc(n.pais) : ""}</dd><dt>Proveedor</dt><dd>${esc(n.prov || "—")}${n.ia ? " · enriquecida con IA" : ""}</dd></dl>
      <a class="ml-btn" style="display:inline-block;text-decoration:none" href="${esc(n.urlReal || n.url)}" target="_blank" rel="noopener">Abrir nota original ↗</a>${n.lug && n.lug.lat != null ? ` <button class="ml-btn ml-btn-map" type="button" data-vermapa="${esc(n.id)}">${ic("pin")}Ver en mapa</button>` : ""}`;
  }

  /* ───────── indicadores: medidor, curva de incidencia y grafos de entidades ───────── */

  const ESCCOL = ["#14b8a6", "#22c55e", "#84cc16", "#eab308", "#f59e0b", "#f97316", "#ef4444", "#b91c1c"];
  const IC = { pin: '<path d="M12 21s7-6.2 7-12a7 7 0 0 0-14 0c0 5.8 7 12 7 12z"/><circle cx="12" cy="9" r="2.5"/>', doc: '<path d="M6 3h8l4 4v14H6z"/><path d="M14 3v4h4M9 12h6M9 16h6"/>', net: '<circle cx="6" cy="12" r="2.5"/><circle cx="18" cy="6" r="2.5"/><circle cx="18" cy="18" r="2.5"/><path d="M8.2 11l7.6-4M8.2 13l7.6 4"/>', trend: '<path d="M3 17l6-6 4 4 8-9"/><path d="M15 6h6v6"/>', bar: '<path d="M5 21V10M12 21V4M19 21v-8"/>', smile: '<circle cx="12" cy="12" r="9"/><path d="M8 14s1.5 2 4 2 4-2 4-2M9 9h.01M15 9h.01"/>', users: '<circle cx="9" cy="8" r="3"/><path d="M3 20c0-3.3 2.7-6 6-6s6 2.7 6 6"/><circle cx="17" cy="9" r="2.5"/><path d="M16 14.2c2.8.3 5 2.6 5 5.8"/>', bld: '<path d="M4 21V5l8-2v18M12 9h8v12M3 21h18M8 8h1M8 12h1M8 16h1M15 13h1M15 17h1"/>', shield: '<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/><path d="M12 8v5M12 16h.01"/>', flame: '<path d="M12 3c1 3 5 5 5 10a5 5 0 0 1-10 0c0-2 1-3 2-4 0 2 1 3 2 3 0-3-1-6 1-9z"/>', gauge: '<path d="M4 17a8 8 0 1 1 16 0"/><path d="M12 17l4-5"/>', cross: '<rect x="4" y="4" width="16" height="16" rx="4"/><path d="M12 8v8M8 12h8"/>', zap: '<path d="M13 3L5 14h6l-1 7 8-11h-6z"/>', lr: '<path d="M8 7l-5 5 5 5M16 7l5 5-5 5M3 12h18"/>', arrow: '<path d="M5 12h14M13 6l6 6-6 6"/>', e1: '<circle cx="12" cy="8" r="3.6"/><path d="M5 20.5c0-3.9 3.1-7 7-7s7 3.1 7 7"/>', e2: '<circle cx="12" cy="8" r="3"/><circle cx="5.2" cy="10.5" r="2.2"/><circle cx="18.8" cy="10.5" r="2.2"/><path d="M7 20c0-2.8 2.2-5 5-5s5 2.2 5 5M1.5 19c0-2 1.6-3.6 3.7-3.6M22.5 19c0-2-1.6-3.6-3.7-3.6"/>', e3: '<path d="M2.5 12l4.7-4.2L12 12v7.5H2.5zM12 14.5l4.5-4 4.5 4v5H12z"/><path d="M5.5 19.5v-4h3v4M15 19.5v-3h3v3"/>', e4: '<path d="M3.4 10.8 L6.6 4.6 L13.6 3.8 L20.4 7.9 L20.0 16.6 L12.8 20.3 L5.7 18.1z" fill="currentColor" fill-opacity=".12"/><path d="M6.3 11.2 L8.8 7.5 L13.1 6.5 L17.1 9.5 L17.3 15.1 L12.5 17.5 L8.2 15.7z" fill="currentColor" fill-opacity=".22"/><path d="M9.4 11.6 L10.4 9.7 L12.5 9.6 L14.5 10.8 L14.4 13.4 L12.2 14.7 L10.1 13.8z" fill="currentColor" fill-opacity=".38"/><circle cx="12" cy="12" r="1.3" fill="currentColor"/>', e5: '<path d="M2.5 21h19M4.5 21v-9h4v9M8.5 21V4.5h5.5V21M14 21V9h5.5v12"/><path d="M10.5 8h1.5M10.5 12h1.5M10.5 16h1.5M16 13h1.5M16 17h1.5"/>', e6: '<path d="M4 7.5 10 4l6.5.8 4 4.2-1 6.5L14 20l-6-1-3.5-5z" fill="currentColor" fill-opacity=".18"/><path d="M10 4v6.5L4.5 14M10 10.5l5 2M15 12.5 20.5 9M15 12.5 14 20M15 12.5l1.5-7.7"/><circle cx="12.4" cy="15.6" r="1.2" fill="currentColor"/>', e7: '<path d="M3.4 5.5 L3.3 6.1 L5.7 6.4 L7.5 6.4 L7.5 6.1 L8.6 6.1 L9.6 6.9 L9.9 7.6 L10.8 8.1 L11.2 7.5 L11.9 7.5 L12.3 8.0 L12.6 8.5 L13.1 9.1 L13.4 9.9 L13.9 10.1 L14.6 10.2 L14.3 11.3 L14.2 12.6 L14.6 13.9 L15.3 14.9 L15.9 15.5 L16.4 15.7 L17.1 15.5 L17.9 15.3 L18.7 14.5 L18.9 13.7 L19.4 13.5 L21.0 13.2 L21.2 13.6 L20.8 14.7 L20.6 15.6 L20.3 15.4 L19.7 15.9 L18.5 15.9 L18.3 16.3 L18.6 17.0 L17.8 17.7 L17.8 18.2 L16.6 17.0 L15.8 17.2 L15.0 17.4 L14.1 16.9 L12.8 16.6 L11.8 16.0 L10.5 15.5 L10.0 15.0 L9.4 14.0 L9.2 13.2 L9.1 12.5 L8.7 12.1 L7.4 10.6 L6.7 9.7 L5.9 8.8 L5.0 8.1 L4.5 6.9 L3.4 6.2z" fill="currentColor" fill-opacity=".22"/><path d="M1.8 5.6 L3.4 5.5 L3.3 6.4 L3.8 7.4 L4.6 8.2 L5.5 10.0 L6.2 11.4 L6.7 12.0 L6.4 12.3 L6.2 12.2 L5.4 11.2 L5.0 10.7 L3.9 9.7 L3.8 9.1 L3.4 8.8 L2.7 7.1 L2.2 6.2z" fill="currentColor" fill-opacity=".22"/>', e8: '<circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c3.2 3.2 3.2 14.8 0 18M12 3c-3.2 3.2-3.2 14.8 0 18"/>', news: '<path d="M5 4h11v16H6a1 1 0 0 1-1-1z"/><path d="M16 8h3v11a1 1 0 0 1-1 1M8 8h5M8 12h5M8 16h3"/>', stack: '<path d="M12 3l9 5-9 5-9-5z"/><path d="M3 13l9 5 9-5"/>', tag: '<path d="M3 12V4h8l10 10-8 8z"/><circle cx="7.5" cy="8.5" r="1.3"/>', clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>', warn: '<path d="M12 3l10 18H2z"/><path d="M12 10v4M12 17h.01"/>' };
  /* iconos de dos tonos para los KPIs */
  const IC2 = { news: '<rect x="4" y="3" width="13" height="18" rx="2.5" fill="currentColor" opacity=".3"/><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H17v18H6.5A2.5 2.5 0 0 1 4 18.5z"/><path d="M17 8h2.5A1.5 1.5 0 0 1 21 9.5V18a3 3 0 0 1-3 3"/><path d="M8 8h6M8 12h6M8 16h3.5"/>', stack: '<path d="M12 3l9 5-9 5-9-5z" fill="currentColor" opacity=".3"/><path d="M12 3l9 5-9 5-9-5z"/><path d="M3 12.5l9 5 9-5"/><path d="M3 17l9 5 9-5"/>', tag: '<path d="M3 12V4.5A1.5 1.5 0 0 1 4.5 3H12l9 9-9 9z" fill="currentColor" opacity=".3"/><path d="M3 12V4.5A1.5 1.5 0 0 1 4.5 3H12l9 9-9 9z"/><circle cx="8" cy="8" r="1.6" fill="currentColor"/>', clock: '<circle cx="12" cy="12" r="9" fill="currentColor" opacity=".3"/><circle cx="12" cy="12" r="9"/><path d="M12 6.5V12l3.5 2.2"/>' };
  const ic2 = n => `<svg class="ml-ic2" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${IC2[n] || ""}</svg>`;
  const ic = n => `<svg class="ml-ic" viewBox="0 0 24 24" aria-hidden="true">${IC[n] || ""}</svg>`;
  const NIV = v => v >= 75 ? ["Crítico", "#ef4444"] : v >= 50 ? ["Alto", "#f97316"] : v >= 25 ? ["Moderado", "#f59e0b"] : ["Bajo", "#22c55e"];
  const RELCOL = { "Estratégica": "#dc2626", Alta: "#ea580c", Media: "#d97706", Baja: "#2563eb", Marginal: "#64748b" };
  function afeHtml(I) {
    const A = I.afe; if (!A) return "";
    const col = RELCOL[A.relN] || "#64748b", pasos = ["Particular", "Reducido", "Comunidad", "Zona", "Ciudad", "Estado", "Nacional", "Global"];
    const BANDS = [["Marginal", "#94a3b8"], ["Baja", "#3b82f6"], ["Media", "#f59e0b"], ["Alta", "#f97316"], ["Estratégica", "#dc2626"]];
    const pt = (v, r) => [(100 - r * Math.cos(Math.PI * v / 100)).toFixed(1), (100 - r * Math.sin(Math.PI * v / 100)).toFixed(1)];
    const arcos = BANDS.map(([nm, c], i) => { const a = pt(i * 20 + 1.2, 78), b = pt(i * 20 + 18.8, 78); return `<path d="M${a[0]} ${a[1]} A78 78 0 0 1 ${b[0]} ${b[1]}" stroke="${c}" class="${nm === A.relN ? "on" : ""}"/>`; }).join("");
    const marcas = BANDS.map(([nm], i) => { const p = pt(i * 20 + 10, 98); return `<text x="${p[0]}" y="${p[1]}" text-anchor="middle" class="${nm === A.relN ? "on" : ""}">${nm === "Estratégica" ? "Estrat." : nm}</text>`; }).join("");
    const gauge = `<svg class="ml-afg" viewBox="0 0 200 122" aria-label="Relevancia ${A.rel} de 100">${arcos}${marcas}<g class="nd" style="--a:${(A.rel * 1.8).toFixed(1)}deg"><path d="M100 100 L42 100" stroke="${col}"/><circle cx="100" cy="100" r="8" fill="${col}"/><circle cx="100" cy="100" r="3" fill="#fff"/></g></svg>`;
    const escala = pasos.map((t, i) => `<span class="${i + 1 === A.esc ? "on" : i + 1 < A.esc ? "pre" : ""}"><i${i + 1 === A.esc ? ` style="background:${col};border-color:${col};color:${col}"` : ""}></i><u>${t}</u></span>`).join("");
    return `<div class="ml-bl ml-afe"><h5>${ic("users")}Población afectada y relevancia</h5>
      <div class="ml-af-top">
        <div class="ml-af-q"><small>¿Quiénes son los afectados?</small><b>${esc(A.grupo || A.tipoN || "No determinado")}</b>${A.tipoN ? `<em>${ic("users")}${esc(A.tipoN)}</em>` : ""}${A.act ? `<div class="ml-af-a"><small>Actor principal</small><b>${esc(A.actn || "No identificado")}</b><div class="ml-af-d" title="Influencia de los actores involucrados">${[1, 2, 3, 4, 5].map(k => `<i class="${k <= A.act ? "on" : ""}" style="${k <= A.act ? "background:" + col : ""}"></i>`).join("")}<span>${esc(A.actN)}</span></div></div>` : `<div class="ml-af-a"><small>Actores involucrados</small><span class="ml-af-s">Sin evaluar en esta nota</span><button class="ml-btn2" type="button" data-go="resumen">✨ Reanalizar con IA</button></div>`}<div class="ml-af-n"><span style="color:${col}">${esc(A.escN)}</span><i>${esc(A.rango)}</i>${I.pob ? `<i>Estimación de la nota: ${nf(I.pob)} personas</i>` : ""}</div></div>
        <div class="ml-af-g"><small>Relevancia para evaluar proyectos</small>${gauge}<div class="ml-af-v" style="color:${col}"><b>${A.rel}</b><span>/100</span></div><div class="ml-af-l" style="background:${col}">${esc(A.relN)}</div><em>Rol social: ${esc(A.rolN)}</em></div>
      </div>
      <div class="ml-af-sc" title="Escala de la población afectada">${escala}</div>
      <div class="ml-fa">Relevancia = tamaño del grupo afectado (50 %) + importancia de su rol económico o social (25 %) + influencia de los actores involucrados (25 %).</div></div>`;
  }
  function riesgoHtml(I) {
    const [nv, col] = NIV(I.risk), por = v => Math.max(0, Math.min(100, v));
    const lect = `Peligro ${I.haz >= 70 ? "alto" : I.haz >= 40 ? "medio" : "bajo"}, alcance ${I.alc || "no determinado"}${I.pob ? (I.pob === 1 ? " y 1 persona afectada" : ` y ${I.pob < 20 ? "" : "cerca de "}${nf(I.pob)} personas afectadas`) : ""}.`;
    const ix = (nom, v, max, tabla, nota) => { const [n2, c2] = max === 10 ? (v >= 7 ? ["Alta", "#ef4444"] : v >= 4 ? ["Media", "#f59e0b"] : ["Baja", "#22c55e"]) : NIV(v), pc = max === 10 ? v * 10 : por(v), imp = tabla ? tabla.filas.filter(f => f.c > 0) : [];
      return `<div class="ml-ixr"><div class="ml-ixh"><b>${ic({ Gravedad: "cross", Violencia: "zap", "Polarización": "lr" }[nom])}${nom}</b><span style="color:${c2}">${n2}</span><em>${max === 10 ? v + " / 10" : v}</em></div><div class="ml-ixb"><i style="width:${pc}%;background:${c2}"></i>${max !== 10 && v > 100 ? `<u title="Supera la escala de referencia (100)">▸</u>` : ""}</div>${imp.length ? `<div class="ml-ixc">${imp.map(f => `<span>${esc(f.nom)} <b>×${f.c}</b></span>`).join("")}</div>` : `<div class="ml-ixc"><span class="n">${nota}</span></div>`}</div>`; };
    const calc = (tt, T) => `<div class="ml-cl"><b>${tt}</b>${T.filas.map(f => `<div class="${f.c ? "on" : ""}"><span>${esc(f.nom)}</span><em>${f.w} × ${f.c}</em><i>${f.cw}</i></div>`).join("")}<div class="tot"><span>Σ ÷ Σ ponderadores × 100</span><i>${T.pct}</i></div></div>`;
    return `<section class="ml-rk"><div class="ml-rkh"><span>${ic("shield")}Evaluación de riesgo</span><em style="color:${col};border-color:${col}">● ${nv}</em></div>
      <div class="ml-rkm"><div class="ml-rkn" style="color:${col}">${I.risk}<small>/100</small></div><div class="ml-rkt"><div class="ml-rkb"><i style="left:${por(I.risk)}%"></i></div><div class="ml-rkl"><span>Bajo</span><span>Moderado</span><span>Alto</span><span>Crítico</span></div></div></div>
      <div class="ml-eq"><div><small>${ic("users")}Exposición</small><b>${I.expo}</b><u><i style="width:${por(I.expo)}%"></i></u></div><span>×</span><div><small>${ic("flame")}Peligro</small><b>${I.haz}</b><u><i style="width:${por(I.haz)}%"></i></u></div><span>÷ 100 =</span><div class="r"><small>${ic("gauge")}Riesgo</small><b style="color:${col}">${I.risk}</b></div></div>
      <p class="ml-lect">${lect}</p>${I.riesgos.length ? `<div class="ml-tr"><small>${ic("warn")}Tipos de riesgo</small>${I.riesgos.map(r => `<span>${esc(r)}</span>`).join("")}</div>` : ""}</section>
      <section class="ml-rk"><div class="ml-rkh"><span>${ic("bar")}Índices de la nota</span></div>${ix("Gravedad", I.grav.pct, 100, I.grav, "Sin lesiones, armas ni menores")}${ix("Violencia", I.viol.pct, 100, I.viol, "Sin hechos violentos")}${ix("Polarización", I.pol == null ? 1 : I.pol, 10, null, "Qué tan divisivo es el tema")}
      <details class="ml-dt"><summary>Ver cálculo detallado</summary><div class="ml-cg">${calc("Gravedad", I.grav)}${calc("Violencia", I.viol)}</div><p class="ml-lect">Riesgo = Exposición × Peligro ÷ 100. Exposición = mayor entre la escala logarítmica de población afectada y el alcance de la nota. Polarización (1–10) se obtiene del análisis del texto.</p></details></section>`;
  }

  const SB = v => v >= 7 ? ["POS", "ok"] : v >= 4 ? ["NEU", ""] : ["NEG", "bad"], EMO = v => v >= 7 ? "🙂" : v >= 4 ? "😐" : "🙁", BAR = v => v >= 66 ? "#ef4444" : v >= 33 ? "#f59e0b" : "#22c55e";
  const anCard = n => n.an ? `<div class="ml-an"><span class="ml-chip">${EMO(n.an.s)} sent <b>${n.an.s}</b></span><span class="ml-chip">riesgo <b>${n.an.r}</b></span><span class="ml-chip">expo <b>${Math.round(n.an.e)}</b></span></div>${(() => { const r = Math.max(0, Math.min(100, n.an.r)), w = Math.max(3, r), [nv, col] = NIV(r); return `<div class="ml-rbw" title="Riesgo ${r} · ${nv}"><div class="ml-rb"><span style="width:${w}%;background-size:${(10000 / w).toFixed(1)}% 100%"></span><i style="left:${w}%;border-color:${col}"></i></div><b style="color:${col}">${nv}</b></div>`; })()}` : "";
  function gauge(v) {
    const R = 78, cx = 100, cy = 100, pt = (f, r) => { const a = Math.PI * (1 - f); return [cx + (r || R) * Math.cos(a), cy - (r || R) * Math.sin(a)]; }, arc = (a, b, c) => { const [x0, y0] = pt(a), [x1, y1] = pt(b); return `<path d="M${x0.toFixed(1)} ${y0.toFixed(1)}A${R} ${R} 0 0 1 ${x1.toFixed(1)} ${y1.toFixed(1)}" stroke="${c}" stroke-width="14" fill="none"/>`; }, [nx, ny] = pt(Math.min(1, Math.max(0, v / 100)), R - 14);
    return `<svg viewBox="0 0 200 130" class="ml-gau">${arc(0, .33, "#22c55e")}${arc(.33, .66, "#f59e0b")}${arc(.66, 1, "#ef4444")}<line x1="100" y1="100" x2="${nx.toFixed(1)}" y2="${ny.toFixed(1)}" stroke="currentColor" stroke-width="3" stroke-linecap="round"/><circle cx="100" cy="100" r="5" fill="currentColor"/><text x="100" y="124" text-anchor="middle" font-size="20" font-weight="800" fill="currentColor">${v}</text><text x="14" y="120" font-size="9" fill="currentColor" opacity=".6">0</text><text x="176" y="120" font-size="9" fill="currentColor" opacity=".6">100</text></svg>`;
  }
  const TCOL = { pers: "#22d3ee", inst: "#a78bfa", top: "#fbbf24" };
  /* ───────── grafos 3D en canvas: etiquetas, arrastre de nodos y giro con el cursor ───────── */
  function grafo3D(cv, nodos, enlaces, tipo, raiz, sel, keep, onSel) {
    const ctx = cv.getContext("2d"), dpr = Math.min(window.devicePixelRatio || 1, 2); let W = cv.clientWidth || 340, H = cv.clientHeight || 300; cv.width = W * dpr; cv.height = H * dpr;
    const ids = nodos.map(n => n.id), NM = new Map(nodos.map(n => [n.id, n])), peso = new Map(), P = new Map(), fm = Math.max(1, ...nodos.map(n => n.f));
    enlaces.forEach(e => { peso.set(e.a + "|" + e.b, e.w); peso.set(e.b + "|" + e.a, e.w); });
    let reduce = false; try { reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (_) { }
    const cam = keep.cam || (keep.cam = { yaw: .55, pitch: -.3, zoom: 1, auto: !reduce });
    const est = getComputedStyle(cv), tx = est.color || "#0f172a", bg = (est.getPropertyValue("--ml-bg") || "#fff").trim() || "#fff", fib = (k, m, R) => { const phi = Math.acos(1 - 2 * (k + .5) / Math.max(1, m)), th = Math.PI * (1 + Math.sqrt(5)) * (k + .5); return { x: R * Math.sin(phi) * Math.cos(th), y: R * Math.cos(phi), z: R * Math.sin(phi) * Math.sin(th) }; };
    let alpha = .08;
    const reuse = keep.pos && ids.every(i => keep.pos[i]);
    if (reuse) ids.forEach(i => P.set(i, Object.assign({ vx: 0, vy: 0, vz: 0 }, keep.pos[i])));
    else if (tipo === "geo") {
      P.set(raiz, { x: 0, y: 0, z: 0 }); const otros = ids.filter(i => i !== raiz).sort((a, b) => (peso.get(raiz + "|" + b) || 0) - (peso.get(raiz + "|" + a) || 0)), n1 = Math.min(10, otros.length), n2 = otros.length - n1;
      otros.forEach((id, i) => P.set(id, i < 10 ? fib(i, n1, 92) : fib(i - 10, n2, 168)));
    } else if (tipo === "arbol") {
      const padre = new Map(), dentro = new Set([raiz]), prof = new Map([[raiz, 0]]); while (dentro.size < ids.length) { let mejor = null; for (const a of dentro) for (const b of ids) { if (dentro.has(b)) continue; const w = peso.get(a + "|" + b) || 0; if (w && (!mejor || w > mejor[2])) mejor = [a, b, w]; } if (!mejor) break; padre.set(mejor[1], mejor[0]); prof.set(mejor[1], prof.get(mejor[0]) + 1); dentro.add(mejor[1]); }
      ids.forEach(i => { if (!dentro.has(i)) { padre.set(i, raiz); prof.set(i, 1); } }); const hijos = new Map(); ids.forEach(i => { if (i !== raiz) { const a = hijos.get(padre.get(i)) || []; a.push(i); hijos.set(padre.get(i), a); } });
      let hoja = 0; const xs = new Map(), zs = new Map(), rec = id => { const h = hijos.get(id) || []; if (!h.length) { xs.set(id, hoja); zs.set(id, ((hoja % 3) - 1) * 60); hoja++; return; } h.forEach(rec); xs.set(id, (xs.get(h[0]) + xs.get(h[h.length - 1])) / 2); zs.set(id, zs.get(h[0])); }; rec(raiz); const md = Math.max(...prof.values()) || 1, sp = Math.min(46, 300 / Math.max(1, hoja));
      ids.forEach(i => P.set(i, { x: (xs.get(i) - (hoja - 1) / 2) * sp, y: -120 + 240 * (prof.get(i) / md), z: zs.get(i) || 0 }));
    } else { ids.forEach((id, i) => { const q = fib(i, ids.length, 120); P.set(id, q); }); alpha = 1; }
    ids.forEach(i => { const q = P.get(i); q.vx = q.vx || 0; q.vy = q.vy || 0; q.vz = q.vz || 0; });
    const guarda = () => { keep.pos = {}; ids.forEach(i => { const q = P.get(i); keep.pos[i] = { x: q.x, y: q.y, z: q.z, pin: !!q.pin }; }); };
    function paso() {
      for (let i = 0; i < ids.length; i++) for (let j = i + 1; j < ids.length; j++) { const a = P.get(ids[i]), b = P.get(ids[j]); let dx = a.x - b.x, dy = a.y - b.y, dz = a.z - b.z, d2 = dx * dx + dy * dy + dz * dz + 40, d = Math.sqrt(d2), f = 5200 / d2 * alpha; dx /= d; dy /= d; dz /= d; a.vx += dx * f; a.vy += dy * f; a.vz += dz * f; b.vx -= dx * f; b.vy -= dy * f; b.vz -= dz * f; }
      enlaces.forEach(e => { const a = P.get(e.a), b = P.get(e.b); if (!a || !b) return; let dx = b.x - a.x, dy = b.y - a.y, dz = b.z - a.z, d = Math.sqrt(dx * dx + dy * dy + dz * dz) + .01, L = 90 - Math.min(34, e.w * 7), f = (d - L) * .022 * alpha; dx /= d; dy /= d; dz /= d; a.vx += dx * f; a.vy += dy * f; a.vz += dz * f; b.vx -= dx * f; b.vy -= dy * f; b.vz -= dz * f; });
      ids.forEach(i => { const q = P.get(i); q.vx -= q.x * .004 * alpha; q.vy -= q.y * .004 * alpha; q.vz -= q.z * .004 * alpha; if (q.pin) { q.vx = q.vy = q.vz = 0; return; } q.vx *= .82; q.vy *= .82; q.vz *= .82; q.x += q.vx; q.y += q.vy; q.z += q.vz; }); alpha *= .992;
    }
    const proj = q => { const cy = Math.cos(cam.yaw), sy = Math.sin(cam.yaw), cp = Math.cos(cam.pitch), sp = Math.sin(cam.pitch); const x = q.x * cy - q.z * sy; let z = q.x * sy + q.z * cy; const y = q.y * cp - z * sp; z = q.y * sp + z * cp; const s2 = 520 / (520 + z) * cam.zoom * Math.min(W, H) / 340; return { sx: W / 2 + x * s2, sy: H / 2 + y * s2, s: s2, z }; };
    let pr = new Map(), hovered = null; const rad = n => 4.5 + Math.sqrt(n.f / fm) * 9 + (n.id === raiz ? 3 : 0);
    function dibuja() {
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, W, H); pr = new Map(); ids.forEach(i => pr.set(i, proj(P.get(i))));
      enlaces.forEach(e => { const a = pr.get(e.a), b = pr.get(e.b); if (!a || !b) return; const zz = (a.z + b.z) / 2, al = Math.max(.07, Math.min(.55, .3 - zz / 700 + e.w * .03)) * ((e.a === sel || e.b === sel) ? 1.7 : 1); ctx.strokeStyle = `rgba(124,92,214,${Math.min(.9, al).toFixed(3)})`; ctx.lineWidth = Math.max(.5, (.5 + Math.log(1 + e.w)) * (a.s + b.s) / 2); ctx.beginPath(); ctx.moveTo(a.sx, a.sy); ctx.lineTo(b.sx, b.sy); ctx.stroke(); });
      const orden = [...ids].sort((a, b) => pr.get(b).z - pr.get(a).z);
      orden.forEach(id => { const q = pr.get(id), n = NM.get(id), r = rad(n) * q.s, col = TCOL[n.t] || "#22d3ee", g = ctx.createRadialGradient(q.sx - r * .35, q.sy - r * .4, r * .1, q.sx, q.sy, r); g.addColorStop(0, "#ffffff"); g.addColorStop(.25, col); g.addColorStop(1, col); ctx.globalAlpha = Math.max(.55, Math.min(1, 1 - q.z / 900)); ctx.fillStyle = g; ctx.beginPath(); ctx.arc(q.sx, q.sy, r, 0, 6.2832); ctx.fill(); ctx.globalAlpha = 1; if (id === sel || id === hovered) { ctx.strokeStyle = tx; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(q.sx, q.sy, r + 3, 0, 6.2832); ctx.stroke(); } if (P.get(id).pin) { ctx.fillStyle = tx; ctx.beginPath(); ctx.arc(q.sx, q.sy - r - 4, 2, 0, 6.2832); ctx.fill(); } });
      const puestos = [], imp = [...ids].sort((a, b) => (b === raiz) - (a === raiz) || (b === sel) - (a === sel) || (b === hovered) - (a === hovered) || NM.get(b).f - NM.get(a).f);
      ctx.textAlign = "center"; ctx.lineJoin = "round"; imp.forEach(id => { const q = pr.get(id), n = NM.get(id), r = rad(n) * q.s, fs = Math.max(9, Math.min(13, 10.5 * Math.pow(q.s, .6))), t = n.n.length > 24 ? n.n.slice(0, 23) + "…" : n.n; ctx.font = (id === raiz || id === sel ? "700 " : "500 ") + fs + "px system-ui,sans-serif"; const w = ctx.measureText(t).width;
        for (const dy of [r + fs * .95, -(r + fs * .35)]) { const bx = { x: q.sx - w / 2 - 2, y: q.sy + dy - fs, w: w + 4, h: fs + 3 }; if (puestos.some(o => bx.x < o.x + o.w && bx.x + bx.w > o.x && bx.y < o.y + o.h && bx.y + bx.h > o.y) && id !== raiz && id !== sel) continue; puestos.push(bx); ctx.globalAlpha = Math.max(.6, Math.min(1, 1 - q.z / 1000)); ctx.strokeStyle = bg; ctx.lineWidth = 3.5; ctx.strokeText(t, q.sx, q.sy + dy); ctx.fillStyle = tx; ctx.fillText(t, q.sx, q.sy + dy); ctx.globalAlpha = 1; break; } });
    }
    const hit = (mx, my) => { let mejor = null, md = 1e9; pr.forEach((q, id) => { const d = Math.hypot(q.sx - mx, q.sy - my), r = rad(NM.get(id)) * q.s + 5; if (d < r && q.z - r * .01 < md) { md = q.z; mejor = id; } }); return mejor; };
    let drag = null; const pos = e => { const r = cv.getBoundingClientRect(); return [e.clientX - r.left, e.clientY - r.top]; };
    cv.addEventListener("pointerdown", e => { try { cv.setPointerCapture(e.pointerId); } catch (_) { } const [mx, my] = pos(e); drag = { id: hit(mx, my), x: mx, y: my, tot: 0 }; cam.auto = false; cv.style.cursor = "grabbing"; if (drag.id && !P.get(drag.id).pin) alpha = Math.max(alpha, .45); });
    cv.addEventListener("pointermove", e => { const [mx, my] = pos(e); if (!drag) { const h = hit(mx, my); if (h !== hovered) { hovered = h; cv.style.cursor = h ? "pointer" : "grab"; cv.title = h ? NM.get(h).n + " · " + NM.get(h).f + " notas" : ""; } return; }
      const dx = mx - drag.x, dy = my - drag.y; drag.tot += Math.abs(dx) + Math.abs(dy); drag.x = mx; drag.y = my;
      if (drag.id) { const q = P.get(drag.id), s2 = pr.get(drag.id).s, cy = Math.cos(cam.yaw), sy = Math.sin(cam.yaw), cp = Math.cos(cam.pitch), sp = Math.sin(cam.pitch), dxc = dx / s2, dyc = dy / s2, dz1 = -dyc * sp; q.y += dyc * cp; q.x += dxc * cy + dz1 * sy; q.z += -dxc * sy + dz1 * cy; q.pin = true; alpha = Math.max(alpha, .3); }
      else { cam.yaw += dx * .011; cam.pitch = Math.max(-1.45, Math.min(1.45, cam.pitch + dy * .011)); } });
    const fin = () => { if (!drag) return; const d = drag; drag = null; cv.style.cursor = "grab"; guarda(); if (d.tot < 4 && d.id && onSel) onSel(d.id); };
    cv.addEventListener("pointerup", fin); cv.addEventListener("pointercancel", () => { drag = null; });
    cv.addEventListener("dblclick", e => { const [mx, my] = pos(e), h = hit(mx, my); if (h) { P.get(h).pin = false; alpha = Math.max(alpha, .5); guarda(); } });
    cv.addEventListener("wheel", e => { e.preventDefault(); cam.auto = false; cam.zoom = Math.max(.45, Math.min(3, cam.zoom * (e.deltaY < 0 ? 1.1 : .91))); }, { passive: false });
    cv.style.cursor = "grab"; let n = 0;
    (function bucle() { if (!cv.isConnected) { guarda(); return; } if (alpha > .012 || drag) paso(); if (cam.auto) cam.yaw += .0035; dibuja(); if (++n % 90 === 0) guarda(); requestAnimationFrame(bucle); })();
  }
  function curva(serie, paso) {
    if (!serie.length) return `<div class="ml-fa">Sin menciones con fecha.</div>`; const W = 340, H = 150, l = 30, b = 22, mx = Math.max(...serie.map(x => x.n)), t0 = new Date(serie[0].f).getTime(), t1 = new Date(serie[serie.length - 1].f).getTime() || t0 + 1, X = f => l + (W - l - 8) * (serie.length > 1 ? (new Date(f).getTime() - t0) / (t1 - t0) : .5), Y = v => H - b - (H - b - 12) * v / mx;
    const pts = serie.map(x => [X(x.f), Y(x.n)]), lin = pts.map((q, i) => (i ? "L" : "M") + q[0].toFixed(1) + " " + q[1].toFixed(1)).join("");
    return `<svg viewBox="0 0 ${W} ${H}" class="ml-grf"><line x1="${l}" y1="${H - b}" x2="${W - 6}" y2="${H - b}" stroke="currentColor" opacity=".3"/><line x1="${l}" y1="10" x2="${l}" y2="${H - b}" stroke="currentColor" opacity=".3"/><text x="${l - 4}" y="${Y(mx) + 3}" text-anchor="end" font-size="9" fill="currentColor" opacity=".7">${mx}</text><text x="${l - 4}" y="${H - b + 3}" text-anchor="end" font-size="9" fill="currentColor" opacity=".7">0</text>${serie.length > 1 ? `<path d="${lin}L${pts[pts.length - 1][0].toFixed(1)} ${H - b}L${pts[0][0].toFixed(1)} ${H - b}Z" fill="#3b82f6" fill-opacity=".18"/><path d="${lin}" fill="none" stroke="#3b82f6" stroke-width="2"/>` : ""}${pts.map((q, i) => `<circle cx="${q[0].toFixed(1)}" cy="${q[1].toFixed(1)}" r="3" fill="#3b82f6"><title>${esc(fechaC(serie[i].f))}: ${serie[i].n}</title></circle>`).join("")}<text x="${l}" y="${H - 6}" font-size="9" fill="currentColor" opacity=".7">${esc(fechaC(serie[0].f))}</text><text x="${W - 6}" y="${H - 6}" text-anchor="end" font-size="9" fill="currentColor" opacity=".7">${esc(fechaC(serie[serie.length - 1].f))}</text></svg><div class="ml-fa">Menciones por ${paso === "mes" ? "mes" : paso === "semana" ? "semana" : "día"} en las noticias del proyecto.</div>`;
  }
  async function abreEnt(nombre) {
    S.entV = { n: nombre, cargando: true, lay: "fuerza", sel: null }; pintaDet();
    try { const r = await api(`/proyectos/${S.sel}/entidad?n=${encodeURIComponent(nombre)}`); Object.assign(S.entV, r, { cargando: false, sel: r.nodos && r.nodos[0] ? r.nodos[0].id : null }); } catch (e) { S.entV.error = e.message; S.entV.cargando = false; } pintaDet();
  }
  function pintaEnt(d) {
    const x = S.entV, st = d.scrollTop; d.classList.add("wide"); const top = `<button class="x" type="button" data-go="cierra">✕</button><button class="ml-btn2" type="button" data-go="volver" style="margin:0 0 10px">← Volver a la nota</button><h3>Entidad · ${esc(x.n)}</h3>`;
    if (x.cargando) { d.innerHTML = top + `<div class="ml-vacio">Calculando recurrencia y red de relaciones…</div>`; return; } if (x.error) { d.innerHTML = top + `<div class="ml-warn">${esc(x.error)}</div>`; return; }
    const E = x.entidad, raiz = E.id, sel = x.sel || raiz, nodos = x.nodos, nm = new Map(nodos.map(n => [n.id, n])), vec = new Set([sel]); x.enlaces.forEach(e => { if (e.a === sel) vec.add(e.b); if (e.b === sel) vec.add(e.a); });
    const sn = nodos.filter(n => vec.has(n.id)), se = x.enlaces.filter(e => vec.has(e.a) && vec.has(e.b) && (e.a === sel || e.b === sel)), LY = [["fuerza", "Fuerzas"], ["geo", "Geocéntrico"], ["arbol", "Árbol"]];
    d.innerHTML = top + `<div class="ml-bl"><h5>${ic("pin")}Contexto de selección</h5><div class="ml-fa"><span>Entidad:</span> <b>${esc(E.n)}</b> <span>· Tipo:</span> <b>${E.t === "pers" ? "persona" : E.t === "inst" ? "institución" : "tópico"}</b> <span>· Notas que la mencionan:</span> <b>${E.notas}</b></div>
      <div style="margin-top:6px"><button class="ml-btn2" type="button" data-ent="${esc(E.t + ":" + E.n)}">Filtrar el tablero por esta entidad</button></div>${(x.recientes || []).length ? `<div class="ml-fa" style="margin-top:8px"><span>Notas recientes:</span></div>${x.recientes.map(n => `<div class="ml-rec" data-n="${esc(n.id)}"><b>${esc(n.med)}</b> · ${esc(fechaC(n.f))}<br>${esc(n.t)}</div>`).join("")}` : ""}</div>
      <div class="ml-bl"><h5>${ic("trend")}Incidencia (curva de menciones)</h5>${curva(x.serie, x.paso)}</div>
      <div class="ml-bl"><h5>${ic("net")}Red de relaciones de entidades</h5><div class="ml-lay">${LY.map(([k, t]) => `<button type="button" data-lay="${k}" class="${x.lay === k ? "on" : ""}">${t}</button>`).join("")}</div>
        ${nodos.length > 1 ? `<div class="ml-fa">Grafo principal · Nodos: ${nodos.length} · Enlaces: ${x.enlaces.length}</div><canvas class="ml-g3" id="gMain" style="height:340px"></canvas>
        <div class="ml-fa">Subgrafo (nodo seleccionado: <b>${esc((nm.get(sel) || {}).n || "")}</b>) · Nodos: ${sn.length} · Enlaces: ${se.length}</div><canvas class="ml-g3" id="gSub" style="height:260px"></canvas>
        <div class="ml-fa"><button class="ml-btn2" type="button" data-g3reset="1" style="margin:0 6px 6px 0">↺ Restablecer vista</button>Arrastra un nodo para moverlo (queda fijado; doble clic lo suelta) · arrastra el fondo para girar en 3D · rueda para acercar · clic en un nodo para ver su subgrafo. Color: <span style="color:${TCOL.pers}">● persona</span> <span style="color:${TCOL.inst}">● institución</span> <span style="color:${TCOL.top}">● tópico</span>. Tamaño = notas; grosor = veces que aparecen juntos.</div>` : `<div class="ml-fa">Esta entidad aún no coincide con otras en las mismas notas, por eso no hay red.</div>`}</div>
      ${nodos.length > 1 ? (() => { const L = [...nodos].sort((a, b) => b.c - a.c), mx = Math.max(1, L[0].c), tn = t => t === "pers" ? "persona" : t === "inst" ? "institución" : "tópico"; return `<div class="ml-bl"><h5>${ic("bar")}Centralidad · quién conecta más</h5><div class="ml-cen">${L.map((n, i) => `<div class="ml-cr${n.id === sel ? " on" : ""}" data-gn="${esc(n.id)}"><span class="rk">${i + 1}</span><span class="dt" style="background:${TCOL[n.t] || "#22d3ee"}" title="${tn(n.t)}"></span><div class="nm"><b>${esc(n.n)}</b><small>${n.f} ${n.f === 1 ? "nota" : "notas"} · ${n.g} ${n.g === 1 ? "conexión" : "conexiones"} · peso ${n.gp}</small></div><div class="bw"><div class="bb"><i style="width:${Math.max(4, n.c / mx * 100).toFixed(0)}%;background:${TCOL[n.t] || "#22d3ee"}"></i></div><em>${n.c}%</em></div><a href="#" class="go" data-ver="${esc(n.n)}" title="Abrir la ficha de esta entidad">${ic("arrow")}</a></div>`).join("")}</div><div class="ml-fa">La barra muestra qué parte del peso total de vínculos de la red concentra cada entidad. «Conexiones» son las entidades con las que coincide en las mismas notas. Toca una fila para resaltarla en el grafo; la flecha abre su ficha.</div></div>` })() : ""}`;
    d.scrollTop = st;
    if (nodos.length > 1) { const kp = x.keep || (x.keep = {}), onSel = id => { x.sel = id; pintaDet(); }, m1 = $("#gMain", d), m2 = $("#gSub", d); if (m1) grafo3D(m1, nodos, x.enlaces, x.lay, raiz, sel, kp["main:" + x.lay] || (kp["main:" + x.lay] = {}), onSel); if (m2) grafo3D(m2, sn, se, "geo", sel, sel, kp["sub:" + sel] || (kp["sub:" + sel] = {}), onSel); }
  }

  /* ───────── mapa ───────── */
  async function mapaDatos() { const p = qs(); p.set("geo", "1"); S.geo = await api(`/proyectos/${S.sel}/noticias?${p}`); }
  function leafletListo(cb) { if (window.L) return cb(); if (window._rsvEnsureLeaflet_) return window._rsvEnsureLeaflet_(cb); const l = document.createElement("link"); l.rel = "stylesheet"; l.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"; document.head.appendChild(l); const s = document.createElement("script"); s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"; s.onload = cb; document.head.appendChild(s); }
  /* araña: al pasar el cursor por un pin agrupado, sus noticias se abren en círculo alrededor */
  function aranaCierra(M) { if (!M) return; if (M._ml_ov) M._ml_ov.remove(); M._ml_ov = null; if (M._ml_off) { M._ml_off(); M._ml_off = null; } }
  function resaltaCard(id) {
    const c = $$(".ml-card").find(x => x.dataset.n === id);
    if (!c) { abreDet(id); return; }
    $$(".ml-card.sel").forEach(x => x.classList.remove("sel")); void c.offsetWidth; c.classList.add("sel"); c.scrollIntoView({ block: "center", behavior: "smooth" });
    setTimeout(() => c.classList.remove("sel"), 4000);
  }
  function arana(M, mapEl, L2, ll) {
    if (M._ml_ov && M._ml_key === ll.join()) return;
    aranaCierra(M); M._ml_key = ll.join();
    const c = M.latLngToContainerPoint(ll), items = L2.slice().sort((a, b) => (b.r == null ? -1 : b.r) - (a.r == null ? -1 : a.r));
    const rings = [[110, 6], [200, 11], [290, 17]], cap = rings.reduce((t, r) => t + r[1], 0), show = items.slice(0, items.length > cap ? cap - 1 : cap), extra = items.length - show.length;
    const ov = document.createElement("div"); ov.className = "ml-sp"; ov.innerHTML = "<svg></svg>"; mapEl.appendChild(ov); M._ml_ov = ov; const svg = ov.firstChild;
    const all = show.map(q => ({ q })); if (extra > 0) all.push({ more: extra });
    let ix = 0, maxR = 0;
    rings.forEach(([R, n]) => {
      const seg = all.slice(ix, ix + n); ix += n; if (!seg.length) return; maxR = R;
      seg.forEach((o, k) => {
        const a = -Math.PI / 2 + 2 * Math.PI / seg.length * k, x = c.x + Math.cos(a) * R, y = c.y + Math.sin(a) * R;
        const ln = document.createElementNS("http://www.w3.org/2000/svg", "line"); ln.setAttribute("x1", c.x); ln.setAttribute("y1", c.y); ln.setAttribute("x2", x); ln.setAttribute("y2", y); svg.appendChild(ln);
        const ch = document.createElement("div"), q = o.q; ch.style.left = x + "px"; ch.style.top = y + "px";
        if (q) {
          const nv = q.r == null ? -1 : q.r >= 75 ? 3 : q.r >= 50 ? 2 : q.r >= 25 ? 1 : 0; ch.className = "ml-sp-ch ml-rk" + nv; ch.title = q.t;
          ch.innerHTML = `<i style="background:${catCol(q.cat)}"></i><b>${esc(corto(q.t))}</b><small>${esc(q.med)} · ${esc(fechaC(q.f))}</small>`;
          ch.onclick = ev => { ev.stopPropagation(); resaltaCard(q.id); };
        } else {
          ch.className = "ml-sp-ch ml-sp-more"; ch.innerHTML = `<b>+${o.more} más</b><small>ver lista</small>`;
          ch.onclick = ev => { ev.stopPropagation(); aranaCierra(M); S.det = { lista: { nombre: L2[0].lug || "", items: L2.slice(0, 60) } }; pintaDet(); };
        }
        ov.appendChild(ch); requestAnimationFrame(() => ch.classList.add("in"));
      });
    });
    const mv = ev => { const r = mapEl.getBoundingClientRect(), dx = ev.clientX - r.left - c.x, dy = ev.clientY - r.top - c.y; if (Math.hypot(dx, dy) > maxR + 80) aranaCierra(M); };
    mapEl.addEventListener("mousemove", mv); const mz = () => aranaCierra(M); M.on("movestart zoomstart", mz);
    M._ml_off = () => { mapEl.removeEventListener("mousemove", mv); M.off("movestart zoomstart", mz); };
  }
  const p0n = p => p.lug || "";
  const corto = t => { const w = String(t || "").replace(/\s+[-–|]\s+[^-–|]{2,40}$/, "").split(/\s+/); return w.length > 7 ? w.slice(0, 7).join(" ") + "…" : w.join(" "); };
  const CATCOLS = ["#2563eb", "#7c3aed", "#0891b2", "#db2777", "#65a30d", "#0d9488", "#9333ea", "#ca8a04", "#4f46e5", "#be185d", "#0369a1", "#15803d"];
  const catCol = c => { let h = 0; for (const ch of String(c || "")) h = (h * 31 + ch.charCodeAt(0)) >>> 0; return CATCOLS[h % CATCOLS.length]; };
  /* «Ver en mapa»: centra el mapa en la noticia, resalta su pin y, si está agrupado, abre la araña */
  function enfocaMapa() {
    const f = S.foco, M = S.mapa; S.foco = null; if (!f || !M) return; const el = $("#mlMap"); if (!el) return;
    const g = (S.marcas || []).find(m => m.L2.some(q => q.id === f.id)), ll = g ? g.ll : [f.lat, f.lng];
    M.invalidateSize(); M.setView(ll, Math.max(M.getZoom(), 13), { animate: false });
    if (!g) return; const ic2 = g.mk.getElement(); if (ic2) { ic2.classList.remove("ml-foco"); void ic2.offsetWidth; ic2.classList.add("ml-foco"); setTimeout(() => ic2.classList.remove("ml-foco"), 5000); }
    if (g.L2.length > 1) setTimeout(() => arana(M, el, g.L2, g.ll), 120); resaltaCard(f.id);
  }
  function mapa() {
    const el = $("#mlMap"); if (!el) return;
    leafletListo(() => {
      const el2 = $("#mlMap"); if (!el2 || !window.L) return; const L = window.L, g = S.geo || { puntos: [], total: 0 }, M = L.map(el2, { zoomControl: true }).setView([23.6, -102.5], 5);
      L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 18, attribution: "© OpenStreetMap" }).addTo(M); S.mapa = M;
      S.marcas = [];
      const grupos = new Map(); g.puntos.forEach(p => { const k = p.lat.toFixed(3) + "," + p.lng.toFixed(3); (grupos.get(k) || grupos.set(k, []).get(k)).push(p); });
      const pts = [];
      grupos.forEach(L2 => {
        const top = L2.reduce((m, q) => (q.r != null && (m.r == null || q.r > m.r)) ? q : m, L2[0]), uno = L2.length === 1, nom = p0n(L2[0]), rk = top.r, nvl = rk == null ? -1 : rk >= 75 ? 3 : rk >= 50 ? 2 : rk >= 25 ? 1 : 0;
        const cc = {}; L2.forEach(q => cc[q.cat || ""] = (cc[q.cat || ""] || 0) + 1); const cat = Object.keys(cc).sort((a, b) => cc[b] - cc[a])[0], col = catCol(cat), p0 = L2[0];
        const ic = L.divIcon({ className: "", html: `<div class="ml-pin" style="background:${col}"><b>${uno ? "" : L2.length > 99 ? "99+" : L2.length}</b></div>`, iconSize: [26, 26], iconAnchor: [13, 28] });
        const mk = L.marker([p0.lat, p0.lng], { icon: ic }).addTo(M);
        mk.bindTooltip(`<b>${esc(corto(top.t))}</b><small>${esc(nom)}${uno ? "" : " · " + L2.length + " noticias"}</small>`, { permanent: true, direction: "right", offset: [12, -14], className: "ml-tip ml-rk" + nvl });
        if (!uno) mk.on("mouseover", () => arana(M, el2, L2, [p0.lat, p0.lng]));
        mk.on("click", () => { if (uno) abreDet(p0.id); else { S.det = { lista: { nombre: nom, items: L2.slice(0, 60) } }; pintaDet(); } }); pts.push([p0.lat, p0.lng]); S.marcas.push({ mk, L2, ll: [p0.lat, p0.lng] });
      });
      const nl = () => el2.classList.toggle("ml-nolbl", grupos.size > 40 && M.getZoom() < 9); M.on("zoomend", nl); nl();
      if (S.foco) { setTimeout(enfocaMapa, 250); } else if (pts.length) M.fitBounds(pts, { padding: [40, 40], maxZoom: 11 }); setTimeout(() => M.invalidateSize(), 200);
      const n = $("#mlMapN"); if (n) n.innerHTML = `<span>📍 ${nf(g.total)} noticias georreferenciadas en ${nf(grupos.size)} lugares</span><span>Nivel: punto (coordenada), zona/colonia, municipio o estado; los pines agrupan por lugar.</span>` + (g.total > 4000 ? `<span>Se muestran las 4,000 más recientes.</span>` : "");
    });
  }

  /* ───────── costos: uso de las APIs por servicio, mes, día y proyecto ───────── */
  const USD = n => "US$ " + Number(n || 0).toLocaleString("es-MX", { minimumFractionDigits: n < 1 && n > 0 ? 3 : 2, maximumFractionDigits: n < 1 && n > 0 ? 3 : 2 });
  const iso = d => d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0");
  const rangoCos = () => { const h = new Date(), p = S.cos.periodo; if (p === "mes") return [iso(new Date(h.getFullYear(), h.getMonth(), 1)), iso(h)]; if (p === "mesant") return [iso(new Date(h.getFullYear(), h.getMonth() - 1, 1)), iso(new Date(h.getFullYear(), h.getMonth(), 0))]; if (p === "30d") return [iso(new Date(h.getTime() - 30 * 864e5)), iso(h)]; return ["0000-00-00", "9999-99-99"]; };
  async function abreCostos() { S.cos = S.cos || { periodo: "mes", proy: "", data: null }; S.modo = "costos"; titulo("Costos de las APIs"); lado(); mlWin(true); principal(); await cargaCostos(); }
  async function cargaCostos() { const [d, h] = rangoCos(); try { S.cos.data = await api(`/costos?desde=${d}&hasta=${h}${S.cos.proy ? "&proy=" + S.cos.proy : ""}`); } catch (e) { S.cos.data = null; toast(e.message, true); } if (S.modo === "costos") principal(); }
  function costos() {
    const m = $("#mlMain"), C = S.cos || (S.cos = { periodo: "mes", proy: "", data: null }), D = C.data; let h = `<div class="ml-tb"><div class="ml-f" style="margin:0;min-width:190px"><label>Periodo</label><select data-cos="periodo">${[["mes", "Este mes"], ["mesant", "Mes anterior"], ["30d", "Últimos 30 días"], ["todo", "Todo el historial"]].map(([v, t]) => `<option value="${v}"${C.periodo === v ? " selected" : ""}>${t}</option>`).join("")}</select></div><div class="ml-f" style="margin:0;min-width:220px"><label>Proyecto</label><select data-cos="proy"><option value="">Todos</option>${S.proy.map(p => `<option value="${p.id}"${C.proy === p.id ? " selected" : ""}>${esc(p.titulo)}</option>`).join("")}</select></div><span class="ml-sp"></span><button class="ml-btn2" type="button" data-go="recargaCos">↻ Actualizar</button></div>`;
    if (!D) { m.innerHTML = h + `<div class="ml-vacio">Cargando costos…</div>`; return; }
    const T = D.total, mx = Math.max(0.0001, ...Object.values(D.porMes).map(x => x.costo)), meses = Object.keys(D.porMes).sort().reverse().slice(0, 12), dias = Object.keys(D.porDia).sort().slice(-31), mxd = Math.max(0.0001, ...dias.map(k => D.porDia[k].costo));
    const SV = D.servicios, svs = Object.keys(D.porSvc).filter(k => k !== "notas").sort((a, b) => D.porSvc[b].costo - D.porSvc[a].costo || D.porSvc[b].n - D.porSvc[a].n);
    const cpn = T.notas ? T.costo / T.notas : 0;
    h += `<div class="ml-kp"><div><b>${USD(T.costo)}</b><span>costo estimado del periodo</span></div><div><b>${nf(T.llamadas)}</b><span>llamadas a APIs y descargas</span></div><div><b>${nf(T.in + T.out)}</b><span>tokens de IA (${nf(T.in)} entrada · ${nf(T.out)} salida)</span></div><div><b>${nf(T.notas)}</b><span>notas nuevas guardadas${T.notas ? " · " + USD(cpn) + " por nota" : ""}</span></div></div>`;
    h += `<div class="ml-pn"><h3>Por servicio</h3><p>Qué API se usó, cuántas veces y cuánto costaría con las tarifas de abajo.</p><div style="overflow:auto"><table class="ml-tbl"><thead><tr><th>Servicio</th><th class="r">Llamadas</th><th class="r">Unidades</th><th class="r">Costo est.</th></tr></thead><tbody>${svs.length ? svs.map(k => { const e = D.porSvc[k]; return `<tr><td>${esc(SV[k] || k)}</td><td class="r">${nf(e.n)}</td><td class="r">${k === "ia" ? nf(e.in) + " ent. / " + nf(e.out) + " sal. · " + nf(e.k) + " notas" : "—"}</td><td class="r"><b>${USD(e.costo)}</b></td></tr>`; }).join("") : `<tr><td colspan="4" class="ml-mut">Sin uso registrado en este periodo.</td></tr>`}</tbody></table></div></div>`;
    h += `<div class="ml-pn"><h3>Por mes</h3><p>Costo estimado y llamadas de los últimos 12 meses.</p>${meses.length ? meses.map(k => { const e = D.porMes[k]; return `<div class="ml-cbar"><span>${esc(k)}</span><div><i style="width:${Math.max(2, Math.round(e.costo / mx * 100))}%"></i></div><b>${USD(e.costo)}</b><small>${nf(e.llamadas)} llamadas · ${nf(e.notas)} notas</small></div>`; }).join("") : `<div class="ml-mut">Aún no hay datos.</div>`}</div>`;
    h += `<div class="ml-pn"><h3>Por día</h3><p>Últimos días del periodo elegido.</p>${dias.length ? `<div class="ml-dias">${dias.map(k => `<div title="${esc(k)} · ${USD(D.porDia[k].costo)} · ${nf(D.porDia[k].llamadas)} llamadas"><i style="height:${Math.max(3, Math.round(D.porDia[k].costo / mxd * 100))}%"></i><small>${k.slice(8)}</small></div>`).join("")}</div>` : `<div class="ml-mut">Sin datos en este periodo.</div>`}</div>`;
    const pr = Object.entries(D.porProy).sort((a, b) => b[1].costo - a[1].costo);
    h += `<div class="ml-pn" id="mlCosProy"><h3>Por proyecto</h3><div style="overflow:auto"><table class="ml-tbl"><thead><tr><th>Proyecto</th><th class="r">Llamadas</th><th class="r">Notas nuevas</th><th class="r">Costo est.</th></tr></thead><tbody>${pr.length ? pr.map(([k, e]) => `<tr><td>${esc(e.titulo)}</td><td class="r">${nf(e.llamadas)}</td><td class="r">${nf(e.notas)}</td><td class="r"><b>${USD(e.costo)}</b></td></tr>`).join("") : `<tr><td colspan="4" class="ml-mut">Sin datos.</td></tr>`}</tbody></table></div></div>`;
    const t = D.tarifas, mods = Object.keys(t.ia || {}).filter(k => k !== "def"), mesK = iso(new Date()).slice(0, 7), gastoMes = ((D.porMes || {})[mesK] || {}).costo || 0, lim = +D.limite || 0, pcl = lim ? Math.round(gastoMes / lim * 100) : null;
    const modNom = k => /haiku/i.test(k) ? ["Claude Haiku · IA rápida y económica", "Clasifica, resume y extrae personas, lugares y riesgos de cada noticia."] : /sonnet/i.test(k) ? ["Claude Sonnet · IA más potente", "Se usa cuando la IA rápida no logra resolver una noticia."] : [k, "Modelo de IA"];
    const OTR = { places: ["Google Places", "Ubica en el mapa los lugares que menciona la noticia", "consulta"], newsdata: ["NewsData.io", "Proveedor opcional de noticias", "consulta"], gnews: ["GNews", "Proveedor opcional de noticias", "consulta"], texto: ["Descarga de páginas de medios", "Baja el texto completo de cada nota", "página"], gnres: ["Enlaces de Google Noticias", "Obtiene el enlace real de cada nota", "enlace"], gdelt: ["GDELT", "Proveedor de noticias (gratuito)", "consulta"], grss: ["Google Noticias (RSS)", "Proveedor de noticias (gratuito)", "consulta"] };
    const usd = (att, val, st) => `<span class="ml-ig"><i>US$</i><input type="number" step="${st}" min="0" ${att} value="${val}"></span>`;
    const panelLim = `<div class="ml-pn" id="mlCosLim"><h3>Límite de gasto mensual</h3><p>Es el monto máximo que <b>tú decides</b> gastar al mes en estas APIs, en <b>dólares estadounidenses (US$)</b>. Es un aviso: al llegar al 80 % y al 100 % verás la alerta aquí; <b>no detiene</b> las extracciones. Cada proyecto tiene además su propio presupuesto en <b>📁 Proyectos › ⚙️ Gestionar</b>.</p>
      <div class="ml-lim"><div class="ml-gr"><span class="ml-gk0">Mi límite mensual</span>${usd('data-lim="1" placeholder="0.00"', lim || "", "1")}<span class="ml-mut">(0 o vacío = sin límite)</span></div>
      <div class="ml-limv"><div><b>${USD(gastoMes)}</b><span>gastado en ${esc(mesK)}</span></div><div><b>${lim ? USD(lim) : "Sin límite"}</b><span>límite mensual</span></div><div><b>${lim ? Math.max(0, 100 - pcl) + " %" : "—"}</b><span>disponible</span></div></div>
      ${lim ? `<div class="ml-cst"><div class="bar${pcl >= 100 ? " x" : pcl >= 80 ? " w" : ""}"><i style="width:${Math.min(100, pcl)}%"></i></div><small>${pcl} % del límite${pcl >= 100 ? " · límite superado" : pcl >= 80 ? " · cerca del límite" : ""}</small></div>` : ""}</div></div>`;
    h = h.replace('<div class="ml-pn" id="mlCosProy"><h3>Por proyecto</h3>', panelLim + '<div class="ml-pn" id="mlCosProy"><h3>Por proyecto</h3>');
    h += `<div class="ml-pn" id="mlCosTar"><h3>Tarifas de referencia (editables)</h3><p>Todos los montos están en <b>dólares estadounidenses (US$)</b>. El costo mostrado es <b>estimado</b>: cada llamada que hace el sistema se multiplica por la tarifa de esta tabla. Cámbialas por las de tu plan y todo el historial se recalcula. No incluye Cloud Run ni Cloud Storage, y no descuenta la cuota gratuita mensual que Google pueda darte.</p>
      <div style="overflow:auto"><table class="ml-tbl ml-tar"><thead><tr><th>Servicio</th><th>¿Para qué se usa?</th><th>¿Cómo se cobra?</th><th>Tarifa</th></tr></thead><tbody>
      ${mods.map(k => { const [nm, ds] = modNom(k); return `<tr><td><b>${esc(nm)}</b><small>${esc(k)}</small></td><td>${esc(ds)}</td><td>Por <b>millón de tokens</b><small>un token es ¾ de palabra aprox.</small></td><td><div class="ml-tr2"><label>Entrada <small>(lo que se le envía)</small>${usd(`data-tar="ia:${esc(k)}:in"`, t.ia[k].in, "0.01")}</label><label>Salida <small>(lo que responde)</small>${usd(`data-tar="ia:${esc(k)}:out"`, t.ia[k].out, "0.01")}</label></div></td></tr>`; }).join("")}
      ${["places", "newsdata", "gnews", "texto", "gnres", "gdelt", "grss"].map(k => `<tr><td><b>${esc(OTR[k][0])}</b></td><td>${esc(OTR[k][1])}</td><td>Por <b>${OTR[k][2]}</b></td><td>${usd(`data-tar="${k}"`, t[k] || 0, "0.0001")}</td></tr>`).join("")}</tbody></table></div><button class="ml-btn2" type="button" data-go="guardaTar" style="margin-top:10px">Guardar tarifas y límite</button></div>`;
    m.innerHTML = h;
  }
  async function guardaTarifas() {
    const D = S.cos && S.cos.data; if (!D) return; const t = { ia: {} }; $$("[data-tar]", $("#mlMain")).forEach(i => { const k = i.dataset.tar.split(":"); if (k[0] === "ia") { (t.ia[k[1]] = t.ia[k[1]] || {})[k[2]] = +i.value || 0; } else t[k[0]] = +i.value || 0; });
    const L0 = $("[data-lim]", $("#mlMain")); try { await post("/costos/tarifas", { tarifas: t, limite: L0 ? (+L0.value || 0) : undefined }); toast("Tarifas y límite guardados"); await cargaCostos(); } catch (e) { toast(e.message, true); }
  }
  /* ───────── asistente: crear / editar proyecto ───────── */
  const PASOS = ["1 · Proyecto", "2 · Términos", "3 · Zona", "4 · Periodo y volumen", "5 · Periodicidad", "6 · Lanzar"];
  const WNEW = () => ({ paso: 0, id: null, creador: (() => { try { return localStorage.getItem("ml_creador") || ""; } catch (_) { return ""; } })(), titulo: "", objetivo: "", terminos: "", estados: [], ciudades: "", estricta: false, tipo: "movil", dias: 7, desde: "", hasta: "", max: 5000, idiomas: ["spanish"], medios: "", proveedor: "grss", periodicidad: "manual", texto: true, textoMax: 500, ia: true, iaMax: 0, geo: false, est: null, lanzando: null });
  /* selector de medios: lista de los medios ya guardados (crece con cada extracción), con búsqueda y selección múltiple */
  const medSel = () => String((S.W && S.W.medios) || "").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
  const medNom = d => { const m = (S.medios || []).find(x => x.d === d); return m ? m.nom : d; };
  function medChips() { return medSel().map(d => `<span class="ml-mch" title="${esc(d)}">${esc(medNom(d))}<button type="button" data-medx="${esc(d)}" aria-label="Quitar">✕</button></span>`).join(""); }
  function medLista(q) {
    const sel = new Set(medSel()), t = String(q || "").trim().toLowerCase(), L = (S.medios || []).filter(m => !t || m.d.includes(t) || String(m.nom).toLowerCase().includes(t));
    if (!S.medios) return `<div class="ml-mvac">Cargando medios…</div>`;
    return (L.length ? L.map(m => `<div class="ml-mi${sel.has(m.d) ? " on" : ""}" data-med="${esc(m.d)}"><span class="ck">✓</span><span class="nm"><b>${esc(m.nom)}</b><small>${esc(m.d)}</small></span><em>${nf(m.n)} ${m.n === 1 ? "nota" : "notas"}</em></div>`).join("") : "") + (t && !L.some(m => m.d === t) && /\./.test(t) ? `<div class="ml-mi nuevo" data-med="${esc(t)}"><span class="ck">＋</span><span class="nm"><b>Agregar «${esc(t)}»</b><small>medio nuevo</small></span></div>` : "") || `<div class="ml-mvac">Aún no hay medios guardados. Escribe un dominio (p. ej. milenio.com) y pulsa Enter.</div>`;
  }
  const mediosHtml = () => `<div class="ml-mdd" id="mlMdd"><div class="ml-mbox"><span id="mlMch" class="ml-mchs">${medChips()}</span><input type="text" id="mlMq" autocomplete="off" placeholder="${medSel().length ? "Agregar otro medio…" : "Buscar medio o escribir un dominio…"}"></div><div class="ml-mlist" id="mlMlist" hidden>${medLista("")}</div></div>`;
  function medRefresca() { const c = $("#mlMch"), l = $("#mlMlist"), q = $("#mlMq"); if (c) c.innerHTML = medChips(); if (l) l.innerHTML = medLista(q ? q.value : ""); }
  function medToggle(d) { d = String(d).trim().toLowerCase().replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/.*$/, ""); if (!d || !S.W) return; const a = medSel(), i = a.indexOf(d); i >= 0 ? a.splice(i, 1) : a.push(d); S.W.medios = a.join(", "); if (!(S.medios || []).some(m => m.d === d)) (S.medios = S.medios || []).push({ d, nom: d, n: 0 }); medRefresca(); }
  async function cargaMedios() { try { S.medios = (await api("/medios")).medios || []; } catch (_) { S.medios = S.medios || []; } if (S.modo === "crear") { const l = $("#mlMlist"); if (l) { const q = $("#mlMq"); l.innerHTML = medLista(q ? q.value : ""); const c = $("#mlMch"); if (c) c.innerHTML = medChips(); } } }
  function nuevo() { S.W = WNEW(); S.modo = "crear"; titulo("Crear proyecto"); lado(); mlWin(true); principal(); if (!S.prov) cargaProv(); cargaMedios(); }
  async function cargaProv() { try { S.prov = await api("/proveedores"); } catch (e) { S.prov = { proveedores: {}, estados: {}, error: e.message }; } if (S.modo === "crear") principal(); }
  async function editar(id) {
    cargaMedios();
    try { const r = await api(`/proyectos/${id}?terminos=1`), p = r.proyecto, c = p.config || {}; S.W = Object.assign(WNEW(), { id, creador: p.creador || (() => { try { return localStorage.getItem("ml_creador") || ""; } catch (_) { return ""; } })(), titulo: p.titulo, objetivo: p.objetivo || "", terminos: (c.terminos || []).join("\n"), estados: (c.zona && c.zona.estados) || [], ciudades: ((c.zona && c.zona.ciudades) || []).join(", "), estricta: !!(c.zona && c.zona.estricta), tipo: c.periodo && c.periodo.tipo === "fijo" ? "fijo" : "movil", dias: (c.periodo && c.periodo.dias) || 7, desde: (c.periodo && c.periodo.desde || "").slice(0, 10), hasta: (c.periodo && c.periodo.hasta || "").slice(0, 10), max: c.maxNoticias || 5000, idiomas: c.idiomas || ["spanish"], medios: (c.medios || []).join(", "), proveedor: c.proveedor || "gdelt", periodicidad: c.periodicidad || "manual", texto: c.texto !== false, textoMax: c.textoMax == null ? 500 : c.textoMax, ia: c.ia !== false, iaMax: c.iaMax || 0, geo: !!c.geocodificar }); S.modo = "crear"; titulo("Editar proyecto"); lado(); mlWin(true); principal(); if (!S.prov) cargaProv(); } catch (e) { toast(e.message, true); }
  }
  const cuentaT = txt => { const L = String(txt || "").split(/[\n,;]+/).map(x => x.trim()).filter(Boolean), v = new Set(); let pos = 0, neg = 0, dup = 0; L.forEach(t => { const ng = t.startsWith("-"), k = norm(ng ? t.slice(1) : t).trim(); if (k.length < 3) return; if (v.has(k)) { dup++; return; } v.add(k); ng ? neg++ : pos++; }); return { pos, neg, dup }; };
  function wizard() {
    const m = $("#mlMain"), W = S.W || (S.W = WNEW()), P = S.prov || { proveedores: {}, estados: {} };
    const pasos = `<div class="ml-steps">${PASOS.map((t, i) => `<span class="${i === W.paso ? "on" : i < W.paso ? "ok" : ""}">${t}</span>`).join("")}</div>`; let h = "";
    if (W.paso === 0) h = `<div class="ml-pn"><h3>Proyecto</h3><p>Dale un título y describe qué quieres vigilar en los medios.</p><div class="ml-f"><label>Creado por (nombre de quien crea el proyecto)</label><input data-w="creador" value="${esc(W.creador)}" maxlength="60" placeholder="Ej. Andrés Carreón"></div><div class="ml-f"><label>Título del proyecto *</label><input data-w="titulo" value="${esc(W.titulo)}" maxlength="140" placeholder="Ej. Obra pública y agua en Saltillo"></div><div class="ml-f"><label>Objetivo *</label><textarea data-w="objetivo" style="min-height:90px" placeholder="¿Para qué se monitorea? ¿Qué decisiones apoya?">${esc(W.objetivo)}</textarea></div></div>`;
    else if (W.paso === 1) { const c = cuentaT(W.terminos); h = `<div class="ml-pn"><h3>Términos y tópicos de interés</h3><p>Uno por línea (o separados por coma). Admite <b>miles</b> de términos: se agrupan en consultas automáticamente. Una frase se busca exacta; anteponer <b>-</b> excluye (ej. <i>-futbol</i>).</p><div class="ml-f"><textarea data-w="terminos" style="min-height:260px" placeholder="alcalde de Saltillo&#10;desabasto de agua&#10;Parque Las Maravillas&#10;-futbol">${esc(W.terminos)}</textarea></div><div class="ml-sem"><span><b>${nf(c.pos)}</b> términos válidos</span><span>${nf(c.neg)} exclusiones</span><span>${nf(c.dup)} repetidos omitidos</span><span>máx. 20,000</span></div><div class="ml-f"><label>Importar desde archivo (.txt o .csv, un término por línea)</label><input type="file" id="mlArch" accept=".txt,.csv,text/plain"></div></div>`; }
    else if (W.paso === 2) { const ed = P.estados || {}; h = `<div class="ml-pn"><h3>Zona geográfica de interés</h3><p>Elige estados y/o escribe ciudades o colonias. Sin zona, se busca en todo el país.</p>${S.ctx && S.ctx.ent ? `<button class="ml-btn2" type="button" data-go="zonaGeo">📍 Usar el lugar del proyecto de GeoLab (${esc(S.ctx.municipio || S.ctx.estado || S.ctx.ent)})</button>` : ""}<div class="ml-h4">Estados</div><div class="ml-grid">${Object.keys(ed).sort((x, y) => String(ed[x]).localeCompare(String(ed[y]), "es")).map(k => `<div class="ml-ck${W.estados.includes(k) ? " on" : ""}" data-edo="${k}"><i>✓</i>${esc(ed[k])}</div>`).join("") || "<span class='ml-sem'>Cargando…</span>"}</div><div class="ml-f" style="margin-top:10px"><label>Ciudades, municipios o colonias (separados por coma)</label><input data-w="ciudades" value="${esc(W.ciudades)}" placeholder="Saltillo, Ramos Arizpe, Arteaga"></div><div class="ml-ck${W.estricta ? " on" : ""}" data-tg="estricta"><i>✓</i>Conservar solo noticias ubicadas en estas zonas (filtro estricto)</div></div>`; }
    else if (W.paso === 3) { const pv = P.proveedores || {}, e = W.est; h = `<div class="ml-pn"><h3>Intervalo de tiempo, volumen y fuente de noticias</h3>
      <div class="ml-seg"><button type="button" data-w2="tipo:movil" class="${W.tipo === "movil" ? "on" : ""}">Últimos N días (móvil)</button><button type="button" data-w2="tipo:fijo" class="${W.tipo === "fijo" ? "on" : ""}">Rango fijo</button></div>
      ${W.tipo === "movil" ? `<div class="ml-f"><label>Días hacia atrás</label><input type="number" min="1" max="3650" data-w="dias" value="${W.dias}"></div>` : `<div class="ml-row2"><div class="ml-f"><label>Desde</label><input type="date" data-w="desde" value="${esc(W.desde)}"></div><div class="ml-f"><label>Hasta</label><input type="date" data-w="hasta" value="${esc(W.hasta)}"></div></div>`}
      <div class="ml-row2"><div class="ml-f"><label>Cantidad máxima de noticias</label><select data-w="max">${[1000, 5000, 20000, 50000].map(v => `<option value="${v}"${+W.max === v ? " selected" : ""}>${nf(v)}</option>`).join("")}</select></div><div class="ml-f"><label>Idioma</label><select data-w="idioma"><option value="spanish"${W.idiomas[0] === "spanish" ? " selected" : ""}>Español</option><option value="english"${W.idiomas[0] === "english" ? " selected" : ""}>Inglés</option></select></div></div>
      <div class="ml-f"><label>Solo estos medios (elige de los que ya tienes guardados o escribe un dominio; vacío = todos)</label>${mediosHtml()}</div>
      <div class="ml-f"><label>Servicio de noticias</label><select data-w="proveedor">${Object.keys(pv).map(k => `<option value="${k}"${W.proveedor === k ? " selected" : ""}${pv[k].ok ? "" : " disabled"}>${esc(pv[k].n)}${pv[k].ok ? "" : " — requiere " + esc(pv[k].clave)}</option>`).join("")}</select></div>
      ${pv[W.proveedor] ? `<div class="ml-sem"><span>${esc(pv[W.proveedor].nota)}</span></div>` : ""}
      <button class="ml-btn2" type="button" data-go="estimar">📊 Estimar cantidad de noticias y tiempo</button>
      ${W.estimando ? `<div class="ml-est">Estimando con una muestra real…</div>` : e ? `<div class="ml-est"><b>${nf(e.terminos)}</b> términos → <b>${nf(e.grupos)}</b> grupos de consulta (~${nf(e.consultas)} consultas, ≈ <b>${nf(e.minutos)} min</b>).<br>Muestra: ${e.muestra.map(x => `${nf(x.n)}${x.saturado ? "+" : ""} notas`).join(" · ") || "—"} · estimado total ≈ <b>${nf(e.notasEstimadas)}</b>${e.saturado ? " (consultas saturadas: se subdividen por fechas)" : ""}.${(e.advertencias || []).map(a => `<div class="ml-warn">${esc(a)}</div>`).join("")}</div>` : ""}</div>`; }
    else if (W.paso === 4) h = `<div class="ml-pn"><h3>Periodicidad de la extracción</h3><p>Con qué frecuencia se descargan automáticamente noticias nuevas.</p><div class="ml-f"><select data-w="periodicidad">${Object.keys(PER).map(k => `<option value="${k}"${W.periodicidad === k ? " selected" : ""}>${PER[k]}</option>`).join("")}</select></div>
      <div class="ml-ck${W.texto ? " on" : ""}" data-tg="texto"><i>✓</i>Descargar el texto de las notas en segundo plano (mejora el resumen y la búsqueda)</div>${W.texto ? `<div class="ml-f" style="margin:8px 0"><label>Máximo de notas por extracción cuyo texto se descarga</label><input type="number" min="0" max="5000" data-w="textoMax" value="${W.textoMax}"></div>` : ""}
      <div class="ml-ck${W.ia ? " on" : ""}" data-tg="ia" style="margin-top:8px"><i>✓</i>Enriquecer con IA: categoría, lugar, personas, instituciones y síntesis ${P.ia === false ? "(sin clave de IA en el servidor)" : ""}</div>${W.ia ? `<div class="ml-f" style="margin-top:8px"><label>Notas nuevas a enriquecer con IA por extracción (0 = todas)</label><input type="number" min="0" max="50000" data-w="iaMax" value="${W.iaMax}"><small style="color:var(--ml-mut)">Con 0 se enriquecen todas las notas nuevas del proyecto. Pon un tope solo si quieres limitar el consumo de IA.</small></div>` : ""}
      <div class="ml-ck${W.geo ? " on" : ""}" data-tg="geo" style="margin-top:8px"><i>✓</i>Ubicar colonias y sitios con Google Places (más preciso; consume cuota) ${P.geocodificar === false ? "(sin clave de Google)" : ""}</div></div>`;
    else { const c = cuentaT(W.terminos); h = `<div class="ml-pn"><h3>Resumen y primera extracción</h3><div class="ml-sem" style="flex-direction:column;gap:4px;color:var(--ml-tx)"><span>📌 <b>${esc(W.titulo)}</b></span><span>🎯 ${esc(W.objetivo)}</span><span>🔎 ${nf(c.pos)} términos · ${nf(c.neg)} exclusiones</span><span>🗺️ ${W.estados.length ? W.estados.map(k => esc((P.estados || {})[k])).join(", ") : "todo el país"}${W.ciudades ? " · " + esc(W.ciudades) : ""}</span><span>🕒 ${W.tipo === "movil" ? "últimos " + W.dias + " días" : esc(W.desde) + " → " + esc(W.hasta || "hoy")} · hasta ${nf(W.max)} noticias</span><span>📡 ${esc(((P.proveedores || {})[W.proveedor] || {}).n || W.proveedor)} · ${esc(PER[W.periodicidad])}</span></div>
      ${W.lanzando ? progreso(W.lanzando) : `<button class="ml-btn" type="button" data-go="lanzar" style="margin-top:12px">${W.id ? "Guardar y extraer ahora" : "Crear proyecto y lanzar la primera extracción"}</button>`}</div>`; }
    const nav = `<div class="ml-nav"><button class="ml-btn2" type="button" data-go="atras" ${W.paso === 0 ? "disabled" : ""}>← Atrás</button>${W.paso < 5 ? `<button class="ml-btn" type="button" data-go="sigue">Siguiente →</button>` : `<span></span>`}</div>`;
    const fh = document.activeElement && document.activeElement.dataset ? document.activeElement.dataset.w : null; m.innerHTML = pasos + h + (W.lanzando && W.paso === 5 ? "" : nav);
    if (fh) { const e = $(`[data-w="${fh}"]`, m); if (e) { e.focus(); try { e.setSelectionRange(e.value.length, e.value.length); } catch (_) { } } }
  }
  const progreso = j => `<div class="ml-est"><b>${j.fin ? "✓ Extracción terminada" : "⏳ Extrayendo noticias…"}</b> ${j.error ? `<div class="ml-warn">${esc(j.error)}</div>` : ""}${j.aviso ? `<div class="ml-warn">ℹ️ ${esc(j.aviso)}</div>` : ""}${j.ultimoError && j.errores ? `<div class="ml-sem">Último aviso del servicio: ${esc(j.ultimoError)}</div>` : ""}${j.ocupado && !j.hecho ? `<div class="ml-sem">El servidor está procesando la primera consulta; puede tardar hasta un minuto.</div>` : ""}<div class="ml-bar"><i style="width:${j.total ? Math.min(100, Math.round(j.hecho / j.total * 100)) : 5}%"></i></div>Consultas ${nf(j.hecho)}/${nf(j.total)} · <b>${nf(j.nuevas)}</b> noticias nuevas · ${nf(j.errores)} avisos${j.fase === "texto" ? " · descargando el texto de las notas…" : ""}${j.fase === "enriquecimiento" ? " · enriqueciendo con IA…" : ""}${j.fin ? `<br><br><button class="ml-btn" type="button" data-go="abrir">Abrir el tablero →</button>` : `<br><small>Puedes cerrar esta ventana: la extracción continúa en el servidor.</small>`}</div>`;
  function validaPaso() { const W = S.W; if (W.paso === 0) { if (W.titulo.trim().length < 3) return "Escribe el título del proyecto"; if (W.objetivo.trim().length < 5) return "Describe el objetivo del proyecto"; } if (W.paso === 1 && !cuentaT(W.terminos).pos) return "Agrega al menos un término de interés"; if (W.paso === 3 && W.tipo === "fijo" && !W.desde) return "Indica la fecha inicial del rango"; return ""; }
  const cuerpo = () => { const W = S.W; try { localStorage.setItem("ml_creador", W.creador || ""); } catch (_) { } return { id: W.id || undefined, creador: W.creador, titulo: W.titulo, objetivo: W.objetivo, terminos: W.terminos, zona: { estados: W.estados, ciudades: W.ciudades.split(",").map(x => x.trim()).filter(Boolean), estricta: W.estricta }, periodo: W.tipo === "fijo" ? { tipo: "fijo", desde: W.desde, hasta: W.hasta || null } : { tipo: "movil", dias: +W.dias || 7 }, maxNoticias: +W.max, periodicidad: W.periodicidad, proveedor: W.proveedor, idiomas: W.idiomas, paises: ["MX"], medios: W.medios.split(",").map(x => x.trim()).filter(Boolean), texto: W.texto, textoMax: +W.textoMax || 0, ia: W.ia, iaMax: +W.iaMax || 0, geocodificar: W.geo }; };
  /* progreso: si el servidor responde «ocupado», se usa el avance guardado del proyecto (no un 0/0) */
  const avance = r => { const a = r.avance || {}, w = r.proyecto && r.proyecto.estado && r.proyecto.estado.trabajo; if (w && (a.ocupado || a.hecho == null)) return Object.assign({}, { hecho: w.hecho, total: w.total, nuevas: w.nuevas, errores: w.errores, fase: w.fase, aviso: w.aviso, ultimoError: w.ultimoError }, { ocupado: !!a.ocupado }); return a; };
  async function lanzar() {
    const W = S.W; W.lanzando = { hecho: 0, total: 0, nuevas: 0, errores: 0, fase: "extraccion" }; wizard();
    try {
      const c = await post("/proyectos", cuerpo()); W.id = c.id; let r = await post(`/proyectos/${c.id}/extraer`, { completa: true }), a = avance(r);
      for (let i = 0; i < 2000; i++) { W.lanzando = { hecho: a.hecho || 0, total: a.total || 0, nuevas: a.nuevas || 0, errores: a.errores || 0, fase: a.fase, fin: a.fase === "fin" || a.fin, aviso: a.aviso, ultimoError: a.ultimoError, ocupado: a.ocupado }; if (S.modo === "crear") wizard(); if (W.lanzando.fin) break; await new Promise(res => setTimeout(res, a.ocupado ? 3000 : 800)); r = await post(`/proyectos/${c.id}/avanzar`, {}); a = avance(r); }
      await cargaProy(); lado();
    } catch (e) { W.lanzando = Object.assign(W.lanzando || {}, { error: e.message }); if (S.modo === "crear") wizard(); toast(e.message, true); }
  }
  async function extraer(id, completa) {
    if (S.extrayendo && S.extrayendo.has(id)) { toast("Ya hay una extracción en curso en este proyecto"); return; } (S.extrayendo = S.extrayendo || new Set()).add(id);
    lado(); if (S.sel === id && S.modo === "tablero") principal(); pintaProg();
    clearInterval(S.poll); S.poll = setInterval(async () => { await cargaProy(); const p = S.proy.find(x => x.id === id); if (p) { $$(`.ml-pj[data-pj="${id}"] .ml-chip`).forEach(c => { if (c.dataset.chip) c.textContent = chipEn(p); }); pintaProg(); } }, 2000);
    toast("Extracción iniciada…"); try {
      let r = await post(`/proyectos/${id}/extraer`, completa ? { completa: true } : {}), a = avance(r); for (let i = 0; i < 2000 && !(a.fase === "fin" || a.fin); i++) { await cargaProy(); pintaProg(); await new Promise(res => setTimeout(res, a.ocupado ? 3000 : 800)); r = await post(`/proyectos/${id}/avanzar`, {}); a = avance(r); } const wf = r.proyecto && r.proyecto.estado && r.proyecto.estado.trabajo; if (wf && wf.aviso) toast(wf.aviso); if (wf && !wf.nuevas) toast("La extracción terminó sin notas nuevas" + (wf.desde ? " (periodo consultado: " + fechaC(wf.desde) + " → " + fechaC(wf.hasta) + ")" : "") + ". Prueba ampliar el periodo o quitar el filtro de zona estricta.", true);
      S.extrayendo.delete(id); clearInterval(S.poll); await cargaProy(); toast("Extracción terminada"); if (S.sel === id) { await consulta(); } else lado();
    } catch (e) { toast(e.message, true); await cargaProy(); lado(); } finally { S.extrayendo.delete(id); clearInterval(S.poll); lado(); if (S.sel === id && S.modo === "tablero") principal(); else if (S.modo === "proyectos") proyectosVista(); }
  }
  async function cargaProy() { try { S.proy = (await api("/proyectos")).proyectos || []; } catch (e) { toast(e.message, true); } }

  /* ───────── eventos ───────── */
  function clic(e) {
    const t = e.target, go = t.closest("[data-go]");
    const hm = t.closest("#mlHam"); if (hm) { const a = $("#mlApp"); if (a) { S.sideAuto = false; a.classList.toggle("ml-sidehide"); try { localStorage.setItem("ml_side", a.classList.contains("ml-sidehide") ? "0" : "1"); } catch (_) { } setTimeout(() => { if (S.mapa) S.mapa.invalidateSize(); }, 120); } return; }
    const nv = t.closest("[data-nav]"); if (nv) { e.preventDefault(); navTo(nv.dataset.nav); return; }
    const nc = t.closest("[data-navc]"); if (nc) { S.navC = S.navC || {}; S.navC[nc.dataset.navc] = !S.navC[nc.dataset.navc]; lado(); return; }
    if (go) {
      const k = go.dataset.go, W = S.W;
      if (k === "proyectos") { S.modo = "proyectos"; S.det = null; titulo("Proyectos"); lado(); principal(); }
      else if (k === "tablero") { S.modo = "tablero"; titulo((S.proy.find(x => x.id === S.sel) || {}).titulo || "Media Lab"); lado(); principal(); if (S.sel && !S.data) consulta(); }
      else if (k === "crear") nuevo(); else if (k === "costos") abreCostos(); else if (k === "guardaTar") guardaTarifas(); else if (k === "limpiar") { S.f = {}; consulta(); } else if (k === "mas") consulta(true); else if (k === "cierra") cierraDet();
      else if (k === "sigue") { const er = validaPaso(); if (er) { toast(er, true); return; } W.paso++; wizard(); } else if (k === "atras") { W.paso--; wizard(); }
      else if (k === "estimar") estima(); else if (k === "lanzar") lanzar(); else if (k === "abrir") { const id = W.id; S.W = null; selecciona(id); }
      else if (k === "zonaGeo" && S.ctx) { if (S.ctx.ent && !W.estados.includes(S.ctx.ent)) W.estados.push(S.ctx.ent); const ci = S.ctx.ciudad || S.ctx.municipio; if (ci && !W.ciudades.includes(ci)) W.ciudades = (W.ciudades ? W.ciudades + ", " : "") + ci; wizard(); }
      else if (k === "volver") { S.entV = null; pintaDet(); } else if (k === "resumen" && S.det && S.det.n) abreDet(S.det.n.id, true); else if (k === "recargaCos") cargaCostos(); return;
    }
    if (S.modo === "crear") {
      const mi = t.closest("[data-med]"); if (mi) { medToggle(mi.dataset.med); const q = $("#mlMq"); if (q) { q.value = ""; q.focus(); } medRefresca(); return; }
      const mx = t.closest("[data-medx]"); if (mx) { medToggle(mx.dataset.medx); return; }
      const box = t.closest("#mlMdd"); const lst = $("#mlMlist"); if (lst) { if (box) { lst.hidden = false; const q = $("#mlMq"); if (q && !t.closest("#mlMlist")) q.focus(); } else lst.hidden = true; }
    }
    const gx = t.closest("[data-gx]"); if (gx) { if (gx.dataset.gx === "fuera") { if (t === gx) cierraGestion(); return; } gxAccion(gx.dataset.gx); return; }
    const ge = t.closest("[data-gest]"); if (ge) { abreGestion(ge.dataset.gest); return; }
    const ab = t.closest("[data-abrir]"); if (ab) { e.preventDefault(); selecciona(ab.dataset.abrir); return; }
    const st = t.closest("[data-sit]"); if (st) { const [id0, v0] = st.dataset.sit.split("|"); cambiaSit(id0, v0); return; }
    const pf = t.closest("[data-pf]"); if (pf) { S.pf = pf.dataset.pf; proyectosVista(); return; }
    const pv2 = t.closest("[data-pv]"); if (pv2) { S.pv = pv2.dataset.pv; proyectosVista(); return; }
    const pj = t.closest("[data-pj]"); if (pj && !t.closest(".ml-acc") && !t.closest(".ml-tree")) { selecciona(pj.dataset.pj); return; }
    const ed = t.closest("[data-ed]"); if (ed) { editar(ed.dataset.ed); return; } const ex = t.closest("[data-ex]"); if (ex) { extraer(ex.dataset.ex); return; } const exa = t.closest("[data-exall]"); if (exa) { extraer(exa.dataset.exall, true); return; }
    const del = t.closest("[data-del]"); if (del) { const p = S.proy.find(x => x.id === del.dataset.del); if (p && confirm("¿Eliminar el proyecto «" + p.titulo + "» y todas sus noticias?")) api(`/proyectos/${p.id}`, { method: "DELETE" }).then(async () => { if (S.sel === p.id) { S.sel = null; S.data = null; } await cargaProy(); lado(); principal(); toast("Proyecto eliminado"); }).catch(er => toast(er.message, true)); return; }
    const sm = t.closest(".ml-ff > summary"); if (sm && !t.closest("button")) { e.preventDefault(); S.ffC = !S.ffC; sm.parentNode.open = !S.ffC; return; }
    const vm = t.closest("[data-vermapa]"); if (vm && S.det && S.det.n && S.det.n.lug) { const L0 = S.det.n.lug; S.foco = { id: vm.dataset.vermapa, lat: +L0.lat, lng: +L0.lng }; if (S.vista === "mapa" && S.mapa) { enfocaMapa(); return; } S.vista = "mapa"; principal(); consulta(); return; }
    const v = t.closest("[data-v]"); if (v) { S.vista = v.dataset.v; S.det = null; principal(); consulta(); return; }
    const rs = t.closest("[data-g3reset]"); if (rs && S.entV) { S.entV.keep = {}; pintaDet(); return; }
    const gv = t.closest("[data-ver]"); if (gv) { e.preventDefault(); abreEnt(gv.dataset.ver); return; }
    const ly = t.closest("[data-lay]"); if (ly && S.entV) { S.entV.lay = ly.dataset.lay; pintaDet(); return; }
    const gn = t.closest("[data-gn]"); if (gn && S.entV) { S.entV.sel = gn.dataset.gn; pintaDet(); return; }
    const n = t.closest("[data-n]"); if (n) { abreDet(n.dataset.n); return; }
    const en = t.closest("[data-ent]"); if (en) { S.f.ent = en.dataset.ent; consulta(); return; }
    const edo = t.closest("[data-edo]"); if (edo && S.W) { const k = edo.dataset.edo, a = S.W.estados, i = a.indexOf(k); i >= 0 ? a.splice(i, 1) : a.push(k); edo.classList.toggle("on", i < 0); return; }
    const tg = t.closest("[data-tg]"); if (tg && S.W) { S.W[tg.dataset.tg] = !S.W[tg.dataset.tg]; wizard(); return; }
    const w2 = t.closest("[data-w2]"); if (w2 && S.W) { const [k, val] = w2.dataset.w2.split(":"); S.W[k] = val; wizard(); }
  }
  let tq; function entrada(e) {
    const t = e.target; if (t.id === "mlMq") { const l = $("#mlMlist"); if (l) { l.hidden = false; l.innerHTML = medLista(t.value); } return; } if (t.id === "mlPQ") { S.pq = t.value; clearTimeout(tq); tq = setTimeout(proyectosVista, 200); return; } if (t.id === "mlQ") { clearTimeout(tq); tq = setTimeout(() => { S.f.q = t.value.trim(); consulta(); }, 400); return; }
    if (t.dataset && t.dataset.w && S.W) { const k = t.dataset.w; S.W[k] = t.type === "number" ? +t.value : t.value; if (k === "terminos") { const c = cuentaT(S.W.terminos), s = $(".ml-sem", $("#mlMain")); if (s) s.innerHTML = `<span><b>${nf(c.pos)}</b> términos válidos</span><span>${nf(c.neg)} exclusiones</span><span>${nf(c.dup)} repetidos omitidos</span><span>máx. 20,000</span>`; } }
  }
  function cambio(e) {
    const t = e.target;
    if (t.dataset && t.dataset.cos) { S.cos[t.dataset.cos] = t.value; cargaCostos(); return; }
    if (t.dataset && t.dataset.gxrol !== undefined && S.gx) { S.gx.usuarios[+t.dataset.gxrol].rol = t.value; return; }
    if (t.dataset && t.dataset.sitsel) { cambiaSit(t.dataset.sitsel, t.value); return; }
    if (t.dataset && t.dataset.f) { S.f[t.dataset.f] = t.value; if (t.dataset.f === "cat") S.f.sub = ""; if (t.dataset.f === "est" || t.dataset.f === "reg") S.f.mun = ""; consulta(); return; }
    if (t.id === "mlArch" && t.files && t.files[0]) { const r = new FileReader(); r.onload = () => { S.W.terminos = (S.W.terminos ? S.W.terminos + "\n" : "") + String(r.result || ""); wizard(); toast("Términos importados"); }; r.readAsText(t.files[0]); return; }
    if (t.dataset && t.dataset.w && S.W) { const k = t.dataset.w; if (k === "idioma") S.W.idiomas = [t.value]; else S.W[k] = t.type === "number" ? +t.value : t.value; if (k === "proveedor" || k === "periodicidad" || k === "max") wizard(); }
  }
  async function estima() {
    const W = S.W, er = validaPaso(); if (er) { toast(er, true); return; } W.estimando = true; wizard();
    try { const b = cuerpo(); W.est = await post("/estimar", b); } catch (e) { W.est = null; toast(e.message, true); } W.estimando = false; wizard();
  }

  document.addEventListener("keydown", e => { const t = e.target; if (t && t.id === "mlMq") { if (e.key === "Enter") { e.preventDefault(); if (t.value.trim()) { medToggle(t.value); t.value = ""; medRefresca(); } } else if (e.key === "Escape") { const l = $("#mlMlist"); if (l) l.hidden = true; } else if (e.key === "Backspace" && !t.value) { const a = medSel(); if (a.length) medToggle(a[a.length - 1]); } } });
  /* ───────── arranque ───────── */
  window.medialabInit = async function () {
    host = document.getElementById("medialab-host"); if (!host) return; shell(); lado(); principal();
    await cargaProy(); lado(); principal();
    if (EMB) { try { parent.postMessage({ gl: "ctx?" }, location.origin); } catch (_) { } }
    cargaProv();
  };
})();
