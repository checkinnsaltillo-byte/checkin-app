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
  const enCurso = p => { const w = p.estado && p.estado.trabajo; return !!((w && w.fase !== "fin") || (S.extrayendo && S.extrayendo.has(p.id))); };
  const FASES = { extraccion: "Descargando noticias", texto: "Descargando el texto de las notas", enriquecimiento: "Enriqueciendo con IA (categoría, lugar, personas, síntesis)" };
  const chipEn = p => { const w = p.estado && p.estado.trabajo; return w && w.fase !== "fin" ? `⏳ ${w.fase === "texto" ? "descargando texto" : w.fase === "enriquecimiento" ? "enriqueciendo" : "extrayendo"} ${nf(w.hecho)}/${nf(w.total)} · ${nf(w.nuevas)} nuevas` : "⏳ iniciando extracción…"; };
  const PER = { manual: "Solo manual", horaria: "Cada hora", "6h": "Cada 6 horas", diaria: "Diaria", semanal: "Semanal" };
  const S = { proy: [], sel: null, modo: "tablero", vista: "lista", f: {}, data: null, det: null, prov: null, W: null, job: null, ctx: null, mapa: null, cargando: false };
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
  function mockApi(p, o) {
    const j = x => Promise.resolve(Object.assign({ ok: true }, x)), m = (o && o.method) || "GET";
    const CATS = [{ c: "Política", subs: ["Gobierno"] }, { c: "Seguridad y justicia", subs: ["Delincuencia"] }, { c: "Economía y negocios", subs: ["Finanzas", "Empleo"] }, { c: "Deportes", subs: ["Futbol"] }];
    if (!MOCKDB.items) { const med = ["Milenio", "El Heraldo de México", "Imagen", "MVS Noticias", "Noticieros Televisa", "Vanguardia"], lug = [{ nivel: "municipio", est: "05", estN: "Coahuila", mun: "05030", munN: "Saltillo", lat: 25.425, lng: -101 }, { nivel: "municipio", est: "19", estN: "Nuevo León", mun: "19039", munN: "Monterrey", lat: 25.67, lng: -100.3 }, { nivel: "estado", est: "14", estN: "Jalisco", lat: 20.6, lng: -103.3 }, null];
      MOCKDB.items = Array.from({ length: 120 }, (_, i) => { const c = CATS[i % 4]; return { id: "n" + i, t: ["Anuncian inversión en parque industrial", "Detienen a presunto responsable de robo", "Rayados vencen en casa", "Alcalde presenta plan de agua", "Inflación se modera en octubre"][i % 5] + " #" + (i + 1), med: med[i % 6], f: new Date(Date.now() - i * 36e5 * 5).toISOString(), img: "", res: "Resumen de ejemplo de la nota " + (i + 1) + ": se informó sobre el tema y se detallaron las acciones anunciadas por las autoridades.", cat: c.c, sub: c.subs[i % c.subs.length], lug: lug[i % 4], ent: { pers: i % 3 ? ["Claudia Pérez"] : [], inst: ["Gobierno de Coahuila"], top: ["agua"] }, ia: 1, tx: i % 3 }; }); }
    if (/^\/costos\/tarifas/.test(p)) return j({});
    if (/^\/costos/.test(p)) return j({ tarifas: { ia: { "claude-haiku-5-5": { in: 1, out: 5 }, "claude-sonnet-5": { in: 3, out: 15 }, def: { in: 3, out: 15 } }, places: 0.032, gdelt: 0, grss: 0, newsdata: 0, gnews: 0, texto: 0, gnres: 0 }, servicios: { ia: "IA · Anthropic (Claude)", places: "Google Places (búsqueda de lugares)", gdelt: "GDELT (noticias)", grss: "Google Noticias (RSS)", newsdata: "NewsData.io", gnews: "GNews", texto: "Descarga de páginas de medios", gnres: "Resolución de enlaces de Google Noticias", notas: "Notas nuevas guardadas" }, porSvc: { ia: { n: 42, in: 210000, out: 96000, k: 420, costo: 0.69 }, places: { n: 55, costo: 1.76, in: 0, out: 0, k: 0 }, grss: { n: 120, costo: 0, in: 0, out: 0, k: 0 }, texto: { n: 380, costo: 0, in: 0, out: 0, k: 0 } }, porMes: { "2026-10": { costo: 1.1, llamadas: 300, notas: 210 }, "2026-11": { costo: 2.45, llamadas: 597, notas: 462 } }, porDia: { "2026-11-16": { costo: 0.4, llamadas: 90 }, "2026-11-17": { costo: 1.2, llamadas: 300 }, "2026-11-18": { costo: 0.85, llamadas: 207 } }, porProy: { demo1: { titulo: "Agua y sequía en el noreste", costo: 2.45, llamadas: 597, notas: 462 } }, total: { costo: 2.45, llamadas: 597, in: 210000, out: 96000, notas: 462, ia_notas: 420 } });
    if (p === "/proveedores") return j({ proveedores: { gdelt: { n: "GDELT (gratis)", ok: true, nota: "Cobertura masiva sin clave." }, grss: { n: "Google Noticias (RSS)", ok: true, nota: "Gratis, 100 por consulta." }, newsdata: { n: "NewsData.io", ok: false, clave: "NEWSDATA_API_KEY", nota: "De pago." }, gnews: { n: "GNews", ok: false, clave: "GNEWS_API_KEY", nota: "De pago." } }, ia: true, geocodificar: true, categorias: CATS, estados: { "05": "Coahuila", "19": "Nuevo León", "14": "Jalisco", "09": "Ciudad de México" } });
    if (p === "/proyectos" && m === "GET") return j({ proyectos: [{ id: "demo1", titulo: "Agua y sequía en el noreste", objetivo: "Monitorear cobertura", config: { nTerminos: 1250, periodicidad: "diaria", proveedor: "gdelt", maxNoticias: 5000 }, estado: { total: 120, ultima: new Date().toISOString(), trabajo: null } }] });
    if (p === "/estimar") return j({ terminos: 1250, excluidos: 3, grupos: 94, consultas: 94, minutos: 9, muestra: [{ q: "(agua OR sequía…)", n: 250, saturado: true }], notasPorGrupo: 180, notasEstimadas: 10000, saturado: true, advertencias: ["Se subdivide por fechas cuando una consulta se satura."] });
    if (p === "/proyectos" && m === "POST") return j({ id: "demo2", nTerminos: 10 });
    if (/\/extraer$|\/avanzar$/.test(p)) { MOCKDB.n++; const fin = MOCKDB.n % 3 === 0; return j({ avance: { fase: fin ? "fin" : "extraccion", hecho: Math.min(MOCKDB.n * 30, 94), total: 94, nuevas: MOCKDB.n * 40, errores: 0 }, proyecto: { id: "demo2", estado: { trabajo: { fase: fin ? "fin" : "extraccion", hecho: Math.min(MOCKDB.n * 30, 94), total: 94, nuevas: MOCKDB.n * 40, errores: 0 } } } }); }
    if (/\/noticia\//.test(p)) { const id = p.split("/").pop(), n = MOCKDB.items.find(x => x.id === id); return j({ noticia: Object.assign({}, n, { url: "https://example.com/nota", texto: "Texto de ejemplo de la nota.\n\nSegundo párrafo con más detalles de lo ocurrido en la zona." }) }); }
    if (/\/noticias/.test(p)) { const q = new URLSearchParams(p.split("?")[1] || ""); let L = MOCKDB.items.filter(n => (!q.get("cat") || n.cat === q.get("cat")) && (!q.get("est") || (n.lug && n.lug.est === q.get("est"))) && (!q.get("q") || norm(n.t).includes(norm(q.get("q")))) && (q.get("geo") !== "1" || (n.lug && n.lug.lat != null)));
      if (q.get("geo") === "1") return j({ total: L.length, puntos: L.map(n => ({ id: n.id, t: n.t, lat: n.lug.lat, lng: n.lug.lng, niv: n.lug.nivel, lug: (n.lug.munN ? n.lug.munN + ", " : "") + n.lug.estN, med: n.med, f: n.f, cat: n.cat })) });
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
      <header class="ml-top"><span class="ml-sp"></span><button class="ml-theme" id="mlTema" type="button" title="Cambiar tema (aplica a GeoLab, EconoLab, DataLab y Media Lab)">🌙 Oscuro</button></header>
      <div class="ml-body"><aside class="ml-side" id="mlSide"></aside>
        <div class="ml-wbar"><b id="mlWT">Media Lab</b><button class="ml-wx" id="mlWX" type="button" aria-label="Cerrar ventana" title="Cerrar">✕</button></div>
        <main class="ml-main" id="mlMain"></main></div></div>`;
    const app = $("#mlApp"); if (window.__mlTema) app.classList.toggle("ml-light", window.__mlTema !== "osc"); if (!EMB && !window.__mlTema) app.classList.remove("ml-light");
    temaPinta();
    $("#mlTema").onclick = () => { const claro = app.classList.contains("ml-light"); if (EMB) { try { parent.postMessage({ gl: "tema", v: claro ? "osc" : "claro" }, location.origin); } catch (_) { } } else { app.classList.toggle("ml-light"); temaPinta(); } };
    $("#mlWX").onclick = () => mlWin(false); document.addEventListener("keydown", e => { if (e.key === "Escape") { if (S.det) cierraDet(); else mlWin(false); } });
    tipEl = null; host.onclick = clic; host.oninput = entrada; host.onchange = cambio;
  }
  const titulo = t => { const w = $("#mlWT"); if (w) w.textContent = t; };

  /* ───────── menú izquierdo ───────── */
  function lado() {
    const side = $("#mlSide"); if (!side) return;
    let h = `<div class="ml-seg"><button type="button" data-go="tablero" class="${S.modo === "tablero" ? "on" : ""}">📰 Tablero</button><button type="button" data-go="crear" class="${S.modo === "crear" ? "on" : ""}">➕ Crear</button><button type="button" data-go="costos" class="${S.modo === "costos" ? "on" : ""}">💰 Costos</button></div><div class="ml-h4">Proyectos (${S.proy.length})</div>`;
    h += S.proy.length ? S.proy.map(p => { const w = p.estado && p.estado.trabajo, en = enCurso(p); return `<div class="ml-pj${p.id === S.sel ? " on" : ""}" data-pj="${p.id}"><b>${esc(p.titulo)}</b><small>${nf(p.estado && p.estado.total)} noticias · ${nf(p.config && p.config.nTerminos)} términos · ${esc(PER[p.config && p.config.periodicidad] || "")}</small>${en ? `<span class="ml-chip run" data-chip="${p.id}">${chipEn(p)}</span>` : p.estado && p.estado.ultima ? `<span class="ml-chip ok">✓ ${esc(fechaC(p.estado.ultima))}</span>` : `<span class="ml-chip">sin extraer</span>`}${w && w.ultimoError && !en ? `<span class="ml-chip err" title="${esc(w.ultimoError)}">avisos</span>` : ""}
        <div class="ml-acc"><button type="button" data-ed="${p.id}">✏️ Editar</button>${en ? "" : `<button type="button" data-ex="${p.id}" title="Trae solo lo nuevo desde la última extracción (o todo el periodo si el proyecto está vacío)">↻ Extraer</button><button type="button" data-exall="${p.id}" title="Vuelve a recorrer todo el periodo configurado">⟲ Todo</button>`}<button type="button" data-del="${p.id}">🗑</button></div></div>`; }).join("") : `<div class="ml-vacio">Aún no hay proyectos.<br>Crea el primero con «Crear proyecto».</div>`;
    if (S.modo === "tablero" && S.sel && S.data) h += filtros();
    side.innerHTML = h;
  }
  const opt = (arr, val, vf, tf) => `<option value="">Todas</option>` + (arr || []).map(x => `<option value="${esc(vf ? vf(x) : x.v)}"${String(vf ? vf(x) : x.v) === String(val || "") ? " selected" : ""}>${esc(tf ? tf(x) : x.v)} (${nf(x.n)})</option>`).join("");
  function filtros() {
    const F = S.data.facetas || {}, f = S.f, ents = [["Personas", "pers", F.pers], ["Instituciones", "inst", F.inst], ["Tópicos", "top", F.top]];
    const eo = ents.map(([t, k, L]) => (L && L.length) ? `<optgroup label="${t}">` + L.map(x => `<option value="${k}:${esc(x.v)}"${f.ent === k + ":" + x.v ? " selected" : ""}>${esc(x.v)} (${nf(x.n)})</option>`).join("") + `</optgroup>` : "").join("");
    return `<div class="ml-h4">Filtros · ${nf(S.data.total)} de ${nf(S.data.de)} noticias</div>
      <div class="ml-f"><label>Buscar en título y resumen</label><input type="search" id="mlQ" placeholder="Palabra o frase…" value="${esc(f.q || "")}"></div>
      <div class="ml-f"><label>Categoría</label><select data-f="cat">${opt(F.cat, f.cat)}</select></div>
      <div class="ml-f"><label>Subcategoría</label><select data-f="sub">${opt(F.sub, f.sub)}</select></div>
      <div class="ml-f"><label>Lugar · zona del país</label><select data-f="reg">${opt(F.reg, f.reg)}</select></div>
      <div class="ml-f"><label>Lugar · estado</label><select data-f="est">${opt(F.est, f.est, null, x => x.nom || x.v)}</select></div>
      <div class="ml-f"><label>Lugar · ciudad o municipio</label><select data-f="mun">${opt(F.mun, f.mun, null, x => x.nom || x.v)}</select></div>
      ${(F.zona && F.zona.length) ? `<div class="ml-f"><label>Lugar · colonia o zona</label><select data-f="zona">${opt(F.zona, f.zona)}</select></div>` : ""}
      <div class="ml-f"><label>Medio</label><select data-f="med">${opt(F.med, f.med)}</select></div>
      <div class="ml-f"><label>Texto de la nota</label><select data-f="tx"><option value="">Todas</option>${[["1", "📄 Con texto"], ["2", "🔒 Sin texto (no accesible)"], ["0", "⏳ Texto pendiente"]].map(([v, t]) => `<option value="${v}"${f.tx === v ? " selected" : ""}>${t} (${nf(((F.tx || []).find(x => x.v === v) || {}).n || 0)})</option>`).join("")}</select></div>
      <div class="ml-row2"><div class="ml-f"><label>Fecha desde</label><input type="date" data-f="desde" value="${esc(f.desde || "")}"></div><div class="ml-f"><label>Fecha hasta</label><input type="date" data-f="hasta" value="${esc(f.hasta || "")}"></div></div>
      <div class="ml-f"><label>Entidad (personas, instituciones, tópicos)</label><select data-f="ent"><option value="">Todas</option>${eo}</select></div>
      <button class="ml-btn2" type="button" data-go="limpiar" style="width:100%">Limpiar filtros</button>`;
  }

  /* ───────── tablero ───────── */
  const qs = () => { const f = S.f, p = new URLSearchParams(); Object.keys(f).forEach(k => { if (f[k]) p.set(k, f[k]); }); return p; };
  async function consulta(mas) {
    if (!S.sel) return; if (S.cargando) return; S.cargando = true;
    try {
      if (S.vista === "mapa") { await mapaDatos(); }
      else { const p = qs(); p.set("n", "40"); p.set("pag", mas ? String(S.pag + 1) : "0"); const r = await api(`/proyectos/${S.sel}/noticias?${p}`); if (mas && S.data) { S.pag++; S.data.items = S.data.items.concat(r.items); S.data.total = r.total; } else { S.pag = 0; S.data = r; } }
    } catch (e) { toast(e.message, true); } finally { S.cargando = false; }
    if (S.vista !== "mapa" || !S.data) { if (S.vista === "mapa" && !S.data) { try { const p = qs(); p.set("n", "1"); S.data = await api(`/proyectos/${S.sel}/noticias?${p}`); } catch (_) { } } }
    lado(); principal();
  }
  async function selecciona(id) {
    S.sel = id; S.modo = "tablero"; S.f = {}; S.data = null; S.det = null; S.pag = 0; const p = S.proy.find(x => x.id === id); titulo(p ? p.titulo : "Media Lab"); lado(); mlWin(true); principal(); await consulta();
  }
  function principal() {
    const m = $("#mlMain"); if (!m) return; S.mapa = null;
    if (S.modo === "crear") { wizard(); return; }
    if (S.modo === "costos") { costos(); return; }
    const p = S.proy.find(x => x.id === S.sel);
    if (!p) { m.innerHTML = `<div class="ml-vacio"><h3>Bienvenido a Media Lab</h3>Monitor masivo de noticias. Crea un proyecto con los términos, la zona y el periodo que te interesan, o elige uno de la lista.<br><br><button class="ml-btn" type="button" data-go="crear">➕ Crear proyecto</button></div>`; return; }
    const d = S.data, en = enCurso(p);
    let h = `<div class="ml-kp"><div><b>${nf(d ? d.total : 0)}</b><span>noticias con estos filtros</span></div><div><b>${nf(p.estado && p.estado.total)}</b><span>noticias en el proyecto</span></div><div><b>${nf(p.config && p.config.nTerminos)}</b><span>términos de interés</span></div><div><b>${p.estado && p.estado.ultima ? esc(fechaC(p.estado.ultima)) : "—"}</b><span>última extracción${p.estado && p.estado.proxima ? " · próxima " + esc(fechaC(p.estado.proxima)) : ""}</span></div></div>`;
    h += `<div class="ml-tb"><div class="ml-seg" style="margin:0;width:260px"><button type="button" data-v="lista" class="${S.vista === "lista" ? "on" : ""}">📰 Noticias</button><button type="button" data-v="mapa" class="${S.vista === "mapa" ? "on" : ""}">🗺️ Mapa</button></div><span class="ml-sp"></span>${en ? `<span class="ml-chip run" data-chip="${p.id}">${chipEn(p)}</span>` : `<button class="ml-btn2" type="button" data-exall="${p.id}" title="Vuelve a recorrer todo el periodo configurado y agrega las notas que falten">⟲ Extraer todo el periodo</button> <button class="ml-btn" type="button" data-ex="${p.id}" title="Solo lo nuevo desde la última extracción">↻ Extraer ahora</button>`}</div><div id="mlProg"></div>`;
    if (p.objetivo) h += `<div class="ml-sem"><span>🎯 ${esc(p.objetivo)}</span></div>`;
    if (S.vista === "mapa") h += `<div class="ml-map" id="mlMap"></div><div class="ml-sem" id="mlMapN"></div>`;
    else if (!d) h += `<div class="ml-vacio">Cargando noticias…</div>`;
    else if (!d.items.length) h += `<div class="ml-vacio">${p.estado && p.estado.total ? "Ninguna noticia coincide con los filtros." : "Este proyecto aún no tiene noticias. Pulsa «Extraer ahora»."}</div>`;
    else h += `<div class="ml-cards">${d.items.map(card).join("")}</div>${d.items.length < d.total ? `<button class="ml-btn2 ml-more" type="button" data-go="mas">Cargar más (${nf(d.items.length)} de ${nf(d.total)})</button>` : ""}`;
    m.innerHTML = h + `<aside class="ml-det" id="mlDet" hidden></aside>`; pintaProg(); if (S.det) pintaDet(); if (S.vista === "mapa") mapa();
  }
  const TXC = { 1: ["ok", "📄 Con texto", "Se descargó el cuerpo de la nota"], 2: ["", "🔒 Sin texto", "El medio no permitió descargar el cuerpo (bloqueo o suscripción)"], 0: ["run", "⏳ Texto pendiente", "Aún no se intenta descargar el cuerpo"] };
  const MET = { regla: ["🔎 Texto", "Detectado en el título o la descripción con el catálogo de municipios del INEGI"], ia: ["🤖 IA", "La IA interpretó el lugar del texto de la nota y se ubicó con el catálogo de municipios del INEGI"], places: ["🗺️ Google Places", "Ubicado con la API de Google Places y verificado dentro del estado de la nota"] };
  const metChip = L => L ? `<span class="ml-chip" title="${esc((MET[L.met || "regla"] || MET.regla)[1])}">${(MET[L.met || "regla"] || MET.regla)[0]}</span>` : "";
  const card = n => `<div class="ml-card${S.det && S.det.id === n.id ? " on" : ""}" data-n="${n.id}">${n.img ? `<div class="im" style="background-image:url('${esc(n.img)}')"></div>` : ""}<div class="bd"><div class="mt"><b>${esc(n.med)}</b><span>${esc(fecha(n.f))}</span></div><h4>${esc(n.t)}</h4><p>${esc(n.res || "")}</p><div><span class="ml-chip ${TXC[n.tx || 0][0]}" title="${TXC[n.tx || 0][2]}">${TXC[n.tx || 0][1]}</span><span class="ml-chip">${esc(n.cat)}</span><span class="ml-chip">${esc(n.sub)}</span>${n.lug ? `<span class="ml-chip">📍 ${esc(n.lug.munN ? n.lug.munN + ", " + n.lug.estN : n.lug.estN)}</span>${metChip(n.lug)}` : ""}</div></div></div>`;

  function pintaProg() {
    const el = $("#mlProg"), p = S.proy.find(x => x.id === S.sel); if (!el || !p) return; if (!enCurso(p)) { el.innerHTML = ""; return; }
    const w = p.estado && p.estado.trabajo, act = w && w.fase !== "fin", pct = act && w.total ? Math.min(100, Math.round(w.hecho / w.total * 100)) : 0;
    el.innerHTML = `<div class="ml-est"><b>⏳ ${act ? (FASES[w.fase] || "Procesando") : "Iniciando la extracción…"}</b><div class="ml-bar${act && w.total ? "" : " ind"}"><i style="width:${act && w.total ? pct : 35}%"></i></div>${act ? `Consultas <b>${nf(w.hecho)}/${nf(w.total)}</b> (${pct} %) · <b>${nf(w.nuevas)}</b> noticias nuevas · ${nf(w.dup)} repetidas omitidas · ${nf(w.errores)} avisos${w.prov ? " · servicio: " + esc(w.prov === "grss" ? "Google Noticias" : w.prov) : ""}` : `La primera consulta puede tardar hasta un minuto; el avance se actualiza solo.`}${act && w.desde ? `<div class="ml-sem">Periodo consultado: ${esc(fechaC(w.desde))} → ${esc(fechaC(w.hasta))} · notas vistas: ${nf(w.vistas)}</div>` : ""}${w && w.aviso ? `<div class="ml-warn">ℹ️ ${esc(w.aviso)}</div>` : ""}${act && w.ultimoError && w.errores ? `<div class="ml-sem">Último aviso: ${esc(w.ultimoError)}</div>` : ""}${w && w.act ? (() => { const seg = Math.max(0, Math.round((Date.now() - (w.act.t || 0)) / 1000)), viva = seg < 45; return `<div class="ml-act"><span class="ml-dot${viva ? "" : " off"}"></span><b>Ahora:</b> ${esc(w.act.m)} <small>· ${viva ? "hace " + seg + " s" : "sin señal reciente (" + seg + " s); el servidor sigue intentando"}</small></div>`; })() : ""}${w && w.inicio ? `<div class="ml-sem">Tiempo transcurrido: ${(() => { const t = Math.max(0, Math.round((Date.now() - new Date(w.inicio).getTime()) / 1000)); return Math.floor(t / 60) + ":" + String(t % 60).padStart(2, "0"); })()}</div>` : ""}<br><small style="color:var(--ml-mut)">Puedes seguir usando el tablero o cerrar la ventana: la extracción continúa en el servidor.</small></div>`;
    $$(`[data-chip="${p.id}"]`).forEach(c => { c.textContent = chipEn(p); });
  }

  /* ───────── detalle (columna lateral derecha) ───────── */
  async function abreDet(id, forzarIA) {
    S.det = { id, cargando: true }; pintaDet(); $$(".ml-card").forEach(c => c.classList.toggle("on", c.dataset.n === id));
    try { const r = await api(`/proyectos/${S.sel}/noticia/${id}${forzarIA ? "?ia=1" : ""}`); S.det = { id, n: r.noticia }; } catch (e) { S.det = { id, error: e.message }; } pintaDet();
  }
  function cierraDet() { S.det = null; const d = $("#mlDet"); if (d) d.hidden = true; $$(".ml-card.on").forEach(c => c.classList.remove("on")); }
  function pintaDet() {
    const d = $("#mlDet"); if (!d || !S.det) return; d.hidden = false; const x = S.det;
    if (x.lista) { d.innerHTML = `<button class="x" type="button" data-go="cierra">✕</button><h3>📍 ${esc(x.lista.nombre)}</h3><div class="ml-sem"><span>${x.lista.items.length} noticias en este lugar</span></div>` + x.lista.items.map(n => `<div class="ml-card" data-n="${n.id}" style="margin:0 0 10px"><div class="bd"><div class="mt"><b>${esc(n.med)}</b><span>${esc(fecha(n.f))}</span></div><h4>${esc(n.t)}</h4><div><span class="ml-chip">${esc(n.cat)}</span></div></div></div>`).join(""); return; }
    if (x.cargando) { d.innerHTML = `<button class="x" type="button" data-go="cierra">✕</button><div class="ml-vacio">Cargando detalle…</div>`; return; }
    if (x.error) { d.innerHTML = `<button class="x" type="button" data-go="cierra">✕</button><div class="ml-warn">${esc(x.error)}</div>`; return; }
    const n = x.n, L = n.lug, ent = n.ent || {}, chips = (t, k, a) => (a && a.length) ? `<dt>${t}</dt><dd>${a.map(v => `<span class="ml-ent" data-ent="${k}:${esc(v)}" title="Filtrar por esta entidad">${esc(v)}</span>`).join("")}</dd>` : "";
    d.innerHTML = `<button class="x" type="button" data-go="cierra">✕</button><div class="mt" style="font-size:12px;color:var(--ml-mut)"><b style="color:var(--ml-ac2)">${esc(n.med)}</b> · ${esc(fecha(n.f))}</div><h3>${esc(n.t)}</h3>
      ${n.img ? `<img class="g" src="${esc(n.img)}" alt="" onerror="this.remove()">` : ""}
      ${n.res ? `<div class="ml-pn" style="padding:10px 12px"><b>Síntesis</b><br>${esc(n.res)}</div>` : (n.ia === 0 ? `<button class="ml-btn2" type="button" data-go="resumen" style="margin:6px 0">✨ Generar síntesis con IA</button>` : "")}
      ${n.texto ? `<div class="tx">${esc(n.texto)}</div>` : (n.d ? `<div class="tx">${esc(n.d)}</div>` : "")}
      <dl><dt>Categoría</dt><dd><span class="ml-chip">${esc(n.cat)}</span><span class="ml-chip">${esc(n.sub)}</span></dd>
      <dt>Lugar</dt><dd>${L ? esc([L.sitio, L.zona, L.munN, L.estN].filter(Boolean).join(", ")) + ` <small style="color:var(--ml-mut)">(nivel: ${esc(L.nivel)})</small><br>${metChip(L)} <small style="color:var(--ml-mut)">${esc((MET[L.met || "regla"] || MET.regla)[1])}</small>` : "Sin ubicación detectada"}</dd>
      ${chips("Personas", "pers", ent.pers)}${chips("Instituciones", "inst", ent.inst)}${chips("Tópicos", "top", ent.top)}
      <dt>Texto</dt><dd>${n.texto ? `<span class="ml-chip ok">📄 Descargado</span> ${nf(n.texto.length)} caracteres` : n.txErr ? `<span class="ml-chip">🔒 No accesible</span> el medio no permitió descargarlo` : `<span class="ml-chip run">⏳ Pendiente</span>`}</dd><dt>Idioma</dt><dd>${esc(n.idi || "—")}${n.pais ? " · " + esc(n.pais) : ""}</dd><dt>Proveedor</dt><dd>${esc(n.prov || "—")}${n.ia ? " · enriquecida con IA" : ""}</dd></dl>
      <a class="ml-btn" style="display:inline-block;text-decoration:none" href="${esc(n.urlReal || n.url)}" target="_blank" rel="noopener">Abrir nota original ↗</a>`;
  }

  /* ───────── mapa ───────── */
  async function mapaDatos() { const p = qs(); p.set("geo", "1"); S.geo = await api(`/proyectos/${S.sel}/noticias?${p}`); }
  function leafletListo(cb) { if (window.L) return cb(); if (window._rsvEnsureLeaflet_) return window._rsvEnsureLeaflet_(cb); const l = document.createElement("link"); l.rel = "stylesheet"; l.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"; document.head.appendChild(l); const s = document.createElement("script"); s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"; s.onload = cb; document.head.appendChild(s); }
  function mapa() {
    const el = $("#mlMap"); if (!el) return;
    leafletListo(() => {
      const el2 = $("#mlMap"); if (!el2 || !window.L) return; const L = window.L, g = S.geo || { puntos: [], total: 0 }, M = L.map(el2, { zoomControl: true }).setView([23.6, -102.5], 5);
      L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 18, attribution: "© OpenStreetMap" }).addTo(M); S.mapa = M;
      const grupos = new Map(); g.puntos.forEach(p => { const k = p.lat.toFixed(3) + "," + p.lng.toFixed(3); (grupos.get(k) || grupos.set(k, []).get(k)).push(p); });
      const pts = [];
      grupos.forEach(L2 => {
        const p0 = L2[0], uno = L2.length === 1, nom = p0.lug || "";
        const ic = L.divIcon({ className: "", html: `<div class="ml-pin"><b>${uno ? "" : L2.length > 99 ? "99+" : L2.length}</b></div>`, iconSize: [26, 26], iconAnchor: [13, 28] });
        const mk = L.marker([p0.lat, p0.lng], { icon: ic }).addTo(M);
        mk.bindTooltip(uno ? `${esc(p0.t.length > 70 ? p0.t.slice(0, 69) + "…" : p0.t)}<small>${esc(p0.med)} · ${esc(fechaC(p0.f))} · ${esc(p0.cat)}</small>` : `${esc(nom)}<small>${L2.length} noticias</small>`, { permanent: true, direction: "right", offset: [12, -14], className: "ml-tip" });
        mk.on("click", () => { if (uno) abreDet(p0.id); else { S.det = { lista: { nombre: nom, items: L2.slice(0, 60) } }; pintaDet(); } }); pts.push([p0.lat, p0.lng]);
      });
      const nl = () => el2.classList.toggle("ml-nolbl", grupos.size > 40 && M.getZoom() < 9); M.on("zoomend", nl); nl();
      if (pts.length) M.fitBounds(pts, { padding: [40, 40], maxZoom: 11 }); setTimeout(() => M.invalidateSize(), 200);
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
    h += `<div class="ml-pn"><h3>Por proyecto</h3><div style="overflow:auto"><table class="ml-tbl"><thead><tr><th>Proyecto</th><th class="r">Llamadas</th><th class="r">Notas nuevas</th><th class="r">Costo est.</th></tr></thead><tbody>${pr.length ? pr.map(([k, e]) => `<tr><td>${esc(e.titulo)}</td><td class="r">${nf(e.llamadas)}</td><td class="r">${nf(e.notas)}</td><td class="r"><b>${USD(e.costo)}</b></td></tr>`).join("") : `<tr><td colspan="4" class="ml-mut">Sin datos.</td></tr>`}</tbody></table></div></div>`;
    const t = D.tarifas, mods = Object.keys(t.ia || {}).filter(k => k !== "def");
    h += `<div class="ml-pn"><h3>Tarifas de referencia (editables)</h3><p>Los costos son <b>estimados</b>: llamadas medidas × estas tarifas. Cámbialas por las de tu plan y todo el historial se recalcula. No incluye Cloud Run ni Cloud Storage; Google puede dar cuota gratuita mensual que aquí no se descuenta.</p><div class="ml-grid2">${mods.map(k => `<div class="ml-f"><label>IA · ${esc(k)} · USD por millón de tokens (entrada / salida)</label><div class="ml-row2"><input type="number" step="0.01" min="0" data-tar="ia:${esc(k)}:in" value="${t.ia[k].in}"><input type="number" step="0.01" min="0" data-tar="ia:${esc(k)}:out" value="${t.ia[k].out}"></div></div>`).join("")}${["places", "newsdata", "gnews", "texto", "gnres", "gdelt", "grss"].map(k => `<div class="ml-f"><label>${esc(SV[k])} · USD por llamada</label><input type="number" step="0.0001" min="0" data-tar="${k}" value="${t[k] || 0}"></div>`).join("")}</div><button class="ml-btn2" type="button" data-go="guardaTar">Guardar tarifas</button></div>`;
    m.innerHTML = h;
  }
  async function guardaTarifas() {
    const D = S.cos && S.cos.data; if (!D) return; const t = { ia: {} }; $$("[data-tar]", $("#mlMain")).forEach(i => { const k = i.dataset.tar.split(":"); if (k[0] === "ia") { (t.ia[k[1]] = t.ia[k[1]] || {})[k[2]] = +i.value || 0; } else t[k[0]] = +i.value || 0; });
    try { await post("/costos/tarifas", { tarifas: t }); toast("Tarifas guardadas"); await cargaCostos(); } catch (e) { toast(e.message, true); }
  }
  /* ───────── asistente: crear / editar proyecto ───────── */
  const PASOS = ["1 · Proyecto", "2 · Términos", "3 · Zona", "4 · Periodo y volumen", "5 · Periodicidad", "6 · Lanzar"];
  const WNEW = () => ({ paso: 0, id: null, titulo: "", objetivo: "", terminos: "", estados: [], ciudades: "", estricta: false, tipo: "movil", dias: 7, desde: "", hasta: "", max: 5000, idiomas: ["spanish"], medios: "", proveedor: "grss", periodicidad: "manual", texto: true, textoMax: 500, ia: true, iaMax: 0, geo: false, est: null, lanzando: null });
  function nuevo() { S.W = WNEW(); S.modo = "crear"; titulo("Crear proyecto"); lado(); mlWin(true); principal(); if (!S.prov) cargaProv(); }
  async function cargaProv() { try { S.prov = await api("/proveedores"); } catch (e) { S.prov = { proveedores: {}, estados: {}, error: e.message }; } if (S.modo === "crear") principal(); }
  async function editar(id) {
    try { const r = await api(`/proyectos/${id}?terminos=1`), p = r.proyecto, c = p.config || {}; S.W = Object.assign(WNEW(), { id, titulo: p.titulo, objetivo: p.objetivo || "", terminos: (c.terminos || []).join("\n"), estados: (c.zona && c.zona.estados) || [], ciudades: ((c.zona && c.zona.ciudades) || []).join(", "), estricta: !!(c.zona && c.zona.estricta), tipo: c.periodo && c.periodo.tipo === "fijo" ? "fijo" : "movil", dias: (c.periodo && c.periodo.dias) || 7, desde: (c.periodo && c.periodo.desde || "").slice(0, 10), hasta: (c.periodo && c.periodo.hasta || "").slice(0, 10), max: c.maxNoticias || 5000, idiomas: c.idiomas || ["spanish"], medios: (c.medios || []).join(", "), proveedor: c.proveedor || "gdelt", periodicidad: c.periodicidad || "manual", texto: c.texto !== false, textoMax: c.textoMax == null ? 500 : c.textoMax, ia: c.ia !== false, iaMax: c.iaMax || 0, geo: !!c.geocodificar }); S.modo = "crear"; titulo("Editar proyecto"); lado(); mlWin(true); principal(); if (!S.prov) cargaProv(); } catch (e) { toast(e.message, true); }
  }
  const cuentaT = txt => { const L = String(txt || "").split(/[\n,;]+/).map(x => x.trim()).filter(Boolean), v = new Set(); let pos = 0, neg = 0, dup = 0; L.forEach(t => { const ng = t.startsWith("-"), k = norm(ng ? t.slice(1) : t).trim(); if (k.length < 3) return; if (v.has(k)) { dup++; return; } v.add(k); ng ? neg++ : pos++; }); return { pos, neg, dup }; };
  function wizard() {
    const m = $("#mlMain"), W = S.W || (S.W = WNEW()), P = S.prov || { proveedores: {}, estados: {} };
    const pasos = `<div class="ml-steps">${PASOS.map((t, i) => `<span class="${i === W.paso ? "on" : i < W.paso ? "ok" : ""}">${t}</span>`).join("")}</div>`; let h = "";
    if (W.paso === 0) h = `<div class="ml-pn"><h3>Proyecto</h3><p>Dale un título y describe qué quieres vigilar en los medios.</p><div class="ml-f"><label>Título del proyecto *</label><input data-w="titulo" value="${esc(W.titulo)}" maxlength="140" placeholder="Ej. Obra pública y agua en Saltillo"></div><div class="ml-f"><label>Objetivo *</label><textarea data-w="objetivo" style="min-height:90px" placeholder="¿Para qué se monitorea? ¿Qué decisiones apoya?">${esc(W.objetivo)}</textarea></div></div>`;
    else if (W.paso === 1) { const c = cuentaT(W.terminos); h = `<div class="ml-pn"><h3>Términos y tópicos de interés</h3><p>Uno por línea (o separados por coma). Admite <b>miles</b> de términos: se agrupan en consultas automáticamente. Una frase se busca exacta; anteponer <b>-</b> excluye (ej. <i>-futbol</i>).</p><div class="ml-f"><textarea data-w="terminos" style="min-height:260px" placeholder="alcalde de Saltillo&#10;desabasto de agua&#10;Parque Las Maravillas&#10;-futbol">${esc(W.terminos)}</textarea></div><div class="ml-sem"><span><b>${nf(c.pos)}</b> términos válidos</span><span>${nf(c.neg)} exclusiones</span><span>${nf(c.dup)} repetidos omitidos</span><span>máx. 20,000</span></div><div class="ml-f"><label>Importar desde archivo (.txt o .csv, un término por línea)</label><input type="file" id="mlArch" accept=".txt,.csv,text/plain"></div></div>`; }
    else if (W.paso === 2) { const ed = P.estados || {}; h = `<div class="ml-pn"><h3>Zona geográfica de interés</h3><p>Elige estados y/o escribe ciudades o colonias. Sin zona, se busca en todo el país.</p>${S.ctx && S.ctx.ent ? `<button class="ml-btn2" type="button" data-go="zonaGeo">📍 Usar el lugar del proyecto de GeoLab (${esc(S.ctx.municipio || S.ctx.estado || S.ctx.ent)})</button>` : ""}<div class="ml-h4">Estados</div><div class="ml-grid">${Object.keys(ed).map(k => `<div class="ml-ck${W.estados.includes(k) ? " on" : ""}" data-edo="${k}"><i>✓</i>${esc(ed[k])}</div>`).join("") || "<span class='ml-sem'>Cargando…</span>"}</div><div class="ml-f" style="margin-top:10px"><label>Ciudades, municipios o colonias (separados por coma)</label><input data-w="ciudades" value="${esc(W.ciudades)}" placeholder="Saltillo, Ramos Arizpe, Arteaga"></div><div class="ml-ck${W.estricta ? " on" : ""}" data-tg="estricta"><i>✓</i>Conservar solo noticias ubicadas en estas zonas (filtro estricto)</div></div>`; }
    else if (W.paso === 3) { const pv = P.proveedores || {}, e = W.est; h = `<div class="ml-pn"><h3>Intervalo de tiempo, volumen y fuente de noticias</h3>
      <div class="ml-seg"><button type="button" data-w2="tipo:movil" class="${W.tipo === "movil" ? "on" : ""}">Últimos N días (móvil)</button><button type="button" data-w2="tipo:fijo" class="${W.tipo === "fijo" ? "on" : ""}">Rango fijo</button></div>
      ${W.tipo === "movil" ? `<div class="ml-f"><label>Días hacia atrás</label><input type="number" min="1" max="3650" data-w="dias" value="${W.dias}"></div>` : `<div class="ml-row2"><div class="ml-f"><label>Desde</label><input type="date" data-w="desde" value="${esc(W.desde)}"></div><div class="ml-f"><label>Hasta</label><input type="date" data-w="hasta" value="${esc(W.hasta)}"></div></div>`}
      <div class="ml-row2"><div class="ml-f"><label>Cantidad máxima de noticias</label><select data-w="max">${[1000, 5000, 20000, 50000].map(v => `<option value="${v}"${+W.max === v ? " selected" : ""}>${nf(v)}</option>`).join("")}</select></div><div class="ml-f"><label>Idioma</label><select data-w="idioma"><option value="spanish"${W.idiomas[0] === "spanish" ? " selected" : ""}>Español</option><option value="english"${W.idiomas[0] === "english" ? " selected" : ""}>Inglés</option></select></div></div>
      <div class="ml-f"><label>Solo estos medios (dominios separados por coma; vacío = todos)</label><input data-w="medios" value="${esc(W.medios)}" placeholder="milenio.com, elheraldodemexico.com, imagen.com.mx"></div>
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
  const cuerpo = () => { const W = S.W; return { id: W.id || undefined, titulo: W.titulo, objetivo: W.objetivo, terminos: W.terminos, zona: { estados: W.estados, ciudades: W.ciudades.split(",").map(x => x.trim()).filter(Boolean), estricta: W.estricta }, periodo: W.tipo === "fijo" ? { tipo: "fijo", desde: W.desde, hasta: W.hasta || null } : { tipo: "movil", dias: +W.dias || 7 }, maxNoticias: +W.max, periodicidad: W.periodicidad, proveedor: W.proveedor, idiomas: W.idiomas, paises: ["MX"], medios: W.medios.split(",").map(x => x.trim()).filter(Boolean), texto: W.texto, textoMax: +W.textoMax || 0, ia: W.ia, iaMax: +W.iaMax || 0, geocodificar: W.geo }; };
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
    } catch (e) { toast(e.message, true); await cargaProy(); lado(); } finally { S.extrayendo.delete(id); clearInterval(S.poll); lado(); if (S.sel === id && S.modo === "tablero") principal(); }
  }
  async function cargaProy() { try { S.proy = (await api("/proyectos")).proyectos || []; } catch (e) { toast(e.message, true); } }

  /* ───────── eventos ───────── */
  function clic(e) {
    const t = e.target, go = t.closest("[data-go]");
    if (go) {
      const k = go.dataset.go, W = S.W;
      if (k === "tablero") { S.modo = "tablero"; titulo((S.proy.find(x => x.id === S.sel) || {}).titulo || "Media Lab"); lado(); principal(); if (S.sel && !S.data) consulta(); }
      else if (k === "crear") nuevo(); else if (k === "costos") abreCostos(); else if (k === "guardaTar") guardaTarifas(); else if (k === "limpiar") { S.f = {}; consulta(); } else if (k === "mas") consulta(true); else if (k === "cierra") cierraDet();
      else if (k === "sigue") { const er = validaPaso(); if (er) { toast(er, true); return; } W.paso++; wizard(); } else if (k === "atras") { W.paso--; wizard(); }
      else if (k === "estimar") estima(); else if (k === "lanzar") lanzar(); else if (k === "abrir") { const id = W.id; S.W = null; selecciona(id); }
      else if (k === "zonaGeo" && S.ctx) { if (S.ctx.ent && !W.estados.includes(S.ctx.ent)) W.estados.push(S.ctx.ent); const ci = S.ctx.ciudad || S.ctx.municipio; if (ci && !W.ciudades.includes(ci)) W.ciudades = (W.ciudades ? W.ciudades + ", " : "") + ci; wizard(); }
      else if (k === "resumen" && S.det && S.det.n) abreDet(S.det.n.id, true); else if (k === "recargaCos") cargaCostos(); return;
    }
    const pj = t.closest("[data-pj]"); if (pj && !t.closest(".ml-acc")) { selecciona(pj.dataset.pj); return; }
    const ed = t.closest("[data-ed]"); if (ed) { editar(ed.dataset.ed); return; } const ex = t.closest("[data-ex]"); if (ex) { extraer(ex.dataset.ex); return; } const exa = t.closest("[data-exall]"); if (exa) { extraer(exa.dataset.exall, true); return; }
    const del = t.closest("[data-del]"); if (del) { const p = S.proy.find(x => x.id === del.dataset.del); if (p && confirm("¿Eliminar el proyecto «" + p.titulo + "» y todas sus noticias?")) api(`/proyectos/${p.id}`, { method: "DELETE" }).then(async () => { if (S.sel === p.id) { S.sel = null; S.data = null; } await cargaProy(); lado(); principal(); toast("Proyecto eliminado"); }).catch(er => toast(er.message, true)); return; }
    const v = t.closest("[data-v]"); if (v) { S.vista = v.dataset.v; S.det = null; principal(); consulta(); return; }
    const n = t.closest("[data-n]"); if (n) { abreDet(n.dataset.n); return; }
    const en = t.closest("[data-ent]"); if (en) { S.f.ent = en.dataset.ent; consulta(); return; }
    const edo = t.closest("[data-edo]"); if (edo && S.W) { const k = edo.dataset.edo, a = S.W.estados, i = a.indexOf(k); i >= 0 ? a.splice(i, 1) : a.push(k); edo.classList.toggle("on", i < 0); return; }
    const tg = t.closest("[data-tg]"); if (tg && S.W) { S.W[tg.dataset.tg] = !S.W[tg.dataset.tg]; wizard(); return; }
    const w2 = t.closest("[data-w2]"); if (w2 && S.W) { const [k, val] = w2.dataset.w2.split(":"); S.W[k] = val; wizard(); }
  }
  let tq; function entrada(e) {
    const t = e.target; if (t.id === "mlQ") { clearTimeout(tq); tq = setTimeout(() => { S.f.q = t.value.trim(); consulta(); }, 400); return; }
    if (t.dataset && t.dataset.w && S.W) { const k = t.dataset.w; S.W[k] = t.type === "number" ? +t.value : t.value; if (k === "terminos") { const c = cuentaT(S.W.terminos), s = $(".ml-sem", $("#mlMain")); if (s) s.innerHTML = `<span><b>${nf(c.pos)}</b> términos válidos</span><span>${nf(c.neg)} exclusiones</span><span>${nf(c.dup)} repetidos omitidos</span><span>máx. 20,000</span>`; } }
  }
  function cambio(e) {
    const t = e.target;
    if (t.dataset && t.dataset.cos) { S.cos[t.dataset.cos] = t.value; cargaCostos(); return; }
    if (t.dataset && t.dataset.f) { S.f[t.dataset.f] = t.value; if (t.dataset.f === "cat") S.f.sub = ""; if (t.dataset.f === "est" || t.dataset.f === "reg") S.f.mun = ""; consulta(); return; }
    if (t.id === "mlArch" && t.files && t.files[0]) { const r = new FileReader(); r.onload = () => { S.W.terminos = (S.W.terminos ? S.W.terminos + "\n" : "") + String(r.result || ""); wizard(); toast("Términos importados"); }; r.readAsText(t.files[0]); return; }
    if (t.dataset && t.dataset.w && S.W) { const k = t.dataset.w; if (k === "idioma") S.W.idiomas = [t.value]; else S.W[k] = t.type === "number" ? +t.value : t.value; if (k === "proveedor" || k === "periodicidad" || k === "max") wizard(); }
  }
  async function estima() {
    const W = S.W, er = validaPaso(); if (er) { toast(er, true); return; } W.estimando = true; wizard();
    try { const b = cuerpo(); W.est = await post("/estimar", b); } catch (e) { W.est = null; toast(e.message, true); } W.estimando = false; wizard();
  }

  /* ───────── arranque ───────── */
  window.medialabInit = async function () {
    host = document.getElementById("medialab-host"); if (!host) return; shell(); lado(); principal();
    await cargaProy(); lado(); principal();
    if (EMB) { try { parent.postMessage({ gl: "ctx?" }, location.origin); } catch (_) { } }
    cargaProv();
  };
})();
