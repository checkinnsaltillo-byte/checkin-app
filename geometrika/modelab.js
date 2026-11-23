/* Model Lab · modelado estadístico y aprendizaje automático por proyecto (prefijo mo-).
   Misma estructura y estética que Data Lab: barra oscura, menú lateral, KPIs, tarjetas/tabla. Flujo por proyecto:
   Mis proyectos → Nuevo proyecto (asistente) → Variables → Enfoque → Métodos → Preprocesamiento → Modelar → Resultados → Calidad del modelo.
   El cálculo corre en el navegador con modelab-motor.js; los proyectos, puntos, datos y resultados se guardan en el servidor (/geometrika/modelab). */
(function () {
  "use strict";
  const API = "https://api.check-inn.mx", R = "/geometrika/modelab";
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const gN = (v, d) => v == null || !isFinite(v) ? "—" : Number(v).toLocaleString("es-MX", { maximumFractionDigits: d == null ? (Math.abs(v) < 10 ? 2 : Math.abs(v) < 1000 ? 1 : 0) : d });
  const usd = v => "US$ " + Number(v).toLocaleString("es-MX", { minimumFractionDigits: v < 100 ? 2 : 0, maximumFractionDigits: v < 100 ? 2 : 0 });
  const fecha = s => { try { return new Date(s).toLocaleDateString("es-MX", { day: "numeric", month: "short", year: "numeric" }); } catch (_) { return ""; } };
  let host = null;
  const $ = (s, r) => (r || host).querySelector(s), $$ = (s, r) => Array.from((r || host).querySelectorAll(s));
  const tick = () => new Promise(r => setTimeout(r, 0));
  const S = { sec: "proyectos", proy: [], cargado: false, pv: "card", pf: "", pq: "", id: null, P: null, puntos: [], M: null, ds: null, res: null, bib: window.MO_BIBLIO || [], wiz: null, busy: false };
  const SEC = [["variables", "1", "Variables", "Elige las variables del modelo"], ["enfoque", "2", "Enfoque", "Supervisado o no supervisado"], ["metodos", "3", "Métodos", "Métodos analíticos factibles"], ["prep", "4", "Preprocesamiento", "Limpieza y preparación"], ["modelar", "5", "Modelar", "Entrenar, calibrar y evaluar"], ["resultados", "6", "Resultados", "Hallazgos y gráficas"], ["calidad", "7", "Calidad del modelo", "Estadísticos y diagnóstico"]];

  /* ───────── servidor ───────── */
  const token = () => { try { const g = JSON.parse(localStorage.getItem("gmTok") || "null"); return g && g.token ? g.token : ""; } catch (_) { return ""; } };
  async function api(ruta, cuerpo, metodo) {
    const o = { method: metodo || (cuerpo ? "POST" : "GET"), headers: { "x-gm-token": token() } }; if (cuerpo) { o.headers["Content-Type"] = "application/json"; o.body = JSON.stringify(cuerpo); }
    const r = await fetch(API + ruta, o); let j = null; try { j = await r.json(); } catch (_) { }
    if (r.status === 401) throw new Error("Tu sesión expiró. Vuelve a entrar con tu contraseña."); if (!r.ok || !j || j.ok === false) throw new Error((j && j.error) || "No se pudo completar la operación (" + r.status + ").");
    return j;
  }
  let _tt = null; function toast(m, ms) { const t = $("#moToast"); if (!t) return; t.textContent = m; t.classList.add("on"); clearTimeout(_tt); _tt = setTimeout(() => t.classList.remove("on"), ms || 3200); }
  const ck = on => `<span class="mo-ck${on ? " on" : ""}" aria-hidden="true">${on ? "✓" : ""}</span>`;   /* casilla propia: nunca checkbox nativo */
  const chips = (lista, sel, attr) => lista.map(([v, t]) => `<button type="button" class="mo-c${sel.includes(v) ? " on" : ""}" ${attr}="${esc(v)}">${esc(t)}</button>`).join("");
  const MI = { folder: '<path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>', plus: '<path d="M12 5v14M5 12h14"/>', list: '<path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/>', grid: '<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>', back: '<path d="M15 18l-6-6 6-6"/>', x: '<path d="M18 6L6 18M6 6l12 12"/>', menu: '<path d="M3 6h18M3 12h18M3 18h18"/>' };
  const mi = n => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" width="18" height="18" aria-hidden="true">${MI[n] || ""}</svg>`;

  /* ───────── cascarón ───────── */
  function shell() {
    const off = (() => { try { const v = localStorage.getItem("mo_side"); return v ? v === "off" : window.innerWidth < 900; } catch (_) { return false; } })();
    host.innerHTML = `<div class="mo-app${off ? " mo-off" : ""}" id="moApp">
      <header class="mo-top"><button type="button" class="mo-burger" id="moBurger" aria-label="Menú">${mi("menu")}</button><img class="mo-logo" src="modelab-logo-claro.png" alt="Model Lab"><div class="mo-brand"><b>Sistema de Modelado</b><span>Estadística y aprendizaje automático por proyecto</span></div><span class="mo-sp"></span>
        <span class="mo-chip">v.Beta.1</span><a class="mo-btn" href="index.html?s=geo" id="moIrGeo">Ir a Geo Lab</a><button type="button" class="mo-btn" id="moSalir">Cerrar sesión</button></header>
      <div class="mo-body"><aside class="mo-side" id="moSide"><div class="mo-side-t">Menú</div><div id="moNav"></div><div class="mo-side-f">Model Lab · Check Inn</div></aside><main class="mo-main" id="moMain"><div class="mo-wrap" id="moWrap"></div></main></div><div class="mo-toast" id="moToast"></div></div>`;
    $("#moBurger").onclick = () => { const a = $("#moApp"); a.classList.toggle("mo-off"); try { localStorage.setItem("mo_side", a.classList.contains("mo-off") ? "off" : "on"); } catch (_) { } };
    $("#moSalir").onclick = () => { if (window.GM_SALIR) window.GM_SALIR(); else location.reload(); };
    host.addEventListener("click", clic); host.addEventListener("input", entrada); host.addEventListener("change", cambia);
  }
  const hecho = k => { const M = S.M || {}; return k === "variables" ? !!(M.vars && M.vars.length) : k === "enfoque" ? !!(M.enfoque && (M.enfoque === "nosup" || M.y)) : k === "metodos" ? !!M.metodo : k === "prep" ? !!M.prepOk : k === "modelar" || k === "resultados" || k === "calidad" ? !!S.res : false; };
  function nav() {
    const n = $("#moNav"); if (!n) return; let h = `<label class="mo-psel"><small>Proyecto</small><select id="moPSel" class="mo-in"><option value="">Todos los proyectos</option>${S.proy.map(p => `<option value="${p.id}"${p.id === S.id ? " selected" : ""}>${esc(p.nombre)}</option>`).join("")}</select></label><button type="button" class="mo-nav${S.sec === "proyectos" || S.sec === "nuevo" ? " on" : ""}" data-sec="proyectos"><span class="ic">${mi("folder")}</span><span class="tx">Mis proyectos</span><span class="bd">${S.proy.length}</span></button>`;
    if (S.id && S.P) { h += `<div class="mo-prj"><small>Proyecto abierto</small><b>${esc(S.P.nombre)}</b><span>${gN(S.puntos.length, 0)} puntos · ${S.P.fuente === "denue" ? "unidades DENUE" : S.P.fuente === "archivo" ? "archivo" : S.P.fuente === "geolab" ? "Geo Lab" : "mapa"}</span></div>`;
      SEC.forEach(([k, n2, t, d]) => { h += `<button type="button" class="mo-nav${S.sec === k ? " on" : ""}" data-sec="${k}" title="${esc(d)}"><span class="ic mo-n">${n2}</span><span class="tx">${t}</span><span class="bd mo-ok">${hecho(k) ? "✓" : ""}</span></button>`; }); }
    n.innerHTML = h;
  }
  const wrap = h => { const w = $("#moWrap"); if (w) { w.innerHTML = h; $("#moMain").scrollTop = 0; } };
  async function ir(sec) {
    if (sec !== "nuevo") S.wiz = sec === "proyectos" ? null : S.wiz; S.sec = sec; nav();
    try { history.replaceState(null, "", "#" + sec); } catch (_) { }
    if (sec === "proyectos") return vistaProyectos(); if (sec === "nuevo") return wizard();
    if (!S.id) return ir("proyectos"); const f = { variables: secVariables, enfoque: secEnfoque, metodos: secMetodos, prep: secPrep, modelar: secModelar, resultados: secResultados, calidad: secCalidad }[sec]; if (f) f();
  }

  /* ───────── Mis proyectos (misma estructura que Media Lab: KPIs arriba y tarjetas o tabla) ───────── */
  const EST = { nuevo: ["Sin configurar", "#64748b"], configurado: ["Configurado", "#d97706"], modelado: ["Modelado", "#059669"] };
  async function cargaProy() { try { const j = await api(R + "/proyectos"); S.proy = j.proyectos || []; S.cargado = true; } catch (e) { toast(e.message); S.cargado = true; } nav(); }
  function vistaProyectos() {
    S.sec = "proyectos"; nav(); const L = S.proy, q = S.pq.trim().toLowerCase(), vis = L.filter(p => (!S.pf || p.estado === S.pf) && (!q || (p.nombre + " " + p.objetivo).toLowerCase().includes(q)));
    const cuenta = e => L.filter(p => p.estado === e).length, K = [["", "Todos los proyectos", L.length, "#4f46e5"], ["modelado", "Modelados", cuenta("modelado"), "#059669"], ["configurado", "Configurados", cuenta("configurado"), "#d97706"], ["nuevo", "Sin configurar", cuenta("nuevo"), "#64748b"]];
    const tarj = p => { const e = EST[p.estado] || EST.nuevo, k = (p.kpi || [])[0]; return `<article class="mo-pc" style="--c:${e[1]}"><div class="hd"><span class="mo-b" style="background:${e[1]}1a;color:${e[1]}">${e[0]}</span><i>${fecha(p.actualizado || p.creado)}</i></div><h3><a href="#" data-abrir="${p.id}">${esc(p.nombre)}</a></h3><p class="ob">${esc(p.objetivo || "Sin objetivo escrito.")}</p>
      <div class="mx"><div><b>${gN(p.nPuntos, 0)}</b><span>puntos</span></div><div><b>${p.vars || 0}</b><span>variables</span></div><div><b>${p.enfoque ? (p.enfoque === "sup" ? "Supervisado" : "No supervisado") : "—"}</b><span>enfoque</span></div></div>
      ${k ? `<div class="kp"><span>${esc(k.etiqueta)}</span><b>${esc(k.valor)}</b></div>` : ""}<div class="ac"><button type="button" class="mo-new" data-abrir="${p.id}">Abrir</button><button type="button" class="mo-ghost" data-ed="${p.id}">Editar puntos y opciones</button><button type="button" class="mo-ghost mo-pel" data-del="${p.id}">Eliminar</button></div></article>`; };
    const tabla = `<div class="mo-tw"><table class="mo-t"><thead><tr><th>Proyecto</th><th>Estado</th><th>Puntos</th><th>Variables</th><th>Enfoque</th><th>Método</th><th>Resultado</th><th>Actualizado</th><th></th></tr></thead><tbody>${vis.map(p => { const e = EST[p.estado] || EST.nuevo, k = (p.kpi || [])[0]; return `<tr class="mo-r"><td><b><a href="#" data-abrir="${p.id}">${esc(p.nombre)}</a></b></td><td><span class="mo-b" style="background:${e[1]}1a;color:${e[1]}">${e[0]}</span></td><td>${gN(p.nPuntos, 0)}</td><td>${p.vars || 0}</td><td>${p.enfoque ? (p.enfoque === "sup" ? "Supervisado" : "No supervisado") : "—"}</td><td>${esc(p.metodo || "—")}</td><td>${k ? esc(k.etiqueta + ": " + k.valor) : "—"}</td><td>${fecha(p.actualizado || p.creado)}</td><td class="mo-ac"><button type="button" class="mo-ghost" data-abrir="${p.id}">Abrir</button><button type="button" class="mo-ghost mo-pel" data-del="${p.id}">Eliminar</button></td></tr>`; }).join("")}</tbody></table></div>`;
    wrap(`<div class="mo-pv"><div class="mo-pvh"><div class="mo-h"><div><h1>Mis proyectos</h1><p>Cada proyecto reúne sus puntos de interés, sus variables y el modelo estadístico que se ajusta con ellas.</p></div></div><button type="button" class="mo-new" id="moNuevo">${mi("plus")} Nuevo proyecto</button></div>
      <div class="mo-kpis">${K.map(([v, t, n, c]) => `<button type="button" class="mo-kpi${S.pf === v ? " on" : ""}" style="--c:${c}" data-pf="${v}"><b>${n}</b><span>${t}</span></button>`).join("")}</div>
      <div class="mo-pvb"><label class="mo-q${S.pq ? " has" : ""}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg><input id="moPQ" type="search" placeholder="Buscar proyecto…" value="${esc(S.pq)}"></label>
        <div class="mo-seg"><button type="button" class="${S.pv === "card" ? "on" : ""}" data-pv="card" title="Tarjetas">${mi("grid")}</button><button type="button" class="${S.pv === "tabla" ? "on" : ""}" data-pv="tabla" title="Tabla">${mi("list")}</button></div></div>
      ${!S.cargado ? '<div class="mo-empty"><b>Cargando proyectos…</b></div>' : !vis.length ? `<div class="mo-empty"><b>${L.length ? "Ningún proyecto coincide." : "Aún no tienes proyectos."}</b>${L.length ? "" : "<p>Crea el primero: define el objetivo, los puntos de interés y las variables, y Model Lab te sugiere los métodos aplicables.</p>"}</div>` : S.pv === "tabla" ? tabla : `<div class="mo-pg">${vis.map(tarj).join("")}</div>`}</div>`);
    const i = $("#moPQ"); if (i && S._foco) { i.focus(); i.setSelectionRange(i.value.length, i.value.length); S._foco = false; }
  }
  async function abre(id) {
    try { S.busy = true; wrap('<div class="mo-empty"><b>Abriendo proyecto…</b></div>'); const j = await api(R + "/proyectos/" + id); S.id = id; S.P = j.proyecto; S.puntos = j.puntos || []; S.M = j.modelo || { vars: [], enfoque: null, y: null, x: [], metodo: null, prep: null, prepOk: false }; if (!(S.M.vars || []).length && (S.P.columnas || []).length) S.M.vars = S.P.columnas.map(c => c.name); S.ds = null; S.res = null;
      try { const r = await api(R + "/proyectos/" + id + "/resultados"); S.res = r.res || null; } catch (_) { } S.busy = false; await ir(S.res ? "resultados" : (S.M.vars && S.M.vars.length) || (S.P.columnas || []).length ? "variables" : "variables"); }
    catch (e) { S.busy = false; toast(e.message); ir("proyectos"); }
  }
  async function elimina(id) { const p = S.proy.find(x => x.id === id); if (!p || !confirm("¿Eliminar el proyecto «" + p.nombre + "»? Se borran sus puntos, datos y resultados. No se puede deshacer.")) return; try { await api(R + "/proyectos/" + id, null, "DELETE"); if (S.id === id) { S.id = S.P = S.res = null; } await cargaProy(); vistaProyectos(); toast("Proyecto eliminado."); } catch (e) { toast(e.message); } }

  /* ═══════════════ Asistente «Nuevo proyecto» (mismos pasos que Geo Lab; sin mapa conceptual; el último botón es «Modelar») ═══════════════ */
  const PASOS = [["Objetivo del proyecto", "qué quieres entender o predecir"], ["Puntos de interés", "dónde se mide: en el mapa, en un archivo o en el DENUE"], ["Radio de maqueta", "opcional · fotos de calle"], ["Vialidades", "opcional · red vial de OpenStreetMap"], ["Días y horas de tráfico", "opcional · cuándo se mide"], ["Verificación", "revisa los datos antes de modelar"]];
  const GMW_PR = { sv: .007, ia: .0053, sat: .012, ruta: .01, od: 364, m3d: .006 }, GMW_X = 10;   /* mismas tarifas de lista y factor que el asistente de Geo Lab */
  const DIAS = [[1, "Lun"], [2, "Mar"], [3, "Mié"], [4, "Jue"], [5, "Vie"], [6, "Sáb"], [0, "Dom"]], VIAS = [["motorway", "Autopistas"], ["trunk", "Troncales"], ["primary", "Primarias"], ["secondary", "Secundarias"], ["tertiary", "Terciarias"], ["residential", "Residenciales"]];
  const wizVacio = () => ({ paso: 1, nombre: "", objetivo: "", fuente: "mapa", tab: "mapa", puntos: [], columnas: [], mapa: null, arch: null, den: { muns: [], ciudades: [], q: "", sector: "", sub: "", cod: "", tmin: 0, tmax: 6, res: null, cargando: false }, radio: 200, fotos: 24, vias: ["primary", "secondary", "tertiary"], pre: 12, dias: [1, 2, 3, 4, 5], horas: [7, 8, 9, 17, 18, 19], opt: { fotos: false, trafico: false, od: false, mapa3d: false }, editId: null, msg: "", gl: null });
  function costo(W) {
    const nP = Math.max(1, W.puntos.length), o = W.opt, fotos = o.fotos ? W.fotos * nP : 0, ts = W.radio > 400 ? 344 : 172, k = Math.max(1, Math.ceil((W.radio - ts / 2) / ts)), tiles = o.fotos ? (2 * k + 1) ** 2 * nP : 0;
    const sv = fotos * GMW_PR.sv * GMW_X, ia = fotos * GMW_PR.ia * GMW_X, sat = tiles * GMW_PR.sat * GMW_X, sent = W.pre * 2 * nP, calls = o.trafico ? sent * W.dias.length * W.horas.length : 0, tr = calls * GMW_PR.ruta * GMW_X, od = o.od ? nP * GMW_PR.od * GMW_PR.ruta * GMW_X : 0, m3d = o.mapa3d ? GMW_PR.m3d * GMW_X : 0;
    return { nP, fotos, tiles, sv, ia, sat, fo: sv + ia + sat, sent, calls, tr, od, m3d, total: sv + ia + sat + tr + od + m3d };
  }
  /* aviso de «análisis costoso» (como en Geo Lab) para los pasos opcionales */
  function aviso(W, paso) {
    const c = costo(W), n = W.puntos.length, muchos = n > 20, base = `<b>Paso opcional.</b> En Model Lab las variables salen de la biblioteca nacional de indicadores (Censo, DENUE, OpenStreetMap, INEGI…) y de las columnas de tu archivo: este paso no es necesario para modelar. Lo que actives aquí queda <b>registrado en el proyecto</b>; el análisis por punto con Geo Lab todavía no se lanza automáticamente desde Model Lab.`;
    let costoTxt = "";
    if (paso === 3) costoTxt = W.opt.fotos ? `Fotos de calle con IA: ${gN(c.fotos, 0)} fotos (${W.fotos} por punto × ${gN(c.nP, 0)} puntos) y ${gN(c.tiles, 0)} teselas satelitales = <b>${usd(c.fo)}</b>.` : "Sin análisis de fotos: costo $0.";
    if (paso === 4) costoTxt = `Red vial: ${gN(c.sent, 0)} sentidos de vialidad (${W.pre} vías × 2 sentidos × ${gN(c.nP, 0)} puntos). La descarga de OpenStreetMap es gratuita; el costo aparece al consultar el tráfico.`;
    if (paso === 5) costoTxt = W.opt.trafico ? `Tráfico: ${gN(c.sent, 0)} sentidos × ${W.dias.length} días × ${W.horas.length} horas = ${gN(c.calls, 0)} consultas = <b>${usd(c.tr)}</b>.` : "Sin consulta de tráfico: costo $0.";
    const alto = (paso === 3 && W.opt.fotos && c.fo > 500) || (paso === 5 && W.opt.trafico && c.tr > 500) || (muchos && (W.opt.fotos || W.opt.trafico));
    return `<div class="mo-av${alto ? " alto" : ""}"><div>${base}</div>${costoTxt ? `<div class="c">⚠ ${costoTxt}</div>` : ""}${muchos ? `<div class="c">Tu proyecto tiene <b>${gN(n, 0)} coordenadas</b>: los análisis de «Radio de maqueta», «Vialidades» y «Días y horas de tráfico» se cobran <b>por punto</b> y pueden resultar muy costosos con decenas de puntos o más. Estimado total de lo activado: <b>${usd(c.total)}</b>.</div>` : ""}</div>`;
  }
  function wizard(P, pts) {
    if (!S.wiz) S.wiz = wizVacio(); S.sec = "nuevo"; nav(); const W = S.wiz, p = W.paso, pasos = PASOS.map((_, i) => `<i class="${i + 1 < p ? "ok" : i + 1 === p ? "on" : ""}"></i>`).join("");
    wrap(`<div class="mo-wz"><div class="mo-wzh"><span class="mo-wzp">${String(p).padStart(2, "0")}/06</span><div><h2>${PASOS[p - 1][0]}</h2><p>${PASOS[p - 1][1]}</p></div></div><div class="mo-wzg">${pasos}</div><div class="mo-wzb" id="wzBody">${wzPaso(W)}</div>
      <div class="mo-wzf"><button type="button" class="mo-ghost" data-wz="atras">${mi("back")} ${p === 1 ? "Cancelar" : "Atrás"}</button><span class="mo-wzm" id="wzMsg">${esc(W.msg || "")}</span>${p < 6 ? `<button type="button" class="mo-new" data-wz="sig">Continuar →</button>` : `<button type="button" class="mo-new mo-rojo" data-wz="modelar">${W.editId ? "Guardar y modelar" : "Modelar"}</button>`}</div></div>`);
    wzPost(W);
  }
  function wzPaso(W) {
    const p = W.paso;
    if (p === 1) return `<label class="mo-fl">Nombre del proyecto<input id="wzNom" class="mo-in" maxlength="80" placeholder="Ej. Sucursales y entorno urbano" value="${esc(W.nombre)}"></label>
      <label class="mo-fl">Objetivo<textarea id="wzObj" class="mo-in" rows="6" maxlength="4000" placeholder="Describe con tus palabras qué quieres lograr: p. ej. «Entender qué características del entorno explican las ventas de mis sucursales» o «Agrupar colonias con perfiles parecidos».">${esc(W.objetivo)}</textarea></label>
      <div class="mo-nota">Después podrás elegir variables de la biblioteca de indicadores y definir si el análisis es supervisado (hay una variable de respuesta) o no supervisado.</div>
      <div class="mo-imp"><button type="button" class="mo-ghost" data-wz="geolab">Usar un proyecto de Geo Lab</button><span>Trae su nombre, objetivo, puntos y mapa conceptual.</span></div><div id="wzGL"></div>`;
    if (p === 2) return wzPuntos(W);
    if (p === 3) return aviso(W, 3) + `<div class="mo-sw"><button type="button" class="mo-swb${W.opt.fotos ? " on" : ""}" data-wzopt="fotos"><i></i></button><div><b>Analizar fotos de calle de cada punto (maqueta con IA)</b><span>Descarga fotos de Street View alrededor de cada punto y las interpreta con IA.</span></div></div>
      <label class="mo-fl">Radio de la maqueta: <b id="wzRadV">${W.radio} m</b><input type="range" id="wzRad" min="50" max="600" step="10" value="${W.radio}"></label><div class="mo-chs">${[100, 200, 400, 600].map(r => `<button type="button" class="mo-c${W.radio === r ? " on" : ""}" data-wzrad="${r}">${r} m</button>`).join("")}</div>
      <label class="mo-fl">Fotos por punto: <b id="wzFotV">${W.fotos}</b><input type="range" id="wzFot" min="10" max="60" step="2" value="${W.fotos}"></label>`;
    if (p === 4) return aviso(W, 4) + `<div class="mo-fl">Tipos de vialidad a descargar</div><div class="mo-chs">${chips(VIAS, W.vias, "data-wzvia")}</div><label class="mo-fl">Vías preseleccionadas por punto: <b id="wzPreV">${W.pre}</b><input type="range" id="wzPre" min="2" max="30" step="1" value="${W.pre}"></label>`;
    if (p === 5) return aviso(W, 5) + `<div class="mo-sw"><button type="button" class="mo-swb${W.opt.trafico ? " on" : ""}" data-wzopt="trafico"><i></i></button><div><b>Consultar el tráfico por día y hora</b><span>Una consulta de Google Routes por cada sentido de vía, día y hora.</span></div></div>
      <div class="mo-fl">Días</div><div class="mo-chs">${chips(DIAS, W.dias, "data-wzdia")}</div><div class="mo-fl">Horas del día</div><div class="mo-chs mo-hs">${Array.from({ length: 24 }, (_, h) => [h, String(h).padStart(2, "0")]).map(([h, t]) => `<button type="button" class="mo-c${W.horas.includes(h) ? " on" : ""}" data-wzhor="${h}">${t}</button>`).join("")}</div>
      <div class="mo-chs"><button type="button" class="mo-ghost" data-wzq="pico">Hora pico</button><button type="button" class="mo-ghost" data-wzq="habil">Entre semana</button><button type="button" class="mo-ghost" data-wzq="finde">Fin de semana</button><button type="button" class="mo-ghost" data-wzq="todo">Todo el día</button></div>`;
    return wzVerif(W);
  }
  function wzVerif(W) {
    const c = costo(W), n = W.puntos.length, falta = []; if (!W.nombre.trim()) falta.push("el nombre del proyecto"); if (!n) falta.push("al menos un punto de interés");
    const fuente = { mapa: "elegidos en el mapa", archivo: "cargados desde un archivo", denue: "unidades del DENUE", geolab: "traídos de un proyecto de Geo Lab" }[W.fuente] || "";
    const fila = (t, v, b) => `<tr><td>${t}</td><td>${v}</td><td class="r">${b != null ? usd(b) : "—"}</td></tr>`;
    return `<div class="mo-fic"><div><small>Proyecto</small><b>${esc(W.nombre || "—")}</b></div><div><small>Puntos</small><b>${gN(n, 0)}</b><span>${fuente}</span></div><div><small>Columnas importadas</small><b>${W.columnas.length}</b><span>${esc(W.columnas.slice(0, 4).map(c => c.label).join(", ") || "ninguna")}${W.columnas.length > 4 ? "…" : ""}</span></div></div>
      <p class="mo-fo">${esc(W.objetivo || "Sin objetivo escrito.")}</p>
      <table class="mo-ct"><thead><tr><th>Concepto opcional</th><th>Qué incluye</th><th class="r">Estimado</th></tr></thead><tbody>${fila("Fotos de calle con IA", W.opt.fotos ? `${gN(c.fotos, 0)} fotos · radio ${W.radio} m` : "no incluido", W.opt.fotos ? c.fo : 0)}${fila("Tráfico por día y hora", W.opt.trafico ? `${gN(c.calls, 0)} consultas` : "no incluido", W.opt.trafico ? c.tr : 0)}${fila("Origen-destino", `<button type="button" class="mo-swb${W.opt.od ? " on" : ""}" data-wzopt="od"><i></i></button>`, W.opt.od ? c.od : 0)}${fila("Mapa 3D de Google", `<button type="button" class="mo-swb${W.opt.mapa3d ? " on" : ""}" data-wzopt="mapa3d"><i></i></button>`, W.opt.mapa3d ? c.m3d : 0)}</tbody><tfoot><tr><td colspan="2">Total estimado de lo activado</td><td class="r"><b>${usd(c.total)}</b></td></tr></tfoot></table>
      ${n > 20 && c.total > 0 ? `<div class="mo-av alto"><div class="c">⚠ Con ${gN(n, 0)} coordenadas, el estimado crece por punto. Revisa los pasos 3, 4 y 5 antes de continuar.</div></div>` : ""}
      <div class="mo-nota">Las variables del modelo salen de la biblioteca nacional de indicadores y de las columnas de tu archivo. Estos conceptos opcionales solo se registran en el proyecto.</div>
      ${falta.length ? `<div class="mo-av alto"><div class="c">Falta ${falta.join(" y ")}.</div></div>` : ""}`;
  }

  /* ── paso 2: puntos de interés ── */
  function wzPuntos(W) {
    const T = [["mapa", "Elegir en el mapa"], ["archivo", "Cargar archivo (CSV, SHP, GeoJSON)"], ["denue", "Unidades del DENUE"]], tab = W.tab;
    return `<div class="mo-tabs">${T.map(([k, t]) => `<button type="button" class="${tab === k ? "on" : ""}" data-wzt="${k}">${t}</button>`).join("")}</div>
      <div class="mo-pts"><b>${gN(W.puntos.length, 0)}</b> punto${W.puntos.length === 1 ? "" : "s"} listo${W.puntos.length === 1 ? "" : "s"}${W.puntos.length ? ` <i>· ${{ mapa: "elegidos en el mapa", archivo: "del archivo", denue: "del DENUE", geolab: "de Geo Lab" }[W.fuente] || ""}</i> <button type="button" class="mo-ghost" data-wz="limpia">Quitar todos</button>` : ""}</div>
      ${tab === "mapa" ? wzMapaHtml(W) : tab === "archivo" ? wzArchHtml(W) : wzDenueHtml(W)}`;
  }
  const wzMapaHtml = W => `<div class="mo-nota">Haz clic en el mapa para agregar puntos de interés (máximo 200 en este modo; para más, carga un archivo). Busca un lugar para moverte.</div>
    <div class="mo-bus"><input id="wzBus" class="mo-in" placeholder="Buscar ciudad, colonia o dirección…" autocomplete="off"><div class="mo-bl" id="wzBL" style="display:none"></div></div><div id="wzMap" class="mo-map"></div>
    <div class="mo-chs" id="wzPL">${W.puntos.slice(0, 200).map((q, i) => `<span class="mo-c on">${esc(q.id || "P" + (i + 1))} <button type="button" data-wzrm="${i}" aria-label="Quitar">×</button></span>`).join("")}</div>`;
  const wzArchHtml = W => {
    const A = W.arch; let h = `<div class="mo-nota">Carga un <b>CSV</b>, <b>GeoJSON</b> o <b>Shapefile (.zip)</b> con las coordenadas. Verás una vista previa y podrás elegir la columna de coordenadas, la de nombre o identificador y otras columnas para usarlas como variables.</div>
      <label class="mo-file"><input type="file" id="wzFile" accept=".csv,.txt,.tsv,.geojson,.json,.zip,.shp"></label>`;
    if (!A) return h;
    if (A.err) return h + `<div class="mo-av alto"><div class="c">${esc(A.err)}</div></div>`;
    const op = (sel, vacio) => (vacio ? `<option value="-1">${vacio}</option>` : "") + A.cols.map((c, i) => `<option value="${i}"${sel === i ? " selected" : ""}>${esc(c)}</option>`).join("");
    h += `<div class="mo-fa"><b>${esc(A.nombre)}</b> · ${gN(A.rows.length, 0)} filas · ${A.cols.length} columnas</div>
      <div class="mo-tw"><table class="mo-t mo-prev"><thead><tr>${A.cols.map(c => `<th>${esc(c)}</th>`).join("")}</tr></thead><tbody>${A.rows.slice(0, 8).map(r => `<tr>${A.cols.map((_, i) => `<td>${esc(r[i])}</td>`).join("")}</tr>`).join("")}</tbody></table></div>
      <div class="mo-g3"><label class="mo-fl">Columna de latitud<select class="mo-in" id="wzLat">${op(A.ilat, "— elegir —")}</select></label><label class="mo-fl">Columna de longitud<select class="mo-in" id="wzLng">${op(A.ilng, "— elegir —")}</select></label><label class="mo-fl">Nombre o identificador (opcional)<select class="mo-in" id="wzId">${op(A.iid, "— ninguno —")}</select></label></div>
      <div class="mo-fl">Otras columnas para usar como variables (opcional)</div><div class="mo-chs">${A.cols.map((c, i) => i === A.ilat || i === A.ilng ? "" : `<button type="button" class="mo-c${A.extra.includes(i) ? " on" : ""}" data-wzex="${i}">${ck(A.extra.includes(i))} ${esc(c)}</button>`).join("")}</div>
      <div class="mo-chs"><button type="button" class="mo-new" data-wz="usaarch">Usar ${gN(A.rows.length, 0)} filas como puntos</button><button type="button" class="mo-ghost" data-wzex="todas">Marcar todas</button><button type="button" class="mo-ghost" data-wzex="ninguna">Quitar todas</button></div>`;
    return h;
  };
  const SC_S = () => (S.scian && S.scian.s) || {}, SC_SS = () => (S.scian && S.scian.ss) || {};
  const wzDenueHtml = W => {
    const D = W.den, sec = Object.entries(SC_S()), sub = Object.entries(SC_SS()).filter(([k]) => D.sector && k.startsWith(D.sector));
    let h = `<div class="mo-nota">Primero define la <b>ciudad o municipio</b> para acotar la búsqueda; después filtra por palabras y por la clasificación SCIAN. El DENUE guardado incluye el <b>nombre comercial</b>, el código SCIAN y el estrato de personal (no incluye la razón social).</div>
      <div class="mo-fl">1 · Ciudades o municipios</div><div class="mo-bus"><input id="wzCiu" class="mo-in" placeholder="Escribe una ciudad o municipio (p. ej. Saltillo, Monterrey…)" autocomplete="off"><div class="mo-bl" id="wzCL" style="display:none"></div></div>
      <div class="mo-chs">${D.ciudades.map((c, i) => `<span class="mo-c on">${esc(c.n)} <small>${c.muns.length} mun.</small> <button type="button" data-wzcx="${i}" aria-label="Quitar">×</button></span>`).join("") || '<span class="mo-mut">Ninguna elegida todavía.</span>'}</div>
      <div class="mo-fl">2 · Filtros</div><div class="mo-g2"><label class="mo-fl">Palabras en el nombre del establecimiento<input id="wzQ" class="mo-in" placeholder="Ej. farmacia, abarrotes  (coma = o; espacio = y)" value="${esc(D.q)}"></label>
      <label class="mo-fl">Sector SCIAN<select class="mo-in" id="wzSec"><option value="">Todos los sectores</option>${sec.map(([k, t]) => `<option value="${k}"${D.sector === k ? " selected" : ""}>${k} · ${esc(t.length > 60 ? t.slice(0, 58) + "…" : t)}</option>`).join("")}</select></label>
      <label class="mo-fl">Subsector<select class="mo-in" id="wzSub"><option value="">Todos${D.sector ? "" : " (elige un sector)"}</option>${sub.map(([k, t]) => `<option value="${k}"${D.sub === k ? " selected" : ""}>${k} · ${esc(t)}</option>`).join("")}</select></label>
      <label class="mo-fl">Rama, subrama o clase (código de 4 a 6 dígitos; varios con coma)<input id="wzCod" class="mo-in" placeholder="Ej. 4612, 722511" value="${esc(D.cod)}"></label>
      <label class="mo-fl">Tamaño mínimo (personal ocupado)<select class="mo-in" id="wzTmin">${["0-5", "6-10", "11-30", "31-50", "51-100", "101-250", "251+"].map((t, i) => `<option value="${i}"${D.tmin === i ? " selected" : ""}>${t}</option>`).join("")}</select></label>
      <label class="mo-fl">Tamaño máximo<select class="mo-in" id="wzTmax">${["0-5", "6-10", "11-30", "31-50", "51-100", "101-250", "251+"].map((t, i) => `<option value="${i}"${D.tmax === i ? " selected" : ""}>${t}</option>`).join("")}</select></label></div>
      <div class="mo-chs"><button type="button" class="mo-new" data-wz="denbusca"${D.ciudades.length ? "" : " disabled"}>${D.cargando ? "Buscando…" : "Buscar unidades económicas"}</button></div>`;
    const r = D.res; if (r) h += `<div class="mo-fa"><b>${gN(r.total, 0)}</b> unidades encontradas${r.truncado ? ` · se usarán las primeras ${gN(r.devueltos, 0)} (afina los filtros para acotar)` : ""}</div>
      <div class="mo-tw"><table class="mo-t mo-prev"><thead><tr><th>Nombre</th><th>SCIAN</th><th>Sector</th><th>Personal</th></tr></thead><tbody>${r.items.slice(0, 10).map(i => `<tr><td>${esc(i.nombre)}</td><td>${esc(i.scian)}</td><td>${esc(SC_S()[i.scian.slice(0, 2)] ? SC_S()[i.scian.slice(0, 2)].slice(0, 40) : i.scian.slice(0, 2))}</td><td>${r.tamanos[i.tam]}</td></tr>`).join("")}</tbody></table></div>
      ${r.total ? `<div class="mo-chs"><button type="button" class="mo-new" data-wz="usaden">Usar ${gN(r.devueltos, 0)} unidades como puntos</button><span class="mo-mut">Se guardan como variables: sector, subsector, rama, grupo de giro y tamaño.</span></div>` : ""}`;
    return h;
  };

  /* ── lógica del asistente ── */
  const slug = s => "c:" + String(s || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "").slice(0, 40);
  const numV = v => { if (v == null || v === "") return null; if (typeof v === "number") return isFinite(v) ? v : null; let t = String(v).trim().replace(/[$%\s]/g, ""); if (/^-?\d+,\d+$/.test(t)) t = t.replace(",", "."); else t = t.replace(/,/g, ""); const n = Number(t); return t !== "" && isFinite(n) ? n : null; };
  function leaflet(cb) { if (window.L) return cb(); if (window._rsvEnsureLeaflet_) return window._rsvEnsureLeaflet_(cb); const c = document.createElement("link"); c.rel = "stylesheet"; c.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"; document.head.appendChild(c); const s = document.createElement("script"); s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"; s.onload = cb; document.head.appendChild(s); }
  function cargaScript(src) { return new Promise((ok, mal) => { const s = document.createElement("script"); s.src = src; s.onload = ok; s.onerror = () => mal(new Error("No se pudo cargar una biblioteca necesaria.")); document.head.appendChild(s); }); }
  function wzRe() { const W = S.wiz; if (!W) return; const b = $("#wzBody"); if (b) { b.innerHTML = wzPaso(W); wzPost(W); } const m = $("#wzMsg"); if (m) m.textContent = W.msg || ""; }
  function wzPost(W) {
    if (W.paso === 2 && W.tab === "mapa") wzMapa(W);
    if (W.paso === 2 && W.tab === "denue" && !S.scian) api(R + "/scian").then(j => { S.scian = j; if (S.wiz === W && W.paso === 2 && W.tab === "denue") wzRe(); }).catch(() => { });
  }
  function wzMapa(W) {
    leaflet(() => { const el = $("#wzMap"); if (!el || !window.L) return; if (W.gl) { try { W.gl.remove(); } catch (_) { } }
      const c = W.puntos[0] || { lat: 23.8, lng: -102.3 }, m = L.map(el).setView([c.lat, c.lng], W.puntos.length ? 13 : 5); L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 18, maxNativeZoom: 16, attribution: "Esri" }).addTo(m); W.gl = m; W.capa = L.layerGroup().addTo(m);
      const pinta = () => { W.capa.clearLayers(); W.puntos.slice(0, 500).forEach((q, i) => L.circleMarker([q.lat, q.lng], { radius: 6, color: "#4f46e5", weight: 2, fillColor: "#818cf8", fillOpacity: .9 }).bindTooltip(esc(q.id || "P" + (i + 1))).addTo(W.capa)); }; W.pinta = pinta; pinta();
      m.on("click", e => { if (W.puntos.length >= 200 && W.fuente === "mapa") { toast("En el modo mapa el máximo son 200 puntos; para más, carga un archivo."); return; } if (W.fuente !== "mapa") { W.puntos = []; W.columnas = []; W.fuente = "mapa"; }
        W.puntos.push({ lat: +e.latlng.lat.toFixed(6), lng: +e.latlng.lng.toFixed(6), id: "P" + (W.puntos.length + 1) }); pinta(); wzPts(W); }); });
  }
  function wzPts(W) {
    const n = $(".mo-pts b"); if (n) n.textContent = gN(W.puntos.length, 0); const pl = $("#wzPL"); if (pl) pl.innerHTML = W.puntos.slice(0, 200).map((q, i) => `<span class="mo-c on">${esc(q.id || "P" + (i + 1))} <button type="button" data-wzrm="${i}" aria-label="Quitar">×</button></span>`).join("");
    const pp = $(".mo-pts"); if (pp) { const b = $(".mo-pts b"); const txt = pp.querySelector("i"); if (txt) txt.textContent = "· elegidos en el mapa"; if (!pp.querySelector("[data-wz=limpia]") && W.puntos.length) pp.insertAdjacentHTML("beforeend", ' <button type="button" class="mo-ghost" data-wz="limpia">Quitar todos</button>'); }
  }
  /* búsqueda de lugares (mapa y ciudades) */
  let _bt = null;
  function busca(inp, lst, ruta, pinta, alElegir) {
    clearTimeout(_bt); const q = inp.value.trim(); if (q.length < 2) { lst.style.display = "none"; return; }
    _bt = setTimeout(async () => { lst.style.display = ""; lst.innerHTML = '<div class="mo-bn">Buscando…</div>'; try { const j = await api(ruta + encodeURIComponent(q)); const L2 = j.items || []; lst._it = L2; lst._al = alElegir; lst.innerHTML = L2.length ? L2.map((it, i) => `<button type="button" data-bli="${i}"><b>${esc(it.n)}</b><span>${esc(pinta(it))}</span></button>`).join("") : '<div class="mo-bn">Sin resultados</div>'; } catch (e) { lst.innerHTML = '<div class="mo-bn">No se pudo buscar</div>'; } }, 350);
  }
  /* archivos */
  function parseCSV(t) {
    t = t.replace(/^﻿/, ""); const f = t.split(/\r?\n/, 1)[0], sep = [",", ";", "\t", "|"].map(s => [s, f.split(s).length]).sort((a, b) => b[1] - a[1])[0][0], rows = []; let row = [], cur = "", q = false;
    for (let i = 0; i < t.length; i++) { const ch = t[i]; if (q) { if (ch === '"') { if (t[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += ch; } else if (ch === '"') q = true; else if (ch === sep) { row.push(cur); cur = ""; } else if (ch === "\n" || ch === "\r") { if (ch === "\r" && t[i + 1] === "\n") i++; row.push(cur); cur = ""; if (row.length > 1 || row[0] !== "") rows.push(row); row = []; } else cur += ch; }
    if (cur !== "" || row.length) { row.push(cur); rows.push(row); } return rows;
  }
  function geoATabla(fc) {
    const F = (fc.features || []).filter(f => f && f.geometry), props = []; F.slice(0, 300).forEach(f => Object.keys(f.properties || {}).forEach(k => { if (!props.includes(k) && props.length < 40) props.push(k); }));
    const cen = g => { const c = g.coordinates; if (g.type === "Point") return c; if (g.type === "MultiPoint") return c[0]; const pts = (g.type === "LineString" ? c : g.type === "Polygon" ? c[0] : g.type === "MultiPolygon" ? c[0][0] : g.type === "MultiLineString" ? c[0] : []); if (!pts || !pts.length) return null; let x = 0, y = 0; pts.forEach(p => { x += p[0]; y += p[1]; }); return [x / pts.length, y / pts.length]; };
    const rows = []; F.forEach(f => { const p = cen(f.geometry); if (!p || !isFinite(p[0]) || !isFinite(p[1])) return; rows.push([p[1], p[0], ...props.map(k => (f.properties || {})[k])]); }); return { cols: ["latitud", "longitud", ...props], rows };
  }
  function detCoord(cols, rows) {
    const nm = (re) => cols.findIndex(c => re.test(String(c).trim())); let ilat = nm(/^(lat|latitud|latitude|lat_dd|coord_?y|y)$/i), ilng = nm(/^(lon|lng|long|longitud|longitude|lon_dd|coord_?x|x)$/i);
    if (ilat < 0 || ilng < 0) { const s = rows.slice(0, 200); cols.forEach((_, i) => { const v = s.map(r => numV(r[i])).filter(x => x != null); if (v.length < s.length * .8) return; if (ilat < 0 && v.every(x => x >= 14 && x <= 33.5)) ilat = i; else if (ilng < 0 && v.every(x => x >= -119 && x <= -85)) ilng = i; }); }
    return [ilat, ilng];
  }
  async function wzArchivo(file) {
    const W = S.wiz; W.arch = { nombre: file.name, cols: [], rows: [], ilat: -1, ilng: -1, iid: -1, extra: [] }; const ext = (file.name.split(".").pop() || "").toLowerCase();
    try { let tabla;
      if (ext === "zip" || ext === "shp") { if (ext === "shp") throw new Error("Sube el shapefile completo en un .zip (con sus archivos .shp, .dbf y .prj)."); if (!window.shp) await cargaScript("https://unpkg.com/shpjs@4.0.4/dist/shp.min.js"); let g = await window.shp(await file.arrayBuffer()); if (Array.isArray(g)) g = g[0]; tabla = geoATabla(g); }
      else if (ext === "geojson" || ext === "json") { const g = JSON.parse(await file.text()); tabla = geoATabla(g.type === "Feature" ? { features: [g] } : g); }
      else { const r = parseCSV(await file.text()); if (r.length < 2) throw new Error("El archivo no tiene filas de datos."); tabla = { cols: r[0].map((c, i) => String(c).trim() || "columna_" + (i + 1)), rows: r.slice(1) }; }
      if (!tabla.rows.length) throw new Error("No se encontraron filas con coordenadas."); if (tabla.rows.length > 20000) { tabla.rows = tabla.rows.slice(0, 20000); toast("Se usarán las primeras 20 000 filas."); }
      const [ilat, ilng] = detCoord(tabla.cols, tabla.rows); Object.assign(W.arch, tabla, { ilat, ilng, iid: tabla.cols.findIndex(c => /^(id|nombre|name|clave|folio|sucursal|razon_social|nom_estab)$/i.test(String(c).trim())) });
    } catch (e) { W.arch.err = "No se pudo leer el archivo: " + e.message; } wzRe();
  }
  function usaArchivo(W) {
    const A = W.arch; if (A.ilat < 0 || A.ilng < 0) { W.msg = "Elige las columnas de latitud y longitud."; return wzRe(); }
    const ex = A.extra.filter(i => i !== A.ilat && i !== A.ilng), cols = ex.map(i => { const v = A.rows.map(r => numV(r[i])).filter(x => x != null); const tipo = v.length >= A.rows.filter(r => r[i] !== "" && r[i] != null).length * .9 && v.length ? "num" : "cat"; return { name: slug(A.cols[i]), label: String(A.cols[i]), tipo }; });
    const puntos = []; A.rows.forEach((r, k) => { const lat = numV(r[A.ilat]), lng = numV(r[A.ilng]); if (lat == null || lng == null || Math.abs(lat) > 90 || Math.abs(lng) > 180) return; const q = { lat, lng, id: A.iid >= 0 && r[A.iid] != null && r[A.iid] !== "" ? String(r[A.iid]).slice(0, 80) : "F" + (k + 1), x: {} }; ex.forEach((i, j) => { q.x[cols[j].name] = cols[j].tipo === "num" ? numV(r[i]) : (r[i] == null || r[i] === "" ? null : String(r[i]).slice(0, 120)); }); puntos.push(q); });
    if (!puntos.length) { W.msg = "Ninguna fila tiene coordenadas válidas."; return wzRe(); } W.puntos = puntos; W.columnas = cols; W.fuente = "archivo"; W.msg = puntos.length < A.rows.length ? `Se omitieron ${A.rows.length - puntos.length} filas sin coordenadas válidas.` : ""; wzRe(); toast(`${gN(puntos.length, 0)} puntos cargados.`);
  }
  async function denBusca(W) {
    const D = W.den; if (!D.ciudades.length) return; D.cargando = true; wzRe(); const cod = D.cod.split(/[,\s]+/).filter(c => /^\d{4,6}$/.test(c)), scian = cod.length ? cod : D.sub ? [D.sub] : D.sector ? [D.sector] : [];
    try { D.res = await api(R + "/denue", { muns: D.muns, q: D.q, scian, tmin: D.tmin, tmax: D.tmax, limite: 5000 }); } catch (e) { W.msg = e.message; D.res = null; } D.cargando = false; wzRe();
  }
  function usaDenue(W) {
    const r = W.den.res; if (!r || !r.items.length) return; const GN = r.grupos || [];
    W.puntos = r.items.map(i => ({ lat: i.lat, lng: i.lng, id: i.nombre, x: { tam: i.tam, sector: i.scian.slice(0, 2), subsector: i.scian.slice(0, 3), rama: i.scian.slice(0, 4), grupo: GN[i.grupo] || null } }));
    W.columnas = [{ name: "tam", label: "Tamaño del establecimiento (estrato 0-6)", tipo: "num" }, { name: "sector", label: "Sector SCIAN", tipo: "cat" }, { name: "subsector", label: "Subsector SCIAN", tipo: "cat" }, { name: "rama", label: "Rama SCIAN", tipo: "cat" }, { name: "grupo", label: "Grupo de giro", tipo: "cat" }]; W.fuente = "denue"; W.msg = ""; wzRe(); toast(`${gN(W.puntos.length, 0)} unidades listas como puntos.`);
  }
  async function wzGeoLab(W) {
    const b = $("#wzGL"); if (!b) return; b.innerHTML = '<div class="mo-bn">Cargando proyectos de Geo Lab…</div>';
    try { const j = await api("/geometrika/proyectos"), L2 = (j.proyectos || j.items || []).filter(p => p.origen === "geometrika" || !p.origen); W._gl = L2; b.innerHTML = L2.length ? `<div class="mo-gl">${L2.slice(0, 30).map((p, i) => `<button type="button" data-wzgl="${i}"><b>${esc(p.nombre)}</b><span>${esc((p.objetivo || p.direccion || "").slice(0, 90))} · ${p.puntos || 1} punto${(p.puntos || 1) === 1 ? "" : "s"}</span></button>`).join("")}</div>` : '<div class="mo-bn">No hay proyectos de Geo Lab.</div>'; }
    catch (e) { b.innerHTML = `<div class="mo-bn">${esc(e.message)}</div>`; }
  }
  async function wzTraeGeo(W, i) {
    const p = (W._gl || [])[i]; if (!p) return; try { const j = await api("/geometrika/proyecto?id=" + encodeURIComponent(p.id)), d = j.proyecto || j.p || j, pts = (d.puntos && d.puntos.length ? d.puntos : [{ lat: d.lat, lng: d.lng }]).filter(q => isFinite(+q.lat) && isFinite(+q.lng));
      W.nombre = d.nombre || p.nombre || ""; W.objetivo = d.objetivo || p.objetivo || ""; W.mapa = d.mapa || null; if (pts.length) { W.puntos = pts.map((q, k) => ({ lat: +q.lat, lng: +q.lng, id: (q.dir || "P" + (k + 1)).slice(0, 80) })); W.fuente = "geolab"; W.columnas = []; } W.msg = ""; wzRe(); toast("Proyecto de Geo Lab importado."); }
    catch (e) { toast(e.message); }
  }
  async function wzModelar(W) {
    if (!W.nombre.trim()) { W.paso = 1; W.msg = "Escribe el nombre del proyecto."; return wizard(); } if (!W.puntos.length) { W.paso = 2; W.msg = "Elige al menos un punto de interés."; return wizard(); }
    const c = costo(W); try { S.busy = true; const j = await api(R + "/proyectos", { id: W.editId || undefined, nombre: W.nombre.trim(), objetivo: W.objetivo.trim(), fuente: W.fuente, puntos: W.puntos, columnas: W.columnas, mapa: W.mapa, cfg: { radio: W.radio, fotos: W.fotos, vias: W.vias, pre: W.pre, dias: W.dias, horas: W.horas, opt: W.opt, costoEstimadoUSD: Math.round(c.total * 100) / 100 } });
      S.wiz = null; await cargaProy(); await abre(j.proyecto.id); toast(W.editId ? "Proyecto actualizado." : "Proyecto creado."); } catch (e) { S.busy = false; W.msg = e.message; wizard(); }
  }
  async function editar(id) {
    try { const j = await api(R + "/proyectos/" + id), p = j.proyecto, W = wizVacio(), c = p.cfg || {}; Object.assign(W, { nombre: p.nombre, objetivo: p.objetivo || "", fuente: p.fuente || "mapa", tab: p.fuente === "denue" ? "denue" : p.fuente === "archivo" ? "archivo" : "mapa", puntos: j.puntos || [], columnas: p.columnas || [], mapa: p.mapa || null, editId: id, radio: c.radio || 200, fotos: c.fotos || 24, vias: c.vias || W.vias, pre: c.pre || 12, dias: c.dias || W.dias, horas: c.horas || W.horas, opt: Object.assign(W.opt, c.opt || {}) }); S.wiz = W; wizard(); }
    catch (e) { toast(e.message); }
  }
  function wizClic(e) {
    const W = S.wiz, t = e.target; let b;
    if ((b = t.closest("[data-wz]"))) { const a = b.dataset.wz;
      if (a === "atras") { if (W.paso === 1) { S.wiz = null; return ir("proyectos"), true; } W.paso--; W.msg = ""; wizard(); return true; }
      if (a === "sig") { if (W.paso === 1 && !W.nombre.trim()) { W.msg = "Escribe el nombre del proyecto."; wzRe(); return true; } if (W.paso === 2 && !W.puntos.length) { W.msg = "Elige al menos un punto de interés."; wzRe(); return true; } W.paso++; W.msg = ""; wizard(); return true; }
      if (a === "modelar") { wzModelar(W); return true; } if (a === "limpia") { W.puntos = []; W.columnas = []; W.fuente = W.tab === "denue" ? "denue" : W.tab; if (W.pinta) W.pinta(); wzRe(); return true; }
      if (a === "geolab") { wzGeoLab(W); return true; } if (a === "usaarch") { usaArchivo(W); return true; } if (a === "denbusca") { denBusca(W); return true; } if (a === "usaden") { usaDenue(W); return true; } }
    if ((b = t.closest("[data-wzt]"))) { W.tab = b.dataset.wzt; W.msg = ""; wzRe(); const bar = $(".mo-tabs"); if (bar) bar.outerHTML = bar.outerHTML; return true; }
    if ((b = t.closest("[data-wzopt]"))) { const k = b.dataset.wzopt; W.opt[k] = !W.opt[k]; wzRe(); return true; }
    if ((b = t.closest("[data-wzrad]"))) { W.radio = +b.dataset.wzrad; wzRe(); return true; }
    if ((b = t.closest("[data-wzvia]"))) { const k = b.dataset.wzvia; W.vias = W.vias.includes(k) ? W.vias.filter(x => x !== k) : W.vias.concat(k); wzRe(); return true; }
    if ((b = t.closest("[data-wzdia]"))) { const k = +b.dataset.wzdia; W.dias = W.dias.includes(k) ? W.dias.filter(x => x !== k) : W.dias.concat(k); wzRe(); return true; }
    if ((b = t.closest("[data-wzhor]"))) { const k = +b.dataset.wzhor; W.horas = W.horas.includes(k) ? W.horas.filter(x => x !== k) : W.horas.concat(k); wzRe(); return true; }
    if ((b = t.closest("[data-wzq]"))) { const q = b.dataset.wzq; if (q === "pico") W.horas = [7, 8, 9, 17, 18, 19]; if (q === "habil") W.dias = [1, 2, 3, 4, 5]; if (q === "finde") W.dias = [6, 0]; if (q === "todo") W.horas = Array.from({ length: 24 }, (_, h) => h); wzRe(); return true; }
    if ((b = t.closest("[data-wzrm]"))) { W.puntos.splice(+b.dataset.wzrm, 1); if (W.pinta) W.pinta(); wzPts(W); return true; }
    if ((b = t.closest("[data-wzex]"))) { const v = b.dataset.wzex, A = W.arch; if (v === "todas") A.extra = A.cols.map((_, i) => i).filter(i => i !== A.ilat && i !== A.ilng); else if (v === "ninguna") A.extra = []; else { const i = +v; A.extra = A.extra.includes(i) ? A.extra.filter(x => x !== i) : A.extra.concat(i); } wzRe(); return true; }
    if ((b = t.closest("[data-wzcx]"))) { W.den.ciudades.splice(+b.dataset.wzcx, 1); W.den.muns = [...new Set(W.den.ciudades.flatMap(c => c.muns))]; W.den.res = null; wzRe(); return true; }
    if ((b = t.closest("[data-wzgl]"))) { wzTraeGeo(W, +b.dataset.wzgl); return true; }
    if ((b = t.closest("[data-bli]"))) { const l = b.closest(".mo-bl"), it = (l._it || [])[+b.dataset.bli]; if (it && l._al) l._al(it); l.style.display = "none"; return true; }
    return false;
  }
  function wizEntrada(e) {
    const W = S.wiz, t = e.target, id = t.id; if (!W) return false;
    if (id === "wzNom") { W.nombre = t.value; return true; } if (id === "wzObj") { W.objetivo = t.value; return true; }
    if (id === "wzRad") { W.radio = +t.value; const v = $("#wzRadV"); if (v) v.textContent = W.radio + " m"; return true; } if (id === "wzFot") { W.fotos = +t.value; const v = $("#wzFotV"); if (v) v.textContent = W.fotos; return true; } if (id === "wzPre") { W.pre = +t.value; const v = $("#wzPreV"); if (v) v.textContent = W.pre; return true; }
    if (id === "wzQ") { W.den.q = t.value; return true; } if (id === "wzCod") { W.den.cod = t.value; return true; }
    if (id === "wzBus") { busca(t, $("#wzBL"), "/geometrika/malla/buscar?q=", it => it.t + (it.d ? " · " + it.d : ""), it => { const m = W.gl; if (!m) return; const bb = it.bb; if (bb && bb[2] - bb[0] > 1e-4) m.fitBounds([[bb[1], bb[0]], [bb[3], bb[2]]], { maxZoom: 17 }); else m.setView([it.lat, it.lng], 16); t.value = it.n; }); return true; }
    if (id === "wzCiu") { busca(t, $("#wzCL"), R + "/ciudades?q=", it => it.t + " · " + it.d, it => { if (!W.den.ciudades.some(c => c.n === it.n)) W.den.ciudades.push({ n: it.n, muns: it.muns }); W.den.muns = [...new Set(W.den.ciudades.flatMap(c => c.muns))]; W.den.res = null; wzRe(); }); return true; }
    return false;
  }
  function wizCambia(e) {
    const W = S.wiz, t = e.target, id = t.id; if (!W) return false;
    if (id === "wzFile") { if (t.files[0]) wzArchivo(t.files[0]); return true; }
    if (id === "wzLat") { W.arch.ilat = +t.value; wzRe(); return true; } if (id === "wzLng") { W.arch.ilng = +t.value; wzRe(); return true; } if (id === "wzId") { W.arch.iid = +t.value; return true; }
    if (id === "wzSec") { W.den.sector = t.value; W.den.sub = ""; W.den.res = null; wzRe(); return true; } if (id === "wzSub") { W.den.sub = t.value; W.den.res = null; return true; }
    if (id === "wzTmin") { W.den.tmin = +t.value; if (W.den.tmax < W.den.tmin) W.den.tmax = W.den.tmin; wzRe(); return true; } if (id === "wzTmax") { W.den.tmax = +t.value; if (W.den.tmin > W.den.tmax) W.den.tmin = W.den.tmax; wzRe(); return true; }
    if (id === "wzRad" || id === "wzFot" || id === "wzPre") { wzRe(); return true; }
    return false;
  }

  /* ═══════════════ Flujo del proyecto: 1 Variables · 2 Enfoque · 3 Métodos · 4 Preprocesamiento · 5 Modelar ═══════════════ */
  const BIB = () => S.bib, BIBK = () => { if (!S._bk || S._bkn !== S.bib.length) { S._bk = {}; S.bib.forEach(b => { S._bk[b.k] = b; }); S._bkn = S.bib.length; } return S._bk; };
  const esLib = k => !!BIBK()[k];
  const colsProy = () => (S.P && S.P.columnas) || [];
  const nomVar = k => { const b = BIBK()[k]; if (b) return b.n; const c = colsProy().find(c => c.name === k); return c ? c.label : k; };
  async function guardaModelo() { if (!S.id) return; try { await api(R + "/proyectos/" + S.id + "/modelo", { modelo: S.M }); const p = S.proy.find(x => x.id === S.id); if (p) { p.enfoque = S.M.enfoque; p.metodo = S.M.metodo; p.vars = (S.M.x || []).length + (S.M.y ? 1 : 0); } } catch (e) { toast("No se pudo guardar la configuración: " + e.message); } nav(); }
  const cab = (n, t, d) => `<div class="mo-h"><div><h1><span class="mo-hn">${n}</span> ${t}</h1><p>${d}</p></div></div>`;
  const pie = (atras, sig, txt, dis) => `<div class="mo-wzf">${atras ? `<button type="button" class="mo-ghost" data-sec="${atras}">${mi("back")} Atrás</button>` : "<span></span>"}<span class="mo-wzm" id="fMsg"></span>${sig ? `<button type="button" class="mo-new" data-fl="${sig}"${dis ? " disabled" : ""}>${txt} →</button>` : ""}</div>`;

  /* ── 1 · Variables ── */
  function secVariables() {
    S.sec = "variables"; nav(); const M = S.M, sel = new Set(M.vars || []), colsP = colsProy(), cat = S.vcat || "", q = (S.vq || "").toLowerCase(), B = BIB().filter(b => (!cat || b.c === cat) && (!q || (b.n + " " + b.e + " " + b.k).toLowerCase().includes(q)));
    const cats = [...new Set(BIB().map(b => b.c))], grp = {}; B.forEach(b => { (grp[b.c + " › " + b.e] = grp[b.c + " › " + b.e] || []).push(b); });
    const mapa = S.P.mapa && S.P.mapa.nodos ? mapaHtml(S.P.mapa, sel) : "";
    wrap(`${cab("1", "Variables del modelo", "Elige las variables que usará el análisis: las columnas de tu proyecto y los indicadores de la biblioteca nacional.")}
      ${mapa}
      ${colsP.length ? `<section class="mo-sx2"><h3>Columnas de tu proyecto <small>${colsP.length}</small></h3><p class="mo-mut">Vienen de tu archivo o del DENUE. Pueden ser variables explicativas o la variable de respuesta.</p><div class="mo-chs">${colsP.map(c => `<button type="button" class="mo-c${sel.has(c.name) ? " on" : ""}" data-v="${esc(c.name)}">${ck(sel.has(c.name))} ${esc(c.label)} <small>${c.tipo === "num" ? "número" : "categoría"}</small></button>`).join("")}</div></section>` : ""}
      <section class="mo-sx2"><h3>Biblioteca de indicadores <small>${BIB().length} disponibles por coordenada</small></h3><p class="mo-mut">Cada punto se ubica en su AGEB y recibe el valor precalculado de cada indicador (Censo, DENUE, OpenStreetMap, INEGI, WorldCover, WorldClim…). Los puntos fuera de una AGEB urbana quedan sin dato.</p>
        <div class="mo-vb"><label class="mo-q${S.vq ? " has" : ""}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg><input id="moVQ" type="search" placeholder="Buscar indicador…" value="${esc(S.vq || "")}"></label>
        <div class="mo-chs"><button type="button" class="mo-c${cat ? "" : " on"}" data-vcat="">Todas</button>${cats.map(c => `<button type="button" class="mo-c${cat === c ? " on" : ""}" data-vcat="${esc(c)}">${esc(c)}</button>`).join("")}</div></div>
        ${Object.keys(grp).length ? Object.entries(grp).map(([g, L2]) => { const n = L2.filter(b => sel.has(b.k)).length; return `<details class="mo-sx"${q || n ? " open" : ""}><summary><b>${esc(g)}</b><span>${n}/${L2.length} elegidos</span></summary><div class="mo-vl"><div class="mo-chs"><button type="button" class="mo-ghost" data-vg="${esc(L2.map(b => b.k).join("|"))}" data-on="1">Elegir todos</button><button type="button" class="mo-ghost" data-vg="${esc(L2.map(b => b.k).join("|"))}" data-on="0">Quitar</button></div>${L2.map(b => `<button type="button" class="mo-vi${sel.has(b.k) ? " on" : ""}" data-v="${esc(b.k)}">${ck(sel.has(b.k))}<span><b>${esc(b.n)}</b><i>${esc(b.u)}${b.cat ? " · categórico" : ""}</i></span></button>`).join("")}</div></details>`; }).join("") : '<div class="mo-empty"><b>Ningún indicador coincide.</b></div>'}</section>
      <div class="mo-resv" id="vRes">${resumenVars()}</div>${pie(null, "construir", "Construir tabla de datos y continuar", !(M.vars || []).length)}`);
    const i = $("#moVQ"); if (i && S._vfoco) { i.focus(); i.setSelectionRange(i.value.length, i.value.length); S._vfoco = false; }
  }
  const resumenVars = () => { const v = S.M.vars || [], nl = v.filter(esLib).length; return `<b>${v.length}</b> variable${v.length === 1 ? "" : "s"} elegida${v.length === 1 ? "" : "s"} · ${nl} de la biblioteca · ${v.length - nl} de tu proyecto${S.ds ? ` · tabla actual: ${gN(S.ds.rows.length, 0)} filas × ${S.ds.cols.length} columnas` : ""}`; };
  function mapaHtml(mapa, sel) {
    const nodos = mapa.nodos || [], niv = [...new Set(nodos.map(n => n.niv))].sort((a, b) => a - b); const rec = n => (n.rec || []).map(r => { const k = String(r.id || "").replace(/^i:/, ""); return BIBK()[k] ? `<button type="button" class="mo-c${sel.has(k) ? " on" : ""}" data-v="${esc(k)}">${ck(sel.has(k))} ${esc(BIBK()[k].n)}</button>` : ""; }).join("");
    const todos = [...new Set(nodos.flatMap(n => (n.rec || []).map(r => String(r.id || "").replace(/^i:/, "")).filter(k => BIBK()[k])))];
    return `<section class="mo-sx2"><h3>Mapa conceptual del proyecto <small>${nodos.length} conceptos</small></h3><p class="mo-mut">Estos indicadores se definieron al crear el proyecto en Geo Lab. Puedes usarlos, quitarlos u omitirlos y tomar otros de la biblioteca.</p><div class="mo-chs"><button type="button" class="mo-ghost" data-vg="${esc(todos.join("|"))}" data-on="1">Usar todos los del mapa</button><button type="button" class="mo-ghost" data-vg="${esc(todos.join("|"))}" data-on="0">Omitirlos</button></div>
      ${niv.map(l => `<div class="mo-mn"><small>${l === 0 ? "Objetivo final" : "Nivel " + l}</small>${nodos.filter(n => n.niv === l).map(n => `<div class="mo-mc"><b>${esc(n.t)}</b>${n.d ? `<span>${esc(n.d)}</span>` : ""}<div class="mo-chs">${rec(n) || '<i class="mo-mut">sin indicadores disponibles por coordenada</i>'}</div></div>`).join("")}</div>`).join("")}</section>`;
  }
  async function construyeDatos() {
    const M = S.M, vars = M.vars || [], lib = vars.filter(esLib), prj = vars.filter(v => !esLib(v)), pts = S.puntos; if (!vars.length) return;
    const ov = popup("Construyendo la tabla de datos", "Ubicando cada punto en su AGEB y leyendo los indicadores…"); let sin = 0, ageb = [];
    try { const rows = pts.map(q => prj.map(c => (q.x && q.x[c] != null) ? q.x[c] : null)); let lr = pts.map(() => []);
      if (lib.length) { const specs = lib.map(k => BIBK()[k].s); for (let i = 0; i < pts.length; i += 3000) { const trozo = pts.slice(i, i + 3000), j = await api(R + "/tabla", { puntos: trozo.map(q => [q.lat, q.lng]), specs }); sin += j.sinAgeb; j.rows.forEach((r, k) => { lr[i + k] = r; }); ageb = ageb.concat(j.ageb); popupAv(ov, (i + trozo.length) / pts.length); await tick(); } }
      const cols = prj.map(c => { const d = colsProy().find(x => x.name === c) || {}; return { name: c, label: d.label || c, tipo: d.tipo || "", unit: "" }; }).concat(lib.map(k => ({ name: k, label: BIBK()[k].n, unit: BIBK()[k].u, tipo: BIBK()[k].cat ? "cat" : "num" })));
      S.ds = { cols, rows: rows.map((r, i) => r.concat(lr[i])), ids: pts.map((q, i) => q.id || "P" + (i + 1)), lat: pts.map(q => q.lat), lng: pts.map(q => q.lng), ageb, sinAgeb: sin };
      S.perfiles = null; S.res = null; try { await api(R + "/proyectos/" + S.id + "/datos", { ds: S.ds }); } catch (_) { }
      popupCierra(ov); await guardaModelo(); if (sin) toast(`${gN(sin, 0)} de ${gN(pts.length, 0)} puntos no cayeron en una AGEB urbana: sus indicadores de la biblioteca quedan vacíos.`, 6000); ir("enfoque"); }
    catch (e) { popupCierra(ov); toast(e.message, 6000); }
  }
  async function aseguraDs() {
    if (S.ds) return true; try { const j = await api(R + "/proyectos/" + S.id + "/datos"); if (j.ds && j.ds.rows) { S.ds = j.ds; return true; } } catch (_) { } return false;
  }
  const perfiles = () => { if (!S.perfiles && S.ds && window.MLMotor) { try { S.perfiles = window.MLMotor.perfilar(S.ds); } catch (e) { toast(e.message); } } return S.perfiles || []; };
  const perfilDe = n => perfiles().find(p => p.name === n);

  /* ── 2 · Enfoque y variable de respuesta ── */
  async function secEnfoque() {
    S.sec = "enfoque"; nav(); if (!(await aseguraDs())) { wrap(`${cab("2", "Enfoque", "")}<div class="mo-empty"><b>Primero construye la tabla de datos.</b><p>Elige las variables y pulsa «Construir tabla de datos».</p><button type="button" class="mo-new" data-sec="variables">Ir a Variables</button></div>`); return; }
    const M = S.M, P = perfiles(), enf = M.enfoque, usables = P.filter(p => p.tipo !== "texto" && !p.constante), y = M.y, x = new Set(M.x || []);
    const tipoTxt = p => ({ num: "numérica continua", int: "numérica entera", bin: "binaria (2 valores)", cat: "categórica", texto: "texto (no usable)" }[p.tipo] || p.tipo);
    const filas = usables.filter(p => !(enf === "sup" && p.name === y)).map(p => `<tr><td><button type="button" class="mo-vi${x.has(p.name) ? " on" : ""}" data-x="${esc(p.name)}">${ck(x.has(p.name))}<span><b>${esc(p.label || p.name)}</b></span></button></td><td>${tipoTxt(p)}</td><td>${gN(p.unicos, 0)}</td><td${p.pctFaltantes > 30 ? ' class="mo-mal"' : ""}>${gN(p.pctFaltantes, 1)} %</td><td>${p.tipo === "num" || p.tipo === "int" ? gN(p.media) + " ± " + gN(p.sd) : (p.categorias || []).slice(0, 2).map(c => esc(c.v)).join(", ")}</td></tr>`).join("");
    const exc = P.filter(p => p.tipo === "texto" || p.constante);
    wrap(`${cab("2", "Enfoque del análisis", "¿Hay algo que quieras predecir o explicar, o quieres explorar la estructura de los datos?")}
      <div class="mo-bxg"><button type="button" class="mo-bxo${enf === "sup" ? " on" : ""}" data-enf="sup"><i>🎯</i><b>Supervisado</b><small>Hay una <b>variable de respuesta</b> (lo que quieres predecir o explicar) y variables explicativas. Ej.: ventas ← entorno urbano.</small></button>
        <button type="button" class="mo-bxo${enf === "nosup" ? " on" : ""}" data-enf="nosup"><i>🧭</i><b>No supervisado</b><small>Solo un conjunto de variables: se buscan <b>grupos, patrones, relaciones o casos atípicos</b> sin variable objetivo.</small></button></div>
      ${enf ? `<section class="mo-sx2">${enf === "sup" ? `<h3>Variable de respuesta (dependiente) <small>obligatoria</small></h3><label class="mo-fl"><select class="mo-in" id="moY"><option value="">— Elige la variable de respuesta —</option>${usables.map(p => `<option value="${esc(p.name)}"${y === p.name ? " selected" : ""}>${esc(p.label || p.name)} · ${tipoTxt(p)}</option>`).join("")}</select></label>${y && perfilDe(y) ? `<div class="mo-nota">La respuesta es <b>${tipoTxt(perfilDe(y))}</b>${perfilDe(y).tipo === "num" || perfilDe(y).tipo === "int" ? ": el análisis será de <b>regresión</b>." : perfilDe(y).tipo === "bin" ? ": el análisis será de <b>clasificación binaria</b>." : ": el análisis será de <b>clasificación en varias categorías</b>."}</div>` : ""}` : ""}
        <h3>${enf === "sup" ? "Variables explicativas (independientes)" : "Variables del análisis"} <small>${x.size} elegidas</small></h3><p class="mo-mut">${enf === "sup" ? "Por omisión entran todas las demás variables; quita las que no quieras usar." : "Por omisión entran todas; quita las que no quieras usar."}</p>
        <div class="mo-chs"><button type="button" class="mo-ghost" data-xg="1">Elegir todas</button><button type="button" class="mo-ghost" data-xg="0">Quitar todas</button></div>
        <div class="mo-tw"><table class="mo-t"><thead><tr><th>Variable</th><th>Naturaleza</th><th>Valores distintos</th><th>Faltantes</th><th>Resumen</th></tr></thead><tbody>${filas}</tbody></table></div>
        ${exc.length ? `<div class="mo-nota">No se usarán: ${exc.map(p => esc(p.label || p.name) + (p.constante ? " (constante)" : " (texto libre)")).join(", ")}.</div>` : ""}</section>` : ""}
      ${pie("variables", "metodos", "Continuar a Métodos", !(enf && (enf === "nosup" ? x.size >= 2 : (y && x.size >= 1))))}`);
  }

  /* ── 3 · Métodos analíticos factibles ── */
  async function secMetodos() {
    S.sec = "metodos"; nav(); if (!(await aseguraDs()) || !S.M.enfoque) return ir("enfoque"); if (!window.MLMotor) { wrap('<div class="mo-empty"><b>No se cargó el motor de modelado.</b></div>'); return; }
    const M = S.M, P = perfiles(), py = M.y ? perfilDe(M.y) : null, px = (M.x || []).map(perfilDe).filter(Boolean); let L2 = [];
    try { L2 = window.MLMotor.sugerirMetodos(M.enfoque, py, px, S.ds.rows.length) || []; } catch (e) { toast(e.message); }
    const rec = L2.find(m => m.recomendado && m.aplica); if (!M.metodo && rec) { M.metodo = rec.id; M.metodoNombre = rec.nombre; guardaModelo(); } const sm = L2.find(m => m.id === M.metodo); if (sm) M.metodoNombre = sm.nombre;
    const nivel = v => `<span class="mo-nv ${v}">${{ bajo: "Bajo", baja: "Baja", medio: "Medio", media: "Media", alto: "Alto", alta: "Alta" }[v] || v}</span>`;
    const fam = [...new Set(L2.map(m => m.familia))];
    wrap(`${cab("3", "Métodos analíticos factibles", M.enfoque === "sup" ? `Con tu variable de respuesta (${py ? esc(py.tipo === "num" || py.tipo === "int" ? "numérica" : py.tipo === "bin" ? "binaria" : "categórica") : "—"}) y ${px.length} variables explicativas, estos son los métodos aplicables.` : `Con ${px.length} variables y sin variable objetivo, estos son los métodos de exploración aplicables.`)}
      ${fam.map(f => `<section class="mo-sx2"><h3>${esc(f)}</h3><div class="mo-mg">${L2.filter(m => m.familia === f).map(m => `<button type="button" class="mo-mt${M.metodo === m.id ? " on" : ""}${m.aplica ? "" : " no"}" data-met="${m.id}"${m.aplica ? "" : " disabled"}><div class="t"><b>${esc(m.nombre)}</b>${m.recomendado && m.aplica ? '<span class="mo-rec">Recomendado</span>' : ""}${m.aplica ? "" : '<span class="mo-no">No aplica</span>'}</div><p>${esc(m.desc)}</p><div class="mo-razon">${esc(m.razon || "")}</div>
        <div class="mo-pc2"><div><small>Ventajas</small><ul>${(m.ventajas || []).map(v => `<li>${esc(v)}</li>`).join("")}</ul></div><div><small>Alcances y límites</small><ul>${(m.limites || []).map(v => `<li>${esc(v)}</li>`).join("")}</ul></div></div>
        <div class="mo-rs"><span>Tiempo ${nivel(m.recursos.tiempo)}</span><span>Memoria ${nivel(m.recursos.memoria)}</span><i>${esc(m.recursos.etiqueta || "")}</i></div></button>`).join("")}</div></section>`).join("")}
      ${M.enfoque === "nosup" && (M.metodo === "kmeans" || M.metodo === "jerarquico") ? `<label class="mo-fl">Número de grupos <small class="mo-mut">(déjalo vacío para que Model Lab lo elija)</small><input class="mo-in mo-num" id="moK" type="number" min="2" max="20" value="${M.k || ""}"></label>` : ""}
      ${pie("enfoque", "prep", "Continuar a Preprocesamiento", !M.metodo)}`);
  }

  /* ── 4 · Preprocesamiento ── */
  const CAT_NOM = { faltantes: "Datos faltantes e imputación", outliers: "Valores atípicos (outliers)", escalado: "Estandarización de unidades y escalas", codificacion: "Codificación de categorías", transformacion: "Transformaciones de forma", duplicados: "Filas duplicadas", colineales: "Variables muy correlacionadas entre sí", constantes: "Variables constantes", columnaFaltantes: "Columnas con demasiados faltantes" };
  async function secPrep() {
    S.sec = "prep"; nav(); if (!(await aseguraDs()) || !S.M.metodo) return ir("metodos"); const M = S.M, P = perfiles(), cat = window.MLMotor.catalogoPrep;
    const ctx = { enfoque: M.enfoque, y: M.y || null, x: M.x || [], n: S.ds.rows.length, metodo: M.metodo }; let sug; try { sug = window.MLMotor.sugerirPrep(P, ctx); } catch (e) { toast(e.message); return; }
    if (!M.prep) { M.prep = { global: Object.fromEntries(Object.entries(sug.global).map(([k, v]) => [k, v.id])), porColumna: {} }; }
    const g = M.prep.global;
    wrap(`${cab("4", "Preprocesamiento", "Model Lab eligió automáticamente cómo limpiar y preparar los datos según la naturaleza de cada variable. Revisa lo que se hará y cámbialo si lo prefieres.")}
      <section class="mo-sx2"><h3>Lo que se hará <small>automático</small></h3><ul class="mo-rsm">${(sug.resumen || []).map(r => `<li>${esc(r)}</li>`).join("")}</ul></section>
      <section class="mo-sx2"><h3>Métodos elegidos <small>puedes cambiarlos</small></h3><div class="mo-pg2">${Object.keys(CAT_NOM).filter(k => cat[k]).map(k => `<div class="mo-pm"><label class="mo-fl">${CAT_NOM[k]}<select class="mo-in" data-prep="${k}">${cat[k].map(o => `<option value="${o.id}"${g[k] === o.id ? " selected" : ""}>${esc(o.nombre)}${sug.global[k] && sug.global[k].id === o.id ? " (sugerido)" : ""}</option>`).join("")}</select></label><p>${esc((cat[k].find(o => o.id === g[k]) || {}).desc || "")}</p><i>${g[k] === (sug.global[k] || {}).id ? esc((sug.global[k] || {}).razon || "") : "Elegido por ti; el sugerido era «" + esc(((cat[k].find(o => o.id === (sug.global[k] || {}).id)) || {}).nombre || "") + "»."}</i></div>`).join("")}</div>
        <div class="mo-nota">Siempre se aplica también la limpieza de formatos: se recortan espacios, se convierten textos numéricos («1,234.5», «12 %») y se unifican las categorías escritas distinto.</div></section>
      ${Object.keys(sug.porColumna || {}).length ? `<details class="mo-sx"><summary><b>Ajustes por variable</b><span>${Object.keys(sug.porColumna).length} variables con tratamiento propio</span></summary><div class="mo-tw"><table class="mo-t"><thead><tr><th>Variable</th><th>Qué se hará</th></tr></thead><tbody>${Object.entries(sug.porColumna).map(([n, o]) => `<tr><td>${esc(nomVar(n))}</td><td>${esc(o.razon || [o.faltantes, o.outliers, o.transformacion].filter(Boolean).join(" · "))}</td></tr>`).join("")}</tbody></table></div></details>` : ""}
      <div class="mo-nota">Para evitar contaminar la evaluación, todo el preprocesamiento se calcula <b>solo con los datos de entrenamiento</b> y después se aplica a los de prueba.</div>${pie("metodos", "modelar", "Confirmar y continuar", false)}`);
    M.prep.porColumna = Object.assign({}, sug.porColumna || {}, M.prep.porColumna || {});
  }

  /* ── ventana emergente de etapas ── */
  function popup(titulo, sub) {
    const o = document.createElement("div"); o.className = "mo-pop"; o.setAttribute("role", "dialog"); o.setAttribute("aria-modal", "true");
    o.innerHTML = `<div class="bx mo-pp"><header><div><small>Model Lab</small><b>${esc(titulo)}</b></div></header><div class="bd"><p class="mo-pps">${esc(sub || "")}</p><div class="mo-pbar"><i style="width:2%"></i></div><ul class="mo-pet"></ul><div class="mo-pd"></div></div><footer></footer></div>`; $("#moApp").appendChild(o); return o;
  }
  const popupAv = (o, f) => { const i = o.querySelector(".mo-pbar i"); if (i) i.style.width = Math.max(2, Math.min(100, Math.round(f * 100))) + "%"; };
  const popupCierra = o => { if (o && o.parentNode) o.parentNode.removeChild(o); };
  function popupEtapas(o, E, act, hechas, det) {
    o.querySelector(".mo-pet").innerHTML = E.map(([id, n], i) => `<li class="${hechas.has(id) ? "ok" : id === act ? "on" : ""}"><i>${hechas.has(id) ? "✓" : id === act ? '<b class="mo-spin"></b>' : i + 1}</i><span>${esc(n)}</span>${hechas.has(id) && hechas.get(id) != null ? `<em>${gN(hechas.get(id) / 1000, 1)} s</em>` : ""}</li>`).join(""); o.querySelector(".mo-pd").textContent = det || "";
  }
  const ETAPAS = { sup: [["prep", "Preprocesamiento"], ["entrenamiento", "Entrenamiento"], ["calibracion", "Calibración de hiperparámetros"], ["evaluacion", "Evaluación con datos de prueba"], ["diagnostico", "Diagnóstico e importancia de variables"]], nosup: [["prep", "Preprocesamiento"], ["reduccion", "Reducción y estandarización"], ["ajuste", "Ajuste del modelo"], ["validacion", "Validación"], ["interpretacion", "Interpretación"]] };

  /* ── 5 · Modelar / Analizar ── */
  function secModelar() {
    S.sec = "modelar"; nav(); const M = S.M; if (!M.metodo || !M.prepOk && !M.prep) return ir(M.metodo ? "prep" : "metodos"); const sup = M.enfoque === "sup";
    wrap(`${cab("5", "Modelar / Analizar", sup ? "Se entrena el modelo, se calibra y se evalúa con datos que no vio al entrenar." : "Se ajusta el análisis exploratorio y se valida su calidad.")}
      <div class="mo-fic"><div><small>Enfoque</small><b>${sup ? "Supervisado" : "No supervisado"}</b></div><div><small>${sup ? "Variable de respuesta" : "Variables"}</small><b>${sup ? esc(nomVar(M.y)) : (M.x || []).length}</b></div><div><small>${sup ? "Variables explicativas" : "Observaciones"}</small><b>${sup ? (M.x || []).length : gN(S.ds ? S.ds.rows.length : S.puntos.length, 0)}</b></div></div>
      <div class="mo-fic"><div><small>Método</small><b>${esc(M.metodoNombre || M.metodo)}</b></div><div><small>Observaciones</small><b>${gN(S.ds ? S.ds.rows.length : S.puntos.length, 0)}</b></div><div><small>Preprocesamiento</small><b>${Object.keys((M.prep || {}).global || {}).length} reglas</b></div></div>
      <div class="mo-etp"><b>Etapas</b><ol>${ETAPAS[sup ? "sup" : "nosup"].map(([, n]) => `<li>${n}</li>`).join("")}</ol></div>
      <div class="mo-go"><button type="button" class="mo-new mo-rojo mo-big" data-fl="correr">${sup ? "Modelar" : "Analizar"}</button>${S.res ? '<span class="mo-mut">Ya hay un resultado guardado; volver a modelar lo reemplaza.</span>' : ""}</div>${pie("prep", null, "", false)}`);
  }
  const fmtK = k => typeof k.valor === "number" ? (k.formato === "pct" ? gN(k.valor * 100, 1) + " %" : gN(k.valor)) : String(k.valor == null ? "—" : k.valor);
  async function corre() {
    const M = S.M, sup = M.enfoque === "sup", E = ETAPAS[sup ? "sup" : "nosup"]; if (!(await aseguraDs())) { toast("Falta la tabla de datos."); return; } S.perfiles = null;
    const o = popup(sup ? "Modelando" : "Analizando", `${S.ds.rows.length} observaciones · ${M.metodoNombre || M.metodo}`), hechas = new Map(); let act = E[0][0], t0 = Date.now(), ult = null; const tIni = Date.now(); popupEtapas(o, E, act, hechas, "");
    const cfg = { enfoque: M.enfoque, y: M.y || null, x: M.x, metodo: M.metodo, prep: M.prep, semilla: 42, prop_prueba: .2, folds: 5, k: M.k || null };
    try { const res = await window.MLMotor.correr(cfg, S.ds, ev => { if (!ev) return; const i = E.findIndex(x => x[0] === ev.id); if (i < 0) return; if (ult && ult !== ev.id) hechas.set(ult, Date.now() - t0); if (ult !== ev.id) { t0 = Date.now(); } ult = ev.id; act = ev.id; E.forEach(([id], j) => { if (j < i) hechas.set(id, hechas.get(id) != null ? hechas.get(id) : 0); }); popupAv(o, (i + (ev.progreso || 0)) / E.length); popupEtapas(o, E, act, hechas, ev.detalle || ""); });
      popupAv(o, 1); const resta = 1400 - (Date.now() - tIni); if (resta > 0) await new Promise(r => setTimeout(r, resta)); S.res = res; S.M.prepOk = true; S.M.hecho = new Date().toISOString(); await guardaModelo();
      try { await api(R + "/proyectos/" + S.id + "/resultados", { res, kpi: (res.kpis || []).slice(0, 2).map(k => ({ etiqueta: k.etiqueta, valor: fmtK(k) })) }); const p = S.proy.find(x => x.id === S.id); if (p) { p.estado = "modelado"; p.kpi = (res.kpis || []).slice(0, 2).map(k => ({ etiqueta: k.etiqueta, valor: fmtK(k) })); p.metodo = res.nombreMetodo || p.metodo; } } catch (e) { toast("El resultado se calculó pero no se pudo guardar: " + e.message, 6000); }
      popupCierra(o); nav(); ir("resultados"); }
    catch (e) { o.querySelector(".mo-pbar").classList.add("err"); o.querySelector(".mo-pd").innerHTML = `<b class="mo-mal">No se pudo completar:</b> ${esc(e.message)}`; o.querySelector("footer").innerHTML = '<button type="button" class="mo-ghost" data-fl="cierrapop">Cerrar</button>'; }
  }

  /* ═══════════════ gráficas SVG compactas ═══════════════ */
  const PAL = ["#4f46e5", "#06b6d4", "#f59e0b", "#10b981", "#ef4444", "#8b5cf6", "#ec4899", "#64748b", "#84cc16", "#f97316"];
  const nz = v => v != null && isFinite(v);
  function ejes(x0, x1, y0, y1, o) { const W = o.w, H = o.h, m = o.m, sx = v => m.l + (v - x0) / ((x1 - x0) || 1) * (W - m.l - m.r), sy = v => H - m.b - (v - y0) / ((y1 - y0) || 1) * (H - m.t - m.b); const tk = (a, b, n) => Array.from({ length: n + 1 }, (_, i) => a + (b - a) * i / n);
    return { sx, sy, h: tk(x0, x1, 4).map(v => `<line x1="${sx(v)}" y1="${m.t}" x2="${sx(v)}" y2="${H - m.b}" stroke="#eef2f7"/><text x="${sx(v)}" y="${H - m.b + 14}" text-anchor="middle">${gN(v)}</text>`).join("") + tk(y0, y1, 4).map(v => `<line x1="${m.l}" y1="${sy(v)}" x2="${W - m.r}" y2="${sy(v)}" stroke="#eef2f7"/><text x="${m.l - 6}" y="${sy(v) + 3}" text-anchor="end">${gN(v)}</text>`).join("") + (o.xl ? `<text x="${(W + m.l) / 2}" y="${H - 4}" text-anchor="middle" class="ax">${esc(o.xl)}</text>` : "") + (o.yl ? `<text transform="translate(12 ${(H - m.b + m.t) / 2}) rotate(-90)" text-anchor="middle" class="ax">${esc(o.yl)}</text>` : "") }; }
  const base = o => Object.assign({ w: 520, h: 300, m: { l: 52, r: 14, t: 12, b: 38 } }, o || {});
  function svgBarras(items, o) {
    o = o || {}; const n = items.length, rh = 22, W = 520, lw = 170, H = n * rh + 8, mx = Math.max(1e-9, ...items.map(i => Math.abs(i.v))), neg = items.some(i => i.v < 0), cx = neg ? lw + (W - lw - 50) / 2 : lw, sc = (neg ? (W - lw - 50) / 2 : (W - lw - 50)) / mx;
    return `<svg viewBox="0 0 ${W} ${H}" class="mo-svg" role="img">${items.map((it, i) => { const y = i * rh + 4, w = Math.abs(it.v) * sc, x = it.v < 0 ? cx - w : cx; return `<text x="${lw - 8}" y="${y + 14}" text-anchor="end" class="lb">${esc(String(it.n).length > 28 ? String(it.n).slice(0, 27) + "…" : it.n)}</text><rect x="${x}" y="${y + 2}" width="${Math.max(1, w)}" height="${rh - 6}" rx="3" fill="${it.c || (it.v < 0 ? "#ef4444" : "#4f46e5")}"/><text x="${it.v < 0 ? x - 4 : x + w + 4}" y="${y + 14}" text-anchor="${it.v < 0 ? "end" : "start"}" class="vl">${esc(it.t != null ? it.t : gN(it.v))}</text>`; }).join("")}</svg>`;
  }
  function svgDisp(xs, ys, o) {
    o = base(o); const P = []; for (let i = 0; i < xs.length; i++) if (nz(xs[i]) && nz(ys[i])) P.push(i); const st = Math.max(1, Math.ceil(P.length / 1500)), Q = P.filter((_, i) => i % st === 0); if (!Q.length) return '<div class="mo-mut">Sin datos para graficar.</div>';
    let x0 = Math.min(...Q.map(i => xs[i])), x1 = Math.max(...Q.map(i => xs[i])), y0 = Math.min(...Q.map(i => ys[i])), y1 = Math.max(...Q.map(i => ys[i])); if (o.diag) { x0 = y0 = Math.min(x0, y0); x1 = y1 = Math.max(x1, y1); } if (o.y0 != null) y0 = o.y0; if (o.y1 != null) y1 = o.y1; const E = ejes(x0, x1, y0, y1, o), cr = o.col || [];
    return `<svg viewBox="0 0 ${o.w} ${o.h}" class="mo-svg" role="img"><g class="ejes">${E.h}</g>${o.diag ? `<line x1="${E.sx(x0)}" y1="${E.sy(y0)}" x2="${E.sx(x1)}" y2="${E.sy(y1)}" stroke="#94a3b8" stroke-dasharray="4 3"/>` : ""}${o.hz != null ? `<line x1="${o.m.l}" y1="${E.sy(o.hz)}" x2="${o.w - o.m.r}" y2="${E.sy(o.hz)}" stroke="#94a3b8" stroke-dasharray="4 3"/>` : ""}${Q.map(i => `<circle cx="${E.sx(xs[i]).toFixed(1)}" cy="${E.sy(ys[i]).toFixed(1)}" r="3" fill="${cr[i] != null ? PAL[cr[i] % PAL.length] : "#4f46e5"}" fill-opacity=".6"/>`).join("")}</svg>`;
  }
  function svgLineas(series, o) {
    o = base(o); const all = series.flatMap(s => s.x.map((x, i) => [x, s.y[i]]).filter(p => nz(p[0]) && nz(p[1]))); if (!all.length) return '<div class="mo-mut">Sin datos para graficar.</div>'; const x0 = o.x0 != null ? o.x0 : Math.min(...all.map(p => p[0])), x1 = o.x1 != null ? o.x1 : Math.max(...all.map(p => p[0])), y0 = o.y0 != null ? o.y0 : Math.min(...all.map(p => p[1])), y1 = o.y1 != null ? o.y1 : Math.max(...all.map(p => p[1])), E = ejes(x0, x1, y0, y1, o);
    return `<svg viewBox="0 0 ${o.w} ${o.h}" class="mo-svg" role="img"><g class="ejes">${E.h}</g>${o.diag ? `<line x1="${E.sx(x0)}" y1="${E.sy(y0)}" x2="${E.sx(x1)}" y2="${E.sy(y1)}" stroke="#94a3b8" stroke-dasharray="4 3"/>` : ""}${series.map((s, k) => { const c = s.c || PAL[k % PAL.length], pts = s.x.map((x, i) => nz(x) && nz(s.y[i]) ? E.sx(x).toFixed(1) + "," + E.sy(s.y[i]).toFixed(1) : null).filter(Boolean); return `<polyline points="${pts.join(" ")}" fill="none" stroke="${c}" stroke-width="2.2"/>${s.pts === false ? "" : pts.map(p => `<circle cx="${p.split(",")[0]}" cy="${p.split(",")[1]}" r="3" fill="${c}"/>`).join("")}`; }).join("")}${series.length > 1 ? `<g class="lg">${series.map((s, k) => `<rect x="${o.m.l + 8 + k * 120}" y="${o.m.t + 2}" width="10" height="10" fill="${s.c || PAL[k % PAL.length]}"/><text x="${o.m.l + 22 + k * 120}" y="${o.m.t + 11}">${esc(s.n)}</text>`).join("")}</g>` : ""}</svg>`;
  }
  function svgHist(vals, o) {
    o = base(o); const v = vals.filter(nz); if (v.length < 2) return '<div class="mo-mut">Sin datos para graficar.</div>'; const nb = Math.min(24, Math.max(6, Math.round(Math.sqrt(v.length)))), a = Math.min(...v), b = Math.max(...v), w = (b - a) / nb || 1, c = new Array(nb).fill(0); v.forEach(x => { c[Math.min(nb - 1, Math.floor((x - a) / w))]++; });
    const my = Math.max(...c), E = ejes(a, b, 0, my, o); return `<svg viewBox="0 0 ${o.w} ${o.h}" class="mo-svg" role="img"><g class="ejes">${E.h}</g>${c.map((k, i) => `<rect x="${E.sx(a + i * w) + 1}" y="${E.sy(k)}" width="${Math.max(1, E.sx(a + (i + 1) * w) - E.sx(a + i * w) - 2)}" height="${E.sy(0) - E.sy(k)}" fill="${o.c || "#06b6d4"}" fill-opacity=".8"/>`).join("")}${o.ref != null ? `<line x1="${E.sx(o.ref)}" y1="${o.m.t}" x2="${E.sx(o.ref)}" y2="${o.h - o.m.b}" stroke="#ef4444" stroke-dasharray="4 3"/>` : ""}</svg>`;
  }
  function svgCalor(m, filas, cols, o) {
    o = o || {}; const nr = m.length, nc = (m[0] || []).length, cw = Math.max(34, Math.min(78, Math.floor(440 / Math.max(1, nc)))), rh = 26, lw = 150, th = o.rot ? 80 : 28, W = lw + nc * cw + 10, H = th + nr * rh + 6, cm = o.max != null ? o.max : Math.max(1e-9, ...m.flat().filter(nz).map(Math.abs)), neg = o.div !== false && m.flat().some(v => v < 0);
    const col = v => { if (!nz(v)) return "#f1f5f9"; const t = Math.min(1, Math.abs(v) / cm); if (neg) return v >= 0 ? `rgba(79,70,229,${(.12 + .78 * t).toFixed(2)})` : `rgba(239,68,68,${(.12 + .78 * t).toFixed(2)})`; return `rgba(79,70,229,${(.08 + .85 * t).toFixed(2)})`; };
    return `<svg viewBox="0 0 ${W} ${H}" class="mo-svg" role="img">${cols.map((c, j) => `<text x="${lw + j * cw + cw / 2}" y="${th - 6}" text-anchor="${o.rot ? "start" : "middle"}" class="lb" ${o.rot ? `transform="rotate(-40 ${lw + j * cw + cw / 2} ${th - 6})"` : ""}>${esc(String(c).length > (o.rot ? 18 : 9) ? String(c).slice(0, o.rot ? 17 : 8) + "…" : c)}</text>`).join("")}${m.map((r, i) => `<text x="${lw - 8}" y="${th + i * rh + 17}" text-anchor="end" class="lb">${esc(String(filas[i]).length > 24 ? String(filas[i]).slice(0, 23) + "…" : filas[i])}</text>${r.map((v, j) => `<rect x="${lw + j * cw}" y="${th + i * rh}" width="${cw - 2}" height="${rh - 2}" rx="3" fill="${col(v)}"/><text x="${lw + j * cw + cw / 2 - 1}" y="${th + i * rh + 16}" text-anchor="middle" class="ce" fill="${nz(v) && Math.abs(v) / cm > .55 ? "#fff" : "#0f172a"}">${nz(v) ? (o.entero ? gN(v, 0) : gN(v, 2)) : ""}</text>`).join("")}`).join("")}</svg>`;
  }
  const bloque = (t, svg, txt, extra) => `<article class="mo-gr"><h4>${t}</h4>${svg}${txt ? `<div class="mo-int"><b>Cómo leerla</b><p>${esc(txt)}</p></div>` : ""}${extra || ""}</article>`;
  const inter = (res, id, def) => { const l = ((res.interpretaciones || {}).graficas || []).find(x => x.id === id); return l ? l.texto : def || ""; };
  const interM = (res, id, def) => { const l = ((res.interpretaciones || {}).metricas || []).find(x => x.id === id); return l ? l.texto : def || ""; };
  const TONO = { bueno: "#059669", medio: "#d97706", malo: "#dc2626", neutro: "#4f46e5" };
  const kpisHtml = res => `<div class="mo-kpis mo-kp2">${(res.kpis || []).map(k => `<div class="mo-kpi" style="--c:${TONO[k.tono] || TONO.neutro}"><b>${esc(fmtK(k))}</b><span>${esc(k.etiqueta)}</span>${k.nota ? `<small>${esc(k.nota)}</small>` : ""}</div>`).join("")}</div>`;
  const hallHtml = res => (res.hallazgos || []).length ? `<section class="mo-sx2"><h3>Hallazgos principales</h3><ul class="mo-hz">${res.hallazgos.map(h => `<li>${esc(h)}</li>`).join("")}</ul></section>` : "";
  function descarga(nombre, filas) { const csv = filas.map(r => r.map(v => { const t = v == null ? "" : String(v); return /[",\n;]/.test(t) ? '"' + t.replace(/"/g, '""') + '"' : t; }).join(",")).join("\r\n"), a = document.createElement("a"); a.href = URL.createObjectURL(new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8" })); a.download = nombre; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 2000); }
  function mapaPuntos(id, colores, rotulos) {
    leaflet(() => { const el = $("#" + id); if (!el || !window.L || !S.ds) return; if (S._mapRes) { try { S._mapRes.remove(); } catch (_) { } } const m = L.map(el).setView([S.ds.lat[0] || 23.8, S.ds.lng[0] || -102.3], 10); L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 18, maxNativeZoom: 16, attribution: "Esri" }).addTo(m); S._mapRes = m; const g = []; const st = Math.max(1, Math.ceil(S.ds.lat.length / 4000));
      S.ds.lat.forEach((la, i) => { if (i % st || colores[i] == null) return; const mk = L.circleMarker([la, S.ds.lng[i]], { radius: 5, color: "#fff", weight: 1, fillColor: colores[i], fillOpacity: .85 }).bindTooltip(esc(rotulos[i] || "")); mk.addTo(m); g.push([la, S.ds.lng[i]]); }); if (g.length) m.fitBounds(g, { padding: [20, 20], maxZoom: 15 }); }); }

  /* ── 6 · Resultados ── */
  const MNOM = { r2: "R²", r2_ajustado: "R² ajustado", rmse: "RMSE (error típico)", mae: "MAE (error absoluto medio)", mape: "MAPE (%)", sesgo: "Sesgo medio", ratio_rmse_sd: "RMSE / desviación de la respuesta", exactitud: "Exactitud", exactitud_balanceada: "Exactitud balanceada", precision_macro: "Precisión (promedio)", recall_macro: "Sensibilidad (promedio)", f1_macro: "F1 (promedio)", precision: "Precisión", recall: "Sensibilidad", f1: "F1", auc: "AUC", log_loss: "Pérdida logarítmica", kappa: "Kappa" };
  const MACOT = new Set(["r2", "r2_ajustado", "exactitud", "exactitud_balanceada", "precision_macro", "recall_macro", "f1_macro", "precision", "recall", "f1", "auc", "kappa"]);
  const mNom = k => MNOM[k] || String(k).replace(/_/g, " ");
  const nombreV = v => nomVar(String(v).replace(/^(num__|cat__)/, ""));
  function secResultados() {
    S.sec = "resultados"; nav(); const r = S.res; if (!r) { wrap(`${cab("6", "Resultados", "")}<div class="mo-empty"><b>Aún no hay resultados.</b><p>Configura el análisis y pulsa «${S.M && S.M.enfoque === "nosup" ? "Analizar" : "Modelar"}».</p><button type="button" class="mo-new" data-sec="${S.M && S.M.metodo ? "modelar" : "variables"}">Ir al flujo</button></div>`); return; }
    const sup = r.enfoque === "sup"; let h = `${cab("6", "Resultados", `${esc(r.nombreMetodo || r.metodo)} · ${sup ? (r.tipoY === "regresion" ? "regresión" : r.tipoY === "binaria" ? "clasificación binaria" : "clasificación multiclase") : "análisis no supervisado"}`)}${kpisHtml(r)}${hallHtml(r)}<div class="mo-gg">`;
    if (sup) {
      const imp = (r.importancias || []).slice(0, 15); if (imp.length) h += bloque("Variables que más influyen", svgBarras(imp.map(i => ({ n: nombreV(i.var), v: i.valor, t: gN(i.valor, 0) }))), inter(r, "importancias", "Mide cuánto empeora el modelo cuando se desordena cada variable: más largo, más influyente. La escala va de 0 a 100."));
      const P = r.predicciones || {};
      if (r.tipoY === "regresion" && P.real) { const res2 = P.real.map((v, i) => v - P.pred[i]); h += bloque("Valores reales frente a predichos (datos de prueba)", svgDisp(P.real, P.pred, { diag: true, xl: "Real", yl: "Predicho" }), inter(r, "real_vs_pred", "Cada punto es una observación que el modelo no vio al entrenar. Cuanto más cerca de la línea punteada, mejor la predicción.")); h += bloque("Errores del modelo (residuos)", svgHist(res2, { xl: "Real − predicho", ref: 0 }), inter(r, "residuos", "Lo ideal es una campana centrada en cero: errores pequeños y sin sesgo hacia arriba o abajo.")); }
      if (r.tipoY !== "regresion" && r.matrizConfusion) { const C = r.matrizConfusion; h += bloque("Matriz de confusión (datos de prueba)", svgCalor(C.m, C.clases.map(c => "real: " + c), C.clases.map(c => "pred: " + c), { div: false, entero: true, rot: true }), inter(r, "confusion", "Las filas son lo que realmente fue y las columnas lo que predijo el modelo. La diagonal son los aciertos.")); }
      if (r.roc && r.roc.fpr) h += bloque(`Curva ROC · AUC ${gN(r.roc.auc, 3)}`, svgLineas([{ n: "Modelo", x: r.roc.fpr, y: r.roc.tpr, pts: false }], { diag: true, x0: 0, x1: 1, y0: 0, y1: 1, xl: "Falsos positivos", yl: "Verdaderos positivos" }), inter(r, "roc", "Cuanto más se pega la curva a la esquina superior izquierda, mejor separa las clases. La diagonal es el azar (AUC 0.5)."));
      if ((r.coeficientes || []).length) h += `<article class="mo-gr mo-ancho"><h4>${r.tipoY === "regresion" ? "Coeficientes del modelo" : "Coeficientes y razones de momios"}</h4><div class="mo-tw"><table class="mo-t"><thead><tr><th>Variable</th><th>Coeficiente</th><th>Error</th><th>t</th><th>p</th><th>IC 95 %</th>${r.tipoY !== "regresion" ? "<th>Razón de momios</th>" : ""}</tr></thead><tbody>${r.coeficientes.map(c => `<tr><td>${esc(nombreV(c.var))}</td><td>${gN(c.coef, 4)}</td><td>${gN(c.error, 4)}</td><td>${gN(c.t, 2)}</td><td class="${c.p != null && c.p < .05 ? "mo-sig" : ""}">${c.p == null ? "—" : c.p < .001 ? "< 0.001" : gN(c.p, 3)}</td><td>${c.ic95 ? gN(c.ic95[0], 3) + " a " + gN(c.ic95[1], 3) : "—"}</td>${r.tipoY !== "regresion" ? `<td>${gN(c.oddsRatio, 3)}</td>` : ""}</tr>`).join("")}</tbody></table></div><div class="mo-int"><b>Cómo leerla</b><p>${esc(inter(r, "coeficientes", "El signo indica si la variable sube o baja la respuesta; una p menor a 0.05 sugiere que el efecto es distinguible del azar."))}</p></div></article>`;
      if (P.idx) h += `<article class="mo-gr mo-ancho"><button type="button" class="mo-ghost" data-fl="csv">Descargar predicciones (CSV)</button></article>`;
    } else {
      if (r.clusters) { const C = r.clusters, k = C.k; h += `<article class="mo-gr mo-ancho"><h4>${k} grupos encontrados</h4><div class="mo-cg">${(C.perfil || []).map((p, i) => `<div class="mo-cc" style="--c:${PAL[i % PAL.length]}"><b>${(C.etiquetaSugerida || [])[i] ? esc(C.etiquetaSugerida[i]) : "Grupo " + (i + 1)}</b><span>${gN(p.n, 0)} observaciones (${gN(p.n / Math.max(1, (C.asignacion || []).filter(a => a >= 0).length) * 100, 0)} %)</span><ul>${(p.variablesDistintivas || []).slice(0, 3).map(v => `<li>${esc(nombreV(v.var))}: ${v.z > 0 ? "alto" : "bajo"} (${v.z > 0 ? "+" : ""}${gN(v.z, 1)} σ)</li>`).join("")}</ul></div>`).join("")}</div></article>`;
        const vs = [...new Set((C.perfil || []).flatMap(p => (p.variablesDistintivas || []).map(v => v.var)))].slice(0, 12); if (vs.length) h += bloque("Perfil de cada grupo (diferencia respecto al promedio)", svgCalor((C.perfil || []).map(p => vs.map(v => { const d = (p.variablesDistintivas || []).find(x => x.var === v); return d ? d.z : null; })), (C.perfil || []).map((_, i) => "Grupo " + (i + 1)), vs.map(nombreV), { rot: true }), inter(r, "perfil_clusters", "Azul: el grupo está por encima del promedio en esa variable; rojo: por debajo. Sirve para ponerle nombre a cada grupo."));
        if ((C.elbow || []).length) h += bloque("¿Cuántos grupos? (silueta por número de grupos)", svgLineas([{ n: "Silueta", x: C.elbow.map(e => e.k), y: C.elbow.map(e => e.silueta), c: "#10b981" }], { xl: "Número de grupos", yl: "Silueta" }), inter(r, "elbow", "Un valor de silueta alto indica grupos compactos y bien separados. Se elige el número de grupos donde es más alta.")); h += `<article class="mo-gr mo-ancho"><h4>Mapa de los puntos por grupo</h4><div id="moMapRes" class="mo-map"></div><div class="mo-chs"><button type="button" class="mo-ghost" data-fl="csv">Descargar asignación de grupos (CSV)</button></div></article>`; }
      if (r.pca) { const P2 = r.pca, cum = P2.acumulada || []; h += bloque("Varianza explicada por componente", svgLineas([{ n: "Por componente", x: P2.varianza.map((_, i) => i + 1), y: P2.varianza.map(v => v * (v <= 1 ? 100 : 1)), c: "#4f46e5" }, { n: "Acumulada", x: cum.map((_, i) => i + 1), y: cum.map(v => v * (v <= 1 ? 100 : 1)), c: "#f59e0b" }], { xl: "Componente", yl: "% de la varianza", y0: 0 }), inter(r, "pca_var", `Los primeros ${P2.nComponentes80 || "pocos"} componentes recogen ~80 % de la información: se pueden resumir las variables con ellos.`));
        if (P2.scores && P2.scores.length) { const asg = r.clusters ? r.clusters.asignacion : null; h += bloque("Observaciones en los dos primeros componentes", svgDisp(P2.scores.map(s => s[0]), P2.scores.map(s => s[1]), { xl: "Componente 1", yl: "Componente 2", col: asg || [] }), inter(r, "pca_scores", "Cada punto es una observación; los que quedan juntos se parecen. Con grupos, el color indica a cuál pertenece.")); }
        if ((P2.cargas || []).length) { const top = P2.cargas.slice().sort((a, b) => Math.abs(b.pc[0] || 0) + Math.abs(b.pc[1] || 0) - Math.abs(a.pc[0] || 0) - Math.abs(a.pc[1] || 0)).slice(0, 14); h += bloque("Qué variables componen cada componente", svgCalor(top.map(c => c.pc.slice(0, 4)), top.map(c => nombreV(c.var)), ["PC1", "PC2", "PC3", "PC4"].slice(0, (top[0].pc || []).slice(0, 4).length)), inter(r, "pca_cargas", "Cuanto más intenso el color, más pesa la variable en ese componente; el signo indica la dirección.")); } }
      if (r.correlaciones) { const C = r.correlaciones, vs = C.vars.slice(0, 18); h += bloque("Correlaciones entre variables (Pearson)", svgCalor(C.pearson.slice(0, 18).map(f => f.slice(0, 18)), vs.map(nombreV), vs.map(nombreV), { rot: true, max: 1 }), inter(r, "correlaciones", "Azul: cuando una sube la otra también; rojo: cuando una sube la otra baja. Cerca de 0: no hay relación lineal.")); h += `<article class="mo-gr"><h4>Relaciones más fuertes</h4><table class="mo-t"><thead><tr><th>Variable A</th><th>Variable B</th><th>Pearson</th><th>Spearman</th></tr></thead><tbody>${(C.paresFuertes || []).slice(0, 10).map(p => `<tr><td>${esc(nombreV(p.a))}</td><td>${esc(nombreV(p.b))}</td><td>${gN(p.r, 2)}</td><td>${gN(p.rho, 2)}</td></tr>`).join("")}</tbody></table></article>`; }
      if (r.anomalias) { const A = r.anomalias; h += bloque("Casos atípicos (puntaje de anomalía)", svgHist(A.puntaje.filter(nz), { xl: "Puntaje", ref: A.umbral }), inter(r, "anomalias", "A la derecha de la línea roja están las observaciones que se alejan mucho del patrón del resto: conviene revisarlas.")); h += `<article class="mo-gr"><h4>Observaciones más atípicas</h4><table class="mo-t"><thead><tr><th>Observación</th><th>Puntaje</th><th>Variables que más aportan</th></tr></thead><tbody>${(A.idx || []).slice(0, 10).map((i, k) => `<tr><td>${esc(S.ds && S.ds.ids ? S.ds.ids[i] : "#" + (i + 1))}</td><td>${gN(A.puntaje[i], 2)}</td><td>${esc((((A.variablesQueMasAportan || []).find(v => v.idx === i) || {}).vars || []).map(nombreV).join(", "))}</td></tr>`).join("")}</tbody></table></article>`; }
    }
    h += "</div>" + pie("modelar", "calidad", "Ver calidad del modelo", false); wrap(h);
    if (!sup && r.clusters && S.ds) { const a = r.clusters.asignacion || []; mapaPuntos("moMapRes", a.map(x => x >= 0 ? PAL[x % PAL.length] : "#94a3b8"), a.map((x, i) => (S.ds.ids[i] || "") + " · grupo " + (x + 1))); }
  }

  /* ── 7 · Calidad del modelo ── */
  const gauge = (k, v) => !nz(v) ? "—" : MACOT.has(k) ? `<div class="mo-ga"><i style="width:${Math.max(0, Math.min(100, v * 100))}%;background:${v >= .8 ? "#059669" : v >= .6 ? "#d97706" : "#dc2626"}"></i><b>${gN(v, 3)}</b></div>` : gN(v, 3);
  function secCalidad() {
    S.sec = "calidad"; nav(); const r = S.res; if (!r) return ir("resultados"); const sup = r.enfoque === "sup"; let h = cab("7", "Calidad del modelo", "Estadísticos y diagnósticos para saber cuánto confiar en los resultados.");
    if (sup) {
      const M2 = r.metricas || {}, tr = M2.entrenamiento || {}, te = M2.prueba || {}, cv = M2.cv || {}, keys = [...new Set([...Object.keys(te), ...Object.keys(tr)])].filter(k => nz(te[k]) || nz(tr[k]));
      h += `<section class="mo-sx2"><h3>Medidas de ajuste</h3><div class="mo-tw"><table class="mo-t mo-mq"><thead><tr><th>Medida</th><th>Entrenamiento</th><th>Prueba</th><th>Validación cruzada</th><th>Qué significa</th></tr></thead><tbody>${keys.map(k => `<tr><td><b>${esc(mNom(k))}</b></td><td>${gauge(k, tr[k])}</td><td>${gauge(k, te[k])}</td><td>${cv.media && nz(cv.media[k]) ? gN(cv.media[k], 3) + (cv.sd && nz(cv.sd[k]) ? ` <small>± ${gN(cv.sd[k], 3)}</small>` : "") : "—"}</td><td class="mo-q2">${esc(interM(r, k, ""))}</td></tr>`).join("")}</tbody></table></div><div class="mo-nota">«Entrenamiento» mide qué tan bien se ajusta a los datos con que aprendió; «Prueba», con datos que no vio (la medida honesta); «Validación cruzada» repite la prueba en ${(cv.folds || []).length || 5} partes distintas para ver qué tan estable es.</div></section>`;
      const g = r.tipoY === "regresion" ? "r2" : "exactitud", gap = nz(tr[g]) && nz(te[g]) ? tr[g] - te[g] : null; if (gap != null) h += `<section class="mo-sx2"><h3>¿Hay sobreajuste?</h3><div class="mo-ga mo-gb"><i style="width:${Math.max(0, Math.min(100, Math.abs(gap) * 100 * 2))}%;background:${Math.abs(gap) < .08 ? "#059669" : Math.abs(gap) < .2 ? "#d97706" : "#dc2626"}"></i><b>Diferencia entrenamiento − prueba: ${gN(gap, 3)}</b></div><p class="mo-mut">${Math.abs(gap) < .08 ? "El modelo rinde parecido con datos nuevos: generaliza bien." : Math.abs(gap) < .2 ? "Hay algo de sobreajuste: el modelo se aprendió un poco de memoria los datos de entrenamiento." : "Sobreajuste marcado: el modelo memoriza y falla con datos nuevos. Reduce variables o usa un método más simple."}</p></section>`;
      h += '<div class="mo-gg">'; const ca = r.curvaAprendizaje; if (ca && ca.n) h += bloque("Curva de aprendizaje", svgLineas([{ n: "Entrenamiento", x: ca.n, y: ca.entrenamiento, c: "#4f46e5" }, { n: "Validación", x: ca.n, y: ca.validacion, c: "#f59e0b" }], { xl: "Observaciones usadas", yl: r.tipoY === "regresion" ? "R²" : "Exactitud" }), inter(r, "curva_aprendizaje", "Si las dos curvas siguen subiendo, más datos mejorarían el modelo; si están juntas y planas, el límite es el modelo o las variables."));
      if (r.calibracion && (r.calibracion.bins || []).length) h += bloque("Calibración de las probabilidades", svgLineas([{ n: "Modelo", x: r.calibracion.bins.map(b => b.p_media), y: r.calibracion.bins.map(b => b.frec_obs), c: "#10b981" }], { diag: true, x0: 0, x1: 1, y0: 0, y1: 1, xl: "Probabilidad predicha", yl: "Frecuencia observada" }), inter(r, "calibracion", "Si los puntos siguen la diagonal, cuando el modelo dice 70 % realmente ocurre ~70 % de las veces."));
      const rs = r.residuos; if (r.tipoY === "regresion" && rs) { const P = r.predicciones || {}; h += bloque("Residuos frente a lo predicho", svgDisp(P.pred || [], (P.real || []).map((v, i) => v - P.pred[i]), { hz: 0, xl: "Predicho", yl: "Error" }), inter(r, "residuos_pred", "Los errores deben repartirse sin forma, alrededor de cero. Un abanico o curva indica que el modelo no captura algo.")); h += `<article class="mo-gr"><h4>Pruebas sobre los residuos</h4><table class="mo-t"><tbody><tr><td>Normalidad (Jarque-Bera)</td><td>${rs.normalidad ? gN(rs.normalidad.estadistico != null ? rs.normalidad.estadistico : rs.normalidad.jb, 2) + " · p = " + (rs.normalidad.p == null ? "—" : gN(rs.normalidad.p, 3)) : "—"}</td></tr><tr><td>Varianza constante (Breusch-Pagan)</td><td>${rs.heterocedasticidad ? gN(rs.heterocedasticidad.estadistico, 2) + " · p = " + (rs.heterocedasticidad.p == null ? "—" : gN(rs.heterocedasticidad.p, 3)) : "—"}</td></tr><tr><td>Independencia (Durbin-Watson)</td><td>${nz(rs.durbinWatson) ? gN(rs.durbinWatson, 2) : "—"}</td></tr></tbody></table><div class="mo-int"><b>Cómo leerla</b><p>p mayor a 0.05 en las dos primeras pruebas indica que no hay evidencia de problemas. Durbin-Watson cerca de 2 indica errores independientes.</p></div></article>`; }
      if (((r.hiperparametros || {}).rejilla || []).length) { const hp = r.hiperparametros; h += `<article class="mo-gr mo-ancho"><h4>Calibración de hiperparámetros</h4><p class="mo-mut">Elegidos: ${esc(JSON.stringify(hp.elegidos || {}).replace(/[{}"]/g, "").replace(/,/g, " · "))}</p><div class="mo-tw"><table class="mo-t"><thead><tr><th>Configuración probada</th><th>Puntaje (validación cruzada)</th></tr></thead><tbody>${hp.rejilla.slice(0, 12).map(x => `<tr><td>${esc(JSON.stringify(x.params).replace(/[{}"]/g, "").replace(/,/g, " · "))}</td><td>${gN(x.score, 4)}</td></tr>`).join("")}</tbody></table></div></article>`; }
      h += "</div>";
    } else {
      h += '<section class="mo-sx2"><h3>Medidas de calidad</h3><div class="mo-tw"><table class="mo-t mo-mq"><thead><tr><th>Medida</th><th>Valor</th><th>Qué significa</th></tr></thead><tbody>';
      if (r.clusters) { const C = r.clusters; h += `<tr><td><b>Silueta</b></td><td>${gauge("r2", C.silueta)}</td><td class="mo-q2">${esc(interM(r, "silueta", "De −1 a 1. Más de 0.5 indica grupos bien separados; menos de 0.25, grupos que se traslapan."))}</td></tr><tr><td><b>Índice Calinski-Harabasz</b></td><td>${gN(C.indiceCH, 1)}</td><td class="mo-q2">${esc(interM(r, "ch", "Mientras más alto, grupos más compactos y distantes entre sí."))}</td></tr><tr><td><b>Índice Davies-Bouldin</b></td><td>${gN(C.indiceDB, 2)}</td><td class="mo-q2">${esc(interM(r, "db", "Mientras más bajo, mejor separación (0 es lo ideal)."))}</td></tr>`; }
      if (r.pca) h += `<tr><td><b>Componentes para ~80 % de la varianza</b></td><td>${gN(r.pca.nComponentes80, 0)}</td><td class="mo-q2">${esc(interM(r, "pca80", "Cuántos componentes bastan para conservar la mayor parte de la información."))}</td></tr>`;
      if (r.anomalias) h += `<tr><td><b>Casos atípicos detectados</b></td><td>${gN((r.anomalias.idx || []).length, 0)}</td><td class="mo-q2">${esc(interM(r, "anom", "Observaciones por encima del umbral de anomalía."))}</td></tr>`;
      h += "</tbody></table></div></section>";
    }
    h += `<section class="mo-sx2"><h3>Datos y preprocesamiento</h3><ul class="mo-rsm">${((r.prep || {}).pasos || []).map(p => `<li>${esc(p)}</li>`).join("")}</ul><div class="mo-nota">${r.prep && r.prep.nTrain != null ? `Entrenamiento: ${gN(r.prep.nTrain, 0)} observaciones · Prueba: ${gN(r.prep.nTest, 0)}. ` : ""}${((r.prep || {}).descartadas || []).length ? "Variables descartadas: " + r.prep.descartadas.map(d => esc(nombreV(d.name) + " (" + d.razon + ")")).join(", ") + "." : ""}</div></section>`;
    wrap(h + pie("resultados", null, "", false));
  }

  /* ── eventos del flujo ── */
  const guardaPrep = () => { S.M.prepOk = false; };
  function flujoClic(e) {
    const t = e.target; let b;
    if ((b = t.closest("[data-v]")) && S.sec === "variables") { const k = b.dataset.v, s = new Set(S.M.vars || []); s.has(k) ? s.delete(k) : s.add(k); S.M.vars = [...s]; resetModelo(); const y = $("#moMain").scrollTop; secVariables(); $("#moMain").scrollTop = y; return true; }
    if ((b = t.closest("[data-vg]"))) { const ks = b.dataset.vg.split("|").filter(Boolean), s = new Set(S.M.vars || []); ks.forEach(k => b.dataset.on === "1" ? s.add(k) : s.delete(k)); S.M.vars = [...s]; resetModelo(); const y = $("#moMain").scrollTop; secVariables(); $("#moMain").scrollTop = y; return true; }
    if ((b = t.closest("[data-vcat]"))) { S.vcat = b.dataset.vcat; secVariables(); return true; }
    if ((b = t.closest("[data-enf]"))) { const M = S.M; if (M.enfoque !== b.dataset.enf) { M.enfoque = b.dataset.enf; M.y = null; M.metodo = null; M.prep = null; M.prepOk = false; const P = perfiles(); M.x = P.filter(p => p.tipo !== "texto" && !p.constante).map(p => p.name); guardaModelo(); } secEnfoque(); return true; }
    if ((b = t.closest("[data-x]"))) { const k = b.dataset.x, s = new Set(S.M.x || []); s.has(k) ? s.delete(k) : s.add(k); S.M.x = [...s]; S.M.metodo = null; S.M.prep = null; S.M.prepOk = false; guardaModelo(); const y = $("#moMain").scrollTop; secEnfoque().then(() => { $("#moMain").scrollTop = y; }); return true; }
    if ((b = t.closest("[data-xg]"))) { const P = perfiles().filter(p => p.tipo !== "texto" && !p.constante && !(S.M.enfoque === "sup" && p.name === S.M.y)); S.M.x = b.dataset.xg === "1" ? P.map(p => p.name) : []; S.M.metodo = null; S.M.prep = null; S.M.prepOk = false; guardaModelo(); secEnfoque(); return true; }
    if ((b = t.closest("[data-met]"))) { if (b.disabled) return true; S.M.metodo = b.dataset.met; S.M.prep = null; S.M.prepOk = false; guardaModelo(); const y = $("#moMain").scrollTop; secMetodos().then(() => { $("#moMain").scrollTop = y; }); return true; }
    if ((b = t.closest("[data-fl]"))) { const a = b.dataset.fl;
      if (a === "construir") { construyeDatos(); return true; } if (a === "metodos") { ir("metodos"); return true; } if (a === "prep") { ir("prep"); return true; } if (a === "modelar") { S.M.prepOk = true; guardaModelo(); ir("modelar"); return true; }
      if (a === "correr") { corre(); return true; } if (a === "calidad") { ir("calidad"); return true; } if (a === "cierrapop") { $$(".mo-pop").forEach(popupCierra); return true; }
      if (a === "csv") { const r = S.res, ds = S.ds; if (!r || !ds) return true; if (r.clusters) { descarga("grupos.csv", [["id", "latitud", "longitud", "grupo"]].concat((r.clusters.asignacion || []).map((g, i) => [ds.ids[i], ds.lat[i], ds.lng[i], g >= 0 ? g + 1 : ""]))); } else if (r.predicciones) { const P = r.predicciones; descarga("predicciones.csv", [["id", "latitud", "longitud", "real", "predicho"]].concat((P.idx || []).map((i, k) => [ds.ids[i], ds.lat[i], ds.lng[i], P.real[k], P.pred[k]]))); } return true; } }
    return false;
  }
  function resetModelo() { const M = S.M; M.metodo = null; M.prep = null; M.prepOk = false; M.y = null; M.x = []; M.enfoque = M.enfoque; S.ds = null; S.perfiles = null; const r = $("#vRes"); if (r) r.innerHTML = resumenVars(); }
  function flujoEntrada(e) { const t = e.target; if (t.id === "moVQ") { S.vq = t.value; S._vfoco = true; secVariables(); } }
  function flujoCambia(e) {
    const t = e.target; if (t.id === "moY") { S.M.y = t.value || null; S.M.x = (S.M.x || []).filter(k => k !== S.M.y); if (S.M.y && !(S.M.x || []).length) { S.M.x = perfiles().filter(p => p.tipo !== "texto" && !p.constante && p.name !== S.M.y).map(p => p.name); } S.M.metodo = null; S.M.prep = null; S.M.prepOk = false; guardaModelo(); secEnfoque(); return; }
    if (t.dataset && t.dataset.prep) { S.M.prep.global[t.dataset.prep] = t.value; guardaModelo(); secPrep(); return; } if (t.id === "moK") { S.M.k = t.value ? Math.max(2, Math.min(20, +t.value)) : null; guardaModelo(); }
  }

  /* ───────── eventos ───────── */
  function clic(e) {
    const t = e.target; if (S.wiz && S.sec === "nuevo" && wizClic(e)) { e.preventDefault(); return; }
    if (!t.closest(".mo-bus")) $$(".mo-bl").forEach(l => { l.style.display = "none"; });
    let b;
    if ((b = t.closest("[data-sec]"))) { e.preventDefault(); ir(b.dataset.sec); return; }
    if (t.closest("#moNuevo")) { S.wiz = wizVacio(); wizard(); return; }
    if ((b = t.closest("[data-pf]"))) { S.pf = b.dataset.pf; vistaProyectos(); return; }
    if ((b = t.closest("[data-pv]"))) { S.pv = b.dataset.pv; vistaProyectos(); return; }
    if ((b = t.closest("[data-abrir]"))) { e.preventDefault(); abre(b.dataset.abrir); return; }
    if ((b = t.closest("[data-ed]"))) { editar(b.dataset.ed); return; }
    if ((b = t.closest("[data-del]"))) { elimina(b.dataset.del); return; }
    if (typeof flujoClic === "function" && flujoClic(e)) return;
  }
  function entrada(e) {
    const t = e.target; if (S.wiz && S.sec === "nuevo" && wizEntrada(e)) return;
    if (t.id === "moPQ") { S.pq = t.value; S._foco = true; vistaProyectos(); return; }
    if (typeof flujoEntrada === "function") flujoEntrada(e);
  }
  function cambia(e) { if (e.target.id === "moPSel") { const v = e.target.value; if (v) abre(v); else { S.id = S.P = S.res = null; S.M = null; ir("proyectos"); } return; } if (S.wiz && S.sec === "nuevo" && wizCambia(e)) return; if (typeof flujoCambia === "function") flujoCambia(e); }
  /* arranque */
  window.modelabInit = async function () {
    host = document.getElementById("modelab-host"); if (!host) return; shell(); nav(); vistaProyectos(); await cargaProy(); if (S.sec === "proyectos") vistaProyectos();
  };
})();
