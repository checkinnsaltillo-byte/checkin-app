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
  const S = { sec: "proyectos", proy: [], cargado: false, pv: "card", pf: "", pq: "", id: null, P: null, puntos: [], M: null, ds: null, res: null, bib: window.MO_BIBLIO || [], wiz: null, busy: false, AN: [], aid: null, A: null, af: "", aq: "", av: "card" };
  const SEC = [["datos", "0", "Datos", "Tabla cargada o coordenadas del proyecto"], ["variables", "1", "Variables", "Elige las variables del modelo"], ["enfoque", "2", "Enfoque", "Supervisado o no supervisado"], ["metodos", "3", "Métodos", "Métodos analíticos factibles"], ["prep", "4", "Preprocesamiento", "Limpieza y preparación"], ["modelar", "5", "Modelar", "Entrenar, calibrar y evaluar"], ["resultados", "6", "Resultados y calidad", "Hallazgos, gráficas y calidad del modelo"], ["simulacion", "7", "Simulación", "Evalúa valores y puntos nuevos con el modelo"]];

  /* ───────── servidor ───────── */
  const token = () => { try { const g = JSON.parse(localStorage.getItem("gmTok") || "null"); return g && g.token ? g.token : ""; } catch (_) { return ""; } };
  async function api(ruta, cuerpo, metodo) {
    if (S.aid && /\/proyectos\/[a-z0-9]+\/(modelo|datos|resultados|sim)/.test(ruta)) ruta += (ruta.includes("?") ? "&" : "?") + "a=" + encodeURIComponent(S.aid);
    const o = { method: metodo || (cuerpo ? "POST" : "GET"), headers: { "x-gm-token": token() } }; if (cuerpo) { o.headers["Content-Type"] = "application/json"; o.body = JSON.stringify(cuerpo); }
    const r = await fetch(API + ruta, o); let j = null; try { j = await r.json(); } catch (_) { }
    if (r.status === 401) throw new Error("Tu sesión expiró. Vuelve a entrar con tu contraseña."); if (!r.ok || !j || j.ok === false) throw new Error((j && j.error) || "No se pudo completar la operación (" + r.status + ").");
    if (cuerpo && /\/proyectos\/[a-z0-9]+\/(modelo|resultados)(\?|$)/.test(ruta)) setTimeout(cargaAn, 250);
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
    host.addEventListener("click", clic); host.addEventListener("input", entrada); host.addEventListener("change", cambia); tipInit();
  }
  const hecho = k => { const M = S.M || {}; return k === "datos" ? !!(S.puntos && S.puntos.length) : k === "variables" ? !!(M.vars && M.vars.length) : k === "enfoque" ? !!(M.enfoque && (M.enfoque === "nosup" || M.y)) : k === "metodos" ? !!M.metodo : k === "prep" ? !!M.prepOk : k === "modelar" || k === "resultados" ? !!S.res : k === "simulacion" ? !!(S.sim && ((S.sim.conjuntos || []).length || (S.sim.escenarios || []).length)) : false; };
  function nav() {
    const n = $("#moNav"); if (!n) return; let h = `<label class="mo-psel"><small>Proyecto</small><select id="moPSel" class="mo-in"><option value="">Todos los proyectos</option>${S.proy.map(p => `<option value="${p.id}"${p.id === S.id ? " selected" : ""}>${esc(p.nombre)}</option>`).join("")}</select></label><button type="button" class="mo-nav${S.sec === "proyectos" || (S.sec === "nuevo" && !(S.wiz && S.wiz.sim)) ? " on" : ""}" data-sec="proyectos"><span class="ic">${mi("folder")}</span><span class="tx">Mis proyectos</span><span class="bd">${S.proy.length}</span></button>`;
    if (S.id && S.P) { h += `<div class="mo-prj"><small>Proyecto abierto</small><b>${esc(S.P.nombre)}</b><span>${gN(S.puntos.length, 0)} puntos · ${S.P.fuente === "denue" ? "unidades DENUE" : S.P.fuente === "archivo" ? "archivo" : S.P.fuente === "inmuebles" ? "bienes inmuebles" : S.P.fuente === "geolab" ? "Geo Lab" : "mapa"}</span></div>`;
      h += `<button type="button" class="mo-nav${S.sec === "analisis" ? " on" : ""}" data-sec="analisis"><span class="ic">${mi("list")}</span><span class="tx">Análisis del proyecto</span><span class="bd">${(S.AN || []).length}</span></button>`;
      if (S.aid && S.A) h += `<div class="mo-prj mo-prja"><small>Análisis abierto</small><b>${esc(S.A.nombre)}</b><span>${S.M && S.M.enfoque ? (S.M.enfoque === "val" ? "Valuación inmobiliaria" : S.M.enfoque === "sup" ? "Supervisado" : "No supervisado") : "Sin configurar"}${S.M && S.M.metodo ? " · " + esc(S.M.metodo === "auto" ? "mejor modelo" : S.M.metodo) : ""}</span></div>`;
      if (S.aid) SEC.filter(([k]) => !((k === "simulacion" && S.M && (S.M.enfoque === "nosup" || S.M.enfoque === "val")) || (k === "prep" && S.M && S.M.enfoque === "val"))).forEach(([k, n2, t, d]) => { h += `<button type="button" class="mo-nav${S.sec === k || (k === "simulacion" && S.sec === "nuevo" && S.wiz && S.wiz.sim) ? " on" : ""}" data-sec="${k}" title="${esc(d)}"><span class="ic mo-n">${n2}</span><span class="tx">${t}</span><span class="bd mo-ok">${hecho(k) ? "✓" : ""}</span></button>`; }); }
    n.innerHTML = h;
  }
  /* migas de pan: Mis proyectos › Proyecto › Análisis (cada una regresa a esa pantalla) */
  const migas = () => {
    const flujo = ["datos", "variables", "enfoque", "metodos", "prep", "modelar", "resultados", "simulacion"]; if (!S.P || !S.id) return "";
    if (S.sec === "analisis") return `<nav class="mo-bc"><button type="button" class="mo-bcb" data-sec="proyectos">${mi("back")} Mis proyectos</button><span>›</span><b>${esc(S.P.nombre)}</b></nav>`;
    if (flujo.includes(S.sec) && S.aid && S.A) return `<nav class="mo-bc"><button type="button" class="mo-bcb" data-sec="analisis">${mi("back")} Análisis del proyecto</button><span>›</span><a href="#" data-sec="proyectos">Mis proyectos</a><span>›</span><a href="#" data-sec="analisis">${esc(S.P.nombre)}</a><span>›</span><b>${esc(S.A.nombre)}</b></nav>`;
    return "";
  };
  const wrap = h => { const w = $("#moWrap"); if (w) { w.innerHTML = migas() + h; $("#moMain").scrollTop = 0; } };
  async function ir(sec) {
    if (sec === "calidad") sec = "resultados"; if (sec === "simulacion" && S.M && (S.M.enfoque === "nosup" || S.M.enfoque === "val")) sec = "resultados";
    if (sec !== "nuevo") S.wiz = (sec === "proyectos" || (S.wiz && S.wiz.sim)) ? null : S.wiz; S.sec = sec; nav();
    try { history.replaceState(null, "", "#" + sec); } catch (_) { }
    if (sec === "proyectos") { vistaProyectos(); cargaProy().then(() => { if (S.sec === "proyectos") vistaProyectos(); }); return; } if (sec === "nuevo") return wizard(); if (sec === "analisis") return S.id ? vistaAnalisis() : ir("proyectos");
    if (!S.id) return ir("proyectos"); if (!S.aid) return ir("analisis"); const f = { datos: secDatos, variables: secVariables, enfoque: secEnfoque, metodos: secMetodos, prep: secPrep, modelar: secModelar, resultados: secResultados, simulacion: secSimulacion }[sec]; if (f) f();
  }

  /* ───────── Mis proyectos (misma estructura que Media Lab: KPIs arriba y tarjetas o tabla) ───────── */
  const EST = { nuevo: ["Sin configurar", "#64748b"], configurado: ["Configurado", "#d97706"], modelado: ["Modelado", "#059669"] };
  async function cargaProy() { try { const j = await api(R + "/proyectos"); S.proy = j.proyectos || []; S.cargado = true; } catch (e) { toast(e.message); S.cargado = true; } nav(); }
  function vistaProyectos() {
    S.sec = "proyectos"; nav(); const L = S.proy, q = S.pq.trim().toLowerCase(), vis = L.filter(p => (!S.pf || p.estado === S.pf) && (!q || (p.nombre + " " + p.objetivo).toLowerCase().includes(q)));
    const cuenta = e => L.filter(p => p.estado === e).length, K = [["", "Todos los proyectos", L.length, "#4f46e5"], ["modelado", "Modelados", cuenta("modelado"), "#059669"], ["configurado", "Configurados", cuenta("configurado"), "#d97706"], ["nuevo", "Sin configurar", cuenta("nuevo"), "#64748b"]];
    const tarj = p => { const e = EST[p.estado] || EST.nuevo, k = (p.kpi || [])[0]; return `<article class="mo-pc" style="--c:${e[1]}"><div class="hd"><span class="mo-b" style="background:${e[1]}1a;color:${e[1]}">${e[0]}</span><i>${fecha(p.actualizado || p.creado)}</i></div><h3><a href="#" data-abrir="${p.id}">${esc(p.nombre)}</a></h3><p class="ob">${esc(p.objetivo || "Sin objetivo escrito.")}</p>
      <div class="mx"><div><b>${gN(p.nPuntos, 0)}</b><span>puntos</span></div><div><b>${p.nAn || 1}</b><span>análisis</span></div><div><b>${p.vars || 0}</b><span>variables</span></div><div><b>${p.enfoque ? (p.enfoque === "sup" ? "Supervisado" : "No supervisado") : "—"}</b><span>enfoque</span></div></div>
      ${k ? `<div class="kp"><span>${esc(k.etiqueta)}</span><b>${esc(k.valor)}</b></div>` : ""}<div class="ac"><button type="button" class="mo-new" data-abrir="${p.id}">Abrir</button><button type="button" class="mo-ghost" data-ed="${p.id}">Editar puntos y opciones</button><button type="button" class="mo-ghost mo-pel" data-del="${p.id}">Eliminar</button></div></article>`; };
    const tabla = `<div class="mo-tw"><table class="mo-t"><thead><tr><th>Proyecto</th><th>Estado</th><th>Puntos</th><th>Análisis</th><th>Variables</th><th>Enfoque</th><th>Método</th><th>Resultado</th><th>Actualizado</th><th></th></tr></thead><tbody>${vis.map(p => { const e = EST[p.estado] || EST.nuevo, k = (p.kpi || [])[0]; return `<tr class="mo-r"><td><b><a href="#" data-abrir="${p.id}">${esc(p.nombre)}</a></b></td><td><span class="mo-b" style="background:${e[1]}1a;color:${e[1]}">${e[0]}</span></td><td>${gN(p.nPuntos, 0)}</td><td>${p.nAn || 1}</td><td>${p.vars || 0}</td><td>${p.enfoque ? (p.enfoque === "sup" ? "Supervisado" : "No supervisado") : "—"}</td><td>${esc(p.metodo || "—")}</td><td>${k ? esc(k.etiqueta + ": " + k.valor) : "—"}</td><td>${fecha(p.actualizado || p.creado)}</td><td class="mo-ac"><button type="button" class="mo-ghost" data-abrir="${p.id}">Abrir</button><button type="button" class="mo-ghost mo-pel" data-del="${p.id}">Eliminar</button></td></tr>`; }).join("")}</tbody></table></div>`;
    wrap(`<div class="mo-pv"><div class="mo-pvh"><div class="mo-h"><div><h1>Mis proyectos</h1><p>Cada proyecto reúne sus puntos de interés, sus variables y el modelo estadístico que se ajusta con ellas.</p></div></div><button type="button" class="mo-new" id="moNuevo">${mi("plus")} Nuevo proyecto</button></div>
      <div class="mo-kpis">${K.map(([v, t, n, c]) => `<button type="button" class="mo-kpi${S.pf === v ? " on" : ""}" style="--c:${c}" data-pf="${v}"><b>${n}</b><span>${t}</span></button>`).join("")}</div>
      <div class="mo-pvb"><label class="mo-q${S.pq ? " has" : ""}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg><input id="moPQ" type="search" placeholder="Buscar proyecto…" value="${esc(S.pq)}"></label>
        <div class="mo-seg"><button type="button" class="${S.pv === "card" ? "on" : ""}" data-pv="card" title="Tarjetas">${mi("grid")}</button><button type="button" class="${S.pv === "tabla" ? "on" : ""}" data-pv="tabla" title="Tabla">${mi("list")}</button></div></div>
      ${!S.cargado ? '<div class="mo-empty"><b>Cargando proyectos…</b></div>' : !vis.length ? `<div class="mo-empty"><b>${L.length ? "Ningún proyecto coincide." : "Aún no tienes proyectos."}</b>${L.length ? "" : "<p>Crea el primero: define el objetivo, los puntos de interés y las variables, y Model Lab te sugiere los métodos aplicables.</p>"}</div>` : S.pv === "tabla" ? tabla : `<div class="mo-pg">${vis.map(tarj).join("")}</div>`}</div>`);
    const i = $("#moPQ"); if (i && S._foco) { i.focus(); i.setSelectionRange(i.value.length, i.value.length); S._foco = false; }
  }
  /* proyecto → lista de análisis; análisis → flujo (variables, enfoque, métodos, modelar, resultados, simulación) */
  async function abre(id, enAn) {
    try { S.busy = true; wrap('<div class="mo-empty"><b>Abriendo proyecto…</b></div>'); const j = await api(R + "/proyectos/" + id); S.resVer = null; S.sim = null; S.id = id; S.P = j.proyecto; S.puntos = j.puntos || []; S.AN = j.analisis || []; S.aid = null; S.A = null; S.M = null; S.ds = null; S.res = null; S.perfiles = null; S.naForm = false; S.busy = false;
      if (enAn) await abreAn(enAn); else await ir("analisis"); }
    catch (e) { S.busy = false; toast(e.message); ir("proyectos"); }
  }
  async function cargaAn() { if (!S.id) return; try { const j = await api(R + "/proyectos/" + S.id); S.AN = j.analisis || S.AN; if (S.A) S.A = S.AN.find(a => a.id === S.A.id) || S.A; const p = S.proy.find(x => x.id === S.id); if (p && j.proyecto) Object.assign(p, { estado: j.proyecto.estado, kpi: j.proyecto.kpi, metodo: j.proyecto.metodo, enfoque: j.proyecto.enfoque, nAn: S.AN.length, vars: j.proyecto.nVars }); nav(); } catch (_) { } }
  async function abreAn(aid) {
    try { S.busy = true; wrap('<div class="mo-empty"><b>Abriendo análisis…</b></div>'); S.aid = aid; S.resVer = null; S.sim = null; S.ds = null; S.res = null; S.perfiles = null; S.cg = null; const j = await api(R + "/proyectos/" + S.id + "/analisis/" + aid); S.A = j.analisis; S.M = j.modelo || { vars: [], enfoque: null, y: null, x: [], metodo: null, prep: null, prepOk: false };
      if (!(S.M.vars || []).length && (S.P.columnas || []).length) S.M.vars = S.P.columnas.map(c => c.name); const yv = S.P.cfg && S.P.cfg.yVar; if (aid === "principal" && yv && !j.modelo && S.M.vars.includes(yv)) { S.M.enfoque = "sup"; S.M.y = yv; S.M.x = S.M.vars.filter(v => v !== yv); }
      try { const r = await api(R + "/proyectos/" + S.id + "/resultados"); S.res = r.res || null; } catch (_) { } S.busy = false; await ir(S.res ? "resultados" : "datos"); }
    catch (e) { S.busy = false; toast(e.message); ir("analisis"); }
  }
  async function elimina(id) { const p = S.proy.find(x => x.id === id); if (!p || !confirm("¿Eliminar el proyecto «" + p.nombre + "»? Se borran sus puntos, datos y resultados. No se puede deshacer.")) return; try { await api(R + "/proyectos/" + id, null, "DELETE"); if (S.id === id) { S.id = S.P = S.res = null; } await cargaProy(); vistaProyectos(); toast("Proyecto eliminado."); } catch (e) { toast(e.message); } }

  /* ═══════════════ Análisis del proyecto (misma estructura que «Mis proyectos») ═══════════════ */
  const ENFT = e => e === "sup" ? "Supervisado" : e === "nosup" ? "No supervisado" : e === "val" ? "Valuación inmobiliaria" : "—";
  function vistaAnalisis() {
    S.sec = "analisis"; nav(); const L = S.AN || [], q = (S.aq || "").trim().toLowerCase(), vis = L.filter(a => (!S.af || a.estado === S.af) && (!q || (a.nombre + " " + (a.metodo || "")).toLowerCase().includes(q))), P = S.P || {};
    const cuenta = e => L.filter(a => a.estado === e).length, K = [["", "Todos los análisis", L.length, "#4f46e5"], ["modelado", "Modelados", cuenta("modelado"), "#059669"], ["configurado", "Configurados", cuenta("configurado"), "#d97706"], ["nuevo", "Sin configurar", cuenta("nuevo"), "#64748b"]];
    const tarj = a => { const e = EST[a.estado] || EST.nuevo, k = (a.kpi || [])[0]; return `<article class="mo-pc" style="--c:${e[1]}"><div class="hd"><span class="mo-b" style="background:${e[1]}1a;color:${e[1]}">${e[0]}</span><i>${fecha(a.actualizado || a.creado)}</i></div><h3><a href="#" data-aab="${esc(a.id)}">${esc(a.nombre)}</a></h3><p class="ob">${a.metodo ? "Método: " + esc(a.metodo === "auto" ? "mejor modelo (se comparan todos)" : a.metodo) : "Aún sin método elegido."}</p>
      <div class="mx"><div><b>${a.nVars || 0}</b><span>variables</span></div><div><b>${ENFT(a.enfoque)}</b><span>enfoque</span></div></div>${k ? `<div class="kp"><span>${esc(k.etiqueta)}</span><b>${esc(k.valor)}</b></div>` : ""}<div class="ac"><button type="button" class="mo-new" data-aab="${esc(a.id)}">Abrir</button><button type="button" class="mo-ghost" data-aren="${esc(a.id)}">Renombrar</button><button type="button" class="mo-ghost mo-pel" data-adel="${esc(a.id)}">Eliminar</button></div></article>`; };
    const tabla = `<div class="mo-tw"><table class="mo-t"><thead><tr><th>Análisis</th><th>Estado</th><th>Variables</th><th>Enfoque</th><th>Método</th><th>Resultado</th><th>Actualizado</th><th></th></tr></thead><tbody>${vis.map(a => { const e = EST[a.estado] || EST.nuevo, k = (a.kpi || [])[0]; return `<tr class="mo-r"><td><b><a href="#" data-aab="${esc(a.id)}">${esc(a.nombre)}</a></b></td><td><span class="mo-b" style="background:${e[1]}1a;color:${e[1]}">${e[0]}</span></td><td>${a.nVars || 0}</td><td>${ENFT(a.enfoque)}</td><td>${esc(a.metodo || "—")}</td><td>${k ? esc(k.etiqueta + ": " + k.valor) : "—"}</td><td>${fecha(a.actualizado || a.creado)}</td><td class="mo-ac"><button type="button" class="mo-ghost" data-aab="${esc(a.id)}">Abrir</button><button type="button" class="mo-ghost" data-aren="${esc(a.id)}">Renombrar</button><button type="button" class="mo-ghost mo-pel" data-adel="${esc(a.id)}">Eliminar</button></td></tr>`; }).join("")}</tbody></table></div>`;
    const forma = S.naForm ? `<div class="mo-naf"><label class="mo-fl">Nombre del nuevo análisis<input id="moNaN" class="mo-in" maxlength="80" placeholder="Ej. Grupos de tiendas (no supervisado)" autocomplete="off"></label><div class="mo-chs"><button type="button" class="mo-new" data-naok="1">Crear análisis</button><button type="button" class="mo-ghost" data-nacan="1">Cancelar</button></div><p class="mo-mut">Usa el mismo conjunto de datos y puntos del proyecto, pero con sus propias variables, enfoque, método y resultados.</p></div>` : "";
    wrap(`<div class="mo-pv"><div class="mo-pvh"><div class="mo-h"><div><h1>Análisis de «${esc(P.nombre || "Proyecto")}»</h1><p>${gN(S.puntos.length, 0)} puntos de interés · ${esc(String(P.objetivo || "Sin objetivo escrito.").slice(0, 180))}. Cada análisis tiene sus variables, su enfoque, su método y sus resultados.</p></div></div><button type="button" class="mo-new" id="moNuevoA">${mi("plus")} Nuevo análisis</button></div>${forma}
      <div class="mo-kpis">${K.map(([v, t, n, c]) => `<button type="button" class="mo-kpi${S.af === v ? " on" : ""}" style="--c:${c}" data-af="${v}"><b>${n}</b><span>${t}</span></button>`).join("")}</div>
      <div class="mo-pvb"><label class="mo-q${S.aq ? " has" : ""}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg><input id="moAQ" type="search" placeholder="Buscar análisis…" value="${esc(S.aq || "")}"></label>
        <div class="mo-seg"><button type="button" class="${S.av === "card" ? "on" : ""}" data-av="card" title="Tarjetas">${mi("grid")}</button><button type="button" class="${S.av === "tabla" ? "on" : ""}" data-av="tabla" title="Tabla">${mi("list")}</button></div></div>
      ${!vis.length ? `<div class="mo-empty"><b>${L.length ? "Ningún análisis coincide." : "Este proyecto aún no tiene análisis."}</b></div>` : S.av === "tabla" ? tabla : `<div class="mo-pg">${vis.map(tarj).join("")}</div>`}</div>`);
    const i = $("#moAQ"); if (i && S._afoco) { i.focus(); i.setSelectionRange(i.value.length, i.value.length); S._afoco = false; } const nn = $("#moNaN"); if (nn) nn.focus();
  }
  async function creaAn() { const n = (($("#moNaN") || {}).value || "").trim(); if (!n) { toast("Escribe el nombre del análisis."); return; } try { const j = await api(R + "/proyectos/" + S.id + "/analisis", { nombre: n }); S.AN.push(j.analisis); S.naForm = false; await cargaProy(); await abreAn(j.analisis.id); toast("Análisis creado: define sus variables."); } catch (e) { toast(e.message); } }
  async function renombraAn(aid) { const a = S.AN.find(x => x.id === aid); if (!a) return; const n = prompt("Nuevo nombre del análisis:", a.nombre); if (!n || !n.trim() || n.trim() === a.nombre) return; try { await api(R + "/proyectos/" + S.id + "/analisis/" + aid, { nombre: n.trim() }); a.nombre = n.trim().slice(0, 80); if (S.A && S.A.id === aid) S.A.nombre = a.nombre; vistaAnalisis(); toast("Análisis renombrado."); } catch (e) { toast(e.message); } }
  async function eliminaAn(aid) { const a = S.AN.find(x => x.id === aid); if (!a) return; if ((S.AN || []).length <= 1) { toast("Un proyecto necesita al menos un análisis."); return; } if (!confirm("¿Eliminar el análisis «" + a.nombre + "»? Se borran sus variables, resultados y simulaciones. No se puede deshacer.")) return; try { await api(R + "/proyectos/" + S.id + "/analisis/" + aid, null, "DELETE"); S.AN = S.AN.filter(x => x.id !== aid); if (S.aid === aid) { S.aid = S.A = S.M = S.res = S.ds = null; } await cargaProy(); vistaAnalisis(); toast("Análisis eliminado."); } catch (e) { toast(e.message); } }

  /* ═══════════════ Asistente «Nuevo proyecto» (mismos pasos que Geo Lab; sin mapa conceptual; el último botón es «Modelar») ═══════════════ */
  const PASOS = [["Objetivo del proyecto", "qué quieres entender o predecir"], ["Puntos de interés", "dónde se mide: en el mapa, en un archivo o en el DENUE"], ["Radio de maqueta", "opcional · fotos de calle"], ["Vialidades", "opcional · red vial de OpenStreetMap"], ["Días y horas de tráfico", "opcional · cuándo se mide"], ["Origen / Destino vehicular", "de dónde llega el tráfico a cada punto · gratis con OpenStreetMap"], ["Verificación", "revisa los datos antes de modelar"]];
  const GMW_PR = { sv: .007, ia: .0053, sat: .012, ruta: .01, od: 364, m3d: .006 }, GMW_X = 10;   /* mismas tarifas de lista y factor que el asistente de Geo Lab */
  const DIAS = [[1, "Lun"], [2, "Mar"], [3, "Mié"], [4, "Jue"], [5, "Vie"], [6, "Sáb"], [0, "Dom"]], VIAS = [["motorway", "Autopistas"], ["trunk", "Troncales"], ["primary", "Primarias"], ["secondary", "Secundarias"], ["tertiary", "Terciarias"], ["residential", "Residenciales"]];
  const wizVacio = () => ({ paso: 1, nombre: "", objetivo: "", fuente: "mapa", tab: "mapa", puntos: [], columnas: [], mapa: null, arch: null, den: { muns: [], ciudades: [], q: "", sector: "", sub: "", cod: "", tmin: 0, tmax: 6, res: null, sel: new Set(), vista: 200, cargando: false }, radio: 200, fotos: 24, vias: ["primary", "secondary", "tertiary"], pre: 12, dias: [1, 2, 3, 4, 5], horas: [7, 8, 9, 17, 18, 19], odOn: true, odModo: "osm", opt: { fotos: false, vial: false, trafico: false, od: false, mapa3d: false }, editId: null, msg: "", gl: null });
  function costo(W) {
    const nP = Math.max(1, W.puntos.length), o = W.opt, fotos = o.fotos ? W.fotos * nP : 0, ts = W.radio > 400 ? 344 : 172, k = Math.max(1, Math.ceil((W.radio - ts / 2) / ts)), tiles = o.fotos ? (2 * k + 1) ** 2 * nP : 0;
    const sv = fotos * GMW_PR.sv * GMW_X, ia = fotos * GMW_PR.ia * GMW_X, sat = tiles * GMW_PR.sat * GMW_X, sent = W.pre * 2 * nP, calls = o.trafico ? sent * W.dias.length * W.horas.length : 0, tr = calls * GMW_PR.ruta * GMW_X, od = o.od ? nP * GMW_PR.od * GMW_PR.ruta * GMW_X : 0, m3d = o.mapa3d ? GMW_PR.m3d * GMW_X : 0;
    return { nP, fotos, tiles, sv, ia, sat, fo: sv + ia + sat, sent, calls, tr, od, m3d, total: sv + ia + sat + tr + od + m3d };
  }
  /* ── desglose detallado de costos (mismas tarifas y factor que Geo Lab) ── */
  const usdF = v => "US$ " + Number(v).toLocaleString("es-MX", { minimumFractionDigits: 2, maximumFractionDigits: v < 1 ? 4 : 2 });
  const dFila = (t, nota, cant, unit, sub) => `<tr><td><b>${t}</b>${nota ? `<small>${nota}</small>` : ""}</td><td class="r">${gN(cant, 0)}</td><td class="r">${usdF(unit)}</td><td class="r">${usdF(unit * GMW_X)}</td><td class="r"><b>${usd(sub)}</b></td></tr>`;
  const dTabla = (filas, pie) => `<div class="mo-tw"><table class="mo-ct mo-dc"><thead><tr><th>Concepto</th><th class="r">Cantidad</th><th class="r">Costo unitario del proveedor</th><th class="r">Precio unitario (× ${GMW_X})</th><th class="r">Subtotal</th></tr></thead><tbody>${filas}</tbody><tfoot>${pie}</tfoot></table></div>`;
  const dPie = (c, total, etiqueta) => `<tr><td colspan="4">Costo del proveedor (sin el factor × ${GMW_X})</td><td class="r">${usd(total / GMW_X)}</td></tr><tr><td colspan="4">Costo por punto (${gN(c.nP, 0)} puntos)</td><td class="r">${usd(total / c.nP)}</td></tr><tr class="t"><td colspan="4">${etiqueta}</td><td class="r"><b>${usd(total)}</b></td></tr>`;
  function dMaqueta(W) {
    const c = costo(W), ts = W.radio > 400 ? 344 : 172, k = Math.max(1, Math.ceil((W.radio - ts / 2) / ts)), tpp = (2 * k + 1) ** 2;
    return `<div class="mo-fa">Desglose del costo de la maqueta</div>` + dTabla(
      dFila("Fotos de calle (Google Street View)", `${W.fotos} fotos por punto × ${gN(c.nP, 0)} puntos`, c.fotos, GMW_PR.sv, c.sv) + dFila("Interpretación de las fotos con IA", `una lectura por foto`, c.fotos, GMW_PR.ia, c.ia) + dFila("Teselas satelitales", `${tpp} teselas por punto × ${gN(c.nP, 0)} puntos`, c.tiles, GMW_PR.sat, c.sat),
      dPie(c, c.fo, "Total de la maqueta")) + `<p class="mo-mut mo-dn">Teselas por punto: con radio de ${W.radio} m se usan teselas de ${ts} m; ${k} anillo${k === 1 ? "" : "s"} alrededor del punto da (2 × ${k} + 1)² = <b>${tpp}</b> teselas. Los precios son de lista del proveedor por unidad; Model Lab aplica el mismo factor × ${GMW_X} que Geo Lab.</p>`;
  }
  function dTrafico(W) {
    const c = costo(W), vias = W.pre;
    return `<div class="mo-fa">Desglose del costo del tráfico</div>` + dTabla(
      `<tr><td><b>Sentidos de vía</b><small>${vias} vías por punto × 2 sentidos × ${gN(c.nP, 0)} puntos</small></td><td class="r">${gN(c.sent, 0)}</td><td class="r" colspan="3"></td></tr><tr><td><b>Días × horas</b><small>${W.dias.length} días × ${W.horas.length} horas</small></td><td class="r">${W.dias.length * W.horas.length}</td><td class="r" colspan="3"></td></tr>` + dFila("Consultas de ruta (Google Routes)", `${gN(c.sent, 0)} sentidos × ${W.dias.length * W.horas.length} franjas`, c.calls, GMW_PR.ruta, c.tr),
      dPie(c, c.tr, "Total del tráfico")) + `<p class="mo-mut mo-dn">Cada consulta mide el tiempo de recorrido de un sentido de vía en un día y hora. Para bajar el costo: menos vías por punto, menos días o menos horas.</p>`;
  }
  function dExtras(W) { const c = costo(W); let f = ""; if (W.opt.od) f += dFila("Origen-destino", `${gN(c.nP, 0)} puntos × ${GMW_PR.od} consultas por punto`, c.nP * GMW_PR.od, GMW_PR.ruta, c.od); if (W.opt.mapa3d) f += dFila("Mapa 3D de Google", "una activación", 1, GMW_PR.m3d, c.m3d); return f ? `<div class="mo-fa">Otros conceptos</div>` + dTabla(f, `<tr class="t"><td colspan="4">Total de otros conceptos</td><td class="r"><b>${usd(c.od + c.m3d)}</b></td></tr>`) : ""; }
  /* aviso de «análisis costoso» (como en Geo Lab) para los pasos opcionales */
  function aviso(W, paso) {
    const c = costo(W), n = W.puntos.length, muchos = n > 20, base = `<b>Paso opcional.</b> En Model Lab las variables salen de la biblioteca nacional de indicadores (Censo, DENUE, OpenStreetMap, INEGI…) y de las columnas de tu archivo: este paso no es necesario para modelar. Lo que actives aquí queda <b>registrado en el ${W.sim ? "conjunto" : "proyecto"}</b>; el análisis por punto con Geo Lab todavía no se lanza automáticamente desde Model Lab.`;
    let costoTxt = "";
    if (paso === 3) costoTxt = W.opt.fotos ? `Fotos de calle con IA: ${gN(c.fotos, 0)} fotos (${W.fotos} por punto × ${gN(c.nP, 0)} puntos) y ${gN(c.tiles, 0)} teselas satelitales = <b>${usd(c.fo)}</b>.` : "Sin análisis de fotos: costo $0.";
    if (paso === 4) costoTxt = `Red vial: ${gN(c.sent, 0)} sentidos de vialidad (${W.pre} vías × 2 sentidos × ${gN(c.nP, 0)} puntos). La descarga de OpenStreetMap es gratuita; el costo aparece al consultar el tráfico.`;
    if (paso === 5) costoTxt = W.opt.trafico ? `Tráfico: ${gN(c.sent, 0)} sentidos × ${W.dias.length} días × ${W.horas.length} horas = ${gN(c.calls, 0)} consultas = <b>${usd(c.tr)}</b>.` : "Sin consulta de tráfico: costo $0.";
    const alto = (paso === 3 && W.opt.fotos && c.fo > 500) || (paso === 5 && W.opt.trafico && c.tr > 500) || (muchos && (W.opt.fotos || W.opt.trafico));
    return `<div class="mo-av${alto ? " alto" : ""}"><div>${base}</div>${costoTxt ? `<div class="c">⚠ ${costoTxt}</div>` : ""}${muchos ? `<div class="c">Tu proyecto tiene <b>${gN(n, 0)} coordenadas</b>: los análisis de «Radio de maqueta», «Vialidades» y «Días y horas de tráfico» se cobran <b>por punto</b> y pueden resultar muy costosos con decenas de puntos o más. Estimado total de lo activado: <b>${usd(c.total)}</b>.</div>` : ""}</div>`;
  }
  function wizard(P, pts) {
    if (!S.wiz) S.wiz = wizVacio(); S.sec = "nuevo"; nav(); const W = S.wiz, p = W.paso, sim = !!W.sim, off = sim ? 1 : 0, lista = sim ? PASOS.slice(1) : PASOS, total = lista.length + off, pasos = lista.map((_, i) => { const n = i + 1 + off; return `<i class="${n < p ? "ok" : n === p ? "on" : ""}"></i>`; }).join("");
    wrap(`<div class="mo-wz"><div class="mo-wzh"><span class="mo-wzp">${String(p).padStart(2, "0")}/${String(total).padStart(2, "0")}</span><div><h2>${PASOS[p - 1][0]}</h2><p>${sim && p === 7 ? "revisa el conjunto antes de analizarlo" : sim && p === 2 ? "dónde están los puntos nuevos: en el mapa, con coordenadas, en un archivo o en el DENUE" : PASOS[p - 1][1]}</p></div></div><div class="mo-wzg">${pasos}</div><div class="mo-wzb" id="wzBody">${wzPaso(W)}</div>
      <div class="mo-wzf"><button type="button" class="mo-ghost" data-wz="atras">${mi("back")} ${p === (sim ? 2 : 1) ? "Cancelar" : "Atrás"}</button><span class="mo-wzm" id="wzMsg">${esc(W.msg || "")}</span>${p < total ? `<button type="button" class="mo-new" data-wz="sig">Continuar →</button>` : sim ? `<button type="button" class="mo-new mo-rojo" data-wz="simular">Analizar puntos</button>` : `<button type="button" class="mo-new mo-rojo" data-wz="modelar">${W.editId ? "Guardar y modelar" : "Modelar"}</button>`}</div></div>`);
    wzPost(W);
  }
  function wzPaso(W) {
    const p = W.paso;
    if (p === 1) return `<label class="mo-fl">Nombre del proyecto<input id="wzNom" class="mo-in" maxlength="80" placeholder="Ej. Sucursales y entorno urbano" value="${esc(W.nombre)}"></label>
      <label class="mo-fl">Objetivo<textarea id="wzObj" class="mo-in" rows="6" maxlength="4000" placeholder="Describe con tus palabras qué quieres lograr: p. ej. «Entender qué características del entorno explican las ventas de mis sucursales» o «Agrupar colonias con perfiles parecidos».">${esc(W.objetivo)}</textarea></label>
      <div class="mo-nota">Después podrás elegir variables de la biblioteca de indicadores y definir si el análisis es supervisado (hay una variable de respuesta) o no supervisado.</div>
      <div class="mo-imp"><button type="button" class="mo-ghost" data-wz="geolab">Usar un proyecto de Geo Lab</button><span>Trae su nombre, objetivo, puntos y mapa conceptual.</span></div><div id="wzGL"></div>`;
    if (p === 2) return wzPuntos(W);
    if (p === 3) { const on = W.opt.fotos; return `<div class="mo-swr mo-swg"><button type="button" class="mo-swb${on ? " on" : ""}" data-wzopt="fotos" aria-label="Crear maqueta"><i></i></button><div><b>¿Crear maqueta?</b><span>⚠ <strong>Incurre en costos</strong>: descarga fotos de calle (Street View) y teselas satelitales alrededor de cada punto y las interpreta con IA. ${on ? "" : "Es opcional: déjalo apagado si no lo necesitas."}</span></div></div>`
      + (on ? aviso(W, 3) + dMaqueta(W) + `${wzMapaPts(W)}<label class="mo-fl">Radio de la maqueta: <b id="wzRadV">${W.radio} m</b><input type="range" id="wzRad" min="50" max="600" step="10" value="${W.radio}"></label><div class="mo-chs">${[100, 200, 400, 600].map(r => `<button type="button" class="mo-c${W.radio === r ? " on" : ""}" data-wzrad="${r}">${r} m</button>`).join("")}</div>
      <label class="mo-fl">Fotos por punto: <b id="wzFotV">${W.fotos}</b><input type="range" id="wzFot" min="10" max="60" step="2" value="${W.fotos}"></label>` : `<div class="mo-nota">Maqueta desactivada: no se descargarán fotos ni se generará costo. Actívala para elegir el radio y las fotos por punto, y ver el estimado.</div>`); }
    if (p === 4) { const on = W.opt.vial; return `<div class="mo-swr mo-swg"><button type="button" class="mo-swb${on ? " on" : ""}" data-wzopt="vial" aria-label="Descargar vialidades"><i></i></button><div><b>¿Descargar vialidades?</b><span>La descarga de la red vial de OpenStreetMap es gratuita, pero define las vías sobre las que luego se consulta el tráfico (paso 5), que <strong>sí incurre en costos</strong>. ${on ? "" : "Es opcional: déjalo apagado si no lo necesitas."}</span></div></div>`
      + (on ? aviso(W, 4) + `${wzMapaPts(W)}<div class="mo-fl">Tipos de vialidad a descargar</div><div class="mo-chs">${chips(VIAS, W.vias, "data-wzvia")}</div><label class="mo-fl">Vías preseleccionadas por punto: <b id="wzPreV">${W.pre}</b><input type="range" id="wzPre" min="2" max="30" step="1" value="${W.pre}"></label>` : `<div class="mo-nota">Vialidades desactivadas: no se descargará la red vial. Actívalo para elegir los tipos de vía y cuántas se preseleccionan por punto.</div>`); }
    if (p === 5) { const on = W.opt.trafico; return `<div class="mo-swr mo-swg"><button type="button" class="mo-swb${on ? " on" : ""}" data-wzopt="trafico" aria-label="Consultar tráfico"><i></i></button><div><b>¿Consultar el tráfico por día y hora?</b><span>⚠ <strong>Incurre en costos</strong>: una consulta de Google Routes por cada sentido de vía, día y hora. ${on ? "" : "Es opcional: déjalo apagado si no lo necesitas."}</span></div></div>`
      + (on ? aviso(W, 5) + dTrafico(W) + `<div class="mo-fl">Días</div><div class="mo-chs">${chips(DIAS, W.dias, "data-wzdia")}</div><div class="mo-fl">Horas del día</div><div class="mo-chs mo-hs">${Array.from({ length: 24 }, (_, h) => [h, String(h).padStart(2, "0")]).map(([h, t]) => `<button type="button" class="mo-c${W.horas.includes(h) ? " on" : ""}" data-wzhor="${h}">${t}</button>`).join("")}</div>
      <div class="mo-chs"><button type="button" class="mo-ghost" data-wzq="pico">Hora pico</button><button type="button" class="mo-ghost" data-wzq="habil">Entre semana</button><button type="button" class="mo-ghost" data-wzq="finde">Fin de semana</button><button type="button" class="mo-ghost" data-wzq="todo">Todo el día</button></div>` : `<div class="mo-nota">Tráfico desactivado: no se consultará Google Routes ni se generará costo. Actívalo para elegir los días y las horas, y ver el estimado.</div>`); }
    if (p === 6) return wzOD(W);
    return W.sim ? wzSimVerif(W) : wzVerif(W);
  }
  /* resumen del conjunto de puntos nuevos antes de analizarlo */
  function wzSimVerif(W) {
    const v = verDe(W.simVer), n = W.puntos.length, falta = []; if (!W.nombre.trim()) falta.push("el nombre del conjunto"); if (!n) falta.push("al menos un punto");
    const fuente = { mapa: "elegidos en el mapa o por coordenadas", archivo: "cargados desde un archivo", denue: "unidades del DENUE", inmuebles: "propiedades de la base de bienes inmuebles" }[W.fuente] || "";
    return `${falta.length ? `<div class="mo-av alto"><div class="c">Falta ${falta.join(" y ")}. Regresa al paso 2 para completarlo.</div></div>` : ""}<div class="mo-fa">Resumen del conjunto</div><div class="mo-tw"><table class="mo-t"><tbody><tr><td><b>Nombre</b></td><td>${esc(W.nombre)}</td></tr><tr><td><b>Puntos</b></td><td>${gN(n, 0)} ${fuente ? "· " + fuente : ""}</td></tr><tr><td><b>Versión del modelo</b></td><td>${esc(v ? v.nombre : "—")}</td></tr></tbody></table></div>${wzSimResumen(W)}`;
  }
  function wzSimResumen(W) {
    const v = verDe(W.simVer), c = costo(W), A = simAnalisis(W, (S.M.x || []).length, v ? v.nombre : "—"), pago = c.total;
    return `<div class="mo-fa">Qué se hará al pulsar «Analizar puntos»</div><div class="mo-tw"><table class="mo-t"><tbody>${A.map(a => { const e = EST_A[a.e]; return `<tr><td><b>${esc(a.t)}</b></td><td><span class="mo-b" style="background:${e[0]}1a;color:${e[0]}">${a.e === "hecho" || a.e === "proc" ? "Se hace ahora" : a.e === "reg" ? "Se registra" : "No solicitado"}</span></td><td class="mo-mut">${esc(a.d)}</td></tr>`; }).join("")}</tbody></table></div>
      <p class="mo-mut">${gN(W.puntos.length, 0)} ${W.puntos.length === 1 ? "punto" : "puntos"} · modelo ${esc(v ? v.nombre : "—")}. Los análisis gratuitos corren siempre; lo que incurre en costos (maqueta con fotos, tráfico, Google Routes) solo se registra, salvo el Origen / Destino con Google Routes si lo elegiste (${usd(c.od)}).</p>`;
  }
  /* tiempo estimado del análisis Origen/Destino: ≈ 10 s por punto (medido), uno tras otro y en segundo plano */
  const tiempoOD = n => { const m = Math.round(n * 10 / 60); return m < 1 ? "menos de 1 minuto" : m < 90 ? `≈ ${gN(m, 0)} minutos` : `≈ ${gN(m / 60, 1)} horas`; };
  function wzOD(W) {
    const n = Math.max(1, W.puntos.length), c = costo(W), on = W.odOn, g = W.odModo === "google";
    return `<div class="mo-swr mo-swg"><button type="button" class="mo-swb${on ? " on" : ""}" data-wzod="on" aria-label="Calcular Origen/Destino"><i></i></button><div><b>¿Calcular Origen / Destino vehicular?</b><span>Estima de qué zonas llegan los viajes en auto a cada punto y qué porcentaje cruza por su calle. <strong>Por omisión se calcula gratis con OpenStreetMap.</strong></span></div></div>`
      + (on ? `<div class="mo-fl">Fuente de los tiempos de viaje</div><div class="mo-gl2">
        <button type="button" class="mo-opc${!g ? " on" : ""}" data-wzod="osm"><b>OpenStreetMap <span class="mo-gratis">Gratis · predeterminado</span></b><span>Los tiempos salen de la red vial de OpenStreetMap, con un ajuste de +30 % por tráfico típico. Sin costo ni confirmación.</span></button>
        <button type="button" class="mo-opc${g ? " on" : ""}" data-wzod="google"><b>Google Routes <span class="mo-paga">De paga</span></b><span>Tiempos con tráfico real de Google (hora pico). ${GMW_PR.od} consultas por punto: <b>${usd(c.nP * GMW_PR.od * GMW_PR.ruta * GMW_X)}</b> con ${gN(c.nP, 0)} ${c.nP === 1 ? "punto" : "puntos"}.</span></button></div>
        <div class="mo-av${n > 200 || g ? " alto" : ""}"><div><b>⏱ Tiempo estimado: ${tiempoOD(n)}</b> para ${gN(n, 0)} ${n === 1 ? "punto" : "puntos"} (unos 10 segundos por punto, uno tras otro, en segundo plano). Puedes abrir tu proyecto y modelar mientras corre; el resultado de cada punto aparece cuando termina.</div>${g ? `<div class="c">⚠ Google Routes <b>incurre en costos</b>: ${usd(c.nP * GMW_PR.od * GMW_PR.ruta * GMW_X)} estimados. Al continuar, autorizas ese gasto.</div>` : ""}${!g && n > 200 ? `<div class="c">Con ${gN(n, 0)} puntos tardará varias horas, pero no cuesta nada.</div>` : ""}</div>`
        : `<div class="mo-nota">Origen / Destino desactivado: no se calculará ninguna estimación de origen del tráfico para los puntos de este proyecto. Actívalo si quieres tenerla; la opción gratuita (OpenStreetMap) viene seleccionada.</div>`);
  }
  function wzVerif(W) {
    const c = costo(W), n = W.puntos.length, falta = []; if (!W.nombre.trim()) falta.push("el nombre del proyecto"); if (!n) falta.push("al menos un punto de interés");
    const fuente = { mapa: "elegidos en el mapa", archivo: "cargados desde un archivo", denue: "unidades del DENUE", inmuebles: "propiedades de la base de bienes inmuebles", geolab: "traídos de un proyecto de Geo Lab" }[W.fuente] || "";
    const libre = (t, v) => `<tr class="gr"><td>${t}</td><td>${v}</td><td class="r"><span class="mo-gratis">Gratis</span></td></tr>`;
    const fila = (t, v, b) => `<tr><td>${t}</td><td>${v}</td><td class="r">${b != null ? usd(b) : "—"}</td></tr>`;
    return `<div class="mo-fic"><div><small>Proyecto</small><b>${esc(W.nombre || "—")}</b></div><div><small>Puntos</small><b>${gN(n, 0)}</b><span>${fuente}</span></div><div><small>Columnas importadas</small><b>${W.columnas.length}</b><span>${esc(W.columnas.slice(0, 4).map(c => c.label).join(", ") || "ninguna")}${W.columnas.length > 4 ? "…" : ""}</span></div></div>
      <p class="mo-fo">${esc(W.objetivo || "Sin objetivo escrito.")}</p>
      <table class="mo-ct"><thead><tr><th>Concepto</th><th>Qué incluye</th><th class="r">Estimado</th></tr></thead><tbody>
      <tr class="sec"><td colspan="3">Con costo · opcionales (apagados por omisión)</td></tr>
      ${fila("Fotos de calle con IA (maqueta)", W.opt.fotos ? `${gN(c.fotos, 0)} fotos · ${gN(c.tiles, 0)} teselas · radio ${W.radio} m` : "no incluido", W.opt.fotos ? c.fo : 0)}${fila("Tráfico por día y hora (Google Routes)", W.opt.trafico ? `${gN(c.calls, 0)} consultas` : "no incluido", W.opt.trafico ? c.tr : 0)}${fila("Origen-destino (Google Routes)", W.odOn && W.odModo === "google" ? `${gN(c.nP, 0)} puntos × ${GMW_PR.od} consultas` : "no incluido (se calcula gratis con OpenStreetMap)", W.odOn && W.odModo === "google" ? c.od : 0)}${fila("Mapa 3D de Google", `<button type="button" class="mo-swb${W.opt.mapa3d ? " on" : ""}" data-wzopt="mapa3d"><i></i></button>`, W.opt.mapa3d ? c.m3d : 0)}
      <tr class="sec"><td colspan="3">Sin costo · incluidos en el proyecto</td></tr>
      ${W.odOn && W.odModo === "osm" ? libre("Origen / Destino vehicular (OpenStreetMap)", `${gN(n, 0)} puntos · tiempo estimado ${tiempoOD(n)}`) : ""}${libre("Variables de la biblioteca nacional", `${S.bib.length} indicadores del Censo, DENUE, OpenStreetMap, INEGI, WorldCover y WorldClim, ya calculados por AGEB`)}${libre("Ubicación de cada punto en su AGEB", `${gN(n, 0)} consultas a la malla nacional (sin análisis por sitio)`)}${libre("Vialidades de OpenStreetMap", W.opt.vial ? `${W.vias.length} tipos de vía · ${W.pre} por punto (la descarga es gratuita)` : "no incluido")}${W.fuente === "denue" ? libre("Unidades del DENUE", "búsqueda y selección de establecimientos de INEGI") : ""}${W.fuente === "archivo" ? libre("Columnas de tu archivo", `${W.columnas.length} columnas importadas como variables`) : ""}${libre("Mapas base y búsqueda de lugares", "Esri y OpenStreetMap (Nominatim)")}${libre("Preprocesamiento y modelado estadístico", "12 métodos supervisados y 6 no supervisados; se calculan en tu navegador")}${libre("Resultados, gráficas y descarga de tablas", "KPIs, calidad del modelo y exportación a CSV")}
      </tbody><tfoot><tr><td colspan="2">Total estimado de lo activado</td><td class="r"><b>${usd(c.total)}</b></td></tr></tfoot></table>
      ${c.total > 0 ? `<details class="mo-sx" open><summary><b>Desglose detallado del estimado</b><span>cantidades, costo del proveedor y precio</span></summary><div class="mo-vl">${W.opt.fotos ? dMaqueta(W) : ""}${W.opt.trafico ? dTrafico(W) : ""}${dExtras(W)}</div></details>` : ""}
      ${n > 20 && c.total > 0 ? `<div class="mo-av alto"><div class="c">⚠ Con ${gN(n, 0)} coordenadas, el estimado crece por punto. Revisa los pasos 3, 4, 5 y 6 antes de continuar.</div></div>` : ""}
      <div class="mo-nota">Las variables del modelo salen de la biblioteca nacional de indicadores y de las columnas de tu archivo. Estos conceptos opcionales solo se registran en el proyecto.</div>
      ${falta.length ? `<div class="mo-av alto"><div class="c">Falta ${falta.join(" y ")}.</div></div>` : ""}`;
  }

  /* ── paso 2: puntos de interés ── */
  function wzSimPanel(W) {
    const usaPr = (S.M.x || []).filter(n => !esLib(n)).map(n => nomVar(n));
    return `<div class="mo-sm-lo"><label class="mo-fl">Nombre del conjunto<input id="wzNomS" class="mo-in" maxlength="80" value="${esc(W.nombre)}"></label><label class="mo-fl">Versión del modelo con la que se evaluará<select id="wzVer" class="mo-in">${verOpts(W.simVer)}</select></label></div>
      ${usaPr.length ? `<div class="mo-nota">Este modelo también usa columnas propias del proyecto (${esc(usaPr.slice(0, 6).join(", "))}${usaPr.length > 6 ? "…" : ""}). Los puntos nuevos no las traen, salvo que cargues un archivo (o unidades del DENUE) con esas mismas columnas; si faltan, el modelo las rellena según el preprocesamiento.</div>` : ""}`;
  }
  function wzPuntos(W) {
    const T = [["mapa", "Elegir en el mapa"], ["archivo", "Cargar archivo (CSV, SHP, GeoJSON)"], ["denue", "Unidades del DENUE"], ["inmuebles", "Bienes inmuebles"]], tab = W.tab;
    return `${W.sim ? wzSimPanel(W) : ""}<div class="mo-tabs">${T.map(([k, t]) => `<button type="button" class="${tab === k ? "on" : ""}" data-wzt="${k}">${t}</button>`).join("")}</div>
      <div class="mo-pts"><b>${gN(W.puntos.length, 0)}</b> punto${W.puntos.length === 1 ? "" : "s"} listo${W.puntos.length === 1 ? "" : "s"}${W.puntos.length ? ` <i>· ${{ mapa: "elegidos en el mapa", archivo: "del archivo", denue: "del DENUE", geolab: "de Geo Lab", inmuebles: "de la base de bienes inmuebles" }[W.fuente] || ""}</i> <button type="button" class="mo-ghost" data-wz="limpia">Quitar todos</button>` : ""}</div>
      ${tab === "mapa" ? wzMapaHtml(W) : tab === "archivo" || tab === "inmuebles" ? wzArchHtml(W) : wzDenueHtml(W)}`;
  }
  const wzMapaHtml = W => {
    const M = W.modoP || "mapa", OPC = [["coord", "1 · Coordenada manual", "Escribe latitud y longitud"], ["url", "2 · URL de Google Maps", "Pega el enlace de un lugar"], ["mapa", "3 · Seleccionar en el mapa", "Busca un lugar y haz clic"]];
    const panel = M === "coord" ? `<div class="mo-sm-cc2"><textarea id="wzCoord" class="mo-in" rows="2" placeholder="Latitud, longitud (una pareja por renglón). Ej. 25.6866, -100.3161"></textarea><button type="button" class="mo-new" data-wz="coord">Agregar coordenadas</button></div>`
      : M === "url" ? `<div class="mo-sm-cc2"><textarea id="wzUrl" class="mo-in" rows="2" placeholder="Pega el enlace de Google Maps (largo o corto, como maps.app.goo.gl/…). Uno por renglón."></textarea><button type="button" class="mo-new" data-wz="url">Agregar desde el enlace</button></div>`
      : `<div class="mo-nota">Haz clic en el mapa para agregar puntos (máximo 200 en este modo; para más, carga un archivo). Usa el buscador de arriba del mapa para moverte a un lugar.</div>`;
    return `<div class="mo-fa">¿Cómo quieres agregar los puntos?</div><div class="mo-gl2 mo-gl3">${OPC.map(([k, t, d]) => `<button type="button" class="mo-opc${M === k ? " on" : ""}" data-wzmodo="${k}"><b>${t}</b><span>${d}</span></button>`).join("")}</div>${panel}
    <div class="mo-fa">Puntos agregados</div><div class="mo-chs" id="wzPL">${wzChips(W)}</div><div class="mo-mapw"><div id="wzMap" class="mo-map"></div><div class="mo-mbus"><input id="wzMb" class="mo-in" placeholder="Buscar un lugar: dirección, colonia, negocio, ciudad…" autocomplete="off"><div class="mo-mbl" id="wzMbL" style="display:none"></div></div></div>`;
  };
  const wzChips = W => W.puntos.length ? W.puntos.slice(0, 200).map((q, i) => `<span class="mo-c on">${esc(q.id || "P" + (i + 1))} <i class="mo-mut">${q.lat}, ${q.lng}</i> <button type="button" data-wzrm="${i}" aria-label="Quitar">×</button></span>`).join("") : '<span class="mo-mut">Aún no hay puntos.</span>';
  /* ── Bienes inmuebles: bases de propiedades descargadas en Check Inn (Alojamientos, Facebook, Mercado Libre) ── */
  const wzInmHtml = W => {
    const I = W.inm || (W.inm = { lista: null, cargando: false, err: "", cargaId: "" });
    if (!I.lista && !I.cargando && !I.err) { I.cargando = true; api(R + "/inmuebles").then(j => { I.lista = j.fuentes || []; }).catch(e => { I.err = e.message; }).then(() => { I.cargando = false; if (S.wiz === W && W.tab === "inmuebles") wzRe(); }); }
    const act = W.arch && W.arch.inm;
    return `<div class="mo-nota">Elige una de las <b>bases de propiedades descargadas</b> en Check Inn. Verás la tabla completa con los mismos filtros y funciones que en cualquier archivo de puntos, y podrás analizarla con un método de <b>valuación inmobiliaria</b> que detecta propiedades por debajo o por encima del precio de mercado.</div>
      ${I.err ? `<div class="mo-av alto"><div class="c">${esc(I.err)}</div></div>` : ""}${!I.lista && !I.err ? '<div class="mo-bn">Cargando las bases disponibles…</div>' : ""}
      <div class="mo-gl2 mo-gl3">${(I.lista || []).map(f => `<button type="button" class="mo-opc${act === f.id ? " on" : ""}" data-wzinm="${esc(f.id)}"${f.n ? "" : " disabled"}><b>${esc(f.nombre)}</b><span>${esc(f.desc || "")}</span><span class="mo-mut">${gN(f.n, 0)} con ubicación · ${gN(f.conPrecio, 0)} con precio</span></button>`).join("")}</div>${I.cargaId ? '<div class="mo-bn">Cargando la tabla…</div>' : ""}`;
  };
  async function wzInmCarga(W, id) {
    const I = W.inm; if (!I || I.cargaId) return; I.cargaId = id; wzRe();
    try { const j = await api(R + "/inmuebles/" + encodeURIComponent(id)), ix = n => j.cols.indexOf(n), il = ix("Latitud"), io = ix("Longitud"), ii = ix("ID"), ip = ix("Precio");
      W.arch = { nombre: j.fuente, inm: id, cols: j.cols, rows: j.rows, sel: new Set(j.rows.map((_, k) => k)), _hadF: false, ilat: il, ilng: io, iid: ii, iy: ip, extra: j.cols.map((_, k) => k).filter(k => k !== il && k !== io && k !== ii && k !== ip) }; W.puntos = []; W.columnas = []; W.msg = ""; }
    catch (e) { I.err = e.message; } I.cargaId = ""; wzRe();
  }
  const wzArchHtml = W => {
    const A = W.arch; let h = W.tab === "inmuebles" ? wzInmHtml(W) : `<div class="mo-nota">Carga un <b>CSV</b>, <b>GeoJSON</b> o <b>Shapefile (.zip)</b> con las coordenadas. Verás una vista previa y podrás elegir la columna de coordenadas, la de nombre o identificador y otras columnas para usarlas como variables.</div>
      <label class="mo-file"><input type="file" id="wzFile" accept=".csv,.txt,.tsv,.geojson,.json,.zip,.shp"></label>`;
    if (!A || (W.tab === "inmuebles") !== !!A.inm) return h;
    if (A.err) return h + `<div class="mo-av alto"><div class="c">${esc(A.err)}</div></div>`;
    const op = (sel, vacio) => (vacio ? `<option value="-1">${vacio}</option>` : "") + A.cols.map((c, i) => `<option value="${i}"${sel === i ? " selected" : ""}>${esc(c)}</option>`).join("");
    h += `<div class="mo-fa"><b>${esc(A.nombre)}</b> · ${gN(A.rows.length, 0)} filas · ${A.cols.length} columnas</div>${wzFiltHtml(A)}
      <p class="mo-mut" id="wzTbSt"></p><div class="mo-tw mo-twp mo-twv"><table class="mo-t mo-prev"><thead><tr><th class="mo-thr"><span class="mo-ck" id="wzRall" data-wzrall="1" role="checkbox" title="Marcar o quitar todas las filas visibles"></span></th>${A.cols.map((c, i) => i === A.ilat || i === A.ilng ? `<th class="mo-thc geo on" title="Columna de ${i === A.ilat ? "latitud" : "longitud"} (se elige abajo)"><span class="mo-ck on">✓</span><span class="mo-thg">${i === A.ilat ? "LATITUD" : "LONGITUD"}</span> ${esc(c)}${wzFhBtn(A, i)}</th>` : i === A.iy ? `<th class="mo-thc geo y on" title="Variable a modelar (se elige abajo)"><span class="mo-ck on">✓</span><span class="mo-thg">A MODELAR</span> ${esc(c)}${wzFhBtn(A, i)}</th>` : i === A.iid ? `<th class="mo-thc geo id on" title="Nombre o identificador del punto (se elige abajo)"><span class="mo-ck on">✓</span><span class="mo-thg">IDENTIFICADOR</span> ${esc(c)}${wzFhBtn(A, i)}</th>` : `<th class="mo-thc${A.extra.includes(i) ? " on" : ""}" data-wzex="${i}" title="Usar como variable">${ck(A.extra.includes(i))} ${esc(c)}${wzFhBtn(A, i)}</th>`).join("")}</tr></thead><tbody id="wzTb"></tbody></table></div>
      <div class="mo-g3" style="grid-template-columns:repeat(4,minmax(0,1fr))"><label class="mo-fl">Columna de latitud<select class="mo-in" id="wzLat">${op(A.ilat, "— elegir —")}</select></label><label class="mo-fl">Columna de longitud<select class="mo-in" id="wzLng">${op(A.ilng, "— elegir —")}</select></label><label class="mo-fl">Nombre o identificador (opcional)<select class="mo-in" id="wzId">${op(A.iid, "— ninguno —")}</select></label><label class="mo-fl">Variable a modelar (opcional)<select class="mo-in" id="wzY">${op(A.iy, "— ninguna —")}</select></label></div>
      <p class="mo-mut">Marca en el encabezado de cada columna las que quieras usar como variables (opcional) · <b id="wzExN">${A.extra.length}</b> de ${A.cols.length - (A.ilat >= 0 ? 1 : 0) - (A.ilng >= 0 ? 1 : 0)} marcadas.</p>
      <div class="mo-chs"><button type="button" class="mo-new" data-wz="usaarch">Usar ${gN(selN(A), 0)} filas como puntos</button><button type="button" class="mo-ghost" data-wzex="todas">Marcar todas</button><button type="button" class="mo-ghost" data-wzex="ninguna">Quitar todas</button></div>`;
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
    const r = D.res; if (r) { const ns = D.sel.size, todas = ns === r.items.length && ns > 0, vis = r.items.slice(0, D.vista);
      h += `<div class="mo-fa"><b>${gN(r.total, 0)}</b> unidades encontradas${r.truncado ? ` · se cargaron las primeras ${gN(r.devueltos, 0)} (afina los filtros para acotar)` : ""} · <b id="wzNSel">${gN(ns, 0)}</b> seleccionada${ns === 1 ? "" : "s"}</div>
      <div class="mo-chs"><button type="button" class="mo-ghost" data-wzds="todas">Seleccionar todas</button><button type="button" class="mo-ghost" data-wzds="ninguna">Ninguna</button></div>
      <div class="mo-dsplit"><div class="mo-dleft"><div class="mo-tw mo-tsc"><table class="mo-t mo-prev"><thead><tr><th class="mo-tck"><button type="button" class="mo-ckb" data-wzds="${todas ? "ninguna" : "todas"}" aria-label="Seleccionar o quitar todas">${ck(todas)}</button></th><th>Nombre</th><th>SCIAN</th><th>Sector</th><th>Personal</th></tr></thead><tbody>${vis.map((i, k) => `<tr class="mo-trs${D.sel.has(k) ? " on" : ""}${D.foco === k ? " foco" : ""}" data-wzrow="${k}"><td class="mo-tck" data-wzds="${k}" title="Seleccionar o quitar">${ck(D.sel.has(k))}</td><td>${esc(i.nombre)}</td><td>${esc(i.scian)}</td><td>${esc(SC_S()[i.scian.slice(0, 2)] ? SC_S()[i.scian.slice(0, 2)].slice(0, 40) : i.scian.slice(0, 2))}</td><td>${r.tamanos[i.tam]}</td></tr>`).join("")}</tbody></table></div>
      ${r.items.length > D.vista ? `<div class="mo-chs"><span class="mo-mut">Mostrando ${gN(D.vista, 0)} de ${gN(r.items.length, 0)}.</span><button type="button" class="mo-ghost" data-wzds="mas">Mostrar 200 más</button></div>` : ""}</div>
      <div class="mo-dright"><div class="mo-fa">Mapa de las unidades seleccionadas <span class="mo-mut">· clic en un renglón o en un pin para enlazarlos${ns > 100 ? "; acércate para ver las etiquetas" : ""}</span></div><div id="wzDMap" class="mo-map mo-mapd"></div></div></div>
      ${r.total ? `<div class="mo-chs"><button type="button" class="mo-new" data-wz="usaden"${ns ? "" : " disabled"}>Usar ${gN(ns, 0)} unidades seleccionadas como puntos</button><span class="mo-mut">Se guardan como variables: sector, subsector, rama, grupo de giro y tamaño.</span></div>` : ""}`; }
    return h;
  };

  /* ── lógica del asistente ── */
  const slug = s => "c:" + String(s || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "").slice(0, 40);
  const numV = v => { if (v == null || v === "") return null; if (typeof v === "number") return isFinite(v) ? v : null; let t = String(v).trim().replace(/[$%\s]/g, ""); if (/^-?\d+,\d+$/.test(t)) t = t.replace(",", "."); else t = t.replace(/,/g, ""); const n = Number(t); return t !== "" && isFinite(n) ? n : null; };
  function leaflet(cb) { if (window.L) return cb(); if (window._rsvEnsureLeaflet_) return window._rsvEnsureLeaflet_(cb); const c = document.createElement("link"); c.rel = "stylesheet"; c.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"; document.head.appendChild(c); const s = document.createElement("script"); s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"; s.onload = cb; document.head.appendChild(s); }
  function cargaScript(src) { return new Promise((ok, mal) => { const s = document.createElement("script"); s.src = src; s.onload = ok; s.onerror = () => mal(new Error("No se pudo cargar una biblioteca necesaria.")); document.head.appendChild(s); }); }
  function wzRe() { const W = S.wiz; if (!W) return; const b = $("#wzBody"); if (b) { b.innerHTML = wzPaso(W); wzPost(W); } const m = $("#wzMsg"); if (m) m.textContent = W.msg || ""; }
  /* mapa de las unidades del DENUE enlazado con la tabla: clic en un renglón → resalta y acerca el mapa; clic en un pin → resalta y recorre la tabla */
  const ICO = { pin: () => L.divIcon({ className: "mo-pin", html: "<i></i>", iconSize: [22, 28], iconAnchor: [11, 28] }), on: () => L.divIcon({ className: "mo-pin on", html: "<i></i>", iconSize: [32, 40], iconAnchor: [16, 40] }) };
  function wzDenFoco(W, k, o) {
    const D = W.den; o = o || {}; if (!D.res || !D.res.items[k]) return; D.foco = k; const it = D.res.items[k], tb = $(".mo-tsc");
    if (k >= D.vista) { D.vista = Math.ceil((k + 1) / 200) * 200; D.keep = true; D.focoPend = { k, zoom: o.zoom }; wzRe(); return; }   /* el renglón aún no estaba en la tabla */
    $$(".mo-trs.foco").forEach(r => r.classList.remove("foco")); const tr = $('.mo-trs[data-wzrow="' + k + '"]'); if (tr) { tr.classList.add("foco"); if (tb && o.scroll !== false) { const top = tr.offsetTop - tb.clientHeight / 2 + tr.clientHeight / 2; tb.scrollTo({ top: Math.max(0, top), behavior: "smooth" }); } }
    const m = D.gl; if (!m || !window.L) return; if (D.mkOn) { const a = D.mk && D.mk[D.mkOn.k]; if (a && D.mkOn.pin) a.setIcon(ICO.pin()); else if (a && a.setStyle) a.setStyle({ radius: 5, fillColor: "#e11d48" }); D.mkOn = null; } if (D.tmp) { m.removeLayer(D.tmp); D.tmp = null; }
    const mk = D.mk && D.mk[k];
    if (mk) { if (mk.setIcon) { mk.setIcon(ICO.on()); mk.setZIndexOffset(1000); D.mkOn = { k, pin: true }; } else { mk.setStyle({ radius: 9, fillColor: "#f59e0b" }); mk.bringToFront(); D.mkOn = { k, pin: false }; } mk.openTooltip && mk.openTooltip(); }
    else { D.tmp = L.circleMarker([it.lat, it.lng], { radius: 9, color: "#fff", weight: 2, fillColor: "#f59e0b", fillOpacity: 1 }).bindTooltip(esc(it.nombre), { permanent: true, direction: "top", offset: [0, -8], className: "mo-etq" }).addTo(m); }
    if (o.zoom !== false) m.flyTo([it.lat, it.lng], Math.max(m.getZoom(), 17), { duration: .6 });
  }
  function wzDenMapa(W) {
    const D = W.den; if (!D.res) return;
    leaflet(() => { const el = $("#wzDMap"); if (!el || !window.L) return; if (D.gl) { try { D.view = { c: D.gl.getCenter(), z: D.gl.getZoom() }; D.gl.remove(); } catch (_) { } }
      const m = L.map(el); L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 19, maxNativeZoom: 16, attribution: "Esri" }).addTo(m); D.gl = m; D.mk = {}; D.mkOn = null; D.tmp = null;
      const sel = []; D.res.items.forEach((_, k) => { if (D.sel.has(k) && sel.length < 1500) sel.push(k); }); const grp = L.featureGroup().addTo(m), pines = sel.length <= 600, fija = sel.length <= 100, off = pines ? [0, -26] : [0, -5];
      const tt = (mk, nom, perm) => mk.bindTooltip(esc(nom), { permanent: perm, direction: "top", offset: off, className: "mo-etq" });
      sel.forEach(k => { const i = D.res.items[k], mk = pines ? L.marker([i.lat, i.lng], { icon: ICO.pin() }) : L.circleMarker([i.lat, i.lng], { radius: 5, color: "#fff", weight: 1, fillColor: "#e11d48", fillOpacity: .9 }); mk._nm = i.nombre; tt(mk, i.nombre, fija); mk.on("click", () => wzDenFoco(W, k, { zoom: false })); mk.addTo(grp); D.mk[k] = mk; });
      if (sel.length) { if (D.keep && D.view) m.setView(D.view.c, D.view.z); else m.fitBounds(grp.getBounds(), { padding: [30, 30], maxZoom: 16 }); } else m.setView([23.8, -102.3], 5); D.keep = false;
      if (!fija && sel.length) { const etiq = () => { const cerca = m.getZoom() >= 16, b = m.getBounds().pad(.1); let n = 0; grp.eachLayer(l => { const dentro = cerca && n < 120 && b.contains(l.getLatLng()); if (dentro) n++; if (!!l._perm !== dentro) { l.unbindTooltip(); tt(l, l._nm, dentro); l._perm = dentro; } }); }; m.on("zoomend moveend", etiq); etiq(); }
      if (D.focoPend) { const p = D.focoPend; D.focoPend = null; wzDenFoco(W, p.k, { zoom: p.zoom }); } else if (D.foco != null && D.res.items[D.foco]) { const k = D.foco; D.foco = null; wzDenFoco(W, k, { zoom: false, scroll: false }); } });
  }
  const wzMapaPts = W => W.puntos.length ? `<div class="mo-fa">${W.paso === 3 ? "Tus puntos de interés y el radio de la maqueta" : "Tus puntos de interés"}${W.puntos.length > 1500 ? ` <span class="mo-mut">· se muestran los primeros 1,500</span>` : ""}</div><div id="wzMap3" class="mo-map"></div>` : '<div class="mo-nota">Aún no hay puntos: vuelve al paso 2 para elegirlos.</div>';
  function wzRadios(W) { if (!W.capa3 || !window.L) return; W.capa3.clearLayers(); if (W.paso !== 3) return; W.puntos.slice(0, 1500).forEach(q => L.circle([q.lat, q.lng], { radius: W.radio, color: "#4f46e5", weight: 1.5, fillColor: "#818cf8", fillOpacity: .15, interactive: false }).addTo(W.capa3)); }
  function wzMapaPuntos(W) {
    leaflet(() => { const el = $("#wzMap3"); if (!el || !window.L) return; if (W.gl3) { try { W.gl3.remove(); } catch (_) { } } const m = L.map(el); L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 19, maxNativeZoom: 16, attribution: "Esri" }).addTo(m); W.gl3 = m;
      const g = L.featureGroup().addTo(m); W.capa3 = L.featureGroup().addTo(m); const pts = W.puntos.slice(0, 1500), perm = pts.length <= 30;
      pts.forEach((q, i) => L.circleMarker([q.lat, q.lng], { radius: 6, color: "#fff", weight: 1.5, fillColor: "#e11d48", fillOpacity: .95 }).bindTooltip(esc(q.id || "P" + (i + 1)), { permanent: perm, direction: "top", offset: [0, -5], className: "mo-etq" }).addTo(g)); wzRadios(W);
      if (pts.length) { const b = L.latLngBounds(pts.map(q => [q.lat, q.lng])); m.fitBounds(b.pad(W.paso === 3 ? .5 : .1), { padding: [30, 30], maxZoom: W.paso === 3 && W.radio > 300 ? 15 : 16 }); } });
  }
  function wzPost(W) {
    if ((W.paso === 3 || W.paso === 4) && W.puntos.length) wzMapaPuntos(W);
    if (W.paso === 2 && (W.tab === "archivo" || W.tab === "inmuebles") && W.arch && W.arch.rows && W.arch.rows.length) { wzRowsMas(W.arch, true); const tw = $(".mo-twp"); if (tw && !tw._pg) { tw._pg = 1; tw.addEventListener("scroll", () => { const A = S.wiz && S.wiz.arch; if (A && tw.scrollTop + tw.clientHeight >= tw.scrollHeight - 200 && (A._nr || 0) < visL(A).length) wzRowsMas(A, false); }); } }
    if (W.paso === 2 && W.tab === "denue") wzDenMapa(W);
    if (W.paso === 2 && W.tab === "mapa") wzMapa(W);
    if (W.paso === 2 && W.tab === "denue" && !S.scian) api(R + "/scian").then(j => { S.scian = j; if (S.wiz === W && W.paso === 2 && W.tab === "denue") wzRe(); }).catch(() => { });
  }
  function wzMapa(W) {
    leaflet(() => { const el = $("#wzMap"); if (!el || !window.L) return; if (W.gl) { try { W.gl.remove(); } catch (_) { } }
      const cp = W.sim && S.puntos && S.puntos.length ? { lat: S.puntos.reduce((a, q) => a + +q.lat, 0) / S.puntos.length, lng: S.puntos.reduce((a, q) => a + +q.lng, 0) / S.puntos.length } : null, c = W.puntos[0] || cp || { lat: 23.8, lng: -102.3 }, m = L.map(el).setView([c.lat, c.lng], W.puntos.length ? 13 : cp ? 10 : 5);   /* puntos nuevos: el mapa abre sobre la zona del proyecto */ L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 18, maxNativeZoom: 16, attribution: "Esri" }).addTo(m); W.gl = m; W.capa = L.layerGroup().addTo(m);
      const pinta = () => { W.capa.clearLayers(); W.puntos.slice(0, 500).forEach((q, i) => L.circleMarker([q.lat, q.lng], { radius: 6, color: "#4f46e5", weight: 2, fillColor: "#818cf8", fillOpacity: .9 }).bindTooltip(esc(q.id || "P" + (i + 1))).addTo(W.capa)); }; W.pinta = pinta; pinta();
      m.on("click", e => { if ((W.modoP || "mapa") !== "mapa") { toast("Para agregar puntos con un clic elige la opción «3 · Seleccionar en el mapa»."); return; } if (W.puntos.length >= 200 && W.fuente === "mapa") { toast("En el modo mapa el máximo son 200 puntos; para más, carga un archivo."); return; } if (W.fuente !== "mapa") { W.puntos = []; W.columnas = []; W.fuente = "mapa"; }
        W.puntos.push({ lat: +e.latlng.lat.toFixed(6), lng: +e.latlng.lng.toFixed(6), id: (W.sim ? "Nuevo " : "P") + (W.puntos.length + 1) }); pinta(); wzPts(W); }); });
  }
  /* coordenadas escritas a mano: una pareja «latitud, longitud» por renglón (acepta coma, punto y coma o espacio) */
  function wzCoord(W) {
    const t = ($("#wzCoord") || {}).value || "", nuevos = [], malos = []; t.split(/\n+/).map(x => x.trim()).filter(Boolean).forEach(l => { const m = l.replace(/[°;]/g, " ").match(/-?\d+(?:\.\d+)?/g); const la = m ? +m[0] : NaN, lo = m ? +m[1] : NaN; if (m && m.length >= 2 && Math.abs(la) <= 90 && Math.abs(lo) <= 180 && !(la === 0 && lo === 0)) nuevos.push({ lat: +la.toFixed(6), lng: +lo.toFixed(6) }); else malos.push(l); });
    if (!nuevos.length) { toast("Escribe una pareja «latitud, longitud» válida, por ejemplo 25.6866, -100.3161."); return; }
    if (W.fuente !== "mapa") { W.puntos = []; W.columnas = []; W.fuente = "mapa"; } if (W.puntos.length + nuevos.length > 200) { toast("En el modo mapa el máximo son 200 puntos; para más, carga un archivo."); return; }
    nuevos.forEach(q => { q.id = (W.sim ? "Nuevo " : "P") + (W.puntos.length + 1); W.puntos.push(q); }); if (W.pinta) W.pinta(); wzPts(W); if (W.gl) { try { W.gl.setView([nuevos[0].lat, nuevos[0].lng], Math.max(W.gl.getZoom(), 14)); } catch (_) { } } const ta = $("#wzCoord"); if (ta) ta.value = ""; toast(nuevos.length + " punto" + (nuevos.length === 1 ? "" : "s") + " agregado" + (nuevos.length === 1 ? "" : "s") + (malos.length ? "; " + malos.length + " renglón(es) no se entendieron." : "."), 4000);
  }
  /* buscador de lugares con texto libre dentro del mapa (mismas sugerencias que Geo Lab) */
  let _mbt = null, _mbs = 0;
  function wzMbBusca(W, q) {
    clearTimeout(_mbt); const L = $("#wzMbL"); if (!L) return; q = q.trim(); if (q.length < 3) { L.style.display = "none"; W.mb = null; return; }
    _mbt = setTimeout(async () => { const seq = ++_mbs; if (!W.mbSt) W.mbSt = (crypto.randomUUID ? crypto.randomUUID() : String(Date.now())); L.style.display = ""; L.innerHTML = '<div class="mo-bn">Buscando…</div>'; let r = [];
      try { const j = await api("/geometrika/sugerir?q=" + encodeURIComponent(q) + "&st=" + W.mbSt); r = j.r || []; if (!r.length) { const k = await api("/geometrika/direccion?q=" + encodeURIComponent(q)); r = k.r || []; } } catch (_) { }
      if (seq !== _mbs) return; W.mb = r; L.innerHTML = r.length ? r.slice(0, 8).map((x, i) => `<button type="button" data-wzmb="${i}"><b>${esc(x.n || x.dir || "")}</b><span>${esc(x.dir && x.dir !== x.n ? x.dir : "")}</span></button>`).join("") : '<div class="mo-bn">Sin resultados</div>'; }, 260);
  }
  async function wzMbElige(W, i) {
    let r = (W.mb || [])[i]; if (!r) return; const L = $("#wzMbL"); try { if (r.pid) { const j = await api("/geometrika/lugar?id=" + encodeURIComponent(r.pid) + "&st=" + (W.mbSt || "")); W.mbSt = ""; r = { n: j.n || r.n, lat: j.lat, lng: j.lng }; } } catch (_) { toast("No se pudo ubicar ese lugar."); return; }
    if (L) L.style.display = "none"; const inp = $("#wzMb"); if (inp && r.n) inp.value = r.n; if (!isFinite(r.lat) || !W.gl) return;
    W.gl.setView([r.lat, r.lng], 17); if (W.mbPin) { try { W.mbPin.remove(); } catch (_) { } } W.mbPin = L2marker(W.gl, r.lat, r.lng, r.n);
  }
  const L2marker = (m, la, lo, n) => window.L.marker([la, lo], { title: n || "" }).addTo(m).bindTooltip(esc(n || "Lugar buscado"), { permanent: false });
  async function wzUrl(W) {
    const t = ($("#wzUrl") || {}).value || "", L = t.split(/\n+/).map(x => x.trim()).filter(Boolean); if (!L.length) { toast("Pega un enlace de Google Maps."); return; } if (W.fuente !== "mapa") { W.puntos = []; W.columnas = []; W.fuente = "mapa"; }
    let ok = 0, mal = []; const b = $('[data-wz="url"]'); if (b) { b.disabled = true; b.textContent = "Leyendo enlace…"; }
    for (const u of L) { try { if (W.puntos.length >= 200) break; const j = await api(R + "/gmaps?url=" + encodeURIComponent(u)); W.puntos.push({ lat: j.lat, lng: j.lng, id: j.nombre || (W.sim ? "Nuevo " : "P") + (W.puntos.length + 1) }); ok++; } catch (e) { mal.push(e.message); } }
    if (b) { b.disabled = false; b.textContent = "Agregar desde el enlace"; } if (ok) { wzPts(W); if (W.pinta) W.pinta(); const q = W.puntos[W.puntos.length - 1]; if (W.gl) { try { W.gl.setView([q.lat, q.lng], Math.max(W.gl.getZoom(), 15)); } catch (_) { } } const ta = $("#wzUrl"); if (ta) ta.value = ""; }
    toast(ok ? ok + " punto" + (ok === 1 ? "" : "s") + " agregado" + (ok === 1 ? "" : "s") + (mal.length ? "; " + mal[0] : ".") : mal[0] || "No se pudo leer el enlace.", 6000);
  }
  function wzPts(W) {
    const n = $(".mo-pts b"); if (n) n.textContent = gN(W.puntos.length, 0); const pl = $("#wzPL"); if (pl) pl.innerHTML = wzChips(W);
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
  /* ── filtros de las filas del archivo (arriba de la tabla): por valores, rango numérico o texto; solo las filas que coinciden se usan como puntos ── */
  const FMAXV = 60;
  function wzCS(A, i) {
    A.cs = A.cs || {}; if (A.cs[i]) return A.cs[i]; const m = new Map(); let n = 0, nn = 0, mn = Infinity, mx = -Infinity;
    A.rows.forEach(r => { const t = r[i]; if (t === "" || t == null) return; n++; const x = numV(t); if (x != null) { nn++; if (x < mn) mn = x; if (x > mx) mx = x; } const k = String(t); m.set(k, (m.get(k) || 0) + 1); });
    const uni = m.size, num = n > 0 && nn >= n * .9, tipo = uni <= 20 || (uni <= FMAXV && !num) ? "cat" : num ? "num" : "txt";
    return A.cs[i] = { tipo, uni, n, mn, mx, vals: tipo === "cat" ? [...m].sort((a, b) => num ? numV(a[0]) - numV(b[0]) : b[1] - a[1]) : [] };
  }
  /* filtros rápidos: un control sobre cada columna (valor, rango o texto) */
  const cfFiltros = A => Object.entries(A.cf || {}).map(([i, c]) => { const cs = wzCS(A, +i); return cs.tipo === "cat" ? { i: +i, tipo: "cat", sel: c.sel || new Set() } : cs.tipo === "num" ? { i: +i, tipo: "num", min: c.min == null ? null : c.min, max: c.max == null ? null : c.max } : { i: +i, tipo: "txt", q: c.q || "" }; }).filter(f => fValido(f));
  const cfActivo = (A, i) => { const c = (A.cf || {})[i]; return !!c && cfFiltros({ cf: { [i]: c }, rows: A.rows, cs: A.cs }).length > 0; };
  /* filtro desplegable desde el encabezado de cada columna */
  const wzFhBtn = (A, i) => `<button type="button" class="mo-thf${cfActivo(A, i) ? " on" : ""}" data-wzfh="${i}" title="Filtrar esta columna" aria-label="Filtrar ${esc(A.cols[i])}"><svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M3 5h18l-7 8v6l-4 2v-8z"/></svg></button>`;
  function wzFpopHtml(A, i) {
    const cs = wzCS(A, i), c = (A.cf || {})[i] || {}, n = visN(A);
    const cuerpo = cs.tipo === "cat" ? `<input id="wzFpq" class="mo-in" placeholder="Buscar valor…" autocomplete="off"><div class="mo-fvals mo-fpl" id="wzFpl">${wzFpList(A, i, "")}</div><div class="mo-chs"><button type="button" class="mo-ghost" data-wzfpall="1">Marcar visibles</button></div>`
      : cs.tipo === "num" ? `<div class="mo-fin2"><label>Mínimo<input id="wzFpmin" class="mo-in" inputmode="decimal" placeholder="${esc(gN(cs.mn))}" value="${c.min == null ? "" : c.min}"></label><label>Máximo<input id="wzFpmax" class="mo-in" inputmode="decimal" placeholder="${esc(gN(cs.mx))}" value="${c.max == null ? "" : c.max}"></label></div><p class="mo-mut">Rango en el archivo: ${gN(cs.mn)} a ${gN(cs.mx)}</p>`
      : `<label class="mo-fl">La columna contiene<input id="wzFpt" class="mo-in" placeholder="Texto a buscar" value="${esc(c.q || "")}" autocomplete="off"></label>`;
    return `<div class="mo-fph"><b>${esc(A.cols[i])}</b><button type="button" data-wzfpx="1" aria-label="Cerrar">×</button></div>${cuerpo}<div class="mo-fpf"><span class="mo-mut" id="wzFpn"><b>${gN(n, 0)}</b> filas coinciden</span><button type="button" class="mo-ghost" data-wzfpclr="1">Limpiar</button></div>`;
  }
  const wzFpList = (A, i, vq) => { const cs = wzCS(A, i), sel = ((A.cf || {})[i] || {}).sel || new Set(), q = (vq || "").toLowerCase(); return cs.vals.map((v, k) => [v, k]).filter(([v]) => !q || v[0].toLowerCase().includes(q)).slice(0, 300).map(([v, k]) => `<button type="button" class="mo-c${sel.has(v[0]) ? " on" : ""}" data-wzfpv="${k}">${ck(sel.has(v[0]))} ${esc(v[0] === "" ? "(vacío)" : v[0])} <small>${gN(v[1], 0)}</small></button>`).join("") || '<span class="mo-mut">Sin valores.</span>'; };
  function wzFiltCierra() { const p = $("#wzFpop"); if (p) p.remove(); document.removeEventListener("keydown", wzFiltEsc); }
  const wzFiltEsc = e => { if (e.key === "Escape") wzFiltCierra(); };
  function wzFiltAbre(W, i, btn) {
    const A = W.arch; const ya = $("#wzFpop"); const mismo = ya && +ya.dataset.i === i; wzFiltCierra(); if (mismo) return; A.cf = A.cf || {};
    const el = document.createElement("div"); el.id = "wzFpop"; el.className = "mo-fpop"; el.dataset.i = i; el.innerHTML = wzFpopHtml(A, i); $("#moApp").appendChild(el);
    const r = btn.getBoundingClientRect(), w = el.offsetWidth; el.style.left = Math.max(8, Math.min(window.innerWidth - w - 8, r.left - 8)) + "px"; el.style.top = Math.min(window.innerHeight - el.offsetHeight - 8, r.bottom + 6) + "px";
    document.addEventListener("keydown", wzFiltEsc); const tw = $(".mo-twp"); if (tw) tw.addEventListener("scroll", wzFiltCierra, { once: true }); const q = el.querySelector("input"); if (q) q.focus();
  }
  /* actualiza solo las filas y los conteos (los controles conservan el foco) */
  let _fiT = null;
  /* selección por renglón: por omisión todas; al aplicar filtros se parte de ninguna y se marca a mano (o con «Seleccionar todos»); se usan las marcadas que cumplen los filtros */
  const visL = A => { const v = A._vis !== undefined ? A._vis : (A._vis = visIdx(A)); return v || (A._all || (A._all = A.rows.map((_, k) => k))); };
  const selN = A => { let n = 0; visL(A).forEach(k => { if (A.sel.has(k)) n++; }); return n; };
  function wzSelSync(A) { const has = (A.f || []).length + cfFiltros(A).length > 0; if (!A._hadF && has) A.sel = new Set(); else if (A._hadF && !has) A.sel = new Set(A.rows.map((_, k) => k)); A._hadF = has; }
  const wzRowHtml = (A, k) => { const r = A.rows[k], on = A.sel.has(k); return `<tr data-wzrow2="${k}" class="${on ? "sel" : ""}"><td class="mo-rck"><span class="mo-ck${on ? " on" : ""}" data-wzr="${k}" role="checkbox" aria-checked="${on}">${on ? "✓" : ""}</span></td>${A.cols.map((_, i) => `<td>${esc(r[i])}</td>`).join("")}</tr>`; };
  function wzRowsMas(A, reinicia) {
    const tb = $("#wzTb"); if (!tb) return; if (reinicia) { tb.innerHTML = ""; A._nr = 0; const tw = $(".mo-twp"); if (tw) tw.scrollTop = 0; } const L = visL(A), a = A._nr || 0, b = Math.min(L.length, a + 100);
    if (b > a) tb.insertAdjacentHTML("beforeend", L.slice(a, b).map(k => wzRowHtml(A, k)).join("")); A._nr = b; if (!L.length) tb.innerHTML = `<tr><td colspan="${A.cols.length + 1}" class="mo-mut">Ninguna fila coincide con los filtros.</td></tr>`;
    const st = $("#wzTbSt"); if (st) st.innerHTML = `Mostrando <b>${gN(b, 0)}</b> de <b>${gN(L.length, 0)}</b> filas${b < L.length ? ` · faltan <b>${gN(L.length - b, 0)}</b> por cargar (se cargan solas al llegar al final de la tabla)` : " · todas cargadas"}`; wzSelUI(A);
  }
  function wzSelUI(A) { const n = selN(A), tot = visL(A).length, bt = $('[data-wz="usaarch"]'); if (bt) { bt.textContent = `Usar ${gN(n, 0)} filas como puntos`; bt.disabled = !n; } const c = $("#wzSelC"); if (c) c.innerHTML = `<b>${gN(n, 0)}</b> seleccionadas de ${gN(tot, 0)} visibles`; const h = $("#wzRall"); if (h) { const on = tot > 0 && n === tot; h.classList.toggle("on", on); h.textContent = on ? "✓" : n ? "–" : ""; } }
  function wzFiltRefresca(W) { const A = W.arch; if (!A) return; A._vis = undefined; wzSelSync(A); const el = $("#wzFilt"); if (el) el.outerHTML = wzFiltHtml(A); $$("[data-wzfh]").forEach(b => b.classList.toggle("on", cfActivo(A, +b.dataset.wzfh))); wzRowsMas(A, true); }
  const fOk = (f, r) => { const t = r[f.i], v = t == null ? "" : String(t); if (f.tipo === "cat") return f.sel.has(v); if (f.tipo === "num") { const x = numV(t); return x != null && (f.min == null || x >= f.min) && (f.max == null || x <= f.max); } return v.toLowerCase().includes(String(f.q || "").toLowerCase()); };
  const fValido = f => f.tipo === "cat" ? f.sel.size > 0 : f.tipo === "num" ? (f.min != null || f.max != null) : !!String(f.q || "").trim();
  function visIdx(A, extra) { const fs = (A.f || []).concat(cfFiltros(A), extra && fValido(extra) ? [extra] : []); if (!fs.length) return null; const o = []; A.rows.forEach((r, k) => { if (fs.every(f => fOk(f, r))) o.push(k); }); return o; }
  const filasVis = A => { const v = A._vis !== undefined ? A._vis : (A._vis = visIdx(A)); return v ? v.map(k => A.rows[k]) : A.rows; }, visN = A => { const v = A._vis !== undefined ? A._vis : (A._vis = visIdx(A)); return v ? v.length : A.rows.length; };
  const fDesc = (A, f) => f.tipo === "cat" ? [...f.sel].slice(0, 3).join(", ") + (f.sel.size > 3 ? " +" + (f.sel.size - 3) : "") : f.tipo === "num" ? (f.min != null ? "≥ " + gN(f.min) : "") + (f.min != null && f.max != null ? " y " : "") + (f.max != null ? "≤ " + gN(f.max) : "") : "contiene «" + f.q + "»";
  function wzFiltHtml(A) {
    const n = visL(A).length, nAll = A.rows.length, hay = (A.f || []).length || cfFiltros(A).length;
    return `<div class="mo-fbar" id="wzFilt"><div class="mo-fbh"><span class="mo-mut"><b>${gN(n, 0)}</b> de ${gN(nAll, 0)} filas coinciden${hay ? "" : " · usa el embudo del encabezado de cada columna para filtrar"} · <span id="wzSelC"><b>${gN(selN(A), 0)}</b> seleccionadas de ${gN(n, 0)} visibles</span></span><span class="mo-sp"></span><span class="mo-fgrp"><em>Filas</em><button type="button" class="mo-ghost" data-wzsa="1">Seleccionar todas</button><button type="button" class="mo-ghost" data-wzsa="0">Ninguna</button></span><span class="mo-fgrp"><em>Columnas</em><button type="button" class="mo-ghost" data-wzex="todas">Seleccionar todas</button><button type="button" class="mo-ghost" data-wzex="ninguna">Ninguna</button></span>${hay ? '<button type="button" class="mo-ghost" data-wzfclear="1">Quitar filtros</button>' : ""}</div></div>`;
  }
  const wzFvals = (A, fe) => { const cs = wzCS(A, fe.i), vq = (fe.vq || "").toLowerCase(), L = cs.vals.map((v, k) => [v, k]).filter(([v]) => !vq || v[0].toLowerCase().includes(vq)).slice(0, 300); return L.map(([v, k]) => `<button type="button" class="mo-c${fe.sel.has(v[0]) ? " on" : ""}" data-wzfv="${k}">${ck(fe.sel.has(v[0]))} ${esc(v[0] === "" ? "(vacío)" : v[0])} <small>${gN(v[1], 0)}</small></button>`).join("") || '<span class="mo-mut">Sin valores.</span>'; };
  function wzFiltNuevo(A, i) { const cs = wzCS(A, i); return { i, tipo: cs.tipo, sel: new Set(), min: null, max: null, q: "", vq: "" }; }
  function wzFiltRe(W) { const A = W.arch, el = $("#wzFilt"); if (!el) return wzRe(); const t = $(".mo-twp"), sl = t ? t.scrollLeft : 0, y = $("#moMain").scrollTop; el.outerHTML = wzFiltHtml(A); const t2 = $(".mo-twp"); if (t2) t2.scrollLeft = sl; $("#moMain").scrollTop = y; }
  function wzFiltAplica(W) { A_ = W.arch; A_._vis = undefined; wzSelSync(A_); const t = $(".mo-twp"), sl = t ? t.scrollLeft : 0, y = $("#moMain").scrollTop; wzRe(); const t2 = $(".mo-twp"); if (t2) t2.scrollLeft = sl; $("#moMain").scrollTop = y; }
  let A_ = null;
  async function wzArchivo(file) {
    const W = S.wiz; W.arch = { nombre: file.name, cols: [], rows: [], ilat: -1, ilng: -1, iid: -1, iy: -1, extra: [] }; const ext = (file.name.split(".").pop() || "").toLowerCase();
    try { let tabla;
      if (ext === "zip" || ext === "shp") { if (ext === "shp") throw new Error("Sube el shapefile completo en un .zip (con sus archivos .shp, .dbf y .prj)."); if (!window.shp) await cargaScript("https://unpkg.com/shpjs@4.0.4/dist/shp.min.js"); let g = await window.shp(await file.arrayBuffer()); if (Array.isArray(g)) g = g[0]; tabla = geoATabla(g); }
      else if (ext === "geojson" || ext === "json") { const g = JSON.parse(await file.text()); tabla = geoATabla(g.type === "Feature" ? { features: [g] } : g); }
      else { const r = parseCSV(await file.text()); if (r.length < 2) throw new Error("El archivo no tiene filas de datos."); tabla = { cols: r[0].map((c, i) => String(c).trim() || "columna_" + (i + 1)), rows: r.slice(1) }; }
      if (!tabla.rows.length) throw new Error("No se encontraron filas con coordenadas."); if (tabla.rows.length > 20000) { tabla.rows = tabla.rows.slice(0, 20000); toast("Se usarán las primeras 20 000 filas."); }
      const [ilat, ilng] = detCoord(tabla.cols, tabla.rows); Object.assign(W.arch, tabla, { sel: new Set(tabla.rows.map((_, k) => k)), _hadF: false, ilat, ilng, iid: tabla.cols.findIndex(c => /^(id|nombre|name|clave|folio|sucursal|razon_social|nom_estab)$/i.test(String(c).trim())) });
    } catch (e) { W.arch.err = "No se pudo leer el archivo: " + e.message; } wzRe();
  }
  function usaArchivo(W) {
    const A = W.arch; if (A.ilat < 0 || A.ilng < 0) { W.msg = "Elige las columnas de latitud y longitud."; return wzRe(); }
    if (A.iy >= 0 && !A.extra.includes(A.iy)) A.extra.push(A.iy); W.yVar = A.iy >= 0 && A.iy !== A.ilat && A.iy !== A.ilng ? slug(A.cols[A.iy]) : null; const ex = A.extra.filter(i => i !== A.ilat && i !== A.ilng), cols = ex.map(i => { const v = A.rows.map(r => numV(r[i])).filter(x => x != null); const tipo = v.length >= A.rows.filter(r => r[i] !== "" && r[i] != null).length * .9 && v.length ? "num" : "cat"; return { name: slug(A.cols[i]), label: String(A.cols[i]), tipo }; });
    const puntos = []; visL(A).filter(k => A.sel.has(k)).forEach(k => { const r = A.rows[k], lat = numV(r[A.ilat]), lng = numV(r[A.ilng]); if (lat == null || lng == null || Math.abs(lat) > 90 || Math.abs(lng) > 180) return; const q = { lat, lng, id: A.iid >= 0 && r[A.iid] != null && r[A.iid] !== "" ? String(r[A.iid]).slice(0, 80) : "F" + (k + 1), x: {} }; ex.forEach((i, j) => { q.x[cols[j].name] = cols[j].tipo === "num" ? numV(r[i]) : (r[i] == null || r[i] === "" ? null : String(r[i]).slice(0, 120)); }); puntos.push(q); });
    if (!puntos.length) { W.msg = "Ninguna fila tiene coordenadas válidas."; return wzRe(); } W.puntos = puntos; W.columnas = cols; W.fuente = A.inm ? "inmuebles" : "archivo"; if (A.inm) W.yVar = null; W.msg = puntos.length < selN(A) ? `Se omitieron ${selN(A) - puntos.length} filas sin coordenadas válidas.` : ""; wzRe(); toast(`${gN(puntos.length, 0)} puntos cargados.`);
  }
  async function denBusca(W) {
    const D = W.den; if (!D.ciudades.length) return; D.cargando = true; wzRe(); const cod = D.cod.split(/[,\s]+/).filter(c => /^\d{4,6}$/.test(c)), scian = cod.length ? cod : D.sub ? [D.sub] : D.sector ? [D.sector] : [];
    try { D.res = await api(R + "/denue", { muns: D.muns, q: D.q, scian, tmin: D.tmin, tmax: D.tmax, limite: 5000 }); D.sel = new Set(D.res.items.map((_, k) => k)); D.vista = 200; } catch (e) { W.msg = e.message; D.res = null; } D.cargando = false; wzRe();
  }
  function usaDenue(W) {
    const r = W.den.res; if (!r || !r.items.length) return; const GN = r.grupos || [], elegidas = r.items.filter((_, k) => W.den.sel.has(k)); if (!elegidas.length) { W.msg = "Selecciona al menos una unidad."; return wzRe(); }
    W.puntos = elegidas.map(i => ({ lat: i.lat, lng: i.lng, id: i.nombre, x: { tam: i.tam, sector: i.scian.slice(0, 2), subsector: i.scian.slice(0, 3), rama: i.scian.slice(0, 4), grupo: GN[i.grupo] || null } }));
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
    const c = costo(W); try { S.busy = true; const j = await api(R + "/proyectos", { id: W.editId || undefined, nombre: W.nombre.trim(), objetivo: W.objetivo.trim(), fuente: W.fuente, puntos: W.puntos, columnas: W.columnas, mapa: W.mapa, cfg: { odOn: W.odOn, odModo: W.odModo, yVar: W.yVar || null, radio: W.radio, fotos: W.fotos, vias: W.vias, pre: W.pre, dias: W.dias, horas: W.horas, opt: W.opt, costoEstimadoUSD: Math.round(c.total * 100) / 100 } });
      S.wiz = null; api("/geometrika/modelab-precalcular", { id: j.proyecto.id, od: { on: W.odOn, modo: W.odModo } }).catch(() => { }); await cargaProy(); await abre(j.proyecto.id, "principal"); toast(W.editId ? "Proyecto actualizado." : "Proyecto creado."); } catch (e) { S.busy = false; W.msg = e.message; wizard(); }
  }
  async function editar(id) {
    try { const j = await api(R + "/proyectos/" + id), p = j.proyecto, W = wizVacio(), c = p.cfg || {}; Object.assign(W, { nombre: p.nombre, objetivo: p.objetivo || "", fuente: p.fuente || "mapa", tab: p.fuente === "denue" ? "denue" : p.fuente === "archivo" ? "archivo" : p.fuente === "inmuebles" ? "inmuebles" : "mapa", puntos: j.puntos || [], columnas: p.columnas || [], mapa: p.mapa || null, editId: id, radio: c.radio || 200, fotos: c.fotos || 24, vias: c.vias || W.vias, pre: c.pre || 12, dias: c.dias || W.dias, horas: c.horas || W.horas, opt: Object.assign(W.opt, c.opt || {}), yVar: c.yVar || null, odOn: c.odOn !== false, odModo: c.odModo === "google" ? "google" : "osm" }); S.wiz = W; wizard(); }
    catch (e) { toast(e.message); }
  }
  function wizClic(e) {
    const W = S.wiz, t = e.target; let b;
    if ((b = t.closest("[data-wz]"))) { const a = b.dataset.wz;
      if (a === "atras") { if (W.paso === (W.sim ? 2 : 1)) { S.wiz = null; return ir(W.sim ? "simulacion" : "proyectos"), true; } W.paso--; W.msg = ""; wizard(); return true; }
      if (a === "sig") { if (W.paso === 1 && !W.nombre.trim()) { W.msg = "Escribe el nombre del proyecto."; wzRe(); return true; } if (W.paso === 2 && W.sim && !W.nombre.trim()) { W.msg = "Escribe el nombre del conjunto."; wzRe(); return true; } if (W.paso === 2 && !W.puntos.length) { W.msg = W.sim ? "Agrega al menos un punto nuevo." : "Elige al menos un punto de interés."; wzRe(); return true; } W.paso++; W.msg = ""; wizard(); return true; }
      if (a === "modelar") { wzModelar(W); return true; } if (a === "simular") { wzSimular(W); return true; } if (a === "coord") { wzCoord(W); return true; } if (a === "url") { wzUrl(W); return true; } if (a === "limpia") { W.puntos = []; W.columnas = []; W.fuente = W.tab === "denue" ? "denue" : W.tab; if (W.pinta) W.pinta(); wzRe(); return true; }
      if (a === "geolab") { wzGeoLab(W); return true; } if (a === "usaarch") { usaArchivo(W); return true; } if (a === "denbusca") { denBusca(W); return true; } if (a === "usaden") { usaDenue(W); return true; } }
    if ((b = t.closest("[data-wzmb]"))) { wzMbElige(W, +b.dataset.wzmb); return true; }
    if ((b = t.closest("[data-wzmodo]"))) { W.modoP = b.dataset.wzmodo; wzRe(); return true; }
    { const pop = $("#wzFpop"); if (pop && !t.closest("#wzFpop") && !t.closest("[data-wzfh]")) wzFiltCierra(); }
    if ((b = t.closest("[data-wzr]")) && W.arch) { const A = W.arch, k = +b.dataset.wzr, on = !A.sel.has(k); on ? A.sel.add(k) : A.sel.delete(k); b.classList.toggle("on", on); b.textContent = on ? "✓" : ""; b.setAttribute("aria-checked", on); const tr = b.closest("tr"); if (tr) tr.classList.toggle("sel", on); wzSelUI(A); return true; }
    if ((b = t.closest("[data-wzrall]")) && W.arch) { const A = W.arch, L = visL(A), todas = L.length > 0 && L.every(k => A.sel.has(k)); L.forEach(k => { todas ? A.sel.delete(k) : A.sel.add(k); }); wzRowsMas(A, false); $$("#wzTb tr[data-wzrow2]").forEach(tr => { const on = A.sel.has(+tr.dataset.wzrow2), c = tr.querySelector("[data-wzr]"); tr.classList.toggle("sel", on); if (c) { c.classList.toggle("on", on); c.textContent = on ? "✓" : ""; } }); wzSelUI(A); return true; }
    if ((b = t.closest("[data-wzsa]")) && W.arch) { const A = W.arch, on = b.dataset.wzsa === "1"; visL(A).forEach(k => { on ? A.sel.add(k) : A.sel.delete(k); }); $$("#wzTb tr[data-wzrow2]").forEach(tr => { const o = A.sel.has(+tr.dataset.wzrow2), c = tr.querySelector("[data-wzr]"); tr.classList.toggle("sel", o); if (c) { c.classList.toggle("on", o); c.textContent = o ? "✓" : ""; } }); wzSelUI(A); return true; }
    if ((b = t.closest("[data-wzfh]")) && W.arch) { e.stopPropagation(); wzFiltAbre(W, +b.dataset.wzfh, b); return true; }
    if ((b = t.closest("[data-wzfpx]"))) { wzFiltCierra(); return true; }
    if ((b = t.closest("[data-wzfpv]")) && W.arch) { const pop = $("#wzFpop"), A = W.arch, i = +pop.dataset.i, v = wzCS(A, i).vals[+b.dataset.wzfpv][0]; A.cf = A.cf || {}; const c = A.cf[i] = A.cf[i] || { sel: new Set() }; if (!c.sel) c.sel = new Set(); c.sel.has(v) ? c.sel.delete(v) : c.sel.add(v); if (!c.sel.size) delete A.cf[i]; const on = !!(A.cf[i] && A.cf[i].sel.has(v)), k = b.querySelector(".mo-ck"); b.classList.toggle("on", on); if (k) { k.classList.toggle("on", on); k.textContent = on ? "✓" : ""; } wzFiltRefresca(W); const n = $("#wzFpn"); if (n) n.innerHTML = `<b>${gN(visN(A), 0)}</b> filas coinciden`; return true; }
    if ((b = t.closest("[data-wzfpall]")) && W.arch) { const pop = $("#wzFpop"), A = W.arch, i = +pop.dataset.i, vq = ($("#wzFpq") || {}).value || "", q = vq.toLowerCase(); A.cf = A.cf || {}; const c = A.cf[i] = A.cf[i] || { sel: new Set() }; wzCS(A, i).vals.forEach(v => { if (!q || v[0].toLowerCase().includes(q)) c.sel.add(v[0]); }); $("#wzFpl").innerHTML = wzFpList(A, i, vq); wzFiltRefresca(W); const n = $("#wzFpn"); if (n) n.innerHTML = `<b>${gN(visN(A), 0)}</b> filas coinciden`; return true; }
    if ((b = t.closest("[data-wzfpclr]")) && W.arch) { const pop = $("#wzFpop"), A = W.arch, i = +pop.dataset.i; if (A.cf) delete A.cf[i]; pop.innerHTML = wzFpopHtml(A, i); wzFiltRefresca(W); return true; }
    if ((b = t.closest("[data-wzfadd]")) && W.arch) { const A = W.arch, i0 = A.cols.findIndex((_, i) => i !== A.ilat && i !== A.ilng); A.fe = wzFiltNuevo(A, Math.max(0, i0)); wzFiltRe(W); return true; }
    if ((b = t.closest("[data-wzfno]")) && W.arch) { W.arch.fe = null; wzFiltRe(W); return true; }
    if ((b = t.closest("[data-wzfok]")) && W.arch) { const A = W.arch; if (A.fe && fValido(A.fe)) { A.f.push(A.fe); } A.fe = null; wzFiltAplica(W); return true; }
    if ((b = t.closest("[data-wzfrm]")) && W.arch) { W.arch.f.splice(+b.dataset.wzfrm, 1); wzFiltAplica(W); return true; }
    if ((b = t.closest("[data-wzfclear]")) && W.arch) { W.arch.f = []; W.arch.cf = {}; W.arch.fe = null; wzFiltAplica(W); return true; }
    if ((b = t.closest("[data-wzfv]")) && W.arch && W.arch.fe) { const A = W.arch, fe = A.fe, v = wzCS(A, fe.i).vals[+b.dataset.wzfv][0]; fe.sel.has(v) ? fe.sel.delete(v) : fe.sel.add(v); const on = fe.sel.has(v), k = b.querySelector(".mo-ck"); b.classList.toggle("on", on); if (k) { k.classList.toggle("on", on); k.textContent = on ? "✓" : ""; } const n = $("#wzFn"), pre = visIdx(A, fe); if (n) n.textContent = gN(pre ? pre.length : A.rows.length, 0) + " filas coinciden"; return true; }
    if ((b = t.closest("[data-wzfall],[data-wzfnone]")) && W.arch && W.arch.fe) { const A = W.arch, fe = A.fe; if (b.dataset.wzfall) { const vq = (fe.vq || "").toLowerCase(); wzCS(A, fe.i).vals.forEach(v => { if (!vq || v[0].toLowerCase().includes(vq)) fe.sel.add(v[0]); }); } else fe.sel.clear(); wzFiltRe(W); return true; }
    if ((b = t.closest("[data-wzinm]"))) { if (!b.disabled) wzInmCarga(W, b.dataset.wzinm); return true; }
    if ((b = t.closest("[data-wzt]"))) { W.tab = b.dataset.wzt; W.msg = ""; wzRe(); const bar = $(".mo-tabs"); if (bar) bar.outerHTML = bar.outerHTML; return true; }
    if ((b = t.closest("[data-wzod]"))) { const k = b.dataset.wzod; if (k === "on") W.odOn = !W.odOn; else W.odModo = k; W.opt.od = !!(W.odOn && W.odModo === "google"); wzRe(); return true; }
    if ((b = t.closest("[data-wzopt]"))) { const k = b.dataset.wzopt; W.opt[k] = !W.opt[k]; wzRe(); return true; }
    if ((b = t.closest("[data-wzrad]"))) { W.radio = +b.dataset.wzrad; wzRe(); return true; }
    if ((b = t.closest("[data-wzvia]"))) { const k = b.dataset.wzvia; W.vias = W.vias.includes(k) ? W.vias.filter(x => x !== k) : W.vias.concat(k); wzRe(); return true; }
    if ((b = t.closest("[data-wzdia]"))) { const k = +b.dataset.wzdia; W.dias = W.dias.includes(k) ? W.dias.filter(x => x !== k) : W.dias.concat(k); wzRe(); return true; }
    if ((b = t.closest("[data-wzhor]"))) { const k = +b.dataset.wzhor; W.horas = W.horas.includes(k) ? W.horas.filter(x => x !== k) : W.horas.concat(k); wzRe(); return true; }
    if ((b = t.closest("[data-wzq]"))) { const q = b.dataset.wzq; if (q === "pico") W.horas = [7, 8, 9, 17, 18, 19]; if (q === "habil") W.dias = [1, 2, 3, 4, 5]; if (q === "finde") W.dias = [6, 0]; if (q === "todo") W.horas = Array.from({ length: 24 }, (_, h) => h); wzRe(); return true; }
    if ((b = t.closest("[data-wzrm]"))) { W.puntos.splice(+b.dataset.wzrm, 1); if (W.pinta) W.pinta(); wzPts(W); return true; }
    if ((b = t.closest("[data-wzex]"))) { const v = b.dataset.wzex, A = W.arch; if (v === "todas") A.extra = A.cols.map((_, i) => i).filter(i => i !== A.ilat && i !== A.ilng && i !== A.iid); else if (v === "ninguna") A.extra = A.iy >= 0 ? [A.iy] : []; else { const i = +v; A.extra = A.extra.includes(i) ? A.extra.filter(x => x !== i) : A.extra.concat(i); const on = A.extra.includes(i), th = b.closest("th"); if (th) { th.classList.toggle("on", on); const k = th.querySelector(".mo-ck"); if (k) { k.classList.toggle("on", on); k.textContent = on ? "✓" : ""; } const c = $("#wzExN"); if (c) c.textContent = A.extra.length; return true; } } const sc = $(".mo-twp"), sl = sc ? sc.scrollLeft : 0; wzRe(); const sc2 = $(".mo-twp"); if (sc2) sc2.scrollLeft = sl; return true; }
    if ((b = t.closest("[data-wzcx]"))) { W.den.ciudades.splice(+b.dataset.wzcx, 1); W.den.muns = [...new Set(W.den.ciudades.flatMap(c => c.muns))]; W.den.res = null; wzRe(); return true; }
    if (!t.closest("[data-wzds]") && (b = t.closest("[data-wzrow]"))) { wzDenFoco(W, +b.dataset.wzrow); return true; }
    if ((b = t.closest("[data-wzds]"))) { const D = W.den, v = b.dataset.wzds, n = D.res ? D.res.items.length : 0;
      if (v === "todas") D.sel = new Set(Array.from({ length: n }, (_, k) => k)); else if (v === "ninguna") D.sel = new Set(); else if (v === "mas") D.vista += 200; else { const k = +v; D.sel.has(k) ? D.sel.delete(k) : D.sel.add(k); }
      D.keep = false; const y = $("#moMain").scrollTop, ty = (($(".mo-tsc") || {}).scrollTop) || 0; wzRe(); $("#moMain").scrollTop = y; const tsc = $(".mo-tsc"); if (tsc) tsc.scrollTop = ty; return true; }
    if ((b = t.closest("[data-wzgl]"))) { wzTraeGeo(W, +b.dataset.wzgl); return true; }
    if ((b = t.closest("[data-bli]"))) { const l = b.closest(".mo-bl"), it = (l._it || [])[+b.dataset.bli]; if (it && l._al) l._al(it); l.style.display = "none"; return true; }
    return false;
  }
  function wizEntrada(e) {
    const W = S.wiz, t = e.target, id = t.id; if (!W) return false;
    if (id === "wzNom" || id === "wzNomS") { W.nombre = t.value; return true; } if (id === "wzObj") { W.objetivo = t.value; return true; }
    if (id === "wzRad") { W.radio = +t.value; const v = $("#wzRadV"); if (v) v.textContent = W.radio + " m"; wzRadios(W); return true; } if (id === "wzFot") { W.fotos = +t.value; const v = $("#wzFotV"); if (v) v.textContent = W.fotos; return true; } if (id === "wzPre") { W.pre = +t.value; const v = $("#wzPreV"); if (v) v.textContent = W.pre; return true; }
    if (W.arch && (id === "wzFpq" || id === "wzFpmin" || id === "wzFpmax" || id === "wzFpt")) { const A = W.arch, pop = $("#wzFpop"); if (!pop) return true; const i = +pop.dataset.i; if (id === "wzFpq") { $("#wzFpl").innerHTML = wzFpList(A, i, t.value); return true; }
      A.cf = A.cf || {}; const c = A.cf[i] = A.cf[i] || {}; if (id === "wzFpt") c.q = t.value; else { const x = t.value.trim() === "" ? null : numV(t.value); if (id === "wzFpmin") c.min = x; else c.max = x; } if (!cfActivo(A, i)) delete A.cf[i]; clearTimeout(_fiT); _fiT = setTimeout(() => { wzFiltRefresca(W); const n = $("#wzFpn"); if (n) n.innerHTML = `<b>${gN(visN(A), 0)}</b> filas coinciden`; }, 200); return true; }
    if (W.arch && t.dataset && t.dataset.wzfi != null && t.dataset.k) { const A = W.arch, i = +t.dataset.wzfi, k = t.dataset.k; A.cf = A.cf || {}; const c = A.cf[i] = A.cf[i] || {}; if (k === "q") c.q = t.value; else c[k] = t.value.trim() === "" ? null : numV(t.value); if (!c.q && c.min == null && c.max == null && c.v == null) delete A.cf[i]; clearTimeout(_fiT); _fiT = setTimeout(() => wzFiltRefresca(W), 220); return true; }
    if (W.arch && W.arch.fe && (id === "wzFvq" || id === "wzFq" || id === "wzFmin" || id === "wzFmax")) { const A = W.arch, fe = A.fe;
      if (id === "wzFvq") { fe.vq = t.value; const v = $("#wzFvals"); if (v) v.innerHTML = wzFvals(A, fe); return true; } if (id === "wzFq") fe.q = t.value; else { const x = t.value.trim() === "" ? null : numV(t.value); if (id === "wzFmin") fe.min = x; else fe.max = x; }
      const n = $("#wzFn"), pre = visIdx(A, fe); if (n) n.textContent = gN(pre ? pre.length : A.rows.length, 0) + " filas coinciden"; return true; }
    if (id === "wzQ") { W.den.q = t.value; return true; } if (id === "wzCod") { W.den.cod = t.value; return true; }
    if (id === "wzMb") { wzMbBusca(W, t.value); return true; }
    if (id === "wzBus") { busca(t, $("#wzBL"), "/geometrika/malla/buscar?q=", it => it.t + (it.d ? " · " + it.d : ""), it => { const m = W.gl; if (!m) return; const bb = it.bb; if (bb && bb[2] - bb[0] > 1e-4) m.fitBounds([[bb[1], bb[0]], [bb[3], bb[2]]], { maxZoom: 17 }); else m.setView([it.lat, it.lng], 16); t.value = it.n; }); return true; }
    if (id === "wzCiu") { busca(t, $("#wzCL"), R + "/ciudades?q=", it => it.t + " · " + it.d, it => { if (!W.den.ciudades.some(c => c.n === it.n)) W.den.ciudades.push({ n: it.n, muns: it.muns }); W.den.muns = [...new Set(W.den.ciudades.flatMap(c => c.muns))]; W.den.res = null; wzRe(); }); return true; }
    return false;
  }
  function wizCambia(e) {
    const W = S.wiz, t = e.target, id = t.id; if (!W) return false;
    if (id === "wzFile") { if (t.files[0]) wzArchivo(t.files[0]); return true; } if (id === "wzVer") { W.simVer = t.value; return true; } if (W.arch && t.dataset && t.dataset.wzfi != null && t.tagName === "SELECT") { const A = W.arch, i = +t.dataset.wzfi; A.cf = A.cf || {}; if (t.value === "") delete A.cf[i]; else A.cf[i] = { v: t.value }; wzFiltRefresca(W); return true; } if (id === "wzFcol" && W.arch && W.arch.fe) { W.arch.fe = wzFiltNuevo(W.arch, +t.value); wzFiltRe(W); return true; }
    if (id === "wzLat") { W.arch.ilat = +t.value; wzRe(); return true; } if (id === "wzLng") { W.arch.ilng = +t.value; wzRe(); return true; } if (id === "wzId") { W.arch.iid = +t.value; const sc = $(".mo-twp"), sl = sc ? sc.scrollLeft : 0; wzRe(); const s2 = $(".mo-twp"); if (s2) s2.scrollLeft = sl; return true; } if (id === "wzY") { const A = W.arch; A.iy = +t.value; if (A.iy >= 0 && !A.extra.includes(A.iy)) A.extra.push(A.iy); const sc = $(".mo-twp"), sl = sc ? sc.scrollLeft : 0; wzRe(); const s2 = $(".mo-twp"); if (s2) s2.scrollLeft = sl; return true; }
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
      ${colsP.length ? `<section class="mo-sx2"><h3>Columnas de tu proyecto <small>${colsP.length}</small></h3><p class="mo-mut">Vienen de tu archivo o del DENUE. Pueden ser variables explicativas o la variable de respuesta.</p><div class="mo-chs"><button type="button" class="mo-ghost" data-vg="${esc(colsP.map(c => c.name).join("|"))}" data-on="1">Todas</button><button type="button" class="mo-ghost" data-vg="${esc(colsP.map(c => c.name).join("|"))}" data-on="0">Ninguna</button></div><div class="mo-chs">${colsP.map(c => `<button type="button" class="mo-c${sel.has(c.name) ? " on" : ""}" data-v="${esc(c.name)}">${ck(sel.has(c.name))} ${esc(c.label)} <small>${c.tipo === "num" ? "número" : "categoría"}</small></button>`).join("")}</div></section>` : ""}
      <section class="mo-sx2"><h3>Biblioteca de indicadores <small>${BIB().length} disponibles por coordenada</small></h3><p class="mo-mut">Cada punto se ubica en su AGEB y recibe el valor precalculado de cada indicador (Censo, DENUE, OpenStreetMap, INEGI, WorldCover, WorldClim…). Los puntos fuera de una AGEB urbana quedan sin dato.</p>
        <div class="mo-vb"><label class="mo-q${S.vq ? " has" : ""}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg><input id="moVQ" type="search" placeholder="Buscar indicador…" value="${esc(S.vq || "")}"></label>
        <div class="mo-chs"><button type="button" class="mo-c${cat ? "" : " on"}" data-vcat="">Todas</button>${cats.map(c => `<button type="button" class="mo-c${cat === c ? " on" : ""}" data-vcat="${esc(c)}">${esc(c)}</button>`).join("")}</div></div>
        ${Object.keys(grp).length ? Object.entries(grp).map(([g, L2]) => { const n = L2.filter(b => sel.has(b.k)).length; return `<details class="mo-sx"${q || n ? " open" : ""}><summary><b>${esc(g)}</b><span>${n}/${L2.length} elegidos</span><span class="mo-vgb"><button type="button" class="mo-ghost" data-vg="${esc(L2.map(b => b.k).join("|"))}" data-on="1" title="Elegir todos los indicadores de este grupo">Todos</button><button type="button" class="mo-ghost" data-vg="${esc(L2.map(b => b.k).join("|"))}" data-on="0" title="Quitar todos los de este grupo">Ninguno</button></span></summary><div class="mo-vl">${L2.map(b => `<button type="button" class="mo-vi${sel.has(b.k) ? " on" : ""}" data-v="${esc(b.k)}">${ck(sel.has(b.k))}<span><b>${esc(b.n)}</b><i>${esc(b.u)}${b.cat ? " · categórico" : ""}</i></span></button>`).join("")}</div></details>`; }).join("") : '<div class="mo-empty"><b>Ningún indicador coincide.</b></div>'}</section>
      <div class="mo-resv" id="vRes">${resumenVars()}</div>${pie("datos", "construir", "Construir tabla de datos y continuar", !(M.vars || []).length)}`);
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
  /* origen de cada variable: columna del DENUE, de tu archivo o indicador de la biblioteca */
  const origen = n => { const b = BIBK()[n]; if (b) return { k: "lib", t: "Biblioteca", g: "Biblioteca de indicadores · " + b.c + " › " + b.e, o: 3 }; const f = (S.P && S.P.fuente) || ""; return f === "denue" ? { k: "denue", t: "DENUE", g: "Tu proyecto · unidades del DENUE", o: 0 } : f === "geolab" ? { k: "archivo", t: "Geo Lab", g: "Tu proyecto · Geo Lab", o: 1 } : { k: "archivo", t: "Archivo", g: "Tu proyecto · archivo subido", o: 1 }; };
  const chipOr = n => { const o = origen(n); return `<span class="mo-or ${o.k}">${o.t}</span>`; };
  function opcionesAgrupadas(lista, sel, tipoTxt) { const G = {}; lista.forEach(p => { const o = origen(p.name); (G[o.g] = G[o.g] || { o: o.o, l: [] }).l.push(p); }); return Object.entries(G).sort((a, b) => a[1].o - b[1].o || a[0].localeCompare(b[0])).map(([g, v]) => `<optgroup label="${esc(g)}">${v.l.map(p => `<option value="${esc(p.name)}"${sel === p.name ? " selected" : ""}>${esc(p.label || p.name)} · ${tipoTxt(p)}</option>`).join("")}</optgroup>`).join(""); }
  const perfiles = () => { if (!S.perfiles && S.ds && window.MLMotor) { try { S.perfiles = window.MLMotor.perfilar(S.ds); } catch (e) { toast(e.message); } } return S.perfiles || []; };
  const perfilDe = n => perfiles().find(p => p.name === n);

  /* ── 0 · Datos: la tabla cargada o el conjunto de coordenadas del proyecto ── */
  const datosCols = () => colsProy().map(c => ({ name: c.name, label: c.label || c.name }));
  const datosFilas = () => { const q = ((S.dt && S.dt.q) || "").toLowerCase(), C = datosCols(); return (S.puntos || []).filter(p => !q || String(p.id).toLowerCase().includes(q) || C.some(c => String((p.x || {})[c.name] == null ? "" : p.x[c.name]).toLowerCase().includes(q))); };
  const datosV = v => v == null || v === "" ? '<span class="mo-mut">—</span>' : typeof v === "number" ? esc(gN(v, Math.abs(v) < 1000 && v % 1 ? 2 : 0)) : esc(String(v).slice(0, 80));
  function datosPinta() {
    const tb = $("#moDTb"), L = datosFilas(), n = (S.dt && S.dt.n) || 100, C = datosCols(); if (!tb) return;
    tb.innerHTML = L.slice(0, n).map((p, i) => `<tr><td>${i + 1}</td><td><b>${esc(String(p.id).slice(0, 60))}</b></td><td>${(+p.lat).toFixed(5)}</td><td>${(+p.lng).toFixed(5)}</td>${C.map(c => `<td>${datosV((p.x || {})[c.name])}</td>`).join("")}</tr>`).join("") || `<tr><td colspan="${4 + C.length}" class="mo-mut">Ningún punto coincide con la búsqueda.</td></tr>`;
    const m = $("#moDMas"); if (m) m.innerHTML = L.length > n ? `<span class="mo-mut">Mostrando ${gN(n, 0)} de ${gN(L.length, 0)}.</span><button type="button" class="mo-ghost" data-dmas="1">Mostrar 100 más</button>` : `<span class="mo-mut">${gN(L.length, 0)} punto${L.length === 1 ? "" : "s"}.</span>`;
  }
  function datosCsv() { const C = datosCols(); descarga("datos-del-proyecto.csv", [["id", "latitud", "longitud"].concat(C.map(c => c.label))].concat(datosFilas().map(p => [p.id, p.lat, p.lng].concat(C.map(c => (p.x || {})[c.name] == null ? "" : p.x[c.name]))))); }
  function secDatos() {
    S.sec = "datos"; nav(); const P = S.P, pts = S.puntos || [], C = datosCols(), st = S.dt || (S.dt = { q: "", n: 100 }), ori = { mapa: "elegidos en el mapa", archivo: "cargados desde un archivo", denue: "unidades del DENUE", geolab: "traídos de Geo Lab", inmuebles: "de la base de bienes inmuebles" }[P.fuente] || "";
    wrap(`${cab("0", "Datos del proyecto", "La tabla cargada o el conjunto de coordenadas con los que se trabajará. Revísalos antes de elegir las variables del modelo.")}
      <div class="mo-fic"><div><small>Puntos</small><b>${gN(pts.length, 0)}</b></div><div><small>Origen</small><b>${esc(ori || "—")}</b></div><div><small>Columnas propias</small><b>${C.length}</b></div></div>
      <section class="mo-sx2"><h3>Ubicación de los puntos</h3><div class="mo-mapw"><div id="moDMap" class="mo-map" style="height:300px"></div></div></section>
      <section class="mo-sx2"><h3>Tabla de datos <small>${C.length ? "coordenadas y columnas del archivo" : "coordenadas de los puntos"}</small></h3>
        <div class="mo-chs"><input id="moDQ" class="mo-in" type="search" placeholder="Buscar en la tabla…" value="${esc(st.q)}" style="max-width:280px"><span class="mo-sp"></span><button type="button" class="mo-ghost" data-dcsv="1">⬇ Descargar CSV</button></div>
        <div class="mo-tw" style="max-height:480px;overflow:auto"><table class="mo-t"><thead><tr><th>#</th><th>Identificador</th><th>Latitud</th><th>Longitud</th>${C.map(c => `<th>${esc(c.label)}</th>`).join("")}</tr></thead><tbody id="moDTb"></tbody></table></div><div class="mo-chs" id="moDMas"></div></section>
      <div class="mo-wzf"><span></span><span class="mo-wzm"></span><button type="button" class="mo-new" data-sec="variables">Continuar a Variables →</button></div>`);
    datosPinta();
    leaflet(() => { const el = $("#moDMap"); if (!el || !window.L || !pts.length) return; if (S._dMap) { try { S._dMap.remove(); } catch (_) { } } const m = L.map(el); L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(m); const ll = []; pts.slice(0, 5000).forEach(p => { if (isFinite(+p.lat) && isFinite(+p.lng)) { ll.push([+p.lat, +p.lng]); L.circleMarker([+p.lat, +p.lng], { radius: 4, color: "#fff", weight: 1, fillColor: "#4f46e5", fillOpacity: .85 }).bindTooltip(String(p.id).slice(0, 50)).addTo(m); } }); if (ll.length) m.fitBounds(L.latLngBounds(ll).pad(.1)); S._dMap = m; setTimeout(() => m.invalidateSize(), 150); });
  }

  /* ── 2 · Enfoque y variable de respuesta ── */
  /* la variable a modelar elegida al cargar el archivo se toma sola como variable de respuesta (si aún no hay una y el enfoque no es no supervisado) */
  const esInm = () => !!(S.P && S.P.fuente === "inmuebles"), colPrecio = () => { const c = colsProy().find(c => /^precio$/i.test(String(c.label || c.name))); return c ? c.name : null; };
  /* en un proyecto de bienes inmuebles el enfoque «Valuación inmobiliaria» se elige solo la primera vez */
  function inmAuto() { const M = S.M; if (!esInm() || !M || M.enfoque || !perfiles().length) return; const y = colPrecio(); M.enfoque = "val"; M.y = y; M.x = perfiles().filter(p => p.tipo !== "texto" && !p.constante && p.name !== y).map(p => p.name); M.metodo = null; M.prep = null; M.prepOk = false; guardaModelo(); }
  function aplicaYVar() {
    const yv = S.P && S.P.cfg && S.P.cfg.yVar, M = S.M; if (S.aid !== "principal" || !yv || !M || M.y || M.enfoque === "nosup" || !(M.vars || []).includes(yv)) return;
    const p = perfiles().find(q => q.name === yv); if (p && (p.tipo === "texto" || p.constante)) return; M.enfoque = "sup"; M.y = yv; M.x = (M.x || []).filter(k => k !== yv); if (!M.x.length) M.x = perfiles().filter(q => q.tipo !== "texto" && !q.constante && q.name !== yv).map(q => q.name); M.metodo = null; M.prep = null; M.prepOk = false; guardaModelo();
  }
  async function secEnfoque() {
    S.sec = "enfoque"; nav(); if (!(await aseguraDs())) { wrap(`${cab("2", "Enfoque", "")}<div class="mo-empty"><b>Primero construye la tabla de datos.</b><p>Elige las variables y pulsa «Construir tabla de datos».</p><button type="button" class="mo-new" data-sec="variables">Ir a Variables</button></div>`); return; }
    aplicaYVar(); inmAuto(); const M = S.M, P = perfiles(), enf = M.enfoque, usables = P.filter(p => p.tipo !== "texto" && !p.constante), y = M.y, x = new Set(M.x || []);
    const tipoTxt = p => ({ num: "numérica continua", int: "numérica entera", bin: "binaria (2 valores)", cat: "categórica", texto: "texto (no usable)" }[p.tipo] || p.tipo);
    const filas = usables.filter(p => !(enf === "sup" && p.name === y)).map(p => `<tr><td><button type="button" class="mo-vi${x.has(p.name) ? " on" : ""}" data-x="${esc(p.name)}">${ck(x.has(p.name))}<span><b>${esc(p.label || p.name)}</b></span></button></td><td>${chipOr(p.name)}</td><td>${tipoTxt(p)}</td><td>${gN(p.unicos, 0)}</td><td${p.pctFaltantes > 30 ? ' class="mo-mal"' : ""}>${gN(p.pctFaltantes, 1)} %</td><td>${p.tipo === "num" || p.tipo === "int" ? gN(p.media) + " ± " + gN(p.sd) : (p.categorias || []).slice(0, 2).map(c => esc(c.v)).join(", ")}</td></tr>`).join("");
    const exc = P.filter(p => p.tipo === "texto" || p.constante);
    wrap(`${cab("2", "Enfoque del análisis", "¿Hay algo que quieras predecir o explicar, o quieres explorar la estructura de los datos?")}
      <div class="mo-bxg${esInm() ? " t3" : ""}">${esInm() ? `<button type="button" class="mo-bxo${enf === "val" ? " on" : ""}" data-enf="val"><i>🏠</i><b>Valuación inmobiliaria</b><small>Estima el <b>precio esperado</b> de cada propiedad según sus características y su ubicación, y detecta las que están <b>por debajo o por encima</b> del mercado. Incluye limpieza de datos, modelo predictivo y geoestadística (variograma y kriging).</small></button>` : ""}<button type="button" class="mo-bxo${enf === "sup" ? " on" : ""}" data-enf="sup"><i>🎯</i><b>Supervisado</b><small>Hay una <b>variable de respuesta</b> (lo que quieres predecir o explicar) y variables explicativas. Ej.: ventas ← entorno urbano.</small></button>
        <button type="button" class="mo-bxo${enf === "nosup" ? " on" : ""}" data-enf="nosup"><i>🧭</i><b>No supervisado</b><small>Solo un conjunto de variables: se buscan <b>grupos, patrones, relaciones o casos atípicos</b> sin variable objetivo.</small></button></div>
      ${enf === "val" ? `<section class="mo-sx2"><h3>Variable de respuesta: precio de la propiedad</h3><div class="mo-nota">Se analiza el <b>${esc(nomVar(M.y) || "Precio")}</b> usando todas las características de la base (superficie, recámaras, baños, tipo, antigüedad…) y la ubicación de cada propiedad. En el siguiente paso eliges la operación (venta, renta o noche) y el método.</div></section>` : ""}${enf && enf !== "val" ? `<section class="mo-sx2">${enf === "sup" ? `<h3>Variable de respuesta (dependiente) <small>obligatoria</small></h3><label class="mo-fl"><select class="mo-in" id="moY"><option value="">— Elige la variable de respuesta —</option>${opcionesAgrupadas(usables, y, tipoTxt)}</select></label>${y && perfilDe(y) ? `<div class="mo-nota">La respuesta es <b>${tipoTxt(perfilDe(y))}</b>${perfilDe(y).tipo === "num" || perfilDe(y).tipo === "int" ? ": el análisis será de <b>regresión</b>." : perfilDe(y).tipo === "bin" ? ": el análisis será de <b>clasificación binaria</b>." : ": el análisis será de <b>clasificación en varias categorías</b>."}</div>` : ""}` : ""}
        <h3>${enf === "sup" ? "Variables explicativas (independientes)" : "Variables del análisis"} <small>${x.size} elegidas</small></h3><p class="mo-mut">${enf === "sup" ? "Por omisión entran todas las demás variables; quita las que no quieras usar." : "Por omisión entran todas; quita las que no quieras usar."}</p>
        <div class="mo-chs"><button type="button" class="mo-ghost" data-xg="1">Elegir todas</button><button type="button" class="mo-ghost" data-xg="0">Quitar todas</button></div>
        <div class="mo-tw"><table class="mo-t"><thead><tr><th>Variable</th><th>Origen</th><th>Naturaleza</th><th>Valores distintos</th><th>Faltantes</th><th>Resumen</th></tr></thead><tbody>${filas}</tbody></table></div>
        ${exc.length ? `<div class="mo-nota">No se usarán: ${exc.map(p => esc(p.label || p.name) + (p.constante ? " (constante)" : " (texto libre)")).join(", ")}.</div>` : ""}</section>` : ""}
      ${pie("variables", "metodos", "Continuar a Métodos", !(enf && (enf === "val" ? !!y : enf === "nosup" ? x.size >= 2 : (y && x.size >= 1))))}`);
  }

  /* ── 3 · Métodos analíticos factibles ── */
  async function secMetodos() {
    S.sec = "metodos"; nav(); if (!(await aseguraDs()) || !S.M.enfoque) return ir("enfoque"); if (S.M.enfoque === "val") { if (!window.MLVal) { wrap('<div class="mo-empty"><b>No se cargó el módulo de valuación.</b></div>'); return; } wrap(window.MLVal.htmlMetodos(HV)); return; } if (!window.MLMotor) { wrap('<div class="mo-empty"><b>No se cargó el motor de modelado.</b></div>'); return; }
    const M = S.M, P = perfiles(), py = M.y ? perfilDe(M.y) : null, px = (M.x || []).map(perfilDe).filter(Boolean); let L2 = [];
    try { L2 = window.MLMotor.sugerirMetodos(M.enfoque, py, px, S.ds.rows.length) || []; } catch (e) { toast(e.message); }
    const rec = L2.find(m => m.recomendado && m.aplica), aplic = L2.filter(m => m.aplica), hayAuto = M.enfoque === "sup" && aplic.length > 1; if (M.metodo === "auto" && !hayAuto) M.metodo = null;
    if (!M.metodo) { if (hayAuto) { M.metodo = "auto"; M.metodoNombre = "Elegir el mejor modelo"; guardaModelo(); } else if (rec) { M.metodo = rec.id; M.metodoNombre = rec.nombre; guardaModelo(); } } const sm = L2.find(m => m.id === M.metodo); if (sm) M.metodoNombre = sm.nombre; else if (M.metodo === "auto") M.metodoNombre = "Elegir el mejor modelo";
    const peso = { bajo: 1, baja: 1, medio: 4, media: 4, alto: 12, alta: 12 }, nfil = S.ds.rows.length, seg = Math.round(aplic.reduce((a, m) => a + (peso[m.recursos.tiempo] || 3), 0) * Math.pow(Math.max(nfil, 300) / 3000, .8) + 2);
    const autoCard = hayAuto ? `<section class="mo-sx2 mo-auto"><button type="button" class="mo-mt auto${M.metodo === "auto" ? " on" : ""}" data-met="auto"><div class="mo-mh"><span class="mo-mi">${iconoMetodo("auto")}</span><div class="t"><b>Elegir el mejor modelo</b><span class="mo-rec">Predeterminado</span></div></div><p>Entrena <b>todos los métodos aplicables (${aplic.length})</b>, los compara por precisión y calidad (validación cruzada y datos de prueba, penalizando el sobreajuste) y se queda con los resultados del mejor. Después puedes consultar también los resultados de los demás.</p><div class="mo-razon">Probará: ${aplic.map(m => esc(m.nombre)).join(" · ")}.</div><div class="mo-rs"><span>Tiempo <span class="mo-nv ${seg > 90 ? "alto" : seg > 20 ? "medio" : "bajo"}">${seg > 90 ? "Alto" : seg > 20 ? "Medio" : "Bajo"}</span></span><i>aprox. ${seg} segundos con tus ${gN(nfil, 0)} filas</i></div></button><p class="mo-mut mo-o">…o elige un solo método:</p></section>` : "";
    const nivel = v => `<span class="mo-nv ${v}">${{ bajo: "Bajo", baja: "Baja", medio: "Medio", media: "Media", alto: "Alto", alta: "Alta" }[v] || v}</span>`;
    const fam = [...new Set(L2.map(m => m.familia))];
    wrap(`${cab("3", "Métodos analíticos factibles", M.enfoque === "sup" ? `Con tu variable de respuesta (${py ? esc(py.tipo === "num" || py.tipo === "int" ? "numérica" : py.tipo === "bin" ? "binaria" : "categórica") : "—"}) y ${px.length} variables explicativas, estos son los métodos aplicables.` : `Con ${px.length} variables y sin variable objetivo, estos son los métodos de exploración aplicables.`)}
      ${autoCard}
      ${fam.map(f => `<section class="mo-sx2"><h3>${esc(f)}</h3><div class="mo-mg">${L2.filter(m => m.familia === f).map(m => `<button type="button" class="mo-mt${M.metodo === m.id ? " on" : ""}${m.aplica ? "" : " no"}" data-met="${m.id}"${m.aplica ? "" : " disabled"}><div class="mo-mh"><span class="mo-mi">${iconoMetodo(m.id)}</span><div class="t"><b>${esc(m.nombre)}</b>${m.recomendado && m.aplica ? '<span class="mo-rec">Recomendado</span>' : ""}${m.aplica ? "" : '<span class="mo-no">No aplica</span>'}</div></div><p>${esc(m.desc)}</p><div class="mo-razon">${esc(m.razon || "")}</div>
        <div class="mo-pc2"><div><small>Ventajas</small><ul>${(m.ventajas || []).map(v => `<li>${esc(v)}</li>`).join("")}</ul></div><div><small>Alcances y límites</small><ul>${(m.limites || []).map(v => `<li>${esc(v)}</li>`).join("")}</ul></div></div>
        <div class="mo-rs"><span>Tiempo ${nivel(m.recursos.tiempo)}</span><span>Memoria ${nivel(m.recursos.memoria)}</span><i>${esc(m.recursos.etiqueta || "")}</i></div></button>`).join("")}</div></section>`).join("")}
      ${M.enfoque === "nosup" && (M.metodo === "kmeans" || M.metodo === "jerarquico") ? `<label class="mo-fl">Número de grupos <small class="mo-mut">(déjalo vacío para que Model Lab lo elija)</small><input class="mo-in mo-num" id="moK" type="number" min="2" max="20" value="${M.k || ""}"></label>` : ""}
      ${pie("enfoque", "prep", "Continuar a Preprocesamiento", !M.metodo)}`);
  }

  /* ── 4 · Preprocesamiento ── */
  const CAT_NOM = { faltantes: "Datos faltantes e imputación", outliers: "Valores atípicos (outliers)", escalado: "Estandarización de unidades y escalas", codificacion: "Codificación de categorías", transformacion: "Transformaciones de forma", duplicados: "Filas duplicadas", colineales: "Variables muy correlacionadas entre sí", constantes: "Variables constantes", columnaFaltantes: "Columnas con demasiados faltantes" };
  async function secPrep() {
    S.sec = "prep"; nav(); if (!(await aseguraDs()) || !S.M.metodo) return ir("metodos"); const M = S.M, P = perfiles(), cat = window.MLMotor.catalogoPrep;
    const ctx = { enfoque: M.enfoque, y: M.y || null, x: M.x || [], n: S.ds.rows.length, metodo: M.metodo === "auto" ? undefined : M.metodo }; let sug; try { sug = window.MLMotor.sugerirPrep(P, ctx); } catch (e) { toast(e.message); return; }
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

  /* ilustraciones simples de cada método (SVG en línea, 96 × 64) */
  const ICO_M = (() => {
    const P = (xy, c, r) => xy.map(p => `<circle cx="${p[0]}" cy="${p[1]}" r="${r || 2.6}" fill="${c}"/>`).join(""), L = (a, b, c, w, d) => `<line x1="${a[0]}" y1="${a[1]}" x2="${b[0]}" y2="${b[1]}" stroke="${c}" stroke-width="${w || 2.2}" stroke-linecap="round"${d ? ` stroke-dasharray="${d}"` : ""}/>`;
    const AZ = "#4f46e5", AZ2 = "#818cf8", NA = "#f59e0b", VE = "#10b981", RO = "#e11d48", GR = "#94a3b8", CI = "#06b6d4";
    const arbol = (x, y, s, c) => `<g stroke="${c}" stroke-width="${1.6 * s}" fill="none" stroke-linecap="round"><path d="M${x} ${y} L${x - 9 * s} ${y + 11 * s} M${x} ${y} L${x + 9 * s} ${y + 11 * s} M${x - 9 * s} ${y + 11 * s} L${x - 14 * s} ${y + 22 * s} M${x - 9 * s} ${y + 11 * s} L${x - 4 * s} ${y + 22 * s} M${x + 9 * s} ${y + 11 * s} L${x + 4 * s} ${y + 22 * s} M${x + 9 * s} ${y + 11 * s} L${x + 14 * s} ${y + 22 * s}"/></g><circle cx="${x}" cy="${y}" r="${3.2 * s}" fill="${c}"/><circle cx="${x - 9 * s}" cy="${y + 11 * s}" r="${2.6 * s}" fill="${c}"/><circle cx="${x + 9 * s}" cy="${y + 11 * s}" r="${2.6 * s}" fill="${c}"/>` + [[-14, 22, VE], [-4, 22, NA], [4, 22, VE], [14, 22, NA]].map(q => `<rect x="${x + q[0] * s - 2.8 * s}" y="${y + q[1] * s}" width="${5.6 * s}" height="${5.6 * s}" rx="1" fill="${q[2]}"/>`).join("");
    const sc = (s) => `<svg viewBox="0 0 96 64" aria-hidden="true">${s}</svg>`;
    const puntos = [[14, 48], [22, 41], [28, 45], [36, 33], [44, 35], [52, 25], [60, 29], [70, 17], [78, 19], [86, 11]];
    const o = {};
    o.ols = sc(P(puntos, AZ2) + L([8, 53], [90, 8], AZ, 2.8));
    o.ridge = sc(`<path d="M8 52 C16 30 22 58 32 38 S48 44 56 24 S72 30 90 8" fill="none" stroke="${GR}" stroke-width="1.8" stroke-dasharray="3 3"/>` + P(puntos, AZ2) + L([8, 53], [90, 8], AZ, 2.8));
    o.lasso = sc([[44, 4], [30, 14], [0, 24], [18, 34], [0, 44], [36, 54]].map((b, i) => b[0] ? `<rect x="14" y="${b[1] - 4}" width="${b[0] * 1.6}" height="7" rx="3.5" fill="${AZ}"/>` : `<circle cx="16" cy="${b[1] - .5}" r="2.6" fill="${GR}"/><text x="24" y="${b[1] + 3}" font-size="9" font-weight="700" fill="${GR}" font-family="system-ui">0</text>`).join(""));
    o.logistica = sc(`<path d="M8 50 C34 50 38 48 48 32 S62 14 88 14" fill="none" stroke="${AZ}" stroke-width="2.8" stroke-linecap="round"/>` + P([[14, 52], [26, 52], [38, 52], [46, 52]], VE, 3) + P([[52, 12], [64, 12], [76, 12], [88, 12]], NA, 3) + L([8, 32], [90, 32], GR, 1, "2 3"));
    o.arbol_reg = sc(arbol(48, 8, 1.6, AZ)); o.arbol_clf = o.arbol_reg;
    o.bosque_reg = sc(arbol(18, 14, .62, AZ) + arbol(48, 14, .62, AZ2) + arbol(78, 14, .62, AZ) + arbol(33, 36, .5, CI) + arbol(63, 36, .5, VE)); o.bosque_clf = o.bosque_reg;
    const knnP = P([[16, 48], [24, 40], [20, 28], [30, 50], [70, 14], [80, 24], [62, 22], [76, 34]], NA) + P([[22, 20], [34, 44], [28, 34], [40, 24], [18, 38]], AZ2);
    o.knn_reg = sc(knnP + `<circle cx="48" cy="34" r="17" fill="none" stroke="${AZ}" stroke-width="1.6" stroke-dasharray="3 3"/><path d="M48 28 l2 4 4 .6 -3 3 .8 4 -3.8 -2 -3.8 2 .8 -4 -3 -3 4 -.6z" fill="${RO}"/>`); o.knn_clf = o.knn_reg;
    o.naive_bayes = sc(`<path d="M6 54 C20 54 26 8 38 8 S54 54 70 54" fill="${AZ2}" fill-opacity=".35" stroke="${AZ}" stroke-width="2"/><path d="M28 54 C42 54 54 6 66 6 S80 54 90 54" fill="${NA}" fill-opacity=".35" stroke="${NA}" stroke-width="2"/>` + L([4, 55], [92, 55], GR, 1.2));
    o.lda = sc(P([[14, 14], [22, 22], [16, 30], [28, 16], [26, 32], [12, 24]], AZ2) + P([[62, 42], [72, 34], [80, 46], [70, 50], [84, 36], [58, 52]], NA) + L([30, 58], [66, 6], AZ, 2.6));
    o.pca = sc(`<g transform="rotate(-28 48 34)"><ellipse cx="48" cy="34" rx="36" ry="12" fill="${AZ2}" fill-opacity=".25" stroke="${AZ2}" stroke-width="1.2"/></g>` + P([[24, 44], [34, 38], [42, 36], [54, 30], [62, 24], [72, 20], [40, 42], [58, 36]], AZ2, 2.2) + L([48, 34], [82, 14], AZ, 2.6) + L([48, 34], [38, 18], VE, 2.6) + `<path d="M82 14 l-7 .5 m7 -.5 l-3 6" stroke="${AZ}" stroke-width="2.4" fill="none" stroke-linecap="round"/><path d="M38 18 l1 6 m-1 -6 l5 3" stroke="${VE}" stroke-width="2.4" fill="none" stroke-linecap="round"/>`);
    const cx = (x, y, c) => `<path d="M${x - 3.5} ${y - 3.5} l7 7 m0 -7 l-7 7" stroke="${c}" stroke-width="2.4" stroke-linecap="round"/>`;
    o.kmeans = sc(P([[16, 14], [24, 20], [14, 24], [22, 12], [26, 24]], AZ2) + P([[66, 12], [74, 18], [80, 10], [70, 24], [84, 20]], NA) + P([[40, 48], [50, 54], [58, 46], [44, 56], [54, 44]], VE) + cx(20, 18, AZ) + cx(75, 17, "#b45309") + cx(49, 50, "#047857"));
    o.jerarquico = sc(`<g stroke="${AZ}" stroke-width="2" fill="none" stroke-linecap="round"><path d="M12 56 V38 H24 V56 M18 38 V26 H40 M40 26 V56 M30 26 V14 H66 M66 14 V30 M52 30 H80 M52 30 V56 M80 30 V56 M66 30 V30"/></g>` + P([[12, 58], [24, 58], [40, 58], [52, 58], [80, 58]], AZ2, 2.4));
    o.dbscan = sc(P([[18, 18], [26, 14], [24, 24], [16, 26], [30, 22], [22, 20]], AZ2) + P([[66, 40], [74, 36], [72, 48], [62, 46], [80, 44], [70, 42]], NA) + `<circle cx="23" cy="20" r="14" fill="none" stroke="${AZ2}" stroke-width="1.2" stroke-dasharray="3 3"/><circle cx="71" cy="42" r="14" fill="none" stroke="${NA}" stroke-width="1.2" stroke-dasharray="3 3"/>` + P([[50, 10], [44, 52], [88, 12], [8, 52]], GR, 2.2));
    o.correlaciones = sc([0, 1, 2, 3].map(i => [0, 1, 2, 3].map(j => { const v = i === j ? 1 : Math.abs(Math.cos((i + 1) * (j + 2))), c = (i * 3 + j) % 3 === 1 && i !== j ? RO : AZ; return `<rect x="${20 + j * 15}" y="${5 + i * 14}" width="13" height="12" rx="2.5" fill="${c}" fill-opacity="${(.15 + .8 * v).toFixed(2)}"/>`; }).join("")).join(""));
    o.anomalias = sc(P([[22, 40], [30, 34], [28, 46], [38, 42], [34, 30], [44, 38], [26, 38], [40, 48]], AZ2) + `<circle cx="78" cy="14" r="9" fill="none" stroke="${RO}" stroke-width="2"/>` + P([[78, 14]], RO, 3.6) + L([70, 20], [46, 36], GR, 1.2, "2 3"));
    o.svm_clf = sc(P([[14, 14], [24, 20], [18, 30], [30, 12], [26, 28]], AZ2) + P([[64, 44], [74, 38], [82, 50], [70, 54], [86, 40]], NA) + L([40, 60], [58, 4], AZ, 2.8) + L([30, 60], [48, 4], AZ2, 1.2, "3 3") + L([50, 60], [68, 4], AZ2, 1.2, "3 3") + `<circle cx="30" cy="12" r="5" fill="none" stroke="${RO}" stroke-width="1.6"/><circle cx="64" cy="44" r="5" fill="none" stroke="${RO}" stroke-width="1.6"/>`);
    o.svm_reg = sc(`<path d="M8 50 L90 12 L90 24 L8 62 Z" fill="${AZ2}" fill-opacity=".18"/>` + P([[16, 46], [26, 44], [36, 40], [46, 32], [54, 34], [66, 22], [78, 18], [86, 14]], AZ2) + L([8, 56], [90, 18], AZ, 2.6) + L([8, 50], [90, 12], AZ2, 1.2, "3 3") + L([8, 62], [90, 24], AZ2, 1.2, "3 3"));
    const nodos = (cols) => { let h = ""; const xs = [14, 38, 62, 84]; cols.forEach((n, i) => { for (let j = 0; j < n; j++) cols[i + 1] && Array.from({ length: cols[i + 1] }, (_, k) => k).forEach(k => { h += `<line x1="${xs[i]}" y1="${32 + (j - (n - 1) / 2) * 14}" x2="${xs[i + 1]}" y2="${32 + (k - (cols[i + 1] - 1) / 2) * 14}" stroke="${GR}" stroke-width=".8"/>`; }); }); cols.forEach((n, i) => { for (let j = 0; j < n; j++) h += `<circle cx="${xs[i]}" cy="${32 + (j - (n - 1) / 2) * 14}" r="4.2" fill="${i === 0 ? AZ2 : i === cols.length - 1 ? NA : AZ}"/>`; }); return h; };
    o.mlp_clf = sc(nodos([3, 4, 4, 1])); o.mlp_reg = o.mlp_clf;
    const mini = (x, c) => arbol(x, 10, .62, c);
    o.gbm_clf = sc(mini(14, AZ) + `<text x="31" y="26" font-size="15" font-weight="800" fill="${GR}" font-family="system-ui">+</text>` + mini(50, AZ2) + `<text x="67" y="26" font-size="15" font-weight="800" fill="${GR}" font-family="system-ui">+</text>` + mini(84, CI)); o.gbm_reg = o.gbm_clf;
    return o;
  })();
  const iconoMetodo = id => ICO_M[id] || '<svg viewBox="0 0 96 64" aria-hidden="true"><circle cx="48" cy="32" r="14" fill="none" stroke="#818cf8" stroke-width="3"/></svg>';

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
  ETAPAS.val = [["limpieza", "Limpieza exhaustiva de las publicaciones"], ["hedonico", "Modelo de precios por atributos y zona"], ["variograma", "Variograma de los residuos"], ["kriging", "Kriging (corrección espacial)"], ["clasificacion", "Clasificación: por debajo, en línea o por encima"]];
  /* puente con modelab-valuacion.js */
  const HV = { S, cab, pie, gN, ck, leaflet, api, guarda: () => guardaModelo(), reMetodos: () => secMetodos(), reResultados: () => secResultados() };

  /* ── 5 · Modelar / Analizar ── */
  function secModelar() {
    S.sec = "modelar"; nav(); const M = S.M; if (M.enfoque === "val") { if (!M.metodo) return ir("metodos"); const V = M.val || {}, NO = { venta: "Venta", renta: "Renta mensual", noche: "Alquiler por noche" };
      wrap(`${cab("5", "Valuar las propiedades", "Se limpian las publicaciones, se estima el precio esperado de cada una y se clasifican frente al mercado.")}
        <div class="mo-fic"><div><small>Enfoque</small><b>Valuación inmobiliaria</b></div><div><small>Operación</small><b>${esc(NO[V.op] || V.op || "Todas")}</b></div><div><small>Publicaciones en la tabla</small><b>${gN(S.ds ? S.ds.rows.length : S.puntos.length, 0)}</b></div></div>
        <div class="mo-fic"><div><small>Método</small><b>${esc(M.metodoNombre || M.metodo)}</b></div><div><small>Diferencia mínima</small><b>${V.minDif || 10} %</b></div><div><small>Variable analizada</small><b>${esc(nomVar(M.y) || "Precio")}</b></div></div>
        <div class="mo-etp"><b>Etapas</b><ol>${ETAPAS.val.map(([, n]) => `<li>${n}</li>`).join("")}</ol></div>
        <div class="mo-go"><button type="button" class="mo-new mo-rojo mo-big" data-fl="correr">Valuar</button>${S.res && S.res.enfoque === "val" ? '<span class="mo-mut">Ya hay un resultado guardado; volver a valuar lo reemplaza.</span>' : ""}</div>${pie("metodos", null, "", false)}`); return; }
    if (!M.metodo || !M.prepOk && !M.prep) return ir(M.metodo ? "prep" : "metodos"); const sup = M.enfoque === "sup";
    wrap(`${cab("5", "Modelar / Analizar", sup ? "Se entrena el modelo, se calibra y se evalúa con datos que no vio al entrenar." : "Se ajusta el análisis exploratorio y se valida su calidad.")}
      <div class="mo-fic"><div><small>Enfoque</small><b>${sup ? "Supervisado" : "No supervisado"}</b></div><div><small>${sup ? "Variable de respuesta" : "Variables"}</small><b>${sup ? esc(nomVar(M.y)) : (M.x || []).length}</b></div><div><small>${sup ? "Variables explicativas" : "Observaciones"}</small><b>${sup ? (M.x || []).length : gN(S.ds ? S.ds.rows.length : S.puntos.length, 0)}</b></div></div>
      <div class="mo-fic"><div><small>Método</small><b>${esc(M.metodoNombre || M.metodo)}</b></div><div><small>Observaciones</small><b>${gN(S.ds ? S.ds.rows.length : S.puntos.length, 0)}</b></div><div><small>Preprocesamiento</small><b>${Object.keys((M.prep || {}).global || {}).length} reglas</b></div></div>
      <div class="mo-etp"><b>Etapas</b><ol>${ETAPAS[sup ? "sup" : "nosup"].map(([, n]) => `<li>${n}</li>`).join("")}</ol></div>
      <div class="mo-go"><button type="button" class="mo-new mo-rojo mo-big" data-fl="correr">${sup ? "Modelar" : "Analizar"}</button>${S.res ? '<span class="mo-mut">Ya hay un resultado guardado; volver a modelar lo reemplaza.</span>' : ""}</div>${pie("prep", null, "", false)}`);
  }
  const fmtK = k => typeof k.valor === "number" ? (k.formato === "pct" ? gN(k.valor * 100, 1) + " %" : gN(k.valor)) : String(k.valor == null ? "—" : k.valor);
  async function correVal() {
    const M = S.M, E = ETAPAS.val, V = M.val || {}; if (!(await aseguraDs())) { toast("Falta la tabla de datos."); return; } if (!window.MLVal) { toast("No se cargó el módulo de valuación."); return; } S.perfiles = null;
    const o = popup("Valuando propiedades", `${S.ds.rows.length} publicaciones · ${M.metodoNombre || M.metodo}`), hechas = new Map(); let act = E[0][0], t0 = Date.now(), ult = null; const tIni = Date.now(); popupEtapas(o, E, act, hechas, "");
    const cfg = { y: M.y, op: V.op || null, metodo: M.metodo, minDif: (V.minDif || 10) / 100, semilla: 42 };
    try { const res = await window.MLVal.correr(S.ds, cfg, ev => { if (!ev) return; const i = E.findIndex(x => x[0] === ev.id); if (i < 0) return; if (ult && ult !== ev.id) hechas.set(ult, Date.now() - t0); if (ult !== ev.id) { t0 = Date.now(); } ult = ev.id; act = ev.id; E.forEach(([id], j) => { if (j < i) hechas.set(id, hechas.get(id) != null ? hechas.get(id) : 0); }); popupAv(o, (i + (ev.progreso || 0)) / E.length); popupEtapas(o, E, act, hechas, ev.detalle || ""); });
      popupAv(o, 1); const resta = 1200 - (Date.now() - tIni); if (resta > 0) await new Promise(r => setTimeout(r, resta)); S.res = res; S.mv = null; S._mvM = null; S.M.prepOk = true; S.M.hecho = new Date().toISOString(); await guardaModelo();
      try { const kpi = (res.kpis || []).slice(0, 2).map(k => ({ etiqueta: k.etiqueta, valor: fmtK(k) })); await api(R + "/proyectos/" + S.id + "/resultados", { res, kpi }); const p = S.proy.find(x => x.id === S.id); if (p) { p.estado = "modelado"; p.kpi = kpi; p.metodo = res.nombreMetodo || p.metodo; } } catch (e) { toast("El resultado se calculó pero no se pudo guardar: " + e.message, 6000); }
      popupCierra(o); nav(); ir("resultados"); }
    catch (e) { o.querySelector(".mo-pbar").classList.add("err"); o.querySelector(".mo-pd").innerHTML = `<b class="mo-mal">No se pudo completar:</b> ${esc(e.message)}`; o.querySelector("footer").innerHTML = '<button type="button" class="mo-ghost" data-fl="cierrapop">Cerrar</button>'; }
  }
  async function corre() {
    if (S.M.enfoque === "val") return correVal();
    if (S.M.metodo === "auto") return correAuto();
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
  /* índices de las observaciones sin valores extremos (fuera de 3 rangos intercuartílicos): así un solo punto lejano no aplasta la escala de la gráfica */
  function sinExtremos(xs, ys) { const lim = a => { const v = a.filter(nz).sort((p, q) => p - q); if (v.length < 8) return [-Infinity, Infinity]; const q1 = v[Math.floor(v.length * .25)], q3 = v[Math.floor(v.length * .75)], iq = q3 - q1 || 1e-9; return [q1 - 3 * iq, q3 + 3 * iq]; }, lx = lim(xs), ly = lim(ys), keep = []; xs.forEach((x, i) => { if (nz(x) && nz(ys[i]) && x >= lx[0] && x <= lx[1] && ys[i] >= ly[0] && ys[i] <= ly[1]) keep.push(i); }); return keep; }
  /* leyenda de los grupos: mismos colores en el mapa, las tarjetas, el biplot y el diagrama de observaciones */
  const leyendaGrupos = C => { if (!C || !(C.k >= 1)) return ""; const n = C.tamanos || [], et = C.etiquetaSugerida || []; return `<div class="mo-lgg">${Array.from({ length: C.k }, (_, g) => `<span title="${esc(et[g] || "")}"><i style="background:${PAL[g % PAL.length]}"></i><b>Grupo ${g + 1}</b>${et[g] ? ` · ${esc(et[g])}` : ""}${n[g] != null ? ` <small>(${gN(n[g], 0)} casos)</small>` : ""}</span>`).join("")}${C.ruido > 0 ? `<span><i style="background:#cbd5e1"></i><b>Sin grupo</b> <small>(${gN(C.ruido, 0)})</small></span>` : ""}</div>`; };
  /* comparación de grupos: gráfica interactiva (barras de diferencia frente al promedio con ejes, valores y metadatos al pasar el cursor o pulsar) + tabla de promedios */
  function cgFilas(C) {
    const k = C.k, cols = C.columnas || [], Z = C.centroidesZ, Mu = C.centroides || [], n = C.tamanos || [], ver = Math.min(k, 8), N = n.slice(0, k).reduce((a, x) => a + x, 0) || 1; let filas;
    if (Z && Z.length) filas = cols.map((v, j) => ({ v, z: Array.from({ length: ver }, (_, g) => Z[g] ? Z[g][j] : null), m: Array.from({ length: ver }, (_, g) => Mu[g] ? Mu[g][j] : null) }));
    else { const map = new Map(); (C.perfil || []).forEach(pf => (pf.variablesDistintivas || []).forEach(d => { if (!map.has(d.var)) map.set(d.var, { v: d.var, z: Array(ver).fill(null), m: Array(ver).fill(null) }); map.get(d.var).z[pf.cluster] = d.z; })); filas = [...map.values()]; }
    return filas.filter(f => f.z.some(x => nz(x))).map(f => { const zz = f.z.filter(nz); f.sp = Math.max(...zz) - Math.min(...zz); f.mg = f.m.every(nz) ? f.m.reduce((a, x, g) => a + x * (n[g] || 0), 0) / N : null; return f; }).sort((a, b) => b.sp - a.sp).slice(0, 10);
  }
  /* promedios por grupo en unidades originales: si el resultado guardado no los trae (o vienen vacíos), se calculan de la tabla de datos con la asignación de grupos */
  function cgMediasDs(C, filas) {
    const ds = S.ds, A = C.asignacion || []; if (!ds || !A.length) return; const k = Math.min(C.k, 8);
    filas.forEach(f => { if (f.m.every(nz) && f.mg != null) return; const j = ds.cols.findIndex(c => c.label === f.v || c.name === f.v); if (j < 0) return; const sm = new Array(k).fill(0), ct = new Array(k).fill(0); let st = 0, ctt = 0;
      for (let i = 0; i < ds.rows.length; i++) { const g = A[i]; if (g == null || g < 0 || g >= k) continue; const v = ds.rows[i][j], x = typeof v === "number" ? v : parseFloat(String(v).replace(",", ".")); if (!isFinite(x)) continue; sm[g] += x; ct[g]++; st += x; ctt++; }
      if (ct.every(c => c > 0)) { f.m = sm.map((x, g) => x / ct[g]); f.mg = ctt ? st / ctt : null; } });
  }
  const cgUni = v => { const c = S.ds ? S.ds.cols.find(x => x.label === v || x.name === v) : null; return c && c.unit ? c.unit : ""; };
  function compGrupos(C) {
    if (!C || !(C.k >= 2)) return ""; S._cgC = C; const filas = cgFilas(C); if (!filas.length) return ""; cgMediasDs(C, filas); if (!S.ds && !S._cgDs) { S._cgDs = 1; aseguraDs().then(ok => { S._cgDs = 0; if (ok && S.sec === "resultados") cgRe(); }).catch(() => { S._cgDs = 0; }); }
    const ver = Math.min(C.k, 8), sel = S.cg || {}, et = C.etiquetaSugerida || [], n = C.tamanos || [], conM = C.k <= 4, mw = 56;
    const mz = Math.max(...filas.flatMap(f => f.z.filter(nz).map(Math.abs))), LIM = Math.max(1.5, Math.min(3.5, Math.ceil(mz * 2) / 2)), W = 560, lw = 148, mcol = conM ? ver * mw : 0, pw = W - lw - 44 - mcol, rh = ver * 12 + 14, X = z => lw + pw / 2 + Math.max(-LIM, Math.min(LIM, z)) / LIM * (pw / 2), tks = []; for (let t = -Math.floor(LIM); t <= Math.floor(LIM); t++) tks.push(t);
    const grid = h => tks.map(t => `<line x1="${X(t)}" x2="${X(t)}" y1="0" y2="${h}" stroke="${t === 0 ? "#64748b" : "#e8edf3"}" stroke-width="${t === 0 ? 1.4 : 1}"/>`).join("");
    const tip = (f, g) => { const z = f.z[g], m = f.m[g], u = cgUni(f.v), pc = f.mg != null && f.mg !== 0 && nz(m) ? (m - f.mg) / Math.abs(f.mg) * 100 : null; return [`${nombreV(f.v)}`, `Grupo ${g + 1}${et[g] ? " · " + et[g] : ""} (${gN(n[g] || 0, 0)} casos)`, `Frente al promedio general: ${z >= 0 ? "+" : ""}${gN(z, 2)} desviaciones estándar (${z >= 0 ? "más alto" : "más bajo"})`, nz(m) ? `Promedio del grupo: ${gN(m)}${u ? " " + u : ""}` : "", f.mg != null ? `Promedio general: ${gN(f.mg)}${u ? " " + u : ""}` : "", pc != null ? `Diferencia: ${pc >= 0 ? "+" : ""}${gN(pc, 0)} % frente al promedio general` : "", "Clic en el renglón para ver el detalle de la variable"].filter(Boolean).join("\n"); };
    const cabeza = `<svg viewBox="0 0 ${W} 40" class="mo-svg mo-cgs" aria-hidden="true"><text x="${lw - 8}" y="30" text-anchor="end" font-size="10.5" font-weight="800" fill="#475569">VARIABLE</text><text x="${lw + pw / 2}" y="30" text-anchor="middle" font-size="10.5" font-weight="800" fill="#475569">FRENTE AL PROMEDIO GENERAL (σ)</text>${conM ? `<text x="${W - mcol / 2 - 2}" y="14" text-anchor="middle" font-size="10" font-weight="800" fill="#475569">PROMEDIO</text>` + Array.from({ length: ver }, (_, g) => `<circle cx="${W - mcol + g * mw + 12}" cy="26" r="4.5" fill="${PAL[g % PAL.length]}"/><text x="${W - mcol + g * mw + 20}" y="30" font-size="10.5" font-weight="800" fill="#334155">G${g + 1}</text>`).join("") : ""}</svg>`;
    const unaFila = (f, i) => {
      const on = sel.v === i, u = cgUni(f.v), nom = String(nombreV(f.v));
      const svg = `<svg viewBox="0 0 ${W} ${rh}" class="mo-svg mo-cgs" role="img" aria-label="${esc(nom)}">${grid(rh)}
        <text x="${lw - 8}" y="${rh / 2 + 4}" text-anchor="end" font-size="11.5" font-weight="${on ? 800 : 600}" fill="#0f172a">${esc(nom.length > 24 ? nom.slice(0, 23) + "…" : nom)}<title>${esc(nom)}</title></text>
        ${f.z.map((z, g) => { if (!nz(z)) return ""; const x0 = X(0), x1 = X(z), dim = sel.g != null && sel.g !== g, by = 5 + g * 12; return `<g data-cgb="1" data-tip="${esc(tip(f, g))}" opacity="${dim ? .25 : 1}"><rect x="${Math.min(x0, x1)}" y="${by}" width="${Math.max(2, Math.abs(x1 - x0))}" height="10" rx="3" fill="${PAL[g % PAL.length]}"/><text x="${z >= 0 ? x1 + 4 : x1 - 4}" y="${by + 8.5}" text-anchor="${z >= 0 ? "start" : "end"}" font-size="9.5" font-weight="700" fill="#334155">${z >= 0 ? "+" : "−"}${gN(Math.abs(z), 1)}</text></g>`; }).join("")}
        ${conM ? f.m.map((m, g) => `<text x="${W - mcol + g * mw + 52}" y="${rh / 2 + 4}" text-anchor="end" font-size="11" font-weight="600" fill="${sel.g != null && sel.g !== g ? "#cbd5e1" : "#0f172a"}">${nz(m) ? esc(gN(m)) : "—"}</text>`).join("") : ""}</svg>`;
      let det = ""; if (on) { const b = (typeof BIB === "function" ? BIB() : []).find(x => x.n === f.v), o = origenEtq(f.v), ord = f.m.map((m, g) => [m, g]).filter(x => nz(x[0])).sort((a1, b1) => b1[0] - a1[0]), hi = ord[0], lo = ord[ord.length - 1];
        det = `<div class="mo-cgd"><div class="mo-cgdh"><span class="mo-or ${o.k}">${o.t}</span>${u ? `<span class="mo-cgu">${esc(u)}</span>` : ""}${b ? `<span class="mo-cgu">${esc(b.c)} › ${esc(b.e)}</span>` : ""}<button type="button" data-cgv="${i}" class="mo-cgx" aria-label="Cerrar detalle">×</button></div>
          <table class="mo-t mo-cmp"><thead><tr><th>Grupo</th><th class="r">Promedio</th><th class="r">Frente al promedio</th><th class="r">Diferencia</th></tr></thead><tbody>${f.m.map((m, g) => { const z = f.z[g], pc = f.mg != null && f.mg !== 0 && nz(m) ? (m - f.mg) / Math.abs(f.mg) * 100 : null; return `<tr><td><i class="mo-cgi" style="background:${PAL[g % PAL.length]}"></i>Grupo ${g + 1}</td><td class="r"><b>${nz(m) ? gN(m) : "—"}</b>${u ? ` <small>${esc(u)}</small>` : ""}</td><td class="r">${nz(z) ? (z >= 0 ? "+" : "−") + gN(Math.abs(z), 2) + " σ" : "—"}</td><td class="r">${pc != null ? (pc >= 0 ? "+" : "−") + gN(Math.abs(pc), 0) + " %" : "—"}</td></tr>`; }).join("")}${f.mg != null ? `<tr class="t"><td>Promedio general</td><td class="r"><b>${gN(f.mg)}</b>${u ? ` <small>${esc(u)}</small>` : ""}</td><td class="r">0</td><td class="r">—</td></tr>` : ""}</tbody></table>
          ${hi && lo && hi[1] !== lo[1] ? `<p class="mo-mut">El <b>Grupo ${hi[1] + 1}</b> tiene el valor más alto (${gN(hi[0])}) y el <b>Grupo ${lo[1] + 1}</b> el más bajo (${gN(lo[0])})${lo[0] > 0 ? `: ${gN(hi[0] / lo[0], 1)} veces más` : ""}.</p>` : ""}</div>`; }
      return `<div class="mo-cgr${on ? " on" : ""}"><div class="mo-cgrow" data-cgv="${i}" title="Clic para ver el detalle de la variable">${svg}</div>${det}</div>`;
    };
    const eje = `<svg viewBox="0 0 ${W} 54" class="mo-svg mo-cgs" aria-hidden="true">${tks.map(t => `<line x1="${X(t)}" x2="${X(t)}" y1="0" y2="6" stroke="#94a3b8"/><text x="${X(t)}" y="19" text-anchor="middle" font-size="${t === 0 ? 10.5 : 10}" font-weight="${t === 0 ? 800 : 400}" fill="${t === 0 ? "#0f172a" : "#64748b"}">${t === 0 ? "0 = promedio general" : (t > 0 ? "+" : "−") + Math.abs(t)}</text>`).join("")}<text x="${lw + pw / 2}" y="35" text-anchor="middle" font-size="10.5" font-weight="700" fill="#334155">Diferencia frente al promedio general (desviaciones estándar)</text><text x="${lw}" y="49" font-size="9.5" fill="#94a3b8">← más bajo que el promedio</text><text x="${lw + pw}" y="49" text-anchor="end" font-size="9.5" fill="#94a3b8">más alto que el promedio →</text></svg>`;
    const chips = Array.from({ length: ver }, (_, g) => `<button type="button" class="mo-cgc${sel.g === g ? " on" : ""}${sel.g != null && sel.g !== g ? " dim" : ""}" data-cgg="${g}" title="Clic para resaltar este grupo"><i style="background:${PAL[g % PAL.length]}"></i><b>Grupo ${g + 1}</b>${et[g] ? ` · ${esc(et[g])}` : ""} <small>(${gN(n[g] || 0, 0)} casos)</small></button>`).join("");
    return `<article class="mo-gr mo-cmpg"><h4>Qué perfil tiene cada grupo</h4><p class="mo-mut mo-cmpn">Las ${filas.length} variables que más separan a los grupos, de la más a la menos distintiva. <b>La línea del 0 es el promedio de todos los casos.</b> Cada barra es el <b>promedio de ese grupo</b>: a la derecha, el grupo está por encima del promedio general en esa variable; a la izquierda, por debajo. Pulsa un renglón para ver su detalle justo debajo.</p><div class="mo-cgch">${chips}</div>${cabeza}<div class="mo-cgl">${filas.map(unaFila).join("")}</div>${eje}</article>`;
  }
  /* eventos de la comparación de grupos (selección de variable o grupo) y tooltip genérico con data-tip */
  function cgClic(e) {
    const t = e.target; let b; if (!(S.cg || (S.cg = {}))) return false;
    if ((b = t.closest("[data-cgv]"))) { const v = +b.dataset.cgv; S.cg.v = S.cg.v === v ? null : v; cgRe(); return true; }
    if ((b = t.closest("[data-cgg]"))) { const g = +b.dataset.cgg; S.cg.g = S.cg.g === g ? null : g; cgRe(); return true; }
    return false;
  }
  function cgRe() { const el = $(".mo-cmpg"); if (!el || !S._cgC) return; const y = $("#moMain").scrollTop; el.outerHTML = compGrupos(S._cgC); $("#moMain").scrollTop = y; }
  function tipInit() {
    let tp = null; const mu = e => { const t = e.target.closest && e.target.closest("[data-tip]"); if (!t) { if (tp) tp.style.display = "none"; return; } if (!tp) { tp = document.createElement("div"); tp.className = "mo-tip"; document.body.appendChild(tp); } const txt = t.getAttribute("data-tip") || ""; if (tp._t !== txt) { tp._t = txt; const L = txt.split("\n"); tp.innerHTML = `<b>${esc(L[0])}</b>` + L.slice(1).map(x => `<div>${esc(x)}</div>`).join(""); } tp.style.display = "block"; const w = tp.offsetWidth, h = tp.offsetHeight; tp.style.left = Math.max(8, Math.min(window.innerWidth - w - 8, e.clientX + 14)) + "px"; tp.style.top = Math.max(8, Math.min(window.innerHeight - h - 8, e.clientY + 16)) + "px"; };
    host.addEventListener("mousemove", mu); host.addEventListener("mouseleave", () => { if (tp) tp.style.display = "none"; });
  }
  /* grupo de cada observación del PCA: las observaciones del PCA son las filas completas, así que se busca por su fila original */
  const asgScores = (P2, A) => P2 && P2.scoresIdx && A ? P2.scores.map((_, i) => A[P2.scoresIdx[i]]) : A;
  const conPerfil = f => (...a) => String(f(...a)).replace('<article class="mo-gr"', '<article class="mo-gr mo-ancho mo-pcx"');
  const ancho = f => (...a) => String(f(...a)).replace('<article class="mo-gr"', '<article class="mo-gr mo-ancho"');
  /* descripción corta del perfil de cada componente según las variables que más pesan (positivas y negativas) */
  function perfilesPC(P2) {
    const car = (P2.cargas || []).filter(c => c.pc && c.pc.length), nc = Math.min(4, car.length ? car[0].pc.length : 0), f = factorPct(P2.varianza, P2.acumulada); if (!nc) return "";
    const lista = a => a.length > 1 ? a.slice(0, -1).map(x => `<b>${esc(x)}</b>`).join(", ") + " y <b>" + esc(a[a.length - 1]) + "</b>" : a.length ? `<b>${esc(a[0])}</b>` : "";
    const filas = Array.from({ length: nc }, (_, k) => {
      const ord = car.filter(c => nz(c.pc[k])).sort((a, b) => Math.abs(b.pc[k]) - Math.abs(a.pc[k])), fuertes = ord.filter(c => Math.abs(c.pc[k]) >= .3), base = fuertes.length ? fuertes : ord.slice(0, 3), pos = base.filter(c => c.pc[k] > 0).slice(0, 3).map(c => nombreV(c.var)), neg = base.filter(c => c.pc[k] < 0).slice(0, 3).map(c => nombreV(c.var)), v = (P2.varianza || [])[k];
      let t = ""; if (pos.length && neg.length) t = `Distingue las observaciones con más ${lista(pos)} de las que tienen más ${lista(neg)}.`; else if (pos.length) t = `Crece cuando aumentan ${lista(pos)}: resume sobre todo ese bloque de variables.`; else if (neg.length) t = `Crece cuando disminuyen ${lista(neg)} (las variables pesan en sentido negativo).`; else t = "Ninguna variable pesa claramente en este componente.";
      if (!fuertes.length && ord.length) t += " Los pesos son moderados (menores a 0.3): la descripción es aproximada.";
      return `<div class="mo-pcd-i"><span class="mo-pcd-k">PC${k + 1}</span><div><b>Componente ${k + 1}</b>${nz(v) ? ` <i>${gN(v * f, 1)} % de la varianza</i>` : ""}<p>${t}</p></div></div>`;
    });
    return `<div class="mo-pcd"><div class="mo-pcd-h">Perfil de cada componente</div>${filas.join("")}</div>`;
  }
  /* varianza explicada: barras por componente y línea de la varianza acumulada (la escala se decide con la serie completa, no valor por valor) */
  const factorPct = (a, b) => Math.max(...(b && b.length ? b : a).filter(nz), 0) <= 1.0001 ? 100 : 1;
  function svgVarianza(v, cum) {
    const f = factorPct(v, cum), V = (v || []).map(x => Math.max(0, x * f)), C = (cum && cum.length ? cum : V.reduce((a, x) => (a.push((a.length ? a[a.length - 1] : 0) + x), a), [])).map(x => Math.min(100, x * f)), n = V.length;
    if (!n) return '<div class="mo-mut">Sin datos para graficar.</div>'; const W = 560, H = 300, ml = 46, mr = 14, mt = 28, mb = 40, bw = (W - ml - mr) / n, X = i => ml + bw * (i + .5), Y = p => mt + (H - mt - mb) * (1 - p / 100), paso = n <= 12 ? 1 : n <= 24 ? 2 : n <= 50 ? 5 : 10;
    return `<svg viewBox="0 0 ${W} ${H}" class="mo-svg" role="img" aria-label="Varianza explicada por componente">
      ${[0, 25, 50, 75, 100].map(g => `<line x1="${ml}" x2="${W - mr}" y1="${Y(g)}" y2="${Y(g)}" stroke="#eef2f7"/><text x="${ml - 6}" y="${Y(g) + 3}" text-anchor="end" class="ax">${g}</text>`).join("")}
      ${V.map((x, i) => `<rect x="${X(i) - Math.max(1, bw * .36)}" y="${Y(x)}" width="${Math.max(2, bw * .72)}" height="${Math.max(0, Y(0) - Y(x))}" rx="1.5" fill="#4f46e5"><title>Componente ${i + 1}: ${gN(x, 1)} % (acumulada ${gN(C[i], 1)} %)</title></rect>`).join("")}
      <polyline points="${C.map((x, i) => X(i) + "," + Y(x)).join(" ")}" fill="none" stroke="#f59e0b" stroke-width="2.2" stroke-linejoin="round"/>${C.length <= 30 ? C.map((x, i) => `<circle cx="${X(i)}" cy="${Y(x)}" r="2.6" fill="#f59e0b" stroke="#fff" stroke-width="1"/>`).join("") : ""}
      ${V.map((_, i) => (i + 1) % paso === 0 || i === 0 ? `<text x="${X(i)}" y="${H - mb + 15}" text-anchor="middle" class="ax">${i + 1}</text>` : "").join("")}
      <text x="${(ml + W - mr) / 2}" y="${H - 6}" text-anchor="middle" class="lb">Componente</text><text transform="translate(12 ${(mt + H - mb) / 2}) rotate(-90)" text-anchor="middle" class="lb">% de la varianza</text>
      <rect x="${ml}" y="6" width="11" height="11" rx="2" fill="#4f46e5"/><text x="${ml + 16}" y="15" class="lb">Por componente</text><line x1="${ml + 124}" x2="${ml + 142}" y1="11.5" y2="11.5" stroke="#f59e0b" stroke-width="2.4"/><text x="${ml + 148}" y="15" class="lb">Acumulada</text></svg>`;
  }
  /* biplot estándar del PCA: observaciones en los dos primeros componentes (puntos) y variables que más pesan (flechas) */
  function svgBiplot(P2, asg) {
    const SS = (P2.scores || []).slice(0, 2500), kk = sinExtremos(SS.map(s => s[0]), SS.map(s => s[1])), S0 = kk.map(i => SS[i]), asg0 = asg ? kk.map(i => asg[i]) : null, car = (P2.cargas || []).filter(c => c.pc && c.pc.length > 1 && nz(c.pc[0]) && nz(c.pc[1])).sort((a, b) => Math.hypot(b.pc[0], b.pc[1]) - Math.hypot(a.pc[0], a.pc[1])).slice(0, 10);
    if (!S0.length) return '<div class="mo-mut">Sin datos para graficar.</div>'; const W = 640, H = 470, ml = 56, mr = 24, mt = 14, mb = 48, mxL = Math.max(1e-9, ...car.map(c => Math.hypot(c.pc[0], c.pc[1])));
    let x0 = Math.min(...S0.map(s => s[0])), x1 = Math.max(...S0.map(s => s[0])), y0 = Math.min(...S0.map(s => s[1])), y1 = Math.max(...S0.map(s => s[1])); const k = .85 * Math.min(Math.max(Math.abs(x0), Math.abs(x1)), Math.max(Math.abs(y0), Math.abs(y1))) / mxL, T = car.map(c => [c.pc[0] * k, c.pc[1] * k, c]);
    T.forEach(t => { x0 = Math.min(x0, t[0] * 1.18); x1 = Math.max(x1, t[0] * 1.18); y0 = Math.min(y0, t[1] * 1.18); y1 = Math.max(y1, t[1] * 1.18); }); const px = (x1 - x0) * .04 || 1, py = (y1 - y0) * .04 || 1; x0 -= px; x1 += px; y0 -= py; y1 += py;
    const X = v => ml + (W - ml - mr) * (v - x0) / (x1 - x0), Y = v => mt + (H - mt - mb) * (1 - (v - y0) / (y1 - y0)), pv = i => { const v = (P2.varianza || [])[i]; return nz(v) ? gN(v * factorPct(P2.varianza, P2.acumulada), 1) + " % de la varianza" : ""; };
    const tk = (a, b) => [0, .25, .5, .75, 1].map(t => a + (b - a) * t), cr = asg0 || [];
    return `<svg viewBox="0 0 ${W} ${H}" class="mo-svg" role="img" aria-label="Biplot de componentes principales"><defs><marker id="bpa" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#dc2626"/></marker></defs>
      ${tk(x0, x1).map(v => `<text x="${X(v)}" y="${H - mb + 16}" text-anchor="middle" class="ax">${gN(v, 1)}</text>`).join("")}${tk(y0, y1).map(v => `<text x="${ml - 8}" y="${Y(v) + 3}" text-anchor="end" class="ax">${gN(v, 1)}</text>`).join("")}
      <rect x="${ml}" y="${mt}" width="${W - ml - mr}" height="${H - mt - mb}" fill="none" stroke="#e2e8f0"/><line x1="${X(0)}" x2="${X(0)}" y1="${mt}" y2="${H - mb}" stroke="#cbd5e1" stroke-dasharray="4 3"/><line x1="${ml}" x2="${W - mr}" y1="${Y(0)}" y2="${Y(0)}" stroke="#cbd5e1" stroke-dasharray="4 3"/>
      ${S0.map((s, i) => `<circle cx="${X(s[0])}" cy="${Y(s[1])}" r="2.6" fill="${cr[i] != null && cr[i] >= 0 ? PAL[cr[i] % PAL.length] : "#4f46e5"}" fill-opacity=".55"/>`).join("")}
      ${T.map(([tx, ty, c]) => `<line x1="${X(0)}" y1="${Y(0)}" x2="${X(tx)}" y2="${Y(ty)}" stroke="#dc2626" stroke-width="1.8" marker-end="url(#bpa)"/>`).join("")}
      ${T.map(([tx, ty, c]) => { const nm = String(nombreV(c.var)), t = nm.length > 26 ? nm.slice(0, 25) + "…" : nm, right = tx >= 0; return `<text x="${X(tx) + (right ? 7 : -7)}" y="${Y(ty) + (ty >= 0 ? -5 : 12)}" text-anchor="${right ? "start" : "end"}" font-size="10.5" font-weight="700" fill="#b91c1c" paint-order="stroke" stroke="#fff" stroke-width="3">${esc(t)}</text>`; }).join("")}
      <text x="${(ml + W - mr) / 2}" y="${H - 8}" text-anchor="middle" class="lb">Componente 1 · ${pv(0)}</text><text transform="translate(14 ${(mt + H - mb) / 2}) rotate(-90)" text-anchor="middle" class="lb">Componente 2 · ${pv(1)}</text></svg>`;
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
    const my = Math.max(...c), E = ejes(a, b, 0, my, o); return `<svg viewBox="0 0 ${o.w} ${o.h}" class="mo-svg" role="img"><g class="ejes">${E.h}</g>${c.map((k, i) => `<rect x="${E.sx(a + i * w) + 1}" y="${E.sy(k)}" width="${Math.max(1, E.sx(a + (i + 1) * w) - E.sx(a + i * w) - 2)}" height="${E.sy(0) - E.sy(k)}" fill="${o.c || "#06b6d4"}" fill-opacity=".8"/>`).join("")}${o.ref != null ? `<line x1="${E.sx(o.ref)}" y1="${o.m.t}" x2="${E.sx(o.ref)}" y2="${o.h - o.m.b}" stroke="#ef4444" stroke-width="2" stroke-dasharray="5 3"/><text x="${E.sx(o.ref) + 4}" y="${o.m.t + 10}" class="ax" fill="#ef4444">0</text>` : ""}</svg>`;
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
  const hallHtml = res => (res.hallazgos || []).length ? `<section class="mo-sx2"><h3>Hallazgos principales</h3><ul class="mo-hz">${res.hallazgos.map(h => `<li>${esc(hum(h))}</li>`).join("")}</ul></section>` : "";
  function descarga(nombre, filas) { const csv = filas.map(r => r.map(v => { const t = v == null ? "" : String(v); return /[",\n;]/.test(t) ? '"' + t.replace(/"/g, '""') + '"' : t; }).join(",")).join("\r\n"), a = document.createElement("a"); a.href = URL.createObjectURL(new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8" })); a.download = nombre; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 2000); }
  /* mapa de los puntos por grupo + tabla con su clasificación; el buscador filtra la tabla y resalta y acerca el mapa a lo encontrado */
  function mapaGrupos(r) {
    leaflet(() => {
      const el = $("#moMapRes"), C = r.clusters, ds = S.ds; if (!el || !window.L || !ds || !C) return; if (S._mapRes) { try { S._mapRes.remove(); } catch (_) { } }
      const A = C.asignacion || [], et = C.etiquetaSugerida || [], m = L.map(el).setView([ds.lat[0] || 23.8, ds.lng[0] || -102.3], 10); L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 18, maxNativeZoom: 16, attribution: "Esri" }).addTo(m); S._mapRes = m;
      const Pts = [], col = g => g >= 0 ? PAL[g % PAL.length] : "#94a3b8", nomG = g => g >= 0 ? "Grupo " + (g + 1) : "Sin grupo";
      ds.lat.forEach((la, i) => { if (!isFinite(+la) || !isFinite(+ds.lng[i])) return; const g = A[i] != null ? A[i] : -1; Pts.push({ i, id: String(ds.ids && ds.ids[i] != null ? ds.ids[i] : "P" + (i + 1)), lat: +la, lng: +ds.lng[i], g, txt: ((ds.ids && ds.ids[i]) + " " + nomG(g) + " " + (g >= 0 ? et[g] || "" : "") + " " + (+la).toFixed(5) + " " + (+ds.lng[i]).toFixed(5)).toLowerCase() }); });
      const st = Math.max(1, Math.ceil(Pts.length / 5000)), MK = {}, grp = L.featureGroup().addTo(m); Pts.forEach((p, k) => { if (k % st) return; const mk = L.circleMarker([p.lat, p.lng], { radius: 5, color: "#fff", weight: 1, fillColor: col(p.g), fillOpacity: .85 }).bindTooltip(`<b>${esc(p.id)}</b> · ${nomG(p.g)}`).addTo(grp); mk.on("click", () => elige(p.i, false)); MK[p.i] = mk; });
      const ver = Pts.slice(); let lista = ver, nr = 0, SEL = null, q = ""; const tb = $("#mgTb"), tw = $("#mgTw");
      const estilo = (p, modo) => { const mk = MK[p.i]; if (!mk) return; if (modo === "sel") mk.setStyle({ radius: 11, color: "#0f172a", weight: 3, fillOpacity: 1 }).bringToFront(); else if (modo === "hit") mk.setStyle({ radius: 7, color: "#0f172a", weight: 1.6, fillOpacity: .95 }); else if (modo === "dim") mk.setStyle({ radius: 3, color: "#fff", weight: .5, fillOpacity: .15 }); else mk.setStyle({ radius: 5, color: "#fff", weight: 1, fillOpacity: .85 }); };
      const fila = p => `<tr data-mgp="${p.i}" class="${SEL === p.i ? "on" : ""}"><td colspan="2"><div class="mo-mgr"><span class="mo-mgc" style="--c:${col(p.g)}"></span><div class="mo-mgn"><b>${esc(p.id)}</b><small class="mo-mgs">${p.lat.toFixed(5)}, ${p.lng.toFixed(5)}</small></div><span class="mo-mgk" style="--c:${col(p.g)}">${nomG(p.g)}</span></div></td></tr>`;
      const mas = reinicia => { if (reinicia) { tb.innerHTML = ""; nr = 0; tw.scrollTop = 0; } const b = Math.min(lista.length, nr + 100); if (b > nr) tb.insertAdjacentHTML("beforeend", lista.slice(nr, b).map(fila).join("")); nr = b; if (!lista.length) tb.innerHTML = '<tr><td colspan="2" class="mo-mut">Ningún punto coincide con la búsqueda.</td></tr>'; const sx = $("#mgSt"); if (sx) sx.innerHTML = `Mostrando <b>${gN(b, 0)}</b> de <b>${gN(lista.length, 0)}</b> puntos${b < lista.length ? ` · faltan <b>${gN(lista.length - b, 0)}</b> por cargar (se cargan solos al llegar al final)` : ""}`; };
      const nn = $("#mgN"); const info = () => { if (nn) nn.textContent = q ? `${gN(lista.length, 0)} de ${gN(Pts.length, 0)} puntos coinciden` : `${gN(Pts.length, 0)} puntos`; };
      const encuadra = () => { const L2 = lista.filter(p => MK[p.i]); if (!L2.length) return; if (L2.length === 1) m.flyTo([L2[0].lat, L2[0].lng], 17, { duration: .6 }); else m.fitBounds(L.latLngBounds(L2.map(p => [p.lat, p.lng])), { padding: [30, 30], maxZoom: 17 }); };
      const aplica = () => { const qq = q.trim().toLowerCase(); lista = qq ? Pts.filter(p => p.txt.includes(qq)) : Pts; const set = new Set(lista.map(p => p.i)); SEL = null; Pts.forEach(p => estilo(p, qq ? (set.has(p.i) ? "hit" : "dim") : "base")); mas(true); info(); if (qq) encuadra(); else if (Pts.length) m.fitBounds(grp.getBounds(), { padding: [20, 20], maxZoom: 15 }); };
      const elige = (i, desdeTabla) => { const p = Pts.find(x => x.i === i); if (!p) return; if (SEL != null) { const a = Pts.find(x => x.i === SEL); if (a) estilo(a, q.trim() ? (lista.includes(a) ? "hit" : "dim") : "base"); } SEL = i; estilo(p, "sel"); $$("#mgTb tr.on").forEach(t => t.classList.remove("on")); let tr = $(`#mgTb tr[data-mgp="${i}"]`); if (!tr) { tb.insertAdjacentHTML("afterbegin", fila(p)); tr = tb.firstElementChild; } tr.classList.add("on"); if (!desdeTabla) tw.scrollTo({ top: Math.max(0, tr.offsetTop - 40), behavior: "smooth" }); m.flyTo([p.lat, p.lng], Math.max(m.getZoom(), 16), { duration: .6 }); if (MK[i]) MK[i].openTooltip(); };
      S._mg = { aplica, elige, setQ: v => { q = v; aplica(); }, todos: () => { const inp = $("#mgQ"); if (inp) inp.value = ""; q = ""; aplica(); } };
      tw.addEventListener("scroll", () => { if (tw.scrollTop + tw.clientHeight >= tw.scrollHeight - 160 && nr < lista.length) mas(false); }); mas(true); info(); if (Pts.length) m.fitBounds(grp.getBounds(), { padding: [20, 20], maxZoom: 15 });
    });
  }
  function mapaPuntos(id, colores, rotulos) {
    leaflet(() => { const el = $("#" + id); if (!el || !window.L || !S.ds) return; if (S._mapRes) { try { S._mapRes.remove(); } catch (_) { } } const m = L.map(el).setView([S.ds.lat[0] || 23.8, S.ds.lng[0] || -102.3], 10); L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 18, maxNativeZoom: 16, attribution: "Esri" }).addTo(m); S._mapRes = m; const g = []; const st = Math.max(1, Math.ceil(S.ds.lat.length / 4000));
      S.ds.lat.forEach((la, i) => { if (i % st || colores[i] == null) return; const mk = L.circleMarker([la, S.ds.lng[i]], { radius: 5, color: "#fff", weight: 1, fillColor: colores[i], fillOpacity: .85 }).bindTooltip(esc(rotulos[i] || "")); mk.addTo(m); g.push([la, S.ds.lng[i]]); }); if (g.length) m.fitBounds(g, { padding: [20, 20], maxZoom: 15 }); }); }

  /* ── 6 · Resultados ── */
  const MNOM = { r2: "R²", r2_ajustado: "R² ajustado", rmse: "RMSE (error típico)", mae: "MAE (error absoluto medio)", mape: "MAPE (%)", sesgo: "Sesgo medio", ratio_rmse_sd: "RMSE / desviación de la respuesta", exactitud: "Exactitud", exactitud_balanceada: "Exactitud balanceada", precision_macro: "Precisión (promedio)", recall_macro: "Sensibilidad (promedio)", f1_macro: "F1 (promedio)", precision: "Precisión", recall: "Sensibilidad", f1: "F1", auc: "AUC", log_loss: "Pérdida logarítmica", kappa: "Kappa" };
  const MACOT = new Set(["r2", "r2_ajustado", "exactitud", "exactitud_balanceada", "precision_macro", "recall_macro", "f1_macro", "precision", "recall", "f1", "auc", "kappa"]);
  const mNom = k => MNOM[k] || String(k).replace(/_/g, " ");
  /* de dónde viene una variable en los resultados: indicador de la biblioteca o dato de tu archivo / DENUE (se reconoce por nombre o por etiqueta) */
  const origenEtq = v => { const t = String(v).replace(/^(num__|cat__)/, "").replace(/__faltante$/, ""); const cd = S.ds ? S.ds.cols.find(c => c.label === t) : null; if (BIBK()[t] || (cd && BIBK()[cd.name]) || BIB().some(b => b.n === t)) return { k: "lib", t: "Biblioteca" }; const o = origen("\u0000"); return { k: o.k, t: o.t }; };
  const COL_LIB = "#7c3aed", COLU = { denue: "#ea580c", archivo: "#2563eb" }, colOrigen = v => { const k = origenEtq(v).k; return k === "lib" ? COL_LIB : COLU[k] || "#2563eb"; };
  const chipEtq = v => { if (v === "(Intercepto)") return ""; const o = origenEtq(v); return `<span class="mo-or ${o.k}" title="${o.k === "lib" ? "Indicador de la biblioteca nacional" : "Dato de tu archivo o del DENUE"}">${o.t}</span> `; };
  const leyendaOrigen = () => `<div class="mo-lgo"><span><i style="background:${COL_LIB}"></i>Indicador de la biblioteca</span><span><i style="background:${colOrigen("\u0000")}"></i>${origen("\u0000").t === "DENUE" ? "Dato del DENUE" : "Dato de tu archivo"}</span></div>`;
  const nombreV = v => { const t = String(v).replace(/^(num__|cat__)/, ""), b = t.replace(/__faltante$/, ""); return nomVar(b) + (t !== b ? " (¿faltaba el dato?)" : ""); };
  /* nombres internos de variables dentro de textos del motor → nombres legibles */
  const hum = t => String(t == null ? "" : t).replace(/[A-Za-z0-9_:.\-]+/g, tok => { const b = tok.replace(/^(num__|cat__)/, "").replace(/__faltante$/, ""); if (!(BIBK()[b] || colsProy().some(c => c.name === b))) return tok; return "«" + nomVar(b) + "»" + (/__faltante$/.test(tok) ? " (¿faltaba el dato?)" : ""); });
  function resultadosHtml(r) {
    const sup = r.enfoque === "sup"; let h = `${kpisHtml(r)}${hallHtml(r)}<div class="mo-gg">`;
    if (sup) {
      const imp = (r.importancias || []).slice(0, 15); if (imp.length) h += bloque("Variables que más influyen", leyendaOrigen() + svgBarras(imp.map(i => ({ n: nombreV(i.var), v: i.valor, t: gN(i.valor, 0), c: colOrigen(i.var) }))), inter(r, "importancias", "Mide cuánto empeora el modelo cuando se desordena cada variable: más largo, más influyente. La escala va de 0 a 100."));
      const P = r.predicciones || {};
      if (r.tipoY === "regresion" && P.real) { const res2 = P.real.map((v, i) => v - P.pred[i]); h += bloque("Valores reales frente a predichos (datos de prueba)", svgDisp(P.real, P.pred, { diag: true, xl: "Real", yl: "Predicho" }), inter(r, "real_vs_pred", "Cada punto es una observación que el modelo no vio al entrenar. Cuanto más cerca de la línea punteada, mejor la predicción.")); h += bloque("Errores del modelo (residuos)", svgHist(res2, { xl: "Error = real − predicho" + ((S.ds && S.M && (S.ds.cols.find(c => c.name === S.M.y) || {}).unit) ? " (" + S.ds.cols.find(c => c.name === S.M.y).unit + ")" : " (unidades de la respuesta)") + " · 0 = predicción exacta", yl: "Número de observaciones", ref: 0 }), inter(r, "residuos", "Lo ideal es una campana centrada en cero: errores pequeños y sin sesgo hacia arriba o abajo.")); }
      if (r.tipoY !== "regresion" && r.matrizConfusion) { const C = r.matrizConfusion; h += bloque("Matriz de confusión (datos de prueba)", svgCalor(C.m, C.clases.map(c => "real: " + c), C.clases.map(c => "pred: " + c), { div: false, entero: true, rot: true }), inter(r, "confusion", "Las filas son lo que realmente fue y las columnas lo que predijo el modelo. La diagonal son los aciertos.")); }
      if (r.roc && r.roc.fpr) h += bloque(`Curva ROC · AUC ${gN(r.roc.auc, 3)}`, svgLineas([{ n: "Modelo", x: r.roc.fpr, y: r.roc.tpr, pts: false }], { diag: true, x0: 0, x1: 1, y0: 0, y1: 1, xl: "Falsos positivos", yl: "Verdaderos positivos" }), inter(r, "roc", "Cuanto más se pega la curva a la esquina superior izquierda, mejor separa las clases. La diagonal es el azar (AUC 0.5)."));
      if ((r.coeficientes || []).length) h += `<article class="mo-gr mo-ancho"><h4>${r.tipoY === "regresion" ? "Coeficientes del modelo" : "Coeficientes y razones de momios"}</h4><div class="mo-tw"><table class="mo-t"><thead><tr><th>Variable</th><th>Coeficiente</th><th>Error</th><th>t</th><th>p</th><th>IC 95 %</th>${r.tipoY !== "regresion" ? "<th>Razón de momios</th>" : ""}</tr></thead><tbody>${r.coeficientes.map(c => `<tr><td>${chipEtq(c.nombre || c.var)}${esc(nombreV(c.var))}</td><td>${gN(c.coef, 4)}</td><td>${gN(c.error, 4)}</td><td>${gN(c.t, 2)}</td><td class="${c.p != null && c.p < .05 ? "mo-sig" : ""}">${c.p == null ? "—" : c.p < .001 ? "< 0.001" : gN(c.p, 3)}</td><td>${c.ic95 ? gN(c.ic95[0], 3) + " a " + gN(c.ic95[1], 3) : "—"}</td>${r.tipoY !== "regresion" ? `<td>${gN(c.oddsRatio, 3)}</td>` : ""}</tr>`).join("")}</tbody></table></div><div class="mo-int"><b>Cómo leerla</b><p>${esc(inter(r, "coeficientes", "El signo indica si la variable sube o baja la respuesta; una p menor a 0.05 sugiere que el efecto es distinguible del azar."))}</p></div></article>`;
      if (P.idx) h += `<article class="mo-gr mo-ancho"><button type="button" class="mo-ghost" data-fl="csv">Descargar predicciones (CSV)</button></article>`;
    } else {
      let hCl1 = "", hCl = "", hS2sc = ""; if (r.clusters) { const hAnt = h; h = ""; const C = r.clusters, k = C.k; h += `<article class="mo-gr mo-ancho"><h4>${k} grupos encontrados</h4><div class="mo-cg">${(C.perfil || []).map((p, i) => `<div class="mo-cc" style="--c:${PAL[i % PAL.length]}"><b>${(C.etiquetaSugerida || [])[i] ? esc(C.etiquetaSugerida[i]) : "Grupo " + (i + 1)}</b><span>${gN(p.n, 0)} observaciones (${gN(p.n / Math.max(1, (C.asignacion || []).filter(a => a >= 0).length) * 100, 0)} %)</span><ul>${(p.variablesDistintivas || []).slice(0, 3).map(v => `<li>${esc(nombreV(v.var))}: ${v.z > 0 ? "alto" : "bajo"} (${v.z > 0 ? "+" : ""}${gN(v.z, 1)} σ)</li>`).join("")}</ul></div>`).join("")}</div></article>`; hCl1 = h; h = "";
        const vs = [...new Set((C.perfil || []).flatMap(p => (p.variablesDistintivas || []).map(v => v.var)))].slice(0, 12); if (vs.length) h += bloque("Perfil de cada grupo (diferencia respecto al promedio)", svgCalor((C.perfil || []).map(p => vs.map(v => { const d = (p.variablesDistintivas || []).find(x => x.var === v); return d ? d.z : null; })), (C.perfil || []).map((_, i) => "Grupo " + (i + 1)), vs.map(nombreV), { rot: true }), inter(r, "perfil_clusters", "Azul: el grupo está por encima del promedio en esa variable; rojo: por debajo. Sirve para ponerle nombre a cada grupo."));
        if ((C.elbow || []).length) h += bloque("¿Cuántos grupos? (silueta por número de grupos)", svgLineas([{ n: "Silueta", x: C.elbow.map(e => e.k), y: C.elbow.map(e => e.silueta), c: "#10b981" }], { xl: "Número de grupos", yl: "Silueta" }), inter(r, "elbow", "Un valor de silueta alto indica grupos compactos y bien separados. Se elige el número de grupos donde es más alta.")); h += `<article class="mo-gr mo-ancho mo-mgx"><h4>Mapa de los puntos por grupo</h4>${leyendaGrupos(C)}<div class="mo-mgb"><label class="mo-q" id="mgQw"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg><input id="mgQ" type="search" placeholder="Buscar un punto, grupo o coordenada…" autocomplete="off"></label><span class="mo-mut" id="mgN"></span><button type="button" class="mo-ghost" data-mg="todos">Ver todos</button></div><div class="mo-mgg"><div id="moMapRes" class="mo-map"></div><div class="mo-mgt"><div class="mo-tw mo-tsc" id="mgTw"><table class="mo-t"><thead><tr><th colspan="2">Punto · coordenadas · grupo</th></tr></thead><tbody id="mgTb"></tbody></table></div><div id="mgSt" class="mo-mut"></div></div></div><div class="mo-chs"><button type="button" class="mo-ghost" data-fl="csv">Descargar asignación de grupos (CSV)</button></div></article>`; hCl = h; h = hAnt; }
      if (r.pca) { const P2 = r.pca, cum = P2.acumulada || [];
        const hVar = bloque("Varianza explicada por componente", svgVarianza(P2.varianza, cum), inter(r, "pca_var", `Los primeros ${P2.nComponentes80 || "pocos"} componentes recogen ~80 % de la información: se pueden resumir las variables con ellos.`));
        let hSc = ""; if (P2.scores && P2.scores.length) { const asg = r.clusters ? asgScores(P2, r.clusters.asignacion) : null, ks = sinExtremos(P2.scores.map(s => s[0]), P2.scores.map(s => s[1])), om = P2.scores.length - ks.length; hSc = bloque("Observaciones en los dos primeros componentes", leyendaGrupos(r.clusters) + svgDisp(ks.map(i => P2.scores[i][0]), ks.map(i => P2.scores[i][1]), { xl: "Componente 1", yl: "Componente 2", col: asg ? ks.map(i => asg[i]) : [] }), inter(r, "pca_scores", "Cada punto es una observación; los que quedan juntos se parecen. Con grupos, el color indica a cuál pertenece.") + (om ? ` Se omitieron ${om} observación${om === 1 ? "" : "es"} extrema${om === 1 ? "" : "s"} para que la escala sea legible (siguen dentro del análisis).` : "")); }

        let hBp = ""; if ((P2.cargas || []).length && (P2.scores || []).length) hBp = bloque("Biplot: observaciones (puntos) y variables (flechas)", leyendaGrupos(r.clusters) + svgBiplot(P2, r.clusters ? asgScores(P2, r.clusters.asignacion) : null), inter(r, "pca_biplot", "Cada punto es una observación y cada flecha una variable: apunta hacia donde esa variable aumenta. Las flechas largas pesan mucho en estos dos componentes; las que apuntan al mismo lado se mueven juntas y las opuestas, en sentido contrario. Los puntos cercanos a la punta de una flecha tienen valores altos en esa variable."));

        const hCmp = compGrupos(r.clusters); if (r.clusters) h += '<h3 class="mo-sech"><span>1</span>Resultados del PCA<small>Componentes, varianza explicada y qué variables los forman</small></h3>'; h += `<div class="mo-par">${hVar}${hBp}</div>`; hS2sc = hCmp ? `<div class="mo-par mo-par-sc">${hSc}${hCmp}</div>` : hSc.replace('<article class="mo-gr"', '<article class="mo-gr mo-ancho"');
        if ((P2.cargas || []).length) { const top = P2.cargas.slice().sort((a, b) => Math.abs(b.pc[0] || 0) + Math.abs(b.pc[1] || 0) - Math.abs(a.pc[0] || 0) - Math.abs(a.pc[1] || 0)).slice(0, 14); h += conPerfil(bloque)("Qué variables componen cada componente", svgCalor(top.map(c => c.pc.slice(0, 4)), top.map(c => nombreV(c.var)), ["PC1", "PC2", "PC3", "PC4"].slice(0, (top[0].pc || []).slice(0, 4).length)), inter(r, "pca_cargas", "Cuanto más intenso el color, más pesa la variable en ese componente; el signo indica la dirección."), perfilesPC(P2)); } }
      if (r.pca) { if (r.clusters) h += '<h3 class="mo-sech"><span>2</span>Agrupamiento<small>Cada caso asignado a un grupo de casos similares, calculado con los componentes</small></h3>' + hCl1 + hS2sc + hCl; } else h += hCl1 + hCl;
      if (r.correlaciones) { const C = r.correlaciones, vs = C.vars.slice(0, 18); h += bloque("Correlaciones entre variables (Pearson)", svgCalor(C.pearson.slice(0, 18).map(f => f.slice(0, 18)), vs.map(nombreV), vs.map(nombreV), { rot: true, max: 1 }), inter(r, "correlaciones", "Azul: cuando una sube la otra también; rojo: cuando una sube la otra baja. Cerca de 0: no hay relación lineal.")); h += `<article class="mo-gr"><h4>Relaciones más fuertes</h4><table class="mo-t"><thead><tr><th>Variable A</th><th>Variable B</th><th>Pearson</th><th>Spearman</th></tr></thead><tbody>${(C.paresFuertes || []).slice(0, 10).map(p => `<tr><td>${esc(nombreV(p.a))}</td><td>${esc(nombreV(p.b))}</td><td>${gN(p.r, 2)}</td><td>${gN(p.rho, 2)}</td></tr>`).join("")}</tbody></table></article>`; }
      if (r.anomalias) { const A = r.anomalias; h += bloque("Casos atípicos (puntaje de anomalía)", svgHist(A.puntaje.filter(nz), { xl: "Puntaje", ref: A.umbral }), inter(r, "anomalias", "A la derecha de la línea roja están las observaciones que se alejan mucho del patrón del resto: conviene revisarlas.")); h += `<article class="mo-gr"><h4>Observaciones más atípicas</h4><table class="mo-t"><thead><tr><th>Observación</th><th>Puntaje</th><th>Variables que más aportan</th></tr></thead><tbody>${(A.idx || []).slice(0, 10).map((i, k) => `<tr><td>${esc(S.ds && S.ds.ids ? S.ds.ids[i] : "#" + (i + 1))}</td><td>${gN(A.puntaje[i], 2)}</td><td>${esc((((A.variablesQueMasAportan || []).find(v => v.idx === i) || {}).vars || []).map(nombreV).join(", "))}</td></tr>`).join("")}</tbody></table></article>`; }
    }
    h += "</div>"; return h;
  }
  function postResultados(r) {
    const sup = r.enfoque === "sup"; if (!sup && r.clusters && !S.ds) { aseguraDs().then(ok => { if (ok && S.ds && S.sec === "resultados") postResultados(r); }).catch(() => { }); return; }
    if (!sup && r.clusters && S.ds) mapaGrupos(r);
  }

  /* ── 7 · Calidad del modelo ── */
  const gauge = (k, v) => !nz(v) ? "—" : MACOT.has(k) ? `<div class="mo-ga"><i style="width:${Math.max(0, Math.min(100, v * 100))}%;background:${v >= .8 ? "#059669" : v >= .6 ? "#d97706" : "#dc2626"}"></i><b>${gN(v, 3)}</b></div>` : gN(v, 3);
  function calidadHtml(r) {
    const sup = r.enfoque === "sup"; let h = "";
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
    h += `<section class="mo-sx2"><h3>Datos y preprocesamiento</h3><ul class="mo-rsm">${((r.prep || {}).pasos || []).map(p => `<li>${esc(hum(p))}</li>`).join("")}</ul><div class="mo-nota">${r.prep && r.prep.nTrain != null ? `Entrenamiento: ${gN(r.prep.nTrain, 0)} observaciones · Prueba: ${gN(r.prep.nTest, 0)}. ` : ""}${((r.prep || {}).descartadas || []).length ? "Variables descartadas: " + r.prep.descartadas.map(d => esc(nombreV(d.name) + " (" + d.razon + ")")).join(", ") + "." : ""}</div></section>`;
    return h;
  }

  /* ── eventos del flujo ── */
  const guardaPrep = () => { S.M.prepOk = false; };
  function flujoClic(e) {
    const t = e.target; let b; if (window.MLVal && window.MLVal.clic(e, HV)) return true; if (t.closest("[data-dmas]")) { S.dt.n += 100; datosPinta(); return true; } if (t.closest("[data-dcsv]")) { datosCsv(); return true; } if (simClic(e)) return true; if (cgClic(e)) return true; { const b2 = e.target.closest && e.target.closest("[data-mgp]"); if (b2 && S._mg) { S._mg.elige(+b2.dataset.mgp, true); return true; } const b3 = e.target.closest && e.target.closest("[data-mg=todos]"); if (b3 && S._mg) { S._mg.todos(); return true; } }
    if ((b = t.closest("[data-v]")) && S.sec === "variables") { const k = b.dataset.v, s = new Set(S.M.vars || []); s.has(k) ? s.delete(k) : s.add(k); S.M.vars = [...s]; resetModelo(); const y = $("#moMain").scrollTop; secVariables(); $("#moMain").scrollTop = y; return true; }
    if ((b = t.closest("[data-vg]"))) { e.preventDefault(); const ks = b.dataset.vg.split("|").filter(Boolean), s = new Set(S.M.vars || []); ks.forEach(k => b.dataset.on === "1" ? s.add(k) : s.delete(k)); S.M.vars = [...s]; resetModelo(); const y = $("#moMain").scrollTop; secVariables(); $("#moMain").scrollTop = y; return true; }
    if ((b = t.closest("[data-vermod]"))) { S.resVer = b.dataset.vermod; secResultados(); return true; }
    if ((b = t.closest("[data-calt]"))) { const sec = b.closest(".mo-cal"), x = $(".mo-calx", sec), ab = x.hidden; x.hidden = !ab; b.setAttribute("aria-expanded", String(ab)); sec.classList.toggle("on", ab); return true; }
    if ((b = t.closest("[data-jmp]"))) { const el = $(b.dataset.jmp === "cal" ? "#moSecCal" : "#moSecRes"); if (b.dataset.jmp === "cal" && el) { const x = $(".mo-calx", el); if (x && x.hidden) { x.hidden = false; $(".mo-calh", el).setAttribute("aria-expanded", "true"); el.classList.add("on"); } } if (el) el.scrollIntoView({ behavior: "smooth", block: "start" }); return true; }
    if ((b = t.closest("[data-vcat]"))) { S.vcat = b.dataset.vcat; secVariables(); return true; }
    if ((b = t.closest("[data-enf]"))) { const M = S.M; if (M.enfoque !== b.dataset.enf) { M.enfoque = b.dataset.enf; M.y = null; M.metodo = null; M.prep = null; M.prepOk = false; const P = perfiles(); M.x = P.filter(p => p.tipo !== "texto" && !p.constante).map(p => p.name); if (M.enfoque === "val") { M.y = colPrecio(); M.x = M.x.filter(k => k !== M.y); } guardaModelo(); } secEnfoque(); return true; }
    if ((b = t.closest("[data-x]"))) { const k = b.dataset.x, s = new Set(S.M.x || []); s.has(k) ? s.delete(k) : s.add(k); S.M.x = [...s]; S.M.metodo = null; S.M.prep = null; S.M.prepOk = false; guardaModelo(); const y = $("#moMain").scrollTop; secEnfoque().then(() => { $("#moMain").scrollTop = y; }); return true; }
    if ((b = t.closest("[data-xg]"))) { const P = perfiles().filter(p => p.tipo !== "texto" && !p.constante && !(S.M.enfoque === "sup" && p.name === S.M.y)); S.M.x = b.dataset.xg === "1" ? P.map(p => p.name) : []; S.M.metodo = null; S.M.prep = null; S.M.prepOk = false; guardaModelo(); secEnfoque(); return true; }
    if ((b = t.closest("[data-met]"))) { if (b.disabled) return true; S.M.metodo = b.dataset.met; S.M.prep = null; S.M.prepOk = false; guardaModelo(); const y = $("#moMain").scrollTop; secMetodos().then(() => { $("#moMain").scrollTop = y; }); return true; }
    if ((b = t.closest("[data-fl]"))) { const a = b.dataset.fl; if (a === "reporte") { reporte(); return true; }
      if (a === "construir") { construyeDatos(); return true; } if (a === "metodos") { ir("metodos"); return true; } if (a === "prep") { ir("prep"); return true; } if (a === "modelar") { S.M.prepOk = true; guardaModelo(); ir("modelar"); return true; }
      if (a === "correr") { corre(); return true; } if (a === "detener") { S._stop = true; const bt = t.closest("[data-fl]"); if (bt) { bt.disabled = true; bt.textContent = "Deteniendo al terminar el modelo en curso…"; } return true; } if (a === "calidad") { ir("calidad"); return true; } if (a === "cierrapop") { $$(".mo-pop").forEach(popupCierra); return true; }
      if (a === "csv") { const r = resActual(), ds = S.ds; if (!r || !ds) return true; if (r.clusters) { descarga("grupos.csv", [["id", "latitud", "longitud", "grupo"]].concat((r.clusters.asignacion || []).map((g, i) => [ds.ids[i], ds.lat[i], ds.lng[i], g >= 0 ? g + 1 : ""]))); } else if (r.predicciones) { const P = r.predicciones; descarga("predicciones.csv", [["id", "latitud", "longitud", "real", "predicho"]].concat((P.idx || []).map((i, k) => [ds.ids[i], ds.lat[i], ds.lng[i], P.real[k], P.pred[k]]))); } return true; } }
    return false;
  }
  function resetModelo() { const M = S.M; M.metodo = null; M.prep = null; M.prepOk = false; M.y = null; M.x = []; M.enfoque = M.enfoque; S.ds = null; S.perfiles = null; const r = $("#vRes"); if (r) r.innerHTML = resumenVars(); }
  function flujoEntrada(e) { const t = e.target; if (t.id === "moDQ") { S.dt = S.dt || { q: "", n: 100 }; S.dt.q = t.value; S.dt.n = 100; datosPinta(); return; } if (window.MLVal && window.MLVal.entrada(e, HV)) return; if (simEntrada(e)) return; if (t.id === "moVQ") { S.vq = t.value; S._vfoco = true; secVariables(); } }
  function flujoCambia(e) {
    const t = e.target; if (window.MLVal && window.MLVal.cambia(e, HV)) return; if (simCambia(e)) return; if (t.id === "moY") { S.M.y = t.value || null; S.M.x = (S.M.x || []).filter(k => k !== S.M.y); if (S.M.y && !(S.M.x || []).length) { S.M.x = perfiles().filter(p => p.tipo !== "texto" && !p.constante && p.name !== S.M.y).map(p => p.name); } S.M.metodo = null; S.M.prep = null; S.M.prepOk = false; guardaModelo(); secEnfoque(); return; }
    if (t.dataset && t.dataset.prep) { S.M.prep.global[t.dataset.prep] = t.value; guardaModelo(); secPrep(); return; } if (t.id === "moK") { S.M.k = t.value ? Math.max(2, Math.min(20, +t.value)) : null; guardaModelo(); }
  }

  /* ═══════════════ Elegir el mejor modelo (compara todos los métodos factibles) + Resultados y calidad en una sola ventana ═══════════════ */
  const resActual = () => { const r = S.res; if (!r) return null; return S.resVer && r._todos && r._todos[S.resVer] ? r._todos[S.resVer] : r; };
  ICO_M.auto = '<svg viewBox="0 0 96 64" aria-hidden="true"><rect x="10" y="38" width="24" height="20" rx="3" fill="#818cf8"/><rect x="36" y="26" width="24" height="32" rx="3" fill="#4f46e5"/><rect x="62" y="44" width="24" height="14" rx="3" fill="#06b6d4"/><path d="M48 4 l3.6 7.2 7.9 1.2 -5.7 5.5 1.3 7.9 -7.1 -3.8 -7.1 3.8 1.3 -7.9 -5.7 -5.5 7.9 -1.2z" fill="#f59e0b"/><g font-family="system-ui" font-weight="800" font-size="11" fill="#fff" text-anchor="middle"><text x="22" y="52">2</text><text x="48" y="48">1</text><text x="74" y="55">3</text></g></svg>';
  function puntajeModelo(r) {
    const reg = r.tipoY === "regresion", M2 = r.metricas || {}, claves = reg ? ["r2"] : ["exactitud_balanceada", "f1_macro", "exactitud"], k = claves.find(c => nz((M2.prueba || {})[c]));
    if (!k) return { score: null }; const te = M2.prueba[k], tr = (M2.entrenamiento || {})[k], cv = ((M2.cv || {}).media || {})[k], base = nz(cv) ? (te + cv) / 2 : te, gap = nz(tr) ? tr - te : 0, score = base - Math.max(0, gap - .10) * .5;
    return { score, test: te, cv: nz(cv) ? cv : null, gap, clave: k, txt: (reg ? "R² " : "exactitud ") + gN(te, 2) };
  }
  async function correAuto() {
    const M = S.M; if (!(await aseguraDs())) { toast("Falta la tabla de datos."); return; } S.perfiles = null; const py = perfilDe(M.y), px = (M.x || []).map(perfilDe).filter(Boolean), n = S.ds.rows.length; let cand = [];
    try { cand = window.MLMotor.sugerirMetodos("sup", py, px, n).filter(m => m.aplica); } catch (e) { toast(e.message); return; } let nota = ""; if (n > 8000) { const antes = cand.length; cand = cand.filter(m => m.recursos.tiempo !== "alto"); if (cand.length < antes) nota = "Con más de 8 000 observaciones se omitieron los métodos más lentos."; }
    if (!cand.length) { toast("No hay métodos aplicables a estas variables."); return; }
    const o = popup("Eligiendo el mejor modelo", `${cand.length} modelos · ${gN(n, 0)} observaciones${nota ? " · " + nota : ""}`), est = {}, todos = {}, tabla = []; S._stop = false; const t00 = Date.now();
    const pinta = det => { o.querySelector(".mo-pet").innerHTML = cand.map(m => { const e = est[m.id] || {}; return `<li class="${e.ok ? "ok" : e.act ? "on" : ""}"><i>${e.ok ? "✓" : e.act ? '<b class="mo-spin"></b>' : e.err ? "!" : "·"}</i><span>${esc(m.nombre)}</span><em>${e.ok ? esc(e.txt) : e.err ? "no se pudo" : e.act ? esc((e.sub || "entrenando…").slice(0, 46)) : ""}</em></li>`; }).join(""); o.querySelector(".mo-pd").textContent = det || ""; };
    o.querySelector("footer").innerHTML = '<button type="button" class="mo-ghost" data-fl="detener">Detener y quedarme con lo ya calculado</button>'; pinta("");
    for (let i = 0; i < cand.length; i++) {
      if (S._stop && tabla.some(t => !t.error)) break; const m = cand[i]; est[m.id] = { act: true }; popupAv(o, i / cand.length); pinta(`Modelo ${i + 1} de ${cand.length}`); await tick(); const t0 = Date.now();
      try { const res = await window.MLMotor.correr({ enfoque: "sup", y: M.y, x: M.x, metodo: m.id, prep: M.prep, semilla: 42, prop_prueba: .2, folds: 5, k: null }, S.ds, ev => { if (ev) { est[m.id].sub = (ev.nombre || ev.id) + (ev.detalle ? " · " + ev.detalle : ""); const em = o.querySelector(".mo-pet li.on em"); if (em) em.textContent = est[m.id].sub.slice(0, 46); popupAv(o, (i + (ev.progreso || 0) * .9) / cand.length); } });
        const sc = puntajeModelo(res); todos[m.id] = res; est[m.id] = { ok: true, txt: sc.txt || "listo" }; tabla.push({ id: m.id, nombre: res.nombreMetodo || m.nombre, familia: m.familia, score: sc.score, prueba: sc.test, cv: sc.cv, gap: sc.gap, clave: sc.clave, ms: Date.now() - t0 }); }
      catch (e) { est[m.id] = { err: true }; tabla.push({ id: m.id, nombre: m.nombre, familia: m.familia, error: String(e.message || e), ms: Date.now() - t0 }); }
      pinta(""); await tick();
    }
    const ok = tabla.filter(t => !t.error && nz(t.score)).sort((a, b) => b.score - a.score);
    if (!ok.length) { o.querySelector(".mo-pbar").classList.add("err"); o.querySelector(".mo-pd").innerHTML = '<b class="mo-mal">Ningún modelo se pudo ajustar con estas variables.</b>'; o.querySelector("footer").innerHTML = '<button type="button" class="mo-ghost" data-fl="cierrapop">Cerrar</button>'; return; }
    const mejor = ok[0], fallidos = tabla.filter(t => t.error || !nz(t.score)), res0 = todos[mejor.id], reg = res0.tipoY === "regresion";
    const cmp = { elegido: mejor.id, tipoY: res0.tipoY, clave: mejor.clave, criterio: reg ? "R² (promedio entre datos de prueba y validación cruzada)" : "exactitud balanceada (promedio entre datos de prueba y validación cruzada)", modelos: ok.concat(fallidos), seg: Math.round((Date.now() - t00) / 1000), parcial: cand.length > tabla.length, nota };
    popupAv(o, 1); S.res = Object.assign({}, res0, { _cmp: cmp, _todos: todos }); S.resVer = null; S.M.prepOk = true; S.M.hecho = new Date().toISOString(); await guardaModelo();
    try { const kp = (res0.kpis || []).slice(0, 2).map(k => ({ etiqueta: k.etiqueta, valor: fmtK(k) })); await api(R + "/proyectos/" + S.id + "/resultados", { res: S.res, kpi: kp }); const p = S.proy.find(x => x.id === S.id); if (p) { p.estado = "modelado"; p.kpi = kp; p.metodo = "Mejor: " + (res0.nombreMetodo || mejor.nombre); } } catch (e) { toast("El resultado se calculó pero no se pudo guardar: " + e.message, 6000); }
    popupCierra(o); nav(); ir("resultados");
  }
  /* banner con el modelo elegido y la comparación de todos los modelos */
  function bannerModelo() {
    const r0 = S.res, c = r0 && r0._cmp; if (!c) return ""; const ver = S.resVer && r0._todos[S.resVer] ? S.resVer : c.elegido, ganador = c.modelos.find(m => m.id === c.elegido), vista = c.modelos.find(m => m.id === ver), seg2 = c.modelos.filter(m => !m.error && nz(m.score))[1], rn = c.tipoY === "regresion" ? "R²" : "exactitud balanceada";
    const fila = (m, i) => m.error || !nz(m.score) ? `<tr class="mo-trx"><td>—</td><td><b>${esc(m.nombre)}</b><small>${esc(m.familia || "")}</small></td><td colspan="5" class="mo-q2">No se pudo ajustar: ${esc(m.error || "sin métricas")}</td></tr>` : `<tr class="mo-trv${m.id === ver ? " foco" : ""}" data-vermod="${m.id}" title="Ver los resultados de este modelo"><td>${i + 1}</td><td><b>${esc(m.nombre)}</b><small>${esc(m.familia || "")}</small></td><td>${gauge("r2", Math.max(0, m.score))}</td><td>${gN(m.prueba, 3)}</td><td>${m.cv != null ? gN(m.cv, 3) : "—"}</td><td class="${m.gap > .2 ? "mo-mal" : ""}">${m.gap == null ? "—" : (m.gap > .2 ? "alto · " : m.gap > .1 ? "algo · " : "bajo · ") + gN(m.gap, 2)}</td><td>${gN(m.ms / 1000, 1)} s</td><td>${m.id === c.elegido ? '<span class="mo-rec">Elegido</span>' : m.id === ver ? '<span class="mo-or archivo">Viendo</span>' : ""}</td></tr>`;
    return `<section class="mo-sx2 mo-bnr"><small class="mo-bk">Selección automática del mejor modelo</small><h3>Modelo elegido: <b>${esc(ganador ? ganador.nombre : "")}</b></h3>
      <p>Se entrenaron y compararon <b>${c.modelos.length} modelos</b> con los mismos datos de entrenamiento y de prueba. Ganó <b>${esc(ganador ? ganador.nombre : "")}</b> con un puntaje de ${gN(ganador ? ganador.score : null, 3)}${seg2 ? `, ${gN(ganador.score - seg2.score, 3)} por encima del segundo (${esc(seg2.nombre)})` : ""}. El puntaje combina ${rn} en datos de prueba y en validación cruzada, y resta una penalización cuando el modelo se ajusta mucho mejor al entrenamiento que a datos nuevos (sobreajuste). ${c.parcial ? "<b>Se detuvo antes de probar todos.</b> " : ""}${esc(c.nota || "")}</p>
      ${ver !== c.elegido ? `<div class="mo-av"><div>Estás viendo los resultados de <b>${esc(vista ? vista.nombre : ver)}</b>, que <b>no fue el elegido</b>. <button type="button" class="mo-ghost" data-vermod="${c.elegido}">Volver al modelo elegido</button></div></div>` : ""}
      <details class="mo-sx" open><summary><b>Comparación de los modelos</b><span>clic en un renglón para ver los resultados de ese modelo</span></summary><div class="mo-tw"><table class="mo-t mo-cmpt"><thead><tr><th>#</th><th>Modelo</th><th>Puntaje</th><th>${rn} prueba</th><th>${rn} validación cruzada</th><th>Sobreajuste</th><th>Tiempo</th><th></th></tr></thead><tbody>${c.modelos.map(fila).join("")}</tbody></table></div></details></section>`;
  }
  /* franja que dice con claridad qué se predice (variable de respuesta) y con qué */
  function bannerRespuesta(r) {
    if (r.enfoque !== "sup") return (r.metodo === "pca" && !r.clusters ? `<div class="mo-av"><div>Este resultado se calculó antes de la agrupación automática del PCA. Entra a «Modelar» y vuelve a ejecutar el análisis para que cada caso aparezca asignado a un grupo de casos similares.</div> <button type="button" class="mo-new" data-sec="modelar">Ir a Modelar</button></div>` : "") + `<div class="mo-yb"><span class="mo-ybt">Variable de respuesta</span><b>No hay</b><span class="mo-ybd">Este análisis es no supervisado: busca patrones y grupos sin predecir una variable en particular.</span></div>`;
    const y = r.y || (S.M && S.M.y), c = S.ds ? S.ds.cols.find(k => k.name === y) : null, xs = ((S.M && S.M.x) || []).filter(n => n !== y), u = c && c.unit ? " (" + esc(c.unit) + ")" : "", cat = r.tipoY !== "regresion";
    return `<div class="mo-yb"><span class="mo-ybt">Variable de respuesta · lo que el modelo predice</span><b>${esc(nomVar(y))}${u}</b><span class="mo-ybd">${cat ? "El modelo clasifica cada punto en una categoría" + (r.clases && r.clases.length ? ": " + esc(r.clases.slice(0, 6).join(", ")) + (r.clases.length > 6 ? "…" : "") : "") : "El modelo estima un valor numérico para cada punto"}, usando <b>${xs.length}</b> variable${xs.length === 1 ? "" : "s"} predictora${xs.length === 1 ? "" : "s"}${xs.length ? ": " + esc(xs.slice(0, 5).map(nomVar).join(", ")) + (xs.length > 5 ? " y " + (xs.length - 5) + " más" : "") : ""}.</span></div>`;
  }
  function secResultados() {
    S.sec = "resultados"; nav(); const r = resActual();
    if (!r) { wrap(`${cab("6", "Resultados y calidad del modelo", "")}<div class="mo-empty"><b>Aún no hay resultados.</b><p>Configura el análisis y pulsa «${S.M && S.M.enfoque === "nosup" ? "Analizar" : "Modelar"}».</p><button type="button" class="mo-new" data-sec="${S.M && S.M.metodo ? "modelar" : "variables"}">Ir al flujo</button></div>`); return; }
    if (r.enfoque === "val") { if (!window.MLVal) { wrap('<div class="mo-empty"><b>No se cargó el módulo de valuación.</b></div>'); return; } wrap(`${cab("6", "Resultados de la valuación", esc(r.nombreMetodo || r.metodo))}<div class="mo-rpb"><button type="button" class="mo-ghost" data-sec="modelar">← Cambiar la configuración</button></div>${window.MLVal.htmlResultados(r, HV)}`); window.MLVal.post(r, HV); return; }
    const sup = r.enfoque === "sup", tipo = sup ? (r.tipoY === "regresion" ? "regresión" : r.tipoY === "binaria" ? "clasificación binaria" : "clasificación multiclase") : "análisis no supervisado";
    wrap(`${cab("6", "Resultados y calidad del modelo", `${esc(r.nombreMetodo || r.metodo)} · ${tipo}`)}${bannerModelo()}<div class="mo-rpb"><button type="button" class="mo-new" data-fl="reporte">⬇ Descargar reporte (PDF)</button></div><nav class="mo-jmp"><button type="button" data-jmp="res">Resultados del modelado</button><button type="button" data-jmp="cal">Confiabilidad del modelo</button></nav>
      <section id="moSecRes" class="mo-cal on"><button type="button" class="mo-calh" data-calt="1" aria-expanded="true"><span class="mo-calb">A</span><span class="mo-calt"><b>Resultados del modelado</b><small>Qué encontró el modelo: indicadores clave, hallazgos y gráficas</small></span><span class="mo-calc">▾</span></button><div class="mo-calx">${bannerRespuesta(r)}${resultadosHtml(r)}</div></section><section id="moSecCal" class="mo-cal"><button type="button" class="mo-calh" data-calt="1" aria-expanded="false"><span class="mo-calb">B</span><span class="mo-calt"><b>Confiabilidad del modelo</b><small>Estadísticos y diagnósticos para saber cuánto confiar en los resultados</small></span><span class="mo-calc">▾</span></button><div class="mo-calx" hidden>${calidadHtml(r)}</div></section>${pie("modelar", null, "", false)}`);
    postResultados(r);
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
    if ((b = t.closest("[data-aab]"))) { e.preventDefault(); abreAn(b.dataset.aab); return; }
    if ((b = t.closest("[data-aren]"))) { renombraAn(b.dataset.aren); return; } if ((b = t.closest("[data-adel]"))) { eliminaAn(b.dataset.adel); return; }
    if (t.closest("#moNuevoA")) { S.naForm = true; vistaAnalisis(); return; } if (t.closest("[data-nacan]")) { S.naForm = false; vistaAnalisis(); return; } if (t.closest("[data-naok]")) { creaAn(); return; }
    if ((b = t.closest("[data-af]"))) { S.af = b.dataset.af; vistaAnalisis(); return; } if ((b = t.closest("[data-av]"))) { S.av = b.dataset.av; vistaAnalisis(); return; }
    if ((b = t.closest("[data-ed]"))) { editar(b.dataset.ed); return; }
    if ((b = t.closest("[data-del]"))) { elimina(b.dataset.del); return; }
    if (typeof flujoClic === "function" && flujoClic(e)) return;
  }
  function entrada(e) {
    const t = e.target; if (S.wiz && S.sec === "nuevo" && wizEntrada(e)) return;
    if (t.id === "moPQ") { S.pq = t.value; S._foco = true; vistaProyectos(); return; }
    if (t.id === "mgQ" && S._mg) { clearTimeout(S._mgT); const v = t.value; S._mgT = setTimeout(() => S._mg.setQ(v), 180); return; }
    if (t.id === "moAQ") { S.aq = t.value; S._afoco = true; vistaAnalisis(); return; }
    if (t.id === "moNaN" && e.isTrusted === false) return;
    if (typeof flujoEntrada === "function") flujoEntrada(e);
  }
  function cambia(e) { if (e.target.id === "moPSel") { const v = e.target.value; if (v) abre(v); else { S.id = S.P = S.res = null; S.M = null; S.aid = S.A = null; S.AN = []; ir("proyectos"); } return; } if (S.wiz && S.sec === "nuevo" && wizCambia(e)) return; if (typeof flujoCambia === "function") flujoCambia(e); }
  /* arranque */
  /* ═══════════════ 7 · Simulación: valores nuevos y puntos nuevos evaluados con una versión del modelo ═══════════════ */
  /* error esperado del modelo (para el intervalo): validación cruzada si existe; si no, datos de prueba */
  function simErr(r) {
    const m = (r && r.metricas) || {}, cv = m.cv && m.cv.media, te = m.prueba || {}, rm = cv && nz(cv.rmse) ? cv.rmse : te.rmse, ma = cv && nz(cv.mae) ? cv.mae : te.mae;
    return nz(rm) ? { rmse: rm, mae: nz(ma) ? ma : null, z: 1.96, fuente: cv && nz(cv.rmse) ? "validación cruzada" : "datos de prueba", acc: te.exactitud } : null;
  }
  const simS = () => S.sim || (S.sim = { tab: "puntos", ab: {}, conjuntos: [], escenarios: [], cargado: false, ver: null, vals: null, last: null, view: null, estado: null });
  /* «versiones» del modelo = cada método que se comparó al modelar (y el elegido); si solo se corrió uno, hay una sola */
  function versiones() {
    const r = S.res; if (!r || r.enfoque !== "sup") return []; const c = r._cmp;
    if (c && r._todos) return c.modelos.filter(m => !m.error && r._todos[m.id]).map(m => ({ id: m.id, nombre: m.nombre, familia: m.familia || "", score: m.score, elegido: m.id === c.elegido, res: r._todos[m.id] }));
    return [{ id: r.metodo, nombre: r.nombreMetodo || r.metodo, familia: "", elegido: true, res: r }];
  }
  const verDe = id => { const L = versiones(); return L.find(v => v.id === id) || L.find(v => v.elegido) || L[0] || null; };
  const verOpts = sel => versiones().map(v => `<option value="${esc(v.id)}"${v.id === sel ? " selected" : ""}>${esc(v.nombre)}${v.elegido ? " · elegido" : ""}${nz(v.score) ? " · puntaje " + gN(v.score, 2) : ""}</option>`).join("");
  const colDs = n => S.ds ? S.ds.cols.find(c => c.name === n) : null;
  const unidadY = () => { const c = colDs(S.M.y); return c && c.unit ? " " + c.unit : ""; };
  const hoyTxt = s => { try { return new Date(s).toLocaleString("es-MX", { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" }); } catch (_) { return ""; } };
  function simDefaults() {
    const o = {}; (S.M.x || []).forEach(n => { const p = perfilDe(n); if (!p) return; const cat = (p.tipo === "bin" || p.tipo === "cat") && (p.categorias || []).length;
      o[n] = cat ? String(p.categorias[0].v) : (p.mediana != null && isFinite(p.mediana) ? String(Math.round(p.mediana * 1000) / 1000) : ""); }); return o;
  }
  /* filas alineadas con las columnas de la tabla del proyecto para una lista de puntos: la biblioteca se consulta por coordenada; las columnas propias salen de q.x */
  async function filasDe(pts, av) {
    const cols = S.ds.cols, lib = cols.filter(c => esLib(c.name)); let lr = pts.map(() => []), sin = 0;
    if (lib.length) { const specs = lib.map(c => BIBK()[c.name].s); for (let i = 0; i < pts.length; i += 3000) { const trozo = pts.slice(i, i + 3000), j = await api(R + "/tabla", { puntos: trozo.map(q => [q.lat, q.lng]), specs }); sin += j.sinAgeb || 0; j.rows.forEach((r, k) => { lr[i + k] = r; }); if (av) av((i + trozo.length) / pts.length); await tick(); } }
    return { filas: pts.map((q, i) => { let k = 0; return cols.map(c => esLib(c.name) ? (lr[i][k++] == null ? null : lr[i][k - 1]) : (q.x && q.x[c.name] != null ? q.x[c.name] : null)); }), sinAgeb: sin };
  }
  async function simEvalua(verId, filas, titulo) {
    const v = verDe(verId); if (!v) throw new Error("Primero modela el proyecto."); if (!(await aseguraDs())) throw new Error("Faltan los datos del proyecto; vuelve a construir la tabla.");
    const r = v.res, ov = popup(titulo || "Evaluando con el modelo", v.nombre);
    try { return await window.MLMotor.simular({ metodo: r.metodo, y: r.y || S.M.y, x: S.M.x, prep: S.M.prep }, S.ds, filas, { params: r.hiperparametros && r.hiperparametros.elegidos, clases: r.clases, progreso: (f, d) => { popupAv(ov, f); const p = ov.querySelector(".mo-pps"); if (p) p.textContent = d; } }); }
    finally { popupCierra(ov); }
  }
  async function secSimulacion() {
    S.sec = "simulacion"; nav(); const s = simS();
    if (!S.res) { wrap(`${cab("7", "Simulación", "Evalúa valores nuevos o puntos nuevos con el modelo ya construido.")}<div class="mo-empty"><b>Primero modela el proyecto.</b><p>La simulación usa el modelo que resulta del paso «Modelar».</p><button type="button" class="mo-new" data-sec="modelar">Ir a Modelar</button></div>`); return; }
    if (S.res.enfoque !== "sup") { wrap(`${cab("7", "Simulación", "Evalúa valores nuevos o puntos nuevos con el modelo ya construido.")}<div class="mo-empty"><b>La simulación necesita un modelo supervisado.</b><p>Este proyecto es no supervisado (no hay una variable de respuesta que predecir). Cambia el enfoque y vuelve a modelar para usar la simulación.</p></div>`); return; }
    wrap('<div class="mo-empty"><b>Cargando…</b></div>'); if (!(await aseguraDs())) { wrap(`${cab("7", "Simulación", "")}<div class="mo-empty"><b>Faltan los datos del proyecto.</b><button type="button" class="mo-new" data-sec="variables">Ir a Variables</button></div>`); return; }
    if (!s.cargado) { try { const j = await api(R + "/proyectos/" + S.id + "/sim"); s.conjuntos = j.conjuntos || []; s.escenarios = j.escenarios || []; } catch (e) { toast(e.message); } s.cargado = true; nav(); }
    if (!verDe(s.ver)) s.ver = (versiones().find(v => v.elegido) || versiones()[0] || {}).id; if (!s.vals) s.vals = simDefaults(); simPinta();
  }
  function simPinta() {
    const s = simS(); if (s.view) return simVista(); const v = verDe(s.ver), sup = `${cab("7", "Simulación", "Evalúa valores nuevos o puntos nuevos con el modelo ya construido. Elige con cuál versión del modelo quieres evaluar.")}`;
    const barra = `<section class="mo-sm-b"><label class="mo-fl">Versión del modelo<select id="smVer" class="mo-in">${verOpts(s.ver)}</select></label><p class="mo-mut">${v ? `Predice <b>${esc(nomVar(S.M.y))}</b>${unidadY()} con ${esc(v.nombre)}${v.elegido ? " (el modelo elegido al modelar)" : ""}. Cada evaluación vuelve a ajustar el modelo con todos los datos del proyecto, con el mismo preprocesamiento y los mismos hiperparámetros.` : ""}</p></section>`;
    const tabs = `<div class="mo-tabs"><button type="button" class="${s.tab === "puntos" ? "on" : ""}" data-smtab="puntos">Puntos nuevos <span class="mo-bd2">${s.conjuntos.length}</span></button><button type="button" class="${s.tab === "valores" ? "on" : ""}" data-smtab="valores">Valores manuales</button></div>`;
    wrap(`${sup}${barra}${tabs}${s.tab === "puntos" ? simPuntosHtml() : simValoresHtml()}`);
  }
  /* ── valores manuales ── */
  function simValoresHtml() {
    const s = simS(), xs = (S.M.x || []).filter(n => colDs(n)), campo = n => { const p = perfilDe(n) || {}, c = colDs(n) || {}, cat = (p.tipo === "bin" || p.tipo === "cat") && (p.categorias || []).length, val = s.vals[n] == null ? "" : s.vals[n];
      const pista = cat ? "" : (p.min != null && isFinite(p.min) ? `rango observado ${gN(p.min)} a ${gN(p.max)}` : "");
      return `<label class="mo-sv"><span><b>${esc(nomVar(n))}</b><i>${esc(c.unit || "")}${c.unit && pista ? " · " : ""}${pista}</i></span>${cat ? `<select class="mo-in" data-sv="${esc(n)}">${p.categorias.map(k => `<option value="${esc(k.v)}"${String(k.v) === String(val) ? " selected" : ""}>${esc(k.v)}</option>`).join("")}</select>` : `<input class="mo-in" type="number" step="any" data-sv="${esc(n)}" value="${esc(val)}" placeholder="vacío">`}</span></label>`; };
    const ptos = S.puntos.slice(0, 500);
    return `<section class="mo-sx2"><h3>Valores de las variables <small>${xs.length}</small></h3><p class="mo-mut">Escribe los valores que quieres evaluar. Empiezan en la mediana de tu proyecto; cámbialos y pulsa «Evaluar». Un campo vacío se rellena como indica el preprocesamiento.</p>
      <div class="mo-sm-lo"><label class="mo-fl">Tomar los valores de un punto del proyecto<select id="smPto" class="mo-in"><option value="">— elegir punto —</option>${ptos.map((q, i) => `<option value="${i}">${esc(q.id || "P" + (i + 1))}</option>`).join("")}</select></label>
      <label class="mo-fl">…o de unas coordenadas (latitud, longitud)<span class="mo-sm-cc"><input id="smLat" class="mo-in" placeholder="Latitud"><input id="smLng" class="mo-in" placeholder="Longitud"><button type="button" class="mo-ghost" data-sm="cargacoord">Leer indicadores</button></span></label></div>
      ${(() => { const G = {}; xs.forEach(n => { const o2 = origen(n); (G[o2.g] = G[o2.g] || { o: o2.o, l: [] }).l.push(n); }); const E = Object.entries(G).sort((a, b) => a[1].o - b[1].o || a[0].localeCompare(b[0]));
        return `<div class="mo-chs"><button type="button" class="mo-ghost" data-sm="expande">Expandir todo</button><button type="button" class="mo-ghost" data-sm="contrae">Contraer todo</button></div><div class="mo-sgs">${E.map(([g, v], i) => { const t = g.replace(/^Biblioteca de indicadores · /, "").replace(/^Tu proyecto · /, "Datos de tu proyecto · "), ab = s.ab[g] != null ? s.ab[g] : i === 0, parts = t.split(" › ");
          return `<details class="mo-sg" data-g="${esc(g)}"${ab ? " open" : ""}><summary><span class="mo-sgt"><b>${esc(parts[parts.length - 1])}</b>${parts.length > 1 ? `<small>${esc(parts.slice(0, -1).join(" › "))}</small>` : ""}</span><em>${v.l.length} variable${v.l.length === 1 ? "" : "s"}</em></summary><div class="mo-svg">${v.l.map(campo).join("")}</div></details>`; }).join("")}</div>`; })()}
      <div class="mo-chs"><button type="button" class="mo-new" data-sm="eval">Evaluar con el modelo</button><button type="button" class="mo-ghost" data-sm="reset">Volver a las medianas</button></div></section>
      <div id="smRes">${s.last ? simResHtml(s.last) : ""}</div>${simEscHtml()}`;
  }
  function simResHtml(L) {
    const o = L.out, r = L.ver.res, y = S.M.y; let h = "";
    if (o.pred[0] == null) return `<section class="mo-sx2"><h3>Resultado</h3><div class="mo-av alto"><div class="c">${esc(o.motivo[0] || "No se pudo predecir con estos valores.")}</div></div></section>`;
    if (o.tipoY === "regresion") {
      const ER = simErr(r), rm = ER && ER.rmse, p = o.pred[0], j = S.ds.cols.findIndex(c => c.name === y), vs = j >= 0 ? S.ds.rows.map(f => f[j]).filter(v => typeof v === "number" && isFinite(v)).sort((a, b) => a - b) : [];
      const pct = vs.length ? Math.round(100 * vs.filter(v => v <= p).length / vs.length) : null, mn = vs.length ? vs[0] : null, mx = vs.length ? vs[vs.length - 1] : null, pos = mn != null && mx > mn ? Math.max(0, Math.min(100, 100 * (p - mn) / (mx - mn))) : 50;
      h = `<div class="mo-sm-big"><small>${esc(nomVar(y))} predicho</small><b>${gN(p)}${unidadY()}</b>${nz(rm) ? `<span>Intervalo de confianza 95 %: <b>${gN(p - 1.96 * rm)} a ${gN(p + 1.96 * rm)}</b>${unidadY()}</span><span class="mo-sm-er">Error típico del modelo (RMSE): ± ${gN(rm)}${unidadY()}${nz(ER.mae) ? " · error absoluto medio (MAE): " + gN(ER.mae) + unidadY() : ""} <i>(medido en ${ER.fuente}; el intervalo supone un error parecido en el punto nuevo)</i></span>` : ""}</div>
        ${mn != null ? `<div class="mo-sm-esc"><i style="left:${pos}%"></i><span>${gN(mn)}</span><span>${gN(mx)}</span></div><p class="mo-mut">Es mayor que el <b>${pct} %</b> de los valores observados en tu proyecto (de ${gN(mn)} a ${gN(mx)}${unidadY()}).</p>` : ""}`;
    } else {
      const P = o.prob[0] || [], L2 = o.clases.map((c, i) => [c, P[i] || 0]).sort((a, b) => b[1] - a[1]);
      const ER2 = simErr(r); h = `<div class="mo-sm-big"><small>${esc(nomVar(y))} más probable</small><b>${esc(o.pred[0])}</b><span>probabilidad ${gN(100 * L2[0][1], 1)} %</span>${ER2 && nz(ER2.acc) ? `<span class="mo-sm-er">Exactitud del modelo en datos de prueba: ${gN(100 * ER2.acc, 0)} % <i>(de cada 100 casos nuevos, se esperan ~${gN(100 * ER2.acc, 0)} bien clasificados)</i></span>` : ""}</div><div class="mo-sm-pr">${L2.map(([c, p]) => `<div><span>${esc(c)}</span><i><u style="width:${Math.round(p * 100)}%"></u></i><b>${gN(100 * p, 1)} %</b></div>`).join("")}</div>`;
    }
    return `<section class="mo-sx2"><h3>Resultado <small>${esc(L.ver.nombre)}</small></h3>${h}<div class="mo-sm-gu"><input id="smEscNom" class="mo-in" maxlength="80" placeholder="Nombre del escenario (ej. Sucursal en zona nueva)" value="${esc(L.nom || "")}"><button type="button" class="mo-ghost" data-sm="guardaesc">Guardar escenario</button></div></section>`;
  }
  function simEscHtml() {
    const E = simS().escenarios; if (!E.length) return "";
    return `<section class="mo-sx2"><h3>Escenarios guardados <small>${E.length}</small></h3><div class="mo-tw"><table class="mo-t"><thead><tr><th>Escenario</th><th>Resultado</th><th>Versión</th><th>Fecha</th><th></th></tr></thead><tbody>${E.map(e => `<tr class="mo-r"><td><b>${esc(e.nombre)}</b></td><td>${esc((e.resultado && e.resultado.txt) || "—")}</td><td>${esc(e.verNombre || "")}</td><td>${hoyTxt(e.creado)}</td><td class="mo-ac"><button type="button" class="mo-ghost" data-smesccarga="${esc(e.id)}">Cargar valores</button><button type="button" class="mo-ghost mo-pel" data-smescdel="${esc(e.id)}">Eliminar</button></td></tr>`).join("")}</tbody></table></div></section>`;
  }
  const simTxt = (o, i) => o.pred[i] == null ? "—" : o.tipoY === "regresion" ? gN(o.pred[i]) + unidadY() : String(o.pred[i]) + (o.prob && o.prob[i] ? " (" + gN(100 * Math.max.apply(null, o.prob[i]), 0) + " %)" : "");
  async function simEvalManual() {
    const s = simS(), v = verDe(s.ver); if (!v) return; const fila = S.ds.cols.map(c => { if (!(S.M.x || []).includes(c.name)) return null; const t = s.vals[c.name], p = perfilDe(c.name) || {}; if (t === "" || t == null) return null; return (p.tipo === "bin" || p.tipo === "cat") && !(typeof (p.categorias[0] || {}).v === "number") ? String(t) : (isFinite(+t) ? +t : String(t)); });
    try { const out = await simEvalua(v.id, [fila], "Evaluando los valores"); s.last = { out, ver: v, vals: Object.assign({}, s.vals), nom: "" }; simPinta(); const e = $("#smRes"); if (e) e.scrollIntoView({ behavior: "smooth", block: "nearest" }); } catch (e) { toast(e.message, 6000); }
  }
  async function simCargaIndic(lat, lng) {
    const lib = (S.M.x || []).filter(esLib); if (!lib.length) { toast("Este modelo no usa indicadores de la biblioteca."); return; } try { const j = await api(R + "/tabla", { puntos: [[lat, lng]], specs: lib.map(k => BIBK()[k].s) }); if (j.sinAgeb) toast("Esa coordenada no cae en una AGEB urbana: los indicadores quedan vacíos.", 5000); lib.forEach((k, i) => { const x = j.rows[0][i]; simS().vals[k] = x == null ? "" : String(Math.round(x * 1000) / 1000); }); simS().last = null; simPinta(); toast("Indicadores leídos de la coordenada."); } catch (e) { toast(e.message, 5000); }
  }
  /* ── puntos nuevos: gestión de conjuntos ── */
  const EST_A = { hecho: ["#059669", "✓"], proc: ["#d97706", "…"], pau: ["#64748b", "⏸"], reg: ["#4f46e5", "◷"], no: ["#94a3b8", "—"] };
  function simAnalisis(W, nVars, verNombre) {
    const o = W.opt, od = W.odOn ? (W.odModo === "google" ? "Origen / Destino vehicular con Google Routes (de paga)" : "Origen / Destino vehicular con OpenStreetMap (gratis)") : "";
    return [{ k: "ind", t: "Indicadores de la biblioteca nacional", d: nVars + " variables por coordenada (AGEB)", e: "hecho", g: 1 }, { k: "mod", t: "Evaluación con el modelo", d: verNombre, e: "hecho", g: 1 },
      { k: "amb", t: "Datos ambientales de cada punto", d: "aire, polen, clima y energía solar (parte gratuita)", e: "proc", g: 1 }, { k: "esc", t: "Escena, red vial y población alcanzada", d: "calles, edificios y servicios de OpenStreetMap; fricción vial; población en 15 min", e: "proc", g: 1 },
      { k: "od", t: "Origen / Destino vehicular", d: od || "no se calcula", e: W.odOn ? "proc" : "no", g: W.odModo !== "google" },
      { k: "mq", t: "Maqueta con fotos de calle", d: o.fotos ? `registrada: radio ${W.radio} m, ${W.fotos} fotos por punto (se lanza desde Geo Lab)` : "no se pidió", e: o.fotos ? "reg" : "no", g: 0 },
      { k: "vi", t: "Vialidades", d: o.vial ? `registradas: ${W.vias.length} tipos de vía, ${W.pre} por punto` : "no se pidieron", e: o.vial ? "reg" : "no", g: 1 },
      { k: "tr", t: "Tráfico por día y hora", d: o.trafico ? `registrado: ${W.dias.length} días × ${W.horas.length} horas` : "no se pidió", e: o.trafico ? "reg" : "no", g: 0 }];
  }
  function simPuntosHtml() {
    const s = simS(), L = s.conjuntos;
    const tarj = c => { const m = c.sim || {}, A = (m.analisis || []).filter(a => a.e !== "no"); return `<article class="mo-pc" style="--c:#4f46e5"><div class="hd"><span class="mo-b" style="background:#4f46e51a;color:#4f46e5">${m.evaluado ? "Evaluado" : "Sin evaluar"}</span><i>${fecha(c.creado)}</i></div><h3><a href="#" data-smabre="${esc(c.id)}">${esc(c.nombre)}</a></h3><p class="ob">${esc(m.verNombre ? "Modelo: " + m.verNombre : "")}${m.evaluado ? " · evaluado el " + hoyTxt(m.evaluado) : ""}</p>
      <div class="mx"><div><b>${gN(c.nPuntos, 0)}</b><span>puntos</span></div><div><b>${m.resumen && m.resumen.txt ? esc(m.resumen.txt) : "—"}</b><span>${esc((m.resumen && m.resumen.et) || "resultado")}</span></div></div>
      <div class="mo-sm-an">${A.map(a => `<span class="mo-b" style="background:${EST_A[a.e][0]}1a;color:${EST_A[a.e][0]}" title="${esc(a.d)}">${EST_A[a.e][1]} ${esc(a.t)}</span>`).join("")}</div>
      <div class="ac"><button type="button" class="mo-new" data-smabre="${esc(c.id)}">Abrir</button><button type="button" class="mo-ghost mo-pel" data-smdel="${esc(c.id)}">Eliminar</button></div></article>`; };
    return `<section class="mo-sx2"><div class="mo-sm-hd"><div><h3>Conjuntos de puntos nuevos <small>${L.length}</small></h3><p class="mo-mut">Define puntos nuevos en el mapa, con coordenadas, con un archivo o desde el DENUE. Se analizan con los mismos pasos que al crear el proyecto y se evalúan con la versión del modelo que elijas. Cada conjunto guarda su fecha, su versión del modelo y los análisis realizados.</p></div><button type="button" class="mo-new" data-sm="nuevo">${mi("plus")} Nuevo conjunto de puntos</button></div>
      ${L.length ? `<div class="mo-pg">${L.map(tarj).join("")}</div>` : '<div class="mo-empty"><b>Aún no hay conjuntos.</b><p>Crea el primero para estimar el resultado del modelo en lugares que no están en tu proyecto.</p></div>'}</section>`;
  }
  function simNuevo() {
    const s = simS(), W = wizVacio(); Object.assign(W, { sim: true, paso: 2, nombre: "Conjunto " + (s.conjuntos.length + 1), simVer: s.ver, objetivo: "" }); S.wiz = W; wizard();
  }
  async function simElimina(sid) { const c = simS().conjuntos.find(x => x.id === sid); if (!c || !confirm("¿Eliminar el conjunto «" + c.nombre + "»? No se puede deshacer.")) return; try { await api(R + "/proyectos/" + S.id + "/sim/" + sid, null, "DELETE"); const s = simS(); s.conjuntos = s.conjuntos.filter(x => x.id !== sid); if (s.view === sid) s.view = null; simPinta(); nav(); toast("Conjunto eliminado."); } catch (e) { toast(e.message); } }
  /* guardar y evaluar un conjunto nuevo (último botón del asistente) */
  async function wzSimular(W) {
    if (!W.nombre.trim()) { W.paso = 2; W.msg = "Escribe el nombre del conjunto."; return wizard(); } if (!W.puntos.length) { W.paso = 2; W.msg = "Elige al menos un punto."; return wizard(); }
    const v = verDe(W.simVer); if (!v) { W.msg = "Elige la versión del modelo."; return wizard(); } const s = simS(); let ov = null;
    try { S.busy = true; ov = popup("Analizando los puntos nuevos", "Guardando el conjunto…"); const nVars = (S.M.x || []).length;
      const c = costo(W), meta = { ver: v.id, verNombre: v.nombre, metodo: v.res.metodo, y: S.M.y, yEtiqueta: nomVar(S.M.y), tipoY: v.res.tipoY, analisis: simAnalisis(W, nVars, v.nombre) };
      const j = await api(R + "/proyectos/" + S.id + "/sim", { nombre: W.nombre.trim(), fuente: W.fuente, puntos: W.puntos, columnas: W.columnas, cfg: { odOn: W.odOn, odModo: W.odModo, radio: W.radio, fotos: W.fotos, vias: W.vias, pre: W.pre, dias: W.dias, horas: W.horas, opt: W.opt, costoEstimadoUSD: Math.round(c.total * 100) / 100 }, sim: meta }), sid = j.conjunto.id;
      popupAv(ov, .1); const pps = ov.querySelector(".mo-pps"); if (pps) pps.textContent = "Leyendo los indicadores de cada punto…"; if (!(await aseguraDs())) throw new Error("Faltan los datos del proyecto.");
      const F = await filasDe(W.puntos, f => popupAv(ov, .1 + .3 * f)); popupCierra(ov); ov = null;
      const out = await simEvalua(v.id, F.filas, "Evaluando " + gN(W.puntos.length, 0) + " puntos");
      await simGuardaRes(sid, W.puntos, v, out, F, meta);
      api("/geometrika/modelab-precalcular", { id: sid, od: { on: W.odOn, modo: W.odModo } }).catch(() => { });
      S.wiz = null; S.busy = false; s.cargado = false; s.tab = "puntos"; s.view = sid; await secSimulacion(); toast("Conjunto analizado. Los análisis por punto siguen corriendo en segundo plano.", 5000); }
    catch (e) { S.busy = false; if (ov) popupCierra(ov); W.msg = e.message; wizard(); toast(e.message, 6000); }
  }
  function simResumenTxt(out) {
    const ok = out.pred.filter(x => x != null); if (!ok.length) return { txt: "—", et: "sin predicciones" };
    if (out.tipoY === "regresion") { const m = ok.reduce((a, b) => a + b, 0) / ok.length; return { txt: gN(m) + unidadY(), et: "promedio predicho" }; }
    const k = {}; ok.forEach(c => { k[c] = (k[c] || 0) + 1; }); const t = Object.entries(k).sort((a, b) => b[1] - a[1])[0]; return { txt: String(t[0]), et: "clase más frecuente (" + gN(100 * t[1] / ok.length, 0) + " %)" };
  }
  async function simGuardaRes(sid, puntos, v, out, F, meta) {
    const ahora = new Date().toISOString(), res = { ver: v.id, verNombre: v.nombre, evaluado: ahora, tipoY: out.tipoY, clases: out.clases, cols: S.ds.cols.map(c => c.name), filas: F.filas, pred: out.pred, prob: out.prob ? out.prob.map(p => p && p.map(x => Math.round(x * 1e4) / 1e4)) : null, motivo: out.motivo, sinAgeb: F.sinAgeb };
    const sim = Object.assign({}, meta, { evaluado: ahora, ver: v.id, verNombre: v.nombre, resumen: simResumenTxt(out), nPred: out.pred.filter(x => x != null).length });
    await api(R + "/proyectos/" + S.id + "/sim/" + sid + "/resultados", { res, sim }); return { res, sim };
  }
  /* ── vista de un conjunto ── */
  async function simVista() {
    const s = simS(), sid = s.view; wrap('<div class="mo-empty"><b>Abriendo el conjunto…</b></div>'); let j;
    try { j = await api(R + "/proyectos/" + S.id + "/sim/" + sid); } catch (e) { s.view = null; toast(e.message); return simPinta(); }
    s.cur = j; try { await aseguraDs(); } catch (_) { } try { const q = await api("/geometrika/modelab-precalcular/estado?id=" + sid); s.estado = q.estado || null; } catch (_) { s.estado = null; } simVistaPinta();
    clearTimeout(s._tm); s._ek = JSON.stringify(s.estado && [s.estado.fase, s.estado.hechos, s.estado.od, s.estado.running, s.estado.error]); if (s.estado && s.estado.running && !/pausa/i.test(s.estado.fase || "")) s._tm = setTimeout(simPoll, 20000);
  }
  function simEstadoA(a, E) {
    if (!E) return a; const k = a.k, liston = E.fase === "Listo", error = E.error; const b = Object.assign({}, a);
    if (k === "amb") { b.e = liston ? "hecho" : error ? "no" : "proc"; if (!liston && !error) b.d += " · " + (E.fase === "Datos ambientales" ? "en curso" : "listo"); if (E.fase !== "Datos ambientales") b.e = "hecho"; }
    if (k === "esc") { b.e = liston || /Origen/.test(E.fase || "") ? "hecho" : error ? "no" : (E.fase === "Datos ambientales" ? "proc" : "proc"); if (!liston && /Escena/.test(E.fase || "")) b.d += ` · ${gN(E.hechos || 0, 0)} de ${gN(E.total || 0, 0)} puntos`; }
    if (k === "od" && a.e !== "no") { b.e = liston ? "hecho" : "proc"; if (/pausa/i.test(E.fase || "")) { b.d += " · en pausa"; b.e = "pau"; } else if (/Origen/.test(E.fase || "")) b.d += ` · ${gN(E.od || 0, 0)} de ${gN(E.total || 0, 0)} puntos`; }
    return b;
  }
  const PAL_R = ["#5b7fa6", "#3f8f8b", "#8fb36b", "#c9a24a", "#c2703d", "#8a4f7d"], PAL_C = ["#5b7fa6", "#c9a24a", "#3f8f8b", "#8a4f7d", "#c2703d", "#7a8b99", "#9d6b53", "#6f8f55"];
  function simAnHtml(A, E) {
    return `<section class="mo-sx2" id="simAn"><h3>Análisis del conjunto</h3><div class="mo-tw"><table class="mo-t"><thead><tr><th>Análisis</th><th>Estado</th><th>Detalle</th><th>Costo</th></tr></thead><tbody>${A.map(a => { const e = EST_A[a.e] || EST_A.no, tx = { hecho: "Hecho", proc: "En proceso", pau: "En pausa", reg: "Registrado", no: a.k === "od" || a.k === "mq" || a.k === "vi" || a.k === "tr" ? "No solicitado" : "—" }[a.e]; return `<tr><td><b>${esc(a.t)}</b></td><td><span class="mo-b" style="background:${e[0]}1a;color:${e[0]}">${e[1]} ${tx}</span></td><td class="mo-mut">${esc(a.d)}</td><td>${a.g ? '<span class="mo-gratis">Gratis</span>' : '<span class="mo-paga">De paga</span>'}</td></tr>`; }).join("")}</tbody></table></div>
        <p class="mo-mut">${E && E.running ? `Análisis por punto en curso: ${esc(E.fase || "")}. La tabla se actualiza sola.` : E && E.fase === "Listo" ? "Todos los análisis por punto terminaron." : "Los análisis «Registrado» quedan guardados con el conjunto; se lanzan desde Geo Lab porque incurren en costos."} <button type="button" class="mo-ghost" data-sm="refresca">Actualizar</button></p></section>`;
  }
  /* actualiza solo la tabla de análisis (sin recargar la página ni el mapa) */
  async function simPoll() {
    const s = simS(), sid = s.view; if (!sid || S.sec !== "simulacion" || !s.cur) return; try { const q = await api("/geometrika/modelab-precalcular/estado?id=" + sid); if (s.view !== sid) return; const E = q.estado || null, k = JSON.stringify(E && [E.fase, E.hechos, E.od, E.running, E.error]); if (k !== s._ek) { s._ek = k; s.estado = E; const el = $("#simAn"); if (el) el.outerHTML = simAnHtml(((s.cur.conjunto.sim || {}).analisis || []).map(a => simEstadoA(a, E)), E); } } catch (_) { }
    clearTimeout(s._tm); if (S.sec === "simulacion" && s.view === sid && s.estado && s.estado.running && !/pausa/i.test(s.estado.fase || "")) s._tm = setTimeout(simPoll, 20000);
  }
  function simVistaPinta() {
    const s = simS(), j = s.cur; if (!j) return; const c = j.conjunto, m = c.sim || {}, res = j.res, pts = j.puntos || [], E = s.estado, A = (m.analisis || []).map(a => simEstadoA(a, E));
    const reg = res && res.tipoY === "regresion", vp = res ? res.pred : [], rows = pts.slice(0, 500);
    const kp = [["Puntos", gN(c.nPuntos, 0), "#4f46e5"], ["Versión del modelo", esc(m.verNombre || "—"), "#0f766e"], ["Creado", hoyTxt(c.creado), "#475569"], ["Evaluado", m.evaluado ? hoyTxt(m.evaluado) : "—", "#059669"]];
    s.rng = null; const P = res ? simPack(res, reg) : null; s.P = P, s.fl = s.fl || "t"; const nM = P ? P.M.length : 0;
    const tabla = res ? `<div class="mo-sr-f"><div class="mo-seg2">${[["t", "Todos", P.N.length + nM], ["n", "Nuevos", P.N.length], ["m", "Modelado", nM]].map(([k, t, n]) => `<button type="button" class="${s.fl === k ? "on" : ""}" data-smf="${k}">${t} <em>${gN(n, 0)}</em></button>`).join("")}</div><input id="smQ" class="mo-in" placeholder="Buscar punto…" value="${esc(s.q || "")}"></div><div class="mo-sr-l" id="simCards"></div><div class="mo-sr-st" id="simCardsSt"></div>` : '<div class="mo-empty"><b>Este conjunto aún no se evalúa.</b></div>';
    wrap(`<div class="mo-h"><div><h1>${esc(c.nombre)}</h1><p>Conjunto de puntos nuevos · modelo ${esc(m.verNombre || "—")}</p></div><button type="button" class="mo-ghost" data-sm="volver">${mi("back")} Todos los conjuntos</button></div>
      <div class="mo-kp2 mo-kpis">${kp.map(([t, v, col]) => `<div class="mo-kpi" style="--c:${col}"><b style="font-size:${String(v).length > 16 ? 14 : 20}px">${v}</b><span>${t}</span></div>`).join("")}</div>
      ${simAnHtml(A, E)}
      <section class="mo-sx2"><h3>Resultados del modelo</h3>${res ? `<div class="mo-sm-lo"><label class="mo-fl">Evaluar de nuevo con otra versión<select id="smVer2" class="mo-in">${verOpts(m.ver)}</select></label><div class="mo-chs"><button type="button" class="mo-new" data-sm="reeval">Evaluar de nuevo</button><button type="button" class="mo-ghost" data-sm="csv">Descargar CSV</button></div></div>
        ${res.sinAgeb ? `<div class="mo-av"><div>${gN(res.sinAgeb, 0)} de ${gN(pts.length, 0)} puntos no caen en una AGEB urbana: sus indicadores de la biblioteca están vacíos y el modelo los rellena.</div></div>` : ""}<div class="mo-sr-grid"><div class="mo-sr-left"><div class="mo-mapw"><div id="simMap" class="mo-map"></div></div>${P && P.reg ? simBoxPanel(P) : ""}</div>` : ""}${res ? `<div class="mo-sr-colw"><div class="mo-sr-col">${tabla}</div></div></div>` : tabla}</section>`);
    if (res) { simCardsInit(P); simMapa(P); }
  }
  /* escala continua de color y rango observado de la variable de respuesta en el proyecto */
  const RAMPA = ["#d64541", "#ec8b3c", "#e9c53f", "#8dc152", "#2e9e5b"];   /* semáforo: el valor más bajo en rojo, el más alto en verde */
  function simRango() {
    const j = S.ds ? S.ds.cols.findIndex(c => c.name === S.M.y) : -1, vs = j >= 0 ? S.ds.rows.map(f => f[j]).filter(v => typeof v === "number" && isFinite(v)).sort((a, b) => a - b) : [];
    return { vs, lo: vs.length ? vs[0] : 0, hi: vs.length ? vs[vs.length - 1] : 1 };
  }
  function rampa(v, R) { const t = R.hi > R.lo ? Math.max(0, Math.min(1, (v - R.lo) / (R.hi - R.lo))) : .5, x = t * (RAMPA.length - 1), i = Math.min(RAMPA.length - 2, Math.floor(x)), f = x - i, h = c => [1, 3, 5].map(k => parseInt(c.slice(k, k + 2), 16)), a = h(RAMPA[i]), b = h(RAMPA[i + 1]); return "#" + a.map((u, k) => Math.round(u + (b[k] - u) * f).toString(16).padStart(2, "0")).join(""); }
  const cuant = (a, q) => { if (!a.length) return 0; const p = (a.length - 1) * q, i = Math.floor(p); return a[i] + (a[Math.min(a.length - 1, i + 1)] - a[i]) * (p - i); };
  /* ── puntos del modelado y puntos nuevos en una sola estructura (claves «n0, n1…» nuevos · «m0, m1…» modelado) ── */
  function simPack(res, reg) {
    const ds = S.ds, pts = (simS().cur && simS().cur.puntos) || [], R = simRango(), cl = res.clases || [], jy = ds.cols.findIndex(c => c.name === S.M.y);
    const col = v => v == null ? "#94a3b8" : reg ? rampa(v, R) : PAL_C[Math.max(0, cl.indexOf(v)) % PAL_C.length];
    const N = pts.map((q, i) => ({ k: "n" + i, i, n: 1, id: (!q.id || /^P\d+$/.test(String(q.id))) ? "Nuevo " + (i + 1) : q.id, lat: q.lat, lng: q.lng, v: res.pred[i], pr: res.prob && res.prob[i] ? Math.max.apply(null, res.prob[i]) : null, mot: (res.motivo && res.motivo[i]) || "", fila: (res.filas || [])[i], cols: res.cols })), M = [];
    if (jy >= 0) ds.rows.forEach((f, j) => { const v = f[jy]; if (v == null || v === "" || (reg && !isFinite(+v))) return; const q = S.puntos[j] || {}; M.push({ k: "m" + j, j, n: 0, id: ds.ids && ds.ids[j] != null ? ds.ids[j] : "P" + (j + 1), lat: ds.lat ? ds.lat[j] : q.lat, lng: ds.lng ? ds.lng[j] : q.lng, v: reg ? +v : String(v), fila: f, cols: ds.cols.map(c => c.name) }); });
    const vr = verDe(res.ver), E = reg ? simErr(vr ? vr.res : S.res) : null; if (E) N.forEach(p => { if (p.v != null) { p.lo = p.v - E.z * E.rmse; p.hi = p.v + E.z * E.rmse; } });
    const all = {}; N.concat(M).forEach(p => { all[p.k] = p; });
    return { reg, R, cl, col, N, M, all, E, sid: simS().cur && simS().cur.conjunto.id, pid: S.id };
  }
  /* indicadores con los que se evaluó (nuevos) o se modeló (modelado) un punto: solo las variables predictoras */
  function simDet(p) {
    const f = p.fila || [], xs = new Set(S.M.x || []), L = [];
    (p.cols || []).forEach((n, j) => { if (!xs.has(n)) return; const c = colDs(n) || {}, v = f[j]; L.push(`<div><span>${esc(nomVar(n))}</span><b>${v == null || v === "" ? "—" : typeof v === "number" ? gN(v) : esc(v)}${v != null && v !== "" && c.unit ? ` <i>${esc(c.unit)}</i>` : ""}</b></div>`); });
    return `<div class="mo-sr-dh">${p.n ? "Indicadores usados para evaluar este punto" : "Indicadores con los que se modeló este punto"} <em>${L.length}</em></div><div class="mo-sr-dl">${L.join("")}</div>${p.n ? '<p class="mo-mut mo-sr-dn">«—» = sin dato; el modelo lo rellenó según el preprocesamiento.</p>' : ""}`;
  }
  /* enlace a la maqueta de Geo Lab con ese punto ya seleccionado (nuevo: su conjunto; modelado: el proyecto, con el conjunto para poder volver) */
  function simMaqUrl(P, p) {
    const base = window.GM_MAQ || "/admin/maqueta/entorno.html", v = typeof gmVer_ === "function" ? gmVer_().slice(1) : "", q = new URLSearchParams(); if (v) q.set("v", v); q.set("id", "000000");
    if (p.n) { q.set("ml", P.sid); q.set("p", String(p.i)); } else { q.set("ml", P.pid); q.set("p", String(p.j)); if (P.sid) q.set("sim", P.sid); }
    return base + "?" + q.toString();
  }
  function simCardHtml(P, p) {
    const col = P.col(p.v), val = p.v == null ? "—" : P.reg ? gN(p.v) : esc(p.v); let pcTxt = "", icTxt = ""; if (p.n && P.E && p.lo != null) icTxt = `<small class="mo-sr-ci" title="Intervalo de confianza del 95 %: predicción ± 1.96 × error típico (RMSE ${gN(P.E.rmse)})">IC 95 %: ${gN(p.lo)} – ${gN(p.hi)}</small>`;
    if (P.reg && p.v != null && P.R.vs.length) { const vs = P.R.vs; let a = 0, b = vs.length; while (a < b) { const mid = (a + b) >> 1; if (vs[mid] <= p.v) a = mid + 1; else b = mid; } pcTxt = `<small class="mo-sr-pc" title="Porcentaje de los puntos del modelado con un valor menor o igual">percentil ${Math.round(100 * a / vs.length)}</small>`; }
    return `<div class="mo-sr ${p.n ? "nuevo" : "modelado"}" data-smcard="${p.k}" style="--c:${p.n ? col : "#64748b"}"><span class="mo-sr-n">${p.n ? p.i + 1 : ""}</span><div class="mo-sr-p"><b>${esc(p.id)}</b><small>${p.lat != null ? p.lat + ", " + p.lng : ""}</small><em class="mo-sr-t">${p.n ? "Punto nuevo · predicho" : "Modelado · observado"}</em></div><div class="mo-sr-v"><b>${val}</b><small>${P.reg ? esc(unidadY().trim()) : p.pr != null ? gN(100 * p.pr, 0) + " % prob." : ""}</small>${icTxt}${pcTxt}</div><span class="mo-mut mo-sr-x">${esc(p.mot || "")}</span><div class="mo-sr-d"></div><div class="mo-sr-ab"><a class="mo-ghost" href="${esc(simMaqUrl(P, p))}" target="_blank" rel="noopener" title="Abre este punto en Geo Lab">Abrir maqueta →</a></div></div>`;
  }
  /* lista con carga progresiva: se agregan 120 tarjetas más cada vez que se llega al final */
  const SIM_CH = 120;
  function simCardsMas(P, reinicia) {
    const s = simS(), cs = $("#simCards"); if (!cs) return; if (reinicia) { cs.innerHTML = ""; s._nr = 0; } const L = s._L || [], a = s._nr || 0, b = Math.min(L.length, a + SIM_CH);
    if (b > a) cs.insertAdjacentHTML("beforeend", L.slice(a, b).map(p => simCardHtml(P, p)).join("")); s._nr = b; if (!L.length) cs.innerHTML = '<div class="mo-empty"><b>Sin resultados.</b></div>';
    const st = $("#simCardsSt"); if (st) st.innerHTML = `Mostrando <b>${gN(b, 0)}</b> de <b>${gN(L.length, 0)}</b>${b < L.length ? ` · faltan <b>${gN(L.length - b, 0)}</b> por cargar (se cargan solos al llegar al final)` : " · todos cargados"}`;
  }
  function simCardsLista(P) { const s = simS(), f = s.fl || "t", q = (s.q || "").trim().toLowerCase(); s._L = (f === "n" ? P.N : f === "m" ? P.M : P.N.concat(P.M)).filter(p => (!s.rng || (p.v != null && p.v >= s.rng[0] && p.v <= s.rng[1])) && (!q || (String(p.id) + " " + p.lat + " " + p.lng).toLowerCase().includes(q))); simCardsMas(P, true); }
  function simCardsInit(P) { const cs = $("#simCards"); if (!cs) return; simCardsLista(P); cs.addEventListener("scroll", () => { const s = simS(); if (cs.scrollTop + cs.clientHeight >= cs.scrollHeight - 240 && (s._nr || 0) < (s._L || []).length) simCardsMas(P, false); }); }
  const estrella = (cx, cy, R) => Array.from({ length: 10 }, (_, i) => { const a = -Math.PI / 2 + i * Math.PI / 5, r = i % 2 ? R * .45 : R; return (cx + r * Math.cos(a)).toFixed(1) + "," + (cy + r * Math.sin(a)).toFixed(1); }).join(" ");
  /* boxplot (mismo lenguaje visual que los de Geo Lab: bigotes grises, caja suave, mediana oscura, punto con aro de color) con los puntos del modelado en tira y los nuevos encima */
  function simBoxCtx(P) {
    const R = P.R, rg = simS().rng, ok = p => p.v != null && (!rg || (p.v >= rg[0] && p.v <= rg[1])), Ms = P.M.filter(ok), vs = Ms.map(p => p.v).sort((a, b) => a - b), np = P.N.filter(ok), caja = vs.length >= 4;
    const q1 = cuant(vs, .25), md = cuant(vs, .5), q3 = cuant(vs, .75), iq = q3 - q1, wl = caja ? vs.find(v => v >= q1 - 1.5 * iq) : 0, wh = caja ? [...vs].reverse().find(v => v <= q3 + 1.5 * iq) : 0;
    const lo = rg ? rg[0] : Math.min(R.lo, ...np.map(p => p.v)), hi = rg ? rg[1] : Math.max(R.hi, ...np.map(p => p.v)), pad = (hi - lo) * .03 || 1, x0 = lo - pad, x1 = hi + pad, W = 640, ml = 0, mr = 0;
    return { R, ok, Ms, vs, np, caja, q1, md, q3, wl, wh, lo, hi, x0, x1, W, X: v => ml + (W - ml - mr) * (v - x0) / (x1 - x0) };
  }
  /* parte superior: caja, bigotes, tira de puntos del modelado y estrellas de los nuevos */
  function simBoxSvg(P, selK) {
    const { R, ok, Ms, np, caja, q1, md, q3, wl, wh, W, X } = simBoxCtx(P), yc = 78;
    if (R.vs.length < 4) return '<p class="mo-mut">Faltan datos para dibujar la distribución.</p>';
    const hz = j => ((Math.imul(j + 1, 2654435761) >>> 0) % 1000) / 1000, stride = Math.max(1, Math.ceil(Ms.length / 4000)), ms = Ms.filter((p, i) => p.k !== selK && i % stride === 0);
    const ord = np.slice().sort((a, b) => a.v - b.v), ult = [-1e9, -1e9, -1e9, -1e9];
    const pines = ord.map((p, k) => { const c = P.col(p.v), x = X(p.v), s = p.k === selK; let nv = ult.findIndex(u => Math.abs(x - u) > 104); if (nv < 0) nv = k % 4; ult[nv] = x; const y0 = 14 + nv * 10;
      const eb = P.E && p.lo != null && (np.length <= 14 || s) ? (() => { const a = Math.max(0, X(p.lo)), b2 = Math.min(W, X(p.hi)); return `<line x1="${a}" x2="${b2}" y1="${yc}" y2="${yc}" stroke="${c}" stroke-width="${s ? 4 : 3}" stroke-linecap="round" opacity=".5"/><line x1="${a}" x2="${a}" y1="${yc - 7}" y2="${yc + 7}" stroke="${c}" stroke-width="2" opacity=".8"/><line x1="${b2}" x2="${b2}" y1="${yc - 7}" y2="${yc + 7}" stroke="${c}" stroke-width="2" opacity=".8"/>`; })() : "";
      return `<g data-bx="${p.k}" style="cursor:pointer">${eb}<line x1="${x}" x2="${x}" y1="${y0 + 3}" y2="${yc - 6}" stroke="${c}" stroke-width="${s ? 2 : 1.2}" stroke-dasharray="${s ? "" : "2 2"}"/><polygon points="${estrella(x, yc, s ? 19 : 15)}" fill="${c}" fill-opacity=".22"/><polygon points="${estrella(x, yc, s ? 12.5 : 9.5)}" fill="${c}" stroke="${s ? "#0f172a" : "#fff"}" stroke-width="${s ? 2.2 : 1.6}" stroke-linejoin="round"/><circle cx="${x}" cy="${yc}" r="13" fill="transparent"/>${np.length <= 14 || s ? `<text x="${Math.max(48, Math.min(W - 48, x))}" y="${y0}" text-anchor="middle" font-size="${s ? 12 : 10.5}" font-weight="800" fill="${s ? "#0f172a" : c}" paint-order="stroke" stroke="#fff" stroke-width="3">${esc(String(p.id).slice(0, 18))} · ${gN(p.v)}</text>` : ""}<title>${esc(p.id)}: ${gN(p.v)} (nuevo)${p.lo != null ? " · IC 95 %: " + gN(p.lo) + " – " + gN(p.hi) : ""}</title></g>`; }).join("");
    const mods = ms.map(p => { const x = X(p.v), y = yc + (hz(p.j) - .5) * 20; return `<circle data-bx="${p.k}" style="cursor:pointer" cx="${x}" cy="${y}" r="3" fill="#475569" fill-opacity=".5"><title>${esc(p.id)}: ${gN(p.v)} (modelado)</title></circle>`; }).join("");
    const sp = P.all[selK], selM = sp && !sp.n && ok(sp) ? (() => { const x = X(sp.v), y = yc + (hz(sp.j) - .5) * 20, tx = Math.max(60, Math.min(W - 60, x)); return `<g data-bx="${sp.k}" style="cursor:pointer"><line x1="${x}" x2="${x}" y1="16" y2="${y - 10}" stroke="#2563eb" stroke-width="1.8" stroke-dasharray="3 3"/><circle cx="${x}" cy="${y}" r="14" fill="#2563eb" fill-opacity=".22"/><circle cx="${x}" cy="${y}" r="8.5" fill="#2563eb" stroke="#fff" stroke-width="2.2"/><text x="${tx}" y="11" text-anchor="middle" font-size="12" font-weight="800" fill="#1d4ed8" paint-order="stroke" stroke="#fff" stroke-width="3.5">${esc(String(sp.id).slice(0, 22))} · ${gN(sp.v)}</text></g>`; })() : "";
    const cj = caja ? `<line x1="${X(wl)}" x2="${X(q1)}" y1="${yc}" y2="${yc}" stroke="#94a3b8" stroke-width="2"/><line x1="${X(q3)}" x2="${X(wh)}" y1="${yc}" y2="${yc}" stroke="#94a3b8" stroke-width="2"/><line x1="${X(wl)}" x2="${X(wl)}" y1="${yc - 8}" y2="${yc + 8}" stroke="#94a3b8" stroke-width="2.2"/><line x1="${X(wh)}" x2="${X(wh)}" y1="${yc - 8}" y2="${yc + 8}" stroke="#94a3b8" stroke-width="2.2"/><rect x="${X(q1)}" y="${yc - 16}" width="${Math.max(3, X(q3) - X(q1))}" height="32" rx="5" fill="#cbd5e1" fill-opacity=".55" stroke="#64748b" stroke-width="1.5"><title>Q1 ${gN(q1)} · Q3 ${gN(q3)}</title></rect><line x1="${X(md)}" x2="${X(md)}" y1="${yc - 16}" y2="${yc + 16}" stroke="#0f172a" stroke-width="2.6"><title>Mediana ${gN(md)}</title></line>` : "";
    return `<svg viewBox="0 0 ${W} 100" width="100%" role="img" aria-label="Distribución de la variable de respuesta" style="display:block">${cj}${mods}${pines}${selM}</svg>`;
  }
  /* parte inferior: barra de colores y eje del mismo dominio */
  function simAxisSvg(P) {
    const c = simBoxCtx(P), { R, lo, hi, md, W, X } = c, gid = "bg" + Date.now() % 1e6, ticks = [0, .25, .5, .75, 1].map(t => lo + (hi - lo) * t);
    return `<svg viewBox="0 0 ${W} 50" width="100%" style="display:block"><defs><linearGradient id="${gid}" x1="0" x2="1">${Array.from({ length: 9 }, (_, i) => `<stop offset="${i / 8}" stop-color="${rampa(lo + (hi - lo) * i / 8, R)}"/>`).join("")}</linearGradient></defs>
      <rect x="${X(lo)}" y="3" width="${Math.max(2, X(hi) - X(lo))}" height="6" rx="3" fill="url(#${gid})"/>${ticks.map((v, k) => `<line x1="${X(v)}" x2="${X(v)}" y1="9" y2="14" stroke="#94a3b8"/><text x="${X(v)}" y="27" text-anchor="${k === 0 ? "start" : k === 4 ? "end" : "middle"}" font-size="10" fill="#64748b">${gN(v)}</text>`).join("")}${c.caja ? `<text x="${X(md)}" y="43" text-anchor="middle" font-size="9.5" font-weight="700" fill="#0f172a">mediana ${gN(md)}</text>` : ""}</svg>`;
  }
  const simBoxPanel = P => `<div class="mo-boxp"><div class="mo-boxh2"><span><b>${esc(nomVar(S.M.y))}</b><small>Distribución de los puntos del modelado y dónde caen los puntos nuevos</small></span><div class="mo-boxl"><span><i class="mo-dm"></i> Modelado (${gN(P.M.length, 0)})</span><span><svg viewBox="0 0 30 30" width="15" height="15"><polygon points="${estrella(15, 15.5, 13.5)}" fill="#c9a24a" stroke="#fff" stroke-width="2"/></svg> Nuevos (${P.N.length})</span></div></div><div id="simBoxW">${simBoxSvg(P, null)}</div><div class="mo-rng" id="simRng"><div class="mo-rng-t" id="simRngTr"><svg id="simRngS" viewBox="0 0 100 32" preserveAspectRatio="none"></svg><i class="mo-rng-d" id="simRngD0"></i><i class="mo-rng-d" id="simRngD1"></i><div class="mo-rng-w" id="simRngW"><b class="mo-rng-g" id="simRngH0" tabindex="0" role="slider" aria-label="Valor mínimo" data-h="0"></b><b class="mo-rng-g" id="simRngH1" tabindex="0" role="slider" aria-label="Valor máximo" data-h="1"></b></div></div><div class="mo-rng-k" id="simRngK"></div></div><div class="mo-rng-l"><span id="simRngT">todos los puntos</span><button type="button" id="simRngR" class="mo-rng-r" hidden>Restablecer</button></div><div class="mo-boxs"><span id="simBoxS">${simBoxStats(P)}</span><em>Haz clic en un punto para resaltarlo en el mapa y en la lista.${P.E ? ` Las barras de color de cada estrella son su intervalo de confianza del 95 % (predicción ± 1.96 × error típico ${gN(P.E.rmse)}, medido en ${P.E.fuente}).` : ""}</em></div></div>`;
  const simBoxStats = P => { const rg = simS().rng, Ms = P.M.filter(p => p.v != null && (!rg || (p.v >= rg[0] && p.v <= rg[1]))), vs = Ms.map(p => p.v).sort((a, b) => a - b), f = gN; if (vs.length < 4) return `<b>Universo: ${gN(vs.length, 0)} de ${gN(P.M.length, 0)} puntos del modelado en el rango</b><br>Muy pocos puntos para calcular la caja; amplía el rango.`; const q1 = cuant(vs, .25), md = cuant(vs, .5), q3 = cuant(vs, .75);
    return `<b>Universo: ${gN(vs.length, 0)}${rg ? " de " + gN(P.M.length, 0) : ""} puntos del modelado${rg ? " (dentro del rango elegido)" : ""}</b><br>Mín ${f(vs[0])} · P10 ${f(cuant(vs, .1))} · <b>Q1 ${f(q1)}</b> · <b>Mediana ${f(md)}</b> · <b>Q3 ${f(q3)}</b> · P90 ${f(cuant(vs, .9))} · Máx ${f(vs[vs.length - 1])} · Rango intercuartil ${f(q3 - q1)}`; };
  function simMapa(P) {
    leaflet(() => { const el = $("#simMap"); if (!el || !window.L) return; const s = simS(), m = L.map(el); L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 18, maxNativeZoom: 16, attribution: "Esri" }).addTo(m);
      const MK = {}, gNu = L.featureGroup().addTo(m), gMo = L.featureGroup().addTo(m), perm = P.N.length <= 150; let SEL = null, LBL = null;
      const pin = (c, sel) => `<div class="mo-pw${sel ? " sel" : ""}"><svg viewBox="0 0 30 30" width="30" height="30"><polygon points="${estrella(15, 15.5, 13.5)}" fill="${c}" stroke="#fff" stroke-width="2" stroke-linejoin="round"/></svg></div>`;
      const icN = (p, sel) => L.divIcon({ className: "mo-pinN", html: pin(P.col(p.v), sel), iconSize: [30, 30], iconAnchor: [15, 15] }), icM = (p, sel) => L.divIcon({ className: "mo-pinM", html: `<i class="mo-dm${sel ? " sel" : ""}"></i>`, iconSize: [18, 18], iconAnchor: [9, 9] });
      P.M.slice(0, 6000).forEach(p => { if (p.lat == null) return; const mk = L.marker([p.lat, p.lng], { icon: icM(p), zIndexOffset: 0 }).bindTooltip(esc(p.id) + " · observado " + (P.reg ? gN(p.v) : esc(p.v)), { direction: "top", offset: [0, -8], className: "mo-etq" }).addTo(gMo); mk.on("click", () => sel(p.k, false)); MK[p.k] = mk; });
      P.N.forEach(p => { const mk = L.marker([p.lat, p.lng], { icon: icN(p), zIndexOffset: 1000 }).addTo(gNu); if (perm) mk.bindTooltip("<b>" + esc(String(p.id).slice(0, 22)) + "</b> · " + (p.v == null ? "—" : P.reg ? gN(p.v) : esc(p.v)), { permanent: true, direction: "top", offset: [0, -16], className: "mo-etq mo-etv" }); else mk.bindTooltip(esc(p.id) + " · " + (p.v == null ? "sin predicción" : P.reg ? gN(p.v) : esc(p.v)), { direction: "top", offset: [0, -16], className: "mo-etq" }); mk.on("click", () => sel(p.k, false)); MK[p.k] = mk; });
      /* selección enlazada: tarjeta ⇄ punto del mapa ⇄ boxplot */
      const cards = () => $("#simCards"), pintaBox = () => { const bw = $("#simBoxW"); if (bw && P.reg) bw.innerHTML = simBoxSvg(P, SEL); const st = $("#simBoxS"); if (st && P.reg) st.innerHTML = simBoxStats(P); };
      const marca = (k, vuela, desdeTarjeta) => { const p = P.all[k]; if (!p) return; if (SEL && MK[SEL]) { const q = P.all[SEL]; MK[SEL].setIcon(q.n ? icN(q) : icM(q)); MK[SEL].setZIndexOffset(q.n ? 1000 : 0); } SEL = k; if (LBL) { m.removeLayer(LBL); LBL = null; } if (!p.n && p.lat != null) LBL = L.marker([p.lat, p.lng], { interactive: false, keyboard: false, zIndexOffset: 9000, icon: L.divIcon({ className: "mo-sl-w", html: `<div class="mo-sl"><b>${esc(String(p.id).slice(0, 22))} · ${P.reg ? gN(p.v) : esc(p.v)}</b><i></i></div>`, iconSize: [0, 0], iconAnchor: [0, 0] }) }).addTo(m);
        if (MK[k]) { MK[k].setIcon(p.n ? icN(p, true) : icM(p, true)); MK[k].setZIndexOffset(5000); if (!p.n && !m.hasLayer(gMo)) { gMo.addTo(m); const b = $('[data-ly="m"]'); if (b) b.classList.add("on"); } }
        $$(".mo-sr.on").forEach(c => { c.classList.remove("on"); const d0 = c.querySelector(".mo-sr-d"); if (d0) d0.innerHTML = ""; }); let card = $(`.mo-sr[data-smcard="${k}"]`), cs = cards();
        if (!card && cs) { cs.insertAdjacentHTML("afterbegin", simCardHtml(P, p)); card = cs.firstElementChild; }
        if (card) { card.classList.add("on"); const dd = card.querySelector(".mo-sr-d"); if (dd) dd.innerHTML = simDet(p); if (cs && !desdeTarjeta) cs.scrollTo({ top: Math.max(0, card.offsetTop - cs.offsetTop - 8), behavior: "smooth" }); }
        pintaBox(); if (vuela && p.lat != null) m.flyTo([p.lat, p.lng], Math.max(m.getZoom(), 17), { duration: .6 }); if (MK[k] && MK[k].openTooltip) MK[k].openTooltip(); };
      const sel = (k, desdeTarjeta) => marca(k, true, desdeTarjeta); s.sel = sel;
      s.repintaCards = () => { const cs = cards(); if (!cs) return; simCardsLista(P); if (SEL) { const c = $(`.mo-sr[data-smcard="${SEL}"]`); if (c) { c.classList.add("on"); const dd = c.querySelector(".mo-sr-d"); if (dd) dd.innerHTML = simDet(P.all[SEL]); } } };
      const ptsN = P.N.map(p => [p.lat, p.lng]), todos = ptsN.concat(P.M.filter(p => p.lat != null).map(p => [p.lat, p.lng])), fitN = () => ptsN.length ? m.fitBounds(L.latLngBounds(ptsN).pad(.3), { maxZoom: 16 }) : null, fitT = () => todos.length ? m.fitBounds(L.latLngBounds(todos).pad(.1), { maxZoom: 16 }) : null; fitN();
      /* panel de capas (arriba a la derecha) */
      const LY = L.control({ position: "topright" }); LY.onAdd = () => { const d = L.DomUtil.create("div", "mo-lyr"); d.innerHTML = `<button type="button" class="on" data-ly="n"><svg viewBox="0 0 30 30" width="15" height="15"><polygon points="${estrella(15, 15.5, 13.5)}" fill="#c9a24a" stroke="#fff" stroke-width="2"/></svg> Puntos nuevos <em>${P.N.length}</em></button><button type="button" class="on" data-ly="m"><i class="mo-dm"></i> Puntos del modelado <em>${P.M.length}</em></button><div class="mo-lyr-f"><button type="button" data-ly="fn">Ver nuevos</button><button type="button" data-ly="ft">Ver todos</button></div>`;
        L.DomEvent.disableClickPropagation(d); L.DomEvent.on(d, "click", ev => { const b = ev.target.closest && ev.target.closest("[data-ly]"); if (!b) return; const k = b.dataset.ly; if (k === "fn") fitN(); else if (k === "ft") fitT(); else { const g = k === "n" ? gNu : gMo; if (m.hasLayer(g)) { m.removeLayer(g); b.classList.remove("on"); } else { g.addTo(m); b.classList.add("on"); } } }); return d; }; LY.addTo(m);
      /* boxplot dentro del mapa, abajo */
      /* controlador de rango (como el de EconoLab): tira con la forma de los datos, ventana con dos agarraderas; acota el boxplot y filtra mapa y tarjetas */
      if (P.reg && $("#simRng")) { const todos2 = P.N.concat(P.M); let dmin = Infinity, dmax = -Infinity; todos2.forEach(p => { if (p.v != null && isFinite(p.v)) { if (p.v < dmin) dmin = p.v; if (p.v > dmax) dmax = p.v; } });
        if (isFinite(dmin) && dmax > dmin) { const vis = {}, tr = $("#simRngTr"), h = [$("#simRngH0"), $("#simRngH1")], dd = [$("#simRngD0"), $("#simRngD1")], W = $("#simRngW"), T = $("#simRngT"), RR = $("#simRngR"); let cur = [dmin, dmax], raf = 0;
          const NB = 48, bins = new Array(NB).fill(0); todos2.forEach(p => { if (p.v != null && isFinite(p.v)) bins[Math.min(NB - 1, Math.floor((p.v - dmin) / (dmax - dmin) * NB))]++; }); const mx = Math.max(...bins), gid = "rg" + Date.now() % 1e6;
          $("#simRngS").innerHTML = `<defs><linearGradient id="${gid}" x1="0" x2="1">${Array.from({ length: 9 }, (_, i) => `<stop offset="${i / 8}" stop-color="${rampa(dmin + (dmax - dmin) * i / 8, P.R)}"/>`).join("")}</linearGradient></defs><path d="M0 32 ${bins.map((c, i) => `L${(i + .5) * 100 / NB} ${32 - 28 * c / mx}`).join(" ")} L100 32 Z" fill="url(#${gid})" fill-opacity=".75" stroke="url(#${gid})" stroke-width=".6" vector-effect="non-scaling-stroke"/>`;
          $("#simRngK").innerHTML = [0, .25, .5, .75, 1].map((t, k) => `<span style="left:${t * 100}%" class="${k === 0 ? "i" : k === 4 ? "f" : ""}"><i></i>${gN(dmin + (dmax - dmin) * t)}</span>`).join("");
          const pos = v => 100 * (v - dmin) / (dmax - dmin), dentro = p => p.v != null && (!s.rng || (p.v >= s.rng[0] && p.v <= s.rng[1]));
          const pinta = () => { W.style.left = pos(cur[0]) + "%"; W.style.width = (pos(cur[1]) - pos(cur[0])) + "%"; dd[0].style.width = pos(cur[0]) + "%"; dd[1].style.left = pos(cur[1]) + "%"; h[0].title = gN(cur[0]); h[1].title = gN(cur[1]); };
          const aplica = fit => { const full = cur[0] <= dmin && cur[1] >= dmax; s.rng = full ? null : [cur[0], cur[1]]; pinta(); RR.hidden = full;
            todos2.forEach(p => { const mk = MK[p.k]; if (!mk) return; const v = full || dentro(p), g = p.n ? gNu : gMo; if (vis[p.k] === v || (vis[p.k] === undefined && v)) { vis[p.k] = v; return; } vis[p.k] = v; if (v) g.addLayer(mk); else g.removeLayer(mk); });
            const nN = P.N.filter(dentro).length, nM = P.M.filter(dentro).length; T.innerHTML = full ? `<b>${gN(dmin)} – ${gN(dmax)}</b> · todos los puntos` : `<b>${gN(cur[0])} – ${gN(cur[1])}</b> · ${gN(nN + nM, 0)} de ${gN(todos2.length, 0)} puntos (${gN(nN, 0)} nuevos · ${gN(nM, 0)} del modelado)`;
            [["t", "Todos", nN + nM], ["n", "Nuevos", nN], ["m", "Modelado", nM]].forEach(([k, t, n]) => { const b = $(`[data-smf="${k}"]`); if (b) b.innerHTML = `${t} <em>${gN(n, 0)}</em>`; });
            if (LBL && SEL && !dentro(P.all[SEL])) { m.removeLayer(LBL); LBL = null; } if (s.repintaCards) s.repintaCards(); pintaBox();
            if (fit) { const q = todos2.filter(p => dentro(p) && p.lat != null).map(p => [p.lat, p.lng]); if (q.length) m.fitBounds(L.latLngBounds(q).pad(.15), { maxZoom: 17 }); else if (full) fitN(); } };
          const frac = ev => { const r = tr.getBoundingClientRect(); return Math.max(0, Math.min(1, (ev.clientX - r.left) / r.width)); }, val = ev => dmin + frac(ev) * (dmax - dmin), gap = (dmax - dmin) * .01;
          const mueve = (i, v) => { if (i === 0) cur[0] = Math.max(dmin, Math.min(v, cur[1] - gap)); else cur[1] = Math.min(dmax, Math.max(v, cur[0] + gap)); };
          const programa = () => { if (!raf) raf = requestAnimationFrame(() => { raf = 0; aplica(false); }); };
          h.forEach((el, i) => { el.addEventListener("pointerdown", ev => { ev.preventDefault(); ev.stopPropagation(); el.setPointerCapture(ev.pointerId); el.classList.add("on"); });
            el.addEventListener("pointermove", ev => { if (!el.hasPointerCapture(ev.pointerId)) return; mueve(i, val(ev)); pinta(); programa(); });
            el.addEventListener("pointerup", ev => { el.classList.remove("on"); try { el.releasePointerCapture(ev.pointerId); } catch (_) { } aplica(true); });
            el.addEventListener("keydown", ev => { const k = ev.key === "ArrowLeft" || ev.key === "ArrowDown" ? -1 : ev.key === "ArrowRight" || ev.key === "ArrowUp" ? 1 : 0; if (!k) return; ev.preventDefault(); mueve(i, cur[i] + k * (ev.shiftKey ? 5 : 1) * (dmax - dmin) / 100); aplica(true); }); });
          /* arrastrar la ventana completa para desplazarla */
          let off = null; W.addEventListener("pointerdown", ev => { if (ev.target.closest("[data-h]")) return; ev.preventDefault(); W.setPointerCapture(ev.pointerId); off = [val(ev) - cur[0], cur[1] - cur[0]]; });
          W.addEventListener("pointermove", ev => { if (!off || !W.hasPointerCapture(ev.pointerId)) return; let a = val(ev) - off[0]; a = Math.max(dmin, Math.min(a, dmax - off[1])); cur = [a, a + off[1]]; pinta(); programa(); });
          W.addEventListener("pointerup", ev => { if (!off) return; off = null; try { W.releasePointerCapture(ev.pointerId); } catch (_) { } aplica(true); });
          tr.addEventListener("pointerdown", ev => { if (ev.target.closest("#simRngW")) return; const v = val(ev); mueve(Math.abs(v - cur[0]) <= Math.abs(v - cur[1]) ? 0 : 1, v); aplica(true); });
          RR.addEventListener("click", () => { cur = [dmin, dmax]; aplica(true); }); pinta(); aplica(false); } }
      if (!P.reg) { const lg = P.cl.map((c, k) => `<span><i style="background:${PAL_C[k % PAL_C.length]}"></i>${esc(c)}</span>`).join(""); if (lg) { const C = L.control({ position: "bottomleft" }); C.onAdd = () => { const d = L.DomUtil.create("div", "mo-boxc"); d.innerHTML = `<div class="mo-sm-lg" style="margin:0">${lg}</div>`; return d; }; C.addTo(m); } } });
  }
  async function simReeval() {
    const s = simS(), j = s.cur, sel = $("#smVer2"), v = verDe(sel ? sel.value : null); if (!j || !j.res || !v) return;
    try { const F = { filas: j.res.filas, sinAgeb: j.res.sinAgeb || 0 }, out = await simEvalua(v.id, F.filas, "Evaluando de nuevo"); const m = j.conjunto.sim || {}; const meta = Object.assign({}, m, { verNombre: v.nombre, metodo: v.res.metodo, tipoY: v.res.tipoY }); await simGuardaRes(j.conjunto.id, j.puntos, v, out, F, meta); s.cargado = false; toast("Conjunto evaluado con " + v.nombre + "."); await secSimulacion(); } catch (e) { toast(e.message, 6000); }
  }
  function simCsv() {
    const j = simS().cur; if (!j || !j.res) return; const res = j.res, pts = j.puntos, cl = res.clases || [];
    descarga("puntos-nuevos-" + (j.conjunto.nombre || "conjunto").replace(/[^a-z0-9]+/gi, "-").toLowerCase() + ".csv", [["id", "latitud", "longitud", "prediccion"].concat(res.tipoY === "regresion" ? ["inferior_ic95", "superior_ic95", "error_tipico_rmse"] : []).concat(cl.map(c => "prob_" + c)).concat(["nota"]).concat(res.cols)].concat(pts.map((q, i) => [(!q.id || /^P\d+$/.test(String(q.id))) ? "Nuevo " + (i + 1) : q.id, q.lat, q.lng, res.pred[i] == null ? "" : res.pred[i]].concat(res.tipoY === "regresion" ? (() => { const E2 = simErr((verDe(res.ver) || {}).res || S.res); return res.pred[i] == null || !E2 ? ["", "", ""] : [Math.round((res.pred[i] - 1.96 * E2.rmse) * 1e4) / 1e4, Math.round((res.pred[i] + 1.96 * E2.rmse) * 1e4) / 1e4, Math.round(E2.rmse * 1e4) / 1e4]; })() : []).concat(cl.map((_, k) => res.prob && res.prob[i] ? res.prob[i][k] : "")).concat([res.motivo && res.motivo[i] || ""]).concat(res.filas[i] || []))));
  }
  async function simGuardaEsc() {
    const s = simS(), L = s.last; if (!L) return; const nom = (($("#smEscNom") || {}).value || "").trim() || "Escenario " + (s.escenarios.length + 1);
    try { const j = await api(R + "/proyectos/" + S.id + "/sim-escenarios", { escenario: { nombre: nom, ver: L.ver.id, verNombre: L.ver.nombre, valores: L.vals, resultado: { txt: simTxt(L.out, 0), pred: L.out.pred[0] } } }); s.escenarios.unshift(j.escenario); toast("Escenario guardado."); simPinta(); } catch (e) { toast(e.message, 5000); }
  }
  function simClic(e) {
    const t = e.target, s = simS(); let b;
    if (S.sec !== "simulacion" || S.wiz) return false;
    if ((b = t.closest(".mo-sg > summary"))) { const d = b.parentNode; setTimeout(() => { s.ab[d.dataset.g] = d.open; }, 0); return false; }
    if ((b = t.closest("[data-bx]")) && s.sel) { s.sel(b.dataset.bx, false); return true; }
    if ((b = t.closest("[data-smf]"))) { s.fl = b.dataset.smf; $$("[data-smf]").forEach(x => x.classList.toggle("on", x === b)); if (s.repintaCards) s.repintaCards(); return true; }
    if (t.closest(".mo-sr-ab")) return false;
    if ((b = t.closest("[data-smcard]")) && s.sel) { s.sel(b.dataset.smcard, true); return true; }
    if ((b = t.closest("[data-smtab]"))) { s.tab = b.dataset.smtab; s.view = null; simPinta(); return true; }
    if ((b = t.closest("[data-smabre]"))) { e.preventDefault(); s.view = b.dataset.smabre; s.tab = "puntos"; simVista(); return true; }
    if ((b = t.closest("[data-smdel]"))) { simElimina(b.dataset.smdel); return true; }
    if ((b = t.closest("[data-smesccarga]"))) { const x = s.escenarios.find(q => q.id === b.dataset.smesccarga); if (x) { s.vals = Object.assign(simDefaults(), x.valores || {}); if (verDe(x.ver)) s.ver = x.ver; s.last = null; s.tab = "valores"; simPinta(); toast("Valores cargados: pulsa «Evaluar»."); } return true; }
    if ((b = t.closest("[data-smescdel]"))) { api(R + "/proyectos/" + S.id + "/sim-escenarios/" + b.dataset.smescdel, null, "DELETE").then(() => { s.escenarios = s.escenarios.filter(q => q.id !== b.dataset.smescdel); simPinta(); }).catch(er => toast(er.message)); return true; }
    if ((b = t.closest("[data-sm]"))) { const a = b.dataset.sm;
      if (a === "expande" || a === "contrae") { $$(".mo-sg").forEach(d => { d.open = a === "expande"; s.ab[d.dataset.g] = d.open; }); return true; } if (a === "eval") { simEvalManual(); return true; } if (a === "reset") { s.vals = simDefaults(); s.last = null; simPinta(); return true; } if (a === "guardaesc") { simGuardaEsc(); return true; }
      if (a === "nuevo") { simNuevo(); return true; } if (a === "volver") { s.view = null; clearTimeout(s._tm); simPinta(); return true; } if (a === "refresca") { simPoll(); return true; } if (a === "reeval") { simReeval(); return true; } if (a === "csv") { simCsv(); return true; }
      if (a === "cargacoord") { const la = parseFloat($("#smLat").value), lo = parseFloat($("#smLng").value); if (!isFinite(la) || !isFinite(lo) || Math.abs(la) > 90 || Math.abs(lo) > 180) { toast("Escribe una latitud y una longitud válidas."); return true; } simCargaIndic(la, lo); return true; } }
    return false;
  }
  function simEntrada(e) { const t = e.target; if (S.sec !== "simulacion") return false; if (t.id === "smQ") { simS().q = t.value; if (simS().repintaCards) simS().repintaCards(); return true; } if (t.dataset && t.dataset.sv != null) { simS().vals[t.dataset.sv] = t.value; simS().last = simS().last; return true; } return false; }
  function simCambia(e) {
    const t = e.target, s = simS(); if (S.sec !== "simulacion") return false;
    if (t.dataset && t.dataset.sv != null) { s.vals[t.dataset.sv] = t.value; return true; }
    if (t.id === "smVer") { s.ver = t.value; s.last = null; simPinta(); return true; }
    if (t.id === "smPto") { const i = +t.value; if (t.value !== "" && S.ds && S.ds.rows[i]) { const f = S.ds.rows[i]; S.ds.cols.forEach((c, j) => { if ((S.M.x || []).includes(c.name)) s.vals[c.name] = f[j] == null ? "" : String(typeof f[j] === "number" ? Math.round(f[j] * 1000) / 1000 : f[j]); }); s.last = null; simPinta(); toast("Valores del punto cargados."); } return true; }
    return false;
  }

  /* ═══════════════ Reporte ejecutivo (PDF: se abre maquetado y se guarda como PDF desde el diálogo de impresión) ═══════════════ */
  const RP_CSS = `@page{size:A4;margin:18mm 15mm 20mm}@page :first{margin:0}
*{box-sizing:border-box}html{-webkit-print-color-adjust:exact;print-color-adjust:exact}
body{margin:0;background:#e9edf5;color:#0f172a;font:400 10.5pt/1.55 "Inter","Helvetica Neue",Helvetica,Arial,sans-serif;-webkit-font-smoothing:antialiased}
.pg{max-width:210mm;margin:0 auto;background:#fff}
.cv{height:297mm;position:relative;overflow:hidden;color:#fff;padding:26mm 22mm;display:flex;flex-direction:column;background:radial-gradient(120% 80% at 85% 0%,#4338ca 0%,rgba(67,56,202,0) 60%),radial-gradient(90% 70% at 0% 100%,#0891b2 0%,rgba(8,145,178,0) 55%),#0b1020;break-after:page}
.cv .lg{height:34px;align-self:flex-start}.cv .ey{margin-top:auto;font:600 10pt/1 Inter,"Helvetica Neue",Arial,sans-serif;letter-spacing:.28em;text-transform:uppercase;color:#a5b4fc}
.cv h1{font:800 40pt/1.05 Inter,"Helvetica Neue",Arial,sans-serif;margin:12px 0 14px;letter-spacing:-.025em}.cv .ob{font:300 13pt/1.5 Inter,"Helvetica Neue",Arial,sans-serif;color:#cbd5e1;max-width:150mm;margin:0 0 26px}
.cv .kr{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:8px}.cv .kr div{border:1px solid rgba(255,255,255,.22);background:rgba(255,255,255,.07);border-radius:12px;padding:12px 14px}
.cv .kr b{display:block;font:800 17pt/1.15 Inter,"Helvetica Neue",Arial,sans-serif}.cv .kr span{font-size:8.5pt;color:#c7d2fe;letter-spacing:.04em;text-transform:uppercase}
.cv .pi{margin-top:26px;font-size:9pt;color:#94a3b8;display:flex;justify-content:space-between;border-top:1px solid rgba(255,255,255,.18);padding-top:12px}
.bd{padding:0 1mm}.sc{break-before:page;padding-top:2mm}.sc:first-of-type{break-before:auto}
.sh{display:flex;align-items:baseline;gap:12px;border-bottom:2px solid #e0e7ff;padding-bottom:8px;margin:0 0 14px;break-after:avoid}
.sh i{font:800 26pt/1 Inter,"Helvetica Neue",Arial,sans-serif;font-style:normal;color:#c7d2fe}.sh h2{font:800 19pt/1.15 Inter,"Helvetica Neue",Arial,sans-serif;margin:0;letter-spacing:-.015em;color:#1e1b4b}.sh small{margin-left:auto;color:#64748b;font-size:8.5pt}
h3{font:700 12pt/1.3 Inter,"Helvetica Neue",Arial,sans-serif;color:#1e1b4b;margin:18px 0 6px;break-after:avoid}h4{font:700 10.5pt/1.3 Inter,"Helvetica Neue",Arial,sans-serif;margin:0 0 6px;color:#1e1b4b}
p{margin:0 0 8px}.mu{color:#64748b}.lead{font:400 12pt/1.6 Inter,"Helvetica Neue",Arial,sans-serif;color:#334155}
.kg{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin:10px 0 14px}.kg div{border:1px solid #e0e7ff;border-radius:11px;padding:10px 12px;background:linear-gradient(180deg,#f8faff,#fff);break-inside:avoid}
.kg b{display:block;font:800 17pt/1.1 Inter,"Helvetica Neue",Arial,sans-serif;color:var(--c,#4338ca)}.kg span{display:block;font-size:8.5pt;color:#475569;margin-top:2px}.kg small{display:block;font-size:7.5pt;color:#94a3b8;margin-top:2px}
.fi{display:grid;grid-template-columns:42mm 1fr;border-top:1px solid #e2e8f0;font-size:10pt}.fi div{padding:7px 0;border-bottom:1px solid #eef2f7}.fi div:nth-child(odd){color:#64748b;font-weight:600;font-size:9pt;letter-spacing:.03em;text-transform:uppercase;padding-top:9px}
table{width:100%;border-collapse:collapse;font-size:8.8pt;margin:6px 0 12px}th{text-align:left;font:700 7.8pt/1.3 Inter,"Helvetica Neue",Arial,sans-serif;letter-spacing:.06em;text-transform:uppercase;color:#64748b;border-bottom:1.5px solid #c7d2fe;padding:6px 8px}td{padding:6px 8px;border-bottom:1px solid #eef2f7;vertical-align:top}tr{break-inside:avoid}td small{display:block;color:#94a3b8;font-size:7.8pt}
.tg{display:inline-block;padding:1px 8px;border-radius:99px;font-size:7.8pt;font-weight:700;background:#eef2ff;color:#4338ca}.tg.y{background:#dcfce7;color:#166534}.tg.n{background:#fef3c7;color:#92400e}.tg.d{background:#cffafe;color:#155e75}.tg.a{background:#f1f5f9;color:#475569}
.ba{border-left:4px solid #4f46e5;background:#f5f7ff;border-radius:0 12px 12px 0;padding:12px 16px;margin:10px 0 14px;break-inside:avoid}.ba b{color:#312e81}
ul.hz{padding-left:18px;margin:6px 0}ul.hz li{margin:0 0 5px}
.gg{display:grid;grid-template-columns:1fr 1fr;gap:14px}.gg .an{grid-column:1/-1}
.gr{border:1px solid #e2e8f0;border-radius:12px;padding:12px 14px;break-inside:avoid;margin:0 0 12px}.gg .gr{margin:0}
.gr h4{font-size:10pt}.int{background:#f8fafc;border-radius:8px;padding:7px 10px;margin-top:8px;font-size:8.5pt;color:#475569}.int b{display:block;font-size:7.5pt;letter-spacing:.08em;text-transform:uppercase;color:#4338ca;margin-bottom:2px}.int p{margin:0}
svg{width:100%;height:auto;display:block;font:500 9px Inter,system-ui,sans-serif;fill:#475569}svg .lb{font-size:9px;fill:#334155}svg .vl{font-size:8.5px;fill:#0f172a;font-weight:700}svg .ax{font-size:9px;fill:#64748b}svg .ce{font-size:8.5px;font-weight:700}
.ft{position:fixed;left:15mm;right:15mm;bottom:8mm;font-size:7.5pt;color:#94a3b8;display:flex;justify-content:space-between;border-top:1px solid #e2e8f0;padding-top:5px}
.no{background:#0b1020;color:#fff;text-align:center;padding:10px;font:600 10pt Inter,system-ui,sans-serif;position:sticky;top:0;z-index:9}.no button{margin-left:12px;border:0;border-radius:8px;padding:6px 14px;background:#6366f1;color:#fff;font:600 10pt Inter,system-ui;cursor:pointer}
@media print{body{background:#fff}.pg{max-width:none}.no{display:none}.cv{height:296mm}}
@media screen{.pg{box-shadow:0 10px 40px rgba(15,23,42,.15);margin:18px auto;padding-bottom:20px}.cv{margin:0}.bd{padding:14mm 16mm 6mm}.ft{display:none}}
.mo-gauge,.mo-ga{display:flex;align-items:center;gap:6px}.mo-ga i{display:block;height:6px;border-radius:3px;max-width:60px}.mo-ga b{font-size:8.5pt}
.mo-sig{color:#059669;font-weight:700}.mo-mal{color:#dc2626}`;
  async function reporte() {
    const r = resActual(); if (!r || !S.P) { toast("Aún no hay resultados para el reporte."); return; }
    const w = window.open("", "_blank"); if (!w) { toast("El navegador bloqueó la ventana del reporte. Permite las ventanas emergentes e inténtalo de nuevo."); return; }
    w.document.write('<!DOCTYPE html><meta charset="utf-8"><title>Generando reporte…</title><body style="font:15px system-ui;padding:40px;color:#334155">Preparando el reporte…</body>');
    try { await aseguraDs(); } catch (_) { }
    const P = S.P, M = S.M || {}, ds = S.ds, sup = r.enfoque === "sup", pts = S.puntos || [], hoy = new Date().toLocaleDateString("es-MX", { day: "numeric", month: "long", year: "numeric" });
    const FUENTE = { mapa: "Puntos elegidos en el mapa", archivo: "Archivo cargado por el usuario", denue: "Directorio de unidades económicas (DENUE, INEGI)", geolab: "Proyecto de Geo Lab" };
    const usadas = sup ? [M.y].concat(M.x || []).filter(Boolean) : ((M.x && M.x.length) ? M.x : (M.vars || []));
    const col = n => ds && ds.cols ? ds.cols.findIndex(c => c.name === n) : -1;
    const stat = n => { const j = col(n); if (j < 0) return null; const c = ds.cols[j], v = ds.rows.map(f => f[j]).filter(x => x != null && x !== ""); const falt = ds.rows.length - v.length; if (c.tipo === "cat" || (v.length && typeof v[0] !== "number")) { const k = {}; v.forEach(x => { k[x] = (k[x] || 0) + 1; }); const top = Object.entries(k).sort((a, b) => b[1] - a[1])[0]; return { n: v.length, falt, txt: "Categórica · " + Object.keys(k).length + " categorías" + (top ? " (más frecuente: " + top[0] + ")" : "") }; } const nm = v.filter(x => isFinite(x)); if (!nm.length) return { n: 0, falt, txt: "Sin datos" }; const mn = Math.min(...nm), mx = Math.max(...nm), me = nm.reduce((a, b) => a + b, 0) / nm.length; return { n: nm.length, falt, txt: "Prom. " + gN(me) + " · mín. " + gN(mn) + " · máx. " + gN(mx) }; };
    const orig = n => origen(n);
    const filasV = usadas.map(n => { const b = BIBK()[n], o = orig(n), s = stat(n), rol = sup && n === M.y ? '<span class="tg y">Respuesta</span>' : '<span class="tg">Predictora</span>', cd = ds && col(n) >= 0 ? ds.cols[col(n)] : null; return `<tr><td><b>${esc(nomVar(n))}</b><small>${esc(b ? (b.c === b.e ? b.c : b.c + " › " + b.e) : (P.fuente === "denue" ? "Unidades del DENUE" : "Datos del proyecto"))}</small></td><td>${rol}</td><td><span class="tg ${o.k === "lib" ? "d" : o.k === "denue" ? "n" : "a"}">${esc(o.t)}</span></td><td>${esc(b ? b.u : (cd && cd.unit) || "—")}</td><td>${s ? esc(s.txt) : "—"}${s && s.falt ? `<small>${gN(s.falt, 0)} sin dato (${gN(s.falt / Math.max(1, ds.rows.length) * 100, 0)} %)</small>` : ""}</td></tr>`; }).join("");
    const G = {}; usadas.forEach(n => { const o = orig(n), b = BIBK()[n], k = b ? b.e : (o.k === "denue" ? "DENUE · unidades del proyecto" : o.k === "archivo" && P.fuente === "geolab" ? "Geo Lab" : "Archivo del proyecto"); (G[k] = G[k] || { o: o.t, c: b ? b.c : "Datos propios", l: [] }).l.push(nomVar(n)); });
    const filasF = Object.entries(G).sort((a, b) => b[1].l.length - a[1].l.length).map(([k, v]) => `<tr><td><b>${esc(k)}</b><small>${esc(v.c)}</small></td><td><span class="tg ${v.o === "Biblioteca" ? "d" : "a"}">${esc(v.o)}</span></td><td>${v.l.length}</td><td>${esc(v.l.slice(0, 6).join(", "))}${v.l.length > 6 ? "…" : ""}</td></tr>`).join("");
    const pasoTxt = p => typeof p === "string" ? p : [p.titulo || p.nombre || p.paso || p.id, p.detalle || p.descripcion || p.texto].filter(Boolean).join(" — ");
    const pp = (r.prep && r.prep.pasos) || [], cmp = S.res && S.res._cmp, gan = cmp && cmp.modelos.find(m => m.id === cmp.elegido), rn = cmp && cmp.tipoY === "regresion" ? "R²" : "exactitud balanceada";
    const limpia = html => { const d = document.createElement("div"); d.innerHTML = html; d.querySelectorAll("button,.mo-kpis,.mo-sx2,.mo-chs").forEach(e => e.remove()); d.querySelectorAll("details").forEach(e => e.setAttribute("open", "")); d.querySelectorAll("article").forEach(a => { if (a.querySelector("#moMapRes")) a.remove(); }); d.querySelectorAll(".mo-gg").forEach(e => e.className = "gg"); d.querySelectorAll("article.mo-gr").forEach(e => { e.className = "gr" + (e.classList.contains("mo-ancho") ? " an" : ""); }); d.querySelectorAll(".mo-int").forEach(e => e.className = "int"); d.querySelectorAll(".mo-tw").forEach(e => e.replaceWith(...e.childNodes)); d.querySelectorAll("table").forEach(t => t.removeAttribute("class")); d.querySelectorAll("svg").forEach(s => s.removeAttribute("class")); return d.innerHTML; };
    let resH = limpia(resultadosHtml(r)); if (!sup && r.clusters && pts.length) { const a = r.clusters.asignacion || []; resH += `<div class="gr"><h4>Mapa de los puntos por grupo</h4>${svgDisp(S.ds.lng, S.ds.lat, { col: a, xl: "Longitud", yl: "Latitud" })}</div>`; }
    const calH = limpia(calidadHtml(r)), la = pts.map(q => +q.lat).filter(isFinite), lo = pts.map(q => +q.lng).filter(isFinite);
    const mapaH = la.length ? `<div class="gr"><h4>Ubicación de los puntos del proyecto</h4>${svgDisp(lo, la, { xl: "Longitud", yl: "Latitud", w: 640, h: 300 })}</div>` : "";
    const kp = (r.kpis || []).map(k => `<div style="--c:${TONO[k.tono] || TONO.neutro}"><b>${esc(fmtK(k))}</b><span>${esc(k.etiqueta)}</span>${k.nota ? `<small>${esc(k.nota)}</small>` : ""}</div>`).join("");
    const hall = (r.hallazgos || []).map(h => `<li>${esc(hum(h))}</li>`).join("");
    const cmpT = cmp ? `<h3>Comparación de modelos</h3><p class="mu">Se entrenaron ${cmp.modelos.length} modelos con los mismos datos. El puntaje combina ${rn} en prueba y validación cruzada y penaliza el sobreajuste.</p><table><thead><tr><th>#</th><th>Modelo</th><th>Puntaje</th><th>${rn} prueba</th><th>${rn} val. cruzada</th><th>Sobreajuste</th><th></th></tr></thead><tbody>${cmp.modelos.map((m, i) => m.error || !nz(m.score) ? `<tr><td>—</td><td>${esc(m.nombre)}</td><td colspan="4" class="mu">No se pudo ajustar</td><td></td></tr>` : `<tr><td>${i + 1}</td><td><b>${esc(m.nombre)}</b><small>${esc(m.familia || "")}</small></td><td>${gN(m.score, 3)}</td><td>${gN(m.prueba, 3)}</td><td>${m.cv != null ? gN(m.cv, 3) : "—"}</td><td>${m.gap == null ? "—" : gN(m.gap, 2)}</td><td>${m.id === cmp.elegido ? '<span class="tg y">Elegido</span>' : ""}</td></tr>`).join("")}</tbody></table>` : "";
    const sc = (n, t, sub, cuerpo) => `<section class="sc"><div class="sh"><i>${n}</i><h2>${t}</h2><small>${esc(sub || "")}</small></div>${cuerpo}</section>`;
    const nombreMet = esc(r.nombreMetodo || r.metodo || "—"), enf = sup ? (r.tipoY === "regresion" ? "Supervisado · regresión (predecir un valor numérico)" : "Supervisado · clasificación (predecir una categoría)") : "No supervisado (descubrir patrones sin variable de respuesta)";
    const html = `<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"><title>Reporte · ${esc(P.nombre || "Proyecto")}</title><base href="${esc(location.href)}"><link rel="preconnect" href="https://fonts.googleapis.com"><link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"><style>${RP_CSS}</style></head><body>
<div class="no">Reporte listo · en el diálogo de impresión elige «Guardar como PDF» <button onclick="window.print()">Descargar PDF</button></div>
<div class="pg"><div class="cv"><img class="lg" src="modelab-logo-claro.png" alt="Model Lab"><div class="ey">Reporte ejecutivo · Modelado</div><h1>${esc(P.nombre || "Proyecto")}</h1><p class="ob">${esc(String(P.objetivo || "").slice(0, 420))}</p>
<div class="kr"><div><b>${gN(P.nPuntos || pts.length, 0)}</b><span>Coordenadas</span></div><div><b>${gN(usadas.length, 0)}</b><span>Variables</span></div><div><b>${nombreMet}</b><span>Método</span></div></div>
<div class="pi"><span>${esc(P.zona || "")}</span><span>Elaborado el ${hoy} · Model Lab · Check Inn</span></div></div>
<div class="bd">
${sc("01", "Resumen ejecutivo", "Lo esencial en una página", `<p class="lead">${esc(String(P.objetivo || "Sin objetivo capturado."))}</p><div class="ba"><b>Método usado:</b> ${nombreMet}${gan && cmp.modelos.length > 1 ? ` — elegido automáticamente entre ${cmp.modelos.length} modelos comparados.` : "."}<br><b>Enfoque:</b> ${esc(enf)}.</div><div class="kg">${kp}</div>${hall ? `<h3>Hallazgos principales</h3><ul class="hz">${hall}</ul>` : ""}`)}
${sc("02", "El proyecto", "Alcance y datos de origen", `<div class="fi"><div>Nombre</div><div>${esc(P.nombre || "—")}</div><div>Objetivo</div><div>${esc(P.objetivo || "—")}</div><div>Origen de los puntos</div><div>${esc(FUENTE[P.fuente] || "—")}</div><div>Coordenadas</div><div>${gN(P.nPuntos || pts.length, 0)} puntos de interés</div><div>Zona</div><div>${esc(P.zona || "—")}</div><div>Creado</div><div>${esc(fecha(P.creado))}</div><div>Análisis realizado</div><div>${esc(fecha(M.hecho || P.actualizado))}</div>${ds ? `<div>Tabla de datos</div><div>${gN(ds.rows.length, 0)} observaciones × ${gN(ds.cols.length, 0)} variables${ds.sinAgeb ? ` · ${gN(ds.sinAgeb, 0)} puntos fuera de una AGEB urbana (sin indicadores de la biblioteca)` : ""}</div>` : ""}</div>${mapaH}`)}
${sc("03", "Fuentes de datos", "De dónde sale la información", `<p class="mu">Los indicadores de la biblioteca se obtienen de bases públicas precalculadas por AGEB (Censo de Población y Vivienda, DENUE, OpenStreetMap, INEGI, ESA WorldCover, WorldClim y otras) en la ubicación de cada punto.</p><table><thead><tr><th>Fuente / grupo</th><th>Tipo</th><th>Vars.</th><th>Ejemplos</th></tr></thead><tbody>${filasF}</tbody></table>`)}
${sc("04", "Variables del modelo", "Indicadores e índices con su ficha", `<p class="mu">${usadas.length} variables entraron al modelo${sup ? ": 1 de respuesta y " + (usadas.length - 1) + " predictoras" : ""}.</p><table><thead><tr><th>Variable</th><th>Rol</th><th>Origen</th><th>Unidad</th><th>Resumen estadístico</th></tr></thead><tbody>${filasV}</tbody></table>`)}
${sc("05", "Método y preparación", "Cómo se construyó el modelo", `<div class="fi"><div>Enfoque</div><div>${esc(enf)}</div><div>Método</div><div>${nombreMet}</div>${r.prep && r.prep.nTrain ? `<div>Entrenamiento / prueba</div><div>${gN(r.prep.nTrain, 0)} / ${gN(r.prep.nTest, 0)} observaciones (semilla fija 42, 5 pliegues de validación cruzada)</div>` : ""}${r.prep && r.prep.columnasFinales ? `<div>Columnas finales</div><div>${gN(r.prep.columnasFinales.length, 0)}${(r.prep.descartadas || []).length ? ` (${gN(r.prep.descartadas.length, 0)} descartadas)` : ""}</div>` : ""}</div>${pp.length ? `<h3>Preprocesamiento aplicado</h3><ul class="hz">${pp.map(p => `<li>${esc(hum(pasoTxt(p)))}</li>`).join("")}</ul>` : ""}${cmpT}`)}
${sc("06", "Resultados del modelado", "Qué encontró el modelo", resH)}
${sc("07", "Confiabilidad del modelo", "Cuánto confiar en los resultados", calH)}
<section class="sc"><div class="sh"><i>08</i><h2>Notas</h2></div><p class="mu">Este reporte describe asociaciones estadísticas calculadas sobre los datos del proyecto; no demuestra causalidad. La confiabilidad de las conclusiones depende de la cantidad y calidad de las observaciones, de la cobertura de los indicadores en cada punto y de la sección «Confiabilidad del modelo». Los indicadores de la biblioteca son valores por AGEB, no mediciones en el punto exacto.</p></section>
</div></div><div class="ft"><span>Model Lab · ${esc(P.nombre || "")}</span><span>${hoy}</span></div>
<script>window.addEventListener("load",function(){setTimeout(function(){window.print()},900)})<\/script></body></html>`;
    w.document.open(); w.document.write(html); w.document.close();
  }

  window.modelabInit = async function () {
    host = document.getElementById("modelab-host"); if (!host) return; shell(); nav(); vistaProyectos(); await cargaProy(); if (S.sec === "proyectos") vistaProyectos();
    const pid = new URLSearchParams(location.search).get("p"); if (pid && (S.proy || []).some(x => x.id === pid)) abre(pid);
  };
})();
