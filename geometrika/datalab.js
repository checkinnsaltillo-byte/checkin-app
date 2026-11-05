/* Data Lab · pantalla principal: menú lateral desplegable + sección «Fuente de datos» (resumen, filtros y tablas que se abren y se cierran).
   Necesita datalab-fuentes.js (window.DL_FUENTES, DL_CATS, DL_FECHA) y datalab.css. Se inicia con window.datalabInit(). */
(function () {
  "use strict";
  const $ = (s, r) => (r || document).querySelector(s), esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const norm = s => String(s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
  const ls = { get(k, d) { try { const v = localStorage.getItem(k); return v == null ? d : v; } catch (_) { return d; } }, set(k, v) { try { localStorage.setItem(k, v); } catch (_) { } } };
  const ICON = {
    menu: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M4 6h16M4 12h16M4 18h16"/></svg>',
    lupa: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>',
    chev: '<svg class="dl-chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>',
    chevS: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>'
  };
  const F = window.DL_FUENTES || [], CATS = window.DL_CATS || [], INDS = (window.DL_INDICADORES || []).flatMap(c => c.an.flatMap(e => e.ind));
  const S = { q: "", t: "", a: "", cat: "", m: "", e: "", ind: "", abiertos: new Set(CATS.map(c => c.k)), filas: new Set(), sort: { k: "n", d: 1 } };
  let host;

  const cls = v => String(v).replace(/^De pago$/, "Pago");
  const uniq = (f) => [...new Set(F.map(f).flat())].filter(Boolean).sort((a, b) => a.localeCompare(b, "es"));
  const hay = (f) => norm([f.n, f.p, f.t, f.a, f.m, f.e, f.ob, f.cob, f.act, f.alm, f.lic, f.nota, (f.ind || []).join(" ")].join(" "));
  F.forEach(f => { f._h = hay(f); });

  function pasa(f) {
    if (S.t && f.t !== S.t) return false; if (S.a && f.a !== S.a) return false; if (S.cat && f.cat !== S.cat) return false;
    if (S.m && f.m !== S.m) return false; if (S.e && f.e !== S.e) return false; if (S.ind && !(f.ind || []).includes(S.ind)) return false;
    if (S.q) { const ws = norm(S.q).split(/\s+/).filter(Boolean); if (!ws.every(w => f._h.includes(w))) return false; }
    return true;
  }
  const hayFiltros = () => !!(S.q || S.t || S.a || S.cat || S.m || S.e || S.ind);

  function opciones(id, titulo, lista, val) {
    return `<label class="dl-sel${val ? " on" : ""}"><select data-f="${id}" aria-label="${esc(titulo)}"><option value="">${esc(titulo)}</option>${lista.map(o => { const v = Array.isArray(o) ? o[0] : o, t = Array.isArray(o) ? o[1] : o; return `<option value="${esc(v)}"${val === v ? " selected" : ""}>${esc(t)}</option>`; }).join("")}</select></label>`;
  }

  function shell() {
    const tot = F.length;
    host.innerHTML = `<div class="dl-app" id="dlApp">
      <header class="dl-top">
        <button class="dl-burger" id="dlBurger" type="button" aria-label="Mostrar u ocultar el menú">${ICON.menu}</button>
        <img class="dl-logo" src="datalab-logo-claro.png" alt="Data Lab">
        <div class="dl-brand"><b>Sistema de Datos y Fuentes</b><span>Fuentes de información e indicadores</span></div>
        <div class="dl-sp"></div>
        <span class="dl-chip">v.Beta.1</span>
        <button class="dl-btn" id="dlCambiar" type="button" title="Volver a elegir entre Geo Lab y Data Lab"><span class="lg">Cambiar de sistema</span><span class="sh">Cambiar</span></button>
        <button class="dl-btn" id="dlSalir" type="button"><span class="lg">Cerrar sesión</span><span class="sh">Salir</span></button>
      </header>
      <div class="dl-body">
        <aside class="dl-side" id="dlSide" aria-label="Menú de Data Lab">
          <div class="dl-side-t">Menú</div>
          <button class="dl-nav on" type="button" data-sec="fuentes" title="Fuente de datos"><span class="ic">🗄️</span><span class="tx">Fuente de datos</span><span class="bd">${tot}</span></button>
          <button class="dl-nav" type="button" data-sec="indicadores" title="Indicadores"><span class="ic">📊</span><span class="tx">Indicadores</span><span class="bd">${INDS.length}</span></button>
          <div class="dl-side-f">Data Lab · Check Inn</div>
        </aside>
        <main class="dl-main" id="dlMain"><div class="dl-wrap" id="dlWrap"></div></main>
      </div></div>`;
    const app = $("#dlApp", host);
    if (innerWidth < 900 || ls.get("dl_side", "on") === "off") app.classList.add("dl-off");   // en celular siempre inicia cerrado
    $("#dlBurger", host).onclick = () => { app.classList.toggle("dl-off"); if (innerWidth >= 900) ls.set("dl_side", app.classList.contains("dl-off") ? "off" : "on"); };
    $("#dlSalir", host).onclick = () => window.DL_SALIR && window.DL_SALIR();
    $("#dlCambiar", host).onclick = () => window.DL_CAMBIAR && window.DL_CAMBIAR();
    $("#dlMain", host).addEventListener("scroll", () => { const f = $(".dl-fl", host); if (f) f.classList.toggle("st", $("#dlMain", host).scrollTop > 140); }, { passive: true });
    host.addEventListener("click", clic); host.addEventListener("input", entrada); host.addEventListener("change", cambia);
    // en pantallas angostas el menú es un cajón: se cierra al elegir una sección
    $("#dlSide", host).addEventListener("click", e => { const n = e.target.closest(".dl-nav"); if (!n) return; ir(n.dataset.sec); if (innerWidth < 900) app.classList.add("dl-off"); });
  }

  function fuentes() {
    const pub = F.filter(f => f.t === "Pública").length, pri = F.length - pub, pago = F.filter(f => f.a === "De pago").length, act = F.filter(f => f.e === "Activa").length;
    const kp = (n, t, c, f, v) => `<button type="button" class="dl-kpi${S[f] === v ? " on" : ""}" style="--c:${c}" data-kpi="${f}" data-v="${esc(v)}"><b>${n}</b><span>${t}</span></button>`;
    $("#dlWrap", host).innerHTML = `
      <div class="dl-h"><div><h1>Fuente de datos</h1><p>De dónde sale cada dato que usan los análisis: quién lo provee, si es público o privado, cuánto cuesta, para qué sirve y qué indicadores alimenta. Toca una fila para ver más detalle.</p></div><span class="dl-up">Actualizado el ${esc(window.DL_FECHA || "")}</span></div>
      <div class="dl-kpis">
        <button type="button" class="dl-kpi${hayFiltros() ? "" : " on"}" style="--c:#4f46e5" data-kpi="todo"><b>${F.length}</b><span>Fuentes en total</span></button>
        ${kp(pub, "Públicas", "#059669", "t", "Pública")}${kp(pri, "Privadas", "#7c3aed", "t", "Privada")}${kp(pago, "De pago", "#b45309", "a", "De pago")}${kp(act, "Activas", "#0891b2", "e", "Activa")}
      </div>
      <div class="dl-fl" id="dlFl">
        <div class="dl-fr">
          <div class="dl-q${S.q ? " has" : ""}">${ICON.lupa}<input id="dlQ" type="search" placeholder="Buscar fuente, empresa, indicador u objetivo…" value="${esc(S.q)}" autocomplete="off" aria-label="Buscar"><button class="x" type="button" data-x="q" aria-label="Borrar búsqueda">✕</button></div>
          ${opciones("t", "Tipo", ["Pública", "Privada"], S.t)}
          ${opciones("a", "Costo / acceso", ["Gratuita", "De pago", "Propia"], S.a)}
          ${opciones("cat", "Categoría", CATS.map(c => [c.k, c.n]), S.cat)}
          ${opciones("m", "Método", ["Descarga masiva", "API en vivo", "Interna"], S.m)}
          ${opciones("e", "Estado", ["Activa", "Parcial", "Respaldo", "Pendiente"], S.e)}
          ${opciones("ind", "Indicador", uniq(f => f.ind || []), S.ind)}
          <button class="dl-ghost" type="button" data-a="limpiar"${hayFiltros() ? "" : " disabled style=\"opacity:.45;cursor:default\""}>Limpiar</button>
          <button class="dl-ghost pr" type="button" data-a="abrir">Expandir todo</button>
          <button class="dl-ghost pr" type="button" data-a="cerrar">Contraer todo</button>
        </div>
        <div class="dl-rs" id="dlRs"></div>
      </div>
      <div id="dlLista"></div>
      <div class="dl-foot">Fuente: inventario interno de Check Inn. Las tarifas son precios de lista y pueden cambiar.</div>`;
    lista();
  }

  function lista() {
    const vis = F.filter(pasa), dest = $("#dlLista", host); if (!dest) return;
    const etiquetas = []; [["q", S.q ? "“" + S.q + "”" : ""], ["t", S.t], ["a", S.a], ["cat", (CATS.find(c => c.k === S.cat) || {}).n], ["m", S.m], ["e", S.e], ["ind", S.ind]].forEach(([k, v]) => { if (v) etiquetas.push(`<span class="dl-tag">${esc(v)}<button type="button" data-x="${k}" aria-label="Quitar filtro">✕</button></span>`); });
    $("#dlRs", host).innerHTML = `Mostrando <b>${vis.length}</b> de ${F.length} fuentes ${etiquetas.join("")}`;
    if (!vis.length) { dest.innerHTML = `<div class="dl-empty"><b>No hay fuentes con esos filtros</b>Prueba con otra palabra o quita algún filtro.<br><button class="dl-ghost pr" type="button" data-a="limpiar">Limpiar filtros</button></div>`; return; }
    const ord = (a, b) => { const k = S.sort.k, va = String(a[k] || ""), vb = String(b[k] || ""); return va.localeCompare(vb, "es") * S.sort.d; };
    dest.innerHTML = CATS.map(c => {
      const L = vis.filter(f => f.cat === c.k).sort(ord); if (!L.length) return "";
      const forzar = hayFiltros(), abierto = forzar || S.abiertos.has(c.k), pu = L.filter(f => f.t === "Pública").length, pr = L.length - pu;
      const th = (k, t) => `<th class="so${S.sort.k === k ? " sa" : ""}" data-s="${k}">${t}<span class="ar">${S.sort.k === k && S.sort.d < 0 ? "▼" : "▲"}</span></th>`;
      return `<section class="dl-g${abierto ? " open" : ""}" data-g="${c.k}">
        <button class="dl-gh" type="button" data-gh="${c.k}" aria-expanded="${abierto}">
          <span class="dl-gi">${c.ic}</span><span class="dl-gt"><b>${esc(c.n)}</b><span>${esc(c.d)}</span></span>
          <span class="dl-gc"><span class="dl-bar" title="${pu} públicas · ${pr} privadas"><i class="pu" style="width:${pu / L.length * 100}%"></i><i class="pr" style="width:${pr / L.length * 100}%"></i></span><span class="dl-gn">${L.length} ${L.length === 1 ? "fuente" : "fuentes"}</span></span>${ICON.chev}
        </button>
        <div class="dl-gb"><div><div class="dl-tw"><table class="dl-t"><thead><tr>${th("n", "Fuente")}${th("p", "Empresa / software")}${th("t", "Tipo")}${th("a", "Costo")}<th>Objetivo</th><th>Indicadores que alimenta</th><th class="dl-xc"></th></tr></thead><tbody>${L.map(fila).join("")}</tbody></table></div></div></div>
      </section>`;
    }).join("");
  }

  function fila(f) {
    const ab = S.filas.has(f.id), chips = (f.ind || []).map(i => `<button type="button" class="dl-c${S.ind === i ? " on" : ""}" data-ind="${esc(i)}" title="Filtrar por este indicador">${esc(i)}</button>`).join("");
    const det = (t, v) => v ? `<div><span>${t}</span><p>${v}</p></div>` : "";
    return `<tr class="dl-r${ab ? " open" : ""}" data-r="${f.id}" tabindex="0" aria-expanded="${ab}">
      <td data-l="Fuente"><div class="dl-nm"><i class="dot ${esc(f.e)}" title="Estado: ${esc(f.e)}"></i><b>${esc(f.n)}</b></div></td>
      <td data-l="Empresa / software" class="dl-pv">${esc(f.p)}</td>
      <td data-l="Tipo"><span class="dl-b ${cls(f.t)}">${esc(f.t)}</span></td>
      <td data-l="Costo"><span class="dl-b ${cls(f.a)}">${esc(f.a)}</span></td>
      <td data-l="Objetivo" class="dl-ob">${esc(f.ob)}</td>
      <td data-l="Indicadores que alimenta"><div class="dl-ch">${chips}</div></td>
      <td class="dl-xc">${ICON.chevS}</td></tr>${ab ? `<tr class="dl-d"><td colspan="7"><div class="in">
        ${det("Estado", `<span class="dl-b ${cls(f.e)}">${esc(f.e)}</span>`)}${det("Método de consulta", `<span class="dl-b Propia">${esc(f.m)}</span>`)}
        ${det("Cobertura", esc(f.cob))}${det("Actualización", esc(f.act))}${det("Dónde se guarda", esc(f.alm))}${det("Licencia o costo", esc(f.lic))}
        ${det("Observaciones", esc(f.nota))}${f.url ? det("Enlace", `<a href="${esc(f.url)}" target="_blank" rel="noopener noreferrer">${esc(f.url.replace(/^https?:\/\//, "").replace(/\/$/, ""))} ↗</a>`) : ""}
      </div></td></tr>` : ""}`;
  }

  function entrada(e) {
    if (e.target.id === "dlQ2") return entradaI(e);
    if (e.target.id !== "dlQ") return; S.q = e.target.value; e.target.parentNode.classList.toggle("has", !!S.q); clearTimeout(entrada.t);
    entrada.t = setTimeout(() => { lista(); refrescaBarra(); }, 120);
  }
  function cambia(e) { const f2 = e.target.dataset && e.target.dataset.f2; if (f2) return cambiaI(f2, e.target.value); const f = e.target.dataset && e.target.dataset.f; if (!f) return; S[f] = e.target.value; fuentes(); }
  function refrescaBarra() { const b = $('[data-a="limpiar"]', host); if (b) { b.disabled = !hayFiltros(); b.style.opacity = hayFiltros() ? "" : ".45"; b.style.cursor = hayFiltros() ? "" : "default"; } }

  function clic(e) {
    const t = e.target;
    if (sec === "indicadores") return clicI(e);
    const x = t.closest("[data-x]"); if (x) { S[x.dataset.x] = ""; fuentes(); return; }
    const a = t.closest("[data-a]");
    if (a) { const k = a.dataset.a; if (k === "limpiar" && hayFiltros()) { S.q = S.t = S.a = S.cat = S.m = S.e = S.ind = ""; fuentes(); } else if (k === "abrir") { CATS.forEach(c => S.abiertos.add(c.k)); F.forEach(f => S.filas.add(f.id)); lista(); } else if (k === "cerrar") { S.abiertos.clear(); S.filas.clear(); lista(); } return; }
    const kp = t.closest("[data-kpi]"); if (kp) { const f = kp.dataset.kpi; if (f === "todo") S.q = S.t = S.a = S.cat = S.m = S.e = S.ind = ""; else { S.t = S.a = S.e = ""; S[f] = S[f] === kp.dataset.v ? "" : kp.dataset.v; } fuentes(); return; }
    const c = t.closest("[data-ind]"); if (c) { S.ind = S.ind === c.dataset.ind ? "" : c.dataset.ind; fuentes(); return; }
    const s = t.closest("th[data-s]"); if (s) { const k = s.dataset.s; S.sort = { k, d: S.sort.k === k ? -S.sort.d : 1 }; lista(); return; }
    const g = t.closest("[data-gh]"); if (g) { const k = g.dataset.gh, sec = g.closest(".dl-g"); if (hayFiltros()) { sec.classList.toggle("open"); g.setAttribute("aria-expanded", sec.classList.contains("open")); return; } if (S.abiertos.has(k)) S.abiertos.delete(k); else S.abiertos.add(k); sec.classList.toggle("open"); g.setAttribute("aria-expanded", S.abiertos.has(k)); return; }
    if (t.closest("a")) return;
    const r = t.closest("tr[data-r]"); if (r) { alterna(r.dataset.r); }
  }
  document.addEventListener("keydown", e => { if (!host || !host.contains(document.activeElement)) return; const r = document.activeElement.closest && document.activeElement.closest("tr[data-r]"); if (r && (e.key === "Enter" || e.key === " ") && document.activeElement === r) { e.preventDefault(); alterna(r.dataset.r); } });
  function alterna(id) { if (S.filas.has(id)) S.filas.delete(id); else S.filas.add(id); const y = $("#dlMain", host).scrollTop; lista(); $("#dlMain", host).scrollTop = y; const r = $(`tr[data-r="${id}"]`, host); if (r) r.focus({ preventScroll: true }); }


  /* ═══ Sección «Indicadores»: catálogo de los indicadores de Geo Lab (sin datos de ningún proyecto) ═══ */
  const CATI = window.DL_INDICADORES || [];
  const FAM = f => { const s = String(f || ""), o = [], add = x => { if (!o.includes(x)) o.push(x); };
    if (/inegi|censo|denue|eic/i.test(s)) add("INEGI"); if (/open-?meteo|era5/i.test(s)) add("Open-Meteo"); if (/google/i.test(s)) add("Google");
    if (/osm|openstreetmap|red de calles/i.test(s)) add("OpenStreetMap"); if (/terrarium|relieve/i.test(s)) add("Terrain Tiles"); if (/ghsl/i.test(s)) add("GHSL (JRC)");
    if (/worldcover/i.test(s)) add("ESA WorldCover"); if (/\bia\b/i.test(s)) add("Inteligencia artificial");
    if (/propio|propia|supuesto|usuario|configurable|ficha|detecci|selector/i.test(s)) add("Cálculo propio o del usuario"); if (/pendiente/i.test(s)) add("Pendiente");
    if (!o.length) add("Otra"); return o; };
  const TCLS = { "Dato duro": "dd", "Dato calculado": "dc", "Índice": "ix", "Valoración cualitativa": "vl", "Valoración cuantitativa": "vl", "Dato inferido": "di" };
  CATI.forEach(c => c.an.forEach(e => e.ind.forEach(i => {
    i._c = c; i._e = e; const fam = []; (i.vars.length ? i.vars.map(v => v.f) : [i.f]).forEach(f => FAM(f).forEach(x => { if (!fam.includes(x)) fam.push(x); })); i._fam = fam;
    i._h = norm([i.n, i.d, i.u, i.m, i.f, i.tipo, e.n, c.n, i.vars.map(v => [v.n, v.d, v.f, v.u, v.tr.map(t => t.n + " " + t.d).join(" ")].join(" ")).join(" ")].join(" "));
    i._pend = /pendiente/i.test(i.f) && !i.vars.length; })));
  const INDS_ALL = () => CATI.flatMap(c => c.an.flatMap(e => e.ind));
  const S2 = { q: "", cat: "", enf: "", tipo: "", fam: "", aC: new Set(CATI.map(c => c.k || c.id)), aE: new Set(), filas: new Set() };
  const hayI = () => !!(S2.q || S2.cat || S2.enf || S2.tipo || S2.fam);
  function pasaI(i) {
    if (S2.cat && i._c.id !== S2.cat) return false; if (S2.enf && i._e.id !== S2.enf) return false; if (S2.tipo && i.tipo !== S2.tipo) return false; if (S2.fam && !i._fam.includes(S2.fam)) return false;
    if (S2.q) { const ws = norm(S2.q).split(/\s+/).filter(Boolean); if (!ws.every(w => i._h.includes(w))) return false; } return true;
  }
  function indicadores() {
    const A = INDS_ALL(), nE = CATI.reduce((a, c) => a + c.an.length, 0), nV = A.reduce((a, i) => a + i.vars.length, 0), nT = A.reduce((a, i) => a + i.vars.reduce((b, v) => b + v.tr.length, 0), 0);
    const kp = (n, t, c) => `<div class="dl-kpi" style="--c:${c};cursor:default"><b>${n}</b><span>${t}</span></div>`;
    const tipos = [...new Set(A.map(i => i.tipo))], fams = [...new Set(A.flatMap(i => i._fam))].sort((a, b) => a.localeCompare(b, "es"));
    const enfs = CATI.filter(c => !S2.cat || c.id === S2.cat).flatMap(c => c.an.map(e => [e.id, e.ico + " " + e.n]));
    $("#dlWrap", host).innerHTML = `
      <div class="dl-h"><div><h1>Indicadores</h1><p>Catálogo de los indicadores que calcula Geo Lab: qué mide cada uno, en qué unidades, cómo se calcula (metodología y fórmula), con qué variables y de qué fuente salen. Es solo un catálogo: aquí no hay datos de ningún proyecto. Toca una fila para ver el detalle.</p></div><span class="dl-up">Generado desde Geo Lab</span></div>
      <div class="dl-kpis">${kp(CATI.length, "Categorías", "#4f46e5")}${kp(nE, "Enfoques de análisis", "#0891b2")}${kp(A.length, "Indicadores", "#7c3aed")}${kp(nV, "Variables", "#059669")}${kp(nT, "Transformaciones", "#b45309")}</div>
      <div class="dl-fl" id="dlFl">
        <div class="dl-fr">
          <div class="dl-q${S2.q ? " has" : ""}">${ICON.lupa}<input id="dlQ2" type="search" placeholder="Buscar indicador, variable, fuente o fórmula…" value="${esc(S2.q)}" autocomplete="off" aria-label="Buscar"><button class="x" type="button" data-ix="q" aria-label="Borrar búsqueda">✕</button></div>
          ${opcionesI("cat", "Categoría", CATI.map(c => [c.id, c.ico + " " + c.n]), S2.cat)}
          ${opcionesI("enf", "Enfoque", enfs, S2.enf)}
          ${opcionesI("tipo", "Tipo de dato", tipos, S2.tipo)}
          ${opcionesI("fam", "Fuente", fams, S2.fam)}
          <button class="dl-ghost" type="button" data-ia="limpiar"${hayI() ? "" : " disabled style=\"opacity:.45;cursor:default\""}>Limpiar</button>
          <button class="dl-ghost pr" type="button" data-ia="abrir">Expandir todo</button>
          <button class="dl-ghost pr" type="button" data-ia="cerrar">Contraer todo</button>
        </div>
        <div class="dl-rs" id="dlRs2"></div>
      </div>
      <div id="dlLista2"></div>
      <div class="dl-foot">Catálogo generado automáticamente a partir de la definición de indicadores de Geo Lab.</div>`;
    listaI();
  }
  function opcionesI(id, titulo, lista, val) {
    return `<label class="dl-sel${val ? " on" : ""}"><select data-f2="${id}" aria-label="${esc(titulo)}"><option value="">${esc(titulo)}</option>${lista.map(o => { const v = Array.isArray(o) ? o[0] : o, t = Array.isArray(o) ? o[1] : o; return `<option value="${esc(v)}"${val === v ? " selected" : ""}>${esc(t)}</option>`; }).join("")}</select></label>`;
  }
  function listaI() {
    const dest = $("#dlLista2", host); if (!dest) return; const forzar = hayI(), A = INDS_ALL(), vis = A.filter(pasaI);
    const tg = []; [["q", S2.q ? "“" + S2.q + "”" : ""], ["cat", (CATI.find(c => c.id === S2.cat) || {}).n], ["enf", (INDS_ALL().map(i => i._e).find(e => e.id === S2.enf) || {}).n], ["tipo", S2.tipo], ["fam", S2.fam]].forEach(([k, v]) => { if (v) tg.push(`<span class="dl-tag">${esc(v)}<button type="button" data-ix="${k}" aria-label="Quitar filtro">✕</button></span>`); });
    $("#dlRs2", host).innerHTML = `Mostrando <b>${vis.length}</b> de ${A.length} indicadores ${tg.join("")}`;
    if (!vis.length) { dest.innerHTML = `<div class="dl-empty"><b>No hay indicadores con esos filtros</b>Prueba con otra palabra o quita algún filtro.<br><button class="dl-ghost pr" type="button" data-ia="limpiar">Limpiar filtros</button></div>`; return; }
    dest.innerHTML = CATI.map(c => {
      const L = vis.filter(i => i._c.id === c.id); if (!L.length) return ""; const abC = forzar || S2.aC.has(c.id);
      const enfs = c.an.map(e => { const Le = L.filter(i => i._e.id === e.id); if (!Le.length) return ""; const abE = forzar || S2.aE.has(e.id);
        return `<section class="dl-g dl-sg${abE ? " open" : ""}" data-e="${e.id}"><button class="dl-gh" type="button" data-ie="${e.id}" aria-expanded="${abE}"><span class="dl-gi s">${e.ico}</span><span class="dl-gt"><b>${esc(e.n)}</b><span>${esc(e.d)}</span></span><span class="dl-gc"><span class="dl-gn">${Le.length} ${Le.length === 1 ? "indicador" : "indicadores"}</span></span>${ICON.chev}</button>
          <div class="dl-gb"><div><div class="dl-tw"><table class="dl-t dl-ti"><thead><tr><th>Indicador</th><th>Tipo</th><th>Qué mide</th><th>Unidades</th><th>Variables</th><th>Fuentes</th><th class="dl-xc"></th></tr></thead><tbody>${Le.map(filaI).join("")}</tbody></table></div></div></div></section>`; }).join("");
      return `<section class="dl-g${abC ? " open" : ""}" data-c="${c.id}"><button class="dl-gh" type="button" data-ic="${c.id}" aria-expanded="${abC}"><span class="dl-gi">${c.ico}</span><span class="dl-gt"><b>${esc(c.n)}</b><span>${esc(c.d)}</span></span><span class="dl-gc"><span class="dl-gn">${L.length} ${L.length === 1 ? "indicador" : "indicadores"}</span></span>${ICON.chev}</button>
        <div class="dl-gb"><div class="dl-sgs">${enfs}</div></div></section>`;
    }).join("");
  }
  function filaI(i) {
    const k = i._e.id + ":" + i.id, ab = S2.filas.has(k), fam = i._fam.map(f => `<button type="button" class="dl-c${S2.fam === f ? " on" : ""}" data-ifam="${esc(f)}" title="Filtrar por esta fuente">${esc(f)}</button>`).join("");
    const vars = i.vars.map(v => `<tr><td data-l="Variable"><b>${esc(v.n)}</b></td><td data-l="Unidades">${esc(v.u)}</td><td data-l="Qué es">${esc(v.d)}</td><td data-l="Fuente">${esc(v.f)}</td><td data-l="Transformaciones">${v.tr.length ? `<details class="dl-tr"><summary>${v.tr.length} ${v.tr.length === 1 ? "transformación" : "transformaciones"}</summary>${v.tr.map(t => `<div class="dl-trx"><b>${esc(t.n)}</b><p>${esc(t.d)}</p><code>${esc(t.f)}</code></div>`).join("")}</details>` : `<span class="dl-mut">Dato directo de la fuente</span>`}</td></tr>`).join("");
    return `<tr class="dl-r${ab ? " open" : ""}" data-ir="${esc(k)}" tabindex="0" aria-expanded="${ab}">
      <td data-l="Indicador"><div class="dl-nm"><i class="dot${i._pend ? " Pendiente" : ""}" title="${i._pend ? "Pendiente de construir" : "Disponible"}"></i><b>${esc(i.n)}</b></div></td>
      <td data-l="Tipo"><span class="dl-b t-${TCLS[i.tipo] || "dd"}">${esc(i.tipo)}</span></td>
      <td data-l="Qué mide" class="dl-ob">${esc(i.d)}</td><td data-l="Unidades">${esc(i.u)}</td>
      <td data-l="Variables"><span class="dl-gn">${i.vars.length}</span></td>
      <td data-l="Fuentes"><div class="dl-ch">${fam}</div></td><td class="dl-xc">${ICON.chevS}</td></tr>${ab ? `<tr class="dl-d"><td colspan="7"><div class="in one">
        <div><span>Metodología</span><p>${esc(i.m)}</p></div><div><span>Fórmula</span><code class="dl-code">${esc(i.f)}</code></div>
        <div class="wide"><span>Variables (${i.vars.length})</span>${i.vars.length ? `<div class="dl-tw in2"><table class="dl-t dl-vt"><thead><tr><th>Variable</th><th>Unidades</th><th>Qué es</th><th>Fuente</th><th>Transformaciones</th></tr></thead><tbody>${vars}</tbody></table></div>` : `<p class="dl-mut">Todavía no tiene variables: se construirá más adelante.</p>`}</div>
      </div></td></tr>` : ""}`;
  }
  function entradaI(e) { S2.q = e.target.value; e.target.parentNode.classList.toggle("has", !!S2.q); clearTimeout(entradaI.t); entradaI.t = setTimeout(() => { listaI(); const b = $('[data-ia="limpiar"]', host); if (b) { b.disabled = !hayI(); b.style.opacity = hayI() ? "" : ".45"; b.style.cursor = hayI() ? "" : "default"; } }, 120); }
  function cambiaI(k, v) { S2[k] = v; if (k === "cat" && S2.enf && !(CATI.find(c => c.id === v) || { an: [] }).an.some(e => e.id === S2.enf) && v) S2.enf = ""; indicadores(); }
  function clicI(e) {
    const t = e.target, x = t.closest("[data-ix]"); if (x) { S2[x.dataset.ix] = ""; indicadores(); return; }
    const a = t.closest("[data-ia]"); if (a) { const k = a.dataset.ia; if (k === "limpiar" && hayI()) { S2.q = S2.cat = S2.enf = S2.tipo = S2.fam = ""; indicadores(); } else if (k === "abrir") { CATI.forEach(c => { S2.aC.add(c.id); c.an.forEach(en => { S2.aE.add(en.id); en.ind.forEach(i => S2.filas.add(en.id + ":" + i.id)); }); }); listaI(); } else if (k === "cerrar") { S2.aC.clear(); S2.aE.clear(); S2.filas.clear(); listaI(); } return; }
    const f = t.closest("[data-ifam]"); if (f) { S2.fam = S2.fam === f.dataset.ifam ? "" : f.dataset.ifam; indicadores(); return; }
    const tg = (attr, set, id) => { const g = t.closest("[" + attr + "]"); if (!g) return false; const sec2 = g.closest(".dl-g"); if (hayI()) sec2.classList.toggle("open"); else { if (set.has(id(g))) set.delete(id(g)); else set.add(id(g)); sec2.classList.toggle("open", set.has(id(g))); } g.setAttribute("aria-expanded", sec2.classList.contains("open")); return true; };
    if (tg("data-ie", S2.aE, g => g.dataset.ie)) return; if (tg("data-ic", S2.aC, g => g.dataset.ic)) return;
    if (t.closest("a,details,summary,code,.dl-vt")) return;
    const r = t.closest("tr[data-ir]"); if (r) alternaI(r.dataset.ir);
  }
  function alternaI(k) { if (S2.filas.has(k)) S2.filas.delete(k); else S2.filas.add(k); const y = $("#dlMain", host).scrollTop; listaI(); $("#dlMain", host).scrollTop = y; const r = [...host.querySelectorAll("tr[data-ir]")].find(q => q.dataset.ir === k); if (r) r.focus({ preventScroll: true }); }
  document.addEventListener("keydown", e => { if (!host || sec !== "indicadores" || !host.contains(document.activeElement)) return; const r = document.activeElement.closest && document.activeElement.closest("tr[data-ir]"); if (r && (e.key === "Enter" || e.key === " ") && document.activeElement === r) { e.preventDefault(); alternaI(r.dataset.ir); } });

  let sec = "fuentes";
  function ir(s) {
    if (s !== "fuentes" && s !== "indicadores") s = "fuentes"; sec = s; host.querySelectorAll(".dl-nav").forEach(n => n.classList.toggle("on", n.dataset.sec === s));
    try { history.replaceState(null, "", "#" + s); } catch (_) { } $("#dlMain", host).scrollTop = 0; if (s === "indicadores") indicadores(); else fuentes();
  }

  window.datalabInit = function () {
    host = document.getElementById("datalab-host"); if (!host || host.dataset.listo) return; host.dataset.listo = "1";
    shell(); ir(location.hash === "#indicadores" ? "indicadores" : "fuentes");
  };
})();
