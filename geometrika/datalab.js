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
        <a class="dl-btn dl-go" id="dlIrGeo" href="index.html?s=geo" target="_blank" rel="noopener" title="Abrir Geo Lab en una ventana nueva"><img src="geometrika-logo-claro.png" alt=""><span class="lg">Ir a Geo Lab</span><span class="sh">Geo Lab</span></a>
        <button class="dl-btn" id="dlSalir" type="button"><span class="lg">Cerrar sesión</span><span class="sh">Salir</span></button>
      </header>
      <div class="dl-body">
        <aside class="dl-side" id="dlSide" aria-label="Menú de Data Lab">
          <div class="dl-side-t">Menú</div>
          <button class="dl-nav on" type="button" data-sec="fuentes" title="Fuente de datos"><span class="ic">🗄️</span><span class="tx">Fuente de datos</span><span class="bd">${tot}</span></button>
          <button class="dl-nav" type="button" data-sec="indicadores" title="Indicadores"><span class="ic">📊</span><span class="tx">Indicadores</span><span class="bd">${INDS_ALL().length}</span></button>
          <div class="dl-side-f">Data Lab · Check Inn</div>
        </aside>
        <main class="dl-main" id="dlMain"><div class="dl-wrap" id="dlWrap"></div></main>
      </div><div class="dl-ov" id="dlOv"></div><aside class="dl-dr" id="dlDr" aria-hidden="true" aria-label="Crear indicador"></aside></div>`;
    const app = $("#dlApp", host);
    if (innerWidth < 900 || ls.get("dl_side", "on") === "off") app.classList.add("dl-off");   // en celular siempre inicia cerrado
    $("#dlBurger", host).onclick = () => { app.classList.toggle("dl-off"); if (innerWidth >= 900) ls.set("dl_side", app.classList.contains("dl-off") ? "off" : "on"); };
    $("#dlSalir", host).onclick = () => window.DL_SALIR && window.DL_SALIR();
    
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
          ${opciones("e", "Estado", ["Activa", "Parcial", "Respaldo", "Pendiente", ...(F.some(f => f._creado) ? ["Publicado", "Propuesto", "Borrador"] : [])], S.e)}
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
  const prep = (i, c, e) => {
    i._c = c; i._e = e; const fam = []; (i.vars.length ? i.vars.map(v => v.f) : [i.f]).forEach(f => FAM(f).forEach(x => { if (!fam.includes(x)) fam.push(x); })); i._fam = fam;
    i._h = norm([i.n, i.d, i.u, i.m, i.f, i.tipo, e.n, c.n, i._meta ? [i._meta.objetivo, i._meta.res, i._meta.periodicidad, i._meta.instrucciones, i._meta.responsable].join(" ") : "", i.vars.map(v => [v.n, v.d, v.f, v.u, (v.tr || []).map(t => t.n + " " + t.d).join(" ")].join(" ")).join(" ")].join(" "));
    i._pend = /pendiente/i.test(i.f) && !i.vars.length; };
  CATI.forEach(c => c.an.forEach(e => e.ind.forEach(i => prep(i, c, e))));
  const INDS_ALL = () => CATI.flatMap(c => c.an.flatMap(e => e.ind));
  const S2 = { q: "", cat: "", enf: "", tipo: "", fam: "", org: "", aC: new Set(CATI.map(c => c.k || c.id)), aE: new Set(), filas: new Set() };
  const hayI = () => !!(S2.q || S2.cat || S2.enf || S2.tipo || S2.fam || S2.org);
  function pasaI(i) {
    if (S2.cat && i._c.id !== S2.cat) return false; if (S2.enf && i._e.id !== S2.enf) return false; if (S2.tipo && i.tipo !== S2.tipo) return false; if (S2.fam && !i._fam.includes(S2.fam)) return false; if (S2.org === "dl" && !i._nuevo) return false; if (S2.org === "geo" && i._nuevo) return false;
    if (S2.q) { const ws = norm(S2.q).split(/\s+/).filter(Boolean); if (!ws.every(w => i._h.includes(w))) return false; } return true;
  }
  function indicadores() {
    const A = INDS_ALL(), nE = CATI.reduce((a, c) => a + c.an.length, 0), nV = A.reduce((a, i) => a + i.vars.length, 0), nT = A.reduce((a, i) => a + i.vars.reduce((b, v) => b + v.tr.length, 0), 0);
    const kp = (n, t, c) => `<div class="dl-kpi" style="--c:${c};cursor:default"><b>${n}</b><span>${t}</span></div>`;
    const tipos = [...new Set(A.map(i => i.tipo))], fams = [...new Set(A.flatMap(i => i._fam))].sort((a, b) => a.localeCompare(b, "es"));
    const enfs = CATI.filter(c => !S2.cat || c.id === S2.cat).flatMap(c => c.an.map(e => [e.id, e.ico + " " + e.n]));
    $("#dlWrap", host).innerHTML = `
      <div class="dl-h"><div><h1>Indicadores</h1><p>Catálogo de los indicadores que calcula Geo Lab: qué mide cada uno, en qué unidades, cómo se calcula (metodología y fórmula), con qué variables y de qué fuente salen. Es solo un catálogo: aquí no hay datos de ningún proyecto. Toca una fila para ver el detalle.</p></div><div class="dl-hr"><span class="dl-up">Generado desde Geo Lab</span><button class="dl-new" type="button" id="dlNuevo">＋ Crear Indicador</button></div></div>
      <div class="dl-kpis">${kp(CATI.length, "Categorías", "#4f46e5")}${kp(nE, "Enfoques de análisis", "#0891b2")}${kp(A.length, "Indicadores", "#7c3aed")}${kp(nV, "Variables", "#059669")}${kp(nT, "Transformaciones", "#b45309")}</div>
      <div class="dl-fl" id="dlFl">
        <div class="dl-fr">
          <div class="dl-q${S2.q ? " has" : ""}">${ICON.lupa}<input id="dlQ2" type="search" placeholder="Buscar indicador, variable, fuente o fórmula…" value="${esc(S2.q)}" autocomplete="off" aria-label="Buscar"><button class="x" type="button" data-ix="q" aria-label="Borrar búsqueda">✕</button></div>
          ${opcionesI("cat", "Categoría", CATI.map(c => [c.id, c.ico + " " + c.n]), S2.cat)}
          ${opcionesI("enf", "Enfoque", enfs, S2.enf)}
          ${opcionesI("tipo", "Tipo de dato", tipos, S2.tipo)}
          ${opcionesI("fam", "Fuente", fams, S2.fam)}
          ${opcionesI("org", "Origen", [["geo", "Geo Lab"], ["dl", "Creados en Data Lab"]], S2.org)}
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
    const tg = []; [["q", S2.q ? "“" + S2.q + "”" : ""], ["cat", (CATI.find(c => c.id === S2.cat) || {}).n], ["enf", (INDS_ALL().map(i => i._e).find(e => e.id === S2.enf) || {}).n], ["tipo", S2.tipo], ["fam", S2.fam], ["org", S2.org === "dl" ? "Creados en Data Lab" : S2.org === "geo" ? "Geo Lab" : ""]].forEach(([k, v]) => { if (v) tg.push(`<span class="dl-tag">${esc(v)}<button type="button" data-ix="${k}" aria-label="Quitar filtro">✕</button></span>`); });
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
  // «En Geo Lab»: si el indicador se muestra, dónde, cómo se ve en el mapa y una tarjeta de ejemplo real (datalab-geo.js)
  const CUANDO = { P: ["🆕", "Al crear el proyecto", "Se consulta una vez al crear el proyecto y se guarda con él; después solo se lee."], I: ["🔁", "Cada vez que se ingresa", "Se calcula o se consulta de nuevo cada vez que se abre la maqueta o se pulsa «Calcular y actualizar»."], D: ["👆", "Bajo demanda", "Solo se genera cuando alguien pulsa el botón correspondiente; luego queda guardado o en pantalla."], G: ["📦", "Ya guardado en el sistema", "Es un dato de referencia (Censo, DENUE, INEGI, WorldCover…) ya cargado de antemano: no hay que generarlo."], X: ["⏳", "Pendiente de construir", "Todavía no se genera."] };
  function chipGeo(i) { const g = !i._nuevo && (window.DL_GEO || {})[i._e.id + ":" + i.id]; return g ? `<span class="dl-geo-chip${g.m ? " map" : ""}" title="${g.m ? "Se muestra en Geo Lab y se dibuja en el mapa" : "Se muestra en Geo Lab (tarjeta y ventana); no se dibuja en el mapa"}">${g.m ? "🗺️ En el mapa" : "📋 Solo tarjeta"}</span>${CUANDO[g.w] ? `<span class="dl-geo-chip w${g.w}" title="${esc(CUANDO[g.w][2])}">${CUANDO[g.w][0]} ${esc(CUANDO[g.w][1])}</span>` : ""}` : ""; }
  function bloqueGeo(i) {
    if (i._nuevo) { const pub = i._meta && i._meta.publicado; return `<div class="wide dl-geo-b"><span>En Geo Lab</span><p>${pub ? "✓ Publicado: aparece en el panel Indicadores y como ventana en Análisis › Indicadores generados." : "Todavía no se muestra en Geo Lab: aparecerá cuando se publique."}</p></div>`; }
    const g = (window.DL_GEO || {})[i._e.id + ":" + i.id]; if (!g) return `<div class="wide dl-geo-b"><span>En Geo Lab</span><p class="dl-mut">Todavía no se muestra en Geo Lab.</p></div>`;
    return `<div class="wide dl-geo-b"><span>En Geo Lab</span><div class="dl-geo-g">
      <div class="dl-geo-ej">${g.h ? `<div class="dl-geo">${g.h}</div><small>Así se ve la tarjeta en el panel Indicadores (ejemplo real: ${esc(window.DL_GEO_PROY || "")}).</small>` : `<div class="dl-geo-nd">Sin tarjeta de ejemplo: en el proyecto de muestra no hubo dato para este indicador.</div>`}</div>
      <ul class="dl-geo-l"><li><b>✓ Se muestra</b> en el panel <b>Indicadores</b> y en la ventana <b>${esc(g.v)}</b> (menú Análisis).</li>
      <li>${g.m ? "<b>🗺️ En el mapa:</b> " : "<b>Sin capa en el mapa.</b> "}${esc(g.t)}</li>${CUANDO[g.w] ? `<li><b>⏱️ Cuándo se genera:</b> <span class="dl-geo-chip w${g.w}">${CUANDO[g.w][0]} ${esc(CUANDO[g.w][1])}</span> ${esc(g.wn || "")}<div class="dl-mut">${esc(CUANDO[g.w][2])}</div></li>` : ""}</ul></div></div>`;
  }
  function filaI(i) {
    const k = i._e.id + ":" + i.id, ab = S2.filas.has(k), fam = i._fam.map(f => `<button type="button" class="dl-c${S2.fam === f ? " on" : ""}" data-ifam="${esc(f)}" title="Filtrar por esta fuente">${esc(f)}</button>`).join("");
    const vars = i.vars.map(v => `<tr><td data-l="Variable"><b>${esc(v.n)}</b></td><td data-l="Unidades">${esc(v.u)}</td><td data-l="Qué es">${esc(v.d)}</td><td data-l="Fuente">${esc(v.f)}${v.cob ? `<div class="dl-mut">Cobertura: ${esc(v.cob)}</div>` : ""}${v.disponible === false ? `<div><span class="dl-b Parcial">Dato por conseguir</span></div>` : ""}${v.nota ? `<div class="dl-mut">${esc(v.nota)}</div>` : ""}</td><td data-l="Transformaciones">${v.tr.length ? `<details class="dl-tr"><summary>${v.tr.length} ${v.tr.length === 1 ? "transformación" : "transformaciones"}</summary>${v.tr.map(t => `<div class="dl-trx"><b>${esc(t.n)}</b><p>${esc(t.d)}</p><code>${esc(t.f)}</code></div>`).join("")}</details>` : `<span class="dl-mut">Dato directo de la fuente</span>`}</td></tr>`).join("");
    return `<tr class="dl-r${ab ? " open" : ""}" data-ir="${esc(k)}" tabindex="0" aria-expanded="${ab}">
      <td data-l="Indicador"><div class="dl-nm">${i._nuevo ? swPub(i._meta) : `<i class="dot${i._pend ? " Pendiente" : ""}" title="${i._pend ? "Pendiente de construir" : "Publicado en Geo Lab"}"></i>`}<b>${esc(i.n)}</b>${chipGeo(i)}${i._nuevo ? `<span class="dl-new-b${i._meta.publicado ? " pub" : ""}">${i._meta.publicado ? "Publicado en Geo Lab" : "Nuevo · " + esc(i._meta.estado)}</span>` : ""}</div></td>
      <td data-l="Tipo"><span class="dl-b t-${TCLS[i.tipo] || "dd"}">${esc(i.tipo)}</span></td>
      <td data-l="Qué mide" class="dl-ob">${esc(i.d)}</td><td data-l="Unidades">${esc(i.u)}</td>
      <td data-l="Variables"><span class="dl-gn">${i.vars.length}</span></td>
      <td data-l="Fuentes"><div class="dl-ch">${fam}</div></td><td class="dl-xc">${ICON.chevS}</td></tr>${ab ? `<tr class="dl-d"><td colspan="7"><div class="in one">
        ${bloqueGeo(i)}<div><span>Metodología</span><p class="pre">${esc(i.m)}</p></div><div><span>Fórmula</span><code class="dl-code">${esc(i.f)}</code></div>${i._meta ? metaCreado(i._meta) : ""}
        <div class="wide"><span>Variables (${i.vars.length})</span>${i.vars.length ? `<div class="dl-tw in2"><table class="dl-t dl-vt"><thead><tr><th>Variable</th><th>Unidades</th><th>Qué es</th><th>Fuente</th><th>Transformaciones</th></tr></thead><tbody>${vars}</tbody></table></div>` : `<p class="dl-mut">Todavía no tiene variables: se construirá más adelante.</p>`}</div>
      </div></td></tr>` : ""}`;
  }
  function entradaI(e) { S2.q = e.target.value; e.target.parentNode.classList.toggle("has", !!S2.q); clearTimeout(entradaI.t); entradaI.t = setTimeout(() => { listaI(); const b = $('[data-ia="limpiar"]', host); if (b) { b.disabled = !hayI(); b.style.opacity = hayI() ? "" : ".45"; b.style.cursor = hayI() ? "" : "default"; } }, 120); }
  function cambiaI(k, v) { S2[k] = v; if (k === "cat" && S2.enf && !(CATI.find(c => c.id === v) || { an: [] }).an.some(e => e.id === S2.enf) && v) S2.enf = ""; indicadores(); }
  function clicI(e) {
    const t = e.target, x = t.closest("[data-ix]"); if (x) { S2[x.dataset.ix] = ""; indicadores(); return; }
    const a = t.closest("[data-ia]"); if (a) { const k = a.dataset.ia; if (k === "limpiar" && hayI()) { S2.q = S2.cat = S2.enf = S2.tipo = S2.fam = S2.org = ""; indicadores(); } else if (k === "abrir") { CATI.forEach(c => { S2.aC.add(c.id); c.an.forEach(en => { S2.aE.add(en.id); en.ind.forEach(i => S2.filas.add(en.id + ":" + i.id)); }); }); listaI(); } else if (k === "cerrar") { S2.aC.clear(); S2.aE.clear(); S2.filas.clear(); listaI(); } return; }
    const pb = t.closest("[data-dl-pub]"); if (pb) { publica(pb.dataset.dlPub); return; }
    const del = t.closest("[data-dl-del]"); if (del) { eliminaCreado(del.dataset.dlDel); return; }
    const est = t.closest("[data-dl-est]"); if (est) { cambiaEstado(est.dataset.dlEst, est.dataset.v); return; }
    const f = t.closest("[data-ifam]"); if (f) { S2.fam = S2.fam === f.dataset.ifam ? "" : f.dataset.ifam; indicadores(); return; }
    const tg = (attr, set, id) => { const g = t.closest("[" + attr + "]"); if (!g) return false; const sec2 = g.closest(".dl-g"); if (hayI()) sec2.classList.toggle("open"); else { if (set.has(id(g))) set.delete(id(g)); else set.add(id(g)); sec2.classList.toggle("open", set.has(id(g))); } g.setAttribute("aria-expanded", sec2.classList.contains("open")); return true; };
    if (tg("data-ie", S2.aE, g => g.dataset.ie)) return; if (tg("data-ic", S2.aC, g => g.dataset.ic)) return;
    if (t.closest("a,details,summary,code,.dl-vt")) return;
    const r = t.closest("tr[data-ir]"); if (r) alternaI(r.dataset.ir);
  }
  function alternaI(k) { if (S2.filas.has(k)) S2.filas.delete(k); else S2.filas.add(k); const y = $("#dlMain", host).scrollTop; listaI(); $("#dlMain", host).scrollTop = y; const r = [...host.querySelectorAll("tr[data-ir]")].find(q => q.dataset.ir === k); if (r) r.focus({ preventScroll: true }); }
  document.addEventListener("keydown", e => { if (!host || sec !== "indicadores" || !host.contains(document.activeElement)) return; const r = document.activeElement.closest && document.activeElement.closest("tr[data-ir]"); if (r && (e.key === "Enter" || e.key === " ") && document.activeElement === r) { e.preventDefault(); alternaI(r.dataset.ir); } });


  /* ═══ Crear indicador con ayuda de IA (panel lateral derecho) ═══ */
  const API = "https://api.check-inn.mx", RES_TXT = { punto: "Punto / coordenada", zona: "Zona", ciudad: "Ciudad" };
  const PERIODOS = ["Tiempo real", "Diaria", "Semanal", "Mensual", "Trimestral", "Anual", "Bajo demanda"], TIPOS = ["Dato duro", "Dato calculado", "Índice", "Valoración cualitativa", "Valoración cuantitativa", "Dato inferido"];
  const token = () => { try { return (JSON.parse(localStorage.getItem("gmTok") || "null") || {}).token || ""; } catch (_) { return ""; } };
  async function api(ruta, cuerpo, metodo) {
    const r = await fetch(API + ruta, { method: metodo || (cuerpo ? "POST" : "GET"), headers: Object.assign({ "x-gm-token": token() }, cuerpo ? { "Content-Type": "application/json" } : {}), body: cuerpo ? JSON.stringify(cuerpo) : undefined });
    let j = {}; try { j = await r.json(); } catch (_) { }
    if (r.status === 401) throw new Error("Tu sesión expiró. Cierra sesión y vuelve a entrar.");
    if (!r.ok || j.ok === false) throw new Error(j.error || ("Error " + r.status)); return j;
  }
  const toast = (m, ms) => { let t = $("#dlToast", host); if (!t) { t = document.createElement("div"); t.id = "dlToast"; t.className = "dl-toast"; $("#dlApp", host).appendChild(t); } t.textContent = m; t.classList.add("on"); clearTimeout(toast.t); toast.t = setTimeout(() => t.classList.remove("on"), ms || 4200); };

  // ── dónde y cómo se calcula: unidad espacial, alcance y visualización (con vista previa)
  const UNI = { punto: ["📍", "Punto exacto", "Un valor para una coordenada, sin radio"], radio: ["⭕", "Radio alrededor de un punto", "Todo lo que cae a cierta distancia de la propiedad o del punto elegido"], manzana: ["▭", "Cada manzana", "Un valor por manzana del Censo (hay datos de todo México)"], hexagono: ["⬡", "Cada hexágono de 200 m", "Un valor por hexágono de la malla de la ciudad (por ahora solo Saltillo y Monterrey)"], ageb: ["🗺️", "Cada AGEB", "Un valor por AGEB urbana (conjunto de manzanas)"], municipio: ["🏛️", "Cada municipio", "Un valor por municipio"], ciudad: ["🏙️", "Toda la ciudad (una sola cifra)", "Una cifra para la ciudad o zona metropolitana"] };
  const UNI_RES = { punto: ["punto", "radio"], zona: ["radio", "manzana", "hexagono", "ageb", "municipio"], ciudad: ["ciudad", "municipio"] }, UNI_DEF = { punto: "punto", zona: "radio", ciudad: "ciudad" };
  const ALC = { proyecto: "Solo para el proyecto abierto (su punto y su radio)", maqueta: "Para toda el área de la maqueta", ciudad_completa: "Para todas las unidades de la ciudad (todos los hexágonos, manzanas, etc.)" };
  const VIS = { valor: "Una cifra con semáforo en la ventana «Indicadores»", mapa_zona: "La zona calculada resaltada en el mapa, con su cifra", coropleta: "Mapa coloreado por unidad, con leyenda", ranking: "Tabla ordenada de las unidades (mejores y peores)" };
  const CLA = { cuartiles: "Colores por cuartiles (relativo a la ciudad)", semaforo: "Semáforo con umbrales (verde, ámbar, rojo)", continua: "Escala continua de color" };
  const SEN = { mayor_mejor: "Más es mejor", menor_mejor: "Menos es mejor", neutro: "Neutro (solo informativo)" };
  function resumenEsp(f) {
    const U = { punto: "un punto exacto", radio: "todo lo que está a " + (f.radio || 500) + " m de un punto", manzana: "cada manzana", hexagono: "cada hexágono de 200 m", ageb: "cada AGEB", municipio: "cada municipio", ciudad: "toda la ciudad (una sola cifra)" };
    const A = { proyecto: "solo para el proyecto abierto", maqueta: "para toda el área de la maqueta", ciudad_completa: "para todas las unidades de la ciudad" };
    const V = { valor: "una cifra con semáforo en la ventana Indicadores", mapa_zona: "la zona resaltada en el mapa con su cifra", coropleta: "un mapa coloreado por unidad con leyenda", ranking: "una tabla ordenada de unidades" };
    let t = "Se calcula sobre " + (U[f.unidad] || "—") + ", " + (A[f.alcance] || "—") + ". Se verá como " + (V[f.visual] || "—") + (f.visual === "coropleta" || f.visual === "ranking" || f.visual === "valor" ? ", con " + (CLA[f.clasif] || "").toLowerCase() : "") + ".";
    if (f.unidad === "hexagono") t += " Ojo: la malla de hexágonos existe por ahora solo en Saltillo y Monterrey.";
    if ((f.alcance === "ciudad_completa") && (f.unidad === "punto" || f.unidad === "radio")) t += " Ojo: «ciudad completa» con radio o punto no tiene sentido; elige manzana, hexágono, AGEB o municipio.";
    return t;
  }
  function prevSVG(f) {
    const u = f.unidad, vis = f.visual, alc = f.alcance, coro = vis === "coropleta", W = 300, H = 170, cx = 150, cy = 85;
    const ramp = t => { const a = [224, 242, 254], b = [30, 64, 175]; return "rgb(" + a.map((x, i) => Math.round(x + (b[i] - x) * Math.max(0, Math.min(1, t)))).join(",") + ")"; };
    const rnd = (i, j) => { const x = Math.sin(i * 12.9898 + j * 78.233) * 43758.5453; return x - Math.floor(x); };
    const GRIS = "#e2e8f0", colorea = (i, j, centro, dentro) => alc === "ciudad_completa" ? ramp(.15 + rnd(i, j) * .8) : alc === "maqueta" ? (dentro ? ramp(.15 + rnd(i, j) * .8) : GRIS) : (centro ? ramp(.75) : GRIS);
    let g = '<rect width="300" height="170" rx="12" fill="#f8fafc"/><g stroke="#cbd5e1" stroke-width="3" stroke-linecap="round"><path d="M0 52 H300 M0 118 H300 M60 0 V170 M150 0 V170 M240 0 V170"/></g>';
    const pin = (x, y) => '<g transform="translate(' + x + ',' + y + ')"><path d="M0 0 C-9 -10 -9 -22 0 -22 C9 -22 9 -10 0 0Z" fill="#ef4444" stroke="#fff" stroke-width="1.5"/><circle cx="0" cy="-15" r="3" fill="#fff"/></g>';
    const box = { x1: 70, y1: 32, x2: 230, y2: 138 };   // área «maqueta»
    if (u === "punto") { g += pin(cx, cy + 10); }
    else if (u === "radio") { g += '<circle cx="' + cx + '" cy="' + cy + '" r="52" fill="rgba(79,70,229,.14)" stroke="#4f46e5" stroke-width="2" stroke-dasharray="5 4"/><line x1="' + cx + '" y1="' + cy + '" x2="' + (cx + 52) + '" y2="' + cy + '" stroke="#4f46e5" stroke-width="1.5"/><text x="' + (cx + 26) + '" y="' + (cy - 5) + '" font-size="11" font-weight="700" fill="#3730a3" text-anchor="middle">' + (f.radio || 500) + ' m</text>' + pin(cx, cy + 2); }
    else if (u === "manzana") { const xs = [8, 66, 156, 246, 292], ys = [6, 58, 124, 166]; let i = 0; for (let c = 0; c < 4; c++) for (let r = 0; r < 3; r++, i++) { const x = xs[c] + 3, y = ys[r] + 3, w = xs[c + 1] - xs[c] - 6, h = ys[r + 1] - ys[r] - 6, cen = c === 1 && r === 1, din = x + w > box.x1 && x < box.x2 && y + h > box.y1 && y < box.y2; g += '<rect x="' + x + '" y="' + y + '" width="' + w + '" height="' + h + '" rx="4" fill="' + colorea(c, r, cen, din) + '" stroke="' + (cen ? "#1e3a8a" : "#fff") + '" stroke-width="' + (cen ? 2 : 1) + '"/>'; } g += pin(cx, cy + 10); }
    else if (u === "hexagono") { const R = 21, hx = R * Math.sqrt(3); for (let r = -3; r <= 3; r++) for (let c = -4; c <= 4; c++) { const x = cx + c * hx + (r % 2 ? hx / 2 : 0), y = cy + r * R * 1.5; if (x < -10 || x > W + 10 || y < -10 || y > H + 10) continue; const pts = [0, 1, 2, 3, 4, 5].map(k => { const a = Math.PI / 180 * (60 * k - 30); return (x + R * Math.cos(a)).toFixed(1) + "," + (y + R * Math.sin(a)).toFixed(1); }).join(" "), cen = c === 0 && r === 0, din = x > box.x1 && x < box.x2 && y > box.y1 && y < box.y2; g += '<polygon points="' + pts + '" fill="' + colorea(c + 9, r + 9, cen, din) + '" stroke="' + (cen ? "#1e3a8a" : "#fff") + '" stroke-width="' + (cen ? 2.5 : 1.2) + '"/>'; } g += pin(cx, cy + 8); }
    else if (u === "ageb") { const P = [[6, 6, 108, 70], [118, 6, 176, 52], [118, 62, 176, 108], [6, 80, 108, 164], [118, 118, 176, 164]]; P.forEach((q, i) => { const cen = i === 2; g += '<rect x="' + q[0] + '" y="' + q[1] + '" width="' + (q[2] - q[0] + 100) + '" height="' + (q[3] - q[1]) + '" rx="6" fill="' + colorea(i, 3, cen, i === 2 || i === 1) + '" stroke="' + (cen ? "#1e3a8a" : "#fff") + '" stroke-width="' + (cen ? 2.5 : 1.5) + '"/>'; }); g += pin(cx + 40, cy + 10); }
    else if (u === "municipio") { const P = ["M6 6 L150 6 L130 90 L6 80Z", "M154 6 L294 6 L294 100 L170 96Z", "M6 84 L128 94 L170 100 L150 164 L6 164Z", "M174 104 L294 104 L294 164 L156 164Z"]; P.forEach((d, i) => { const cen = i === 0; g += '<path d="' + d + '" fill="' + colorea(i, 5, cen, true) + '" stroke="' + (cen ? "#1e3a8a" : "#fff") + '" stroke-width="' + (cen ? 2.5 : 2) + '"/>'; }); g += pin(80, 55); }
    else { g += '<path d="M40 90 C30 50 80 20 130 26 C190 20 250 40 262 90 C270 130 220 156 160 150 C100 158 48 140 40 90Z" fill="' + ramp(.65) + '" stroke="#1e3a8a" stroke-width="2.5"/>'; }
    if (alc === "maqueta" && (u === "manzana" || u === "hexagono")) g += '<rect x="' + box.x1 + '" y="' + box.y1 + '" width="' + (box.x2 - box.x1) + '" height="' + (box.y2 - box.y1) + '" fill="none" stroke="#4f46e5" stroke-width="2" stroke-dasharray="6 4" rx="6"/><text x="' + (box.x1 + 4) + '" y="' + (box.y1 - 4) + '" font-size="9.5" font-weight="700" fill="#4f46e5">área de la maqueta</text>';
    // cómo se muestra
    if (vis === "valor") g += '<g transform="translate(196,10)"><rect width="96" height="38" rx="9" fill="#fff" stroke="#e2e8f0"/><text x="10" y="17" font-size="9" fill="#64748b" font-weight="700">INDICADOR</text><text x="10" y="32" font-size="15" font-weight="800" fill="#0f172a">72</text><circle cx="80" cy="25" r="6" fill="#16a34a"/></g>';
    if (vis === "mapa_zona") g += '<g transform="translate(196,10)"><rect width="96" height="26" rx="9" fill="#4f46e5"/><text x="48" y="17" font-size="11" font-weight="800" fill="#fff" text-anchor="middle">Valor: 72</text></g>';
    if (vis === "ranking") g += '<g transform="translate(184,8)"><rect width="108" height="62" rx="9" fill="#fff" stroke="#e2e8f0"/><text x="8" y="14" font-size="9" fill="#64748b" font-weight="700">RANKING</text>' + [88, 64, 40].map((w, i) => '<rect x="8" y="' + (20 + i * 14) + '" width="' + w + '" height="8" rx="4" fill="' + ramp(1 - i * .3) + '"/>').join("") + '</g>';
    if (coro || vis === "ranking" || vis === "valor") g += '<g transform="translate(8,152)"><rect x="-4" y="-16" width="98" height="30" rx="7" fill="#fff" opacity=".88"/><defs><linearGradient id="dlg"><stop offset="0" stop-color="' + ramp(0) + '"/><stop offset="1" stop-color="' + ramp(1) + '"/></linearGradient></defs><rect width="90" height="7" rx="3.5" fill="url(#dlg)"/><text x="0" y="-2" font-size="8.5" fill="#475569">bajo</text><text x="90" y="-2" font-size="8.5" fill="#475569" text-anchor="end">alto</text></g>';
    return '<svg viewBox="0 0 ' + W + ' ' + H + '" role="img" aria-label="Vista previa conceptual de cómo se vería el indicador" style="width:100%;height:auto;display:block;border-radius:12px;border:1px solid #e2e8f0">' + g + '</svg>';
  }

  // receta de cálculo: texto legible y estado de «calculable»
  const RV = { POBTOT: "población total", POB_0_17: "población de 0 a 17 años", POB_18_24: "población de 18 a 24 años", POB_25_59: "población de 25 a 59 años", POB_60MAS: "población de 60 años y más", VIVTOT: "viviendas", VIVHAB: "viviendas habitadas", GRAPROES: "escolaridad promedio", PROM_OCUP: "ocupantes por vivienda", PRO_OCUP_C: "ocupantes por cuarto", V3MASOCU_P: "% de viviendas con 3 o más ocupantes por cuarto", PISO_FIRME_P: "% de viviendas con piso firme", ELECTRICIDAD_P: "% de viviendas con electricidad", EXCUSADO_P: "% de viviendas con excusado", DRENAJE_P: "% de viviendas con drenaje", DISCAPACIDAD_P: "% de población con discapacidad", MAYORES60_P: "% de población de 60 años y más", NSE_Z: "puntaje socioeconómico estimado" };
  const RE = { pav: "pavimento", banq: "banqueta", guarn: "guarnición", alum: "alumbrado público", dren: "drenaje pluvial", arb: "arbolado", rampa: "rampa para silla de ruedas", paso: "paso peatonal", semaf: "semáforo peatonal", ciclo: "ciclovía", transp: "transporte colectivo", bici: "estacionamiento de bicicletas", rest: "acceso restringido", amb: "puestos ambulantes", indice: "índice de infraestructura" };
  const RG = ["comercio al por menor", "comercio al por mayor", "restaurantes y hospedaje", "servicios financieros", "servicios profesionales e inmobiliarios", "salud", "educación y esparcimiento", "industria y construcción", "transporte, información y gobierno", "otros servicios"];
  const opTxt = o => !o ? "—" : o.t === "conteo" ? "negocios del DENUE" + (o.grupos && o.grupos.length ? " (" + o.grupos.map(g => RG[g]).join(", ") + ")" : "") + (o.scian && o.scian.length ? " · SCIAN " + o.scian.join(", ") : "") : o.t === "suma" ? "suma de " + (RV[o.v] || o.v) : o.t === "promedio" ? "promedio de " + (RV[o.v] || o.v) + " (ponderado por población)" : "cobertura de " + (RE[o.e] || o.e);
  const recetaTxt = r => !r ? "" : r.tipo === "razon" ? "(" + opTxt(r.a) + ") ÷ (" + opTxt(r.b) + ")" + (r.factor && r.factor !== 1 ? " × " + r.factor : "") : opTxt(r.a);
  const calculableDe = f => !!f.receta && (f.unidad === "radio" || f.unidad === "manzana");
  const motivoNoCalc = f => !f.receta ? "la IA no encontró una receta automática para esta metodología" : "por ahora Geo Lab solo calcula con radio o manzana (la unidad elegida es «" + ((UNI[f.unidad] || [])[1] || f.unidad) + "»)";
  let CRE = [];
  const REAL = () => INDS_ALL().filter(i => !i._nuevo);
  function mezcla() {   // vuelve a armar el catálogo: Geo Lab + indicadores creados (también aparecen en «Fuente de datos»)
    CATI.forEach(c => { c.an.forEach(e => { e.ind = e.ind.filter(i => !i._nuevo); }); c.an = c.an.filter(e => !e._dl); });
    for (let k = F.length - 1; k >= 0; k--) if (F[k]._creado) F.splice(k, 1); for (let k = CATS.length - 1; k >= 0; k--) if (CATS[k].k === "creados") CATS.splice(k, 1);
    CRE.forEach(it => {
      const c = CATI.find(x => x.id === it.cat); if (!c) return; let e = c.an.find(x => x.id === it.enf);
      if (!e) { e = { id: it.enf, ico: "✨", n: it.enfNuevo || it.enf, d: "Enfoque creado en Data Lab", ind: [], _dl: true }; c.an.push(e); }
      const i = { id: it.id, n: it.n, tipo: it.tipo, d: it.d || it.objetivo, u: it.u, m: it.m, f: it.f || "—", vars: (it.vars || []).map(v => Object.assign({}, v, { tr: [] })), _nuevo: true, _meta: it }; prep(i, c, e); e.ind.push(i);
    });
    if (CRE.length) {
      CATS.push({ k: "creados", n: "Indicadores creados en Data Lab", ic: "✨", d: "Indicadores propios definidos con ayuda de la IA; se calculan con las demás fuentes" });
      CRE.forEach(it => { const f = { id: "dl-" + it.id, cat: "creados", _creado: true, n: it.n, p: "Data Lab · cálculo propio", t: "Privada", a: "Propia", m: "Interna", e: it.publicado ? "Publicado" : it.estado === "Propuesto" ? "Propuesto" : "Borrador", ob: it.objetivo, ind: [it.n],
        cob: (it.unidad ? (UNI[it.unidad] ? UNI[it.unidad][1] : it.unidad) + (it.unidad === "radio" && it.radio ? " (" + it.radio + " m)" : "") : (RES_TXT[it.res] || it.res)) + " · " + it.u, act: it.periodicidad, alm: "Cloud Storage · datalab/indicadores.json", lic: "Propio de Check Inn", url: "", nota: "Se calcula con: " + ((it.vars || []).map(v => v.n).join("; ") || "—") + (it.faltantes && it.faltantes.length ? ". Datos por conseguir: " + it.faltantes.join("; ") : "") }; f._h = hay(f); F.push(f); });
    }
    const bd = host && host.querySelectorAll(".dl-nav .bd"); if (bd && bd.length > 1) { bd[0].textContent = F.length; bd[1].textContent = INDS_ALL().length; }
  }
  async function cargaCreados() { try { const j = await api("/geometrika/datalab/indicadores"); CRE = j.items || []; mezcla(); return true; } catch (e) { return false; } }
  const swPub = m => { const ok = m.calculable, t = m.publicado ? "Publicado en Geo Lab · clic para retirarlo" : ok ? "No publicado · clic para publicarlo en Geo Lab (solo administrador)" : "No se puede publicar todavía: Geo Lab aún no sabe calcularlo"; return `<button type="button" class="dl-sw${m.publicado ? " on" : ""}${ok || m.publicado ? "" : " off"}" role="switch" aria-checked="${!!m.publicado}" data-dl-pub="${esc(m.id)}" title="${esc(t)}"><i></i></button>`; };
  async function publica(id) {
    const it = CRE.find(x => x.id === id); if (!it) return; const nuevo = !it.publicado;
    if (nuevo && !it.calculable) { toast("Todavía no se puede publicar: Geo Lab aún no sabe calcular este indicador."); return; }
    if (!nuevo && !confirm("¿Retirar «" + it.n + "» de Geo Lab?")) return;
    try { const j = await api("/geometrika/datalab/indicadores/publicar", { id, publicado: nuevo }); Object.assign(it, j.item); mezcla(); listaI(); toast(nuevo ? "Publicado en Geo Lab: aparecerá en Análisis › Indicadores generados." : "Retirado de Geo Lab.", 6000); }
    catch (e) { toast(e.message, 7000); }
  }
  function metaCreado(m) {
    const li = (t, a) => a && a.length ? `<div><span>${t}</span><ul class="dl-ul">${a.map(x => `<li>${esc(x)}</li>`).join("")}</ul></div>` : "";
    const det = (t, v) => v ? `<div><span>${t}</span><p>${esc(v)}</p></div>` : "";
    return `${det("Objetivo de negocio", m.objetivo)}${det("Resolución", RES_TXT[m.res] || m.res)}${m.unidad ? det("Se calcula sobre", (UNI[m.unidad] ? UNI[m.unidad][1] : m.unidad) + (m.unidad === "radio" && m.radio ? " de " + m.radio + " m" : "")) : ""}${m.alcance ? det("Se aplica a", ALC[m.alcance] || m.alcance) : ""}${m.visual ? det("Se muestra como", (VIS[m.visual] || m.visual) + (m.clasif ? " · " + (CLA[m.clasif] || m.clasif) : "")) : ""}${m.sentido && m.sentido !== "mayor_mejor" ? det("Sentido del valor", SEN[m.sentido]) : ""}${det("Receta de cálculo", m.receta ? recetaTxt(m.receta) : "Sin receta automática")}${det("¿Se puede publicar en Geo Lab?", m.calculable ? (m.publicado ? "Publicado" + (m.publicadoEn ? " el " + new Date(m.publicadoEn).toLocaleDateString("es-MX", { day: "numeric", month: "long", year: "numeric" }) : "") : "Sí, un administrador puede publicarlo con el interruptor") : "Todavía no: " + motivoNoCalc(m))}${m.unidad ? `<div class="wide"><span>Cómo se verá</span><div class="dl-prev">${prevSVG(m)}<p>${esc(m.comoSeVera || resumenEsp(m))}</p></div></div>` : ""}${det("Periodicidad de cálculo", m.periodicidad)}${det("Estado", m.estado)}${det("Responsable", m.responsable)}${det("Dificultad estimada", m.dificultad)}${det("Creado", new Date(m.creado).toLocaleDateString("es-MX", { day: "numeric", month: "long", year: "numeric" }))}
      ${li("Pasos de cálculo", m.pasos)}${li("Supuestos", m.supuestos)}${li("Limitaciones", m.limitaciones)}${li("Datos por conseguir", m.faltantes)}${det("Instrucciones originales", m.instrucciones)}
      <div class="wide dl-acc"><button class="dl-ghost pr" type="button" data-dl-est="${esc(m.id)}" data-v="${m.estado === "Propuesto" ? "Borrador" : "Propuesto"}">${m.estado === "Propuesto" ? "Regresar a borrador" : "Marcar como propuesto"}</button><button class="dl-ghost pr dl-danger" type="button" data-dl-del="${esc(m.id)}">Eliminar indicador</button></div>`;
  }
  async function eliminaCreado(id) { const it = CRE.find(x => x.id === id); if (!it || !confirm("¿Eliminar el indicador «" + it.n + "»? También saldrá de Fuente de datos.")) return; try { await api("/geometrika/datalab/indicadores/eliminar", { id }); CRE = CRE.filter(x => x.id !== id); S2.filas.forEach(k => { if (k.endsWith(":" + id)) S2.filas.delete(k); }); mezcla(); indicadores(); toast("Indicador eliminado."); } catch (e) { toast("No se pudo eliminar: " + e.message); } }
  async function cambiaEstado(id, estado) { try { await api("/geometrika/datalab/indicadores/estado", { id, estado }); const it = CRE.find(x => x.id === id); if (it) it.estado = estado; mezcla(); listaI(); toast("Estado actualizado: " + estado + "."); } catch (e) { toast("No se pudo actualizar: " + e.message); } }

  // catálogo para elegir (indicadores, variables y fuentes) con su metadata
  const FUENTE_DE = f => { const s = String(f || ""), ids = []; const add = (...a) => a.forEach(x => { if (!ids.includes(x)) ids.push(x); });
    if (/censo/i.test(s)) add("censo"); if (/denue/i.test(s)) add("denue"); if (/eic/i.test(s)) add("eic"); if (/inegi/i.test(s) && !ids.length) add("censo");
    if (/open-?meteo|era5/i.test(s)) add("meteo"); if (/google routes/i.test(s)) add("g-routes"); if (/google places/i.test(s)) add("g-places"); if (/solar|air quality|weather|pollen/i.test(s) && /google/i.test(s)) add("g-env"); if (/street view/i.test(s)) add("g-sv");
    if (/osm|openstreetmap|red de calles/i.test(s)) add("osm"); if (/terrarium|relieve/i.test(s)) add("terrain"); if (/ghsl/i.test(s)) add("ghsl-s", "ghsl-h"); if (/worldcover/i.test(s)) add("worldcover"); if (/\bia\b/i.test(s)) add("claude");
    return ids.map(id => F.find(x => x.id === id)).filter(Boolean); };
  const cobDe = f => { const L = FUENTE_DE(f); return L.length ? L.map(x => x.cob).slice(0, 2).join(" · ") : (/propio|usuario|supuesto|configurable|ficha|detecci|selector/i.test(f) ? "Depende del proyecto" : "—"); };
  let VCAT = null;
  const catalogoVars = () => { if (VCAT) return VCAT; const m = new Map(); REAL().forEach(i => i.vars.forEach(v => { const k = [v.n, v.u, v.f].join("|"); const x = m.get(k) || { n: v.n, u: v.u, d: v.d, f: v.f, cob: cobDe(v.f), en: [] }; x.en.push(i.n); m.set(k, x); })); return VCAT = [...m.values()]; };
  const W = { abierto: false, paso: 1, res: "", texto: "", sel: new Map(), tab: "ind", q: "", props: null, ent: "", adv: [], prop: null, f: {}, cargando: false, error: "", guardando: false };
  const EJEMPLOS = ["Qué tan caminable es la zona: cuántos servicios básicos hay a 10 minutos a pie de un punto.", "Índice de atractivo turístico de una zona combinando atracciones, hospedaje existente y accesibilidad.", "Potencial de demanda de renta a mediano plazo por zona según población joven, universidades y empleo."];
  const clave = (t, n) => t + "|" + n;
  const items = () => {
    const q = norm(W.q), ok = h => !q || q.split(/\s+/).every(w => norm(h).includes(w));
    if (W.tab === "ind") return REAL().filter(i => ok([i.n, i.d, i.u, i._e.n, i._c.n, i._fam.join(" ")].join(" "))).map(i => ({ t: "Indicador", n: i.n, u: i.u, f: i._fam.join(", "), cob: cobDe(i.vars.map(v => v.f).join(" ")), sub: i._e.n + " · " + i.tipo, d: i.d }));
    if (W.tab === "var") return catalogoVars().filter(v => ok([v.n, v.d, v.f, v.u, v.en.join(" ")].join(" "))).map(v => ({ t: "Variable", n: v.n, u: v.u, f: v.f, cob: v.cob, sub: "Usada en: " + [...new Set(v.en)].slice(0, 2).join(", ") + (new Set(v.en).size > 2 ? "…" : ""), d: v.d }));
    return F.filter(f => !f._creado && ok([f.n, f.p, f.ob, f.cob, f.t, f.a, (f.ind || []).join(" ")].join(" "))).map(f => ({ t: "Fuente", n: f.n, u: "", f: f.p + " · " + f.t + " · " + f.a, cob: f.cob, sub: f.m + " · " + f.e, d: f.ob }));
  };
  function pintaDr() {
    const dr = $("#dlDr", host); if (!dr) return; dr.classList.toggle("on", W.abierto); $("#dlOv", host).classList.toggle("on", W.abierto); dr.setAttribute("aria-hidden", W.abierto ? "false" : "true"); if (!W.abierto) return;
    const pasos = ["Describir", "Metodologías", "Ficha"], st = pasos.map((t, k) => `<li class="${W.paso === k + 1 ? "on" : W.paso > k + 1 ? "ok" : ""}"><i>${W.paso > k + 1 ? "✓" : k + 1}</i><span>${t}</span></li>`).join("");
    const cuerpo = W.paso === 1 ? paso1() : W.paso === 2 ? paso2() : paso3();
    dr.innerHTML = `<header class="dl-dh"><div><b>Crear indicador</b><span>Descríbelo con tus palabras y la IA propone cómo calcularlo</span></div><button class="dl-x" type="button" data-dr="cerrar" aria-label="Cerrar">✕</button></header><ol class="dl-st">${st}</ol><div class="dl-db" id="dlDb">${cuerpo.body}</div><footer class="dl-df">${cuerpo.foot}</footer>`;
  }
  function paso1() {
    const rs = [["punto", "📍", "Punto / coordenada", "Una cifra para un lugar exacto, por ejemplo una propiedad"], ["zona", "🗺️", "Zona", "Una cifra por área: radio alrededor de un punto, manzanas, hexágonos, AGEB… (lo defines en la ficha)"], ["ciudad", "🏙️", "Ciudad", "Una sola cifra para toda la ciudad o municipio"]];
    const lst = items(), nSel = W.sel.size, tabs = [["ind", "Indicadores", REAL().length], ["var", "Variables", catalogoVars().length], ["fte", "Fuentes de datos", F.filter(f => !f._creado).length]];
    const ready = W.res && W.texto.trim().length >= 12;
    return { body: `
      <section class="dl-sec"><h3>1 · Nivel de resolución <em>obligatorio</em></h3><div class="dl-rs3">${rs.map(([k, ic, t, d]) => `<button type="button" class="dl-rc${W.res === k ? " on" : ""}" data-res="${k}" role="radio" aria-checked="${W.res === k}"><span class="ic">${ic}</span><b>${t}</b><small>${d}</small></button>`).join("")}</div></section>
      <section class="dl-sec"><h3>2 · ¿Qué quieres medir? <em>obligatorio</em></h3><textarea id="dlTx" rows="5" maxlength="3000" placeholder="Escribe con texto corriente, por ejemplo: «Quiero saber qué tan bien conectada está una propiedad con servicios y transporte, para decidir si conviene rentarla a viajeros de negocios»">${esc(W.texto)}</textarea>
        <div class="dl-tc"><span id="dlTn">${W.texto.length} / 3000</span></div><div class="dl-ej"><small>Ejemplos (toca para usar):</small>${EJEMPLOS.map((e, k) => `<button type="button" class="dl-c" data-ej="${k}">${esc(e.slice(0, 62))}…</button>`).join("")}</div></section>
      <section class="dl-sec"><h3>3 · Datos disponibles <em>opcional · elige los que quieras que use la IA</em></h3>
        <div class="dl-tabs" role="tablist">${tabs.map(([k, t, n]) => `<button type="button" role="tab" class="dl-tab${W.tab === k ? " on" : ""}" data-tab="${k}">${t} <i>${n}</i></button>`).join("")}</div>
        <div class="dl-q dl-q2">${ICON.lupa}<input id="dlCq" type="search" placeholder="Buscar en el catálogo…" value="${esc(W.q)}" autocomplete="off"></div>
        ${nSel ? `<div class="dl-selr">${[...W.sel.values()].map(x => `<span class="dl-tag">${esc(x.n)}<button type="button" data-unsel="${esc(clave(x.t, x.n))}" aria-label="Quitar">✕</button></span>`).join("")}</div>` : ""}
        <div class="dl-cat" id="dlCat">${lst.length ? lst.slice(0, 120).map(x => pick(x)).join("") : `<div class="dl-mut" style="padding:16px;text-align:center">Sin resultados.</div>`}${lst.length > 120 ? `<div class="dl-mut" style="padding:8px;text-align:center">Mostrando 120 de ${lst.length}: afina la búsqueda.</div>` : ""}</div></section>`,
      foot: `<span class="dl-mut">${nSel ? nSel + " elemento" + (nSel > 1 ? "s" : "") + " seleccionado" + (nSel > 1 ? "s" : "") : "Sin elementos seleccionados"}</span><button class="dl-new" type="button" data-dr="proponer"${ready ? "" : " disabled"}>Proponer metodologías →</button>` };
  }
  const pick = x => { const on = W.sel.has(clave(x.t, x.n)); return `<div class="dl-pk${on ? " on" : ""}" role="checkbox" aria-checked="${on}" tabindex="0" data-pk="${esc(clave(x.t, x.n))}"><span class="dl-ck">${on ? "✓" : ""}</span><div class="pb"><b>${esc(x.n)}</b><small>${esc(x.sub)}</small><div class="mt">${x.u ? `<span><u>Unidades</u> ${esc(x.u)}</span>` : ""}<span><u>Fuente</u> ${esc(x.f || "—")}</span><span><u>Cobertura</u> ${esc(x.cob || "—")}</span></div></div></div>`; };
  function paso2() {
    if (W.cargando) return { body: `<div class="dl-load"><div class="sp"></div><b>La IA está analizando tu indicador…</b><p>Revisa el catálogo disponible y arma entre 2 y 3 metodologías. Suele tardar de 20 a 60 segundos.</p></div>`, foot: `<span class="dl-mut">Espera un momento…</span>` };
    if (W.error) return { body: `<div class="dl-err"><b>No se pudieron generar las propuestas</b><p>${esc(W.error)}</p></div>`, foot: `<button class="dl-ghost pr" type="button" data-dr="atras">← Cambiar instrucciones</button><button class="dl-new" type="button" data-dr="proponer">Reintentar</button>` };
    const P = W.props || [];
    const li = (t, a) => a && a.length ? `<div class="dl-pd"><span>${t}</span><ul class="dl-ul">${a.map(x => `<li>${esc(x)}</li>`).join("")}</ul></div>` : "";
    return { body: `${W.ent ? `<div class="dl-ent"><span>Lo que entendí</span><p>${esc(W.ent)}</p>${W.adv.length ? `<ul class="dl-ul">${W.adv.map(a => `<li>${esc(a)}</li>`).join("")}</ul>` : ""}</div>` : ""}
      <div class="dl-props">${P.map(p => `<article class="dl-pp${W.prop === p.id ? " on" : ""}" data-pp="${p.id}"><button type="button" class="dl-ph" data-prop="${p.id}"><span class="dl-rd">${W.prop === p.id ? "●" : ""}</span><span class="t"><b>${esc(p.titulo)}</b><small>${esc(p.resumen)}</small></span><span class="bs">${p.recomendada ? `<span class="dl-b Activa">Recomendada</span>` : ""}${calculableDe(p) ? `<span class="dl-b Publicado">Calculable en Geo Lab</span>` : `<span class="dl-b Pendiente">Solo definición</span>`}<span class="dl-b ${p.dificultad === "Alta" ? "Privada" : p.dificultad === "Baja" ? "Gratuita" : "Parcial"}">Dificultad ${esc(p.dificultad)}</span></span></button>
        <details class="dl-pdt"${W.prop === p.id ? " open" : ""}><summary>Ver detalle de la metodología</summary>
          <div class="dl-pd"><span>Indicador propuesto</span><p><b>${esc(p.nombre)}</b> · ${esc(p.unidades)} · ${esc(p.tipo)} · ${esc(p.periodicidad)}</p></div>${p.descripcion ? `<div class="dl-pd"><span>Qué mide</span><p>${esc(p.descripcion)}</p></div>` : ""}
          ${li("Pasos de cálculo", p.pasos)}${p.formula ? `<div class="dl-pd"><span>Fórmula</span><code class="dl-code">${esc(p.formula)}</code></div>` : ""}
          ${p.variables.length ? `<div class="dl-pd"><span>Variables que usa</span>${p.variables.map(v => `<div class="dl-pv2"><b>${esc(v.n)}</b>${v.en_catalogo ? `<span class="dl-b Activa">En el catálogo</span>` : `<span class="dl-b Parcial">Nueva</span>`}${v.disponible ? "" : `<span class="dl-b Privada">Dato por conseguir</span>`}<small>${[v.unidades && "Unidades: " + v.unidades, v.fuente && "Fuente: " + v.fuente, v.cobertura && "Cobertura: " + v.cobertura, v.nota].filter(Boolean).map(esc).join(" · ")}</small></div>`).join("")}</div>` : ""}
          ${li("Supuestos", p.supuestos)}${li("Limitaciones", p.limitaciones)}${li("Datos por conseguir", p.faltantes)}</details></article>`).join("")}</div>`,
      foot: `<button class="dl-ghost pr" type="button" data-dr="atras">← Cambiar instrucciones</button><button class="dl-new" type="button" data-dr="elegir"${W.prop ? "" : " disabled"}>Usar esta metodología →</button>` };
  }
  function paso3() {
    const f = W.f, cats = CATI.filter(c => !c._dl), enfs = (CATI.find(c => c.id === f.cat) || { an: [] }).an.filter(e => !e._dl);
    const opt = (L, v) => L.map(o => { const a = Array.isArray(o) ? o : [o, o]; return `<option value="${esc(a[0])}"${a[0] === v ? " selected" : ""}>${esc(a[1])}</option>`; }).join("");
    const fld = (id, t, inner, ayuda) => `<label class="dl-fld"><span>${t}</span>${inner}${ayuda ? `<small>${ayuda}</small>` : ""}</label>`;
    return { body: `<div class="dl-fm">
      <div class="dl-sec"><h3>Identificación</h3>
        ${fld("n", "Nombre del indicador *", `<input data-ff="n" type="text" maxlength="120" value="${esc(f.n)}" placeholder="Ej. Índice de caminabilidad a servicios">`)}
        ${fld("d", "Qué mide", `<textarea data-ff="d" rows="2" maxlength="600">${esc(f.d)}</textarea>`)}
        <div class="dl-g2">${fld("u", "Unidades *", `<input data-ff="u" type="text" maxlength="60" value="${esc(f.u)}" placeholder="Ej. puntos 0-100, %, min">`)}${fld("tipo", "Tipo de dato", `<select data-ff="tipo">${opt(TIPOS, f.tipo)}</select>`)}</div></div>
      <div class="dl-sec"><h3>Dónde y cómo se calcula <em>para imaginar el resultado</em></h3>
        <div class="dl-g2">${fld("res", "Resolución / representación *", `<select data-ff="res">${opt([["punto", "Punto / coordenada"], ["zona", "Zona"], ["ciudad", "Ciudad"]], f.res)}</select>`)}${fld("unidad", "Se calcula sobre *", `<select data-ff="unidad">${opt((UNI_RES[f.res] || []).map(k => [k, UNI[k][0] + " " + UNI[k][1]]), f.unidad)}</select>`, (UNI[f.unidad] || [])[2])}</div>
        ${f.unidad === "radio" ? fld("radio", "Radio (metros) *", `<input data-ff="radio" type="number" min="50" max="5000" step="50" value="${esc(f.radio || 500)}">`, "Distancia desde el punto: 500 m ≈ 6 minutos caminando") : ""}
        <div class="dl-g2">${fld("alcance", "Se aplica a *", `<select data-ff="alcance">${opt(Object.keys(ALC).map(k => [k, ALC[k]]), f.alcance)}</select>`)}${fld("visual", "Se muestra como *", `<select data-ff="visual">${opt(Object.keys(VIS).map(k => [k, VIS[k]]), f.visual)}</select>`)}</div>
        <div class="dl-g2">${fld("clasif", "Colores / clasificación", `<select data-ff="clasif">${opt(Object.keys(CLA).map(k => [k, CLA[k]]), f.clasif)}</select>`)}${fld("sentido", "Sentido del valor", `<select data-ff="sentido">${opt(Object.keys(SEN).map(k => [k, SEN[k]]), f.sentido)}</select>`)}</div>
        ${f.clasif === "semaforo" ? `<div class="dl-g2">${fld("umbralVerde", "Verde si es " + (f.sentido === "menor_mejor" ? "menor o igual a" : "mayor o igual a"), `<input data-ff="umbralVerde" type="number" step="any" value="${f.umbralVerde == null ? "" : esc(f.umbralVerde)}">`)}${fld("umbralAmbar", "Ámbar si es " + (f.sentido === "menor_mejor" ? "menor o igual a" : "mayor o igual a"), `<input data-ff="umbralAmbar" type="number" step="any" value="${f.umbralAmbar == null ? "" : esc(f.umbralAmbar)}">`, "Lo demás será rojo")}</div>` : ""}
        <div class="dl-prev">${prevSVG(f)}<p><b>Cómo se verá:</b> ${esc(f.comoSeVera || resumenEsp(f))}</p><p class="dl-mut">${esc(resumenEsp(f))}</p><p class="dl-mut">Vista previa conceptual: los colores y valores son de ejemplo.</p></div>
        <div class="${calculableDe(f) ? "dl-ok" : "dl-warn"}"><b>${calculableDe(f) ? "✓ Geo Lab puede calcularlo automáticamente" : "Solo se guardará como definición"}</b><p>${calculableDe(f) ? "Receta: " + esc(recetaTxt(f.receta)) + ". Un administrador podrá publicarlo en Geo Lab." : "No se podrá publicar todavía: " + esc(motivoNoCalc(f)) + "."}</p></div></div>
      <div class="dl-sec"><h3>Cálculo y negocio</h3>
        ${fld("periodicidad", "Periodicidad de cálculo *", `<select data-ff="periodicidad">${opt(PERIODOS, f.periodicidad)}</select>`)}
        ${fld("objetivo", "Objetivo de negocio al que está alineado *", `<textarea data-ff="objetivo" rows="2" maxlength="500" placeholder="¿Qué decisión ayuda a tomar?">${esc(f.objetivo)}</textarea>`, "Ej.: elegir ubicaciones · fijar precios · medir riesgo · demanda y marketing · operación")}
        ${fld("m", "Metodología *", `<textarea data-ff="m" rows="7" maxlength="3000">${esc(f.m)}</textarea>`, "Puedes ajustar el texto que propuso la IA.")}
        ${fld("f", "Fórmula", `<input data-ff="f" type="text" maxlength="700" value="${esc(f.f)}">`)}</div>
      <div class="dl-sec"><h3>Dónde aparecerá en el catálogo</h3>
        <div class="dl-g2">${fld("cat", "Categoría *", `<select data-ff="cat">${opt(cats.map(c => [c.id, c.ico + " " + c.n]), f.cat)}</select>`)}${fld("enf", "Enfoque *", `<select data-ff="enf">${opt([...enfs.map(e => [e.id, e.ico + " " + e.n]), ["__nuevo", "＋ Crear un enfoque nuevo"]], f.enf)}</select>`)}</div>
        ${f.enf === "__nuevo" ? fld("enfNuevo", "Nombre del enfoque nuevo *", `<input data-ff="enfNuevo" type="text" maxlength="80" value="${esc(f.enfNuevo || "")}" placeholder="Ej. Conectividad urbana">`) : ""}
        <div class="dl-g2">${fld("estado", "Estado inicial", `<select data-ff="estado">${opt(["Borrador", "Propuesto"], f.estado)}</select>`, "Borrador: aún en revisión · Propuesto: listo para aprobarse")}${fld("responsable", "Responsable (opcional)", `<input data-ff="responsable" type="text" maxlength="80" value="${esc(f.responsable || "")}">`)}</div></div>
      <div class="dl-sec"><h3>Variables que usa (${(f.vars || []).length})</h3>${(f.vars || []).length ? (f.vars || []).map((v, k) => `<div class="dl-pv2"><b>${esc(v.n)}</b>${v.en_catalogo ? `<span class="dl-b Activa">En el catálogo</span>` : `<span class="dl-b Parcial">Nueva</span>`}${v.disponible === false ? `<span class="dl-b Privada">Dato por conseguir</span>` : ""}<button type="button" class="dl-x s" data-qv="${k}" aria-label="Quitar variable">✕</button><small>${[v.u && "Unidades: " + v.u, v.f && "Fuente: " + v.f, v.cob && "Cobertura: " + v.cob, v.nota].filter(Boolean).map(esc).join(" · ")}</small></div>`).join("") : `<div class="dl-mut">Sin variables.</div>`}</div>
      ${W.error ? `<div class="dl-err"><p>${esc(W.error)}</p></div>` : ""}</div>`,
      foot: `<button class="dl-ghost pr" type="button" data-dr="atras2">← Volver a metodologías</button><button class="dl-new" type="button" data-dr="guardar"${W.guardando ? " disabled" : ""}>${W.guardando ? "Guardando…" : "Guardar indicador"}</button>` };
  }
  function abreCrear() { W.abierto = true; pintaDr(); setTimeout(() => { const t = $("#dlTx", host); if (t && !W.texto) t.focus(); }, 280); }
  function cierraCrear() { if (W.cargando) return; W.abierto = false; pintaDr(); }
  async function proponer() {
    W.paso = 2; W.cargando = true; W.error = ""; W.props = null; W.prop = null; pintaDr();
    try {
      const j = await api("/geometrika/datalab/indicadores/proponer", { texto: W.texto, resolucion: W.res, seleccion: [...W.sel.values()].map(x => ({ tipo: x.t, n: x.n, u: x.u, f: x.f, cob: x.cob })), catalogo: { fuentes: F.filter(f => !f._creado).map(f => ({ n: f.n, p: f.p, t: f.t, a: f.a, cob: f.cob, act: f.act })), variables: catalogoVars().map(v => ({ n: v.n, u: v.u, f: v.f, cob: v.cob })), indicadores: INDS_ALL().map(i => ({ n: i.n, u: i.u })) } });
      W.props = j.propuestas; W.ent = j.entendimiento; W.adv = j.advertencias || []; const r = W.props.find(p => p.recomendada); W.prop = r ? r.id : null;
    } catch (e) { W.error = e.message; }
    W.cargando = false; pintaDr();
  }
  function elegir() {
    const p = (W.props || []).find(x => x.id === W.prop); if (!p) return; const cats = CATI.filter(c => !c._dl);
    W.f = { n: p.nombre, d: p.descripcion, u: p.unidades, tipo: p.tipo, res: W.res, periodicidad: p.periodicidad, objetivo: p.objetivo, m: [p.resumen, ...p.pasos.map((x, k) => (k + 1) + ". " + x)].filter(Boolean).join("\n"), f: p.formula, cat: cats[0].id, enf: cats[0].an.filter(e => !e._dl)[0].id, enfNuevo: "", estado: "Borrador", responsable: "", unidad: (UNI_RES[W.res] || []).includes(p.unidad) ? p.unidad : UNI_DEF[W.res], radio: p.radio || 500, alcance: p.alcance || "proyecto", visual: p.visual || "valor", clasif: p.clasif || "cuartiles", sentido: p.sentido || "mayor_mejor", umbralVerde: p.umbralVerde, umbralAmbar: p.umbralAmbar, comoSeVera: p.comoSeVera || "", receta: p.receta || null,
      vars: p.variables.map(v => ({ n: v.n, u: v.unidades, f: v.fuente, cob: v.cobertura, d: "", disponible: v.disponible, nota: v.nota, en_catalogo: v.en_catalogo })), pasos: p.pasos, supuestos: p.supuestos, limitaciones: p.limitaciones, faltantes: p.faltantes, dificultad: p.dificultad, titulo: p.titulo };
    W.error = ""; W.paso = 3; pintaDr(); $("#dlDb", host).scrollTop = 0;
  }
  async function guardar() {
    const f = W.f; W.error = ""; W.guardando = true; pintaDr();
    try {
      const j = await api("/geometrika/datalab/indicadores", { n: f.n, d: f.d, u: f.u, tipo: f.tipo, res: f.res, periodicidad: f.periodicidad, objetivo: f.objetivo, m: f.m, f: f.f, cat: f.cat, enf: f.enf, enfNuevo: f.enfNuevo, unidad: f.unidad, radio: f.radio, alcance: f.alcance, visual: f.visual, clasif: f.clasif, sentido: f.sentido, umbralVerde: f.umbralVerde, umbralAmbar: f.umbralAmbar, comoSeVera: f.comoSeVera, receta: f.receta, estado: f.estado, responsable: f.responsable, vars: f.vars, pasos: f.pasos, supuestos: f.supuestos, limitaciones: f.limitaciones, faltantes: f.faltantes, dificultad: f.dificultad, instrucciones: W.texto, metodologiaTitulo: f.titulo });
      CRE.push(j.item); mezcla(); W.guardando = false; Object.assign(W, { abierto: false, paso: 1, res: "", texto: "", sel: new Map(), q: "", props: null, prop: null, f: {}, error: "" }); pintaDr();
      if (sec !== "indicadores") ir("indicadores"); S2.q = S2.cat = S2.enf = S2.tipo = S2.fam = ""; S2.org = ""; const c = CATI.find(x => x.id === j.item.cat); S2.aC.add(j.item.cat); S2.aE.add(j.item.enf); S2.filas.add(j.item.enf + ":" + j.item.id); indicadores();
      setTimeout(() => { const r = host.querySelector(`tr[data-ir="${j.item.enf}:${j.item.id}"]`); if (r) r.scrollIntoView({ behavior: "smooth", block: "center" }); }, 150);
      toast("Indicador creado: " + j.item.n + ". Ya aparece en Indicadores y en Fuente de datos.", 6000);
    } catch (e) { W.guardando = false; W.error = e.message; pintaDr(); const b = $("#dlDb", host); if (b) b.scrollTop = b.scrollHeight; }
  }
  function eventosDr(host0) {
    const dr = $("#dlDr", host0);
    host0.addEventListener("click", e => { if (e.target.closest("#dlNuevo")) { abreCrear(); return; } if (e.target.id === "dlOv") cierraCrear(); });
    dr.addEventListener("click", e => {
      const t = e.target, a = t.closest("[data-dr]"); if (a && !a.disabled) { const k = a.dataset.dr; if (k === "cerrar") cierraCrear(); else if (k === "proponer") proponer(); else if (k === "atras") { W.paso = 1; W.error = ""; pintaDr(); } else if (k === "elegir") elegir(); else if (k === "atras2") { W.paso = 2; W.error = ""; pintaDr(); } else if (k === "guardar") guardar(); return; }
      const rs = t.closest("[data-res]"); if (rs) { W.res = rs.dataset.res; pintaDr(); return; }
      const ej = t.closest("[data-ej]"); if (ej) { W.texto = EJEMPLOS[+ej.dataset.ej]; pintaDr(); return; }
      const tb = t.closest("[data-tab]"); if (tb) { W.tab = tb.dataset.tab; W.q = ""; pintaDr(); return; }
      const pk = t.closest("[data-pk]"); if (pk) { const k = pk.dataset.pk; if (W.sel.has(k)) W.sel.delete(k); else { const [tp, ...n] = k.split("|"), nm = n.join("|"), x = items().find(i => i.t === tp && i.n === nm); if (x) W.sel.set(k, x); } const y = $("#dlDb", host0).scrollTop, cy = $("#dlCat", host0) ? $("#dlCat", host0).scrollTop : 0; pintaDr(); $("#dlDb", host0).scrollTop = y; if ($("#dlCat", host0)) $("#dlCat", host0).scrollTop = cy; return; }
      const us = t.closest("[data-unsel]"); if (us) { W.sel.delete(us.dataset.unsel); pintaDr(); return; }
      const pr = t.closest("[data-prop]"); if (pr) { W.prop = pr.dataset.prop; pintaDr(); return; }
      const qv = t.closest("[data-qv]"); if (qv) { W.f.vars.splice(+qv.dataset.qv, 1); pintaDr(); return; }
    });
    dr.addEventListener("input", e => {
      if (e.target.id === "dlTx") { W.texto = e.target.value; const n = $("#dlTn", host0); if (n) n.textContent = W.texto.length + " / 3000"; const b = $('[data-dr="proponer"]', host0); if (b) b.disabled = !(W.res && W.texto.trim().length >= 12); return; }
      if (e.target.id === "dlCq") { W.q = e.target.value; clearTimeout(eventosDr.t); eventosDr.t = setTimeout(() => { const c = $("#dlCat", host0); if (c) { const lst = items(); c.innerHTML = lst.length ? lst.slice(0, 120).map(x => pick(x)).join("") : `<div class="dl-mut" style="padding:16px;text-align:center">Sin resultados.</div>`; } }, 120); return; }
      const f = e.target.dataset && e.target.dataset.ff; if (f) W.f[f] = e.target.value;
    });
    dr.addEventListener("change", e => { const f = e.target.dataset && e.target.dataset.ff; if (!f) return; W.f[f] = e.target.value; if (f === "cat") { const c = CATI.find(x => x.id === e.target.value); W.f.enf = c.an.filter(x => !x._dl)[0].id; pintaDr(); } else if (f === "enf") pintaDr(); else if (f === "res") { if (!(UNI_RES[W.f.res] || []).includes(W.f.unidad)) W.f.unidad = UNI_DEF[W.f.res]; if (W.f.unidad !== "radio" && W.f.alcance !== "proyecto" && (W.f.unidad === "punto")) W.f.alcance = "proyecto"; W.f.comoSeVera = ""; pintaDr(); } else if (["unidad", "alcance", "visual", "clasif", "sentido"].includes(f)) { if (f !== "clasif" && f !== "sentido") W.f.comoSeVera = ""; if (f === "unidad" && (W.f.unidad === "punto" || W.f.unidad === "radio") && W.f.alcance === "ciudad_completa") W.f.alcance = "proyecto"; pintaDr(); } });
    dr.addEventListener("keydown", e => { const pk = e.target.closest && e.target.closest("[data-pk]"); if (pk && (e.key === " " || e.key === "Enter")) { e.preventDefault(); pk.click(); } });
    document.addEventListener("keydown", e => { if (e.key === "Escape" && W.abierto) cierraCrear(); });
  }
  let sec = "fuentes";
  function ir(s) {
    if (s !== "fuentes" && s !== "indicadores") s = "fuentes"; sec = s; host.querySelectorAll(".dl-nav").forEach(n => n.classList.toggle("on", n.dataset.sec === s));
    try { history.replaceState(null, "", "#" + s); } catch (_) { } $("#dlMain", host).scrollTop = 0; if (s === "indicadores") indicadores(); else fuentes();
  }

  window.datalabInit = function () {
    host = document.getElementById("datalab-host"); if (!host || host.dataset.listo) return; host.dataset.listo = "1";
    shell(); eventosDr(host); ir(location.hash === "#indicadores" ? "indicadores" : "fuentes");
    cargaCreados().then(ok => { if (ok && CRE.length) { if (sec === "indicadores") indicadores(); else fuentes(); } });
  };
})();
