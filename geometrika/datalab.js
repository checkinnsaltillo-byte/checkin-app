/* Data Lab · pantalla principal: menú lateral desplegable + sección «Fuente de datos» (resumen, filtros y tablas que se abren y se cierran).
   Necesita datalab-fuentes.js (window.DL_FUENTES, DL_CATS, DL_FECHA) y datalab.css. Se inicia con window.datalabInit(). */
(function () {
  "use strict";
  const EMB = (() => { try { return new URLSearchParams(location.search).get("emb") === "1" && window.parent !== window; } catch (_) { return false; } })();
  const glTabs = A => EMB ? `<div class="gl-tabs" id="glTabs" role="tablist" aria-label="Sistemas"><button type="button" data-v="geo" role="tab" title="GeoLab"><img src="geometrika-logo.png" alt=""><b>GeoLab</b></button><button type="button" data-v="eco" role="tab"${A === "eco" ? ' class="on"' : ""} title="EconoLab"><img src="econolab-logo.png" alt=""><b>EconoLab</b></button><button type="button" data-v="data" role="tab"${A === "data" ? ' class="on"' : ""} title="DataLab"><img src="datalab-logo.png" alt=""><b>DataLab</b></button></div>` : "";
  const dNotif = o => { try { parent.postMessage({ gl: "win", open: o }, location.origin); } catch (_) { } };
  const dlWin = (o, t) => { if (!EMB) return; const a = document.getElementById("dlApp"); if (!a) return; a.classList.toggle("dl-open", o); if (t) { const w = document.getElementById("dlWT"); if (w) w.textContent = t; } dNotif(o); };
  const glIr = e => { const gt = e.target.closest("#glTabs button, #glBack"); if (!gt) return false; try { parent.postMessage({ gl: "tab", v: gt.dataset.v || "geo" }, location.origin); } catch (_) { } return true; };
  const dlTemaPinta = () => { const b = document.getElementById("dlTema"), o = document.documentElement.classList.contains("dl-dark"); if (b) { b.textContent = o ? "☀️ Claro" : "🌙 Oscuro"; b.title = (o ? "Cambiar a tema claro" : "Cambiar a tema oscuro") + " (aplica a GeoLab, EconoLab y DataLab)"; } };
  if (EMB) addEventListener("message", e => { if (e.origin === location.origin && e.data && e.data.gl === "tema") { document.documentElement.classList.toggle("dl-dark", e.data.v === "osc"); dlTemaPinta(); } });
  if (EMB) document.addEventListener("click", e => { const b = e.target.closest && e.target.closest("#dlTema"); if (b) { try { parent.postMessage({ gl: "tema", v: document.documentElement.classList.contains("dl-dark") ? "claro" : "osc" }, location.origin); } catch (_) { } } });
  if (EMB) addEventListener("message", e => { if (e.origin === location.origin && e.data && e.data.gl === "col") { document.documentElement.style.setProperty("--col", e.data.w + "px"); if (e.data.h != null) document.documentElement.style.setProperty("--tth", e.data.h + "px"); } });
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
        ${EMB ? '<button class="gl-back" id="glBack" type="button" title="Volver al menú de GeoLab">← Menú</button><img class="mt-logo" src="metrika-logo.png" alt="Metrika Business Intelligence">' : ""}<span class="dl-chip">v.Beta.1</span>
        <a class="dl-btn dl-go" id="dlIrGeo" href="index.html?s=geo" target="_blank" rel="noopener" title="Abrir Geo Lab en una ventana nueva"><img src="geometrika-logo-claro.png" alt=""><span class="lg">Ir a Geo Lab</span><span class="sh">Geo Lab</span></a>
        <button class="dl-btn" id="dlSalir" type="button"><span class="lg">Cerrar sesión</span><span class="sh">Salir</span></button>
      </header>
      <div class="dl-body">${EMB ? '<div class="dl-wbar"><b id="dlWT">Data Lab</b><button type="button" class="dl-wx" id="dlWX" aria-label="Cerrar ventana" title="Cerrar">✕</button></div>' : ""}
        <aside class="dl-side" id="dlSide" aria-label="Menú de Data Lab">
          ${glTabs("data")}${EMB ? '<div class="dl-tbar"><button type="button" class="dl-tbtn" id="dlTema"></button></div>' : ""}<div class="dl-side-t">Menú</div>${EMB ? `<button class="dl-nav dl-nav-ind" type="button" data-sec="__ind" title="Indicadores del proyecto de GeoLab"><span class="ic">📋</span><span class="tx">Indicadores del proyecto</span></button><button class="dl-nav dl-nav-ind" type="button" data-sec="__pond" title="Evaluación del proyecto: índice global y peso de cada indicador según el objetivo"><span class="ic">⚖️</span><span class="tx">Evaluación del proyecto</span></button>` : ""}
          <button class="dl-nav on" type="button" data-sec="fuentes" title="Fuente de datos"><span class="ic">🗄️</span><span class="tx">Fuente de datos</span><span class="bd">${tot}</span></button>
          <button class="dl-nav" type="button" data-sec="indicadores" title="Indicadores"><span class="ic">📊</span><span class="tx">Indicadores</span><span class="bd">${INDS_ALL().length}</span></button>
          <div class="dl-side-f">Data Lab · Check Inn</div>
        </aside>
        <main class="dl-main" id="dlMain"><div class="dl-wrap" id="dlWrap"></div></main>
      </div><div class="dl-ov" id="dlOv"></div><aside class="dl-dr" id="dlDr" aria-hidden="true" aria-label="Crear indicador"></aside></div>`;
    const app = $("#dlApp", host); if (EMB) { app.classList.add("dl-emb"); host.addEventListener("click", glIr); }
    if (!EMB && (innerWidth < 900 || ls.get("dl_side", "on") === "off")) app.classList.add("dl-off");   // en celular siempre inicia cerrado
    $("#dlBurger", host).onclick = () => { app.classList.toggle("dl-off"); if (innerWidth >= 900) ls.set("dl_side", app.classList.contains("dl-off") ? "off" : "on"); };
    $("#dlSalir", host).onclick = () => window.DL_SALIR && window.DL_SALIR();
    
    $("#dlMain", host).addEventListener("scroll", () => { const f = $(".dl-fl", host); if (f) f.classList.toggle("st", $("#dlMain", host).scrollTop > 140); }, { passive: true });
    host.addEventListener("click", clic); host.addEventListener("input", entrada); host.addEventListener("change", cambia);
    // en pantallas angostas el menú es un cajón: se cierra al elegir una sección
    $("#dlSide", host).addEventListener("click", e => { const n = e.target.closest(".dl-nav"); if (!n) return; if (n.dataset.sec === "__ind") { try { parent.postMessage({ gl: "ind" }, location.origin); } catch (_) { } return; } if (n.dataset.sec === "__pond") { try { parent.postMessage({ gl: "pond" }, location.origin); } catch (_) { } return; } ir(n.dataset.sec); if (EMB) dlWin(true, n.title || "Data Lab"); else if (innerWidth < 900) app.classList.add("dl-off"); });
    if (EMB) { $("#dlWX", host).onclick = () => dlWin(false); document.addEventListener("keydown", ev => { if (ev.key === "Escape") dlWin(false); }); }
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
    if (/bie\/bise|api bie/i.test(s)) add("INEGI · BIE/BISE"); else if (/inegi|censo|denue|eic/i.test(s)) add("INEGI"); if (/banxico|banco de méxico/i.test(s)) add("Banco de México"); if (/open-?meteo|era5/i.test(s)) add("Open-Meteo"); if (/google/i.test(s)) add("Google");
    if (/osm|openstreetmap|red de calles/i.test(s)) add("OpenStreetMap"); if (/terrarium|relieve/i.test(s)) add("Terrain Tiles"); if (/ghsl/i.test(s)) add("GHSL (JRC)");
    if (/worldcover/i.test(s)) add("ESA WorldCover"); if (/\bia\b/i.test(s)) add("Inteligencia artificial");
    if (/propio|propia|supuesto|usuario|configurable|ficha|detecci|selector/i.test(s)) add("Cálculo propio o del usuario"); if (/pendiente/i.test(s)) add("Pendiente");
    if (!o.length) add("Otra"); return o; };
  const TCLS = { "Dato duro": "dd", "Dato calculado": "dc", "Índice": "ix", "Valoración cualitativa": "vl", "Valoración cuantitativa": "vl", "Dato inferido": "di" };
  const prep = (i, c, e) => {
    i._c = c; i._e = e; i._inegi = i.x || null; const fam = []; (i.vars.length ? i.vars.map(v => v.f) : [i.f]).forEach(f => FAM(f).forEach(x => { if (!fam.includes(x)) fam.push(x); })); i._fam = i.s ? ["INEGI · BIE/BISE"] : c.id === "med" ? ["MediaLab (noticias y redes)"] : fam;   // EconoLab y MediaLab no traen variables: su familia se sabe por su origen
    i._h = norm([i.n, i.d, i.u, i.m, i.f, i.tipo, e.n, c.n, i._meta ? [i._meta.objetivo, i._meta.res, i._meta.periodicidad, i._meta.instrucciones, i._meta.responsable].join(" ") : "", i.vars.map(v => [v.n, v.d, v.f, v.u, (v.tr || []).map(t => t.n + " " + t.d).join(" ")].join(" ")).join(" ")].join(" "));
    i._pend = /pendiente/i.test(i.f) && !i.vars.length; };
  CATI.forEach(c => c.an.forEach(e => e.ind.forEach(i => prep(i, c, e))));
  const INDS_ALL = () => CATI.flatMap(c => c.an.flatMap(e => e.ind));
  const S2 = { q: "", cat: "", enf: "", tipo: "", fam: "", org: "", din: "", gua: "", amb: "", odat: "", aC: new Set(), aE: new Set(), sel: null };
  const hayI = () => !!(S2.q || S2.cat || S2.enf || S2.tipo || S2.fam || S2.org || S2.din || S2.gua || S2.amb || S2.odat);
  function pasaI(i) {
    if (S2.cat && i._c.id !== S2.cat) return false; if (S2.enf && i._e.id !== S2.enf) return false; if (S2.tipo && i.tipo !== S2.tipo) return false; if (S2.fam && !i._fam.includes(S2.fam)) return false; if (S2.org === "dl" && !i._nuevo) return false; if (S2.org === "geo" && (i._nuevo || i._inegi)) return false; if (S2.org === "inegi" && !i._inegi) return false; if (S2.din && dinI(i) !== S2.din) return false; if (S2.gua && guaI(i) !== S2.gua) return false; if (S2.amb && (ambI(i) || [])[0] !== S2.amb) return false; if (S2.odat && odatI(i) !== S2.odat) return false;
    if (S2.q) { const ws = norm(S2.q).split(/\s+/).filter(Boolean); if (!ws.every(w => i._h.includes(w))) return false; } return true;
  }
  function indicadores() {
    const A = INDS_ALL(), nE = CATI.reduce((a, c) => a + c.an.length, 0), nV = A.reduce((a, i) => a + i.vars.length, 0), nT = A.reduce((a, i) => a + i.vars.reduce((b, v) => b + v.tr.length, 0), 0);
    const kp = (n, t, c) => `<div class="dl-kpi" style="--c:${c};cursor:default"><b>${n}</b><span>${t}</span></div>`;
    const tipos = [...new Set(A.map(i => i.tipo))], fams = [...new Set(A.flatMap(i => i._fam))].sort((a, b) => a.localeCompare(b, "es"));
    const enfs = CATI.filter(c => !S2.cat || c.id === S2.cat).flatMap(c => c.an.map(e => [e.id, e.ico + " " + e.n]));
    $("#dlWrap", host).innerHTML = `
      <div class="dl-h"><div><h1>Indicadores</h1><p>Catálogo de los indicadores que calcula Geo Lab y de las series del INEGI (BIE/BISE) que alimentan EconoLab: qué mide cada uno, en qué unidades, cómo se calcula (metodología y fórmula), con qué variables y de qué fuente salen. Es solo un catálogo: aquí no hay datos de ningún proyecto. Elige un elemento de la lista para ver su detalle.</p></div><div class="dl-hr"><span class="dl-up">Geo Lab + INEGI (BIE/BISE)</span><button class="dl-new" type="button" id="dlNuevo">＋ Crear Indicador</button></div></div>
      <div class="dl-kpis">${kp(CATI.length, "Categorías", "#4f46e5")}${kp(nE, "Enfoques de análisis", "#0891b2")}${kp(A.length, "Indicadores", "#7c3aed")}${kp(nV, "Variables", "#059669")}${kp(nT, "Transformaciones", "#b45309")}</div>
      <div class="dl-fl" id="dlFl">
        <div class="dl-fr">
          <div class="dl-q${S2.q ? " has" : ""}">${ICON.lupa}<input id="dlQ2" type="search" placeholder="Buscar indicador, variable, fuente o fórmula…" value="${esc(S2.q)}" autocomplete="off" aria-label="Buscar"><button class="x" type="button" data-ix="q" aria-label="Borrar búsqueda">✕</button></div>
          ${opcionesI("cat", "Categoría", CATI.map(c => [c.id, c.ico + " " + c.n]), S2.cat)}
          ${opcionesI("enf", "Enfoque", enfs, S2.enf)}
          ${opcionesI("tipo", "Tipo de dato", tipos, S2.tipo)}
          ${opcionesI("fam", "Fuente", fams, S2.fam)}
          ${opcionesI("odat", "Origen del dato", Object.keys(ODAT_T).map(k => [k, ODAT_T[k]]), S2.odat)}
          ${opcionesI("org", "Sistema", [["geo", "Geo Lab"], ["inegi", "INEGI (BIE/BISE)"], ["dl", "Creados en Data Lab"]], S2.org)}
          ${opcionesI("din", "Dinámica del dato", Object.keys(DIN_T).map(k => [k, DIN_T[k][0]]), S2.din)}
          ${opcionesI("gua", "Guardado", [["S", "Queda guardado"], ["N", "No se guarda (se lee en cada ingreso)"], ["B", "Bajo demanda"]], S2.gua)}
          ${opcionesI("amb", "Nivel geográfico", Object.keys(AMB_T).map(k => [k, AMB_T[k]]), S2.amb)}
          <button class="dl-ghost" type="button" data-ia="limpiar"${hayI() ? "" : " disabled style=\"opacity:.45;cursor:default\""}>Limpiar</button>
          <button class="dl-ghost pr" type="button" data-ia="abrir">Expandir todo</button>
          <button class="dl-ghost pr" type="button" data-ia="cerrar">Contraer todo</button>
        </div>
        <div class="dl-rs" id="dlRs2"></div>
      </div>
      <div id="dlLista2"></div>
      <div class="dl-foot">Catálogo generado automáticamente: indicadores de Geo Lab (definición de la maqueta) y series del INEGI (BIE/BISE) descargadas para EconoLab.</div>`;
    listaI();
  }
  function opcionesI(id, titulo, lista, val) {
    return `<label class="dl-sel${val ? " on" : ""}"><select data-f2="${id}" aria-label="${esc(titulo)}"><option value="">${esc(titulo)}</option>${lista.map(o => { const v = Array.isArray(o) ? o[0] : o, t = Array.isArray(o) ? o[1] : o; return `<option value="${esc(v)}"${val === v ? " selected" : ""}>${esc(t)}</option>`; }).join("")}</select></label>`;
  }
  // Vista tipo «Banco de Indicadores»: a la izquierda el árbol (dimensión › enfoque › indicador); al elegir un elemento, su detalle se muestra al centro.
  const kI = i => i._e.id + ":" + i.id;
  function listaI() {
    const dest = $("#dlLista2", host); if (!dest) return; const forzar = hayI(), A = INDS_ALL(), vis = A.filter(pasaI);
    const tg = []; [["q", S2.q ? "“" + S2.q + "”" : ""], ["cat", (CATI.find(c => c.id === S2.cat) || {}).n], ["enf", (INDS_ALL().map(i => i._e).find(e => e.id === S2.enf) || {}).n], ["tipo", S2.tipo], ["fam", S2.fam], ["org", S2.org === "dl" ? "Creados en Data Lab" : S2.org === "geo" ? "Geo Lab" : S2.org === "inegi" ? "INEGI (BIE/BISE)" : ""], ["din", S2.din ? DIN_T[S2.din][0] : ""], ["gua", S2.gua ? GUA_T[S2.gua] : ""], ["amb", S2.amb ? AMB_T[S2.amb] : ""], ["odat", S2.odat ? ODAT_T[S2.odat] : ""]].forEach(([k, v]) => { if (v) tg.push(`<span class="dl-tag">${esc(v)}<button type="button" data-ix="${k}" aria-label="Quitar filtro">✕</button></span>`); });
    $("#dlRs2", host).innerHTML = `Mostrando <b>${vis.length}</b> de ${A.length} indicadores ${tg.join("")}`;
    if (!vis.length) { dest.innerHTML = `<div class="dl-empty"><b>No hay indicadores con esos filtros</b>Prueba con otra palabra o quita algún filtro.<br><button class="dl-ghost pr" type="button" data-ia="limpiar">Limpiar filtros</button></div>`; return; }
    let sel = S2.sel; if (sel && sel.t === "i" && !vis.some(i => kI(i) === sel.k)) sel = null; if (sel && sel.t === "e" && !vis.some(i => i._e.id === sel.k)) sel = null; if (sel && sel.t === "c" && !vis.some(i => i._c.id === sel.k)) sel = null;
    const es = (t, k) => !!sel && sel.t === t && sel.k === k, arb0 = $("#dlArb", host), y0 = arb0 ? arb0.scrollTop : 0;
    const arbol = CATI.map(c => {
      const Lc = vis.filter(i => i._c.id === c.id); if (!Lc.length) return ""; const abC = forzar || S2.aC.has(c.id);
      const enfs = !abC ? "" : c.an.map(e => { const Le = Lc.filter(i => i._e.id === e.id); if (!Le.length) return ""; const abE = forzar || S2.aE.has(e.id);
        return `<button class="dl-ixe${es("e", e.id) ? " on" : ""}" type="button" data-ie="${esc(e.id)}" aria-expanded="${abE}"><span class="pm">${abE ? "−" : "+"}</span><span class="tx">${esc(e.n)}</span><span class="n">${Le.length}</span></button>${abE ? `<div class="dl-ixis">${Le.map(i => `<button class="dl-ixi${es("i", kI(i)) ? " on" : ""}" type="button" data-ir="${esc(kI(i))}">${esc(i.n)}</button>`).join("")}</div>` : ""}`; }).join("");
      return `<button class="dl-ixc${es("c", c.id) ? " on" : ""}${abC ? " open" : ""}" type="button" data-ic="${esc(c.id)}" aria-expanded="${abC}"><span class="tx">${esc(c.n)}</span><span class="n">${Lc.length}</span>${ICON.chev}</button>${enfs ? `<div class="dl-ixes">${enfs}</div>` : ""}`;
    }).join("");
    dest.innerHTML = `<div class="dl-ix"><nav class="dl-ixm" id="dlArb" aria-label="Lista de indicadores">${arbol}</nav><section class="dl-ixd" id="dlDet" aria-live="polite">${detalleI(sel, vis)}</section></div>`;
    const arb = $("#dlArb", host); if (arb) arb.scrollTop = y0; pintaGraf();
  }
  // panel central: todas las dimensiones → una dimensión (sus enfoques) → un enfoque (sus indicadores) → la ficha del indicador
  function detalleI(sel, vis) {
    const nI = n => n + (n === 1 ? " indicador" : " indicadores"), ren = (attr, k, ico, n, d, r) => `<button class="dl-ixr" type="button" ${attr}="${esc(k)}">${ico ? `<span class="ic">${ico}</span>` : ""}<span class="tx"><b>${esc(n)}</b><span>${esc(d)}</span></span><span class="r">${r}</span></button>`;
    const miga = p => `<div class="dl-ixb"><button type="button" data-ia="raiz">Todas las dimensiones</button>${p.map(([attr, k, n]) => attr ? `<i>›</i><button type="button" ${attr}="${esc(k)}">${esc(n)}</button>` : `<i>›</i><b>${esc(n)}</b>`).join("")}</div>`;
    if (!sel) return `<h2>Todas las dimensiones</h2><p class="ds">Elige una dimensión, un enfoque o un indicador en la lista de la izquierda; aquí se muestra su detalle.</p><div class="dl-ixrs">${CATI.map(c => { const n = vis.filter(i => i._c.id === c.id).length; return n ? ren("data-ic", c.id, c.ico, c.n, c.d, `<span class="dl-gn">${nI(n)}</span>`) : ""; }).join("")}</div>`;
    if (sel.t === "c") { const c = CATI.find(x => x.id === sel.k); return `${miga([[null, null, c.n]])}<h2><span class="ic">${c.ico}</span>${esc(c.n)}</h2><p class="ds">${esc(c.d)}</p><div class="dl-ixrs">${c.an.map(e => { const n = vis.filter(i => i._e.id === e.id && i._c.id === c.id).length; return n ? ren("data-ie", e.id, e.ico, e.n, e.d, `<span class="dl-gn">${nI(n)}</span>`) : ""; }).join("")}</div>`; }
    if (sel.t === "e") { const Le = vis.filter(i => i._e.id === sel.k), e = Le[0]._e, c = Le[0]._c; return `${miga([["data-ic", c.id, c.n], [null, null, e.n]])}<h2><span class="ic">${e.ico}</span>${esc(e.n)}</h2><p class="ds">${esc(e.d)}</p><div class="dl-ixrs">${Le.map(i => ren("data-ir", kI(i), "", i.n, i.d, `<span class="dl-b t-${TCLS[i.tipo] || "dd"}">${esc(i.tipo)}</span>`)).join("")}</div>`; }
    const i = vis.find(x => kI(x) === sel.k); return miga([["data-ic", i._c.id, i._c.n], ["data-ie", i._e.id, i._e.n], [null, null, i.n]]) + filaI(i);
  }
  // «En Geo Lab»: si el indicador se muestra, dónde, cómo se ve en el mapa y una tarjeta de ejemplo real (datalab-geo.js)
  const CUANDO = { P: ["🆕", "Al crear el proyecto", "Se consulta una vez al crear el proyecto y se guarda con él; después solo se lee."], I: ["🔁", "Cada vez que se ingresa", "Se calcula o se consulta de nuevo cada vez que se abre la maqueta o se pulsa «Calcular y actualizar»."], D: ["👆", "Bajo demanda", "Solo se genera cuando alguien pulsa el botón correspondiente; luego queda guardado o en pantalla."], G: ["📦", "Ya guardado en el sistema", "Es un dato de referencia (Censo, DENUE, INEGI, WorldCover…) ya cargado de antemano: no hay que generarlo."], X: ["⏳", "Pendiente de construir", "Todavía no se genera."] };
  const EN_PANEL = i => !i._nuevo && !i._inegi && (i._c.id === "med" || i._c.id === "ecl");
  // etiquetas independientes que se suman: tarjeta en Geo Lab, capa en el mapa, serie en EconoLab y cuándo se genera
  function chipGeo(i) { const g = !i._nuevo && !i._inegi && (window.DL_GEO || {})[i._e.id + ":" + i.id], o = [], ch = (cls, tit, tx) => o.push(`<span class="dl-geo-chip${cls}" title="${esc(tit)}">${tx}</span>`);
    if ((g && g.w !== "X") || EN_PANEL(i)) ch("", "Se muestra como tarjeta en el panel Indicadores de Geo Lab", "📋 Tarjeta en Geo Lab"); if (g && g.m) ch(" map", "Se dibuja como capa en el mapa de Geo Lab", "🗺️ En el mapa");
    if (i._inegi) ch(" map", "Se grafica y monitorea en EconoLab", "📈 En EconoLab · " + esc(i._inegi.a)); else if (!i._nuevo && i._c.id === "ecl") ch(" map", "Su serie también se grafica y monitorea en EconoLab", "📈 En EconoLab");
    if (g && CUANDO[g.w]) ch(" w" + g.w, CUANDO[g.w][2], CUANDO[g.w][0] + " " + esc(CUANDO[g.w][1])); return o.join(""); }
  function bloqueGeo(i) {
    if (i._inegi) { const x = i._inegi, dnd = x.a === "Nacional" ? "Panorama global, Monitores por sector y Catálogo" : x.a === "Estados" ? "Monitores › Regiones y estados" : "Monitores › Ciudades"; return `<div class="wide dl-geo-b"><span>En EconoLab</span><p>Ámbito <b>${esc(x.a)}</b>. Se consulta en EconoLab (${esc(dnd)}), con su gráfica, pronóstico y reloj económico cuando la serie lo permite. Los datos no son de un proyecto: son series oficiales del INEGI descargadas con su API (BIE/BISE).</p><p><button type="button" class="dl-ghost pr" data-eco="${esc(x.g)}">📈 Abrir en EconoLab ↗</button></p></div>`; }
    if (i._nuevo) { const pub = i._meta && i._meta.publicado; return `<div class="wide dl-geo-b"><span>En Geo Lab</span><p>${pub ? "✓ Publicado: aparece en el panel Indicadores y como ventana en Análisis › Indicadores generados." : "Todavía no se muestra en Geo Lab: aparecerá cuando se publique."}</p></div>`; }
    const g = (window.DL_GEO || {})[i._e.id + ":" + i.id]; if (!g && EN_PANEL(i)) return `<div class="wide dl-geo-b"><span>En Geo Lab</span><p><b>✓ Se muestra</b> en el panel <b>Indicadores</b> de Geo Lab, dentro de «${esc(i._c.n)}» › «${esc(i._e.n)}». Se llena al pulsar «Calcular y actualizar». No tiene capa en el mapa ni tarjeta de ejemplo todavía.${i._c.id === "ecl" ? " Su serie también se grafica en EconoLab." : ""}</p></div>`; if (!g) return `<div class="wide dl-geo-b"><span>En Geo Lab</span><p class="dl-mut">Todavía no se muestra en Geo Lab.</p></div>`;
    return `<div class="wide dl-geo-b"><span>En Geo Lab</span><div class="dl-geo-g">
      <div class="dl-geo-ej">${g.h ? `<div class="dl-geo">${g.h}</div><small>Así se ve la tarjeta en el panel Indicadores (ejemplo real: ${esc(window.DL_GEO_PROY || "")}).</small>` : `<div class="dl-geo-nd">Sin tarjeta de ejemplo: en el proyecto de muestra no hubo dato para este indicador.</div>`}</div>
      <ul class="dl-geo-l"><li><b>✓ Se muestra</b> en el panel <b>Indicadores</b> y en la ventana <b>${esc(g.v)}</b> (menú Análisis).</li>
      <li>${g.m ? "<b>🗺️ En el mapa:</b> " : "<b>Sin capa en el mapa.</b> "}${esc(g.t)}</li>${CUANDO[g.w] ? `<li><b>⏱️ Cuándo se genera:</b> <span class="dl-geo-chip w${g.w}">${CUANDO[g.w][0]} ${esc(CUANDO[g.w][1])}</span> ${esc(g.wn || "")}<div class="dl-mut">${esc(CUANDO[g.w][2])}</div></li>` : ""}</ul></div></div>`;
  }
  /* ═══ Vista previa de los datos: gráfica simple de la serie (las mismas series del INEGI que usa EconoLab; se cargan solo cuando hacen falta) ═══ */
  const EDO = { "00": "Nacional", "01": "Aguascalientes", "02": "Baja California", "03": "Baja California Sur", "04": "Campeche", "05": "Coahuila", "06": "Colima", "07": "Chiapas", "08": "Chihuahua", "09": "Ciudad de México", "10": "Durango", "11": "Guanajuato", "12": "Guerrero", "13": "Hidalgo", "14": "Jalisco", "15": "Estado de México", "16": "Michoacán", "17": "Morelos", "18": "Nayarit", "19": "Nuevo León", "20": "Oaxaca", "21": "Puebla", "22": "Querétaro", "23": "Quintana Roo", "24": "San Luis Potosí", "25": "Sinaloa", "26": "Sonora", "27": "Tabasco", "28": "Tamaulipas", "29": "Tlaxcala", "30": "Veracruz", "31": "Yucatán", "32": "Zacatecas" };
  const CIUN = window.DL_ECO_CIU || {}; let ecoP = null;
  const ecoCarga = () => ecoP || (ecoP = new Promise(res => { if (window.ECO_DATA) return res(window.ECO_DATA); const s = document.createElement("script"); s.src = "ecolab-datos.js?v=" + Math.floor(Date.now() / 36e5); s.onload = () => res(window.ECO_DATA || null); s.onerror = () => { ecoP = null; res(null); }; document.head.appendChild(s); }));
  const refSerie = i => i.s ? i.s : /^inegi_est_/.test(i.id) ? ["est", i.id.slice(10)] : /^inegi_ciu_/.test(i.id) ? ["ciu", i.id.slice(10)] : /^inegi_\d+$/.test(i.id) ? ["nac", i.id.slice(6)] : null;
  const gN = v => { const a = Math.abs(v); return v.toLocaleString("es-MX", { maximumFractionDigits: a >= 1000 ? 0 : a >= 100 ? 1 : 2 }); };
  /* ═══ Origen del dato: ¿dato original de una fuente externa (descargado o consultado) o construcción propia?, con la liga para consultar cada fuente ═══ */
  const ORG_REG = [[/entorno urbano|frente de manzana/i, "inv-fte"], [/inventario nacional de viviendas/i, "inv-mza"], [/denue/i, "denue"], [/\beic\b|intercensal/i, "eic"], [/enigh/i, "enigh"], [/censo/i, "censo"], [/terrarium|relieve/i, "terrain"], [/era5|open-?meteo/i, "meteo"], [/ghsl/i, "ghsl-s"], [/worldcover/i, "worldcover"], [/\bosm\b|openstreetmap|red de calles/i, "osm"], [/google routes/i, "g-routes"], [/google places/i, "g-places"], [/street view/i, "g-sv"], [/google (air quality|solar|pollen|weather)/i, "g-env"]];
  // ficha técnica de cada fuente (complementa el inventario de datalab-fuentes.js): ns nombre simple · arch tipo de archivo · per periodicidad de los datos · lev tipo de levantamiento · dim estructura temporal y espacial · fmt formato técnico
  const TAB = "Estructurado (tablas)", SEMI = "Semiestructurado (JSON)", MALLA = "Estructurado (malla de píxeles)", TXT = "Semiestructurado, con texto no estructurado (las notas se interpretan con IA)";
  const FMETA = {
    censo: { ns: "Censo de Población 2020", arch: "CSV dentro de ZIP (se guarda como JSON)", per: "Cada 10 años (2020), con un levantamiento intermedio cada 5", lev: "Censo", dim: "Corte transversal · geoespacial vectorial (manzanas y AGEB)", fmt: TAB },
    "inv-fte": { ns: "Entorno urbano por frente de manzana", arch: "SHP y CSV dentro de ZIP (se guarda como JSON)", per: "Cada 10 años (se levanta con el Censo)", lev: "Censo (cuestionario de entorno urbano)", dim: "Corte transversal · geoespacial vectorial (líneas: frentes de manzana)", fmt: TAB },
    "inv-mza": { ns: "Inventario de viviendas por manzana", arch: "SHP y CSV dentro de ZIP (se guarda como JSON)", per: "Cada 10 años (se levanta con el Censo)", lev: "Censo", dim: "Corte transversal · geoespacial vectorial (polígonos: manzanas)", fmt: TAB },
    denue: { ns: "Directorio de negocios (DENUE)", arch: "CSV dentro de ZIP (se guarda como JSON)", per: "Dos ediciones al año; base completa con los Censos Económicos (cada 5 años)", lev: "Directorio estadístico (censo económico y registros)", dim: "Corte transversal · geoespacial vectorial (puntos)", fmt: TAB },
    eic: { ns: "Encuesta Intercensal 2025", arch: "Tabulados XLSX / CSV (se guarda como JSON)", per: "Cada 5 años (entre censos)", lev: "Encuesta (muestra grande entre censos)", dim: "Corte transversal (municipios)", fmt: TAB },
    enigh: { ns: "Encuesta de ingresos de los hogares (ENIGH)", arch: "CSV dentro de ZIP (microdatos)", per: "Cada 2 años", lev: "Encuesta en hogares", dim: "Corte transversal agrupado (muestra distinta en cada edición)", fmt: TAB },
    terrain: { ns: "Relieve (elevación del terreno)", arch: "Teselas PNG (formato Terrarium)", per: "Estática (no cambia)", lev: "Sensores remotos (modelos de elevación)", dim: "Geoespacial ráster", fmt: MALLA },
    meteo: { ns: "Clima y calidad del aire (Open-Meteo)", arch: "JSON", per: "Horaria y diaria", lev: "Reanálisis y modelos meteorológicos (ERA5)", dim: "Espacio-temporal (serie de tiempo por punto)", fmt: SEMI },
    "ghsl-s": { ns: "Crecimiento urbano (GHSL)", arch: "GeoTIFF", per: "Cada 5 años (1975–2030)", lev: "Sensores remotos (imágenes de satélite clasificadas)", dim: "Espacio-temporal · ráster", fmt: MALLA },
    worldcover: { ns: "Cobertura del suelo (ESA WorldCover)", arch: "GeoTIFF", per: "Anual (ediciones 2020 y 2021)", lev: "Sensores remotos (satélites Sentinel)", dim: "Geoespacial ráster (10 m)", fmt: MALLA },
    osm: { ns: "Mapa de calles (OpenStreetMap)", arch: "PBF (se guarda como JSON)", per: "Continua (lo edita la comunidad)", lev: "Cartografía colaborativa", dim: "Geoespacial vectorial (puntos, líneas y polígonos)", fmt: "De red o grafos (red vial) y semiestructurado" },
    "g-routes": { ns: "Rutas y tráfico (Google)", arch: "JSON", per: "En tiempo real", lev: "Plataforma privada (datos de navegación)", dim: "Espacio-temporal (rutas según día y hora)", fmt: SEMI + " sobre una red vial (grafo)" },
    "g-places": { ns: "Lugares y negocios (Google)", arch: "JSON", per: "Continua", lev: "Plataforma privada (directorio y reseñas)", dim: "Corte transversal · geoespacial vectorial (puntos)", fmt: SEMI },
    "g-sv": { ns: "Fotos de calle (Street View)", arch: "Imágenes JPEG", per: "Irregular (según los recorridos de Google)", lev: "Levantamiento fotográfico en campo", dim: "Geoespacial (imágenes con ubicación)", fmt: "No estructurado (imágenes; se interpretan con IA)" },
    "g-env": { ns: "Aire, sol, polen y clima (Google)", arch: "JSON (y GeoTIFF en Solar)", per: "Horaria / en tiempo real", lev: "Sensores, satélite y modelos", dim: "Espacio-temporal", fmt: SEMI },
    meta: { ns: "Facebook e Instagram (Meta)", arch: "JSON", per: "En tiempo real", lev: "Plataforma privada (redes sociales)", dim: "Datos de panel (publicaciones seguidas en el tiempo)", fmt: TXT },
    noticias_gdelt: { ns: "Noticias del mundo (GDELT)", arch: "JSON", per: "Continua (cada 15 minutos)", lev: "Monitoreo automático de medios", dim: "Espacio-temporal (notas con fecha y lugar)", fmt: TXT },
    noticias_grss: { ns: "Google Noticias", arch: "XML (RSS)", per: "Continua", lev: "Monitoreo automático de medios", dim: "Espacio-temporal (notas con fecha y lugar)", fmt: TXT },
    noticias_newsdata: { ns: "NewsData.io", arch: "JSON", per: "Continua", lev: "Monitoreo automático de medios", dim: "Espacio-temporal (notas con fecha y lugar)", fmt: TXT },
    noticias_gnews: { ns: "GNews", arch: "JSON", per: "Continua", lev: "Monitoreo automático de medios", dim: "Espacio-temporal (notas con fecha y lugar)", fmt: TXT }
  };
  /* ═══ Nivel de representación geográfica: qué zona abarca el indicador y a qué nivel viene el dato de origen ═══ */
  const AMB_T = { PT: "Punto (la propiedad)", MZ: "Manzana", ZN: "Zona alrededor (radio o contorno)", RT: "Rutas y vialidades", MQ: "Maqueta", CD: "Ciudad", ES: "Estado", NA: "Nacional" };
  const PROP = "El punto de la propiedad", KM2 = "Zona de ≈ 1 km² alrededor de la propiedad (hexágonos de 200 m a 450 m o menos)", MQT = "Toda el área de la maqueta", R500 = "Radio de 500 m alrededor de la propiedad", CALLES = "Calles alrededor de la propiedad (fotos de calle)";
  const AMB_E = { met: ["PT", PROP], inu: ["PT", "Frente a la propiedad"], geo: ["PT", PROP], urb: ["MQ", MQT], rie: ["ZN", CALLES], eu: ["ZN", R500], fri: ["RT", "Rutas en auto entre la propiedad y los destinos de interés"], cob: ["ZN", "Contorno de tiempo de traslado desde la propiedad (isócrona)"], tra: ["RT", "Vialidades consultadas alrededor de la propiedad"], od: ["CD", "Zonas de la ciudad de donde llega el tráfico que pasa frente a la propiedad"], dem: ["ZN", KM2], eco: ["ZN", R500], afl: ["ZN", R500], ent: ["ZN", CALLES], aire: ["PT", PROP + " (celda de ≈ 500 m del modelo de Google)"], psl: ["MQ", "Las azoteas más grandes de la maqueta"], arb: ["ZN", KM2], emi: ["ZN", KM2], eclnac: ["NA", "Todo el país"], eclest: ["ES", "El estado donde está la propiedad"], eclloc: ["CD", "La ciudad con datos más cercana a la propiedad (hasta 80 km)"] };
  const AMB_K = { "inu:calles": ["MQ", MQT], "geo:desn": ["MQ", MQT], "geo:pend5": ["MQ", MQT], "eu:eu_nse": ["MZ", "La manzana de la propiedad"], "eco:gpn": ["ZN", "Radio de 1 km alrededor de la propiedad"], "eco:gpa": ["ZN", "Radio de 1 km alrededor de la propiedad"], "eco:gpr": ["ZN", "Radio de 1 km alrededor de la propiedad"], "arb:arbi": ["ZN", CALLES] };
  const ambI = i => { if (i._nuevo) return null; const sr = refSerie(i); if (i._inegi && sr) return sr[0] === "nac" ? ["NA", "Todo el país"] : sr[0] === "est" ? ["ES", "Cada estado (32 entidades)"] : ["CD", "Cada ciudad con datos (39 ciudades)"]; if (i._c.id === "med") return ["ZN", "Radio de 40 km alrededor de la propiedad"]; return AMB_K[i._e.id + ":" + i.id] || AMB_E[i._e.id] || null; };
  // nivel geográfico al que viene el dato en cada fuente
  const FGEO = { censo: "Manzana y AGEB (el sistema lo agrupa en hexágonos de 200 m)", "inv-fte": "Frente de manzana (tramo de calle)", "inv-mza": "Manzana", denue: "Coordenada geográfica de cada establecimiento (punto)", eic: "Municipio", enigh: "Hogar encuestado; el sistema lo estima por manzana, AGEB y hexágono", terrain: "Píxel de ≈ 35 m", meteo: "Celda de malla de ≈ 10 a 25 km", "ghsl-s": "Píxel de 100 m", worldcover: "Píxel de 10 m", osm: "Coordenada geográfica (puntos, líneas y polígonos)", "g-routes": "Ruta entre dos coordenadas", "g-places": "Coordenada geográfica de cada negocio (punto)", "g-sv": "Coordenada geográfica de cada foto (punto)", "g-env": "Punto consultado (aire: celda de ≈ 500 m; solar: cada azotea)", meta: "Publicación, con ubicación cuando la tiene", noticias_gdelt: "Lugar mencionado en la nota (coordenada aproximada)", noticias_grss: "Lugar mencionado en la nota (coordenada aproximada)", noticias_newsdata: "Lugar mencionado en la nota (coordenada aproximada)", noticias_gnews: "Lugar mencionado en la nota (coordenada aproximada)" };
  const ORG_BIE = "https://www.inegi.org.mx/app/indicadores/";
  const ORG_FORMA = m => /descarga/i.test(m || "") ? "Descarga automática (masiva); se guarda en el sistema" : /bie\/bise/i.test(m || "") ? "API del INEGI (descarga automática cada mes)" : /rss/i.test(m || "") ? "RSS (consulta automática)" : /interna/i.test(m || "") ? "Interna" : "API (consulta en vivo)";
  const levINEGI = (prog, clave) => /registro administrativo|balanza comercial|seguro social/i.test(prog) ? "Registro administrativo" : /encuesta/i.test(prog) || /desoc|^c_/.test(clave || "") ? "Encuesta" + (/econométricos/i.test(prog) ? " (serie calculada por el INEGI)" : "") : /precios/i.test(prog) ? "Índice de precios (cotizaciones levantadas en campo)" : "Estadística derivada (el INEGI la calcula con encuestas, censos y registros)";
  // escala de medición, deducida de las unidades y del tipo de indicador
  const escalaI = i => { const u = String(i.u || ""); if (/cualitativa/i.test(i.tipo) || /bajo|alta \/|niveles|nivel \d|etapas|estrellas/i.test(u)) return "Cualitativo ordinal (niveles con orden)"; if (!u.trim()) return "Cualitativo nominal (categoría o texto)"; if (/notas|publicaciones|personas|habitantes|establecimientos|negocios|riesgos|anclas|árboles|viviendas|unidades|vehículos|número/i.test(u) && !/[\/%]/.test(u)) return "Cuantitativo discreto (conteo)"; return "Cuantitativo continuo"; };
  const ODAT_T = { O: "Dato de origen", C: "Construido en el sistema" };
  const odatI = i => i._odat || (i._odat = (e => e === ODAT_T.O ? "O" : "C")(origenI(i).et[0]));
  function origenI(i) {
    const ext = [], pon = x => { if (!ext.some(y => y.of === x.of)) ext.push(x); };
    const deF = id => { const f = F.find(x => x.id === id), m = FMETA[id] || {}; if (f) pon({ ns: m.ns || f.n, of: f.n, p: f.p, tf: [f.t, f.a].filter(Boolean).join(" · "), forma: ORG_FORMA(f.m), arch: m.arch, lev: m.lev, per: m.per, act: f.act, dim: m.dim, fmt: m.fmt, geo: FGEO[id], url: f.url }); };
    const sr = refSerie(i), g = (window.DL_GEO || {})[i._e.id + ":" + i.id];
    if (sr) { const tw = INDS_ALL().find(x => x.id === (sr[0] === "nac" ? "inegi_" + sr[1] : "inegi_" + sr[0] + "_" + sr[1])) || i, v0 = (tw.vars[0] || {}).f || "", prog0 = v0.replace(/ · API BIE\/BISE.*$/, ""), prog = /^INEGI ·/.test(prog0) || !prog0 ? "" : prog0, idS = sr[0] === "nac" ? sr[1] : (v0.match(/id (\d+)/) || [])[1], per = (String(tw.m || "").match(/Periodicidad (\w+)/) || [])[1] || "";
      pon({ ns: "Banco de Indicadores del INEGI", of: (prog || "INEGI · Banco de Indicadores (BIE/BISE)") + " · serie «" + tw.n + "»" + (idS ? " (id " + idS + ")" : ""), p: "INEGI", tf: "Pública · Gratuita", forma: "API del INEGI (descarga automática cada mes)", arch: "JSON", lev: levINEGI(prog, sr[0] === "nac" ? "" : sr[1]), per: per ? per.charAt(0).toUpperCase() + per.slice(1) : "", perS: !per, act: "Cuando el INEGI publica un dato nuevo" + (per ? " (" + per + ")" : "") + "; el sistema la revisa cada mes", dim: sr[0] === "nac" ? "Serie de tiempo (nacional)" : "Datos de panel (" + (sr[0] === "est" ? "32 estados" : "ciudades") + " a lo largo del tiempo)", fmt: SEMI + ", se guarda como tabla", geo: sr[0] === "nac" ? "Nacional" : sr[0] === "est" ? "Estado (32 entidades)" : "Ciudad o zona metropolitana (39)", url: ORG_BIE }); }
    else if (i._c.id === "med") { const N = F.filter(x => x.cat === "noticias"), A2 = N.filter(x => x.e === "Activa"); (A2.length ? A2 : N).forEach(f => deF(f.id)); if (/pub|eng/.test(i.id)) deF("meta"); }
    else { const txs = i.vars.length ? i.vars.map(v => v.f) : [i.f]; txs.forEach(tx => { let ok = false; ORG_REG.forEach(([re, id]) => { if (re.test(tx)) { deF(id); ok = true; } }); if (!ok && /^\s*inegi\s*$/i.test(tx)) pon({ ns: "INEGI", of: "Instituto Nacional de Estadística y Geografía (INEGI)", p: "INEGI", tf: "Pública · Gratuita", url: "https://www.inegi.org.mx/" }); }); }
    const original = !!sr || (i.tipo === "Dato duro" && i._c.id !== "med" && ext.length > 0), propio = !original && !ext.length;
    const et = original ? ["Dato de origen", "t-dd", sr ? "Es una serie oficial del INEGI: se muestra tal como la publica la fuente, sin modificarla." : "Proviene directamente de la fuente externa, sin modelos ni supuestos propios."]
      : propio ? ["Construido en el sistema", "t-ix", "Lo genera el sistema con modelos, detección o supuestos propios; no usa una fuente externa directa."]
      : ["Construido en el sistema", "t-ix", i._c.id === "med" ? "MediaLab lo construye a partir de las notas y publicaciones que recopila de fuentes externas." : "Lo calcula el sistema a partir de datos de fuentes externas (ver metodología y fórmula)."];
    const estr = sr ? (sr[0] === "nac" ? "Serie de tiempo" : "Datos de panel (" + (sr[0] === "est" ? "estados" : "ciudades") + " a lo largo del tiempo)") : i._c.id === "med" ? "Un valor (escalar) por proyecto y propiedad" : "Un valor (escalar) por propiedad" + (g && g.m ? "; además se dibuja como capa geoespacial en el mapa" : "");
    const amb = ambI(i), fila = (k, v, at) => v ? `<div><dt>${k}</dt><dd${at || ""}>${esc(v)}</dd></div>` : "";
    const fx = x => `<div class="dl-fx"><div class="dl-fxh"><b>${esc(x.ns)}</b>${x.url ? `<a href="${esc(x.url)}" target="_blank" rel="noopener">Consultar fuente ↗</a>` : ""}</div><dl class="dl-fxg">${fila("Nombre oficial", x.of)}${fila("Tipo de fuente", x.tf)}${fila("Forma de consulta o descarga", x.forma)}${fila("Tipo de archivo", x.arch)}${fila("Tipo de levantamiento", x.lev)}${x.perS ? `<div><dt>Periodicidad de los datos</dt><dd data-per-s>Se muestra al cargar la serie…</dd></div>` : fila("Periodicidad de los datos", x.per)}${fila("Periodicidad de actualización", x.act)}${fila("Nivel geográfico del dato", x.geo)}${fila("Estructura temporal y espacial", x.dim)}${fila("Formato técnico", x.fmt)}</dl></div>`;
    return { et, amb, ext, geoIgual: !!sr,
      origen: `<div class="wide"><span>Origen del dato</span><p><span class="dl-b ${et[1]}">${et[0]}</span> ${esc(et[2])}</p></div>`,
      fuentes: ext.length ? `<div class="wide"><span>${ext.length === 1 ? "Fuente" : "Fuentes (" + ext.length + ")"}</span>${ext.map(fx).join("")}</div>` : "",
      tipo: `<div class="wide"><span>Forma del dato</span><dl class="dl-fxg">${fila("Estructura (tiempo y espacio)", estr)}${fila("Escala de medición", escalaI(i))}${fila("Clasificación del indicador", i.tipo)}</dl></div>`,
      geo: amb ? `<div class="wide"><span>Nivel geográfico</span><dl class="dl-fxg"><div><dt>Zona que representa el indicador</dt><dd><span class="dl-b amb">${AMB_T[amb[0]]}</span> ${esc(amb[1])}</dd></div>${!sr && ext.some(x => x.geo) ? `<div><dt>Nivel al que viene el dato de origen</dt><dd>${ext.filter(x => x.geo).map(x => `<b>${esc(x.ns)}:</b> ${esc(x.geo)}`).join("<br>")}</dd></div>` : ""}</dl></div>` : "" };
  }
  /* ═══ Dinámica y guardado: qué tanto cambia el dato con el tiempo y si queda guardado tras la primera lectura o se vuelve a leer en cada ingreso a la maqueta ═══ */
  const DIN_E = { geo: "E", met: "P", inu: "P", urb: "P", rie: "P", eu: "P", fri: "P", cob: "P", tra: "D", od: "D", dem: "P", eco: "P", afl: "P", ent: "P", aire: "D", psl: "P", arb: "P", emi: "P" }, DIN_K = { "met:temp": "E", "met:lluvia": "E", "met:sol": "E", "met:aire": "D", "met:clima": "D", "fri:trafr": "D" };
  const DIN_T = { E: ["Estático", "No cambia de forma importante con el tiempo (por ejemplo el relieve o el clima promedio de varios años)."], P: ["Poco dinámico", "Cambia despacio, en meses o años (censos, uso del suelo, red de calles, negocios)."], R: ["Periódico", "La fuente publica un dato nuevo cada mes o cada trimestre."], D: ["Dinámico", "Cambia en horas o días (tráfico, aire, clima del momento, noticias)."] };
  const GUA_T = { S: "Queda guardado", N: "No se guarda", B: "Bajo demanda", X: "Pendiente" };
  const dinI = i => i._nuevo ? "" : (i._inegi || i._c.id === "ecl") ? "R" : i._c.id === "med" ? "D" : DIN_K[i._e.id + ":" + i.id] || DIN_E[i._e.id] || "";
  const guaI = i => { if (i._nuevo) return ""; if (i._inegi || i._c.id === "ecl") return "S"; if (i._c.id === "med") return "N"; const g = (window.DL_GEO || {})[i._e.id + ":" + i.id]; return g ? ({ P: "S", G: "S", I: "N", D: "B", X: "X" })[g.w] || "" : ""; };
  function dinamicaI(i) {
    const d = dinI(i), gu = guaI(i); if (!d && !gu) return ""; const g = (window.DL_GEO || {})[i._e.id + ":" + i.id], w = g && g.w;
    const lee = (i._inegi || i._c.id === "ecl") ? "El sistema descarga las series del INEGI cada mes" + (i._c.id === "ecl" ? "; en la maqueta se muestran al pulsar «Calcular y actualizar»." : ".") : i._c.id === "med" ? "Cada vez que se pulsa «Calcular y actualizar» en la maqueta." : w === "P" ? "Una sola vez, al crear el proyecto." : w === "G" ? "El dato de origen ya está en el sistema; el indicador se arma al abrir la maqueta, sin volver a descargar." : w === "I" ? "Cada vez que se entra a la maqueta." : w === "D" ? "Solo cuando alguien lo pide con el botón correspondiente." : w === "X" ? "Todavía no se genera." : "";
    const queda = (i._inegi || i._c.id === "ecl") ? "Sí: las series quedan guardadas en el sistema y se renuevan con cada descarga mensual." : i._c.id === "med" ? "No: se consulta de nuevo a MediaLab en cada cálculo." : w === "P" ? "Sí: la primera lectura se guarda con el proyecto y después solo se lee." : w === "G" ? "Sí: es un dato de referencia guardado de antemano para todos los proyectos." : w === "I" ? "No: se consulta o se calcula de nuevo en cada ingreso." : w === "D" ? "Solo después de pedirlo; el resultado queda guardado o en pantalla." : "";
    const f = (k, v) => v ? `<div><dt>${k}</dt><dd>${v}</dd></div>` : "";
    return `<div class="wide"><dl class="dl-fxg">${d ? f("Qué tanto cambia el dato", `<span class="dl-b din-${d}">${DIN_T[d][0]}</span> ${esc(DIN_T[d][1])}`) : ""}${f("Cuándo se lee", esc(lee))}${f("¿Queda guardado?", gu ? `<span class="dl-b gua-${gu}">${GUA_T[gu]}</span> ${esc(queda)}` : "")}${g && g.wn ? f("Detalle", esc(g.wn)) : ""}</dl></div>`;
  }
  function grafSVG(obs0, u) {
    const N = 60, todas = (obs0 || []).filter(o => o && o[1] != null && isFinite(+o[1])), obs = todas.slice(-N); if (obs.length < 2) return "";
    const W = 760, H = 230, l = 54, r = 14, tp = 12, b = 26, vs = obs.map(o => +o[1]); let mn = Math.min(...vs), mx = Math.max(...vs); if (mn === mx) { mn -= 1; mx += 1; } const pad = (mx - mn) * .08; mn -= pad; mx += pad;
    const X = k => l + (W - l - r) * k / (obs.length - 1), Y = v => tp + (H - tp - b) * (1 - (v - mn) / (mx - mn)), pts = vs.map((v, k) => X(k).toFixed(1) + "," + Y(v).toFixed(1));
    const tk = [0, 1, 2, 3].map(k => mn + (mx - mn) * k / 3).map(v => `<line x1="${l}" x2="${W - r}" y1="${Y(v).toFixed(1)}" y2="${Y(v).toFixed(1)}" stroke="#e2e8f0" stroke-width="1"/><text x="${l - 6}" y="${(Y(v) + 3.5).toFixed(1)}" text-anchor="end">${gN(v)}</text>`).join("");
    const cero = mn < 0 && mx > 0 ? `<line x1="${l}" x2="${W - r}" y1="${Y(0).toFixed(1)}" y2="${Y(0).toFixed(1)}" stroke="#94a3b8" stroke-width="1" stroke-dasharray="4 3"/>` : "";
    const xl = [0, Math.floor((obs.length - 1) / 2), obs.length - 1].map((k, j) => `<text x="${X(k).toFixed(1)}" y="${H - 7}" text-anchor="${j === 0 ? "start" : j === 2 ? "end" : "middle"}">${esc(obs[k][0])}</text>`).join("");
    const hit = obs.map((o, k) => `<circle cx="${X(k).toFixed(1)}" cy="${Y(vs[k]).toFixed(1)}" r="7" fill="transparent" class="h"><title>${esc(o[0])}: ${gN(vs[k])} ${esc(u)}</title></circle>`).join("");
    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" font-family="system-ui,-apple-system,sans-serif" font-size="11" fill="#64748b" role="img" aria-label="Gráfica de la serie">${tk}${cero}<polygon fill="#6366f1" fill-opacity=".1" points="${l},${H - b} ${pts.join(" ")} ${W - r},${H - b}"/><polyline fill="none" stroke="#4f46e5" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" points="${pts.join(" ")}"/><circle fill="#4f46e5" stroke="#fff" stroke-width="2" cx="${X(obs.length - 1).toFixed(1)}" cy="${Y(vs[vs.length - 1]).toFixed(1)}" r="4"/>${xl}${hit}</svg>`;
  }
  /* ═══ Vista previa: widget tipo KPI, catálogo de gráficas (se elige la adecuada para el indicador) y mini-widget para reportes ═══
     Los tipos son los que ya se usan en los sistemas: tarjetas de Geo Lab (barra, columnas, medidor, escala, sparkline, pasos, dona, rosa), EconoLab (línea, mapa de calor),
     ventanas de Geo Lab (radar, pirámide, barras apiladas, boxplot) y MediaLab (treemap). Todo se dibuja en SVG con atributos propios para poder descargarlo. */
  const PV = (() => {
    const W = 300, H = 160, P = "#4f46e5", PAL = ["#4f46e5", "#06b6d4", "#f59e0b", "#10b981", "#ef4444", "#8b5cf6", "#64748b"], G = "#e2e8f0", T = "#64748b";
    const svg = (inner, w, h) => `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w || W} ${h || H}" font-family="system-ui,-apple-system,'Segoe UI',sans-serif" role="img">${inner}</svg>`;
    const rng = seed => { let s = 7; for (const ch of String(seed)) s = (s * 31 + ch.charCodeAt(0)) >>> 0; return () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296; };
    const ej = (seed, n, a, b) => { const r = rng(seed); let v = a + (b - a) * r(); return Array.from({ length: n }, () => (v = Math.max(a, Math.min(b, v + (r() - .48) * (b - a) * .3)))); };
    const esc2 = s => String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
    const rango = (v, cero) => { let mn = Math.min(...v), mx = Math.max(...v); if (cero) { mn = Math.min(mn, 0); mx = Math.max(mx, 0); } if (mn === mx) { mn -= 1; mx += 1; } return [mn, mx]; };
    const base = `<line x1="14" x2="${W - 14}" y1="${H - 18}" y2="${H - 18}" stroke="${G}"/>`;
    const pts = v => { const [mn, mx] = rango(v); return v.map((q, k) => (14 + (W - 28) * (v.length === 1 ? .5 : k / (v.length - 1))).toFixed(1) + "," + (14 + (H - 34) * (1 - (q - mn) / (mx - mn))).toFixed(1)); };
    const cols = (v, color) => { const [mn, mx] = rango(v, true), bw = (W - 28) / v.length, Y = q => 14 + (H - 34) * (1 - (q - mn) / (mx - mn)), y0 = Y(0); return `<line x1="14" x2="${W - 14}" y1="${y0.toFixed(1)}" y2="${y0.toFixed(1)}" stroke="${G}"/>` + v.map((q, k) => { const y = Y(q); return `<rect x="${(14 + k * bw + bw * .14).toFixed(1)}" y="${Math.min(y, y0).toFixed(1)}" width="${(bw * .72).toFixed(1)}" height="${Math.max(1, Math.abs(y - y0)).toFixed(1)}" rx="1.5" fill="${color(q)}"/>`; }).join(""); };
    const arco = (p, c, cx, cy, r, sw, fondo) => { const L = Math.PI * r, d = `M${cx - r} ${cy} A${r} ${r} 0 0 1 ${cx + r} ${cy}`; return `<path d="${d}" fill="none" stroke="${fondo || G}" stroke-width="${sw}" stroke-linecap="round"/>` + (p == null ? "" : `<path d="${d}" fill="none" stroke="${c}" stroke-width="${sw}" stroke-linecap="round" stroke-dasharray="${(Math.max(.02, Math.min(1, p)) * L).toFixed(1)} ${(L + 10).toFixed(1)}"/>`); };
    const R = {
      linea: c => base + `<polyline points="${pts(c.s).join(" ")}" fill="none" stroke="${P}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/><circle cx="${pts(c.s).pop().split(",")[0]}" cy="${pts(c.s).pop().split(",")[1]}" r="3.6" fill="${P}" stroke="#fff" stroke-width="1.5"/>`,
      area: c => base + `<polygon points="14,${H - 18} ${pts(c.s).join(" ")} ${W - 14},${H - 18}" fill="${P}" fill-opacity=".14"/><polyline points="${pts(c.s).join(" ")}" fill="none" stroke="${P}" stroke-width="2.2" stroke-linejoin="round"/>`,
      barras: c => cols(c.s.slice(-18), () => P),
      signo: c => cols(c.s.slice(-18), q => q >= 0 ? "#10b981" : "#ef4444"),
      spark: c => `<polyline points="${pts(c.s.slice(-24)).join(" ")}" fill="none" stroke="${P}" stroke-width="2.6" stroke-linejoin="round" stroke-linecap="round"/>`,
      gauge: c => arco(c.p, c.col, 150, 122, 84, 16) + `<text x="150" y="112" text-anchor="middle" font-size="30" font-weight="800" fill="#0f172a">${esc2(c.vt)}</text><text x="150" y="134" text-anchor="middle" font-size="11" fill="${T}">${esc2(c.ut)}</text>`,
      barra: c => `<text x="20" y="62" font-size="28" font-weight="800" fill="#0f172a">${esc2(c.vt)}</text><text x="${W - 20}" y="62" text-anchor="end" font-size="11" fill="${T}">${esc2(c.ut)}</text><rect x="20" y="84" width="${W - 40}" height="14" rx="7" fill="${G}"/><rect x="20" y="84" width="${((W - 40) * Math.max(.02, Math.min(1, c.p))).toFixed(1)}" height="14" rx="7" fill="${c.col}"/>`,
      escala: c => ["#ef4444", "#f59e0b", "#84cc16", "#16a34a"].map((z, k) => `<rect x="${20 + k * 65}" y="88" width="63" height="12" rx="3" fill="${z}"/>`).join("") + `<path d="M${(20 + 260 * Math.max(0, Math.min(1, c.p))).toFixed(1)} 84 l-7 -11 h14 z" fill="#0f172a"/><text x="150" y="58" text-anchor="middle" font-size="26" font-weight="800" fill="#0f172a">${esc2(c.vt)}</text><text x="20" y="118" font-size="10" fill="${T}">bajo</text><text x="${W - 20}" y="118" text-anchor="end" font-size="10" fill="${T}">alto</text>`,
      pasos: c => { const ix = Math.round(Math.max(0, Math.min(1, c.p)) * 4); return [0, 1, 2, 3, 4].map(k => `<circle cx="${50 + k * 50}" cy="86" r="${k === ix ? 15 : 10}" fill="${k === ix ? c.col : k < ix ? "#c7d2fe" : G}"/>`).join("") + `<line x1="50" x2="250" y1="86" y2="86" stroke="${G}" stroke-width="2" stroke-dasharray="2 6"/><text x="150" y="48" text-anchor="middle" font-size="22" font-weight="800" fill="#0f172a">${esc2(c.vt)}</text><text x="150" y="126" text-anchor="middle" font-size="10.5" fill="${T}">nivel ${ix + 1} de 5</text>`; },
      dona: c => { const L = 2 * Math.PI * 48; return `<circle cx="150" cy="80" r="48" fill="none" stroke="${G}" stroke-width="18"/><circle cx="150" cy="80" r="48" fill="none" stroke="${c.col}" stroke-width="18" stroke-dasharray="${(L * Math.max(.02, Math.min(1, c.p))).toFixed(1)} ${L.toFixed(1)}" transform="rotate(-90 150 80)"/><text x="150" y="87" text-anchor="middle" font-size="20" font-weight="800" fill="#0f172a">${esc2(c.vt)}</text>`; },
      rank: c => { const L = c.r.slice(0, 7), mx = Math.max(...L.map(x => Math.abs(x[1]))) || 1; return L.map((x, k) => `<text x="96" y="${20 + k * 20}" text-anchor="end" font-size="10" fill="${T}">${esc2(String(x[0]).slice(0, 15))}</text><rect x="102" y="${11 + k * 20}" width="${Math.max(2, 150 * Math.abs(x[1]) / mx).toFixed(1)}" height="12" rx="3" fill="${x[2] ? "#f59e0b" : P}"/><text x="${(106 + 150 * Math.abs(x[1]) / mx).toFixed(1)}" y="${20 + k * 20}" font-size="9.5" fill="#334155">${esc2(x[3] || "")}</text>`).join(""); },
      apiladas: c => { const r = rng(c.k + "ap"), bw = (W - 28) / 6; return base + [0, 1, 2, 3, 4, 5].map(k => { let y = H - 18; return [0, 1, 2].map(j => { const h = 14 + r() * 30; y -= h; return `<rect x="${(14 + k * bw + bw * .16).toFixed(1)}" y="${y.toFixed(1)}" width="${(bw * .68).toFixed(1)}" height="${h.toFixed(1)}" fill="${PAL[j]}"/>`; }).join(""); }).join(""); },
      radar: c => { const r = rng(c.k + "ra"), n = 6, cx = 150, cy = 82, R0 = 60, pt = (k, f) => (cx + Math.sin(k / n * 2 * Math.PI) * R0 * f).toFixed(1) + "," + (cy - Math.cos(k / n * 2 * Math.PI) * R0 * f).toFixed(1); const an = f => Array.from({ length: n }, (_, k) => pt(k, f)).join(" "); return [1, .66, .33].map(f => `<polygon points="${an(f)}" fill="none" stroke="${G}"/>`).join("") + Array.from({ length: n }, (_, k) => `<line x1="${cx}" y1="${cy}" x2="${pt(k, 1).split(",")[0]}" y2="${pt(k, 1).split(",")[1]}" stroke="${G}"/>`).join("") + `<polygon points="${Array.from({ length: n }, (_, k) => pt(k, .35 + r() * .6)).join(" ")}" fill="${P}" fill-opacity=".22" stroke="${P}" stroke-width="2"/>`; },
      rosa: c => { const r = rng(c.k + "ro"), cx = 150, cy = 82; return `<circle cx="${cx}" cy="${cy}" r="62" fill="none" stroke="${G}"/><circle cx="${cx}" cy="${cy}" r="31" fill="none" stroke="${G}"/>` + Array.from({ length: 8 }, (_, k) => { const R0 = 18 + r() * 44, a0 = (k - .4) / 8 * 2 * Math.PI, a1 = (k + .4) / 8 * 2 * Math.PI, p = a => (cx + Math.sin(a) * R0).toFixed(1) + " " + (cy - Math.cos(a) * R0).toFixed(1); return `<path d="M${cx} ${cy} L${p(a0)} A${R0.toFixed(1)} ${R0.toFixed(1)} 0 0 1 ${p(a1)} Z" fill="${PAL[1]}" fill-opacity="${(.35 + R0 / 110).toFixed(2)}"/>`; }).join("") + `<text x="${cx}" y="14" text-anchor="middle" font-size="10" fill="${T}">N</text>`; },
      piramide: c => { const r = rng(c.k + "pi"); return `<line x1="150" x2="150" y1="10" y2="${H - 12}" stroke="${G}"/>` + [0, 1, 2, 3, 4, 5, 6].map(k => { const a = 30 + (k < 4 ? k : 7 - k) * 16 + r() * 14, b = 30 + (k < 4 ? k : 7 - k) * 16 + r() * 14; return `<rect x="${(148 - a).toFixed(1)}" y="${12 + k * 19}" width="${a.toFixed(1)}" height="15" rx="2" fill="${PAL[0]}"/><rect x="152" y="${12 + k * 19}" width="${b.toFixed(1)}" height="15" rx="2" fill="${PAL[2]}"/>`; }).join(""); },
      heat: c => { const n = c.hc, v = c.s.slice(-(n * 5)), [mn, mx] = rango(v), filas = Math.ceil(v.length / n), cw = (W - 28) / n, ch = Math.min(24, (H - 28) / filas); return v.map((q, k) => `<rect x="${(14 + (k % n) * cw + .8).toFixed(1)}" y="${(14 + Math.floor(k / n) * ch + .8).toFixed(1)}" width="${(cw - 1.6).toFixed(1)}" height="${(ch - 1.6).toFixed(1)}" rx="2" fill="${P}" fill-opacity="${(.1 + .85 * (q - mn) / (mx - mn)).toFixed(2)}"/>`).join(""); },
      box: c => { const v = [...c.s].sort((a, b) => a - b), q = f => v[Math.min(v.length - 1, Math.floor(f * (v.length - 1)))], [mn, mx] = rango(v), X = x => 24 + (W - 48) * (x - mn) / (mx - mn); return `<line x1="${X(v[0]).toFixed(1)}" x2="${X(v[v.length - 1]).toFixed(1)}" y1="80" y2="80" stroke="${T}" stroke-width="1.5"/><rect x="${X(q(.25)).toFixed(1)}" y="56" width="${Math.max(2, X(q(.75)) - X(q(.25))).toFixed(1)}" height="48" rx="4" fill="${P}" fill-opacity=".2" stroke="${P}" stroke-width="2"/><line x1="${X(q(.5)).toFixed(1)}" x2="${X(q(.5)).toFixed(1)}" y1="56" y2="104" stroke="${P}" stroke-width="3"/><line x1="${X(v[0]).toFixed(1)}" x2="${X(v[0]).toFixed(1)}" y1="68" y2="92" stroke="${T}" stroke-width="1.5"/><line x1="${X(v[v.length - 1]).toFixed(1)}" x2="${X(v[v.length - 1]).toFixed(1)}" y1="68" y2="92" stroke="${T}" stroke-width="1.5"/><text x="24" y="128" font-size="10" fill="${T}">mín</text><text x="${W - 24}" y="128" text-anchor="end" font-size="10" fill="${T}">máx</text>`; },
      treemap: c => { const r = rng(c.k + "tm"), v = Array.from({ length: 6 }, () => .4 + r()).sort((a, b) => b - a), tot = v.reduce((a, b) => a + b, 0), a = v[0] / tot, b1 = (v[1] + v[2]) / (tot - v[0]); const x0 = 14, y0 = 12, w = W - 28, h = H - 24, o = []; o.push([x0, y0, w * a, h]); const x1 = x0 + w * a, w1 = w - w * a; o.push([x1, y0, w1 * v[1] / (v[1] + v[2]), h * b1], [x1 + w1 * v[1] / (v[1] + v[2]), y0, w1 * v[2] / (v[1] + v[2]), h * b1]); const s3 = v[3] + v[4] + v[5]; let xx = x1; [3, 4, 5].forEach(k => { o.push([xx, y0 + h * b1, w1 * v[k] / s3, h - h * b1]); xx += w1 * v[k] / s3; }); return o.map((q, k) => `<rect x="${(q[0] + 1).toFixed(1)}" y="${(q[1] + 1).toFixed(1)}" width="${Math.max(1, q[2] - 2).toFixed(1)}" height="${Math.max(1, q[3] - 2).toFixed(1)}" rx="3" fill="${PAL[k % PAL.length]}" fill-opacity=".85"/>`).join(""); }
    };
    // catálogo: nombre, dónde se usa hoy y qué dato necesita (s serie · p valor en una escala · r comparación entre lugares · e solo ejemplo)
    const TIPOS = [["linea", "Línea (serie de tiempo)", "EconoLab", "s"], ["area", "Área", "EconoLab", "s"], ["barras", "Columnas", "Geo Lab · tarjetas", "s"], ["signo", "Columnas con signo", "EconoLab · variaciones", "s"], ["spark", "Sparkline", "Geo Lab · tarjetas", "s"], ["heat", "Mapa de calor", "EconoLab", "s"], ["box", "Boxplot (distribución)", "Geo Lab · ventanas", "s"],
      ["gauge", "Medidor", "Geo Lab · tarjetas", "p"], ["barra", "Barra de avance", "Geo Lab · tarjetas", "p"], ["escala", "Escala por tramos", "Geo Lab · tarjetas", "p"], ["pasos", "Pasos o niveles", "Geo Lab · tarjetas", "p"], ["dona", "Dona", "Geo Lab · tarjetas", "p"],
      ["rank", "Barras horizontales (comparación)", "EconoLab · estados y ciudades", "r"], ["apiladas", "Barras apiladas", "Geo Lab · DENUE por giro", "e"], ["radar", "Radar", "Geo Lab · ventanas", "e"], ["rosa", "Rosa de vientos", "Geo Lab · Meteorología", "e"], ["piramide", "Pirámide", "Geo Lab · Demografía", "e"], ["treemap", "Treemap", "MediaLab", "e"]];
    // dibuja un tipo con los datos del indicador; si no los hay, con datos de ejemplo (y lo dice)
    function dibuja(tipo, c) {
      const nec = (TIPOS.find(x => x[0] === tipo) || [])[3], real = nec === "s" ? !!c.serie : nec === "p" ? c.pReal : nec === "r" ? !!c.rank : false;
      const d = Object.assign({}, c, { s: c.serie && c.serie.length > 1 ? c.serie.slice(-60) : ej(c.k + tipo, 24, tipo === "signo" ? -6 : 20, tipo === "signo" ? 8 : 80), p: c.pReal ? c.p : .62, vt: c.pReal || c.vt !== "—" ? c.vt : "62", ut: c.ut, r: c.rank || ["Zona A", "Zona B", "Zona C", "Zona D", "Zona E", "Zona F"].map((n, k) => [n, 90 - k * 11 - (k % 2) * 6, k === 2, ""]), hc: c.hc || 12 });
      return { svg: svg(R[tipo](d)), real };
    }
    return { TIPOS, dibuja, svg, arco, esc2, pts, W, H };
  })();
  const PVSEL = {};   // gráfica que se está viendo en grande (solo en pantalla)
  let PVOF = {}, PVOFok = false;   // gráficas oficiales por indicador (validadas por un administrador; se guardan en el servidor)
  const cargaOficiales = () => api("/geometrika/datalab/graficas").then(j => { PVOF = j.graficas || {}; PVOFok = true; return true; }).catch(() => false);
  // escala acotada del indicador según sus unidades: fracción 0–1 del valor, o null si no tiene una escala fija
  const escF = (u, v) => /%/.test(u) || /0\s?[–-]\s?100|\/100/.test(u) ? v / 100 : /0\s?[–-]\s?5\b|estrellas|\/5/.test(u) ? v / 5 : /1\s?[–-]\s?9|\/9/.test(u) ? (v - 1) / 8 : /0\s?[–-]\s?1\b/.test(u) ? v : null;
  const PV_COL = { ok: "#22c55e", warn: "#facc15", bad: "#ef4444", info: "#60a5fa", na: "#94a3b8" };
  // datos disponibles del indicador: serie real (INEGI), muestra de Geo Lab (tarjeta capturada del proyecto de ejemplo) o nada
  function pvCtx(i, D) {
    const r = refSerie(i), c = { k: kI(i), u: i.u || "", serie: null, rank: null, val: null, ant: null, per: "", perAnt: "", est: "info", msg: "", vt: "—", ut: i.u || "", p: null, pReal: false, pw: null, pwTx: "", origen: "", sel: "", hc: 12, viz: "" };
    if (r && D) {
      const x = r[0] === "nac" ? (D.indicadores || []).find(q => String(q.id) === r[1]) : ((r[0] === "est" ? D.estatal : D.ciudad) || []).find(q => q.clave === r[1]); let obs = x && x.obs;
      if (x && r[0] !== "nac") { const nom = e => (r[0] === "est" ? EDO[e] : CIUN[e]) || e, ult = e => { const o = (x.est[e] || []).filter(q => q && q[1] != null); return o.length ? +o[o.length - 1][1] : null; }, ks = Object.keys(x.est || {}).filter(e => ult(e) != null).sort((a, b) => nom(a).localeCompare(nom(b), "es")), pk = r[0] === "est" ? "gEst" : "gCiu"; let cur = S2[pk]; if (!ks.includes(cur)) cur = ks.find(e => nom(e) === (r[0] === "est" ? "Coahuila" : "Saltillo")) || ks[0]; obs = cur ? x.est[cur] : null;
        c.sel = `<label class="dl-sel on"><select data-gsel="${pk}" aria-label="${r[0] === "est" ? "Estado" : "Ciudad"}">${ks.map(e => `<option value="${esc(e)}"${e === cur ? " selected" : ""}>${esc(nom(e))}</option>`).join("")}</select></label>`;
        c.rank = ks.map(e => [nom(e), ult(e), e === cur, gN(ult(e))]).sort((a, b) => b[1] - a[1]); const pos = c.rank.findIndex(q => q[2]); if (pos > 6) c.rank = c.rank.slice(0, 6).concat([c.rank[pos]]); c.lugar = nom(cur); c.pos = pos + 1; c.de = ks.length; }
      const ok = (obs || []).filter(o => o && o[1] != null && isFinite(+o[1]));
      if (ok.length) { c.serie = ok.map(o => +o[1]); c.obs = ok; c.val = c.serie[c.serie.length - 1]; c.per = ok[ok.length - 1][0]; if (ok.length > 1) { c.ant = c.serie[c.serie.length - 2]; c.perAnt = ok[ok.length - 2][0]; } c.vt = gN(c.val); const rec = c.serie.slice(-60), mn = Math.min(...rec), mx = Math.max(...rec); c.pw = mx > mn ? (c.val - mn) / (mx - mn) : .5; c.pwTx = "Posición dentro del rango de sus últimos " + rec.length + " datos"; const fs2 = rec.map(q => escF(c.u, q)); if (!/variaci|inflaci|crecimiento|cambio/i.test(i.n + " " + i.d) && fs2.every(q => q != null && q >= 0 && q <= 1.001)) { c.p = Math.min(1, escF(c.u, c.val)); c.pReal = true; c.pw = c.p; c.pwTx = "Posición en la escala del indicador"; } c.origen = "Datos reales del INEGI" + (c.lugar ? " · " + c.lugar : "") + " · " + String(D.generado || "").slice(0, 10); }
      c.hc = /trimes/i.test((x && x.frecuencia) || "") ? 4 : 12; c.frec = (x && x.frecuencia) || "";
    } else if (!r) {
      const g = !i._nuevo && (window.DL_GEO || {})[c.k];
      if (g && g.h) { const d = new DOMParser().parseFromString(g.h, "text/html"), b = d.querySelector(".ind-c2 b"), sm = d.querySelector(".ind-c2 small"), c4 = d.querySelector(".ind-c4"), c3 = d.querySelector(".ind-c3");
        if (b) { c.vt = b.textContent.trim(); const n = parseFloat(c.vt.replace(/,/g, "").replace(/[^\d.\-]/g, "")); c.val = isFinite(n) ? n : null; } if (sm && sm.textContent.trim()) c.ut = sm.textContent.trim();
        if (c4) { c.est = ["ok", "warn", "bad", "info", "na"].find(k => c4.classList.contains(k)) || "info"; const eb = c4.querySelector("b"); c.estTx = eb ? eb.textContent.trim() : ""; c.msg = c4.textContent.replace(c.estTx, "").trim(); }
        if (c3) c.viz = c3.innerHTML; c.origen = "Muestra real del proyecto «" + (window.DL_GEO_PROY || "de ejemplo") + "»";
        const u = c.u + " " + c.ut; if (c.val != null) { const f = escF(u, c.val); if (f != null && f >= 0 && f <= 1.001) { c.p = Math.min(1, f); c.pReal = true; c.pw = c.p; c.pwTx = "Posición en la escala del indicador"; } } }
    }
    c.ord = /ordinal/i.test(escalaI(i)); c.esc = c.pReal || (!c.serie && escF(c.u + " " + c.ut, 50) != null);   // ¿tiene una escala fija por naturaleza?
    c.col = PV_COL[c.est] || PV_COL.info; return c;
  }
  // 1) widget tipo KPI
  function pvWidget(i, c) {
    const d = c.val != null && c.ant != null ? c.val - c.ant : null, lin = [];
    if (c.estTx || c.msg) lin.push(`<p><b>${esc(c.estTx || "")}</b> ${esc(c.msg || "")}</p>`);
    if (c.per) lin.push(`<p>Último dato: <b>${esc(c.per)}</b>${c.lugar ? " · " + esc(c.lugar) : ""}</p>`);
    if (d != null) lin.push(`<p>${d > 0 ? "▲" : d < 0 ? "▼" : "＝"} <b>${gN(Math.abs(d))}</b> frente a ${gN(c.ant)} (${esc(c.perAnt)})</p>`);
    if (c.pos) lin.push(`<p>Lugar <b>${c.pos}</b> de ${c.de}</p>`);
    if (c.pw != null && c.pwTx) lin.push(`<p class="f">${esc(c.pwTx)}: ${Math.round(c.pw * 100)} %</p>`);
    if (c.vt === "—") lin.push(`<p class="f">Sin dato de muestra: el valor se llena con cada proyecto.</p>`);
    return `<div class="dl-kw"><div class="g">${PV.svg(PV.arco(c.pw, c.col, 100, 104, 78, 15, "rgba(255,255,255,.2)") + `<text x="100" y="${c.vt.length > 7 ? 92 : 96}" text-anchor="middle" font-size="${c.vt.length > 9 ? 20 : c.vt.length > 6 ? 27 : 36}" font-weight="800" fill="#fff">${PV.esc2(c.vt)}</text><text x="100" y="118" text-anchor="middle" font-size="11.5" fill="#cbd5e1">${PV.esc2(String(c.ut).slice(0, 26))}</text>`, 200, 132)}</div>
      <div class="tx"><small>${esc(i._e.n)}</small><b>${esc(i.n)}</b>${lin.join("")}</div></div>`;
  }
  // 3) mini-widget para reportes (SVG de 260 × 64)
  function pvMini(i, c) {
    const d = c.val != null && c.ant != null ? c.val - c.ant : null, nom = i.n.length > 34 ? i.n.slice(0, 33) + "…" : i.n, vt = c.vt + (c.ut && c.ut.length <= 12 ? " " + c.ut : "");
    let viz = ""; if (c.serie && c.serie.length > 1) { const v = c.serie.slice(-24), mn = Math.min(...v), mx = Math.max(...v), pt = v.map((q, k) => (172 + 76 * k / (v.length - 1)).toFixed(1) + "," + (48 - 30 * (mx > mn ? (q - mn) / (mx - mn) : .5)).toFixed(1)); viz = `<polyline points="${pt.join(" ")}" fill="none" stroke="#4f46e5" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/><circle cx="${pt[pt.length - 1].split(",")[0]}" cy="${pt[pt.length - 1].split(",")[1]}" r="2.8" fill="#4f46e5"/>`; }
    else if (c.pw != null) viz = `<rect x="172" y="30" width="76" height="8" rx="4" fill="#e2e8f0"/><rect x="172" y="30" width="${(76 * Math.max(.03, c.pw)).toFixed(1)}" height="8" rx="4" fill="${c.col}"/>`;
    else viz = `<circle cx="238" cy="32" r="7" fill="${c.col}"/>`;
    return PV.svg(`<rect x=".5" y=".5" width="259" height="63" rx="10" fill="#fff" stroke="#e2e8f0"/><text x="12" y="20" font-size="10" fill="#64748b">${PV.esc2(nom)}</text><text x="12" y="45" font-size="${vt.length > 14 ? 15 : 20}" font-weight="800" fill="#0f172a">${PV.esc2(vt)}</text>${d != null ? `<text x="12" y="58" font-size="9.5" fill="${d >= 0 ? "#059669" : "#dc2626"}">${d > 0 ? "▲" : d < 0 ? "▼" : "＝"} ${PV.esc2(gN(Math.abs(d)))}</text>` : c.estTx ? `<text x="12" y="58" font-size="9" fill="#64748b">${PV.esc2(c.estTx.slice(0, 30))}</text>` : ""}${viz}`, 260, 64);
  }
  // gráficas recomendadas según los datos que tiene el indicador
  // gráficas factibles según la naturaleza de los datos del indicador (solo estas aparecen en el catálogo)
  function pvFact(c) {
    const o = [];
    if (c.serie) { o.push("linea", "area", "barras"); if (c.serie.some(v => v < 0)) o.push("signo"); o.push("spark", "heat", "box"); if (c.rank) o.push("rank"); }
    if (c.ord) o.push("pasos", "escala"); else if (c.esc) o.push("gauge", "barra", "escala", "dona");
    return [...new Set(o)];
  }
  const pvRecom = c => c.serie ? ["linea", c.serie.some(v => v < 0) ? "signo" : "area"].concat(c.rank ? ["rank"] : []) : c.ord ? ["pasos"] : c.esc ? ["gauge", "barra"] : [];
  function previaI(i) { return `<div class="wide dl-pv" id="dlPv" data-k="${esc(kI(i))}"><p class="dl-mut">Cargando vista previa…</p></div>`; }
  function pvHTML(i, c) {
    const fact = pvFact(c), rec = pvRecom(c), ofi = (PVOF[c.k] || []).filter(x => fact.includes(x)), nomT = x => (PV.TIPOS.find(t => t[0] === x) || [])[1] || x;
    const elegido = fact.includes(PVSEL[c.k]) ? PVSEL[c.k] : ofi[0] || rec[0] || fact[0] || "", G0 = elegido ? PV.dibuja(elegido, c) : null;
    const grande = !elegido ? "" : elegido === "linea" && c.obs ? grafSVG(c.obs, i.u) : G0.svg, tag = real => `<em class="${real ? "r" : "e"}">${real ? (c.serie ? "datos reales" : "muestra real") : "valor de ejemplo"}</em>`;
    const cat = fact.map(x => { const T = PV.TIPOS.find(q => q[0] === x), g = PV.dibuja(x, c), es = ofi.includes(x); return `<div class="dl-pvt${x === elegido ? " on" : ""}${es ? " of" : ""}"><button type="button" class="dl-pvv" data-pvt="${x}" title="Se usa hoy en: ${esc(T[2])}. Clic para verla en grande.">${g.svg}<b>${esc(T[1])}</b><span>${tag(g.real)}${rec.includes(x) ? '<em class="s">sugerida</em>' : ""}</span></button><button type="button" class="dl-pvo${es ? " on" : ""}" data-pvo="${x}" aria-pressed="${es}" title="${es ? "Quitar de las gráficas oficiales" : "Validar como gráfica oficial de este indicador"}"><i class="dl-ck">${es ? "✓" : ""}</i>${es ? "Oficial" : "Validar como oficial"}</button></div>`; }).join("");
    return `${c.sel ? `<div class="dl-pvs"><span>Muestra de</span>${c.sel}</div>` : ""}
      <div class="dl-pv2"><div><h4>Widget</h4><p class="dl-mut">Versión sintética del indicador, tipo KPI.</p>${pvWidget(i, c)}</div>
      <div><h4>Mini-widget</h4><p class="dl-mut">Para reportes o lugares con poco espacio.</p><div class="dl-pvm" id="dlPvMini">${pvMini(i, c)}</div><button type="button" class="dl-ghost pr" data-pvd="mini">Descargar SVG</button></div></div>
      ${fact.length ? `<h4>Gráficas oficiales <small>${ofi.length ? ofi.length + (ofi.length === 1 ? " validada" : " validadas") + ": " + esc(ofi.map(nomT).join(", ")) : "ninguna validada todavía"}</small></h4><p class="dl-mut">Las gráficas oficiales son las que se mostrarán para este indicador en los demás sistemas. Puedes validar varias; solo un administrador puede hacerlo.${PVOFok ? "" : " (No se pudo leer la lista guardada: inicia sesión de nuevo.)"}</p>
      <h4>Gráfica <small>${esc(nomT(elegido))} ${tag(G0.real)}${ofi.includes(elegido) ? ' <em class="o">oficial</em>' : ""}</small></h4><div class="dl-pvb${elegido === "linea" && c.obs ? " dl-gr" : ""}" id="dlPvGraf">${grande}</div><button type="button" class="dl-ghost pr" data-pvd="graf">Descargar SVG</button>
      <h4>Gráficas factibles para este indicador <small>${fact.length} de ${PV.TIPOS.length} tipos</small></h4><p class="dl-mut">Solo aparecen los tipos que tienen sentido con los datos de este indicador (${c.serie ? "una serie de tiempo" + (c.rank ? " por lugar" : "") : c.ord ? "un nivel en una escala ordenada" : "un valor dentro de una escala fija"}). Clic en la imagen para verla en grande; «Validar como oficial» la marca para los demás sistemas.</p><div class="dl-pvc">${cat}</div>`
      : `<h4>Gráficas</h4><p class="dl-mut">Este indicador es un solo valor sin escala fija ni serie de tiempo, así que ninguna gráfica del catálogo le aplica: se representa con el widget y el mini-widget.</p>`}
      <p class="dl-mut">${c.origen ? esc(c.origen) + ". " : "Este indicador no tiene datos de muestra: el valor se llena con cada proyecto. "}${c.frec ? "Periodicidad " + esc(String(c.frec).toLowerCase()) + "." : ""}</p>`;
  }
  function pintaGraf() {
    const el = $("#dlPv", host); if (!el) return; const k = el.dataset.k, i = INDS_ALL().find(x => kI(x) === k); if (!i) return;
    const pinta = D => { const e2 = $("#dlPv", host); if (!e2 || e2.dataset.k !== k) return; const c = pvCtx(i, D); e2.innerHTML = pvHTML(i, c); host.querySelectorAll("[data-per-s]").forEach(ps => { ps.textContent = c.frec || "—"; });
      const s = e2.querySelector("select[data-gsel]"); if (s) s.onchange = ev => { ev.stopPropagation(); S2[s.dataset.gsel] = s.value; pintaGraf(); }; };
    if (refSerie(i)) ecoCarga().then(pinta); else pinta(null);
  }
  function pvClic(t) {
    const el = $("#dlPv", host), repinta = () => { const y = $("#dlMain", host).scrollTop; pintaGraf(); $("#dlMain", host).scrollTop = y; };
    const o = t.closest("[data-pvo]"); if (o) { if (!el) return true; const k = el.dataset.k, antes = (PVOF[k] || []).slice(), x = o.dataset.pvo, nuevo = antes.includes(x) ? antes.filter(q => q !== x) : antes.concat([x]);
      if (nuevo.length) PVOF[k] = nuevo; else delete PVOF[k]; repinta();
      api("/geometrika/datalab/graficas", { k, tipos: nuevo }).then(() => toast(nuevo.includes(x) ? "Gráfica validada como oficial." : "Gráfica retirada de las oficiales.")).catch(e => { if (antes.length) PVOF[k] = antes; else delete PVOF[k]; repinta(); toast("No se guardó: " + e.message, 7000); }); return true; }
    const a = t.closest("[data-pvt]"); if (a) { if (!el) return true; PVSEL[el.dataset.k] = a.dataset.pvt; repinta(); return true; }
    const d = t.closest("[data-pvd]"); if (d) { const sv = $(d.dataset.pvd === "mini" ? "#dlPvMini svg" : "#dlPvGraf svg", host); if (sv && el) { let x = sv.outerHTML; if (!/xmlns=/.test(x)) x = x.replace("<svg", '<svg xmlns="http://www.w3.org/2000/svg"'); const u = URL.createObjectURL(new Blob([x], { type: "image/svg+xml" })), l = document.createElement("a"); l.href = u; l.download = (d.dataset.pvd === "mini" ? "mini-" : "grafica-") + el.dataset.k.replace(/[^a-z0-9_]+/gi, "-") + ".svg"; document.body.appendChild(l); l.click(); l.remove(); setTimeout(() => URL.revokeObjectURL(u), 2000); } return true; }
    return false;
  }
  function filaI(i) {   // ficha completa del indicador (panel central)
    const fam = i._fam.map(f => `<button type="button" class="dl-c${S2.fam === f ? " on" : ""}" data-ifam="${esc(f)}" title="Filtrar por esta fuente">${esc(f)}</button>`).join("");
    const vars = i.vars.map(v => `<tr><td data-l="Variable"><b>${esc(v.n)}</b></td><td data-l="Unidades">${esc(v.u)}</td><td data-l="Qué es">${esc(v.d)}</td><td data-l="Fuente">${esc(v.f)}${v.cob ? `<div class="dl-mut">Cobertura: ${esc(v.cob)}</div>` : ""}${v.disponible === false ? `<div><span class="dl-b Parcial">Dato por conseguir</span></div>` : ""}${v.nota ? `<div class="dl-mut">${esc(v.nota)}</div>` : ""}</td><td data-l="Transformaciones">${v.tr.length ? `<details class="dl-tr"><summary>${v.tr.length} ${v.tr.length === 1 ? "transformación" : "transformaciones"}</summary>${v.tr.map(t => `<div class="dl-trx"><b>${esc(t.n)}</b><p>${esc(t.d)}</p><code>${esc(t.f)}</code></div>`).join("")}</details>` : `<span class="dl-mut">Dato directo de la fuente</span>`}</td></tr>`).join("");
    // la ficha se lee de arriba abajo: qué es → cómo se ve → cómo se calcula → de dónde viene → qué tipo de dato es → cuándo se actualiza
    const O = origenI(i), dn = dinamicaI(i), dC = dinI(i), gC = guaI(i), uq = c => { const v = [...new Set(O.ext.map(x => x[c]).filter(Boolean))]; return v.length ? v.slice(0, 3).map(esc).join("<br>") + (v.length > 3 ? "<br><small>+" + (v.length - 3) + " más (ver «De dónde viene»)</small>" : "") : '<span class="dl-mut">Sin fuente externa</span>'; }, kf = (k, v) => v ? `<div><dt>${k}</dt><dd>${v}</dd></div>` : "";
    const sec = (tit, sub, cont) => cont.trim() ? `<details class="dl-sx" open><summary><b>${tit}</b><span>${sub}</span></summary><div class="dl-d dl-ixf"><div class="in one">${cont}</div></div></details>` : "";
    const varsH = `<div class="wide"><span>Variables (${i.vars.length})</span>${i.vars.length ? `<div class="dl-tw in2"><table class="dl-t dl-vt"><thead><tr><th>Variable</th><th>Unidades</th><th>Qué es</th><th>Fuente</th><th>Transformaciones</th></tr></thead><tbody>${vars}</tbody></table></div>` : `<p class="dl-mut">${i._pend ? "Todavía no tiene variables: se construirá más adelante." : "Sin desglose de variables: el dato se toma directo de la fuente."}</p>`}</div>`;
    return `<div class="dl-ixh"><div class="dl-nm">${i._nuevo ? swPub(i._meta) : `<i class="dot${i._pend ? " Pendiente" : ""}" title="${i._pend ? "Pendiente de construir" : i._inegi ? "Serie oficial del INEGI (BIE/BISE)" : "Publicado en Geo Lab"}"></i>`}<h2>${esc(i.n)}</h2></div>
      <div class="dl-ixt">${chipGeo(i)}${i._nuevo ? `<span class="dl-new-b${i._meta.publicado ? " pub" : ""}">${i._meta.publicado ? "Publicado en Geo Lab" : "Nuevo · " + esc(i._meta.estado)}</span>` : ""}</div></div>
      <p class="dl-lead">${esc(i.d)}</p>
      <dl class="dl-kf">${kf("Tipo de dato", `<span class="dl-b t-${TCLS[i.tipo] || "dd"}">${esc(i.tipo)}</span><small>${odatI(i) === "C" ? "Construido en el sistema" : i._inegi && i.tipo !== "Dato duro" ? "Lo calcula el INEGI (dato de origen)" : "De la fuente (dato de origen)"}</small>`)}${kf("Periodicidad de los datos", O.ext.some(x => x.perS) ? `<span data-per-s>Se muestra al cargar la serie…</span>` : uq("per"))}${kf("Forma de consulta o descarga", uq("forma"))}${kf("Tipo de fuente", uq("tf"))}${kf("Familia de fuente (clic para filtrar)", `<div class="dl-ch">${fam}</div>`)}${kf("Tipo de levantamiento", uq("lev"))}${kf("Nivel geográfico", O.amb ? `<span class="dl-b amb">${AMB_T[O.amb[0]]}</span><small>${esc(O.amb[1])}</small>${!O.geoIgual && O.ext.some(x => x.geo) ? `<small><b>Dato de origen:</b> ${[...new Set(O.ext.map(x => x.geo).filter(Boolean))].slice(0, 3).map(esc).join("; ")}</small>` : ""}` : "—")}${kf("¿Queda guardado?", gC ? `<span class="dl-b gua-${gC}">${GUA_T[gC]}</span>` : "—")}</dl>
      ${sec("Vista previa", "cómo se ve el dato", previaI(i) + bloqueGeo(i))}
      ${sec("Cómo se calcula", "metodología, fórmula y variables", `<div class="wide"><span>Unidades</span><p>${esc(i.u) || "—"}</p></div><div><span>Metodología</span><p class="pre">${esc(i.m)}</p></div><div><span>Fórmula</span><code class="dl-code">${esc(i.f)}</code></div>${varsH}`)}
      ${sec("De dónde viene", "origen y fuentes", O.origen + O.fuentes)}
      ${sec("Qué tipo de dato es", "forma del dato y nivel geográfico", O.tipo + O.geo)}
      ${sec("Actualización y guardado", "qué tanto cambia y si se guarda", dn)}
      ${i._meta ? sec("Definición del indicador creado", "objetivo y receta", metaCreado(i._meta)) : ""}`;
  }
  function entradaI(e) { S2.q = e.target.value; e.target.parentNode.classList.toggle("has", !!S2.q); clearTimeout(entradaI.t); entradaI.t = setTimeout(() => { listaI(); const b = $('[data-ia="limpiar"]', host); if (b) { b.disabled = !hayI(); b.style.opacity = hayI() ? "" : ".45"; b.style.cursor = hayI() ? "" : "default"; } }, 120); }
  function cambiaI(k, v) { S2[k] = v; if (k === "cat" && S2.enf && !(CATI.find(c => c.id === v) || { an: [] }).an.some(e => e.id === S2.enf) && v) S2.enf = ""; indicadores(); }
  function clicI(e) {
    const t = e.target; if (pvClic(t)) return; const x = t.closest("[data-ix]"); if (x) { S2[x.dataset.ix] = ""; indicadores(); return; }
    const a = t.closest("[data-ia]"); if (a) { const k = a.dataset.ia; if (k === "limpiar" && hayI()) { S2.q = S2.cat = S2.enf = S2.tipo = S2.fam = S2.org = S2.din = S2.gua = S2.amb = S2.odat = ""; indicadores(); } else if (k === "abrir") { CATI.forEach(c => { S2.aC.add(c.id); c.an.forEach(en => S2.aE.add(en.id)); }); listaI(); } else if (k === "cerrar") { S2.aC.clear(); S2.aE.clear(); listaI(); } else if (k === "raiz") selI(null); return; }
    const eb = t.closest("[data-eco]"); if (eb) { try { parent.postMessage({ gl: "eco-go", go: eb.dataset.eco }, location.origin); } catch (_) { } return; }
    const pb = t.closest("[data-dl-pub]"); if (pb) { publica(pb.dataset.dlPub); return; }
    const del = t.closest("[data-dl-del]"); if (del) { eliminaCreado(del.dataset.dlDel); return; }
    const est = t.closest("[data-dl-est]"); if (est) { cambiaEstado(est.dataset.dlEst, est.dataset.v); return; }
    const f = t.closest("[data-ifam]"); if (f) { S2.fam = S2.fam === f.dataset.ifam ? "" : f.dataset.ifam; indicadores(); return; }
    const enArbol = !!t.closest("#dlArb"), r = t.closest("[data-ir]"); if (r) { selI("i", r.dataset.ir, enArbol); return; }
    const ge = t.closest("[data-ie]"); if (ge) { selI("e", ge.dataset.ie, enArbol); return; }
    const gc = t.closest("[data-ic]"); if (gc) selI("c", gc.dataset.ic, enArbol);
  }
  // elige un elemento: en el árbol, dimensiones y enfoques además se abren o se cierran; desde el panel central solo se abren
  function selI(t, k, enArbol) {
    const era = S2.sel && S2.sel.t === t && S2.sel.k === k; S2.sel = t ? { t, k } : null;
    if (t === "c") { if (enArbol && S2.aC.has(k) && (era || !hayI())) S2.aC.delete(k); else S2.aC.add(k); }
    if (t === "e") { const c = CATI.find(c => c.an.some(e => e.id === k)); if (c) S2.aC.add(c.id); if (enArbol && S2.aE.has(k) && (era || !hayI())) S2.aE.delete(k); else S2.aE.add(k); }
    if (t === "i") { const i = INDS_ALL().find(x => kI(x) === k); if (i) { S2.aC.add(i._c.id); S2.aE.add(i._e.id); } }
    listaI(); const d = $("#dlDet", host), m = $("#dlMain", host);
    if (d && m && (innerWidth < 900 ? t === "i" || !enArbol : d.getBoundingClientRect().top < m.getBoundingClientRect().top)) d.scrollIntoView({ block: "start" });
    if (enArbol) { const b = [...host.querySelectorAll("#dlArb button")].find(q => (t === "i" ? q.dataset.ir : t === "e" ? q.dataset.ie : q.dataset.ic) === k); if (b) b.focus({ preventScroll: true }); }
  }


  /* ═══ Crear indicador con ayuda de IA (panel lateral derecho) ═══ */
  const API = "https://api.check-inn.mx", RES_TXT = { punto: "Punto / coordenada", zona: "Zona", ciudad: "Ciudad" };
  const PERIODOS = ["Tiempo real", "Diaria", "Semanal", "Mensual", "Trimestral", "Anual", "Bajo demanda"], TIPOS = ["Dato duro", "Dato calculado", "Índice", "Valoración cualitativa", "Valoración cuantitativa", "Dato inferido"];
  const token = () => { try { return (JSON.parse(localStorage.getItem("gmTok") || "null") || {}).token || ""; } catch (_) { return ""; } };
  async function api(ruta, cuerpo, metodo, reintento) {
    const r = await fetch(API + ruta, { method: metodo || (cuerpo ? "POST" : "GET"), headers: Object.assign({ "x-gm-token": token() }, cuerpo ? { "Content-Type": "application/json" } : {}), body: cuerpo ? JSON.stringify(cuerpo) : undefined });
    let j = {}; try { j = await r.json(); } catch (_) { }
    if (r.status === 401) throw new Error("Tu sesión expiró. Cierra sesión y vuelve a entrar.");
    // acción de administrador con un pase de nivel normal (p. ej. lo reemplazó el inicio de sesión del sistema administrativo): se pide la contraseña aquí mismo y se reintenta una vez
    if (r.status === 403 && j.admin) { if (!reintento && window.gmPideClave) { toast("Esta acción requiere la contraseña de administrador.", 6000); if (await window.gmPideClave()) return api(ruta, cuerpo, metodo, true); throw new Error("Se canceló: hace falta la contraseña de administrador."); } throw new Error(reintento ? "Esa contraseña no es la de administrador." : (j.error || "Solo un administrador puede hacerlo.")); }
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
  async function eliminaCreado(id) { const it = CRE.find(x => x.id === id); if (!it || !confirm("¿Eliminar el indicador «" + it.n + "»? También saldrá de Fuente de datos.")) return; try { await api("/geometrika/datalab/indicadores/eliminar", { id }); CRE = CRE.filter(x => x.id !== id); if (S2.sel && S2.sel.t === "i" && S2.sel.k.endsWith(":" + id)) S2.sel = null; mezcla(); indicadores(); toast("Indicador eliminado."); } catch (e) { toast("No se pudo eliminar: " + e.message); } }
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
      if (sec !== "indicadores") ir("indicadores"); S2.q = S2.cat = S2.enf = S2.tipo = S2.fam = ""; S2.org = S2.din = S2.gua = S2.amb = S2.odat = ""; const c = CATI.find(x => x.id === j.item.cat); S2.aC.add(j.item.cat); S2.aE.add(j.item.enf); S2.sel = { t: "i", k: j.item.enf + ":" + j.item.id }; indicadores();
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
    if (EMB) { dlTemaPinta(); try { parent.postMessage({ gl: "ctx?" }, location.origin); } catch (_) { } }
    cargaCreados().then(ok => { if (ok && CRE.length) { if (sec === "indicadores") indicadores(); else fuentes(); } });
    cargaOficiales().then(ok => { if (ok && sec === "indicadores") pintaGraf(); });
  };
})();
