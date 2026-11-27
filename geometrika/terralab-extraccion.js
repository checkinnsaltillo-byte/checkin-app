/* TerraLab · Extracción y programación de las tres fuentes (Airbnb, Facebook Marketplace, Mercado Libre).
   Usa los mismos servicios que Investigación de mercados (/mercado/*): la información se guarda en las mismas bases.
   Se invoca desde terralab.js con window.TLX.abre(fuente, contenedor). */
(function () {
  "use strict";
  const API = "https://api.check-inn.mx";
  const $ = (s, r) => (r || document).querySelector(s), $$ = (s, r) => [...(r || document).querySelectorAll(s)];
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const nf = v => v == null || !isFinite(v) ? "—" : Number(v).toLocaleString("es-MX");
  const fec = s => { if (!s) return "—"; const d = new Date(s); return isNaN(d) ? "—" : d.toLocaleString("es-MX", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" }); };
  const hr = h => `${h % 12 || 12}:00 ${h < 12 ? "a.m." : "p.m."}`;
  async function mapi(ruta, cuerpo) {
    const o = cuerpo === undefined ? {} : { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(cuerpo || {}) };
    const r = await fetch(API + "/mercado/" + ruta, o); let j = null; try { j = await r.json(); } catch (_) { }
    if (!r.ok || !j || j.ok === false) throw new Error((j && j.error) || "No se pudo completar la operación (" + r.status + ").");
    return j;
  }
  let M = null, F = null, T = null, A = null, EST = null, timer = null, toastT = null;
  const ck = on => `<span class="tl-ck${on ? " on" : ""}" aria-hidden="true">${on ? "✓" : ""}</span>`;
  const toast = (m, mal) => { const t = $("#txMsg"); if (!t) return; t.textContent = m; t.className = "tx-msg on" + (mal ? " mal" : ""); clearTimeout(toastT); toastT = setTimeout(() => t.classList.remove("on"), 6000); };
  const card = (n, t, sub, body) => `<section class="tx-card"><h3><span class="tx-n">${n}</span>${t}</h3>${sub ? `<p class="tx-sub">${sub}</p>` : ""}${body}</section>`;
  const kpi = (l, v, s) => `<div class="eco-kpi"><div class="kl">${l}</div><div class="kv">${v}</div><div class="ks">${s || ""}</div></div>`;
  const bar = (pct, txt) => `<div class="tx-bar"><i style="width:${Math.max(0, Math.min(100, pct))}%"></i></div><div class="tx-bt">${txt}</div>`;
  const btn = (a, t, cl, dis) => `<button type="button" class="eco-btn tx-b${cl ? " " + cl : ""}" data-tx="${a}"${dis ? " disabled" : ""}>${t}</button>`;
  const num = (k, v, ph, w) => `<input class="tx-in" data-f="${k}" type="number" value="${v == null ? "" : esc(v)}" placeholder="${ph || ""}" style="width:${w || 96}px">`;
  async function accion(b, fn, ok) { if (b) b.disabled = true; try { const r = await fn(); if (ok) toast(typeof ok === "function" ? ok(r) : ok); return r; } catch (e) { toast(e.message, true); } finally { if (b) b.disabled = false; } }

  /* ─────────────── Airbnb ─────────────── */
  function viewAir(j) {
    M = j; const J = j.job || {}, B = j.busqueda || {}, n = (j.lista || []).length, ex = Object.keys(j.items || {}).length;
    const horas = j.horas || [], es = j.escenarios || [];
    return `<div class="eco-kpis tx-k">${kpi("Publicaciones en la lista", nf(n), "se actualizan al extraer")}${kpi("Con ficha y calendario", nf(ex), "hasta 36 meses")}${kpi("Última actualización", fec(j.actualizado), "")}${kpi("Última corrida", J.fin ? `${nf(J.ok)} bien` + (J.fallidas ? ` · ${nf(J.fallidas)} con error` : "") : "—", J.interrumpida ? "se interrumpió" : "")}</div>
      ${card(1, "Buscar publicaciones", "Repite la búsqueda en airbnb.mx para cada destino y rango de fechas y suma a la lista las publicaciones nuevas.", `
        <div class="tx-row"><label class="tx-l">Destinos (uno por línea, máx. 8)<textarea class="tx-ta" id="txDest" rows="3">${esc((j.destinos || []).join("\n"))}</textarea></label>
          <label class="tx-l">Huéspedes<input class="tx-in" id="txAdu" type="number" min="1" max="16" value="${esc((B.params && B.params.adultos) || 1)}" style="width:80px"></label></div>
        <div class="tx-l">Rangos de fechas (se mueven solos cada día)</div><div id="txEsc">${es.map((e, i) => `<div class="tx-r"><span>Entrada en</span>${num("a", e.a, "días", 66)}<span>días · estancia de</span>${num("n", e.n, "noches", 66)}<span>noches</span><em>${esc(e.checkin)} → ${esc(e.checkout)}</em><button type="button" class="tx-x" data-tx="escX" data-i="${i}" title="Quitar">✕</button></div>`).join("")}</div>
        <div class="tx-bs">${btn("escAdd", "+ Añadir rango")}${btn("guardaAir", "💾 Guardar destinos y fechas")}${btn("buscaAir", B.running ? "⏳ Buscando…" : "🔎 Buscar publicaciones nuevas", "pri", B.running)}${B.running ? btn("detAir", "⛔ Detener", "mal") : ""}</div>
        <div id="txBus">${B.running ? bar(B.rangos ? 100 * B.rango / B.rangos : 0, `Rango ${nf(B.rango)} de ${nf(B.rangos)} · ${nf(B.encontrados)} publicaciones encontradas`) : (B.fin ? `<div class="tx-bt">Última búsqueda: ${fec(B.fin)} · ${nf(B.encontrados)} encontradas · ${nf(B.nuevos)} nuevas${B.error ? ` · <b class="tx-er">${esc(B.error)}</b>` : ""}</div>` : "")}</div>
        <details class="tx-d"><summary>Agregar publicaciones por URL o número</summary><textarea class="tx-ta" id="txUrls" rows="3" placeholder="https://www.airbnb.mx/rooms/123…"></textarea>${btn("urlsAir", "Agregar a la lista")}</details>`)}
      ${card(2, "Extraer y programar", "Lee de cada publicación su ficha y su calendario y actualiza la base: una noche pasada se conserva como ocupada y una reserva cancelada vuelve a quedar libre. Corre solo a las horas elegidas.", `
        <div class="tx-l">Horas del día de la extracción automática (1 a 8)</div><div class="tx-hs" id="txHoras">${Array.from({ length: 24 }, (_, h) => `<button type="button" class="tx-h${horas.includes(h) ? " on" : ""}" data-tx="hora" data-h="${h}">${ck(horas.includes(h))}${hr(h)}</button>`).join("")}</div>
        <div class="tx-bs"><button type="button" class="tx-sw" data-tx="muestreo">${ck(j.muestreo)} Muestreo de precios (estima el precio por noche con fechas de prueba)</button></div>
        <div class="tx-bs">${btn("extraeAir", J.running ? "⏳ Extrayendo…" : "🔍 Extraer / actualizar ahora", "pri", J.running)}${J.running ? (J.cancelar ? btn("x", "⏳ Deteniendo…", "", true) : btn("detAir", "⛔ Detener", "mal")) : ""}<span class="tx-mu">${n > 25 ? `Tardará unos ${Math.ceil(n * 9 / 60)} min.` : ""}</span></div>
        <div id="txJob">${J.running ? bar(J.total ? 100 * J.done / J.total : 0, `${nf(J.done)} de ${nf(J.total)}${J.actual ? " · publicación " + esc(J.actual) : ""}`) : ""}</div>`)}`;
  }
  const leeEsc = () => $$("#txEsc .tx-r").map(r => ({ a: +$('[data-f="a"]', r).value, n: +$('[data-f="n"]', r).value })).filter(e => e.a >= 0 && e.n >= 1);
  const accAir = {
    async escAdd() { M.escenarios = leeEsc().concat([{ a: 7, n: 2, checkin: "", checkout: "" }]); pintaAir(); },
    async escX(b) { const e = leeEsc(); e.splice(+b.dataset.i, 1); M.escenarios = e; pintaAir(); },
    async guardaAir(b) { const d = $("#txDest").value.split(/\n/).map(x => x.trim()).filter(Boolean); await accion(b, async () => { await mapi("airbnb/destinos", { destinos: d }); const r = await mapi("airbnb/escenarios", { escenarios: leeEsc() }); M.escenarios = r.escenarios; M.destinos = d; pintaAir(); }, "Destinos y fechas guardados."); },
    async buscaAir(b) { await accion(b, async () => { await mapi("airbnb/buscar", { adultos: +$("#txAdu").value || 1 }); await recarga(); }, "Búsqueda iniciada."); },
    async detAir(b) { await accion(b, async () => { await mapi("airbnb/detener", {}); await recarga(); }, "Deteniendo…"); },
    async urlsAir(b) { await accion(b, async () => { const r = await mapi("airbnb/lista", { texto: $("#txUrls").value }); $("#txUrls").value = ""; await recarga(); return r; }, r => `${r.nuevos} nuevas, ${r.existentes} ya estaban${r.invalidos && r.invalidos.length ? `, ${r.invalidos.length} no válidas` : ""}.`); },
    async hora(b) { const h = +b.dataset.h, cur = (M.horas || []).slice(), i = cur.indexOf(h); if (i >= 0) { if (cur.length === 1) return toast("Deja al menos una hora.", true); cur.splice(i, 1); } else { if (cur.length >= 8) return toast("Máximo 8 extracciones al día.", true); cur.push(h); } cur.sort((a, c) => a - c); await accion(null, async () => { const r = await mapi("airbnb/horas", { horas: cur }); M.horas = r.horas; pintaAir(); }, "Horas guardadas."); },
    async muestreo() { await accion(null, async () => { const r = await mapi("airbnb/muestreo", { on: !M.muestreo }); M.muestreo = r.muestreo; pintaAir(); }, "Guardado."); },
    async extraeAir(b) { await accion(b, async () => { await mapi("airbnb/extraer", {}); await recarga(); }, "Extracción iniciada."); }
  };

  /* ─────────────── Facebook ─────────────── */
  function viewFb(j) {
    F = j; const c = j.config || {}, s = j.stats || {}, P = c.programa || {}, ub = (c.ubicaciones && c.ubicaciones.length ? c.ubicaciones : [{ nombre: c.ubicacion, lat: c.lat, lng: c.lng }]), ult = (j.ejecuciones || []).slice(-1)[0];
    const FB = window.TLFB || { RAD: [], DORM: [], BANOS: [], TIPOS: [], LISTA: "#", DET: "#", url: () => "#" }, sel = (k, arr, v, lab) => `<select class="tx-in" data-f="${k}">${arr.map(x => `<option value="${esc(x)}"${String(v) === String(x) ? " selected" : ""}>${esc(lab ? lab(x) : x || "Cualquiera")}</option>`).join("")}</select>`;
    const DS = ["D", "L", "M", "X", "J", "V", "S"];
    return `<div class="eco-kpis tx-k">${kpi("Publicaciones", nf(s.total), "en la lista")}${kpi("Extraídas", nf(s.ok), "con ficha completa")}${kpi("Pendientes", nf(s.pendientes), "sin extraer")}${kpi("Con error", nf(s.errores), "se reintentan")}${kpi("Archivadas", nf(s.archivados), "")}</div>
      ${card(0, "Búsqueda en Marketplace", "Facebook bloquea al servidor: la lectura se hace en tu navegador con dos marcadores. Estos filtros arman la búsqueda que se abre en Marketplace.", `
        <div class="tx-l">Ubicaciones (la primera define la búsqueda)</div><div id="txUb">${ub.map((u, i) => `<div class="tx-r"><input class="tx-in" data-f="nombre" value="${esc(u.nombre)}" placeholder="Ciudad" style="width:170px">${num("lat", u.lat, "lat", 100)}${num("lng", u.lng, "lng", 100)}<button type="button" class="tx-x" data-tx="ubX" data-i="${i}">✕</button></div>`).join("")}</div>
        <div class="tx-row" style="margin-top:8px"><label class="tx-l">Radio (km)${sel("radio", FB.RAD, c.radio, x => x + " km")}</label><label class="tx-l">Precio mín.${num("precioMin", c.precioMin, "sin mínimo")}</label><label class="tx-l">Precio máx.${num("precioMax", c.precioMax, "sin tope")}</label><label class="tx-l">Dormitorios${sel("dormitorios", FB.DORM, c.dormitorios, x => x ? x + "+" : "Cualquiera")}</label><label class="tx-l">Baños${sel("banos", FB.BANOS, c.banos, x => x ? x + "+" : "Cualquiera")}</label><label class="tx-l">m² mín.${num("m2Min", c.m2Min, "—", 80)}</label><label class="tx-l">m² máx.${num("m2Max", c.m2Max, "—", 80)}</label></div>
        <div class="tx-l">Tipos de propiedad</div><div class="tx-hs">${FB.TIPOS.map(([id, n]) => `<button type="button" class="tx-h${(c.tipos || []).includes(id) ? " on" : ""}" data-tx="fbTipo" data-id="${id}">${ck((c.tipos || []).includes(id))}${esc(n)}</button>`).join("")}</div>
        <div class="tx-row" style="margin-top:8px"><label class="tx-l">Lote por corrida${num("lote", c.lote, "25", 80)}</label><label class="tx-l">Pausa mín. (s)${num("pausaMin", c.pausaMin, "2", 80)}</label><label class="tx-l">Pausa máx. (s)${num("pausaMax", c.pausaMax, "5", 80)}</label></div>
        <div class="tx-bs">${btn("ubAdd", "+ Añadir ubicación")}${btn("fbGuarda", "💾 Guardar filtros", "pri")}<a class="eco-btn tx-b" target="_blank" rel="noopener" href="${esc(FB.url(c))}">Abrir Marketplace con estos filtros ↗</a></div>`)}
      ${card(1, "Lista de publicaciones", "Con la búsqueda abierta en Marketplace, el marcador recorre los resultados con pausas y envía las tarjetas a TerraLab.", `
        <div class="tx-bs"><a class="tx-bm" href="${esc(FB.LISTA)}" onclick="alert('Arrastra este botón a la barra de marcadores de tu navegador. Después abre Marketplace con los filtros y tócalo desde ahí.');return false">📋 Capturar lista de Marketplace</a><span class="tx-mu">Arrástralo a la barra de marcadores.</span></div>
        <ol class="tx-st"><li>Abre Marketplace con los filtros (botón del paso anterior) e inicia sesión en Facebook.</li><li>Toca el marcador <b>Capturar lista</b>; baja los resultados solo y los envía aquí.</li><li>Vuelve y pulsa «Actualizar».</li></ol>
        <details class="tx-d"><summary>Agregar publicaciones por enlace</summary><textarea class="tx-ta" id="txUrls" rows="3" placeholder="https://www.facebook.com/marketplace/item/…"></textarea>${btn("urlsFb", "Agregar a la lista")}</details>`)}
      ${card(2, "Extraer fichas y renovar fotos", "Lee cada publicación seleccionada (fotos, descripción, ubicación y vendedor). Las fotos se guardan en Cloud Storage para que ya no caduquen.", `
        <div class="tx-bs">${btn("selPend", `Seleccionar las pendientes (${nf(s.pendientes)})`)}${btn("reext", "Re-extraer todas (renueva fotos)")}${btn("reintenta", "Reintentar las que fallaron")}${btn("fbAct", "↻ Actualizar")}</div>
        <div class="tx-bs"><a class="tx-bm" href="${esc(FB.DET)}" onclick="alert('Arrastra este botón a la barra de marcadores. Con Facebook Marketplace abierto (sesión iniciada), tócalo para extraer las fichas seleccionadas.');return false">⬇️ Extraer detalles de Marketplace</a><span class="tx-mu">${nf((F.lista || []).filter(x => x.sel && !x.arch).length)} seleccionadas</span></div>
        <div class="tx-mu">${ult ? `Última captura: ${fec(ult.at)} · ${esc(ult.tipo)}${ult.nuevas != null ? " · " + ult.nuevas + " nuevas" : ""}${ult.ok != null ? " · " + ult.ok + " fichas" : ""}` : ""}</div>`)}
      ${card(3, "Programa", "Recordatorio recurrente de cuándo toca capturar y extraer.", `
        <div class="tx-row"><label class="tx-l">Frecuencia${sel("p_tipo", ["semanal", "mensual", "bimestral"], P.tipo || "semanal", x => x[0].toUpperCase() + x.slice(1))}</label><label class="tx-l">Estado${sel("p_vig", ["Activa", "Pausada", "Cancelada"], P.vigencia || "Pausada", x => x)}</label><label class="tx-l">Inicio<input class="tx-in" data-f="p_ini" type="date" value="${esc(P.inicio || "")}"></label><label class="tx-l">Fin<input class="tx-in" data-f="p_fin" type="date" value="${esc(P.fin || "")}"></label></div>
        <div class="tx-l">Días de la semana</div><div class="tx-hs">${DS.map((d, i) => `<button type="button" class="tx-h${(P.dias_semana || []).includes(i) ? " on" : ""}" data-tx="pDs" data-i="${i}">${ck((P.dias_semana || []).includes(i))}${d}</button>`).join("")}</div>
        <label class="tx-l" style="margin-top:8px">Días del mes (separados por coma)<input class="tx-in" data-f="p_dm" value="${esc((P.dias_mes || []).join(", "))}" style="width:200px"></label>
        <label class="tx-l">Horas (0–23, separadas por coma)<input class="tx-in" data-f="p_h" value="${esc((P.horas || [11]).join(", "))}" style="width:200px"></label>
        <div class="tx-bs">${btn("progGuarda", "💾 Guardar programa", "pri")}</div>`)}`;
  }
  const fv = k => { const e = $(`[data-f="${k}"]`); return e ? e.value : ""; }, nn = k => { const v = fv(k); return v === "" ? null : +v; };
  const accFb = {
    async ubAdd() { leeUb(); F.config.ubicaciones.push({ nombre: "", lat: null, lng: null }); pintaFb(); },
    async ubX(b) { leeUb(); F.config.ubicaciones.splice(+b.dataset.i, 1); pintaFb(); },
    async fbTipo(b) { const t = F.config.tipos = F.config.tipos || [], id = b.dataset.id, i = t.indexOf(id); if (i >= 0) t.splice(i, 1); else t.push(id); leeFbForm(); pintaFb(); },
    async fbGuarda(b) { leeFbForm(); await accion(b, async () => { const r = await mapi("facebook/config", { config: F.config }); F.config = Object.assign(F.config, r.config); pintaFb(); }, "Filtros guardados."); },
    async urlsFb(b) { await accion(b, async () => { const r = await mapi("facebook/lista", { urls: $("#txUrls").value }); $("#txUrls").value = ""; await recarga(); return r; }, r => `${r.nuevas} nuevas, ${r.existentes} ya estaban.`); },
    async selPend(b) { const ids = (F.lista || []).filter(x => !x.arch && x.estado !== "ok" && !x.sel).map(x => x.id); await accion(b, async () => { await mapi("facebook/seleccion", { ids, sel: true }); await recarga(); }, `${ids.length} publicaciones seleccionadas.`); },
    async reext(b) { if (!confirm("Se marcarán todas las publicaciones ya extraídas para volver a leerlas (renueva fotos y datos). ¿Continuar?")) return; await accion(b, async () => { const r = await mapi("facebook/reextraer", { todas: true }); await recarga(); return r; }, r => `${r.marcadas} publicaciones marcadas para re-extraer.`); },
    async reintenta(b) { await accion(b, async () => { const r = await mapi("facebook/reintentar", {}); await recarga(); return r; }, r => `${r.reiniciadas} reiniciadas.`); },
    async fbAct() { await recarga(); toast("Actualizado."); },
    async pDs(b) { leeFbForm(); const P = F.config.programa = F.config.programa || {}, d = P.dias_semana = P.dias_semana || [], i = +b.dataset.i, k = d.indexOf(i); if (k >= 0) d.splice(k, 1); else d.push(i); d.sort(); pintaFb(); },
    async progGuarda(b) { leeFbForm(); const P = F.config.programa; await accion(b, async () => { const r = await mapi("facebook/programa", { programa: P }); F.config.programa = r.programa; pintaFb(); }, "Programa guardado."); }
  };
  function leeUb() { F.config.ubicaciones = $$("#txUb .tx-r").map(r => ({ nombre: $('[data-f="nombre"]', r).value.trim(), lat: $('[data-f="lat"]', r).value === "" ? null : +$('[data-f="lat"]', r).value, lng: $('[data-f="lng"]', r).value === "" ? null : +$('[data-f="lng"]', r).value })); }
  function leeFbForm() {
    const c = F.config; leeUb(); if (!$("#txUb")) return; ["radio", "precioMin", "precioMax", "m2Min", "m2Max", "lote", "pausaMin", "pausaMax"].forEach(k => { if ($(`[data-f="${k}"]`)) c[k] = nn(k); }); c.dormitorios = fv("dormitorios"); c.banos = fv("banos");
    const P = c.programa = c.programa || {}; P.tipo = fv("p_tipo") || P.tipo; P.vigencia = fv("p_vig") || P.vigencia; P.inicio = fv("p_ini"); P.fin = fv("p_fin"); P.dias_mes = fv("p_dm").split(/[,\s]+/).map(Number).filter(x => x >= 1 && x <= 31); P.horas = fv("p_h").split(/[,\s]+/).filter(x => x !== "").map(Number).filter(x => x >= 0 && x <= 23);
  }

  /* ─────────────── Mercado Libre ─────────────── */
  function viewMl(j) {
    T = j; const c = j.config || {}, s = j.stats || {}, J = j.job || {}, run = !!J.running;
    return `<div class="eco-kpis tx-k">${kpi("Publicaciones", nf(s.total), "en la lista")}${kpi("Extraídas", nf(s.ok), "con ficha")}${kpi("Pendientes", nf(s.pendiente), "por extraer")}${kpi("Seleccionadas", nf(s.seleccionadas), "para el paso 2")}${kpi("Con error", nf(s.error), "")}</div>
      ${card(0, "Búsqueda", "Ubicaciones, tipos y clasificaciones que se recorren en mercadolibre.com.mx (con pausas para no saturar el sitio).", `
        <label class="tx-l">Ubicaciones (una por línea: «Ciudad, Estado»)<textarea class="tx-ta" id="txUbm" rows="3" placeholder="Saltillo, Coahuila">${esc((c.ubicaciones || []).map(u => u.nombre).join("\n"))}</textarea></label>
        <div class="tx-l">Operación</div><div class="tx-hs">${(j.tipos || []).map(t => `<button type="button" class="tx-h${(c.tipos || []).includes(t.id) ? " on" : ""}" data-tx="mlTipo" data-id="${t.id}">${ck((c.tipos || []).includes(t.id))}${esc(t.nombre)}</button>`).join("")}</div>
        <div class="tx-l">Clasificación del inmueble</div><div class="tx-hs">${(j.clases || []).map(t => `<button type="button" class="tx-h${(c.clases || []).includes(t.id) ? " on" : ""}" data-tx="mlClase" data-id="${t.id}">${ck((c.clases || []).includes(t.id))}${esc(t.nombre)}</button>`).join("")}</div>
        <div class="tx-row" style="margin-top:8px"><label class="tx-l">Pausa mín. (s)${num("pausaMin", c.pausaMin, "1.5", 80)}</label><label class="tx-l">Pausa máx. (s)${num("pausaMax", c.pausaMax, "3.5", 80)}</label><label class="tx-l">Máx. páginas por ruta${num("maxPaginas", c.maxPaginas, "42", 80)}</label></div>
        <div class="tx-bs">${btn("mlGuarda", "💾 Guardar búsqueda", "pri")}</div>`)}
      ${card(1, "Buscar publicaciones", "Recorre las páginas de resultados y agrega a la lista las publicaciones nuevas (actualiza precio y datos de las que ya estaban).", `
        <div class="tx-bs">${btn("mlBusca", run && J.tipo === "lista" ? "⏳ Buscando…" : "🔎 Buscar publicaciones", "pri", run)}${run ? btn("mlDet", "⛔ Detener", "mal") : ""}</div>`)}
      ${card(2, "Extraer fichas", "Lee la ficha de cada publicación seleccionada (superficie, ubicación, fotos, descripción y vendedor).", `
        <div class="tx-bs">${btn("mlSelPend", `Seleccionar las pendientes`)}${btn("mlSelTodas", "Seleccionar todas (re-extraer)")}${btn("mlSelNada", "Quitar selección")}${btn("mlExtrae", run && J.tipo === "extraer" ? "⏳ Extrayendo…" : "🔍 Extraer seleccionadas", "pri", run)}</div>`)}
      <div id="txJob">${jobMl(J)}</div>`;
  }
  const jobMl = J => J && J.running ? bar(J.total ? 100 * J.hechos / J.total : 0, `${esc(J.paso || "")} · ${nf(J.hechos)} de ${nf(J.total)}${J.leidas != null ? " · leídas " + nf(J.leidas) : ""}${J.nuevas != null ? " · nuevas " + nf(J.nuevas) : ""}${J.ok != null ? " · bien " + nf(J.ok) : ""}${J.err ? ` · <b class="tx-er">${nf(J.err)} con error</b>` : ""}`) : (J && J.fin ? `<div class="tx-bt">Último proceso (${esc(J.tipo === "lista" ? "búsqueda" : "extracción")}): ${esc(J.paso || "")} · ${fec(J.fin)}${J.error ? ` · <b class="tx-er">${esc(J.error)}</b>` : ""}${J.ok != null ? ` · ${nf(J.ok)} bien` : ""}${J.err ? ` · ${nf(J.err)} con error` : ""}${J.nuevas != null ? ` · ${nf(J.nuevas)} nuevas` : ""}</div>` : "");
  const leeMl = () => { const c = T.config; c.ubicaciones = $("#txUbm").value.split(/\n/).map(x => x.trim()).filter(Boolean).map(nombre => ({ nombre })); ["pausaMin", "pausaMax", "maxPaginas"].forEach(k => { c[k] = nn(k); }); };
  const accMl = {
    async mlTipo(b) { leeMl(); const t = T.config.tipos, i = t.indexOf(b.dataset.id); if (i >= 0) t.splice(i, 1); else t.push(b.dataset.id); pintaMl(); },
    async mlClase(b) { leeMl(); const t = T.config.clases, i = t.indexOf(b.dataset.id); if (i >= 0) t.splice(i, 1); else t.push(b.dataset.id); pintaMl(); },
    async mlGuarda(b) { leeMl(); await accion(b, async () => { for (const u of T.config.ubicaciones) await mapi("mercadolibre/ubicacion", { texto: u.nombre }); const r = await mapi("mercadolibre/config", { config: T.config }); T.config = r.config; pintaMl(); }, "Búsqueda guardada."); },
    async mlBusca(b) { await accion(b, async () => { await mapi("mercadolibre/buscar", {}); await recargaMl(); }, "Búsqueda iniciada."); },
    async mlDet(b) { await accion(b, async () => { await mapi("mercadolibre/detener", {}); await recargaMl(); }, "Deteniendo…"); },
    async mlSelPend(b) { const ids = (T.lista || []).filter(x => !x.arch && x.estado !== "ok").map(x => x.id); await accion(b, async () => { await mapi("mercadolibre/seleccion", { ids, sel: true }); await recargaMl(); }, `${ids.length} seleccionadas.`); },
    async mlSelTodas(b) { const ids = (T.lista || []).filter(x => !x.arch).map(x => x.id); await accion(b, async () => { await mapi("mercadolibre/seleccion", { ids, sel: true }); await recargaMl(); }, `${ids.length} seleccionadas.`); },
    async mlSelNada(b) { const ids = (T.lista || []).filter(x => x.sel).map(x => x.id); await accion(b, async () => { await mapi("mercadolibre/seleccion", { ids, sel: false }); await recargaMl(); }, "Selección quitada."); },
    async mlExtrae(b) { await accion(b, async () => { await mapi("mercadolibre/extraer", {}); await recargaMl(); }, "Extracción iniciada."); }
  };


  /* ─────────────── Apify (Inmuebles24 · Vivanuncios) ─────────────── */
  const usd = v => v == null || !isFinite(v) ? "—" : "US$ " + Number(v).toLocaleString("es-MX", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const FUE = { i24: "Inmuebles24", viva: "Vivanuncios" };
  const chip = (a, id, on, t) => `<button type="button" class="tx-h${on ? " on" : ""}" data-tx="${a}" data-id="${esc(id)}">${ck(on)}${esc(t)}</button>`;
  function viewAp(j) {
    A = j; const c = j.config || {}, ac = j.actores || {}, run = (j.corridas || []).some(x => !x.fin);
    const tok = j.token ? `<div class="tx-bt">✓ Token de Apify guardado.</div><div class="tx-bs">${btn("apTokCambia", "Cambiar token")}</div>` : `<label class="tx-l">Token de API de Apify (apify.com › Settings › API & Integrations)<input class="tx-in" id="apTok" type="password" placeholder="apify_api_…" style="width:min(460px,100%)" autocomplete="off"></label><div class="tx-bs">${btn("apTokGuarda", "💾 Guardar token", "pri")}</div><p class="tx-nt">Se guarda en el servidor y no vuelve a mostrarse. Se usa solo para lanzar las corridas de estos dos actores.</p>`;
    const E = EST, estH = !E ? "" : `<div class="tx-tbw"><table class="tx-tb"><thead><tr><th>Fuente</th><th>Búsqueda</th><th>Publicaciones</th></tr></thead><tbody>${E.filas.map(x => `<tr><td>${esc(FUE[x.f])}</td><td title="${esc(x.url)}">${esc(x.et)}${x.error ? ` <b class="tx-er">${esc(x.error)}</b>` : ""}</td><td>${x.total != null ? nf(x.total) + (x.manual ? " (manual)" : "") : `<input class="tx-in" data-man="${esc(x.url)}" type="number" placeholder="no se pudo contar" style="width:130px">`}</td></tr>`).join("")}</tbody></table></div>
      <div class="eco-kpis tx-k">${E.resumen.map(r => kpi(esc(r.nombre), nf(r.publicaciones), "publicaciones · " + usd(r.costo))).join("")}${kpi("Total estimado", nf(E.total), usd(E.costo))}</div>
      ${E.sinConteo ? `<p class="tx-nt">⚠️ ${nf(E.sinConteo)} búsqueda(s) sin conteo: el portal no mostró el total. Escribe el número a mano y vuelve a estimar, o extrae con un tope.</p>` : ""}<p class="tx-nt">ℹ️ ${esc(E.nota)} Vivanuncios cobra US$ ${ac.viva ? ac.viva.item : 0.02} por publicación (plan gratuito de Apify); Inmuebles24 US$ ${ac.i24 ? ac.i24.item : 0.001} por publicación${c.contactos ? " + US$ 0.01 por contacto" : ""} y US$ ${ac.i24 ? ac.i24.start : 0.005} por arranque.</p>
      <div class="tx-row">${E.resumen.map(r => `<label class="tx-l">Traer hasta (${esc(r.nombre)})${num("lim_" + r.f, Math.ceil(r.publicaciones * 1.05), "", 110)}</label>`).join("")}</div>`;
    const cor = (j.corridas || []).map(x => `<tr><td>${esc(FUE[x.f] || x.f)}</td><td>${fec(x.inicio)}</td><td>${esc(x.estado || "")}${x.error ? ` <b class="tx-er">${esc(x.error)}</b>` : ""}</td><td>${nf(x.importado)}</td><td>${x.costo != null ? usd(x.costo) : "—"} <small>(tope ${usd(x.costoMax)})</small></td></tr>`).join("");
    return `<div class="eco-kpis tx-k">${kpi("Publicaciones guardadas", nf(j.total), "de Apify")}${Object.keys(j.por || {}).map(f => kpi(esc(FUE[f] || f), nf(j.por[f]), "")).join("")}${kpi("Token", j.token ? "✓" : "—", j.token ? "guardado" : "falta")}</div>
      ${card(0, "Cuenta de Apify", "Los actores memo23 (Inmuebles24) y scrapers_lat (Vivanuncios) consultan el portal en vivo cada vez que corren y se cobran por publicación a tu cuenta de Apify.", tok)}
      ${card(1, "Búsqueda", "Ciudades, operación y tipos de propiedad. Se arma una búsqueda por ciudad y tipo en Inmuebles24; en Vivanuncios pega las URLs de búsqueda del sitio.", `
        <label class="tx-l">Ciudades / municipios (una por línea)<textarea class="tx-ta" id="apCiu" rows="4" placeholder="Monterrey">${esc((c.ciudades || []).join("\n"))}</textarea></label>
        <div class="tx-l">Operación</div><div class="tx-hs">${chip("apOp", "venta", c.operacion === "venta", "Venta")}${chip("apOp", "renta", c.operacion === "renta", "Renta")}</div>
        <div class="tx-l">Tipo de propiedad</div><div class="tx-hs">${(j.tipos || []).map(t => chip("apTipo", t.id, (c.tipos || []).includes(t.id), t.nombre)).join("")}</div>
        <div class="tx-l">Fuentes</div><div class="tx-hs">${chip("apFte", "i24", (c.fuentes || []).includes("i24"), "Inmuebles24 (memo23)")}${chip("apFte", "viva", (c.fuentes || []).includes("viva"), "Vivanuncios (scrapers_lat)")}</div>
        <label class="tx-l">URLs propias de Inmuebles24 (opcional, una por línea)<textarea class="tx-ta" id="apUi24" rows="2">${esc(((c.urlsExtra || {}).i24 || []).join("\n"))}</textarea></label>
        <label class="tx-l">URLs de búsqueda de Vivanuncios (una por línea; el sitio usa códigos, copia la URL tras filtrar por ciudad y tipo)<textarea class="tx-ta" id="apUviva" rows="3">${esc(((c.urlsExtra || {}).viva || []).join("\n"))}</textarea></label>
        <div class="tx-hs">${chip("apDet", "d", c.detalles, "Detalles completos (descripción, teléfono, coordenadas)")}${chip("apCon", "c", c.contactos, "Contactos de Inmuebles24 (+US$ 0.01 c/u)")}</div>
        <div class="tx-row" style="margin-top:8px"><label class="tx-l">Presupuesto máximo por fuente (US$)${num("presupuesto", c.presupuesto, "10", 90)}</label></div>
        <p class="tx-nt">Búsquedas que se ejecutarán: <b>${nf((j.plan || []).length)}</b>.</p>
        <div class="tx-bs">${btn("apGuarda", "💾 Guardar búsqueda", "pri")}</div>`)}
      ${card(2, "Estimar antes de extraer", "Cuenta cuántas publicaciones muestra cada búsqueda en el portal y calcula el costo. No descarga publicaciones.", `<div class="tx-bs">${btn("apEstima", "🧮 Estimar publicaciones y costo", "pri", !j.token)}</div>${estH}`)}
      ${card(3, "Extraer", "Lanza la corrida en Apify con un tope de publicaciones y de costo, y guarda los resultados en la base de TerraLab.", `<div class="tx-bs">${btn("apExtrae", run ? "⏳ En curso…" : "🕷️ Extraer", "pri", run || !EST)}${run ? btn("apDet2", "⛔ Detener", "mal") : ""}</div><p class="tx-nt">${EST ? "Se traerá hasta el número indicado arriba." : "Primero estima para fijar el tope."}</p>
        <div id="txJob">${cor ? `<div class="tx-tbw"><table class="tx-tb"><thead><tr><th>Fuente</th><th>Inicio</th><th>Estado</th><th>Importadas</th><th>Costo</th></tr></thead><tbody>${cor}</tbody></table></div>` : ""}</div>`)}
      ${Object.keys(j.muestra || {}).length ? card(4, "Muestra de lo recibido", "Primer registro tal como lo entrega cada actor (sirve para ver su estructura real).", Object.keys(j.muestra).map(f => `<details class="tx-d"><summary>${esc(FUE[f] || f)}</summary><pre class="tx-pre">${esc(JSON.stringify((j.muestra[f] || [])[0] || {}, null, 2).slice(0, 6000))}</pre></details>`).join("")) : ""}`;
  }
  const leeAp = () => { if (!$("#apCiu")) return; const c = A.config; c.ciudades = $("#apCiu").value.split(/\n/).map(x => x.trim()).filter(Boolean); c.urlsExtra = { i24: $("#apUi24").value.split(/\n/).map(x => x.trim()).filter(Boolean), viva: $("#apUviva").value.split(/\n/).map(x => x.trim()).filter(Boolean) }; if ($('[data-f="presupuesto"]')) c.presupuesto = nn("presupuesto") || 10; };
  const alt = (arr, id) => { const i = arr.indexOf(id); if (i >= 0) arr.splice(i, 1); else arr.push(id); };
  const accAp = {
    async apTokCambia() { A.token = false; pinta(); },
    async apTokGuarda(b) { await accion(b, async () => { await mapi("apify/token", { token: $("#apTok").value }); await recargaAp(); }, "Token guardado."); },
    async apOp(b) { leeAp(); A.config.operacion = b.dataset.id; EST = null; pinta(); },
    async apTipo(b) { leeAp(); alt(A.config.tipos, b.dataset.id); EST = null; pinta(); },
    async apFte(b) { leeAp(); alt(A.config.fuentes, b.dataset.id); EST = null; pinta(); },
    async apDet() { leeAp(); A.config.detalles = !A.config.detalles; pinta(); },
    async apCon() { leeAp(); A.config.contactos = !A.config.contactos; EST = null; pinta(); },
    async apGuarda(b) { leeAp(); await accion(b, async () => { const r = await mapi("apify/config", { config: A.config }); A.config = r.config; A.plan = r.plan; EST = null; pinta(); }, "Búsqueda guardada."); },
    async apEstima(b) { leeAp(); const manual = {}; $$("[data-man]").forEach(i => { if (i.value !== "") manual[i.dataset.man] = +i.value; }); await accion(b, async () => { await mapi("apify/config", { config: A.config }); EST = await mapi("apify/estimar", { manual }); pinta(); }, r => `Estimado: ${nf(r.total)} publicaciones · ${usd(r.costo)}`); },
    async apExtrae(b) { const limites = {}; (EST.resumen || []).forEach(r => { limites[r.f] = nn("lim_" + r.f) || r.publicaciones; }); const tot = Object.values(limites).reduce((a, x) => a + x, 0); if (!confirm(`Se lanzará la extracción de hasta ${nf(tot)} publicaciones (costo estimado ${usd(EST.costo)}) en tu cuenta de Apify. ¿Continuar?`)) return; await accion(b, async () => { await mapi("apify/extraer", { limites }); await recargaAp(); }, "Extracción iniciada en Apify."); },
    async apDet2(b) { await accion(b, async () => { await mapi("apify/detener", {}); await recargaAp(); }, "Deteniendo…"); }
  };
  async function recargaAp() { const j = await mapi("apify"); A = j; pinta(); }

  /* ─────────────── APIs externas (muestras) ─────────────── */
  const MAPA = [["Precio", "price / priceAmount / Precio"], ["Tipo de propiedad", "propertyType"], ["Operación", "operation"], ["Recámaras / baños", "bedrooms / bathrooms"], ["Superficie", "totalArea / coveredArea / totalAreaM2"], ["Ubicación", "latitude, longitude, city, state/province, colonia"], ["Fotos", "images / imageUrls"], ["Publicante", "publisherName / advertiserName / agencyName"]];
  function viewApis() {
    const L = window.TLAPIS || [];
    return `<p class="tx-sub" style="max-width:820px">Estructura de cada proveedor según lo que publica en su página (no son datos descargados ni de tu cuenta). Sirve para decidir de cuál traer información y cómo encajaría en las bases de TerraLab.</p>` + L.map(a => `<section class="tx-card" style="border-top:3px solid ${a.c}"><h3>${esc(a.n)}</h3>
      <div class="tx-ap"><span class="tl-t" style="--c:${a.c}">${esc(a.tipo)}</span><span class="tl-t">${esc(a.tam)}</span><a class="tx-lk" href="${esc(a.url)}" target="_blank" rel="noopener">Abrir su página ↗</a></div>
      <ul class="tx-ul"><li><b>Costo:</b> ${esc(a.costo)}</li><li><b>Acceso:</b> ${esc(a.acceso)}</li><li><b>Entrega:</b> ${esc(a.entrega)}</li></ul>
      ${a.campos.length ? `<details class="tx-d" open><summary>Campos (${a.campos.length})</summary><div class="tx-fl">${a.campos.map(([k, t]) => `<span><code>${esc(k)}</code><em>${esc(t)}</em></span>`).join("")}</div></details>` : ""}
      ${a.ej ? `<details class="tx-d"><summary>Registro de ejemplo (JSON)</summary><pre class="tx-pre">${esc(JSON.stringify(a.ej, null, 2))}</pre></details>` : ""}
      <p class="tx-nt">ℹ️ ${esc(a.nota)}</p></section>`).join("") +
      `<section class="tx-card"><h3>Cómo encajan en TerraLab</h3><p class="tx-sub">Equivalencias entre los campos de estos proveedores y los de las bases de TerraLab.</p><table class="tx-tb"><thead><tr><th>Dato en TerraLab</th><th>Campo(s) en los proveedores</th></tr></thead><tbody>${MAPA.map(([a, b]) => `<tr><td>${esc(a)}</td><td><code>${esc(b)}</code></td></tr>`).join("")}</tbody></table></section>`;
  }
  /* ─────────────── cascarón común ─────────────── */
  const CFG = {
    apis: { t: "APIs externas (muestras)", ic: "🔌", c: "#22d3ee", url: null, view: viewApis, acc: {}, set: () => { } },
    airbnb: { t: "Airbnb", ic: "🏠", c: "#f43f5e", url: "airbnb", view: viewAir, acc: accAir, set: j => { M = j; } },
    facebook: { t: "Facebook Marketplace", ic: "📣", c: "#3b82f6", url: "facebook", view: viewFb, acc: accFb, set: j => { F = j; } },
    apify: { t: "Apify · Inmuebles24 y Vivanuncios", ic: "🕷️", c: "#a78bfa", url: "apify", view: viewAp, acc: accAp, set: j => { A = j; } },
    mercadolibre: { t: "Mercado Libre", ic: "🛒", c: "#eab308", url: "mercadolibre", view: viewMl, acc: accMl, set: j => { T = j; } }
  };
  let cur = null, root = null;
  function pintaAir() { pinta(); } function pintaFb() { pinta(); } function pintaMl() { pinta(); }
  function pinta() { const C = CFG[cur], d = cur === "airbnb" ? M : cur === "facebook" ? F : cur === "apify" ? A : T, z = $("#txBody", root); if (!z || !d) return; const y = root.scrollTop; z.innerHTML = C.view(d); root.scrollTop = y; if (cur === "mercadolibre") poll(); else poll(); }
  async function recarga() { const j = await mapi(CFG[cur].url); CFG[cur].set(j); pinta(); }
  async function recargaMl() { const j = await mapi("mercadolibre"); T = j; pinta(); }
  function poll() {
    clearInterval(timer); const d = cur === "airbnb" ? M : cur === "facebook" ? F : cur === "apify" ? A : T; if (!d) return;
    if (cur === "apify") { if (!(d.corridas || []).some(x => !x.fin)) return; timer = setInterval(async () => { if (!$("#txBody") || cur !== "apify") { clearInterval(timer); return; } try { const e = await mapi("apify/estado"); const antes = (A.corridas || []).some(x => !x.fin); A.corridas = e.corridas; A.total = e.total; if (antes && !e.running) { await recargaAp(); return; } const z = $("#txJob"); if (z) pinta(); } catch (_) { } }, 7000); return; }
    const run = cur === "airbnb" ? ((d.job && d.job.running) || (d.busqueda && d.busqueda.running)) : cur === "mercadolibre" ? (d.job && d.job.running) : false; if (!run) return;
    timer = setInterval(async () => { if (!$("#txBody")) { clearInterval(timer); return; } try { if (cur === "mercadolibre") { const e = await mapi("mercadolibre/estado"); const antes = T.job && T.job.running; T.job = e; if (antes && !e.running) { await recargaMl(); return; } const z = $("#txJob"); if (z) z.innerHTML = jobMl(e); } else { const j = await mapi("airbnb"), antes = (M.job && M.job.running) || (M.busqueda && M.busqueda.running), ahora = (j.job && j.job.running) || (j.busqueda && j.busqueda.running); if (antes && !ahora) { M = j; pinta(); } else { M = j; const jb = $("#txJob"); if (jb && j.job) jb.innerHTML = j.job.running ? bar(j.job.total ? 100 * j.job.done / j.job.total : 0, `${nf(j.job.done)} de ${nf(j.job.total)}${j.job.actual ? " · publicación " + esc(j.job.actual) : ""}`) : ""; const bb = $("#txBus"); if (bb && j.busqueda && j.busqueda.running) bb.innerHTML = bar(j.busqueda.rangos ? 100 * j.busqueda.rango / j.busqueda.rangos : 0, `Rango ${nf(j.busqueda.rango)} de ${nf(j.busqueda.rangos)} · ${nf(j.busqueda.encontrados)} publicaciones encontradas`); } } } catch (_) { } }, 5000);
  }
  window.TLX = {
    async abre(f, el) {
      cur = f; root = el; clearInterval(timer); const C = CFG[f]; M = F = T = A = null; EST = null;
      el.innerHTML = `<div class="tl-hd"><div><h1 class="tl-h1">${C.ic} ${f === "apis" ? esc(C.t) : "Extracción · " + esc(C.t)}</h1><div class="eco-crumb">${f === "apis" ? "Proveedores externos de datos inmobiliarios y la forma de sus datos." : "Buscar, extraer y programar las publicaciones de esta fuente. Se guardan en la misma base que usa la consulta."}</div></div></div><div class="tx-msg" id="txMsg"></div><div id="txBody"><div class="eco-nada">Cargando…</div></div>`;
      el.onclick = async e => { const b = e.target.closest("[data-tx]"); if (!b || !el.contains(b)) return; const fn = C.acc[b.dataset.tx]; if (fn) { e.stopPropagation(); try { await fn(b); } catch (er) { toast(er.message, true); } } };
      try { if (!C.url) { $("#txBody").innerHTML = viewApis(); return; } const j = await mapi(C.url); if (cur !== f) return; C.set(j); pinta(); } catch (e) { $("#txBody").innerHTML = `<div class="eco-nada">${esc(e.message)}</div>`; }
    },
    para() { clearInterval(timer); cur = null; }
  };
})();
