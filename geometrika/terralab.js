/* TerraLab · laboratorio de bienes raíces: consulta de las bases de datos de publicaciones (Airbnb, Facebook Marketplace, Mercado Libre)
   con mapa, tarjetas enlazadas y ficha de detalle en la columna lateral derecha. Misma estética que EconoLab (usa ecolab.css + terralab.css).
   Se inicia con window.terralabInit(). Lee de /geometrika/modelab/inmuebles/* (mismas bases que usa Model Lab). */
(function () {
  "use strict";
  const API = "https://api.check-inn.mx", RT = "/geometrika/modelab/inmuebles";
  const $ = (s, r) => (r || document).querySelector(s), $$ = (s, r) => [...(r || document).querySelectorAll(s)];
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const norm = s => String(s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
  const ls = { get(k, d) { try { const v = localStorage.getItem(k); return v == null ? d : v; } catch (_) { return d; } }, set(k, v) { try { localStorage.setItem(k, v); } catch (_) { } } };
  const nf = (v, d) => v == null || !isFinite(v) ? "—" : v.toLocaleString("es-MX", { minimumFractionDigits: d || 0, maximumFractionDigits: d || 0 });
  const money = v => v == null ? "Sin precio" : "$" + nf(v);
  const med = a => { const s = a.filter(x => x != null && isFinite(x)).sort((x, y) => x - y); if (!s.length) return null; const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
  const token = () => { try { const g = JSON.parse(localStorage.getItem("gmTok") || "null"); return g && g.token ? g.token : ""; } catch (_) { return ""; } };
  async function api(ruta, cuerpo) {
    const o = { method: cuerpo ? "POST" : "GET", headers: { "x-gm-token": token() } }; if (cuerpo) { o.headers["Content-Type"] = "application/json"; o.body = JSON.stringify(cuerpo); }
    const r = await fetch(API + ruta, o); let j = null; try { j = await r.json(); } catch (_) { }
    if (r.status === 401) throw new Error("Tu sesión expiró. Vuelve a entrar con tu contraseña.");
    if (!r.ok || !j || j.ok === false) throw new Error((j && j.error) || "No se pudo completar la operación (" + r.status + ").");
    return j;
  }
  const FUENTES = [
    { id: "airbnb", n: "Airbnb", ic: "🏠", c: "#f43f5e", u: "por noche", d: "Alojamientos de corta estancia con calificación, capacidad y ocupación." },
    { id: "facebook", n: "Facebook Marketplace", ic: "📣", c: "#3b82f6", u: "MXN", d: "Publicaciones de renta y venta estructuradas desde Marketplace." },
    { id: "mercadolibre", n: "Mercado Libre", ic: "🛒", c: "#eab308", u: "MXN", d: "Inmuebles en venta y renta con superficie, recámaras y ubicación." }
  ];
  const FBY = Object.fromEntries(FUENTES.map(f => [f.id, f]));
  const PAL = ["#22d3ee", "#a78bfa", "#f472b6", "#fbbf24", "#34d399", "#60a5fa", "#fb7185", "#f97316", "#94a3b8"];
  const TIPOS_ORD = ["alojamiento entero", "habitación", "cabaña o campestre", "zona de acampar", "alojamiento único", "departamento", "casa", "terreno", "local", "oficina", "bodega", "edificio", "quinta", "cuarto", "otro"];
  /* Airbnb publica cientos de variantes de «tipo» (p. ej. «habitación en hotel en Santiago»): se agrupan en pocos tipos generales */
  const tipoGen = (f, t) => { t = String(t || "").toLowerCase(); if (f !== "airbnb") return t || "otro"; if (/habitaci[oó]n|bed and breakfast|cuarto/.test(t)) return "habitación"; if (/acampar|tienda|campamento|camping/.test(t)) return "zona de acampar"; if (/caba[nñ]a|rancho|granja|finca|quinta|campestre|bosque|chalet/.test(t)) return "cabaña o campestre"; if (/tipi|domo|minicasa|casa rodante|granero|contenedor|casa del [aá]rbol|barco|torre|molino|cueva|[ií]gloo|yurta/.test(t)) return "alojamiento único"; if (/alojamiento entero|departamento|casa|vivienda|loft|villa|estudio|condominio|apartamento/.test(t)) return "alojamiento entero"; return "otro"; };
  const colTipo = t => PAL[Math.max(0, TIPOS_ORD.indexOf(String(t || "otro")))] || PAL[8];
  const S = { sel: "home", datos: {}, cargando: {}, f: { q: "", op: "", tipos: [], ciudad: "", pmin: "", pmax: "", rec: "", ord: "rel" }, lista: [], n: 40, foco: null, fotos: {}, ficha: null, mapa: null, capa: null, mk: {}, fk: 0 };
  let host;

  /* ───────── datos ───────── */
  function aObjetos(f, j) {
    const cols = j.cols, g = n => cols.indexOf(n), pick = (r, ...ns) => { for (const n of ns) { const i = g(n); if (i >= 0 && r[i] != null && r[i] !== "" && r[i] !== 0) return r[i]; } return null; };
    return j.rows.map(r => {
      const o = { f, id: String(r[g("ID")]), titulo: r[g("Título")] || "", tipoOrig: String(r[g("Tipo")] || ""), tipo: tipoGen(f, r[g("Tipo")]), op: String(r[g("Operación")] || "").toLowerCase(), precio: r[g("Precio")], ciudad: r[g("Ciudad")] || "", estado: r[g("Estado")] || "", colonia: g("Colonia") >= 0 ? r[g("Colonia")] || "" : "", lat: r[g("Latitud")], lng: r[g("Longitud")], url: r[g("URL")] || "" };
      if (f === "airbnb") { o.rec = r[g("Habitaciones")]; o.ban = r[g("Baños")]; o.hu = r[g("Huéspedes")]; o.cal = r[g("Calificación")]; o.ev = r[g("Evaluaciones")]; o.sup = r[g("Superanfitrión")]; o.ocu = r[g("Ocupación a 90 días")]; o.m2 = null; }
      else { o.rec = r[g("Recámaras")]; o.ban = r[g("Baños")]; o.m2 = pick(r, "m² construcción", "m² totales", "m² terreno"); o.est = g("Estacionamientos") >= 0 ? r[g("Estacionamientos")] : null; o.pub = r[g("Publicada")] || r[g("Publicado")] || ""; }
      o.pm2 = o.precio != null && o.m2 > 0 && o.op !== "noche" ? o.precio / o.m2 : null;
      return o;
    });
  }
  async function carga(f) {
    if (S.datos[f]) return S.datos[f]; if (S.cargando[f]) return S.cargando[f];
    return (S.cargando[f] = api(RT + "/" + f).then(j => { const rows = aObjetos(f, j); S.datos[f] = { rows, total: j.total, tipos: [...new Set(rows.map(r => r.tipo))].sort((a, b) => TIPOS_ORD.indexOf(a) - TIPOS_ORD.indexOf(b)), ciudades: [...new Set(rows.map(r => r.ciudad).filter(Boolean))].sort((a, b) => a.localeCompare(b, "es")), ops: [...new Set(rows.map(r => r.op).filter(Boolean))] }; delete S.cargando[f]; return S.datos[f]; }).catch(e => { delete S.cargando[f]; throw e; }));
  }
  const px = u => /^https:\/\/api\.check-inn\.mx\//.test(u) ? u : API + RT + "/img?t=" + encodeURIComponent(token()) + "&u=" + encodeURIComponent(u);
  const mini = u => /mlstatic\.com/.test(u) ? u.replace(/-[A-Z]\.(jpg|webp)/i, "-O.$1") : u;
  const nfTxt = u => u && /fbcdn\.net/.test(u) ? "Foto caducada en Facebook" : "Sin foto";
  const D = () => S.datos[S.sel];

  /* ───────── cascarón (misma estructura y clases que EconoLab) ───────── */
  function shell() {
    host.innerHTML = `<div class="eco-app tl-app" id="tlApp">
      <header class="eco-top"><button class="eco-burger" id="tlBurger" type="button" aria-label="Menú">☰</button>
        <div class="eco-logo"><img src="terralab-logo.png?v=2" alt="TerraLab" height="46"></div>
        <div class="eco-brand"><span>Laboratorio inmobiliario · bases de datos de publicaciones con mapa y fichas</span></div><div class="eco-sp"></div>
        <span class="eco-chip" id="tlChip" title="Publicaciones disponibles en las bases">…</span>
        <button class="eco-theme" id="tlTema" type="button" aria-label="Cambiar tema"></button><button class="eco-btn" id="tlCambia" type="button">Cambiar de sistema</button><button class="eco-btn" id="tlSalir" type="button">Salir</button></header>
      <div class="eco-body"><aside class="eco-side"><div class="eco-srch"><input id="tlQ" type="search" placeholder="Buscar título, colonia o ciudad…" autocomplete="off"></div><div class="eco-tree" id="tlTree"></div><div class="eco-sf">Fuente: bases descargadas en Check Inn</div></aside>
      <main class="eco-main tl-main" id="tlMain"></main></div><div class="eco-ov" id="tlOv"></div></div>`;
    $("#tlBurger", host).onclick = () => $("#tlApp").classList.toggle("eco-menu"); $("#tlOv", host).onclick = () => $("#tlApp").classList.remove("eco-menu");
    $("#tlSalir", host).onclick = () => window.DL_SALIR && window.DL_SALIR(); $("#tlCambia", host).onclick = () => window.DL_CAMBIAR && window.DL_CAMBIAR();
    const tema = c => { $("#tlApp").classList.toggle("eco-light", c === "claro"); $("#tlTema").textContent = c === "claro" ? "🌙 Oscuro" : "☀️ Claro"; ls.set("eco_tema", c); if (S.mapa) setTimeout(() => S.mapa.invalidateSize(), 50); };
    tema(ls.get("eco_tema", "oscuro")); $("#tlTema", host).onclick = () => tema($("#tlApp").classList.contains("eco-light") ? "oscuro" : "claro");
    $("#tlQ", host).oninput = e => { S.f.q = e.target.value; if (S.sel !== "home") aplica(); };
    host.addEventListener("click", clic);
    document.addEventListener("keydown", e => { if (e.key === "Escape" && S.ficha) cierraFicha(); else if (S.ficha && (e.key === "ArrowRight" || e.key === "ArrowLeft") && !/INPUT|SELECT|TEXTAREA/.test((e.target || {}).tagName || "")) reelIr(e.key === "ArrowRight" ? 1 : -1); });
  }
  function arbol() {
    const t = $("#tlTree"); if (!t) return;
    const bd = f => S.datos[f] ? `<span class="bd">${nf(S.datos[f].rows.length)}</span>` : "";
    t.innerHTML = `<button type="button" class="eco-n eco-n0${S.sel === "home" ? " on" : ""}" data-s="home"><span class="ic">📊</span><span class="tx">Resumen de las bases</span></button>
      <div class="tl-gt">Bases de datos</div>` + FUENTES.map(f => `<button type="button" class="eco-n eco-n2${S.sel === f.id ? " on" : ""}" data-s="${f.id}" style="--c:${f.c}"><span class="nm"><i class="tl-dot" style="background:${f.c}"></i>${esc(f.n)}</span>${bd(f.id)}</button>`).join("") +
      `<div class="tl-gt">Extracción y programación</div>` + FUENTES.map(f => `<button type="button" class="eco-n eco-n2${S.sel === "x:" + f.id ? " on" : ""}" data-x="${f.id}" style="--c:${f.c}"><span class="nm"><i class="tl-dot" style="background:${f.c}"></i>${esc(f.n)}</span></button>`).join("") +
      `<div class="tl-gt">Próximas secciones</div><div class="tl-soon">📅 Ocupación y calendario Airbnb<br>🌆 Entorno urbano y competencia</div><p class="tl-soonp">Hoy se gestionan en check-inn.mx › Investigación de mercados; se migran a TerraLab por fases.</p>`;
  }

  /* ───────── resumen ───────── */
  async function home() {
    if (window.TLX) window.TLX.para(); S.sel = "home"; arbol(); const m = $("#tlMain"); if (S.mapa) { try { S.mapa.remove(); } catch (_) { } S.mapa = null; }
    m.className = "eco-main tl-main"; m.innerHTML = `<div class="eco-h"><div><h1 class="tl-h1">Bases de datos inmobiliarias</h1><div class="eco-crumb">Consulta las publicaciones descargadas: elige una base para verla en mapa, tarjetas y fichas.</div></div></div><div class="tl-hg" id="tlHg"><div class="eco-nada">Cargando las bases…</div></div>`;
    const res = await Promise.all(FUENTES.map(f => carga(f.id).then(d => [f, d]).catch(e => [f, { err: e.message }])));
    $("#tlChip").textContent = nf(res.reduce((a, [, d]) => a + (d.rows ? d.rows.length : 0), 0)) + " publicaciones"; arbol();
    if (S.sel !== "home") return;
    $("#tlHg").innerHTML = res.map(([f, d]) => {
      if (d.err) return `<div class="tl-fc" style="--c:${f.c}"><h3>${f.ic} ${esc(f.n)}</h3><p class="tl-err">${esc(d.err)}</p></div>`;
      const r = d.rows, conP = r.filter(x => x.precio != null), mP = med(conP.map(x => x.precio)), mM = med(r.map(x => x.pm2)), cnt = {}; r.forEach(x => { cnt[x.tipo] = (cnt[x.tipo] || 0) + 1; });
      const grupo = (campo, n) => { const c = {}; r.forEach(x => { const k = x[campo] || "Sin dato"; c[k] = (c[k] || 0) + 1; }); const t = Object.entries(c).sort((p, q) => q[1] - p[1]), top = t.slice(0, n), resto = t.slice(n).reduce((acc, [, v]) => acc + v, 0); if (resto) top.push(["Otras (" + (t.length - n) + ")", resto]); return top; };
      const barras = (titulo, top, col) => { const mx = top.length ? Math.max(...top.map(x => x[1])) : 1; return `<div class="tl-bt">${titulo}</div><div class="tl-bars">${top.map(([t, n]) => `<div class="tl-bar"><span title="${esc(t)}">${esc(t)}</span><i style="width:${Math.max(4, n / mx * 100)}%;background:${col ? col(t) : "#22d3ee"}"></i><em>${nf(n)}</em></div>`).join("")}</div>`; };
      return `<div class="tl-fc" style="--c:${f.c}"><div class="tl-fch"><span class="tl-fci">${f.ic}</span><div><h3>${esc(f.n)}</h3><small>${esc(f.d)}</small></div></div>
        <div class="tl-fk"><div><b>${nf(r.length)}</b><span>publicaciones con ubicación</span></div><div><b>${nf(conP.length)}</b><span>con precio</span></div><div><b>${money(mP)}</b><span>precio mediano ${esc(f.u)}</span></div>${mM != null ? `<div><b>$${nf(mM)}</b><span>mediana por m²</span></div>` : ""}</div>
        ${barras("Por tipo", grupo("tipo", 6), colTipo)}${barras("Por estado", grupo("estado", 5), () => "#a78bfa")}${barras("Por ciudad", grupo("ciudad", 6), () => "#34d399")}
        <button type="button" class="eco-btn tl-go" data-s="${f.id}">Abrir base →</button></div>`;
    }).join("");
  }

  function extrae(f) {
    S.sel = "x:" + f; arbol(); if (S.mapa) { try { S.mapa.remove(); } catch (_) { } S.mapa = null; S.capa = null; S.mk = {}; }
    const m = $("#tlMain"); m.className = "eco-main tl-main tl-ext"; $("#tlChip").textContent = "Extracción · " + FBY[f].n;
    const go = () => window.TLX && window.TLX.abre(f, m); if (window.TLX) go(); else m.innerHTML = '<div class="eco-nada">Cargando…</div>';
  }

  /* ───────── base: filtros, mapa, tarjetas y ficha ───────── */
  async function abre(f) {
    if (window.TLX) window.TLX.para();
    S.sel = f; S.f = { q: S.f.q, op: "", tipos: [], ciudad: "", pmin: "", pmax: "", rec: "", ord: "rel" }; S.foco = null; S.ficha = null; S.n = 40; arbol();
    const m = $("#tlMain"); if (S.mapa) { try { S.mapa.remove(); } catch (_) { } S.mapa = null; S.capa = null; S.mk = {}; }
    m.className = "eco-main tl-main tl-base"; m.innerHTML = `<div class="eco-nada">Cargando ${esc(FBY[f].n)}…</div>`;
    let d; try { d = await carga(f); } catch (e) { if (S.sel === f) m.innerHTML = `<div class="eco-nada">${esc(e.message)}</div>`; return; } if (S.sel !== f) return;
    arbol(); $("#tlChip").textContent = nf(d.rows.length) + " en " + FBY[f].n;
    const opt = (a, t) => `<option value="">${t}</option>` + a.map(x => `<option value="${esc(x)}">${esc(x)}</option>`).join("");
    m.innerHTML = `<div class="tl-hd"><div><h1 class="tl-h1">${FBY[f].ic} ${esc(FBY[f].n)}</h1><div class="eco-crumb" id="tlKp"></div></div></div>
      <div class="tl-tb"><label>Operación<select data-k="op">${opt(d.ops, "Todas")}</select></label><label>Ciudad<select data-k="ciudad">${opt(d.ciudades, "Todas")}</select></label>
        <label>Precio mín.<input data-k="pmin" type="number" min="0" placeholder="0"></label><label>Precio máx.<input data-k="pmax" type="number" min="0" placeholder="sin tope"></label>${f === "airbnb" ? "" : `<label>Recámaras<select data-k="rec"><option value="">Todas</option><option value="1">1+</option><option value="2">2+</option><option value="3">3+</option><option value="4">4+</option></select></label>`}
        <label>Orden<select data-k="ord"><option value="rel">Sin ordenar</option><option value="pa">Precio ↑</option><option value="pd">Precio ↓</option><option value="ma">$/m² ↑</option><option value="md">$/m² ↓</option><option value="sd">Superficie ↓</option></select></label><button type="button" class="eco-btn" id="tlLimpia">Limpiar</button></div>
      <div class="tl-tipos" id="tlTipos"><span class="tl-tl">Tipo</span>${d.tipos.map(t => `<button type="button" class="tl-tc" data-tl-tipo="${esc(t)}" style="--c:${colTipo(t)}"><i></i>${esc(t)}<em>${nf(d.rows.filter(r => r.tipo === t).length)}</em></button>`).join("")}</div>
      <div class="eco-kpis tl-kp" id="tlKpis"></div>
      <div class="tl-split"><div class="tl-mapw"><div id="tlMap"></div><div class="tl-leg" id="tlLeg"></div></div><div class="tl-col"><div id="tlLista" class="tl-lista"></div><div id="tlDet" class="tl-det" hidden></div></div></div>`;
    $$("[data-k]", m).forEach(el => { el.addEventListener("input", () => { S.f[el.dataset.k] = el.value; aplica(); }); });
    $("#tlLimpia", m).onclick = () => { S.f = { q: "", op: "", tipos: [], ciudad: "", pmin: "", pmax: "", rec: "", ord: "rel" }; $$(".tl-tc", m).forEach(b => b.classList.remove("on")); $$("[data-k]", m).forEach(el => { el.value = el.dataset.k === "ord" ? "rel" : ""; }); $("#tlQ").value = ""; aplica(); };
    $("#tlLista", m).addEventListener("scroll", e => { const l = e.target; if (l.scrollTop + l.clientHeight > l.scrollHeight - 300 && S.n < S.lista.length) { S.n += 40; pintaLista(true); } });
    leaflet(() => { creaMapa(); aplica(true); });
  }
  function leaflet(cb) { if (window.L) return cb(); if (window._rsvEnsureLeaflet_) return window._rsvEnsureLeaflet_(cb); const c = document.createElement("link"); c.rel = "stylesheet"; c.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"; document.head.appendChild(c); const s = document.createElement("script"); s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"; s.onload = cb; document.body.appendChild(s); }
  function creaMapa() {
    const el = $("#tlMap"); if (!el || !window.L) return; const m = L.map(el, { preferCanvas: true, zoomControl: true });
    const claro = () => $("#tlApp").classList.contains("eco-light");
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", { maxZoom: 19, maxNativeZoom: 16, attribution: "Esri" }).addTo(m);
    S.capa = L.layerGroup().addTo(m); S.mapa = m; m.setView([25.43, -101], 6);
    m.on("popupopen", e => { const b = e.popup._contentNode && $("[data-tl-ficha]", e.popup._contentNode); if (b) { /* delegado en host */ } });
  }
  const filtra = () => {
    const f = S.f, q = norm(f.q), pmin = f.pmin === "" ? null : +f.pmin, pmax = f.pmax === "" ? null : +f.pmax, rec = f.rec === "" ? null : +f.rec;
    let r = D().rows.filter(x => (!q || norm(x.titulo + " " + x.colonia + " " + x.ciudad).includes(q)) && (!f.op || x.op === f.op) && (!f.tipos.length || f.tipos.includes(x.tipo)) && (!f.ciudad || x.ciudad === f.ciudad) && (pmin == null || (x.precio != null && x.precio >= pmin)) && (pmax == null || (x.precio != null && x.precio <= pmax)) && (rec == null || (x.rec != null && x.rec >= rec)));
    const key = { pa: x => x.precio, pd: x => x.precio, ma: x => x.pm2, md: x => x.pm2, sd: x => x.m2 }[f.ord];
    if (key) { const dir = f.ord === "pa" || f.ord === "ma" ? 1 : -1; r = r.filter(x => key(x) != null).concat(r.filter(x => key(x) == null)).sort((a, b) => { const x = key(a), y = key(b); if (x == null) return 1; if (y == null) return -1; return (x - y) * dir; }); }
    return r;
  };
  function aplica(inicial) {
    S.lista = filtra(); S.n = 40; const r = S.lista, mP = med(r.map(x => x.precio)), mM = med(r.map(x => x.pm2));
    $("#tlKp").textContent = `${nf(r.length)} de ${nf(D().rows.length)} publicaciones · todas con ubicación en el mapa`;
    const k = (l, v, s) => `<div class="eco-kpi"><div class="kl">${l}</div><div class="kv">${v}</div><div class="ks">${s || ""}</div></div>`;
    $("#tlKpis").innerHTML = k("Publicaciones", nf(r.length), "con los filtros actuales") + k("Precio mediano", money(mP), FBY[S.sel].u) + (S.sel !== "airbnb" ? k("Mediana por m²", mM != null ? "$" + nf(mM) : "—", "construcción o total") : k("Calificación media", (() => { const c = r.map(x => x.cal).filter(x => x != null); return c.length ? nf(c.reduce((a, b) => a + b, 0) / c.length, 2) : "—"; })(), "de 5")) + k("Ciudades", nf(new Set(r.map(x => x.ciudad).filter(Boolean)).size), "en el resultado");
    pintaMapa(inicial); pintaLista(false);
  }
  function pintaMapa(ajusta) {
    if (!S.mapa) return; S.capa.clearLayers(); S.mk = {}; const tipos = {};
    S.lista.forEach(x => { if (x.lat == null || x.lng == null) return; tipos[x.tipo] = 1; const mk = L.circleMarker([x.lat, x.lng], { radius: 6, color: "#0f172a", weight: 1, fillColor: colTipo(x.tipo), fillOpacity: .88 }); mk._id = x.id;
      mk.bindPopup(() => popup(x), { className: "tl-pop", minWidth: 230, maxWidth: 250, closeButton: false, autoPan: true }); mk.on("click", () => { S.foco = x.id; resalta(true); }); mk.on("popupopen", () => { if (!S.fotos[x.id]) cargaFotos([x.id]).then(() => { try { mk.getPopup().setContent(popup(x)); } catch (_) { } }); }); mk.addTo(S.capa); S.mk[x.id] = mk; });
    $("#tlLeg").innerHTML = Object.keys(tipos).sort((a, b) => TIPOS_ORD.indexOf(a) - TIPOS_ORD.indexOf(b)).map(t => `<span><i style="background:${colTipo(t)}"></i>${esc(t)}</span>`).join("");
    const pts = S.lista.filter(x => x.lat != null).map(x => [x.lat, x.lng]); if (ajusta !== false && pts.length) { try { S.mapa.fitBounds(L.latLngBounds(pts).pad(.08), { maxZoom: 15 }); } catch (_) { } }
    setTimeout(() => S.mapa && S.mapa.invalidateSize(), 60);
  }
  function resalta(desdeMapa) {
    Object.entries(S.mk).forEach(([id, mk]) => { const on = id === S.foco; mk.setStyle({ radius: on ? 10 : 6, weight: on ? 3 : 1, color: on ? "#fff" : "#0f172a" }); if (on) mk.bringToFront(); });
    $$(".tl-card", $("#tlLista")).forEach(c => c.classList.toggle("on", c.dataset.id === S.foco));
    if (desdeMapa) { const i = S.lista.findIndex(x => x.id === S.foco); if (i >= S.n) { S.n = i + 20; pintaLista(false); } const c = $(`.tl-card[data-id="${CSS.escape(S.foco)}"]`, $("#tlLista")); if (c && $("#tlDet").hidden) c.scrollIntoView({ block: "center", behavior: "smooth" }); }
  }
  const foto = id => S.fotos[id];
  const imgHtml = (x, cls) => { const p = foto(x.id), u = p && p.f; return u ? `<img class="${cls || ""}" src="${esc(px(mini(u)))}" data-o="${esc(u)}" alt="" loading="lazy" onerror="if(!this.dataset.r){this.dataset.r=1;this.referrerPolicy='no-referrer';this.src=this.dataset.o}else{this.replaceWith(Object.assign(document.createElement('span'),{className:'tl-nf',textContent:'${nfTxt(u)}'}))}">` : `<span class="tl-nf">${p ? nfTxt("") : "…"}</span>`; };
  const specs = x => [x.rec != null ? `🛏 ${nf(x.rec)}` : "", x.ban != null ? `🛁 ${nf(x.ban)}` : "", x.m2 ? `📐 ${nf(x.m2)} m²` : "", x.hu != null ? `👥 ${nf(x.hu)}` : "", x.cal != null ? `★ ${nf(x.cal, 2)}` : ""].filter(Boolean).join(" · ");
  const precioTxt = x => x.precio == null ? "Sin precio" : money(x.precio) + (x.op === "renta" ? " /mes" : x.op === "noche" ? " /noche" : "");
  const chipPm = x => x.pm2 != null ? `<span class="tl-pm">$${nf(x.pm2)}/m²</span>` : "";
  function card(x) {
    return `<div class="tl-card${S.foco === x.id ? " on" : ""}" data-id="${esc(x.id)}" style="--c:${colTipo(x.tipo)}"><div class="tl-ph" data-tl-card="${esc(x.id)}">${imgHtml(x)}${navBtns()}</div>
      <div class="tl-cb" data-tl-card="${esc(x.id)}"><div class="tl-pr"><b>${precioTxt(x)}</b>${chipPm(x)}</div><h4>${esc(x.titulo || "Sin título")}</h4><div class="tl-sp">${esc(specs(x))}</div>
      <div class="tl-lo">${esc([x.colonia, x.ciudad].filter(Boolean).join(", "))}</div><div class="tl-ch"><span class="tl-t" style="--c:${colTipo(x.tipo)}">${esc(x.tipo)}</span>${x.op ? `<span class="tl-t">${esc(x.op)}</span>` : ""}<button type="button" class="tl-fi" data-tl-ficha="${esc(x.id)}">Ver ficha</button></div></div></div>`;
  }
  function popup(x) {
    return `<div class="tl-mini"><div class="tl-mp">${imgHtml(x)}</div><div class="tl-mb"><b>${precioTxt(x)}</b>${chipPm(x)}<small>${esc(x.titulo || "")}</small><button type="button" class="tl-fi" data-tl-ficha="${esc(x.id)}">Ver ficha</button></div></div>`;
  }
  async function cargaFotos(ids) {
    ids = ids.filter(i => !S.fotos[i]); if (!ids.length) return;
    for (let k = 0; k < ids.length; k += 150) { try { const j = await api(RT + "/fotos", { ids: ids.slice(k, k + 150) }); Object.keys(j.items || {}).forEach(i => { S.fotos[i] = j.items[i]; }); ids.slice(k, k + 150).forEach(i => { if (!S.fotos[i]) S.fotos[i] = { f: "" }; }); } catch (_) { } }
    $$(".tl-card .tl-ph").forEach(ph => { const id = ph.dataset.tlCard, x = S.lista.find(y => y.id === id); if (x && S.fotos[id] && !REEL[id]) ph.innerHTML = imgHtml(x) + navBtns(); });
  }
  function pintaLista(mas) {
    const el = $("#tlLista"); if (!el) return; const r = S.lista, v = r.slice(0, S.n);
    el.innerHTML = v.length ? v.map(card).join("") + (S.n < r.length ? `<div class="tl-mas">Desplázate para ver más (${nf(r.length - S.n)})…</div>` : "") : `<div class="eco-nada">Ninguna publicación coincide con los filtros.</div>`;
    cargaFotos(v.map(x => x.id)); if (!mas) el.scrollTop = 0;
  }

  /* ───────── ficha (columna derecha, con «Volver a la lista») ───────── */
  const ICON = [[/hu[eé]sped/i, "👥"], [/habitaci|rec[aá]mara/i, "🛏"], [/cama/i, "🛌"], [/ba[nñ]o/i, "🛁"], [/calific/i, "⭐"], [/evaluac|rese[nñ]a/i, "💬"], [/superanfitri/i, "🏅"], [/a[nñ]os/i, "📅"], [/noches/i, "🌙"], [/ocupaci/i, "📈"], [/precio|renta|venta|mantenimiento/i, "💲"], [/estaciona/i, "🚗"], [/construc|terreno|superficie|m²|m2/i, "📐"], [/antig/i, "🏗"], [/tipo|operaci/i, "🏷"], [/jard/i, "🌳"], [/amuebl/i, "🛋"], [/ubicaci|colonia|calle|ciudad|estado|direcci/i, "📍"], [/public|fecha/i, "🗓"]];
  const icono = k => { const m = ICON.find(([rx]) => rx.test(k)); return m ? m[1] : "•"; };
  const reelIr = d => { const fo = S._fo || [], b = $("#tlBig"); if (!b || fo.length < 2) return; S._fi = ((S._fi || 0) + d + fo.length) % fo.length; b.dataset.r = ""; b.referrerPolicy = ""; b.dataset.o = fo[S._fi]; b.src = px(fo[S._fi]); const c = $("#tlCnt"); if (c) c.textContent = (S._fi + 1) + " / " + fo.length; $$(".tl-th img").forEach((i, k) => { i.classList.toggle("on", k === S._fi); if (k === S._fi) i.scrollIntoView({ inline: "center", block: "nearest" }); }); };
  /* reel en la tarjeta: las fotos completas se piden al abrir el reel la primera vez */
  const REEL = {};
  /* Airbnb guarda una sola foto en la base: el reel completo se lee de la publicación (el servidor la guarda para la próxima vez) */
  async function fotosAir(id) { try { const r = await fetch(API + "/mercado/airbnb/fotos?id=" + encodeURIComponent(id)), j = await r.json(); return (j.fotos || []).map(x => x.u || x).filter(u => typeof u === "string").slice(0, 30); } catch (_) { return []; } }
  const navBtns = () => '<button type="button" class="tl-nv l" data-tl-cn="-1" aria-label="Foto anterior">‹</button><button type="button" class="tl-nv r" data-tl-cn="1" aria-label="Foto siguiente">›</button>';
  async function reelCard(id, d, ph) {
    let r = REEL[id]; if (!r) { r = REEL[id] = { fo: [], i: 0 }; try { const j = await api(RT + "/ficha", { id }); r.fo = (j.ficha.fotos || []).filter(Boolean); if (j.ficha.fuente === "Airbnb" && r.fo.length < 2) { const m = await fotosAir(id); if (m.length) r.fo = m; } } catch (_) { } }
    if (r.fo.length < 2) return; r.i = (r.i + d + r.fo.length) % r.fo.length; const u = r.fo[r.i];
    ph.innerHTML = `<img src="${esc(px(mini(u)))}" data-o="${esc(u)}" alt="" onerror="if(!this.dataset.r){this.dataset.r=1;this.referrerPolicy='no-referrer';this.src=this.dataset.o}"><span class="tl-cnt">${r.i + 1} / ${r.fo.length}</span>` + navBtns();
  }
  function cierraFicha() { S.ficha = null; const d = $("#tlDet"), l = $("#tlLista"); if (d) { d.hidden = true; d.innerHTML = ""; } if (l) l.hidden = false; if (S.mapa) S.mapa.closePopup(); resalta(false); const c = S.foco && $(`.tl-card[data-id="${CSS.escape(S.foco)}"]`, l); if (c) c.scrollIntoView({ block: "center" }); }
  async function abreFicha(id) {
    const x = S.lista.find(y => y.id === id) || D().rows.find(y => y.id === id); if (!x) return; const tk = ++S.fk; S.ficha = id; S.foco = id;
    const det = $("#tlDet"), lis = $("#tlLista"); lis.hidden = true; det.hidden = false; resalta(false); const mk = S.mk[id]; if (mk && S.mapa) { S.mapa.flyTo(mk.getLatLng(), Math.max(S.mapa.getZoom(), 15), { duration: .5 }); }
    const nav = `<div class="tl-dn"><button type="button" class="tl-bk" data-tl-back>← Volver a la lista</button><span>Detalle de la publicación</span></div>`;
    det.innerHTML = nav + `<div class="eco-nada">Cargando la ficha…</div>`; det.scrollTop = 0;
    try {
      const j = await api(RT + "/ficha", { id }); if (tk !== S.fk) return; const F = j.ficha; let fo = (F.fotos || []).filter(Boolean); if (F.fuente === "Airbnb" && fo.length < 2) { const m = await fotosAir(id); if (tk !== S.fk) return; if (m.length) fo = m; }
      const gal = fo.length ? `<div class="tl-big tl-reel">${fo.length > 1 ? '<button type="button" class="tl-nv l" data-tl-nav="-1" aria-label="Foto anterior">‹</button><button type="button" class="tl-nv r" data-tl-nav="1" aria-label="Foto siguiente">›</button><span class="tl-cnt" id="tlCnt">1 / ' + fo.length + '</span>' : ""}<img id="tlBig" src="${esc(px(fo[0]))}" data-o="${esc(fo[0])}" alt="" onerror="if(!this.dataset.r){this.dataset.r=1;this.referrerPolicy='no-referrer';this.src=this.dataset.o}"></div>${fo.length > 1 ? `<div class="tl-th">${fo.slice(0, 24).map((u, i) => `<img src="${esc(px(u))}" data-u="${esc(u)}" data-tl-ph="${i}" class="${i ? "" : "on"}" alt="" referrerpolicy="no-referrer" onerror="if(!this.dataset.r){this.dataset.r=1;this.src=this.dataset.u}">`).join("")}</div>` : ""}${fo.some(u => /fbcdn\.net/.test(u)) ? `<p class="tl-fbn">Las fotos de Facebook antiguas son enlaces temporales y pueden haber caducado; vuelve a extraer la publicación para renovarlas.</p>` : ""}` : `<div class="tl-big tl-nf">${nfTxt("")}</div>`;
      S._fo = fo; S._fi = 0;
      det.innerHTML = nav + `<div class="tl-fd">${gal}<div class="tl-fh"><span class="tl-t" style="--c:${colTipo(x.tipo)}">${esc(F.fuente)}</span>${x.tipo ? `<span class="tl-t">${esc(x.tipo)}</span>` : ""}${x.op ? `<span class="tl-t">${esc(x.op)}</span>` : ""}</div>
        <div class="tl-fp">${precioTxt(x)} ${chipPm(x)}</div><h2>${esc(F.titulo || x.titulo || x.id)}</h2><p class="eco-crumb">${esc(F.ubicacion || "")}${F.publicado ? " · " + esc(String(F.publicado).slice(0, 10)) : ""}</p>
        ${F.pares && F.pares.length ? `<h3>Características</h3><ul class="tl-pl">${F.pares.map(([a, b]) => `<li><span class="ic">${icono(a)}</span><span class="k">${esc(a)}</span><b>${esc(b)}</b></li>`).join("")}</ul>` : ""}
        ${F.amenidades && F.amenidades.length ? `<h3>Amenidades</h3><div class="tl-am">${F.amenidades.map(a => `<span class="tl-t">${esc(a)}</span>`).join("")}</div>` : ""}
        ${F.vendedor ? `<h3>Publicante</h3><p>👤 ${esc(F.vendedor)}</p>` : ""}${F.descripcion ? `<h3>Descripción</h3><p class="tl-ds">${esc(F.descripcion)}</p>` : ""}
        ${F.url ? `<a class="tl-ab" href="${esc(F.url)}" target="_blank" rel="noopener">Abrir la publicación original ↗</a>` : ""}</div>`;
    } catch (e) { if (tk !== S.fk) return; det.innerHTML = nav + `<div class="tl-fd"><h2>${esc(x.titulo || x.id)}</h2><p class="tl-err">No se pudo cargar el detalle (${esc(e.message)}).</p>${x.url ? `<a class="tl-ab" href="${esc(x.url)}" target="_blank" rel="noopener">Abrir la publicación original ↗</a>` : ""}</div>`; }
  }

  function clic(e) {
    const t = e.target, xs = t.closest("[data-x]"); if (xs && xs.closest("#tlTree")) { $("#tlApp").classList.remove("eco-menu"); extrae(xs.dataset.x); return; }
    const s = t.closest("[data-s]"); if (s && s.closest("#tlTree, .tl-go, .tl-fc")) { $("#tlApp").classList.remove("eco-menu"); const f = s.dataset.s; if (f === "home") home(); else abre(f); return; }
    const fi = t.closest("[data-tl-ficha]"); if (fi) { e.stopPropagation(); abreFicha(fi.dataset.tlFicha); return; }
    if (t.closest("[data-tl-back]")) { cierraFicha(); return; }
    const tc = t.closest("[data-tl-tipo]"); if (tc) { const a = S.f.tipos, k = tc.dataset.tlTipo, i = a.indexOf(k); if (i >= 0) a.splice(i, 1); else a.push(k); tc.classList.toggle("on", i < 0); aplica(); return; }
    const nv = t.closest("[data-tl-nav]"); if (nv) { reelIr(+nv.dataset.tlNav); return; }
    const cn = t.closest("[data-tl-cn]"); if (cn) { e.stopPropagation(); const phc = cn.closest(".tl-ph"); reelCard(phc.dataset.tlCard, +cn.dataset.tlCn, phc); return; }
    const ph = t.closest("[data-tl-ph]"); if (ph && S._fo) { S._fi = +ph.dataset.tlPh; const cc = $("#tlCnt"); if (cc) cc.textContent = (S._fi + 1) + " / " + S._fo.length; const b = $("#tlBig"); if (b) { b.dataset.r = ""; b.referrerPolicy = ""; b.dataset.o = S._fo[+ph.dataset.tlPh]; b.src = px(S._fo[+ph.dataset.tlPh]); } $$(".tl-th img").forEach(i => i.classList.toggle("on", i === ph)); return; }
    const cd = t.closest("[data-tl-card]"); if (cd) { const id = cd.dataset.tlCard; S.foco = id; resalta(false); const mk = S.mk[id]; if (mk && S.mapa) { S.mapa.flyTo(mk.getLatLng(), Math.max(S.mapa.getZoom(), 14), { duration: .5 }); setTimeout(() => mk.openPopup(), 520); } }
  }

  window.terralabInit = function () {
    host = document.getElementById("terralab-host"); shell(); arbol();
    let s = ls.get("tl_sel", "home"); if (!FBY[s]) s = "home"; if (s === "home") home(); else abre(s);
    host.addEventListener("click", e => { const b = e.target.closest("[data-s]"); if (b && FBY[b.dataset.s]) ls.set("tl_sel", b.dataset.s); else if (b && b.dataset.s === "home") ls.set("tl_sel", "home"); });
  };
})();
