/* EcoLab · laboratorio económico: catálogo en árbol (Área › Grupo › Indicador) + tablero interactivo.
   Necesita ecolab-datos.js (window.ECO_DATA, generado por ecolab-descarga.js) y ecolab.css. Se inicia con window.ecolabInit(). */
(function () {
  "use strict";
  const EMB = (() => { try { return new URLSearchParams(location.search).get("emb") === "1" && window.parent !== window; } catch (_) { return false; } })();
  const glTabs = A => EMB ? `<div class="gl-tabs" id="glTabs" role="tablist" aria-label="Sistemas"><button type="button" data-v="geo" role="tab" title="GeoLab"><img src="geometrika-logo.png" alt=""><b>GeoLab</b></button><button type="button" data-v="eco" role="tab"${A === "eco" ? ' class="on"' : ""} title="EconoLab"><img src="econolab-logo.png" alt=""><b>EconoLab</b></button><button type="button" data-v="data" role="tab"${A === "data" ? ' class="on"' : ""} title="DataLab"><img src="datalab-logo.png" alt=""><b>DataLab</b></button></div>` : "";
  const eNotif = o => { try { parent.postMessage({ gl: "win", open: o }, location.origin); } catch (_) { } };
  const ecoWin = o => { if (!EMB) return; const a = document.getElementById("ecoApp"); if (!a) return; a.classList.toggle("eco-open", o); eNotif(o); };
  const glIr = e => { const gt = e.target.closest("#glTabs button, #glBack"); if (!gt) return false; try { parent.postMessage({ gl: "tab", v: gt.dataset.v || "geo" }, location.origin); } catch (_) { } return true; };
  if (EMB) addEventListener("message", e => { if (e.origin === location.origin && e.data && e.data.gl === "col") { document.documentElement.style.setProperty("--col", e.data.w + "px"); if (e.data.h != null) document.documentElement.style.setProperty("--tth", e.data.h + "px"); } });
  const $ = (s, r) => (r || document).querySelector(s), $$ = (s, r) => [...(r || document).querySelectorAll(s)];
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const norm = s => String(s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
  const ls = { get(k, d) { try { const v = localStorage.getItem(k); return v == null ? d : v; } catch (_) { return d; } }, set(k, v) { try { localStorage.setItem(k, v); } catch (_) { } } };
  const nf = (v, d) => v == null || !isFinite(v) ? "—" : v.toLocaleString("es-MX", { minimumFractionDigits: d, maximumFractionDigits: d });
  const MES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];
  const PAL = ["#22d3ee", "#a78bfa", "#f472b6", "#fbbf24", "#34d399", "#60a5fa", "#fb7185", "#f97316"];
  const QD = { exp: ["Expansión", "#34d399"], des: ["Desaceleración", "#fbbf24"], con: ["Contracción", "#fb7185"], rec: ["Recuperación", "#60a5fa"] };
  const PPY = { Mensual: 12, Trimestral: 4, Anual: 1 };
  const D = window.ECO_DATA || { indicadores: [] };
  let host, IND = [], AREAS = [], BYID = {}, S = { sel: "home", q: "", hist: [], zoom: {}, open: new Set(), serie: { mode: "ambas", rango: 120 } }, draws = [], tipEl;

  /* ───────── preparación de datos ───────── */
  /* ───────── ciclo: asignación de coordenadas, filtro HP y derivación desde la tendencia-ciclo ───────── */
  function cicloSet(I, dev) {
    const s = I.s, lag = I.ppy === 12 ? 3 : 1; s.forEach(o => { o.dv = null; o.x = null; o.y = null; });
    if (!dev) { I.quad = null; return; }
    s.forEach((o, i) => { o.dv = dev[i]; });
    const v = dev.filter(x => x != null && isFinite(x)), sd = Math.sqrt(v.reduce((a, b) => a + b * b, 0) / (v.length || 1)) || 1;
    s.forEach(o => { o.x = o.dv == null ? null : o.dv / sd; });
    s.forEach((o, i) => { o.y = i >= lag && o.x != null && s[i - lag].x != null ? o.x - s[i - lag].x : null; });
    const ys = s.map(o => o.y).filter(x => x != null), sy = Math.sqrt(ys.reduce((a, b) => a + b * b, 0) / (ys.length || 1)) || 1; s.forEach(o => { if (o.y != null) o.y /= sy; });
    const l = s[s.length - 1]; I.quad = l.x == null || l.y == null ? null : l.x >= 0 ? (l.y >= 0 ? "exp" : "des") : (l.y >= 0 ? "rec" : "con");
  }
  function hpTrend(y, lam) {   // filtro de Hodrick-Prescott (sistema pentadiagonal por LDL')
    const n = y.length, d = new Float64Array(n), e = new Float64Array(n), f = new Float64Array(n), D = new Float64Array(n), l1 = new Float64Array(n), l2 = new Float64Array(n);
    for (let i = 0; i < n; i++) d[i] = 1 + lam * ((i === 0 || i === n - 1) ? 1 : (i === 1 || i === n - 2) ? 5 : 6);
    for (let i = 0; i < n - 1; i++) e[i] = lam * ((i === 0 || i === n - 2) ? -2 : -4);
    for (let i = 0; i < n - 2; i++) f[i] = lam;
    for (let i = 0; i < n; i++) { let di = d[i]; if (i >= 1) di -= l1[i - 1] * l1[i - 1] * D[i - 1]; if (i >= 2) di -= l2[i - 2] * l2[i - 2] * D[i - 2]; D[i] = di;
      if (i < n - 1) { let v = e[i]; if (i >= 1) v -= l2[i - 1] * l1[i - 1] * D[i - 1]; l1[i] = v / di; } if (i < n - 2) l2[i] = f[i] / di; }
    const z = new Float64Array(n), x = new Float64Array(n);
    for (let i = 0; i < n; i++) { let v = y[i]; if (i >= 1) v -= l1[i - 1] * z[i - 1]; if (i >= 2) v -= l2[i - 2] * z[i - 2]; z[i] = v; }
    for (let i = 0; i < n; i++) z[i] /= D[i];
    for (let i = n - 1; i >= 0; i--) { let v = z[i]; if (i < n - 1) v -= l1[i] * x[i + 1]; if (i < n - 2) v -= l2[i] * x[i + 2]; x[i] = v; }
    return x;
  }
  // serie original → serie «Ciclo» oficial del INEGI que corresponde a la misma variable
  const VINC = [["737121", "334349"], ["737145", "334349"], ["736407", "334344"], ["736526", "334344"], ["737122", "334441"], ["737146", "334441"], ["737130", "334446"], ["737154", "334446"],
    ["6204198547", "334391"], ["6204198549", "334386"], ["6204198561", "334401"], ["6204198556", "334396"], ["6204198564", "334461"], ["701401", "334486"], ["701618", "334496"], ["454168", "334501"]];
  function cicloVinc(I, C) {
    if (!I || !C || !C.quad || I.ppy !== C.ppy) return;
    const by = {}; C.s.forEach(o => { by[o.t] = o; });
    I.s.forEach(o => { const c = by[o.t]; o.dv = c ? c.dv : null; o.x = c ? c.x : null; o.y = c ? c.y : null; });
    const l = I.s[I.s.length - 1]; I.quad = l.x == null || l.y == null ? null : l.x >= 0 ? (l.y >= 0 ? "exp" : "des") : (l.y >= 0 ? "rec" : "con");
    I.ciclo = "oficial"; I.cicloDe = C.id; I.cicloNom = C.nombre;
  }
  const fuenteCorta = I => String(I.fuente || "INEGI").replace(/\s*\d{4}\.?$/, "").replace(/^INEGI\s*Sistema de Cuentas Nacionales de México\.\s*/, "INEGI · SCNM · ").replace(/\.$/, "");
  const alcance = I => I.code && I.area === "Ciudades" && CBY[I.code] ? "Ciudad: " + CBY[I.code][1] : I.code && ENTBY[I.code] ? "Estado: " + ENTBY[I.code][1] : "Nacional";
  const nomOrig = I => { const n = String(I.nombre || ""); return I.code ? n.replace(/ · [^·]+$/, "") : n; };   // nombre tal cual de la serie original (sin el sufijo de estado/ciudad)
  const serieRef = I => "«" + esc(nomOrig(I)) + "»" + (I.grupo && !I.code && I.grupo !== I.nombre && !new RegExp("^" + String(I.nombre).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).test(I.grupo) ? " (" + esc(I.grupo) + ")" : "");
  const serieDesc = I => "Serie «" + nomOrig(I) + "»" + (I.grupo && !I.code && I.grupo !== I.nombre ? " (" + I.grupo + ")" : "") + " · " + fuenteCorta(I) + " · " + (I.frecuencia || "").toLowerCase() + " · " + alcance(I) + " · vía API del INEGI (BIE/BISE)";
  function cicloHint(I) {
    if (!I.quad) return "sin ciclo";
    return I.ciclo === "tc" ? "derivado de la tendencia-ciclo oficial del INEGI (filtro HP)" : I.cicloDe ? "ciclo oficial del INEGI · «" + I.cicloNom.replace(/^Ciclo · /, "") + "»" : "ciclo oficial del INEGI";
  }
  function noAplicaMsg(I) {
    if (I.area === "Precios e inflación") return "Los precios están dominados por su tendencia (inflación acumulada); un ciclo alrededor de esa tendencia no es una lectura adecuada.";
    if (I.area === "Ciudades" || I.area === "Estados") return "El INEGI no publica una serie de tendencia-ciclo a este nivel geográfico.";
    if (I.pct || I.tipo === "yoy") return "Es una variación o una tasa; el ciclo se lee sobre el nivel de la serie, no sobre su variación.";
    return "El INEGI no publica un ciclo ni una tendencia-ciclo para esta serie, o aún no se ha incorporado a EconoLab.";
  }
  function cicloDeTC(I, tc) {   // ciclo de I = desviación (%) de la tendencia-ciclo oficial del INEGI respecto de su tendencia de largo plazo (HP)
    if (!I || !tc || tc.s.length < 24 || I.ppy !== tc.ppy) return;
    const ly = tc.s.map(o => Math.log(o.v)); if (ly.some(v => !isFinite(v))) return;
    const tr = hpTrend(ly, tc.ppy === 12 ? 129600 : 1600), by = {}; tc.s.forEach((o, i) => { by[o.t] = (ly[i] - tr[i]) * 100; });
    cicloSet(I, I.s.map(o => by[o.t] != null ? by[o.t] : null)); I.ciclo = "tc"; I.cicloTC = tc.id;
  }

  function prep(r, k) {
    const ppy = PPY[r.frecuencia] || 1, I = Object.assign({}, r);
    I.ppy = ppy;
    I.s = r.obs.map(([p, v]) => {
      const m = /^(\d{4})(?:\/(\d+))?/.exec(p), yr = +m[1], q = m[2] ? +m[2] : 1;
      return { yr, q, t: yr * ppy + (q - 1), v, l: ppy === 12 ? MES[q - 1] + " " + yr : ppy === 4 ? "T" + q + " " + yr : String(yr) };
    });
    const s = I.s, n = s.length, neg = s.some(o => o.v <= 0);
    I.pct = /^Porcentaje|^Variaci[oó]n porcentual/i.test(r.unidad);
    I.dif = neg || I.pct || /Puntos/.test(r.unidad);
    const ch = (a, b) => b == null ? null : I.dif ? a - b : b > 0 ? (a / b - 1) * 100 : null;
    s.forEach((o, i) => { o.yoy = i >= ppy ? ch(o.v, s[i - ppy].v) : null; o.mom = i >= 1 && ppy > 1 ? ch(o.v, s[i - 1].v) : null; });
    I.ciclo = r.tipo === "ciclo" ? "oficial" : null; I.cicloInv = String(r.id) === "334366"; cicloSet(I, I.ciclo ? s.map(o => (I.cicloInv ? -1 : 1) * (o.v - 100)) : null);   // ciclo oficial del INEGI; el resto se deriva de su tendencia-ciclo (cicloDeTC) o no aplica
    const last = s[n - 1], vals = s.map(o => o.v).sort((a, b) => a - b);
    I.last = last; I.prev = n > 1 ? s[n - 2] : null; I.fc = FCIDS.has(String(r.id)) || (r.tipo === "idx" && !!r.scian);
    I.min = vals[0]; I.max = vals[n - 1]; I.pctil = vals.filter(v => v <= last.v).length / n * 100;
    I.minO = s.find(o => o.v === I.min); I.maxO = s.find(o => o.v === I.max);
    // (la fase se asigna en cicloSet)
    const now = new Date(), mesesAct = (now.getFullYear() * 12 + now.getMonth()) - (last.yr * 12 + (ppy === 12 ? last.q - 1 : ppy === 4 ? (last.q - 1) * 3 : 0));
    const ok = ppy === 12 ? 4 : ppy === 4 ? 8 : 24;
    I.edad = mesesAct; I.est = mesesAct <= ok ? ["ok", "Al día"] : mesesAct <= ok * 2 ? ["warn", "Rezagada"] : ["old", "Sin actualizar"];
    I.thr = r.tipo === "ciclo" ? 100 : (r.unidad === "Puntos" && I.min < 50 && I.max > 50 ? 50 : null); I.th50 = I.thr != null;
    I.u = r.unidad === "Puntos" ? "pts" : I.pct ? "%" : r.unidad === "Dólares" ? (r.mult === "Millones" ? "mdd" : "USD") : /^Índice/.test(r.unidad) ? r.unidad.replace("Índice base", "índice base ") : /^Pesos/i.test(r.unidad) ? (r.mult === "Millones" ? "mdp " : "pesos ") + (/(20\d\d)/.test(r.unidad) ? "(" + /(20\d\d)/.exec(r.unidad)[1] + ")" : "corrientes") : r.unidad.toLowerCase();
    const a = Math.abs(last.v); I.dg = a >= 1000 ? 0 : a >= 100 ? 1 : 2; I.col = PAL[k % PAL.length];
    return I;
  }
  function carga() {
    IND = []; AREAS = [];
    D.indicadores.forEach(r => {
      let a = AREAS.find(x => x.n === r.area); if (!a) { a = { n: r.area, g: [], col: PAL[AREAS.length % PAL.length] }; AREAS.push(a); }
      let g = a.g.find(x => x.n === r.grupo); if (!g) { g = { n: r.grupo, i: [] }; a.g.push(g); }
      const I = prep(r, AREAS.indexOf(a)); I.col = a.col; I.areaO = a; g.i.push(I); IND.push(I); BYID[I.id] = I;
    });
    const TCS = IND.filter(x => x.tipo === "tc");
    IND.forEach(I => { if (I.tipo === "idx" && I.scian) { const t = TCS.find(x => x.scian === I.scian); if (t) cicloDeTC(I, t); } });
    [["737121", "737222"], ["736407", "736888"]].forEach(([a, b]) => cicloDeTC(BYID[a], BYID[b]));
    VINC.forEach(([a, b]) => cicloVinc(BYID[a], BYID[b]));
  }
  const fv = (I, v, d) => nf(v, d == null ? I.dg : d) + (I.u === "%" ? "%" : I.u === "pts" ? " pts" : "");
  const fy = (I, v) => v == null ? "—" : (v >= 0 ? "+" : "−") + nf(Math.abs(v), I.dif ? (Math.abs(v) >= 100 ? 0 : 1) : 1) + (I.dif ? (I.pct ? " pp" : I.u === "pts" ? " pts" : " " + (I.u === "mdd" ? "mdd" : "")) : "%");
  const cc = v => v == null ? "nu" : v > 0 ? "up" : v < 0 ? "dn" : "nu";

  /* ───────── tooltip ───────── */
  function tip(html, e) {
    if (!html) { tipEl.style.opacity = 0; return; }
    tipEl.innerHTML = html; tipEl.style.opacity = 1;
    const w = tipEl.offsetWidth, h = tipEl.offsetHeight; let x = e.clientX + 16, y = e.clientY + 14;
    if (x + w > innerWidth - 8) x = e.clientX - w - 16; if (y + h > innerHeight - 8) y = e.clientY - h - 14;
    tipEl.style.transform = `translate(${Math.max(8, x)}px,${Math.max(8, y)}px)`;
  }
  const tipI = (I, o) => `<b>${esc(I.nombre)}</b><div class="tp-s">${esc(o.l)}</div><div class="tp-v" style="color:${I.col}">${fv(I, o.v)}</div>` +
    (o.yoy != null ? `<div class="tp-r ${cc(o.yoy)}">Var. anual ${fy(I, o.yoy)}</div>` : "") + (o.mom != null ? `<div class="tp-r ${cc(o.mom)}">Vs. periodo previo ${fy(I, o.mom)}</div>` : "");
  const ticksN = (lo, hi, n) => { const st0 = (hi - lo) / n, p = Math.pow(10, Math.floor(Math.log10(st0))), f = st0 / p, st = (f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10) * p, a = []; for (let v = Math.ceil(lo / st) * st; v <= hi + 1e-9; v += st) a.push(+v.toFixed(10)); return a; };
  const tdec = (a) => { const st = a.length > 1 ? Math.abs(a[1] - a[0]) : 1; return st >= 1 ? 0 : st >= .1 ? 1 : 2; };
  const lin = (a, b, t) => a + (b - a) * t;

  /* ───────── pronóstico estadístico (suavizamiento exponencial amortiguado con estacionalidad) ───────── */
  const FCIDS = new Set(["910392", "910399", "910406", "334452", "737121", "737145", "736407", "736526", "737371", "737374", "444603", "444602", "444610", "454168", "701401", "701400", "6204198542", "6204198547", "6204198549", "702094", "702100", "710380", "6207131346"]);
  function holt(y, m, h) {
    const N = y.length, use = m > 1 && N >= 2 * m + 4 ? m : 1, phi = .94, AS = [.1, .3, .5, .7], BS = [.02, .1, .25], GS = use > 1 ? [.05, .2, .4] : [0];
    const run = (a, b, g) => {
      let lev, tr, sea = [];
      if (use > 1) { const s1 = y.slice(0, use).reduce((p, c) => p + c, 0) / use, s2 = y.slice(use, 2 * use).reduce((p, c) => p + c, 0) / use; lev = s1; tr = (s2 - s1) / use; for (let i = 0; i < use; i++) sea[i] = y[i] - s1; }
      else { const k = Math.min(N - 1, 3); lev = y[0]; tr = (y[k] - y[0]) / (k || 1); }
      let sse = 0; const res = [];
      for (let t = use > 1 ? use : 1; t < N; t++) {
        const s = use > 1 ? sea[t % use] : 0, f = lev + phi * tr + s, e = y[t] - f; sse += e * e; res.push(e);
        const ln = a * (y[t] - s) + (1 - a) * (lev + phi * tr); tr = b * (ln - lev) + (1 - b) * phi * tr; if (use > 1) sea[t % use] = g * (y[t] - ln) + (1 - g) * s; lev = ln;
      }
      return { sse, res, lev, tr, sea, a, b, g };
    };
    let best = null; AS.forEach(a => BS.forEach(b => GS.forEach(g => { const r = run(a, b, g); if (isFinite(r.sse) && (!best || r.sse < best.sse)) best = r; })));
    if (!best) return null;
    const rr = best.res.slice(-Math.max(24, use * 4)), sd = Math.sqrt(rr.reduce((p, c) => p + c * c, 0) / (rr.length || 1)) || 0, out = []; let cum = 0, ph = 1, vs = 1;
    for (let k = 1; k <= h; k++) {
      ph *= phi; cum += ph; const s = use > 1 ? best.sea[(N + k - 1) % use] : 0, v = best.lev + best.tr * cum + s;
      if (k > 1) { const j = k - 1; let cj = best.a * (1 + best.b * (j * (j + 1) / 2) / j * .6); if (use > 1 && j % use === 0) cj += best.g * (1 - best.a); vs += cj * cj; }
      const w = 1.2816 * sd * Math.sqrt(vs); out.push({ v, lo: v - w, hi: v + w });
    }
    return out;
  }
  // pronóstico de una serie: horizonte 12 meses / 4 trimestres; devuelve puntos con nivel y variación anual
  function prono(I) {
    if (I._fc !== undefined) return I._fc; I._fc = null;
    if (!I.fc || I.ppy < 4 || I.s.length < 36 || I.est[0] === "old") return null;
    const h = I.ppy === 12 ? 12 : 4, N = Math.min(I.s.length, I.ppy === 12 ? 120 : 60), y = I.s.slice(-N).map(o => o.v), f = holt(y, I.ppy, h); if (!f) return null;
    const last = I.s[I.s.length - 1], by = {}; I.s.forEach(o => { by[o.t] = o.v; });
    const pos = I.min > 0, pts = f.map((p, k) => { const t = last.t + k + 1, vb = by[t - I.ppy] != null ? by[t - I.ppy] : (k >= I.ppy && f[k - I.ppy] ? f[k - I.ppy].v : null), lo = pos ? Math.max(0, p.lo) : p.lo, ch = (v) => vb == null ? null : I.dif ? v - vb : vb > 0 ? (v / vb - 1) * 100 : null;
      by[t] = p.v; return { t, tt: t / I.ppy, l: periodoL(t, I.ppy), v: p.v, lo, hi: p.hi, yoy: ch(p.v), ylo: ch(lo), yhi: ch(p.hi) }; });
    I._fc = pts; return pts;
  }
  const fcLine = (I, tipo) => { const p = prono(I); return p ? p.map(q => ({ t: q.tt, l: q.l, v: tipo === "yoy" ? q.yoy : q.v })).filter(q => q.v != null) : null; };
  const tipF = (I, q, mode) => `<b>${esc(I.nombre)}</b><div class="tp-s">Pronóstico · ${esc(q.l)}</div><div class="tp-v" style="color:${I.col}">${mode === "anual" ? fy(I, q.v) : fv(I, q.v)}</div><div class="tp-r nu">Rango probable (80%): ${mode === "anual" ? fy(I, q.lo) + " a " + fy(I, q.hi) : fv(I, q.lo) + " a " + fv(I, q.hi)}</div><div class="tp-r nu">Estimación estadística, no oficial</div>`;

  /* ───────── suavizado: LOESS (regresión lineal local, pesos tricúbicos) y media móvil ───────── */
  function loess(y, alpha) {
    const idx = []; y.forEach((v, i) => { if (v != null && isFinite(v)) idx.push(i); });
    const n = idx.length; if (n < 6) return y.slice();
    const k = Math.max(5, Math.min(n, Math.round(alpha * n))), out = y.slice();
    for (let a = 0; a < n; a++) {
      const x0 = idx[a], lo = Math.max(0, Math.min(a - (k >> 1), n - k)), hi = lo + k - 1, dmax = Math.max(Math.abs(idx[lo] - x0), Math.abs(idx[hi] - x0)) || 1;
      let sw = 0, swx = 0, swy = 0, swxx = 0, swxy = 0;
      for (let j = lo; j <= hi; j++) { const dx = idx[j] - x0, u = Math.abs(dx) / (dmax * 1.0001), w = u < 1 ? Math.pow(1 - u * u * u, 3) : 0, yy = y[idx[j]]; sw += w; swx += w * dx; swy += w * yy; swxx += w * dx * dx; swxy += w * dx * yy; }
      const den = sw * swxx - swx * swx; out[x0] = Math.abs(den) < 1e-12 ? swy / (sw || 1) : (swy * swxx - swx * swxy) / den;
    }
    return out;
  }
  function mmovil(y, w) {   // media móvil centrada (la ventana se acorta en los extremos)
    const out = y.slice(), h = w >> 1; for (let i = 0; i < y.length; i++) { if (y[i] == null) continue; let a = 0, c = 0; for (let j = Math.max(0, i - h); j <= Math.min(y.length - 1, i + h); j++) if (y[j] != null) { a += y[j]; c++; } out[i] = a / c; }
    return out;
  }
  const SUAV = { l1: ["LOESS suave", .1], l2: ["LOESS medio", .25], l3: ["LOESS fuerte", .45], m: ["Media móvil (1 año)", 0] };
  function suavizaSerie(I, k) {
    I._sm = I._sm || {}; if (I._sm[k]) return I._sm[k];
    const lv = I.s.map(o => o.v), yy = I.s.map(o => o.yoy), f = k === "m" ? (a => mmovil(a, I.ppy === 1 ? 3 : I.ppy + 1)) : (a => loess(a, SUAV[k][1]));
    return (I._sm[k] = { v: f(lv), y: f(yy), m: f(I.s.map(o => o.mom)) });
  }

  function serieChecks(I) {   // casillas para elegir qué series ver en la gráfica de evolución
    const st = S.serie, ck = (k, t, on, tit) => `<button type="button" class="eco-ck" data-k="${k}" title="${tit || ""}"><span class="eco-ckb${on ? " on" : ""}">✓</span>${t}</button>`;
    return `<div class="eco-chks" id="ecoMode">${ck("nivel", st.suav ? "Nivel suavizado" : "Nivel", st.vn !== false)}${ck("anual", "Variación anual", st.va !== false)}${I.ppy > 1 && !I.pct ? ck("mom", "Variación periodo previo", st.vm === true, "Cambio respecto del periodo inmediato anterior (línea rosa)") : ""}${I.fc && prono(I) ? ck("fc", "Pronóstico", st.fc !== false, "Pronóstico estadístico (línea punteada)") : ""}${st.suav ? ck("og", "Nivel original", st.og !== false, "Muestra la serie sin suavizar, tenue, detrás de la suavizada") : ""}</div>`;
  }
  const ckOn = k => k === "nivel" ? S.serie.vn !== false : k === "anual" ? S.serie.va !== false : k === "mom" ? S.serie.vm === true : k === "fc" ? S.serie.fc !== false : S.serie.og !== false;
  /* ───────── gráfica principal de la serie ───────── */
  function drawSerie(el, I) {
    const W = Math.max(320, el.clientWidth), H = W < 520 ? 280 : 360, m = { l: 62, r: 20, t: 16, b: 34 }, st = S.serie, vm = st.vm === true && I.ppy > 1 && !I.pct && I.s.some(o => o.mom != null), va = st.va !== false, vn = st.vn !== false || (!va && !vm), mode = vn ? (va || vm ? "ambas" : "nivel") : "anual"; if (mode === "ambas") m.r = 58;
    const nmax = st.rango ? Math.min(I.s.length, Math.round(st.rango * I.ppy / 12) || I.s.length) : I.s.length, s0 = Math.max(0, I.s.length - Math.max(nmax, 6)), len0 = I.s.length - s0, zk = "s:" + I.id, zz = S.zoom[zk] || [0, 1]; let i0 = s0 + Math.floor(zz[0] * len0), i1 = s0 + Math.ceil(zz[1] * len0); if (i1 - i0 < 6) { i1 = Math.min(I.s.length, i0 + 6); i0 = Math.max(s0, i1 - 6); } const d = I.s.slice(i0, i1), n = d.length, atEnd = i1 >= I.s.length;
    const f = mode === "anual" ? (va ? (o => o.yoy) : (o => o.mom)) : (o => o.v), off = i0, SM = st.suav ? suavizaSerie(I, st.suav) : null, pts = d.map((o, i) => ({ o, i, v: SM ? (mode === "anual" ? (va ? SM.y : SM.m)[off + i] : SM.v[off + i]) : f(o), raw: f(o) })).filter(p => p.v != null), mvArr = vm ? d.map((o, i) => ({ i, v: SM ? SM.m[off + i] : o.mom })).filter(q => q.v != null) : [];
    if (!pts.length) { el.innerHTML = '<div class="eco-vacio">Sin datos suficientes para esta vista.</div>'; return; }
    const FC = I.fc && st.fc !== false && atEnd && !(mode === "anual" && !va) ? prono(I) : null, fcv = (FC || []).map((p, k) => ({ i: n + k, l: p.l, v: mode === "anual" ? p.yoy : p.v, lo: mode === "anual" ? p.ylo : p.lo, hi: mode === "anual" ? p.yhi : p.hi })).filter(q => q.v != null), nn = n + (FC ? FC.length : 0), ALL = pts.concat(fcv.map(q => ({ o: null, i: q.i, v: q.v, fc: q }))); S.curI = I;
    const ext = pts.flatMap(p => [p.v, p.raw]).filter(v => v != null); let lo = Math.min(...ext), hi = Math.max(...ext); fcv.forEach(q => { lo = Math.min(lo, q.lo != null ? q.lo : q.v); hi = Math.max(hi, q.hi != null ? q.hi : q.v); });
    if (mode === "anual" && va && vm) mvArr.forEach(q => { lo = Math.min(lo, q.v); hi = Math.max(hi, q.v); });
    if (mode === "anual") { lo = Math.min(lo, 0); hi = Math.max(hi, 0); } else if (I.th50) { lo = Math.min(lo, I.thr); hi = Math.max(hi, I.thr); }
    const pad = (hi - lo) * .08 || 1; lo -= pad; hi += pad;
    const X = i => m.l + (nn === 1 ? .5 : i / (nn - 1)) * (W - m.l - m.r), Y = v => m.t + (1 - (v - lo) / (hi - lo)) * (H - m.t - m.b);
    const tk = ticksN(lo, hi, 5), td = tdec(tk);
    let s = `<defs><linearGradient id="ecoAr" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${I.col}" stop-opacity=".42"/><stop offset="1" stop-color="${I.col}" stop-opacity="0"/></linearGradient>`;
    const base = mode === "anual" ? Y(0) : I.th50 ? Y(I.thr) : null;
    if (mode !== "anual" && I.th50) { const o = Math.max(0, Math.min(1, (Y(I.thr) - m.t) / (H - m.t - m.b))); s += `<linearGradient id="ecoLn" x1="0" y1="${m.t}" x2="0" y2="${H - m.b}" gradientUnits="userSpaceOnUse"><stop offset="0" stop-color="#34d399"/><stop offset="${o}" stop-color="#34d399"/><stop offset="${o}" stop-color="#fb7185"/><stop offset="1" stop-color="#fb7185"/></linearGradient>`; }
    s += `<filter id="ecoGl" x="-20%" y="-20%" width="140%" height="140%"><feGaussianBlur stdDeviation="3.5" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs>`;
    tk.forEach(v => { s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(v).toFixed(1)}" y2="${Y(v).toFixed(1)}" class="eco-gr"/><text x="${m.l - 8}" y="${(Y(v) + 4).toFixed(1)}" class="eco-ax" text-anchor="end">${nf(v, td)}</text>`; });
    const nl = Math.min(8, Math.max(2, Math.floor((W - m.l) / 90)));
    for (let k = 0; k < nl; k++) { const i = Math.round(k * (nn - 1) / (nl - 1)); s += `<text x="${X(i).toFixed(1)}" y="${H - 10}" class="eco-ax" text-anchor="${k === 0 ? "start" : k === nl - 1 ? "end" : "middle"}">${i < n ? d[i].l : FC[i - n].l}</text>`; }
    if (mode === "anual") {
      const bw = Math.max(1.5, Math.min(22, (W - m.l - m.r) / nn * .72));
      pts.forEach((p, k) => { const y0 = Y(0), y1 = Y(p.v), up = p.v >= 0; s += `<rect class="eco-bar" x="${(X(p.i) - bw / 2).toFixed(1)}" y="${Math.min(y0, y1).toFixed(1)}" width="${bw.toFixed(1)}" height="${Math.max(1, Math.abs(y1 - y0)).toFixed(1)}" rx="${Math.min(3, bw / 2)}" fill="${up ? "#34d399" : "#fb7185"}" style="transform-origin:center ${up ? "bottom" : "top"};transform-box:fill-box;animation-delay:${Math.min(900, k * 6)}ms"/>`; });
      fcv.forEach(q => { const y0 = Y(0), y1 = Y(q.v), up = q.v >= 0; s += `<rect x="${(X(q.i) - bw / 2).toFixed(1)}" y="${Math.min(y0, y1).toFixed(1)}" width="${bw.toFixed(1)}" height="${Math.max(1, Math.abs(y1 - y0)).toFixed(1)}" rx="${Math.min(3, bw / 2)}" fill="${up ? "#34d399" : "#fb7185"}" fill-opacity=".22" stroke="${up ? "#34d399" : "#fb7185"}" stroke-dasharray="3 3"/>`; });
      if (fcv.length) s += `<line x1="${X(n - 1).toFixed(1)}" x2="${X(n - 1).toFixed(1)}" y1="${m.t}" y2="${H - m.b}" class="eco-ch"/><text x="${(X(n - 1) + 6).toFixed(1)}" y="${m.t + 10}" class="eco-ax">pronóstico →</text>`;
      if (va && vm && mvArr.length) s += `<path d="${mvArr.map((q, k) => (k ? "L" : "M") + X(q.i).toFixed(1) + " " + Y(q.v).toFixed(1)).join("")}" fill="none" stroke="#f472b6" stroke-width="1.9" stroke-linejoin="round" stroke-linecap="round"/>`;
      s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(0).toFixed(1)}" y2="${Y(0).toFixed(1)}" stroke="#94a3b8" stroke-opacity=".6"/>`;
    } else {
      if (mode === "ambas") {
        const yv = va ? d.map((o, i) => ({ i, v: SM ? SM.y[off + i] : o.yoy })).filter(q => q.v != null) : [], fy2 = va ? (FC || []).map((q, k) => ({ i: n + k, v: q.yoy })).filter(q => q.v != null) : [], all2 = yv.concat(fy2).concat(mvArr).map(q => q.v);
        let ylo = Math.min(0, ...all2), yhi = Math.max(0, ...all2); const yp = (yhi - ylo) * .1 || 1; ylo -= yp; yhi += yp; const Y2 = v => m.t + (1 - (v - ylo) / (yhi - ylo)) * (H - m.t - m.b), bw2 = Math.max(1.4, Math.min(18, (W - m.l - m.r) / nn * .7)), t2 = ticksN(ylo, yhi, 4), d2 = tdec(t2);
        yv.forEach((q, k) => { const y0 = Y2(0), y1 = Y2(q.v), up = q.v >= 0; s += `<rect class="eco-bar" x="${(X(q.i) - bw2 / 2).toFixed(1)}" y="${Math.min(y0, y1).toFixed(1)}" width="${bw2.toFixed(1)}" height="${Math.max(1, Math.abs(y1 - y0)).toFixed(1)}" rx="${Math.min(2, bw2 / 2)}" fill="${up ? "#34d399" : "#fb7185"}" fill-opacity=".34" style="transform-origin:center ${up ? "bottom" : "top"};transform-box:fill-box;animation-delay:${Math.min(700, k * 5)}ms"/>`; });
        fy2.forEach(q => { const y0 = Y2(0), y1 = Y2(q.v), up = q.v >= 0; s += `<rect x="${(X(q.i) - bw2 / 2).toFixed(1)}" y="${Math.min(y0, y1).toFixed(1)}" width="${bw2.toFixed(1)}" height="${Math.max(1, Math.abs(y1 - y0)).toFixed(1)}" rx="${Math.min(2, bw2 / 2)}" fill="${up ? "#34d399" : "#fb7185"}" fill-opacity=".14" stroke="${up ? "#34d399" : "#fb7185"}" stroke-opacity=".6" stroke-dasharray="2 2"/>`; });
        s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y2(0).toFixed(1)}" y2="${Y2(0).toFixed(1)}" stroke="#94a3b8" stroke-opacity=".45" stroke-dasharray="1 4"/>` + t2.map(v => `<text x="${W - m.r + 8}" y="${(Y2(v) + 4).toFixed(1)}" class="eco-ax" style="fill:#fbbf24">${nf(v, d2)}${I.dif ? "" : "%"}</text>`).join("") + `<text x="${W - m.r + 8}" y="${m.t - 4}" class="eco-ax" style="fill:#fbbf24;font-weight:700">${va && vm ? "anual · previo" : va ? (I.dif ? "var. anual (pp)" : "var. anual") : "vs previo"}</text>`;
        if (vm && mvArr.length) s += `<path d="${mvArr.map((q, k) => (k ? "L" : "M") + X(q.i).toFixed(1) + " " + Y2(q.v).toFixed(1)).join("")}" fill="none" stroke="#f472b6" stroke-width="1.9" stroke-linejoin="round" stroke-linecap="round"/>`;
      }
      const line = pts.map((p, k) => (k ? "L" : "M") + X(p.i).toFixed(1) + " " + Y(p.v).toFixed(1)).join(""), yb = base != null ? base : H - m.b;
      s += `<path d="${line}L${X(pts[pts.length - 1].i).toFixed(1)} ${yb.toFixed(1)}L${X(pts[0].i).toFixed(1)} ${yb.toFixed(1)}Z" fill="url(#ecoAr)" class="eco-area"/>`;
      if (I.th50) s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(I.thr).toFixed(1)}" y2="${Y(I.thr).toFixed(1)}" stroke="#e2e8f0" stroke-opacity=".55" stroke-dasharray="5 5"/><text x="${W - m.r - 4}" y="${(Y(I.thr) - 6).toFixed(1)}" class="eco-ax" text-anchor="end">${I.thr === 100 ? "tendencia = 100" : "umbral neutral 50"}</text>`;
      if (SM && st.og !== false) s += `<path d="${pts.filter(p => p.raw != null).map((p, k) => (k ? "L" : "M") + X(p.i).toFixed(1) + " " + Y(p.raw).toFixed(1)).join("")}" fill="none" stroke="${I.col}" stroke-width="1.2" stroke-opacity=".32" stroke-linejoin="round"/>`;
      s += `<path d="${line}" pathLength="1" class="eco-ln" fill="none" stroke="${I.th50 ? "url(#ecoLn)" : I.col}" stroke-width="2.6" stroke-linejoin="round" stroke-linecap="round" filter="url(#ecoGl)"/>`;
      if (fcv.length) { const a0 = pts[pts.length - 1], P0 = `${X(a0.i).toFixed(1)} ${Y(a0.v).toFixed(1)}`, up = fcv.map(q => `L${X(q.i).toFixed(1)} ${Y(q.hi != null ? q.hi : q.v).toFixed(1)}`).join(""), dn = fcv.slice().reverse().map(q => `L${X(q.i).toFixed(1)} ${Y(q.lo != null ? q.lo : q.v).toFixed(1)}`).join("");
        s += `<path d="M${P0}${up}${dn}Z" fill="${I.col}" fill-opacity=".13"/><path d="M${P0}${fcv.map(q => `L${X(q.i).toFixed(1)} ${Y(q.v).toFixed(1)}`).join("")}" fill="none" stroke="${I.col}" stroke-width="3" stroke-linecap="round" stroke-dasharray="0.1 7"/><line x1="${X(n - 1).toFixed(1)}" x2="${X(n - 1).toFixed(1)}" y1="${m.t}" y2="${H - m.b}" class="eco-ch"/><text x="${(X(n - 1) + 6).toFixed(1)}" y="${m.t + 10}" class="eco-ax">pronóstico →</text>`; }
      const lp = pts[pts.length - 1]; s += `<circle cx="${X(lp.i).toFixed(1)}" cy="${Y(lp.v).toFixed(1)}" r="5" fill="${I.col}" class="eco-pulse"/><circle cx="${X(lp.i).toFixed(1)}" cy="${Y(lp.v).toFixed(1)}" r="4.5" fill="#fff" stroke="${I.col}" stroke-width="2.5"/>`;
    }
    s += `<g id="ecoCh" style="display:none"><line class="eco-ch" y1="${m.t}" y2="${H - m.b}"/><circle r="5.5" fill="#0a0f1f" stroke="#fff" stroke-width="2.4"/></g><rect id="ecoHit" x="${m.l}" y="${m.t}" width="${W - m.l - m.r}" height="${H - m.t - m.b}" fill="transparent"/>`;
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="${esc(I.nombre)}">${s}</svg>`;
    brush(el, zk, I.s.slice(s0).map(o => o.v), () => drawSerie(el, I), d[0].l + " – " + d[d.length - 1].l);
    const g = $("#ecoCh", el), ln = $("line", g), dot = $("circle", g), hit = $("#ecoHit", el);
    hit.onmousemove = hit.ontouchmove = e => {
      const r = hit.getBoundingClientRect(), cx = (e.touches ? e.touches[0].clientX : e.clientX), i = Math.round((cx - r.left) / r.width * (nn - 1));
      let best = ALL[0]; ALL.forEach(p => { if (Math.abs(p.i - i) < Math.abs(best.i - i)) best = p; });
      g.style.display = ""; ln.setAttribute("x1", X(best.i)); ln.setAttribute("x2", X(best.i)); dot.setAttribute("cx", X(best.i)); dot.setAttribute("cy", Y(best.v));
      tip(best.fc ? tipF(I, best.fc, mode) : tipI(I, best.o) + (SM && best.v != null ? `<div class="tp-r nu">${SUAV[st.suav][0]}: <b>${mode === "anual" ? fy(I, best.v) : fv(I, best.v)}</b></div>` : ""), e.touches ? e.touches[0] : e);
    };
    hit.onmouseleave = () => { g.style.display = "none"; tip(null); };
  }

  /* ───────── reloj económico ───────── */
  function drawReloj(el, I, big) {
    if (!I.quad) { el.innerHTML = '<div class="eco-vacio"><b>No aplicable.</b><br>' + esc(noAplicaMsg(I)) + ' Se muestra solo su evolución.</div>'; return; }
    const W = Math.max(260, Math.min(el.clientWidth, big ? 760 : 520)), H = big ? Math.round(W * .74) : Math.round(W * .86), m = 30;
    const tr = I.s.filter(o => o.x != null && o.y != null).slice(-(big ? 30 : 24));
    if (tr.length < 3) { el.innerHTML = '<div class="eco-vacio">Se necesitan más observaciones para el reloj.</div>'; return; }
    const seq = I.s.filter(o => o.x != null && o.y != null).slice(-120);
    const R = Math.max(1.6, Math.max(...seq.map(o => Math.max(Math.abs(o.x), Math.abs(o.y)))) * 1.15), X = v => m + (v / R + 1) / 2 * (W - 2 * m), Y = v => m + (1 - (v / R + 1) / 2) * (H - 2 * m), cx = X(0), cy = Y(0);
    let s = `<defs><filter id="ecoGl2"><feGaussianBlur stdDeviation="3" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs>`;
    const qd = [["exp", cx, m, W - m, cy], ["des", cx, cy, W - m, H - m], ["con", m, cy, cx, H - m], ["rec", m, m, cx, cy]];
    qd.forEach(([k, x0, y0, x1, y1]) => { s += `<rect x="${x0}" y="${y0}" width="${x1 - x0}" height="${y1 - y0}" fill="${QD[k][1]}" opacity="${I.quad === k ? .2 : .08}"/>`; });
    s += `<line x1="${m}" x2="${W - m}" y1="${cy}" y2="${cy}" class="eco-ch"/><line x1="${cx}" x2="${cx}" y1="${m}" y2="${H - m}" class="eco-ch"/>`;
    [["exp", W - m - 6, m + 16, "end"], ["des", W - m - 6, H - m - 8, "end"], ["con", m + 6, H - m - 8, "start"], ["rec", m + 6, m + 16, "start"]].forEach(([k, x, y, a]) => { s += `<text x="${x}" y="${y}" text-anchor="${a}" class="eco-q" fill="${QD[k][1]}">${QD[k][0].toUpperCase()}</text>`; });
    s += `<text x="${W - m}" y="${cy - 6}" class="eco-ax" text-anchor="end">nivel vs tendencia →</text><text x="${cx + 6}" y="${m - 8}" class="eco-ax">↑ impulso</text>`;
    const trail = big ? 30 : 24, RL = el._rl && el._rl.raf ? (cancelAnimationFrame(el._rl.raf), null) : null, ST = el._rl = { pos: seq.length - 1, playing: false, raf: 0 };
    const kq = o => o.x >= 0 ? (o.y >= 0 ? "exp" : "des") : (o.y >= 0 ? "rec" : "con");
    const dyn = (pos, first) => { const t = seq.slice(Math.max(0, pos - trail + 1), pos + 1); if (!t.length) return "";
      let h = `<path d="${t.map((o, i) => (i ? "L" : "M") + X(o.x).toFixed(1) + " " + Y(o.y).toFixed(1)).join("")}"${first ? ' pathLength="1" class="eco-ln"' : ""} fill="none" stroke="${I.col}" stroke-opacity=".75" stroke-width="2.4" stroke-linejoin="round" filter="url(#ecoGl2)"/>`;
      t.forEach((o, i) => { const l = i === t.length - 1; h += `<circle class="${first ? "eco-dotp" : "eco-dotq"}" data-i="${i}" cx="${X(o.x).toFixed(1)}" cy="${Y(o.y).toFixed(1)}" r="${l ? 8 : 3 + 3 * i / t.length}" fill="${l ? "#fff" : I.col}" stroke="${I.col}" stroke-width="${l ? 3 : 0}"${first ? ` style="animation-delay:${i * 45}ms"` : ""}/>`; });
      const lo = t[t.length - 1]; return h + `<text x="${X(lo.x).toFixed(1)}" y="${(Y(lo.y) - 14).toFixed(1)}" text-anchor="middle" class="eco-q" fill="#fff">${esc(lo.l)}</text>`; };
    el.innerHTML = `<div class="eco-cybar eco-rlbar"><button type="button" class="eco-cyplay" data-rl="p" title="Reproducir / pausar la trayectoria">▶</button><button type="button" class="eco-cybtn" data-rl="r" title="Volver al inicio">⏮</button><input type="range" class="eco-cyslider" min="0" max="${seq.length - 1}" value="${ST.pos}" aria-label="Fecha"><span class="eco-cyfecha">${esc(seq[ST.pos].l)}</span></div><svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}"><g class="eco-rlg">${s}</g><g class="eco-rld">${dyn(ST.pos, true)}</g></svg>`;
    const svg = $("svg", el), gd = $(".eco-rld", svg), btn = $("[data-rl=p]", el), sl = $(".eco-cyslider", el), fe = $(".eco-cyfecha", el);
    const pinta = () => { gd.innerHTML = dyn(ST.pos, false); fe.textContent = seq[ST.pos].l; sl.value = ST.pos; btn.textContent = ST.playing ? "❚❚" : "▶"; };
    const para = () => { ST.playing = false; if (ST.raf) cancelAnimationFrame(ST.raf); ST.raf = 0; btn.textContent = "▶"; };
    function loop(ts) { if (!el.isConnected || !ST.playing) { ST.raf = 0; return; } if (!loop.t) loop.t = ts; if (ts - loop.t >= 120) { loop.t = ts; if (ST.pos >= seq.length - 1) { para(); pinta(); return; } ST.pos++; pinta(); } ST.raf = requestAnimationFrame(loop); }
    btn.onclick = () => { if (ST.playing) { para(); return; } if (ST.pos >= seq.length - 1) ST.pos = 0; ST.playing = true; loop.t = 0; pinta(); ST.raf = requestAnimationFrame(loop); };
    $("[data-rl=r]", el).onclick = () => { para(); ST.pos = 0; pinta(); };
    sl.oninput = () => { para(); ST.pos = +sl.value; pinta(); };
    svg.onmousemove = e => { const c = e.target.closest("circle[data-i]"); if (!c) { tip(null); return; } const t = seq.slice(Math.max(0, ST.pos - trail + 1), ST.pos + 1), o = t[+c.dataset.i]; if (!o) return; tip(`<b>${esc(I.nombre)}</b><div class="tp-s">${esc(o.l)}</div><div class="tp-v" style="color:${QD[kq(o)][1]}">${QD[kq(o)][0]}</div><div class="tp-r nu">Nivel vs tendencia: ${nf(o.x, 2)} σ</div><div class="tp-r nu">Impulso: ${nf(o.y, 2)}</div>`, e); };
    svg.onmouseleave = () => tip(null);
  }

  /* ───────── estacionalidad (año × mes) / histograma ───────── */
  function ramp(t) { const a = [[30, 41, 82], [76, 29, 149], [190, 24, 124], [251, 146, 60], [253, 224, 71]], f = Math.max(0, Math.min(1, t)) * (a.length - 1), i = Math.min(a.length - 2, Math.floor(f)), u = f - i; return "rgb(" + [0, 1, 2].map(k => Math.round(lin(a[i][k], a[i + 1][k], u))).join(",") + ")"; }
  function drawEstac(el, I) {
    if (I.ppy === 1) return drawHist(el, I);
    const W = Math.max(280, el.clientWidth), cols = I.ppy, yrs = [...new Set(I.s.map(o => o.yr))].slice(-12), ml = 44, mt = 22, cw = (W - ml - 6) / cols, ch = Math.min(26, 230 / yrs.length), H = mt + yrs.length * ch + 6;
    const vs = I.s.map(o => o.v), lo = Math.min(...vs), hi = Math.max(...vs), by = {}; I.s.forEach(o => by[o.yr + "-" + o.q] = o);
    let s = ""; (I.ppy === 12 ? MES : ["T1", "T2", "T3", "T4"]).forEach((t, c) => { s += `<text x="${ml + c * cw + cw / 2}" y="14" class="eco-ax" text-anchor="middle">${t}</text>`; });
    yrs.forEach((y, r) => { s += `<text x="${ml - 6}" y="${mt + r * ch + ch / 2 + 4}" class="eco-ax" text-anchor="end">${y}</text>`; for (let c = 1; c <= cols; c++) { const o = by[y + "-" + c]; if (o) s += `<rect class="eco-cell" data-k="${y}-${c}" x="${(ml + (c - 1) * cw + 1).toFixed(1)}" y="${mt + r * ch + 1}" width="${(cw - 2).toFixed(1)}" height="${ch - 2}" rx="4" fill="${ramp((o.v - lo) / (hi - lo || 1))}" style="animation-delay:${(r * cols + c) * 5}ms"/>`; } });
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg><div class="eco-lg"><span>${fv(I, lo)}</span><i style="background:linear-gradient(90deg,${ramp(0)},${ramp(.35)},${ramp(.7)},${ramp(1)})"></i><span>${fv(I, hi)}</span></div>`;
    $$(".eco-cell", el).forEach(c => { const o = by[c.dataset.k]; c.onmousemove = e => tip(tipI(I, o), e); c.onmouseleave = () => tip(null); });
  }
  function drawHist(el, I) {
    const W = Math.max(280, el.clientWidth), H = 250, m = { l: 36, r: 8, t: 12, b: 26 }, nb = Math.min(14, Math.max(6, Math.round(Math.sqrt(I.s.length) * 1.6))), step = (I.max - I.min) / nb || 1, bins = Array(nb).fill(0);
    I.s.forEach(o => { bins[Math.min(nb - 1, Math.floor((o.v - I.min) / step))]++; }); const mx = Math.max(...bins), lb = Math.min(nb - 1, Math.floor((I.last.v - I.min) / step)), bw = (W - m.l - m.r) / nb;
    let s = ""; bins.forEach((c, i) => { const h = c / mx * (H - m.t - m.b), x = m.l + i * bw; s += `<rect class="eco-bar" data-i="${i}" x="${(x + 2).toFixed(1)}" y="${(H - m.b - h).toFixed(1)}" width="${(bw - 4).toFixed(1)}" height="${h.toFixed(1)}" rx="5" fill="${i === lb ? "#fff" : I.col}" fill-opacity="${i === lb ? 1 : .55}" style="transform-origin:center bottom;transform-box:fill-box;animation-delay:${i * 35}ms"/>`; if (i % 2 === 0 || nb < 9) s += `<text x="${(x + bw / 2).toFixed(1)}" y="${H - 8}" class="eco-ax" text-anchor="middle">${nf(I.min + step * (i + .5), I.max - I.min > 100 ? 0 : 1)}</text>`; });
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg>`;
    $$(".eco-bar", el).forEach(b => { const i = +b.dataset.i; b.onmousemove = e => tip(`<b>${nf(I.min + step * i, 1)} – ${nf(I.min + step * (i + 1), 1)}</b><div class="tp-r nu">${bins[i]} observaciones (${nf(bins[i] / I.s.length * 100, 0)}%)</div>${i === lb ? '<div class="tp-r up">Aquí está el último dato</div>' : ""}`, e); b.onmouseleave = () => tip(null); });
  }

  /* ───────── piezas del tablero ───────── */
  function spark(I, n, w, h) {
    const d = I.s.slice(-n), lo = Math.min(...d.map(o => o.v)), hi = Math.max(...d.map(o => o.v)), X = i => 2 + i / (d.length - 1 || 1) * (w - 4), Y = v => h - 3 - (v - lo) / (hi - lo || 1) * (h - 8), p = d.map((o, i) => (i ? "L" : "M") + X(i).toFixed(1) + " " + Y(o.v).toFixed(1)).join("");
    return `<svg viewBox="0 0 ${w} ${h}" width="100%" height="${h}" preserveAspectRatio="none"><path d="${p}L${w - 2} ${h}L2 ${h}Z" fill="${I.col}" opacity=".16"/><path d="${p}" pathLength="1" class="eco-ln" fill="none" stroke="${I.col}" stroke-width="2" stroke-linejoin="round" vector-effect="non-scaling-stroke"/><circle cx="${X(d.length - 1)}" cy="${Y(d[d.length - 1].v)}" r="3" fill="${I.col}"/></svg>`;
  }
  const dotE = I => `<i class="eco-st ${I.est[0]}" title="${I.est[1]} · último dato ${I.last.l}"></i>`;
  const chipY = I => I.last.yoy == null ? "" : `<span class="eco-ch2 ${cc(I.last.yoy)}">${I.last.yoy > 0 ? "▲" : I.last.yoy < 0 ? "▼" : "■"} ${fy(I, I.last.yoy)} anual</span>`;
  /* indicadores relacionados con un sector: no forman parte de los filtros (lugar/giro), solo se relacionan por sector */
  const relDe = k => IND.filter(x => x.sectores && x.sectores.includes(k) && !x.scian);
  const REL_G = [["mismo", "Del mismo sector · otra fuente", "encuestas y series específicas de este sector (opinión empresarial)"], ["agr", "Agregados que incluyen al sector", "miden un conjunto de sectores, entre ellos este"]];
  const relGrupos = (k, l) => { const L = l || relDe(k); return { mismo: L.filter(x => x.sectores.length === 1), agr: L.filter(x => x.sectores.length !== 1) }; };
  const varsRel = (I, list) => list.filter(x => x.id !== I.id && (x.nombre === I.nombre || x.grupo === I.grupo)).map(x => ({ I: x, t: x.nombre !== I.nombre ? x.nombre : x.grupo }));
  function relMenu(sc) {
    const l = relDe(sc.k); if (!l.length) return ""; const G = relGrupos(sc.k, l);
    const item = I => `<button type="button" class="eco-n eco-n4${S.sel === "i:" + I.id ? " on" : ""}" data-go="i:${I.id}"><span class="tx">${esc(I.nombre === sc.n ? I.grupo : (I.nombre.includes(I.grupo) || I.grupo.includes(I.nombre) ? I.nombre : I.nombre + " · " + I.grupo))}<small>${esc(alcance(I))} · ${esc((I.frecuencia || "").toLowerCase())}</small></span></button>`;
    let h = `<div class="eco-relh">Relacionados con ${esc(sc.n)}<small>no son parte del filtro: solo se relacionan con el sector</small></div>`;
    REL_G.forEach(([k, t, d]) => { if (G[k].length) h += `<div class="eco-relg">${t}<small>${d}</small></div>` + G[k].map(item).join(""); });
    return h;
  }
  function relNota(sc, rel, stK) {
    const hay = !!stK.length;
    return `<div class="eco-relnota"><b>No forman parte de los filtros.</b> El lugar y el giro elegidos no los filtran: se listan aquí solo porque están <b>relacionados con ${esc(sc.n)}</b>. Cada tarjeta muestra el último dato, su variación anual, la mini-serie, el periodo, la frecuencia y el <b>ámbito geográfico</b>; la línea «Variantes» enlaza con la otra versión de la misma serie (por ejemplo nivel y tendencia).
      <div class="eco-geo"><span class="ok">Nacional ✓ <small>todas estas series</small></span><span class="${hay ? "ok" : "no"}">Estados ${hay ? "✓" : "✗"} <small>${hay ? "ITAEE de " + esc(sc.n) + " (solo variación anual, ver el mapa)" : "sin serie estatal"}</small></span><span class="no">Ciudades ✗ <small>la ENOE no se publica por actividad</small></span></div></div>`;
  }
  function cards(list, conRel, todos) {
    return `<div class="eco-cards">${list.map((I, k) => `<button type="button" class="eco-card" data-go="i:${I.id}" style="--c:${I.col};animation-delay:${k * 40}ms"><div class="ct"><span>${esc(I.nombre)}</span>${dotE(I)}</div><div class="cg">${esc(I.grupo)}</div><div class="cv">${fv(I, I.last.v)}<small>${esc(I.u === "pts" || I.u === "%" ? "" : I.u)}</small></div>${chipY(I)}<div class="cs">${spark(I, 36, 220, 46)}</div><div class="cp">${esc(I.last.l)} · ${esc(I.frecuencia)} · ${esc(alcance(I))}</div>${conRel ? (() => { const v = varsRel(I, todos || list); return `<div class="cvar">${v.length ? "Variantes: " + v.map(q => `<span class="eco-vr" data-go="i:${q.I.id}" title="Abrir esta variante">${esc(q.t)}</span>`).join(" · ") : "Sin otras variantes aquí"}</div>`; })() : ""}</button>`).join("")}</div>`;
  }
  function rank(el, list) {
    const L = list.filter(I => I.last.x != null).map(I => ({ I, z: I.last.x })).sort((a, b) => b.z - a.z), W = Math.max(300, el.clientWidth), rh = 30, H = L.length * rh + 28, mx = Math.max(2, ...L.map(a => Math.abs(a.z))) * 1.05, lw = Math.min(250, W * .42), x0 = lw + (W - lw - 20) / 2, sc = (W - lw - 20) / 2 / mx;
    let s = `<line x1="${x0}" x2="${x0}" y1="6" y2="${H - 22}" class="eco-ch"/>`;
    L.forEach(({ I, z }, i) => { const y = 8 + i * rh, w = Math.abs(z) * sc; s += `<text x="${lw - 8}" y="${y + 17}" class="eco-ax" text-anchor="end" style="fill:#cbd5e1">${esc(I.nombre.length > 36 ? I.nombre.slice(0, 35) + "…" : I.nombre)}</text><rect class="eco-hb" data-id="${I.id}" x="${(z >= 0 ? x0 : x0 - w).toFixed(1)}" y="${y + 4}" width="${w.toFixed(1)}" height="18" rx="6" fill="${z >= 0 ? "#34d399" : "#fb7185"}" style="transform-origin:${z >= 0 ? "left" : "right"} center;transform-box:fill-box;animation-delay:${i * 40}ms"/><text x="${(z >= 0 ? x0 + w + 6 : x0 - w - 6).toFixed(1)}" y="${y + 17}" class="eco-ax" text-anchor="${z >= 0 ? "start" : "end"}" style="fill:#e2e8f0">${(z >= 0 ? "+" : "−") + nf(Math.abs(z), 1)}σ</text>`; });
    s += `<text x="${x0 + (W - x0) / 2}" y="${H - 4}" class="eco-ax" text-anchor="middle">por encima de su tendencia →</text><text x="${lw + (x0 - lw) / 2}" y="${H - 4}" class="eco-ax" text-anchor="middle">← por debajo</text>`;
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg>`;
    $$(".eco-hb", el).forEach(b => { const I = BYID[b.dataset.id]; b.onmousemove = e => tip(`<b>${esc(I.nombre)}</b><div class="tp-s">${esc(I.last.l)}</div><div class="tp-v" style="color:${I.col}">${fv(I, I.last.v)}</div>${I.last.yoy != null ? `<div class="tp-r ${cc(I.last.yoy)}">Var. anual ${fy(I, I.last.yoy)}</div>` : ""}<div class="tp-r nu">${nf(I.last.x, 2)} σ vs su tendencia</div>`, e); b.onmouseleave = () => tip(null); b.onclick = () => ir("i:" + I.id); b.style.cursor = "pointer"; });
  }
  function heatZ(el, list, nper) {
    const L = list.filter(I => I.ppy === 12 && I.s.length > 18); if (L.length < 2) { el.innerHTML = '<div class="eco-vacio">Se necesitan al menos 2 series mensuales para el mapa de calor.</div>'; return; }
    const tmax = Math.max(...L.map(I => I.last.t)), ts = Array.from({ length: nper }, (_, i) => tmax - nper + 1 + i), W = Math.max(320, el.clientWidth), lw = Math.min(230, W * .34), cw = (W - lw - 8) / nper, rh = 26, H = 22 + L.length * rh;
    const col = z => { const t = Math.max(-1, Math.min(1, z / 2.2)); return t >= 0 ? `rgb(${Math.round(lin(30, 52, t))},${Math.round(lin(41, 211, t))},${Math.round(lin(70, 153, t))})` : `rgb(${Math.round(lin(30, 251, -t))},${Math.round(lin(41, 113, -t))},${Math.round(lin(70, 133, -t))})`; };
    let s = ""; ts.forEach((t, c) => { if (c % 3 === 0 || nper < 13) { const m = t % 12, y = Math.floor(t / 12); s += `<text x="${lw + c * cw + cw / 2}" y="12" class="eco-ax" text-anchor="middle">${MES[m]} ${String(y).slice(2)}</text>`; } });
    L.forEach((I, r) => { const by = {}; I.s.forEach(o => by[o.t] = o); s += `<text x="${lw - 8}" y="${22 + r * rh + 17}" class="eco-ax" text-anchor="end" style="fill:#cbd5e1">${esc(I.nombre.length > 32 ? I.nombre.slice(0, 31) + "…" : I.nombre)}</text>`; ts.forEach((t, c) => { const o = by[t]; s += `<rect class="eco-cell" data-i="${I.id}" data-t="${t}" x="${(lw + c * cw + 1).toFixed(1)}" y="${22 + r * rh + 2}" width="${(cw - 2).toFixed(1)}" height="${rh - 4}" rx="4" fill="${o && o.x != null ? col(o.x) : "#1e293b"}" style="animation-delay:${(r * nper + c) * 3}ms"/>`; }); });
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg><div class="eco-lg"><span>por debajo de su tendencia</span><i style="background:linear-gradient(90deg,#fb7185,#1e2946,#34d399)"></i><span>por encima</span></div>`;
    $$(".eco-cell", el).forEach(c => { const I = BYID[c.dataset.i], o = I.s.find(q => q.t === +c.dataset.t); c.onmousemove = e => tip(o ? tipI(I, o) + `<div class="tp-r nu">${o.x != null ? nf(o.x, 2) + " σ vs tendencia" : ""}</div>` : `<b>${esc(I.nombre)}</b><div class="tp-s">Sin dato</div>`, e); c.onmouseleave = () => tip(null); c.onclick = () => ir("i:" + I.id); c.style.cursor = "pointer"; });
  }
  function mapaCicloEstatico(el) {
    const L = IND.filter(I => I.quad && I.ppy > 0), W = Math.max(320, el.clientWidth), H = Math.round(Math.min(560, W * .62)), m = 34;
    const R = Math.max(2, ...L.map(I => Math.max(Math.abs(I.last.x), Math.abs(I.last.y)))) * 1.1, X = v => m + (v / R + 1) / 2 * (W - 2 * m), Y = v => m + (1 - (v / R + 1) / 2) * (H - 2 * m), cx = X(0), cy = Y(0);
    let s = ""; [["exp", cx, m, W - m, cy], ["des", cx, cy, W - m, H - m], ["con", m, cy, cx, H - m], ["rec", m, m, cx, cy]].forEach(([k, x0, y0, x1, y1]) => { s += `<rect x="${x0}" y="${y0}" width="${x1 - x0}" height="${y1 - y0}" fill="${QD[k][1]}" opacity=".08"/>`; });
    s += `<line x1="${m}" x2="${W - m}" y1="${cy}" y2="${cy}" class="eco-ch"/><line x1="${cx}" x2="${cx}" y1="${m}" y2="${H - m}" class="eco-ch"/>`;
    [["exp", W - m - 8, m + 18, "end"], ["des", W - m - 8, H - m - 10, "end"], ["con", m + 8, H - m - 10, "start"], ["rec", m + 8, m + 18, "start"]].forEach(([k, x, y, a]) => { s += `<text x="${x}" y="${y}" text-anchor="${a}" class="eco-q" fill="${QD[k][1]}">${QD[k][0].toUpperCase()}</text>`; });
    L.forEach((I, i) => { s += `<g class="eco-bub" data-id="${I.id}" style="animation-delay:${i * 28}ms"><circle cx="${X(I.last.x).toFixed(1)}" cy="${Y(I.last.y).toFixed(1)}" r="${W < 480 ? 8 : 11}" fill="${I.col}" fill-opacity=".28"/><circle cx="${X(I.last.x).toFixed(1)}" cy="${Y(I.last.y).toFixed(1)}" r="${W < 480 ? 4.5 : 6}" fill="${I.col}" stroke="#fff" stroke-width="1.5"/></g>`; });
    s += `<text x="${W - m}" y="${cy - 6}" class="eco-ax" text-anchor="end">nivel vs tendencia →</text><text x="${cx + 6}" y="${m - 10}" class="eco-ax">↑ impulso</text>`;
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg>`;
    $$(".eco-bub", el).forEach(g => { const I = BYID[g.dataset.id]; g.style.cursor = "pointer"; g.onmousemove = e => tip(`<b>${esc(I.nombre)}</b><div class="tp-s">${esc(I.area)} · ${esc(I.last.l)}</div><div class="tp-v" style="color:${QD[I.quad][1]}">${QD[I.quad][0]}</div><div class="tp-r nu">${fv(I, I.last.v)}${I.last.yoy != null ? " · " + fy(I, I.last.yoy) + " anual" : ""}</div>`, e); g.onmouseleave = () => tip(null); g.onclick = () => ir("i:" + I.id); });
  }
  const ring = (p, col, txt, sub) => { const r = 38, c = 2 * Math.PI * r; return `<svg viewBox="0 0 100 100" width="92" height="92"><circle cx="50" cy="50" r="${r}" fill="none" stroke="#1e293b" stroke-width="9"/><circle cx="50" cy="50" r="${r}" fill="none" stroke="${col}" stroke-width="9" stroke-linecap="round" stroke-dasharray="${(c * p / 100).toFixed(1)} ${c.toFixed(1)}" transform="rotate(-90 50 50)" class="eco-ringc" style="--len:${(c * p / 100).toFixed(1)}"/><text x="50" y="55" text-anchor="middle" class="eco-rt">${txt}</text></svg>`; };

  /* ───────── vistas ───────── */
  function kpi(l, v, s, c, extra) { return `<div class="eco-kpi"><div class="kl">${l}</div><div class="kv ${c || ""}">${v}</div><div class="ks">${s || ""}</div>${extra || ""}</div>`; }
  function vistaInd(I) {
    const L = I.last, q = I.quad ? QD[I.quad] : null, ant = I.prev;
    const rng = [["1A", 12], ["3A", 36], ["5A", 60], ["10A", 120], ["Todo", 0]].filter(([, m]) => !m || m * I.ppy / 12 < I.s.length);
    if (!rng.some(([, m]) => m === S.serie.rango)) S.serie.rango = 0;
    const meta = [["ID INEGI", I.id], ["Descripción oficial", I.descripcion], ["Frecuencia", I.frecuencia], ["Unidad", I.unidad + (I.mult ? " (" + I.mult.toLowerCase() + ")" : "")], ["Tema INEGI", I.tema], ["Fuente", I.fuente], ["Periodo cubierto", I.s[0].l + " – " + L.l + " · " + I.s.length + " obs."], ["Actualizado por INEGI", I.actualizado], ["Nota", I.nota]].filter(r => r[1]);
    return `<div class="eco-crumb"><a data-go="home">Panorama</a> › <a data-go="a:${esc(I.area)}">${esc(I.area)}</a> › <a data-go="g:${esc(I.area)}|${esc(I.grupo)}">${esc(I.grupo)}</a></div>
    <div class="eco-h"><div><h1>${esc(I.nombre)}</h1><div class="eco-sub"><span class="eco-pill" style="--c:${I.col}">${esc(I.area)}</span><span class="eco-pill">${esc(I.frecuencia)}</span><span class="eco-pill">${esc(I.unidad)}${I.mult ? " · " + esc(I.mult.toLowerCase()) : ""}</span><span class="eco-pill st-${I.est[0]}">${dotE(I)} ${I.est[1]} · ${esc(L.l)}</span></div></div><button class="eco-btn" id="ecoCsv" type="button">⬇ CSV</button></div>
    <div class="eco-kpis">
      ${kpi("Último dato · " + esc(L.l), fv(I, L.v), esc(I.u === "pts" || I.u === "%" ? "" : I.u), "big")}
      ${kpi("Vs. periodo anterior", ant && I.ppy > 1 ? fy(I, L.mom) : "—", ant ? "antes " + fv(I, ant.v) : "", cc(L.mom))}
      ${kpi("Variación anual", fy(I, L.yoy), L.yoy != null ? "vs " + esc(I.s[I.s.length - 1 - I.ppy] ? I.s[I.s.length - 1 - I.ppy].l : "") : "", cc(L.yoy))}
      ${kpi("Posición histórica", ring(I.pctil, I.col, nf(I.pctil, 0) + "%"), "percentil del último dato", "", "")}
      ${kpi("Máximo histórico", fv(I, I.max), esc(I.maxO.l))}
      ${kpi("Mínimo histórico", fv(I, I.min), esc(I.minO.l))}
      ${q ? kpi("Fase del ciclo", `<span style="color:${q[1]}">${q[0]}</span>`, "según el reloj económico") : ""}
    </div>
    <div class="eco-panel"><div class="eco-ph"><h3>Evolución</h3>${serieChecks(I)}<label class="eco-sm" title="Suavizar la serie"><select id="ecoSm" aria-label="Suavizado"><option value="">Sin suavizar</option>${Object.entries(SUAV).map(([k, v]) => `<option value="${k}"${S.serie.suav === k ? " selected" : ""}>${v[0]}</option>`).join("")}</select></label><div class="eco-tabs" id="ecoRng">${rng.map(([t, m]) => `<button data-r="${m}" class="${S.serie.rango === m ? "on" : ""}">${t}</button>`).join("")}</div></div><div id="ecoMain"></div></div>
    <div class="eco-two"><div class="eco-panel"><div class="eco-ph"><h3>Reloj económico</h3><span class="eco-hint">${esc(cicloHint(I))}</span></div><div id="ecoClock"></div></div>
    <div class="eco-panel"><div class="eco-ph"><h3>${I.ppy === 1 ? "Distribución histórica" : "Estacionalidad · año × " + (I.ppy === 12 ? "mes" : "trimestre")}</h3></div><div id="ecoEst"></div></div></div>
    <div class="eco-panel"><div class="eco-ph"><h3>Ficha del indicador</h3></div><dl class="eco-meta">${meta.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join("")}</dl></div>`;
  }
  function pintaInd(I) {
    const el = $("#ecoMain"); draws.push(() => drawSerie($("#ecoMain"), I), () => drawReloj($("#ecoClock"), I), () => drawEstac($("#ecoEst"), I));
    $("#ecoMode").onclick = e => { const b = e.target.closest("button"); if (!b) return; return; };
    $("#ecoRng").onclick = e => { const b = e.target.closest("button"); if (!b) return; S.serie.rango = +b.dataset.r; S.zoom["s:" + I.id] = null; $$("#ecoRng button").forEach(x => x.classList.toggle("on", x === b)); drawSerie($("#ecoMain"), I); };
    $("#ecoCsv").onclick = () => { const t = "periodo,valor,variacion_anual\n" + I.s.map(o => [o.l, o.v, o.yoy == null ? "" : o.yoy.toFixed(3)].join(",")).join("\n"), a = document.createElement("a"); a.href = URL.createObjectURL(new Blob(["﻿" + t], { type: "text/csv" })); a.download = "ecolab_" + I.id + ".csv"; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 2000); };
    void el;
  }
  function conteo(list) { const exp = list.filter(I => I.quad === "exp" || I.quad === "rec").length, ok = list.filter(I => I.est[0] === "ok").length; return { exp, ok, n: list.length }; }
  function vistaPano(titulo, sub, list, ruta, col) {
    const c = conteo(list), mens = list.filter(I => I.ppy === 12 && I.s.length > 18).length;
    return `<div class="eco-crumb">${ruta}</div><div class="eco-h"><div><h1>${esc(titulo)}</h1><div class="eco-sub">${sub}</div></div></div>
    <div class="eco-kpis">${kpi("Indicadores", c.n, "en este nivel", "big")}${kpi("Al día", c.ok + " / " + c.n, "actualizados recientemente", c.ok === c.n ? "up" : "")}${kpi("Por encima de su tendencia", list.filter(I => I.last.x != null && I.last.x > 0).length + " / " + list.filter(I => I.last.x != null).length, "nivel actual vs media móvil")}${kpi("Mejorando", c.exp + " / " + list.filter(I => I.quad).length, "en expansión o recuperación", "up")}</div>
    <div class="eco-panel"><div class="eco-ph"><h3>Indicadores</h3><span class="eco-hint">clic en una tarjeta para abrir su tablero</span></div>${cards(list)}</div>
    <div class="eco-two"><div class="eco-panel"><div class="eco-ph"><h3>Posición vs. tendencia (transversal)</h3><span class="eco-hint">desviaciones estándar</span></div><div id="ecoRank"></div></div>
    <div class="eco-panel"><div class="eco-ph"><h3>Mapa de calor · últimos 24 meses</h3><span class="eco-hint">${mens} series mensuales</span></div><div id="ecoHeat"></div></div></div>`;
  }
  function pintaPano(list) { draws.push(() => rank($("#ecoRank"), list), () => heatZ($("#ecoHeat"), list, 24)); }
  function vistaHome() {
    const c = conteo(IND), ult = IND.map(I => I.actualizado).sort((a, b) => b.split("/").reverse().join("").localeCompare(a.split("/").reverse().join("")))[0] || "";
    return `<div class="eco-h"><div><h1>Panorama económico</h1><div class="eco-sub"><span class="eco-pill">${AREAS.length} áreas</span><span class="eco-pill">${IND.length} indicadores</span><span class="eco-pill">Fuente: INEGI · API de Indicadores</span></div></div></div>
    <div class="eco-kpis">${kpi("Indicadores", IND.length, AREAS.length + " áreas temáticas", "big")}${kpi("Al día", c.ok + " / " + c.n, "último dato reciente", "up")}${kpi("En expansión o recuperación", c.exp + " / " + IND.filter(I => I.quad).length, "según el reloj económico", "up")}${kpi("Última actualización", esc(ult), "del conjunto descargado")}</div>
    <div class="eco-panel"><div class="eco-ph"><h3>Mapa del ciclo económico</h3><span class="eco-hint">cada burbuja es un indicador (pasa el cursor; clic para abrir)</span><div class="eco-leg">${AREAS.map(a => `<span><i style="background:${a.col}"></i>${esc(a.n)}</span>`).join("")}</div></div><div id="ecoMapa"></div></div>
    ${AREAS.map(a => { const l = a.g.flatMap(g => g.i); return `<div class="eco-panel"><div class="eco-ph"><h3 style="color:${a.col}">${esc(a.n)}</h3><a class="eco-more" data-go="a:${esc(a.n)}">ver área →</a></div>${cards(l.slice(0, 6))}</div>`; }).join("")}`;
  }

  /* ───────── geografía (entidades y zonas) y sectores ───────── */
  const ZONAS = [["Noroeste", "#22d3ee"], ["Noreste", "#a78bfa"], ["Occidente y Bajío", "#34d399"], ["Centro", "#fbbf24"], ["Sur", "#f472b6"], ["Península y Sureste", "#60a5fa"]];
  // [clave, nombre, abreviatura, columna, fila, zona]  (columna/fila: mapa de mosaicos)
  const ENT = [["01", "Aguascalientes", "AGS", 3, 2, 2], ["02", "Baja California", "BC", 0, 0, 0], ["03", "Baja California Sur", "BCS", 0, 1, 0], ["04", "Campeche", "CAM", 9, 4, 5], ["05", "Coahuila", "COA", 3, 0, 1], ["06", "Colima", "COL", 2, 3, 2], ["07", "Chiapas", "CHP", 8, 5, 4], ["08", "Chihuahua", "CHH", 2, 0, 0],
    ["09", "Ciudad de México", "CDMX", 5, 3, 3], ["10", "Durango", "DGO", 2, 1, 0], ["11", "Guanajuato", "GTO", 4, 2, 2], ["12", "Guerrero", "GRO", 4, 4, 4], ["13", "Hidalgo", "HGO", 6, 2, 3], ["14", "Jalisco", "JAL", 2, 2, 2], ["15", "México", "MEX", 4, 3, 3], ["16", "Michoacán", "MIC", 3, 3, 2],
    ["17", "Morelos", "MOR", 5, 4, 3], ["18", "Nayarit", "NAY", 1, 2, 2], ["19", "Nuevo León", "NL", 4, 0, 1], ["20", "Oaxaca", "OAX", 7, 4, 4], ["21", "Puebla", "PUE", 7, 3, 3], ["22", "Querétaro", "QRO", 5, 2, 2], ["23", "Quintana Roo", "QR", 10, 4, 5], ["24", "San Luis Potosí", "SLP", 4, 1, 2],
    ["25", "Sinaloa", "SIN", 1, 1, 0], ["26", "Sonora", "SON", 1, 0, 0], ["27", "Tabasco", "TAB", 8, 4, 5], ["28", "Tamaulipas", "TAM", 5, 0, 1], ["29", "Tlaxcala", "TLA", 6, 3, 3], ["30", "Veracruz", "VER", 6, 1, 4], ["31", "Yucatán", "YUC", 10, 3, 5], ["32", "Zacatecas", "ZAC", 3, 1, 2]];
  const ENTBY = Object.fromEntries(ENT.map(e => [e[0], e]));
  const SEC = [{ k: "agro", n: "Agropecuario", ic: "🌾", t: 1, scian: ["111", "112"], st: ["s_agro"] },
    { k: "mineria_energia", n: "Minería y energía", ic: "⛏️", t: 2, scian: ["21", "22"], st: ["s_min", "s_ene"] }, { k: "construccion", n: "Construcción", ic: "🏗️", t: 2, scian: ["23"], st: ["s_con"] },
    { k: "manufacturas", n: "Manufacturas", ic: "🏭", t: 2, scian: ["31-33"], st: ["s_man"] }, { k: "comercio", n: "Comercio", ic: "🛒", t: 3, scian: ["43", "46"], st: ["s_com"] },
    { k: "transporte_info", n: "Transportes e información", ic: "🚚", t: 3, scian: ["48-49", "51"], st: ["s_tra"] }, { k: "finanzas_inmob", n: "Finanzas e inmobiliario", ic: "🏦", t: 3, scian: ["52", "53"], st: ["s_fin"] },
    { k: "servicios_neg", n: "Servicios profesionales y de negocios", ic: "💼", t: 3, scian: ["54", "56"], st: ["s_neg"] }, { k: "turismo", n: "Turismo, esparcimiento y alimentos", ic: "🏖️", t: 3, scian: ["71", "72"], st: ["s_tur"] },
    { k: "social", n: "Educación, salud y gobierno", ic: "🎓", t: 3, scian: ["61", "62", "81", "93"], st: ["s_soc"] }];
  SEC.forEach((s, i) => { s.col = PAL[i % PAL.length]; });
  const TIER = [null, ["Primario", "Agricultura, ganadería y pesca", "🌱"], ["Secundario", "Industria: minería, energía, construcción y manufacturas", "🏭"], ["Terciario", "Comercio y servicios", "🏙️"]];
  const SECBY = Object.fromEntries(SEC.map(s => [s.k, s]));
  const REFNAC = { s_agro: "11", s_min: "21", s_ene: "22", s_con: "23", s_man: "31-33", s_sec: "T2", s_ter: "T3" };
  const METR = [["itaee_yoy", "Actividad económica estatal (ITAEE) · variación anual"], ["ind_yoy", "Actividad industrial · variación anual"], ["man_yoy", "Manufacturas (mensual) · variación anual"], ["desoc", "Tasa de desocupación (%)"],
    ["s_agro", "Sector agropecuario · variación anual"], ["s_min", "Minería · variación anual"], ["s_ene", "Energía, agua y gas · variación anual"], ["s_con", "Construcción · variación anual"], ["s_man", "Manufacturas (trimestral) · variación anual"], ["s_com", "Comercio · variación anual"],
    ["s_tra", "Transportes e información · variación anual"], ["s_fin", "Finanzas e inmobiliario · variación anual"], ["s_neg", "Servicios profesionales y de negocios · variación anual"], ["s_tur", "Turismo, esparcimiento y alimentos · variación anual"], ["s_soc", "Educación, salud y gobierno · variación anual"]];
  const HEAD = [["910406", "Inflación anual"], ["737145", "Actividad económica (IGAE) · var. anual"], ["736526", "Actividad industrial · var. anual"], ["737374", "PIB trimestral · var. anual"], ["444603", "Tasa de desocupación"], ["444610", "Informalidad laboral"], ["454168", "Confianza del consumidor"], ["701401", "Confianza empresarial"], ["6204198542", "Balanza comercial"]];
  const POL = { "444603": -1, "444610": -1 };   // indicadores donde “menos es mejor”
  let EST = {}, IGS = {};
  function cargaMon() {
    EST = {}; IGS = {};
    (D.estatal || []).forEach((e, k) => {
      const m = { tipo: e.tipo, def: e, by: {} };
      Object.entries(e.est).forEach(([c, obs]) => { if (!ENTBY[c] || obs.length < 4) return; const I = prep(Object.assign({}, e, { obs, nombre: e.nombre + " · " + ENTBY[c][1], area: "Estados", grupo: e.nombre, mult: "", id: e.id + "-" + c }), k); I.code = c; I.col = ZONAS[ENTBY[c][5]][1]; if (["itaee_idx", "ind_yoy", "desoc"].includes(e.clave)) I.fc = true; m.by[c] = I; });
      EST[e.clave] = m;
    });
    if (EST.itaee_idx && EST.itaee_tc) Object.keys(EST.itaee_idx.by).forEach(c => cicloDeTC(EST.itaee_idx.by[c], EST.itaee_tc.by[c]));
    IND.forEach(I => { if (I.scian && I.tipo) (IGS[I.scian] = IGS[I.scian] || {})[I.tipo] = I; });
    IGS.TOT = { idx: BYID["737121"], yoy: BYID["737145"] };
  }
  const emv = (cl, c) => { const m = EST[cl], I = m && m.by[c]; return I ? (m.tipo === "idx" ? I.last.yoy : I.last.v) : null; };
  const secSer = sc => sc.scian.map(c => IGS[c]).filter(Boolean);
  const lerp3 = (a, b, t) => a.map((v, i) => Math.round(v + (b[i] - v) * t));
  const cDiv = (v, lim) => { if (v == null) return "#334155"; const t = Math.max(-1, Math.min(1, v / lim)), n = [51, 65, 85], c = t >= 0 ? lerp3(n, [52, 211, 153], t) : lerp3(n, [251, 113, 133], -t); return "rgb(" + c + ")"; };
  const cSeq = (v, lo, hi) => { if (v == null) return "#334155"; const t = Math.max(0, Math.min(1, (v - lo) / (hi - lo || 1))); return "rgb(" + (t < .5 ? lerp3([52, 211, 153], [250, 204, 21], t * 2) : lerp3([250, 204, 21], [251, 113, 133], (t - .5) * 2)) + ")"; };
  const sg = (v, d) => v == null ? "—" : (v > 0 ? "+" : v < 0 ? "−" : "") + nf(Math.abs(v), d == null ? 1 : d);
  const fp = (v, d) => v == null ? "—" : sg(v, d) + "%";
  const periodoL = (t, ppy) => ppy === 12 ? MES[((t % 12) + 12) % 12] + " " + String(Math.floor(t / 12)).slice(2) : ppy === 4 ? "T" + ((t % 4) + 1) + " " + String(Math.floor(t / 4)).slice(2) : String(t);
  const panel = (t, hint, inner, cl) => `<div class="eco-panel ${cl || ""}"><div class="eco-ph"><h3>${t}</h3>${hint ? `<span class="eco-hint">${hint}</span>` : ""}</div>${inner}</div>`;

  /* ───────── gráficas de monitor ───────── */
  /* boxplot de la distribución entre estados (sustituye a la barra de colores) con el estado del proyecto resaltado */
  function boxLeg(ok, o, W2) {
    if (ok.length < 4) return "";
    const fm = o.fmt || (v => o.seq ? nf(v, 1) + "%" : fp(v, 1)), nom = o.nombre || (c => ENTBY[c][1]), pl = o.pl || "estados", xs = ok.map(x => x.v).sort((a, b) => a - b), q = pp => { const i = (xs.length - 1) * pp, a = Math.floor(i), b = Math.ceil(i); return xs[a] + (xs[b] - xs[a]) * (i - a); };
    const q1 = q(.25), md = q(.5), q3 = q(.75), ir = q3 - q1, wl = Math.min(...xs.filter(v => v >= q1 - 1.5 * ir)), wh = Math.max(...xs.filter(v => v <= q3 + 1.5 * ir));
    const W = Math.max(280, Math.min(W2, 640)), m = { l: 14, r: 14 }, H = 104, mn = xs[0], mx = xs[xs.length - 1], pad = (mx - mn) * .06 || 1, lo = mn - pad, hi = mx + pad, X = v => m.l + (v - lo) / (hi - lo) * (W - m.l - m.r), cy = 52;
    const pc = o.pin !== undefined ? (ok.find(x => x.c === o.pin) || null) : ((S.ctx && S.ctx.ent && ok.find(x => x.c === S.ctx.ent)) || (o.sel && ok.find(x => x.c === o.sel)) || null), rkp = pc ? ok.filter(x => x.v > pc.v).length + 1 : 0;
    let g = "", tk = ticksN(lo, hi, 5), td = tdec(tk);
    tk.forEach(v => { g += `<line x1="${X(v).toFixed(1)}" x2="${X(v).toFixed(1)}" y1="30" y2="76" class="eco-gr"/><text x="${X(v).toFixed(1)}" y="${H - 6}" class="eco-ax" text-anchor="middle">${nf(v, td)}</text>`; });
    if (!o.seq && lo < 0 && hi > 0) g += `<line x1="${X(0).toFixed(1)}" x2="${X(0).toFixed(1)}" y1="30" y2="76" stroke="#94a3b8" stroke-opacity=".7"/>`;
    g += `<line x1="${X(wl).toFixed(1)}" x2="${X(wh).toFixed(1)}" y1="${cy}" y2="${cy}" stroke="#94a3b8" stroke-width="1.6"/><line x1="${X(wl).toFixed(1)}" x2="${X(wl).toFixed(1)}" y1="${cy - 8}" y2="${cy + 8}" stroke="#94a3b8" stroke-width="1.6"/><line x1="${X(wh).toFixed(1)}" x2="${X(wh).toFixed(1)}" y1="${cy - 8}" y2="${cy + 8}" stroke="#94a3b8" stroke-width="1.6"/>`;
    g += `<rect x="${X(q1).toFixed(1)}" y="${cy - 14}" width="${Math.max(2, X(q3) - X(q1)).toFixed(1)}" height="28" rx="5" fill="#6366f1" fill-opacity=".22" stroke="#6366f1" stroke-width="1.4"/><line x1="${X(md).toFixed(1)}" x2="${X(md).toFixed(1)}" y1="${cy - 14}" y2="${cy + 14}" stroke="#6366f1" stroke-width="2.6"/>`;
    ok.forEach((x, i) => { if (pc && x.c === pc.c) return; const jy = cy + ((i * 37) % 17 - 8) * 1.4; g += `<circle cx="${X(x.v).toFixed(1)}" cy="${jy.toFixed(1)}" r="2.6" fill="#94a3b8" fill-opacity=".55"/>`; });
    if (pc) { const px = X(pc.v), lab = "📍 " + nom(pc.c) + " " + fm(pc.v), anc = px < 90 ? "start" : px > W - 90 ? "end" : "middle";
      g += `<line x1="${px.toFixed(1)}" x2="${px.toFixed(1)}" y1="22" y2="${cy - 6}" stroke="#fbbf24" stroke-width="2"/><circle cx="${px.toFixed(1)}" cy="${cy}" r="7" fill="#fbbf24" stroke="#fff" stroke-width="2"/><text x="${px.toFixed(1)}" y="14" text-anchor="${anc}" class="eco-ax" style="fill:#fbbf24;font-weight:800;font-size:12px">${esc(lab)}</text>`; }
    const nota = `<span>Mediana <b>${fm(md)}</b> · mitad central de ${pl === "estados" ? "los estados" : "las " + pl} entre <b>${fm(q1)}</b> y <b>${fm(q3)}</b>${pc ? ` · ${esc(nom(pc.c))}: lugar <b>${rkp}</b> de ${ok.length}` : ""}</span>`;
    return `<div class="eco-box">${o.titulo ? `<div class="eco-boxt">${o.titulo}</div>` : ""}<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" style="margin:0 auto;display:block">${g}</svg><div class="eco-boxn">${nota}<span class="eco-boxn2">Distribución de ${pl === "estados" ? "los" : "las"} ${ok.length} ${pl}${o.sinColor ? "" : " · el color de cada mosaico va de rojo (menor) a verde (mayor)"}</span></div></div>`;
  }
  function tileMap(el, f, o) {
    o = o || {}; const cols = 11, rows = 6, g = 4, W = Math.max(300, Math.min(el.clientWidth, 820)), ts = Math.floor((W - g * (cols - 1)) / cols), H = rows * ts + (rows - 1) * g + 34, W2 = cols * ts + (cols - 1) * g;
    const vals = ENT.map(e => ({ c: e[0], v: f(e[0]) })), ok = vals.filter(x => x.v != null), srt = ok.slice().sort((a, b) => o.seq ? a.v - b.v : b.v - a.v), rk = Object.fromEntries(srt.map((x, i) => [x.c, i + 1]));
    const col = v => o.seq ? cSeq(v, o.lo, o.hi) : cDiv(v, o.lim || 6);
    let s = "";
    ENT.forEach((e, i) => {
      const v = f(e[0]), x = e[3] * (ts + g), y = e[4] * (ts + g), dim = o.hl && !o.hl.has(e[0]), pj = S.ctx && S.ctx.ent === e[0], sel = o.sel === e[0] || pj;
      s += `<g class="eco-tile" data-c="${e[0]}" style="animation-delay:${i * 18}ms;opacity:${dim ? .22 : 1}"><rect x="${x}" y="${y}" width="${ts}" height="${ts}" rx="${(ts * .2).toFixed(1)}" fill="${col(v)}" stroke="${pj ? "#fbbf24" : sel ? "#fff" : "rgba(255,255,255,.12)"}" stroke-width="${sel ? 3 : 1}"${pj ? ' class="eco-pj"' : ""}/>` +
        `<text x="${x + ts / 2}" y="${y + ts / 2 - (ts > 40 ? 2 : 0)}" text-anchor="middle" class="eco-tt" style="font-size:${Math.max(8, ts * .22).toFixed(0)}px">${e[2]}</text>` +
        (pj ? `<text x="${x + ts - 2}" y="${y + 11}" text-anchor="end" style="font-size:11px">📍</text>` : "") + (ts > 40 ? `<text x="${x + ts / 2}" y="${y + ts / 2 + ts * .26}" text-anchor="middle" class="eco-tv" style="font-size:${Math.max(8, ts * .2).toFixed(0)}px">${v == null ? "—" : o.seq ? nf(v, 1) : sg(v, 1)}</text>` : "") + `</g>`;
    });
    const lg = o.seq ? `<span>${nf(o.lo, 1)}%</span><i style="background:linear-gradient(90deg,${cSeq(o.lo, o.lo, o.hi)},${cSeq((o.lo + o.hi) / 2, o.lo, o.hi)},${cSeq(o.hi, o.lo, o.hi)})"></i><span>${nf(o.hi, 1)}%</span>` : `<span>−${o.lim || 6}%</span><i style="background:linear-gradient(90deg,${cDiv(-1, 1)},${cDiv(0, 1)},${cDiv(1, 1)})"></i><span>+${o.lim || 6}%</span>`;
    el.innerHTML = `<svg viewBox="0 0 ${W2} ${H - 30}" width="${W2}" height="${H - 30}" style="margin:0 auto">${s}</svg>${boxLeg(ok, o, W2)}`;
    $$(".eco-tile", el).forEach(t => { const c = t.dataset.c, e = ENTBY[c], v = f(c); t.style.cursor = "pointer";
      t.onmousemove = ev => tip(`<b>${esc(e[1])}</b><div class="tp-s">${ZONAS[e[5]][0]}</div><div class="tp-v" style="color:${v == null ? "#94a3b8" : o.seq ? "#fbbf24" : v >= 0 ? "#34d399" : "#fb7185"}">${v == null ? "sin dato" : o.seq ? nf(v, 1) + "%" : fp(v, 1)}</div>${rk[c] ? `<div class="tp-r nu">Lugar ${rk[c]} de ${ok.length}</div>` : ""}${S.ctx && S.ctx.ent === c ? '<div class="tp-r" style="color:#fbbf24">📍 Estado del proyecto</div>' : ""}<div class="tp-r nu">Clic para abrir el monitor del estado</div>`, ev); t.onmouseleave = () => tip(null); t.onclick = () => (o.click || (cc2 => ir("e:" + cc2)))(c); });
  }
  function barsH(el, rows, o) {
    o = o || {}; if (!rows.length) { el.innerHTML = '<div class="eco-vacio">Sin datos.</div>'; return; }
    const W = Math.max(300, el.clientWidth), rh = o.rh || 27, lw = Math.min(o.lw || 230, W * .42), H = rows.length * rh + 22, vs = rows.map(r => r.v).concat(rows.filter(r => r.ref != null).map(r => r.ref)), lo = Math.min(0, ...vs), hi = Math.max(0, ...vs), ng = lo < 0 ? 52 : 0, sc = (W - lw - 74 - ng) / ((hi - lo) || 1), x0 = lw + 8 + ng + (-lo) * sc, fm = o.fmt || (v => fp(v, 1));
    let s = `<line x1="${x0}" x2="${x0}" y1="2" y2="${H - 20}" class="eco-ch"/>`;
    rows.forEach((r, i) => { const y = 4 + i * rh, w = Math.abs(r.v) * sc, up = r.v >= 0, cl = r.col || (o.col || (up ? "#34d399" : "#fb7185")), pj = r.go && S.ctx && ((S.ctx.ent && r.go === "e:" + S.ctx.ent) || (S.ctx.city && r.go === "c:" + S.ctx.city));
      if (pj) s += `<rect x="0" y="${y}" width="${W}" height="${rh - 2}" rx="6" fill="#fbbf24" fill-opacity=".16" stroke="#fbbf24" stroke-opacity=".7"/>`;
      s += `<text x="${lw}" y="${y + rh / 2 + 4}" class="eco-ax" text-anchor="end" style="fill:${pj ? "#fbbf24" : "#cbd5e1"};${pj ? "font-weight:800" : ""}">${pj ? "📍 " : ""}${esc(r.label.length > 38 ? r.label.slice(0, 37) + "…" : r.label)}</text><rect class="eco-hb" data-i="${i}" x="${(up ? x0 : x0 - w).toFixed(1)}" y="${y + 3}" width="${Math.max(1, w).toFixed(1)}" height="${rh - 8}" rx="5" fill="${cl}" style="transform-origin:${up ? "left" : "right"} center;transform-box:fill-box;animation-delay:${i * 30}ms;cursor:${r.go ? "pointer" : "default"}"/>` +
        `<text x="${(up ? x0 + w + 6 : x0 - w - 6).toFixed(1)}" y="${y + rh / 2 + 4}" class="eco-ax" text-anchor="${up ? "start" : "end"}" style="fill:#e2e8f0">${fm(r.v)}</text>` + (r.ref != null ? `<path d="M${(x0 + r.ref * sc).toFixed(1)} ${y + 2}l5 ${(rh - 4) / 2}l-5 ${(rh - 4) / 2}l-5 -${(rh - 4) / 2}z" fill="#fff" stroke="#0a0f1f" stroke-width="1" opacity=".9"/>` : "");
    });
    s += o.nota ? `<text x="${W - 4}" y="${H - 4}" class="eco-ax" text-anchor="end">${o.nota}</text>` : "";
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg>`;
    $$(".eco-hb", el).forEach(b => { const r = rows[+b.dataset.i]; b.onmousemove = e => tip(`<b>${esc(r.label)}</b>${r.sub ? `<div class="tp-s">${esc(r.sub)}</div>` : ""}<div class="tp-v" style="color:${r.v >= 0 ? "#34d399" : "#fb7185"}">${fm(r.v)}</div>${r.ref != null ? `<div class="tp-r nu">Nacional: ${fm(r.ref)}</div>` : ""}${r.go ? '<div class="tp-r nu">Clic para abrir</div>' : ""}`, e); b.onmouseleave = () => tip(null); if (r.go) b.onclick = () => ir(r.go); });
  }
  /* ───────── selector de periodo («cejitas»): acota el eje del tiempo de una gráfica de series ───────── */
  function brush(el, key, spark, redraw, label) {
    let b = el._br; const MIN = .07;
    if (!b || !b.isConnected) {
      b = document.createElement("div"); b.className = "eco-brush";
      b.innerHTML = '<div class="eco-btrack" title="Arrastra las cejitas de los extremos (o la franja) para acotar el periodo"><svg class="eco-bsp" viewBox="0 0 100 24" preserveAspectRatio="none"><path d=""/></svg><div class="eco-bsel"><i class="eco-bh l"></i><i class="eco-bh r"></i></div></div><div class="eco-bt"><span class="eco-bl"></span><button type="button" class="eco-bre">↺ Todo el periodo</button></div>';
      el.after(b); el._br = b;
      const tr = $(".eco-btrack", b), z = () => S.zoom[b._key] || [0, 1];
      const mueve = ev => { const d = b._drag; if (!d) return; const dx = (ev.clientX - d.x0) / d.w; let [a, c] = d.z0;
        if (d.mode === "l") a = Math.max(0, Math.min(c - MIN, a + dx)); else if (d.mode === "r") c = Math.min(1, Math.max(a + MIN, c + dx)); else { const w = c - a; a = Math.max(0, Math.min(1 - w, a + dx)); c = a + w; }
        S.zoom[b._key] = a <= .001 && c >= .999 ? null : [a, c]; b._pos(); cancelAnimationFrame(b._raf); b._raf = requestAnimationFrame(() => { el.classList.add("eco-noanim"); b._redraw(); }); };
      tr.addEventListener("pointerdown", e => { const r = tr.getBoundingClientRect(), z0 = z().slice(), h = e.target.closest(".eco-bh"); let mode;
        if (h) mode = h.classList.contains("l") ? "l" : "r"; else if (e.target.closest(".eco-bsel")) mode = "m"; else { const x = (e.clientX - r.left) / r.width; mode = Math.abs(x - z0[0]) < Math.abs(x - z0[1]) ? "l" : "r"; z0[mode === "l" ? 0 : 1] = Math.max(0, Math.min(1, x)); }
        e.preventDefault(); try { tr.setPointerCapture(e.pointerId); } catch (_) { } b._drag = { mode, z0, x0: e.clientX, w: r.width || 1 }; mueve(e); });
      tr.addEventListener("pointermove", mueve); const fin = () => { b._drag = null; }; tr.addEventListener("pointerup", fin); tr.addEventListener("pointercancel", fin);
      $(".eco-bre", b).onclick = () => { S.zoom[b._key] = null; b._pos(); el.classList.add("eco-noanim"); b._redraw(); };
      b._pos = () => { const [a, c] = z(), sl = $(".eco-bsel", b); sl.style.left = a * 100 + "%"; sl.style.width = (c - a) * 100 + "%"; $(".eco-bre", b).style.visibility = S.zoom[b._key] ? "visible" : "hidden"; };
    }
    b._key = key; b._redraw = redraw; $(".eco-bl", b).textContent = label || "";
    const v = (spark || []).filter(x => x != null && isFinite(x)); if (v.length > 1) { const lo = Math.min(...v), hi = Math.max(...v), st = Math.max(1, Math.floor(v.length / 120)); const pts = v.filter((_, i) => i % st === 0 || i === v.length - 1); $(".eco-bsp path", b).setAttribute("d", pts.map((y, i) => (i ? "L" : "M") + (i / (pts.length - 1) * 100).toFixed(1) + " " + (22 - (y - lo) / ((hi - lo) || 1) * 20).toFixed(1)).join("")); }
    b._pos();
  }
  function multiLine(el, lines, o) {
    o = o || {}; const L = lines.filter(l => l.pts.length > 1); if (!L.length) { el.innerHTML = '<div class="eco-vacio">Sin datos.</div>'; return; }
    const W = Math.max(320, el.clientWidth), H = W < 520 ? 260 : 320, m = { l: 48, r: 16, t: 14, b: 28 }, yrs = o.anios || 12, T1 = Math.max(...L.map(l => l.fc && l.fc.length ? l.fc[l.fc.length - 1].t : l.pts[l.pts.length - 1].t)), T0 = T1 - yrs, zk = "m:" + (el.id || ""), zz = S.zoom[zk] || [0, 1], t0 = T0 + zz[0] * (T1 - T0), t1 = T0 + zz[1] * (T1 - T0), atEnd = zz[1] >= .999, P = L.map(l => ({ ...l, pts: l.pts.filter(p => p.t >= t0 - 1e-9 && p.t <= t1 + 1e-9), fc: atEnd ? l.fc : null })).filter(l => l.pts.length > 1);
    if (!P.length) { S.zoom[zk] = null; if (zz[0] !== 0 || zz[1] !== 1) return multiLine(el, lines, o); el.innerHTML = '<div class="eco-vacio">Sin datos.</div>'; return; }
    const vs = P.flatMap(l => l.pts.map(p => p.v).concat((l.fc || []).map(p => p.v))); let lo = Math.min(...vs), hi = Math.max(...vs); if (o.cero !== false) { lo = Math.min(lo, 0); hi = Math.max(hi, 0); } const pad = (hi - lo) * .08 || 1; lo -= pad; hi += pad;
    const X = t => m.l + (t - t0) / (t1 - t0) * (W - m.l - m.r), Y = v => m.t + (1 - (v - lo) / (hi - lo)) * (H - m.t - m.b), tk = ticksN(lo, hi, 5), td = tdec(tk), emph = o.sel;
    let s = `<defs><filter id="ecoGl3"><feGaussianBlur stdDeviation="2.5" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs>`;
    tk.forEach(v => { s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(v).toFixed(1)}" y2="${Y(v).toFixed(1)}" class="eco-gr"/><text x="${m.l - 6}" y="${(Y(v) + 4).toFixed(1)}" class="eco-ax" text-anchor="end">${nf(v, td)}</text>`; });
    if (lo < 0 && hi > 0) s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(0).toFixed(1)}" y2="${Y(0).toFixed(1)}" stroke="#94a3b8" stroke-opacity=".6"/>`;
    { const sp = t1 - t0, fT = t => MES[Math.min(11, Math.floor((t - Math.floor(t)) * 12 + 1e-6))] + " " + Math.floor(t + 1e-6);
      if (sp >= 5) for (let y = Math.ceil(t0); y <= t1; y += Math.max(1, Math.round(sp / 7))) s += `<text x="${X(y).toFixed(1)}" y="${H - 8}" class="eco-ax" text-anchor="middle">${y}</text>`;
      else for (let k = 0; k <= 5; k++) { const t = t0 + sp * k / 5; s += `<text x="${X(t).toFixed(1)}" y="${H - 8}" class="eco-ax" text-anchor="${k === 0 ? "start" : k === 5 ? "end" : "middle"}">${fT(t)}</text>`; } }
    P.forEach((l, i) => { const d = l.pts.map((p, k) => (k ? "L" : "M") + X(p.t).toFixed(1) + " " + Y(p.v).toFixed(1)).join(""); if (l.fc && l.fc.length) { const e0 = l.pts[l.pts.length - 1]; s += `<path d="M${X(e0.t).toFixed(1)} ${Y(e0.v).toFixed(1)}${l.fc.map(p => "L" + X(p.t).toFixed(1) + " " + Y(p.v).toFixed(1)).join("")}" fill="none" stroke="${l.col}" stroke-width="3" stroke-linecap="round" stroke-dasharray="0.1 7" stroke-opacity="${emph == null || emph === i ? 1 : .35}"/>`; }
      s += `<path d="${d}" pathLength="1" class="eco-ln" fill="none" stroke="${l.col}" stroke-width="${emph == null ? 2.4 : emph === i ? 3.4 : 1.4}" stroke-opacity="${emph == null || emph === i ? 1 : .35}" ${l.dash ? 'stroke-dasharray="5 4"' : ""} stroke-linejoin="round" filter="url(#ecoGl3)" style="animation-delay:${i * 120}ms"/>`; });
    s += `<g id="ecoMl" style="display:none"><line class="eco-ch" y1="${m.t}" y2="${H - m.b}"/></g><rect id="ecoMlH" x="${m.l}" y="${m.t}" width="${W - m.l - m.r}" height="${H - m.t - m.b}" fill="transparent"/>`;
    const lg = P.map((l, i) => `<span class="eco-lgi" data-i="${i}" style="opacity:${emph == null || emph === i ? 1 : .45}"><i style="background:${l.col}"></i>${esc(l.name)}</span>`).join("");
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg><div class="eco-leg eco-legl">${lg}</div>`;
    { const fT = t => MES[Math.min(11, Math.floor((t - Math.floor(t)) * 12 + 1e-6))] + " " + Math.floor(t + 1e-6); brush(el, zk, L[0].pts.filter(p => p.t >= T0 - 1e-9).map(p => p.v), () => multiLine(el, lines, o), fT(t0) + " – " + fT(t1)); }
    const hit = $("#ecoMlH", el), g = $("#ecoMl", el), ln = $("line", g);
    hit.onmousemove = e => { const r = hit.getBoundingClientRect(), t = t0 + (e.clientX - r.left) / r.width * (t1 - t0); let best = null; P[0].pts.forEach(p => { if (!best || Math.abs(p.t - t) < Math.abs(best.t - t)) best = p; });
      const rows = P.map(l => { let b = null; l.pts.forEach(p => { if (!b || Math.abs(p.t - t) < Math.abs(b.t - t)) b = p; }); return `<div class="tp-r"><i class="eco-dot" style="background:${l.col}"></i>${esc(l.name)}: <b>${b && Math.abs(b.t - t) < 1.2 ? (o.fmt || (v => fp(v, 1)))(b.v) : "—"}</b></div>`; }).join("");
      g.style.display = ""; ln.setAttribute("x1", X(best.t)); ln.setAttribute("x2", X(best.t)); tip(`<b>${esc(best.l)}</b>${rows}`, e); };
    hit.onmouseleave = () => { g.style.display = "none"; tip(null); };
    $$(".eco-lgi", el).forEach(b => { b.style.cursor = "pointer"; b.onclick = () => { o.sel = o.sel === +b.dataset.i ? null : +b.dataset.i; multiLine(el, lines, o); }; });
  }
  function heatM(el, rows, nper, o) {   // rows [{label,I,go}]; o.f(obs)->valor, o.lim
    o = o || {}; const L = rows.filter(r => r.I && r.I.s.length > 8); if (!L.length) { el.innerHTML = '<div class="eco-vacio">Sin datos.</div>'; return; }
    const ppy = L[0].I.ppy, tmax = Math.max(...L.map(r => r.I.last.t)), ts = Array.from({ length: nper }, (_, i) => tmax - nper + 1 + i), W = Math.max(320, el.clientWidth), lw = Math.min(o.lw || 230, W * .34), cw = (W - lw - 6) / nper, rh = o.rh || 22, H = 22 + L.length * rh, f = o.f || (ob => ob.yoy), lim = o.lim || 8;
    let s = ""; ts.forEach((t, c) => { if (c % (nper > 14 ? 3 : 2) === 0) s += `<text x="${lw + c * cw + cw / 2}" y="12" class="eco-ax" text-anchor="middle">${periodoL(t, ppy)}</text>`; });
    L.forEach((r, i) => { const by = {}; r.I.s.forEach(ob => by[ob.t] = ob); s += `<text x="${lw - 6}" y="${22 + i * rh + rh / 2 + 4}" class="eco-ax" text-anchor="end" style="fill:#cbd5e1">${esc(r.label.length > 34 ? r.label.slice(0, 33) + "…" : r.label)}</text>`;
      ts.forEach((t, c) => { const ob = by[t], v = ob ? f(ob) : null; s += `<rect class="eco-cell" data-i="${i}" data-t="${t}" x="${(lw + c * cw + 1).toFixed(1)}" y="${22 + i * rh + 1}" width="${(cw - 2).toFixed(1)}" height="${rh - 2}" rx="3" fill="${o.seq ? cSeq(v, o.lo, o.hi) : cDiv(v, lim)}" style="animation-delay:${(i * nper + c) * 2}ms"/>`; }); });
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg><div class="eco-lg"><span>${o.seq ? "menor" : "caída"}</span><i style="background:linear-gradient(90deg,${o.seq ? cSeq(o.lo, o.lo, o.hi) : cDiv(-1, 1)},${o.seq ? cSeq((o.lo + o.hi) / 2, o.lo, o.hi) : cDiv(0, 1)},${o.seq ? cSeq(o.hi, o.lo, o.hi) : cDiv(1, 1)})"></i><span>${o.seq ? "mayor" : "crecimiento"}</span></div>`;
    $$(".eco-cell", el).forEach(c => { const r = L[+c.dataset.i], ob = r.I.s.find(q => q.t === +c.dataset.t), v = ob ? f(ob) : null; c.style.cursor = r.go ? "pointer" : "default"; c.onmousemove = e => tip(`<b>${esc(r.label)}</b><div class="tp-s">${periodoL(+c.dataset.t, ppy)}</div><div class="tp-v" style="color:${v == null ? "#94a3b8" : o.seq ? "#fbbf24" : v >= 0 ? "#34d399" : "#fb7185"}">${v == null ? "sin dato" : o.seq ? nf(v, 1) + "%" : fp(v, 1)}</div>`, e); c.onmouseleave = () => tip(null); if (r.go) c.onclick = () => ir(r.go); });
  }
  function gauge(el, score, label, col) {
    const W = Math.min(360, Math.max(240, el.clientWidth)), H = Math.round(W * .62) + 46, cx = W / 2, cy = H - 64, R = W * .38, pt = (a, r) => [cx + r * Math.cos(Math.PI - a), cy - r * Math.sin(Math.PI - a)], arc = (a0, a1, r) => { const p0 = pt(a0, r), p1 = pt(a1, r); return `M${p0[0].toFixed(1)} ${p0[1].toFixed(1)}A${r} ${r} 0 0 1 ${p1[0].toFixed(1)} ${p1[1].toFixed(1)}`; };
    const segs = [["#fb7185", 0, .25], ["#fbbf24", .25, .45], ["#94a3b8", .45, .55], ["#a3e635", .55, .75], ["#34d399", .75, 1]];
    let s = segs.map(([c, a, b]) => `<path d="${arc(a * Math.PI + .02, b * Math.PI - .02, R)}" fill="none" stroke="${c}" stroke-width="16" stroke-linecap="round" opacity=".9"/>`).join("");
    s += `<g style="transform-origin:${cx}px ${cy}px;transform:rotate(-90deg);transition:transform 1.3s cubic-bezier(.2,.9,.3,1.2)" id="ecoNeedle" data-r="${(score / 100 * 180 - 90).toFixed(1)}"><path d="M${cx} ${cy - R + 12}L${cx - 6} ${cy}L${cx + 6} ${cy}Z" fill="#fff" stroke="#0a0f1f" stroke-width="1"/></g><circle cx="${cx}" cy="${cy}" r="9" fill="#fff" stroke="#0a0f1f"/>`;
    s += `<text x="${cx}" y="${cy + 38}" text-anchor="middle" class="eco-gv">${Math.round(score)}</text><text x="${cx}" y="${cy + 58}" text-anchor="middle" class="eco-gl" fill="${col}">${esc(label)}</text>`;
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" style="margin:0 auto">${s}</svg>`; const n = $("#ecoNeedle", el); setTimeout(() => { n.style.transform = `rotate(${n.dataset.r}deg)`; }, 60);
  }

  /* ───────── piezas compartidas de las vistas de monitor ───────── */
  function pulso() {
    const xs = ["334338", "334339", "334349", "334344"].map(id => BYID[id] && BYID[id].last.x != null ? Math.max(-2, Math.min(2, BYID[id].last.x)) : null).filter(v => v != null), sc = xs.length ? 50 + 25 * (xs.reduce((a, b) => a + b, 0) / xs.length) : 50;
    const lab = sc < 35 ? ["Debilitamiento", "#fb7185"] : sc < 45 ? ["Moderación", "#fbbf24"] : sc < 55 ? ["Estable", "#94a3b8"] : sc < 65 ? ["Mejora", "#a3e635"] : ["Dinamismo", "#34d399"];
    return { sc, lab, n: xs.length };
  }
  function panelSerie(I, tag) {
    const rng = [["1A", 12], ["3A", 36], ["5A", 60], ["10A", 120], ["Todo", 0]].filter(([, m]) => !m || m * I.ppy / 12 < I.s.length); if (!rng.some(([, m]) => m === S.serie.rango)) S.serie.rango = 0;
    return `<div class="eco-panel"><div class="eco-ph"><h3>${tag || ""}Evolución</h3>${serieChecks(I)}<label class="eco-sm" title="Suavizar la serie"><select id="ecoSm" aria-label="Suavizado"><option value="">Sin suavizar</option>${Object.entries(SUAV).map(([k, v]) => `<option value="${k}"${S.serie.suav === k ? " selected" : ""}>${v[0]}</option>`).join("")}</select></label><div class="eco-tabs" id="ecoRng">${rng.map(([t, m]) => `<button data-r="${m}" class="${S.serie.rango === m ? "on" : ""}">${t}</button>`).join("")}</div></div><div class="eco-src">${esc(serieDesc(I))}</div><div id="ecoMain"></div></div>`;
  }
  function pintaSerie(I) {
    draws.push(() => drawSerie($("#ecoMain"), I));
    $("#ecoMode").onclick = e => { const b = e.target.closest("button"); if (!b) return; return; };
    $("#ecoRng").onclick = e => { const b = e.target.closest("button"); if (!b) return; S.serie.rango = +b.dataset.r; S.zoom["s:" + I.id] = null; $$("#ecoRng button").forEach(x => x.classList.toggle("on", x === b)); drawSerie($("#ecoMain"), I); };
  }
  const ptsE = (I, tipo) => I.s.map(o => ({ t: o.t / I.ppy, l: o.l, v: tipo === "idx" ? o.yoy : o.v })).filter(p => p.v != null);
  const ptsI = I => I.s.filter(o => o.yoy != null).map(o => ({ t: o.t / I.ppy, l: o.l, v: I.pct ? o.v : o.yoy })).filter(p => p.v != null);
  const selMetr = () => `<label class="eco-sel"><span>Indicador del mapa</span><select id="ecoMetr">${METR.map(([k, n]) => `<option value="${k}"${S.metric === k ? " selected" : ""}>${esc(n)}</option>`).join("")}</select></label>`;
  const zonaChips = zi => `<div class="eco-chips"><button type="button" class="eco-chip2${zi == null ? " on" : ""}" data-go="reg">Todo México</button>${ZONAS.map((z, i) => `<button type="button" class="eco-chip2${zi === i ? " on" : ""}" style="--c:${z[1]}" data-go="z:${i}">${z[0]}</button>`).join("")}</div>`;
  const metrOpts = () => { const cl = S.metric, seq = cl === "desoc", vs = ENT.map(e => emv(cl, e[0])).filter(v => v != null), mx = Math.max(...vs.map(Math.abs), 1); return seq ? { seq: true, lo: Math.min(...vs), hi: Math.max(...vs) } : { lim: Math.max(3, Math.min(20, Math.ceil(mx * .75))) }; };

  /* ───────── monitor nacional ───────── */
  function vistaNac() {
    const heads = HEAD.map(([id, l]) => BYID[id] && Object.assign(Object.create(BYID[id]), { nombre: l })).filter(Boolean), p = pulso(), ph = { exp: 0, des: 0, con: 0, rec: 0 }; IND.forEach(I => { if (I.quad && !I.cicloDe) ph[I.quad]++; });
    return `<div class="eco-h"><div><h1>Panorama global · México</h1><div class="eco-sub"><span class="eco-pill">Monitor nacional</span><span class="eco-pill">${IND.length} indicadores</span><span class="eco-pill">${(D.estatal || []).length} series por estado</span><span class="eco-pill">Fuente: INEGI</span></div></div></div>
    <div class="eco-grid2">${panel("Pulso económico", `${p.n} indicadores del ciclo oficial`, `<div id="ecoGauge"></div><div class="eco-phases">${Object.keys(QD).map(k => `<div class="eco-ph1" style="--c:${QD[k][1]}"><b>${ph[k]}</b><span>${QD[k][0]}</span></div>`).join("")}</div><p class="eco-note">Promedia el ciclo oficial del INEGI (desviación respecto de la tendencia) del indicador coincidente, el adelantado, el IGAE y la actividad industrial. Las fases cuentan las series con ciclo oficial o derivado de su tendencia-ciclo.</p>`, "eco-gaugep")}
    ${panel("Indicadores clave", "clic para abrir el tablero", cards(heads))}</div>
    <div class="eco-two">${panel("Sectores · variación anual del IGAE", "mayor aporte y mayor caída, por subsector SCIAN", '<div id="ecoSecBars"></div>')}${panel("Mapa de calor sectorial · últimos 18 meses", "variación anual mensual", '<div id="ecoSecHeat"></div>')}</div>
    <div class="eco-two">${panel("Mapa económico de los estados", "ITAEE · variación anual", '<div id="ecoNacMap"></div><div class="eco-more2"><a data-go="reg">Abrir monitor regional →</a></div>')}${panel("Estados que más crecen y más caen", "ITAEE · variación anual", '<div id="ecoNacRank"></div>')}</div>
    ${panel("Mapa del ciclo económico", "▶ Play para ver su evolución · la estela muestra los últimos periodos · elige las series", '<div id="ecoMapa"></div>')}`;
  }
  function pintaNac() {
    const p = pulso(); draws.push(() => gauge($("#ecoGauge"), p.sc, p.lab[0], p.lab[1]));
    const rows = SEC.flatMap(sc => secSer(sc).map(s => ({ I: s.yoy, label: s.yoy.nombre, sub: sc.n + " · SCIAN " + s.yoy.scian, v: s.yoy.last.v, col: sc.col, go: "s:" + sc.k }))).filter(r => r.I).sort((a, b) => b.v - a.v);
    draws.push(() => barsH($("#ecoSecBars"), rows.map(r => ({ label: r.label, sub: r.sub, v: r.v, go: r.go })), { rh: 24, lw: 210 }), () => heatM($("#ecoSecHeat"), rows.map(r => ({ label: r.label, I: r.I, go: r.go })), 18, { f: ob => ob.v, lim: 10, rh: 20 }));
    draws.push(() => tileMap($("#ecoNacMap"), c => emv("itaee_yoy", c), { lim: 6 }), () => { const r = ENT.map(e => ({ v: emv("itaee_yoy", e[0]), e })).filter(x => x.v != null).sort((a, b) => b.v - a.v), t = r.slice(0, 5).concat(r.slice(-5)); barsH($("#ecoNacRank"), t.map(x => ({ label: x.e[1], sub: ZONAS[x.e[5]][0], v: x.v, go: "e:" + x.e[0] })), { rh: 27, lw: 150 }); }, () => mapaCiclo($("#ecoMapa")));
  }

  /* ───────── monitor regional (todo México o una zona) ───────── */
  function vistaReg(zi) {
    const cl = S.metric, men = METR.find(m => m[0] === cl)[1], codes = ENT.filter(e => zi == null || e[5] === zi).map(e => e[0]);
    const zs = ZONAS.map((z, i) => { const vs = ENT.filter(e => e[5] === i).map(e => emv(cl, e[0])).filter(v => v != null); return { z, i, n: vs.length, avg: vs.length ? vs.reduce((a, b) => a + b, 0) / vs.length : null }; });
    const head = zi == null ? "Monitor regional · México" : "Zona " + ZONAS[zi][0], sub = zi == null ? `<span class="eco-pill">32 entidades</span><span class="eco-pill">6 zonas</span>` : `<span class="eco-pill" style="--c:${ZONAS[zi][1]}">${codes.length} entidades</span><span class="eco-pill">${codes.map(c => ENTBY[c][2]).join(" · ")}</span>`;
    const est = D.estatal ? (D.estatal.find(e => e.clave === cl) || {}).actualizado : "";
    return `<div class="eco-crumb"><a data-go="nac">Panorama</a> › <a data-go="reg">Regiones</a>${zi != null ? " › " + ZONAS[zi][0] : ""}</div><div class="eco-h"><div><h1>${head}</h1><div class="eco-sub">${sub}<span class="eco-pill">Fuente: INEGI${est ? " · act. " + est : ""}</span></div></div></div>
    <div class="eco-ctl">${selMetr()}${zonaChips(zi)}<button type="button" class="eco-chip2" style="--c:#34d399" data-go="ciu">🏙️ Ciudades (39)</button></div>
    <div class="eco-grid2b">${panel("Mapa de estados", esc(men), '<div id="ecoRMap"></div>')}${panel(zi == null ? "Ranking de entidades" : "Ranking en la zona", "", '<div id="ecoRRank"></div>')}</div>
    ${zi == null ? panel("Resumen por zona", "promedio simple de sus estados", `<div class="eco-cards eco-cards2">${zs.map(x => `<button type="button" class="eco-card" data-go="z:${x.i}" style="--c:${x.z[1]}"><div class="ct"><span>${x.z[0]}</span></div><div class="cg">${ENT.filter(e => e[5] === x.i).map(e => e[2]).join(" · ")}</div><div class="cv" style="color:${x.avg == null ? "#94a3b8" : cl === "desoc" ? "#fbbf24" : x.avg >= 0 ? "#34d399" : "#fb7185"}">${cl === "desoc" ? nf(x.avg, 1) + "%" : fp(x.avg, 1)}</div><div class="cp">${x.n} entidades con dato</div></button>`).join("")}</div>`) : ""}
    <div class="eco-two">${panel(zi == null ? "Evolución por zona" : "Evolución de las entidades de la zona", "promedio simple · variación anual", '<div id="ecoRLine"></div>')}${panel("Mapa de calor por entidad", cl === "desoc" ? "últimos 12 trimestres" : "últimos periodos", '<div id="ecoRHeat"></div>')}</div>`;
  }
  function pintaReg(zi) {
    const cl = S.metric, m = EST[cl], o = metrOpts(), codes = ENT.filter(e => zi == null || e[5] === zi).map(e => e[0]), hl = zi == null ? null : new Set(codes);
    $("#ecoMetr").onchange = e => { S.metric = e.target.value; ir(S.sel); };
    draws.push(() => tileMap($("#ecoRMap"), c => emv(cl, c), Object.assign({ hl }, o)));
    const r = codes.map(c => ({ c, v: emv(cl, c) })).filter(x => x.v != null).sort((a, b) => cl === "desoc" ? a.v - b.v : b.v - a.v);
    draws.push(() => barsH($("#ecoRRank"), r.map(x => ({ label: ENTBY[x.c][1], sub: ZONAS[ENTBY[x.c][5]][0], v: x.v, go: "e:" + x.c, col: cl === "desoc" ? undefined : undefined })), { rh: 22, lw: 140, fmt: cl === "desoc" ? v => nf(v, 1) + "%" : undefined, col: cl === "desoc" ? "#fbbf24" : undefined }));
    if (!m) return;
    const tipo = m.tipo, lines = zi == null ? ZONAS.map((z, i) => { const by = {}; ENT.filter(e => e[5] === i).forEach(e => { const I = m.by[e[0]]; if (I) ptsE(I, tipo).forEach(p => { (by[p.t] = by[p.t] || { l: p.l, v: [] }).v.push(p.v); }); }); return { name: z[0], col: z[1], pts: Object.keys(by).map(Number).sort((a, b) => a - b).map(t => ({ t, l: by[t].l, v: by[t].v.reduce((a, b) => a + b, 0) / by[t].v.length })) }; })
      : codes.map((c, i) => m.by[c] ? { name: ENTBY[c][1], col: PAL[i % PAL.length], pts: ptsE(m.by[c], tipo) } : null).filter(Boolean);
    draws.push(() => multiLine($("#ecoRLine"), lines, { anios: 10, cero: cl !== "desoc", fmt: cl === "desoc" ? v => nf(v, 1) + "%" : undefined }));
    const rows = codes.map(c => m.by[c] ? { label: ENTBY[c][1], I: m.by[c], go: "e:" + c } : null).filter(Boolean).sort((a, b) => (emv(cl, b.I.code) || 0) - (emv(cl, a.I.code) || 0));
    draws.push(() => heatM($("#ecoRHeat"), rows, m.by[codes[0]].ppy === 12 ? 18 : 12, { f: tipo === "idx" ? ob => ob.yoy : ob => ob.v, lim: o.lim, seq: o.seq, lo: o.lo, hi: o.hi, rh: zi == null ? 17 : 22, lw: 140 }));
  }

  /* ───────── monitor de un estado ───────── */
  function vistaEst(c) {
    const e = ENTBY[c], z = ZONAS[e[5]], K = [["itaee_yoy", "Actividad económica estatal (ITAEE)", BYID["737145"], "var. anual · trimestral", "Cuánto creció o cayó, contra el mismo trimestre del año anterior, la producción total de bienes y servicios del estado.", "IGAE nacional"],
      ["ind_yoy", "Actividad industrial estatal", BYID["736526"], "var. anual · mensual", "Variación anual de la producción industrial del estado: minería, energía, construcción y manufacturas.", "actividad industrial nacional"],
      ["man_yoy", "Manufacturas estatales", IGS["31-33"] && IGS["31-33"].yoy, "var. anual · mensual", "Variación anual de la producción de las fábricas del estado (industrias manufactureras, SCIAN 31-33).", "IGAE manufacturas nacional"],
      ["desoc", "Tasa de desocupación estatal", BYID["444603"], "% de la PEA · trimestral", "Personas sin empleo que lo buscan, como porcentaje de la población económicamente activa (PEA). Menos es mejor.", "tasa nacional"]];
    const rank = cl => { const vs = ENT.map(x => emv(cl, x[0])).filter(v => v != null), v = emv(cl, c); return v == null ? null : vs.filter(w => cl === "desoc" ? w < v : w > v).length + 1; };
    const tiles = K.map(([cl, n, nac, u, ds, nn]) => { const v = emv(cl, c), nv = nac ? nac.last.v : null, dl = v != null && nv != null ? v - nv : null, I = EST[cl] && EST[cl].by[c], rk = rank(cl), pct = cl !== "desoc";
      return `<div class="eco-kpi"><div class="kl">${n}</div><div class="kv ${cl === "desoc" ? "nu" : v == null ? "" : v >= 0 ? "up" : "dn"}">${v == null ? "—" : pct ? fp(v, 1) : nf(v, 1) + "%"}</div><div class="ks">${u}${I ? " · " + I.last.l : ""}</div><div class="kd">${ds}</div><div class="eco-vs">${nv == null ? "" : `Nacional (${nn}): <b>${pct ? fp(nv, 1) : nf(nv, 1) + "%"}</b> · ${dl == null ? "" : `<span class="${(cl === "desoc" ? -dl : dl) >= 0 ? "up" : "dn"}">${sg(dl, 1)} pp</span>`}`}</div>${rk ? `<div class="eco-rk">Lugar <b>${rk}</b> de 32</div>` : ""}</div>`; }).join("");
    const I0 = EST.itaee_idx && EST.itaee_idx.by[c], q = I0 && I0.quad ? QD[I0.quad] : null;
    return `<div class="eco-crumb"><a data-go="nac">Panorama</a> › <a data-go="reg">Regiones</a> › <a data-go="z:${e[5]}">${z[0]}</a></div><div class="eco-h"><div><h1>${esc(e[1])}</h1><div class="eco-sub"><span class="eco-pill" style="--c:${z[1]}">${z[0]}</span><span class="eco-pill">Monitor estatal</span>${I0 ? `<span class="eco-pill">ITAEE: ${I0.last.l}</span>` : ""}</div></div></div>
    ${bandaAmb("edo", "📍 ESTADO", esc(e[1]), "Series estatales del INEGI: ITAEE (actividad económica, trimestral), actividad industrial y manufacturas (mensuales) y desocupación (trimestral). Las comparaciones con el país se rotulan «Nacional».")}
    <div class="eco-kmap"><div class="eco-kgrid">${tiles}${q ? kpi("Fase del ciclo", `<span style="color:${q[1]}">${q[0]}</span>`, "ITAEE · reloj económico") : ""}</div><div class="eco-mapbox"><div id="ecoMapa2"></div></div></div>
    ${I0 ? panelSerie(I0, tagAmb("edo")) : ""}
    <div class="eco-two">${panel(tagAmb("mix") + "Actividad económica: estado vs. nación", "variación anual", '<div id="ecoELn1"></div>')}${panel(tagAmb("mix") + "Actividad industrial: estado vs. nación", "variación anual (mensual)", '<div id="ecoELn2"></div>')}</div>
    <div class="eco-two">${panel(tagAmb("edo") + "¿Qué sectores jalan o frenan?", "ITAEE · variación anual · ◆ = nacional", '<div id="ecoESec"></div>')}${I0 ? panel(tagAmb("edo") + "Reloj económico", "ITAEE estatal", '<div id="ecoEClock"></div>') : ""}</div>
    ${panel(tagAmb("cmp") + "El estado en el mapa", "ITAEE · variación anual", '<div id="ecoEMap"></div>')}${ciudadesDe(c)}`;
  }
  function ciudadesDe(c) {
    const l = CIU.filter(x => x[2] === c); if (!l.length) return "";
    return bandaAmb("ciu", "🏙️ CIUDADES DEL ESTADO · ENOE", "Mercado laboral de cada ciudad", "Datos de la Encuesta Nacional de Ocupación y Empleo por ciudad (trimestral). Aquí cambia el ámbito: ya no es el estado completo, sino cada ciudad.") + panel(tagAmb("ciu") + "Ciudades del estado", "ENOE · tasa de desocupación", `<div class="eco-cards eco-cards2">${l.map(x => { const I = CST.c_desoc && CST.c_desoc.by[x[0]]; return `<button type="button" class="eco-card" data-go="c:${x[0]}" style="--c:${ZONAS[ENTBY[c][5]][1]}"><div class="ct"><span>${esc(x[1])}</span></div><div class="cg">Ciudad · ENOE</div><div class="cv">${I ? nf(I.last.v, 1) + "%" : "—"}<small>desocupación</small></div>${I ? `<div class="cs">${spark(I, 40, 220, 46)}</div><div class="cp">${esc(I.last.l)} · participación ${nf(cmv("c_part", x[0]), 1)}%</div>` : ""}</button>`; }).join("")}</div>`);
  }
  function pintaEst(c) {
    const xy = ENT_XY[c]; if (xy) draws.push(() => ecoMapa($("#ecoMapa2"), { lat: xy[0], lon: xy[1], zoom: xy[2], pin: { col: ZONAS[ENTBY[c][5]][1], label: ENTBY[c][1] }, puntos: CIU.filter(x => x[2] === c && CIU_XY[x[0]]).map(x => ({ lat: CIU_XY[x[0]][0], lon: CIU_XY[x[0]][1], label: x[1], go: "c:" + x[0], col: "#22d3ee" })) }));
    const I0 = EST.itaee_idx && EST.itaee_idx.by[c]; if (I0) { pintaSerie(I0); draws.push(() => drawReloj($("#ecoEClock"), I0)); }
    const mk = (cl, nac, nom, nn) => { const m = EST[cl], I = m && m.by[c]; return [I ? { name: ENTBY[c][1], col: "#22d3ee", pts: ptsE(I, m.tipo), fc: fcLine(I, m.tipo === "idx" ? "yoy" : "level") } : null, nac ? { name: nn, col: "#fbbf24", dash: true, pts: ptsI(nac), fc: fcLine(nac, "level") } : null].filter(Boolean); };
    draws.push(() => multiLine($("#ecoELn1"), mk("itaee_yoy", BYID["737145"], "", "México (IGAE)"), { anios: 12 }), () => multiLine($("#ecoELn2"), mk("ind_yoy", BYID["736526"], "", "México (act. industrial)"), { anios: 12 }));
    const rows = METR.filter(([k]) => k.startsWith("s_") && EST[k]).map(([k, n]) => { const v = emv(k, c), rf = REFNAC[k] && IGS[REFNAC[k]] ? IGS[REFNAC[k]].yoy.last.v : null; return v == null ? null : { label: n.replace(" · variación anual", "").replace("Sector agropecuario", "Agropecuario"), v, ref: rf, sub: EST[k].def.actualizado ? "act. " + EST[k].def.actualizado : "" }; }).filter(Boolean).sort((a, b) => b.v - a.v);
    draws.push(() => barsH($("#ecoESec"), rows, { rh: 26, lw: 200, nota: "◆ valor nacional (cuando existe)" }), () => tileMap($("#ecoEMap"), x => emv("itaee_yoy", x), { lim: 6, sel: c, hl: new Set(ENT.filter(x => x[5] === ENTBY[c][5]).map(x => x[0])) }));
  }

  /* ───────── monitores sectoriales ───────── */
  function vistaSecciones() {
    const tiers = [1, 2, 3].map(t => { const l = SEC.filter(s => s.t === t), tot = t === 2 ? IGS.T2 : t === 3 ? IGS.T3 : null, T = tot && tot.yoy;
      return `<div class="eco-tier"><div class="eco-tierh"><span>${TIER[t][2]}</span><div><b>Sector ${TIER[t][0].toLowerCase()}</b><small>${TIER[t][1]}</small></div>${T ? `<span class="eco-ch2 ${cc(T.last.v)}">${T.last.v >= 0 ? "▲" : "▼"} ${fp(T.last.v, 1)} anual</span>` : ""}</div><div class="eco-cards eco-cards2">${l.map(sc => { const ss = secSer(sc), I = ss[0] && ss[0].idx, Y = ss[0] && ss[0].yoy, y = ss.map(s => s.yoy.last.v), av = y.length ? y.reduce((a, b) => a + b, 0) / y.length : null;
        return `<button type="button" class="eco-card" data-go="s:${sc.k}" style="--c:${sc.col}"><div class="ct"><span>${sc.ic} ${esc(sc.n)}</span>${I ? dotE(I) : ""}</div><div class="cg">SCIAN ${sc.scian.join(" · ")}</div><div class="cv">${av == null ? "—" : fp(av, 1)}<small>var. anual${ss.length > 1 ? " (prom.)" : ""}</small></div>${Y ? `<span class="eco-ch2 ${cc(av)}">${I ? I.last.l : ""}</span>` : ""}<div class="cs">${I ? spark(I, 48, 220, 46) : ""}</div></button>`; }).join("")}</div></div>`; }).join("");
    return `<div class="eco-h"><div><h1>Monitores por sector</h1><div class="eco-sub"><span class="eco-pill">3 grandes sectores</span><span class="eco-pill">10 monitores sectoriales</span><span class="eco-pill">Clasificación basada en SCIAN</span></div></div></div>${tiers}
    <div class="eco-two">${panel("¿Quién jala la economía?", "IGAE · variación anual por subsector", '<div id="ecoSecBars"></div>')}${panel("Mapa de calor sectorial", "últimos 18 meses", '<div id="ecoSecHeat"></div>')}</div>`;
  }
  function pintaSecciones() { pintaNacSectores(); }
  function pintaNacSectores() {
    const rows = SEC.flatMap(sc => secSer(sc).map(s => ({ I: s.yoy, label: s.yoy.nombre, sub: sc.n + " · SCIAN " + s.yoy.scian, v: s.yoy.last.v, col: sc.col, go: "s:" + sc.k }))).filter(r => r.I).sort((a, b) => b.v - a.v);
    draws.push(() => barsH($("#ecoSecBars"), rows.map(r => ({ label: r.label, sub: r.sub, v: r.v, go: r.go })), { rh: 24, lw: 210 }), () => heatM($("#ecoSecHeat"), rows.map(r => ({ label: r.label, I: r.I, go: r.go })), 18, { f: ob => ob.v, lim: 10, rh: 20 }));
  }
  function ecoFp(o) { const p = $("#ecoFp"); if (!p) return; if (o && S.ficha) { p.innerHTML = '<button type="button" class="eco-fpx" aria-label="Cerrar">✕</button>' + S.ficha; p.hidden = false; } else { p.hidden = true; p.innerHTML = ""; } }
  /* bandas de ámbito: separan con claridad lo NACIONAL (IGAE) de lo ESTATAL (ITAEE) en la ficha de un sector */
  const TAGS = { nac: "NACIONAL", est: "POR ESTADO", edo: "ESTADO", ciu: "CIUDAD", cmp: "COMPARATIVO", mix: "ESTADO vs. NACIÓN", mxc: "CIUDAD · ESTADO · NACIÓN", cc: "ENTRE CIUDADES" };
  const tagAmb = k => `<span class="eco-tag ${k === "mix" || k === "mxc" ? "cmp" : k === "cc" ? "cmp" : k}">${TAGS[k] || k}</span>`;
  const bandaAmb = (cl, etq, tit, desc) => `<div class="eco-scope ${cl}"><span class="eco-sc-b">${etq}</span><b>${tit}</b><small>${desc}</small></div>`;
  function scopeBand(k, sc, stK) {
    if (k === "est") { const m = EST[S.sk || (stK && stK[0])], nm = m ? METR.find(x => x[0] === (S.sk || stK[0]))[1].replace(" · variación anual", "") : sc.n; return `<div class="eco-scope est"><span class="eco-sc-b">🗺️ DESGLOSE POR ESTADO · ITAEE</span><b>${esc(nm)} en las 32 entidades</b><small>Indicador Trimestral de la Actividad Económica Estatal: solo variación anual, trimestral. Es otra fuente y otra serie que el IGAE de arriba.</small></div>`; }
    return `<div class="eco-scope nac"><span class="eco-sc-b">🇲🇽 NACIONAL · IGAE</span><b>${esc(sc.n)} en todo México</b><small>Indicador Global de la Actividad Económica, mensual. Todo lo que sigue hasta el desglose por estado es del total del país (no depende del estado ni de la ciudad).</small></div>`;
  }
  function fichaSector(sc, I, Y, stK) {
    const T = I.cicloTC && BYID[I.cicloTC], C = I.cicloDe && BYID[I.cicloDe], st = stK.length ? EST[S.sk || stK[0]] : null, nomSt = st ? METR.find(m => m[0] === (S.sk || stK[0]))[1].replace(" · variación anual", "") : "";
    const row = (t, d) => `<tr><th>${t}</th><td>${d}</td></tr>`;
    return `<div class="eco-panel eco-ficha"><div class="eco-ph"><h3>¿De dónde sale cada dato de esta ficha?</h3><span class="eco-hint">todo se descarga de la API del INEGI (Banco de Indicadores: BIE/BISE)</span></div><table class="eco-ft"><tbody>
      ${row("Índice de volumen físico", `Serie <b>${serieRef(I)}</b> · ${esc(fuenteCorta(I))} · ${esc(I.frecuencia.toLowerCase())} · <b>nacional</b> · ${esc(I.u)}. Es la serie de la gráfica «Evolución».`)}
      ${row("Variación anual", `<b>Un solo índice y dos formas de ver su variación.</b> El KPI «Variación anual» y la gráfica «Variación anual por subsector» usan la serie ${serieRef(Y)}, que el INEGI publica ya calculada (cambio % contra el mismo mes del año anterior). En la gráfica «Evolución», «Nivel» es el índice y la «Variación anual» y la «Variación periodo previo» <b>las calcula EconoLab a partir de ese mismo índice</b>; coinciden con la serie del INEGI.`)}
      ${row("Reloj económico", I.ciclo === "oficial" && C ? `Usa la <b>misma serie ${serieRef(I)}</b> y la serie de ciclo oficial <b>${serieRef(C)}</b> del INEGI.` : T ? `Usa la <b>misma serie ${serieRef(I)}</b>. La fase se calcula en EconoLab a partir de la serie de <b>tendencia-ciclo ${serieRef(T)}</b> (publicada por el INEGI) con el filtro Hodrick-Prescott; no es otra actividad, solo la versión suavizada de esta. Nivel = desviación de esa tendencia-ciclo respecto de su tendencia de largo plazo; impulso = su cambio de 3 meses.` : "Este indicador no tiene ciclo oficial ni tendencia-ciclo para derivarlo.")}
      ${st ? row("Mapa y ranking por estado", `ITAEE (Indicador Trimestral de la Actividad Económica Estatal), serie <b>«${esc(st.def.nombre)}»</b>: <b>solo ${esc(nomSt)}</b> de cada entidad, variación anual trimestral. Es una serie distinta y trimestral (no es el IGAE nacional). ${stK.length > 1 ? "Elige abajo el subsector." : ""}`) : ""}
      ${row("Indicadores relacionados", "Todos son <b>nacionales</b> (encuestas de confianza y opinión empresarial, actividad industrial, comercio exterior). EconoLab no tiene estas series por estado ni por ciudad.")}
    </tbody></table></div>`;
  }
  function vistaSector(k) {
    const sc = SECBY[k], ss = secSer(sc); if (!ss.length) return `<div class="eco-vacio">Sin datos para este sector.</div>`;
    if (S.sub == null || S.subK !== k) { S.sub = 0; S.subK = k; } const cur = ss[Math.min(S.sub, ss.length - 1)], I = cur.idx, Y = cur.yoy, q = I.quad ? QD[I.quad] : null;
    const rel = IND.filter(x => x.sectores && x.sectores.includes(k) && !x.scian), stK = sc.st.filter(c => EST[c]);
    if (!stK.includes(S.sk)) S.sk = stK[0];
    return `<div class="eco-crumb"><a data-go="nac">Panorama</a> › <a data-go="sec">Sectores</a> › ${TIER[sc.t][0]}</div><div class="eco-h"><div><h1>${sc.ic} ${esc(sc.n)}</h1><div class="eco-sub"><span class="eco-pill" style="--c:${sc.col}">${TIER[sc.t][0]}</span>${sc.scian.map(c => `<span class="eco-pill">SCIAN ${c}</span>`).join("")}<span class="eco-pill st-${I.est[0]}">${dotE(I)} ${I.est[1]} · ${esc(I.last.l)}</span></div></div></div>
    ${ss.length > 1 ? `<div class="eco-chips" id="ecoSub">${ss.map((s, i) => `<button type="button" class="eco-chip2${i === S.sub ? " on" : ""}" style="--c:${sc.col}" data-sub="${i}">${esc(s.idx.nombre)}</button>`).join("")}</div>` : ""}
    ${scopeBand("nac", sc)}
    <div class="eco-kpis">${kpi("Índice de volumen físico", fv(I, I.last.v), esc(I.last.l) + " · «" + esc(nomOrig(I)) + "»", "big")}${kpi("Variación anual", fp(Y.last.v, 1), "IGAE · " + esc(Y.last.l) + " · «" + esc(nomOrig(Y)) + "»", cc(Y.last.v))}${kpi("Posición histórica", ring(I.pctil, sc.col, nf(I.pctil, 0) + "%"), "percentil del índice")}${kpi("Máximo histórico", fv(I, I.max), esc(I.maxO.l))}${q ? kpi("Fase del ciclo", `<span style="color:${q[1]}">${q[0]}</span>`, "del reloj económico (ver abajo)") : ""}</div>
    ${(S.ficha = fichaSector(sc, I, Y, stK), EMB ? "" : S.ficha)}
    ${panelSerie(I)}
    <div class="eco-two">${panel(tagAmb("nac") + "Variación anual por subsector", "IGAE nacional · serie " + serieRef(Y) + " vs. economía total", '<div id="ecoSLn"></div>')}${panel(tagAmb("nac") + "Reloj económico", "misma serie del índice · " + esc(cicloHint(I)), '<div id="ecoSClock"></div>')}</div>
    ${stK.length ? scopeBand("est", sc, stK) + panel(tagAmb("est") + "Mapa del sector por estado", "ITAEE · «" + esc(EST[S.sk].def.nombre) + "» · solo " + esc(METR.find(m => m[0] === S.sk)[1].replace(" · variación anual", "")) + " · trimestral · variación anual · 32 entidades", (stK.length > 1 ? `<div class="eco-chips">${stK.map(c => `<button type="button" class="eco-chip2${S.sk === c ? " on" : ""}" style="--c:${sc.col}" data-sk="${c}">${esc(METR.find(m => m[0] === c)[1].replace(" · variación anual", ""))}</button>`).join("")}</div>` : "") + '<div class="eco-grid2b"><div id="ecoSMap"></div><div id="ecoSRank"></div></div>') : ""}
    ${rel.length ? panel(tagAmb("nac") + "Indicadores relacionados", "no son parte de los filtros · solo relacionados con el sector", relNota(sc, rel, stK) + (() => { const G = relGrupos(sc.k, rel); return REL_G.filter(([k]) => G[k].length).map(([k, t, d]) => `<div class="eco-relsec"><b>${t}</b><small>${d}</small></div>` + cards(G[k], true, rel)).join(""); })()) : ""}`;
  }
  function pintaSector(k) {
    const sc = SECBY[k], ss = secSer(sc), cur = ss[Math.min(S.sub, ss.length - 1)], I = cur.idx;
    pintaSerie(I); draws.push(() => drawReloj($("#ecoSClock"), I));
    draws.push(() => multiLine($("#ecoSLn"), ss.map((s, i) => ({ name: s.yoy.nombre, col: i === 0 ? sc.col : PAL[(SEC.indexOf(sc) + i * 3) % PAL.length], pts: ptsI(s.yoy) })).concat(IGS.TOT.yoy ? [{ name: "Economía total", col: "#94a3b8", dash: true, pts: ptsI(IGS.TOT.yoy) }] : []), { anios: 14 }));
    const stK = sc.st.filter(c => EST[c]); if (stK.length) {
      const cl = S.sk, vs = ENT.map(e => emv(cl, e[0])).filter(v => v != null), lim = Math.max(4, Math.min(25, Math.ceil(Math.max(...vs.map(Math.abs), 1) * .7)));
      draws.push(() => tileMap($("#ecoSMap"), c => emv(cl, c), { lim }), () => barsH($("#ecoSRank"), ENT.map(e => ({ v: emv(cl, e[0]), e })).filter(x => x.v != null).sort((a, b) => b.v - a.v).map(x => ({ label: x.e[1], sub: ZONAS[x.e[5]][0], v: x.v, go: "e:" + x.e[0] })), { rh: 18, lw: 130 }));
    }
  }

  /* ───────── ciudades (ENOE, 39 ciudades) ───────── */
  // [código de área INEGI, nombre, entidad]
  const CIU = [["47000001", "Ciudad de México", "09"], ["47000002", "Mérida", "31"], ["47000003", "Morelia", "16"], ["47000004", "Guadalajara", "14"], ["47000005", "Monterrey", "19"], ["47000006", "Mexicali", "02"], ["47000007", "Ciudad Juárez", "08"], ["47000008", "Acapulco", "12"], ["47000009", "Culiacán", "25"], ["47000010", "León", "11"],
    ["47000011", "Puebla", "21"], ["47000012", "San Luis Potosí", "24"], ["47000013", "Tapachula", "07"], ["47000014", "Toluca", "15"], ["47000015", "Torreón", "05"], ["47000016", "Veracruz", "30"], ["47000017", "Villahermosa", "27"], ["47000018", "Tampico", "28"], ["47000019", "Chihuahua", "08"], ["47000020", "Hermosillo", "26"],
    ["47000023", "Aguascalientes", "01"], ["47000024", "Tijuana", "02"], ["47000026", "Colima", "06"], ["47000027", "La Paz", "03"], ["47000036", "Durango", "10"], ["47000037", "Tepic", "18"], ["47000038", "Oaxaca", "20"], ["47000039", "Querétaro", "22"], ["47000040", "Cuernavaca", "17"], ["47000041", "Tlaxcala", "29"],
    ["47000043", "Campeche", "04"], ["47000047", "Cancún", "23"], ["47000050", "Reynosa", "28"], ["47000051", "Coatzacoalcos", "30"], ["47000052", "Zacatecas", "32"], ["47000053", "Saltillo", "05"], ["47000054", "Tuxtla Gutiérrez", "07"], ["47000076", "Pachuca", "13"], ["47000077", "Ciudad del Carmen", "04"]];
  const CBY = Object.fromEntries(CIU.map(c => [c[0], c]));
  const CMET = [["c_desoc", "Tasa de desocupación (%)", 1], ["c_part", "Tasa de participación (%)", 0], ["c_ocupr", "Tasa de ocupación (%)", 0], ["c_subo", "Tasa de subocupación (%)", 1], ["c_ocup", "Población ocupada · variación anual", 0], ["c_desoc_m", "Desocupación de mujeres (%)", 1], ["c_desoc_h", "Desocupación de hombres (%)", 1]];
  let CST = {};
  function cargaCiu() {
    CST = {};
    (D.ciudad || []).forEach((e, k) => {
      const m = { tipo: e.tipo, def: e, by: {} };
      Object.entries(e.est).forEach(([c, obs]) => { if (!CBY[c] || obs.length < 4) return; const st = ENTBY[CBY[c][2]], I = prep(Object.assign({}, e, { obs, nombre: e.nombre + " · " + CBY[c][1], area: "Ciudades", grupo: e.nombre, mult: "", id: e.id + "-" + c }), k); I.code = c; I.col = st ? ZONAS[st[5]][1] : "#22d3ee"; if (e.clave === "c_desoc" || e.clave === "c_ocup") I.fc = true; m.by[c] = I; });
      CST[e.clave] = m;
    });
  }
  const cmv = (cl, c) => { const m = CST[cl], I = m && m.by[c]; return I ? (cl === "c_ocup" ? I.last.yoy : I.last.v) : null; };
  const cmeta = cl => CMET.find(m => m[0] === cl) || CMET[0];
  const ptsL = I => I.s.map(o => ({ t: o.t / I.ppy, l: o.l, v: o.v }));
  function cfmt(cl) { return cl === "c_ocup" ? v => fp(v, 1) : v => nf(v, 1) + "%"; }
  function bubbleCiu(el) {
    const L = CIU.map(c => ({ c, x: cmv("c_part", c[0]), y: cmv("c_desoc", c[0]), n: CST.c_ocup && CST.c_ocup.by[c[0]] ? CST.c_ocup.by[c[0]].last.v : 1 })).filter(p => p.x != null && p.y != null);
    if (!L.length) { el.innerHTML = '<div class="eco-vacio">Sin datos.</div>'; return; }
    const W = Math.max(320, el.clientWidth), H = Math.round(Math.min(480, W * .78)), m = { l: 46, r: 16, t: 14, b: 38 }, xs = L.map(p => p.x), ys = L.map(p => p.y), x0 = Math.min(...xs) - 1.5, x1 = Math.max(...xs) + 1.5, y0 = Math.max(0, Math.min(...ys) - .6), y1 = Math.max(...ys) + .6, nm = Math.max(...L.map(p => p.n)),
      X = v => m.l + (v - x0) / (x1 - x0) * (W - m.l - m.r), Y = v => H - m.b - (v - y0) / (y1 - y0) * (H - m.t - m.b), tx = ticksN(x0, x1, 6), ty = ticksN(y0, y1, 5), mx = L.reduce((a, p) => a + p.x, 0) / L.length, my = L.reduce((a, p) => a + p.y, 0) / L.length;
    let s = tx.map(v => `<line x1="${X(v).toFixed(1)}" x2="${X(v).toFixed(1)}" y1="${m.t}" y2="${H - m.b}" class="eco-gr"/><text x="${X(v).toFixed(1)}" y="${H - m.b + 15}" class="eco-ax" text-anchor="middle">${nf(v, 0)}%</text>`).join("") + ty.map(v => `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(v).toFixed(1)}" y2="${Y(v).toFixed(1)}" class="eco-gr"/><text x="${m.l - 6}" y="${(Y(v) + 4).toFixed(1)}" class="eco-ax" text-anchor="end">${nf(v, 1)}%</text>`).join("");
    s += `<line x1="${X(mx).toFixed(1)}" x2="${X(mx).toFixed(1)}" y1="${m.t}" y2="${H - m.b}" class="eco-ch"/><line x1="${m.l}" x2="${W - m.r}" y1="${Y(my).toFixed(1)}" y2="${Y(my).toFixed(1)}" class="eco-ch"/><text x="${W - m.r}" y="${H - 6}" class="eco-ax" text-anchor="end">tasa de participación →</text><text x="${m.l + 4}" y="${m.t + 10}" class="eco-ax">↑ desocupación</text>`;
    s += `<text x="${W - m.r - 4}" y="${m.t + 12}" class="eco-q" fill="#fb7185" text-anchor="end">MÁS PRESIÓN LABORAL</text><text x="${m.l + 6}" y="${H - m.b - 8}" class="eco-q" fill="#34d399">MENOS DESOCUPACIÓN</text>`;
    L.forEach((p, i) => { const st = ENTBY[p.c[2]], col = ZONAS[st[5]][1], r = 5 + 13 * Math.sqrt(p.n / nm); s += `<g class="eco-bub" data-c="${p.c[0]}" style="animation-delay:${i * 22}ms"><circle cx="${X(p.x).toFixed(1)}" cy="${Y(p.y).toFixed(1)}" r="${r.toFixed(1)}" fill="${col}" fill-opacity=".42" stroke="${col}" stroke-width="1.6"/>${r > 11 ? `<text x="${X(p.x).toFixed(1)}" y="${(Y(p.y) + 3).toFixed(1)}" class="eco-ax" text-anchor="middle" style="fill:#fff;pointer-events:none;font-size:9px">${p.c[1].slice(0, 4)}</text>` : ""}</g>`; });
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg>`;
    $$(".eco-bub", el).forEach(g => { const c = CBY[g.dataset.c], p = L.find(q => q.c === c); g.style.cursor = "pointer"; g.onmousemove = e => tip(`<b>${esc(c[1])}</b><div class="tp-s">${esc(ENTBY[c[2]][1])} · ${ZONAS[ENTBY[c[2]][5]][0]}</div><div class="tp-r nu">Desocupación: <b>${nf(p.y, 1)}%</b></div><div class="tp-r nu">Participación: <b>${nf(p.x, 1)}%</b></div><div class="tp-r nu">Ocupados: <b>${nf(p.n, 0)}</b></div>`, e); g.onmouseleave = () => tip(null); g.onclick = () => ir("c:" + c[0]); });
  }

  function vistaCiu() {
    const cl = S.cm || "c_desoc", mt = cmeta(cl), est = CST[cl] ? CST[cl].def.actualizado : "", vs = CIU.map(c => cmv(cl, c[0])).filter(v => v != null), prom = vs.length ? vs.reduce((a, b) => a + b, 0) / vs.length : null;
    return `<div class="eco-crumb"><a data-go="nac">Panorama</a> › <a data-go="reg">Regiones</a> › Ciudades</div><div class="eco-h"><div><h1>Monitor de ciudades</h1><div class="eco-sub"><span class="eco-pill">39 ciudades · ENOE</span><span class="eco-pill">Promedio simple: ${prom == null ? "—" : cfmt(cl)(prom)}</span><span class="eco-pill">Fuente: INEGI${est ? " · act. " + est : ""}</span></div></div></div>
    <div class="eco-ctl"><label class="eco-sel"><span>Indicador</span><select id="ecoCM">${CMET.map(([k, n]) => `<option value="${k}"${k === cl ? " selected" : ""}>${esc(n)}</option>`).join("")}</select></label></div>
    <div class="eco-grid2b">${panel("Ranking de ciudades", esc(mt[1]) + (mt[2] ? " · menos es mejor" : ""), '<div id="ecoCRank"></div>')}${panel("Participación vs. desocupación", "tamaño = población ocupada · color = zona", '<div id="ecoCBub"></div>')}</div>
    ${panel("Mapa de calor por ciudad", "últimos 12 trimestres", '<div id="ecoCHeat"></div>')}`;
  }
  function pintaCiu() {
    const cl = S.cm || "c_desoc", mt = cmeta(cl), m = CST[cl]; $("#ecoCM").onchange = e => { S.cm = e.target.value; rer(); };
    const r = CIU.map(c => ({ c, v: cmv(cl, c[0]) })).filter(x => x.v != null).sort((a, b) => b.v - a.v);
    draws.push(() => barsH($("#ecoCRank"), r.map(x => ({ label: x.c[1], sub: ENTBY[x.c[2]][1], v: x.v, go: "c:" + x.c[0], col: cl === "c_ocup" ? undefined : mt[2] ? "#fbbf24" : "#22d3ee" })), { rh: 21, lw: 120, fmt: cfmt(cl) }), () => bubbleCiu($("#ecoCBub")));
    if (!m) return; const vals = r.map(x => x.v), lo = Math.min(...vals), hi = Math.max(...vals);
    draws.push(() => heatM($("#ecoCHeat"), r.map(x => ({ label: x.c[1], I: m.by[x.c[0]], go: "c:" + x.c[0] })), 12, cl === "c_ocup" ? { f: ob => ob.yoy, lim: 6, rh: 18, lw: 130 } : { f: ob => ob.v, seq: true, lo, hi, rh: 18, lw: 130 }));
  }
  function vistaCiudad(code) {
    const c = CBY[code], st = ENTBY[c[2]], z = ZONAS[st[5]], g = k => CST[k] && CST[k].by[code];
    const rank = (cl, asc) => { const vs = CIU.map(x => cmv(cl, x[0])).filter(v => v != null), v = cmv(cl, code); return v == null ? null : vs.filter(w => asc ? w < v : w > v).length + 1; };
    const sDes = EST.desoc && EST.desoc.by[c[2]], nDes = BYID["444603"];
    const K = [["c_desoc", "Desocupación", "% PEA", 1, sDes ? sDes.last.v : null, "Estado"], ["c_part", "Participación", "% pob. 15+", 0, BYID["444602"] ? BYID["444602"].last.v : null, "Nacional"], ["c_subo", "Subocupación", "% de ocupados", 1, null, ""], ["c_ocup", "Población ocupada", "var. anual", 0, null, ""]];
    const tiles = K.map(([cl, n, u, asc, ref, rl]) => { const v = cmv(cl, code), I = g(cl), pct = cl !== "c_ocup", rk = rank(cl, !!asc), dl = v != null && ref != null ? v - ref : null;
      return `<div class="eco-kpi"><div class="kl">${n}</div><div class="kv ${cl === "c_ocup" ? (v >= 0 ? "up" : "dn") : "nu"}">${v == null ? "—" : cl === "c_ocup" ? fp(v, 1) : nf(v, 1) + "%"}</div><div class="ks">${u}${I ? " · " + I.last.l : ""}${cl === "c_ocup" && I ? " · " + nf(I.last.v, 0) + " personas" : ""}</div>${ref != null ? `<div class="eco-vs">${rl}: <b>${nf(ref, 1)}%</b> · <span class="${(asc ? -dl : dl) >= 0 ? "up" : "dn"}">${sg(dl, 1)} pp</span></div>` : ""}${rk ? `<div class="eco-rk">Lugar <b>${rk}</b> de 39${asc ? " (menor mejor)" : ""}</div>` : ""}</div>`; }).join("");
    const hm = cmv("c_desoc_h", code), mm = cmv("c_desoc_m", code), brecha = hm != null && mm != null ? mm - hm : null, I0 = g("c_desoc");
    return `<div class="eco-crumb"><a data-go="nac">Panorama</a> › <a data-go="reg">Regiones</a> › <a data-go="ciu">Ciudades</a> › <a data-go="e:${c[2]}">${esc(st[1])}</a></div><div class="eco-h"><div><h1>${esc(c[1])}</h1><div class="eco-sub"><span class="eco-pill" style="--c:${z[1]}">${z[0]}</span><span class="eco-pill">${esc(st[1])}</span><span class="eco-pill">Ciudad · ENOE</span>${I0 ? `<span class="eco-pill">${I0.last.l}</span>` : ""}</div></div></div>
    ${bandaAmb("ciu", "🏙️ CIUDAD · ENOE", esc(c[1]), "Mercado laboral de la ciudad (Encuesta Nacional de Ocupación y Empleo, trimestral). El INEGI no publica actividad económica ni industria por ciudad. Las comparaciones con el estado y con el país se rotulan.")}
    <div class="eco-kmap"><div class="eco-kgrid">${tiles}${kpi("Brecha de género", brecha == null ? "—" : sg(brecha, 1) + " pp", "desocupación mujeres − hombres", brecha == null ? "" : brecha > 0 ? "dn" : "up")}</div><div class="eco-mapbox"><div id="ecoMapa2"></div></div></div>
    ${I0 ? panelSerie(I0, tagAmb("ciu")) : ""}
    <div class="eco-two">${panel(tagAmb("mxc") + "Desocupación: ciudad, estado y nación", "tasa trimestral / mensual (nacional)", '<div id="ecoCL1"></div>')}${panel(tagAmb("ciu") + "Mujeres y hombres", "tasa de desocupación", '<div id="ecoCL2"></div>')}</div>
    <div class="eco-two">${panel(tagAmb("ciu") + "Empleo: población ocupada", "variación anual", '<div id="ecoCL3"></div>')}${panel(tagAmb("cc") + "Posición entre las 39 ciudades", "tasa de desocupación (la ciudad en cian)", '<div id="ecoCRk"></div><div id="ecoCBox"></div>')}</div>`;
  }
  function pintaCiudad(code) {
    const c0 = CBY[code], xy = CIU_XY[code]; if (xy) draws.push(() => ecoMapa($("#ecoMapa2"), { lat: xy[0], lon: xy[1], zoom: 10, pin: { col: ZONAS[ENTBY[c0[2]][5]][1], label: c0[1] }, puntos: CIU.filter(x => x[2] === c0[2] && x[0] !== code && CIU_XY[x[0]]).map(x => ({ lat: CIU_XY[x[0]][0], lon: CIU_XY[x[0]][1], label: x[1], go: "c:" + x[0], col: ZONAS[ENTBY[x[2]][5]][1] })) }));
    const c = CBY[code], g = k => CST[k] && CST[k].by[code], I0 = g("c_desoc"); if (I0) pintaSerie(I0);
    const sDes = EST.desoc && EST.desoc.by[c[2]], nDes = BYID["444603"], pc = I => ({ anios: 12, cero: false, fmt: v => nf(v, 1) + "%" });
    draws.push(() => multiLine($("#ecoCL1"), [I0 ? { name: c[1], col: "#22d3ee", pts: ptsL(I0), fc: fcLine(I0, "level") } : null, sDes ? { name: ENTBY[c[2]][1] + " (estado)", col: "#a78bfa", pts: ptsL(sDes), fc: fcLine(sDes, "level") } : null, nDes ? { name: "México", col: "#fbbf24", dash: true, pts: ptsL(nDes), fc: fcLine(nDes, "level") } : null].filter(Boolean), pc()));
    draws.push(() => multiLine($("#ecoCL2"), [g("c_desoc_m") ? { name: "Mujeres", col: "#f472b6", pts: ptsL(g("c_desoc_m")) } : null, g("c_desoc_h") ? { name: "Hombres", col: "#60a5fa", pts: ptsL(g("c_desoc_h")) } : null].filter(Boolean), pc()));
    const O = g("c_ocup"); draws.push(() => multiLine($("#ecoCL3"), O ? [{ name: c[1] + " · ocupados", col: "#34d399", pts: ptsE(O, "idx"), fc: fcLine(O, "yoy") }] : [], { anios: 12 }));
    const r = CIU.map(x => ({ x, v: cmv("c_desoc", x[0]) })).filter(q => q.v != null).sort((a, b) => b.v - a.v);
    draws.push(() => barsH($("#ecoCRk"), r.map(q => ({ label: q.x[1], v: q.v, go: "c:" + q.x[0], col: q.x[0] === code ? "#22d3ee" : "#475569" })), { rh: 18, lw: 120, fmt: v => nf(v, 1) + "%" }));
    draws.push(() => { const bx = $("#ecoCBox"); if (bx) bx.innerHTML = boxLeg(r.map(q => ({ c: q.x[0], v: q.v })), { pin: code, nombre: cd => CBY[cd][1], pl: "ciudades", fmt: v => nf(v, 1) + "%", sinColor: true, titulo: "Tasa de desocupación: " + esc(c[1]) + " frente a las demás ciudades" }, Math.max(280, bx.clientWidth || 420)); });
  }

  /* ───────── mini-mapa (Leaflet + mosaicos CARTO/OpenStreetMap) ───────── */
  // [latitud, longitud, zoom] por entidad (centro aproximado)
  const ENT_XY = { "01": [21.88, -102.29, 9], "02": [30.0, -115.2, 6], "03": [25.5, -111.5, 6], "04": [18.8, -90.4, 8], "05": [27.3, -102.0, 6], "06": [19.1, -103.9, 9], "07": [16.5, -92.5, 7], "08": [28.9, -106.1, 6], "09": [19.33, -99.13, 10], "10": [24.9, -104.9, 7], "11": [20.9, -101.0, 8], "12": [17.6, -100.0, 7],
    "13": [20.5, -98.9, 8], "14": [20.5, -103.8, 7], "15": [19.4, -99.6, 8], "16": [19.2, -101.9, 7], "17": [18.75, -99.1, 9], "18": [21.8, -104.9, 8], "19": [25.6, -99.9, 7], "20": [17.1, -96.7, 7], "21": [19.0, -97.9, 8], "22": [20.85, -99.85, 9], "23": [19.6, -88.0, 7], "24": [22.6, -100.4, 7],
    "25": [25.0, -107.5, 7], "26": [29.5, -110.8, 6], "27": [17.9, -92.6, 8], "28": [24.3, -98.6, 7], "29": [19.4, -98.15, 10], "30": [19.5, -96.7, 7], "31": [20.8, -89.0, 8], "32": [23.2, -102.7, 7] };
  const CIU_XY = { "47000001": [19.43, -99.13], "47000002": [20.97, -89.62], "47000003": [19.70, -101.19], "47000004": [20.67, -103.35], "47000005": [25.67, -100.31], "47000006": [32.66, -115.47], "47000007": [31.69, -106.42], "47000008": [16.85, -99.88], "47000009": [24.80, -107.39], "47000010": [21.12, -101.68],
    "47000011": [19.04, -98.20], "47000012": [22.15, -100.98], "47000013": [14.91, -92.26], "47000014": [19.29, -99.65], "47000015": [25.54, -103.41], "47000016": [19.19, -96.15], "47000017": [17.99, -92.93], "47000018": [22.22, -97.86], "47000019": [28.63, -106.09], "47000020": [29.07, -110.96],
    "47000023": [21.88, -102.29], "47000024": [32.51, -117.04], "47000026": [19.24, -103.72], "47000027": [24.14, -110.31], "47000036": [24.02, -104.66], "47000037": [21.50, -104.89], "47000038": [17.06, -96.72], "47000039": [20.59, -100.39], "47000040": [18.92, -99.23], "47000041": [19.32, -98.24],
    "47000043": [19.85, -90.53], "47000047": [21.16, -86.85], "47000050": [26.08, -98.28], "47000051": [18.15, -94.42], "47000052": [22.77, -102.58], "47000053": [25.42, -101.00], "47000054": [16.75, -93.12], "47000076": [20.12, -98.73], "47000077": [18.65, -91.82] };
  function leafletListo(cb) {
    if (window.L) return cb();
    if (window._rsvEnsureLeaflet_) return window._rsvEnsureLeaflet_(cb);
    const l = document.createElement("link"); l.rel = "stylesheet"; l.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"; document.head.appendChild(l);
    const s = document.createElement("script"); s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"; s.onload = cb; document.head.appendChild(s);
  }
  function ecoMapa(el, o) {   // o: { lat, lon, zoom, pin: {col, label}, puntos: [{lat, lon, label, go, col}] }
    leafletListo(() => {
      if (!el || !el.isConnected) return; if (S.map) { try { S.map.remove(); } catch (_) { } S.map = null; }
      const claro = $("#ecoApp").classList.contains("eco-light"), mp = L.map(el, { zoomControl: true, scrollWheelZoom: false, attributionControl: true, zoomSnap: .5 }).setView([o.lat, o.lon], o.zoom); S.map = mp;
      if (el.parentNode) el.parentNode.classList.toggle("oscuro", !claro);
      L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 18, attribution: "© OpenStreetMap" }).addTo(mp);
      (o.puntos || []).forEach(p => L.marker([p.lat, p.lon], { icon: L.divIcon({ className: "", html: `<div class="eco-dotm" style="--c:${p.col}"></div>`, iconSize: [14, 14], iconAnchor: [7, 7] }) }).bindTooltip(p.label, { direction: "top", offset: [0, -6] }).on("click", () => p.go && ir(p.go)).addTo(mp));
      if (o.pin) L.marker([o.pin.lat != null ? o.pin.lat : o.lat, o.pin.lon != null ? o.pin.lon : o.lon], { icon: L.divIcon({ className: "", html: `<div class="eco-pin" style="--c:${o.pin.col}"></div>`, iconSize: [24, 24], iconAnchor: [12, 12] }), zIndexOffset: 1000 }).bindTooltip(o.pin.label, { permanent: true, direction: "top", offset: [0, -12], className: "eco-tt2" }).addTo(mp);
      setTimeout(() => { try { mp.invalidateSize(); } catch (_) { } }, 120);
    });
  }

  /* ───────── mapa del ciclo animado: Play, estela y selección de series ───────── */
  function cyData() {
    if (S._cyD) return S._cyD;
    const toM = (I, o) => I.ppy === 12 ? o.t : I.ppy === 4 ? Math.floor(o.t / 4) * 12 + (o.t % 4) * 3 + 2 : o.t * 12 + 11, series = [], byId = {};
    IND.forEach(I => { if (I.cicloDe) return; const pts = I.s.filter(o => o.x != null && o.y != null).map(o => ({ m: toM(I, o), x: o.x, y: o.y, l: o.l })); if (pts.length >= 12) { const rec = pts.slice(-120), qc = { exp: 0, des: 0, con: 0, rec: 0 }; rec.forEach(p => { qc[cyQ(p)]++; }); const fr = k => qc[k] / (rec.length || 1), fases = Object.keys(qc).filter(k => fr(k) >= .08).length, lado = Math.max(fr("exp") + fr("des"), fr("con") + fr("rec")), s = { I, pts, fases, lado, qc: Object.fromEntries(Object.keys(qc).map(k => [k, Math.round(fr(k) * 100)])), cov: lado >= .85 ? "tend" : fases >= 4 && lado <= .8 ? "ok" : "par" }; series.push(s); byId[I.id] = s; } });
    const mags = series.flatMap(s => s.pts.map(p => Math.max(Math.abs(p.x), Math.abs(p.y)))).sort((a, b) => a - b), p97 = mags[Math.floor(mags.length * .97)] || 3;
    S._cyD = { series, byId, start: 2010 * 12, end: Math.max(...series.map(s => s.pts[s.pts.length - 1].m)), R: Math.max(2.2, Math.min(4, p97 * 1.1)) };
    return S._cyD;
  }
  const cyAt = (s, T) => { const a = s.pts; let lo = 0, hi = a.length - 1, r = -1; while (lo <= hi) { const mid = (lo + hi) >> 1; if (a[mid].m <= T) { r = mid; lo = mid + 1; } else hi = mid - 1; } return r >= 0 && T - a[r].m <= 15 ? a[r] : null; };
  const cyQ = p => p.x >= 0 ? (p.y >= 0 ? "exp" : "des") : (p.y >= 0 ? "rec" : "con");
  const cyFecha = m => MES[((m % 12) + 12) % 12] + " " + Math.floor(m / 12);
  function cicloAnimado(el) {
    const D2 = cyData(); if (!D2.series.length) { el.innerHTML = '<div class="eco-vacio">Sin datos para el ciclo.</div>'; return; }
    const cy = S.cy = S.cy || { sel: new Set(["334338", "334339", "334349", "334344"].filter(id => D2.byId[id])), trail: 12, speed: 2, t: D2.end, playing: false };
    if (S.cyRaf) { cancelAnimationFrame(S.cyRaf); S.cyRaf = 0; }
    const W = Math.max(320, el.clientWidth), H = Math.round(Math.min(560, Math.max(340, W * .56))), m = 34, R = D2.R, X = v => m + (Math.max(-R * 1.04, Math.min(R * 1.04, v)) / R + 1) / 2 * (W - 2 * m), Y = v => m + (1 - (Math.max(-R * 1.04, Math.min(R * 1.04, v)) / R + 1) / 2) * (H - 2 * m), cx = X(0), cyy = Y(0);
    let st = ""; [["exp", cx, m, W - m, cyy], ["des", cx, cyy, W - m, H - m], ["con", m, cyy, cx, H - m], ["rec", m, m, cx, cyy]].forEach(([k, x0, y0, x1, y1]) => { st += `<rect x="${x0}" y="${y0}" width="${x1 - x0}" height="${y1 - y0}" fill="${QD[k][1]}" opacity=".09"/>`; });
    st += `<line x1="${m}" x2="${W - m}" y1="${cyy}" y2="${cyy}" class="eco-ch"/><line x1="${cx}" x2="${cx}" y1="${m}" y2="${H - m}" class="eco-ch"/>`;
    const SUB = { exp: "sobre su tendencia · mejorando", des: "sobre su tendencia · perdiendo fuerza", con: "bajo su tendencia · empeorando", rec: "bajo su tendencia · mejorando" };
    [["exp", W - m - 8, m + 18, "end", 1], ["des", W - m - 8, H - m - 24, "end", -1], ["con", m + 8, H - m - 24, "start", -1], ["rec", m + 8, m + 18, "start", 1]].forEach(([k, x, y, a, d]) => { st += `<text x="${x}" y="${y}" text-anchor="${a}" class="eco-q" fill="${QD[k][1]}">${QD[k][0].toUpperCase()}</text><text x="${x}" y="${y + 13}" text-anchor="${a}" class="eco-ax" style="font-size:10px">${SUB[k]}</text>`; });
    st += `<text x="${W - m}" y="${cyy - 6}" class="eco-ax" text-anchor="end">nivel vs tendencia →</text><text x="${cx + 6}" y="${m - 10}" class="eco-ax">↑ impulso</text><text id="ecoCyD" x="${W / 2}" y="${H / 2 + 14}" text-anchor="middle" class="eco-cyd"></text>`;
    const nsel = () => D2.series.filter(s => cy.sel.has(s.I.id)).length;
    el.innerHTML = `<div class="eco-cybar"><button type="button" class="eco-cyplay" id="ecoCyP" title="Reproducir / pausar">▶</button><button type="button" class="eco-cybtn" id="ecoCyR" title="Volver al inicio">⏮</button>
      <input type="range" id="ecoCyS" class="eco-cyslider" min="${D2.start}" max="${D2.end}" value="${cy.t}"><span class="eco-cyfecha" id="ecoCyF">${cyFecha(cy.t)}</span>
      <div class="eco-cygrp"><span>Velocidad</span>${[1, 2, 4].map(v => `<button type="button" class="eco-chip3${cy.speed === v ? " on" : ""}" data-sp="${v}">${v}×</button>`).join("")}</div>
      <div class="eco-cygrp"><span>Estela</span>${[[0, "Sin"], [6, "6 m"], [12, "12 m"], [24, "24 m"], [48, "48 m"]].map(([v, t]) => `<button type="button" class="eco-chip3${cy.trail === v ? " on" : ""}" data-tr="${v}">${t}</button>`).join("")}</div>
      <div class="eco-cysel"><button type="button" class="eco-chip3 on" id="ecoCyB">☰ Series (<b id="ecoCyN">${nsel()}</b>) ▾</button><div class="eco-cypop" id="ecoCyPop" style="display:none"></div></div></div>
      <div class="eco-cywrap"><svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" id="ecoCySvg">${st}<g id="ecoCyG"></g></svg></div><div class="eco-cycnt" id="ecoCyC"></div><div class="eco-leg eco-legl">${AREAS.map(a => `<span><i style="background:${a.col}"></i>${esc(a.n)}</span>`).join("")}</div>
      <details class="eco-cyrules" open><summary>📘 Reglas y criterios de cada fase del ciclo</summary>
      <div class="eco-cyrb"><p>Solo se grafican series que tienen <b>ciclo oficial del INEGI</b> o una <b>tendencia-ciclo oficial</b> de la que se puede derivar. Cada punto se ubica con dos coordenadas: <b>nivel</b> (eje horizontal: cuánto está el ciclo por encima o por debajo de su tendencia) e <b>impulso</b> (eje vertical: hacia dónde se mueve).</p>
      <table class="eco-cyt"><thead><tr><th>Fase</th><th>Regla</th><th>Qué significa</th></tr></thead><tbody>
        <tr style="--c:#34d399"><td><b>Expansión</b></td><td>nivel ≥ 0 e impulso ≥ 0</td><td>El ciclo está por encima de su tendencia y sigue subiendo.</td></tr>
        <tr style="--c:#fbbf24"><td><b>Desaceleración</b></td><td>nivel ≥ 0 e impulso &lt; 0</td><td>Sigue por encima de su tendencia, pero ya baja.</td></tr>
        <tr style="--c:#fb7185"><td><b>Contracción</b> <small>(recesión en el lenguaje del INEGI)</small></td><td>nivel &lt; 0 e impulso &lt; 0</td><td>Está por debajo de su tendencia y sigue bajando.</td></tr>
        <tr style="--c:#60a5fa"><td><b>Recuperación</b></td><td>nivel &lt; 0 e impulso ≥ 0</td><td>Sigue por debajo de su tendencia, pero ya sube; suele anteceder a la expansión.</td></tr></tbody></table>
      <p><b>Recorrido normal</b> (sentido de las manecillas del reloj): Expansión → Desaceleración → Contracción → Recuperación → Expansión.</p>
      <p><b>De dónde sale el ciclo.</b></p><ul>
        <li><em class="eco-cyb ofi">ciclo oficial</em> Series «Ciclo» del <i>Sistema de Indicadores Cíclicos</i> del INEGI (indicador coincidente, adelantado y sus componentes). El ciclo viene en puntos, con 100 = tendencia: <i>nivel</i> = (ciclo − 100) ÷ su desviación estándar histórica; <i>impulso</i> = cambio del nivel en 3 meses.</li>
        <li><em class="eco-cyb tcb">deriv. de tend.-ciclo</em> IGAE por sector, actividad industrial y ITAEE estatal: el INEGI publica su serie de <i>tendencia-ciclo</i> (ya desestacionalizada y suavizada). El ciclo es la desviación (%) de esa tendencia-ciclo respecto de su tendencia de largo plazo, estimada con el filtro de Hodrick-Prescott (λ = 129,600 mensual; 1,600 trimestral). Los datos más recientes son menos firmes (efecto de extremo del filtro y revisiones del INEGI).</li>
        <li><b>Series contracíclicas.</b> La desocupación sube cuando la economía se debilita; por eso su ciclo se <i>invierte</i> (menos desocupación = por encima de la tendencia) para que “Expansión” siempre signifique mejor situación económica.</li>
        <li><b>No aplicable.</b> Precios (INPC), saldos que cruzan por cero, variaciones anuales, series sin tendencia-ciclo y las ciudades no se grafican: su comportamiento no es un ciclo alrededor de una tendencia y la lectura engañaría.</li></ul>
      <p><b>Teoría en breve.</b> El ciclo económico se mide sobre el componente cíclico de indicadores <i>coincidentes</i> (actividad, empleo, ventas) y <i>adelantados</i> (expectativas, mercados, comercio exterior): se quita la estacionalidad, se separa la tendencia de largo plazo y se mira la desviación. Aplicar el mismo método a series con tendencia dominante (como los precios) deja a la serie atrapada en Expansión y Desaceleración.</p></div></details>`;
    const svg = $("#ecoCySvg", el), g = $("#ecoCyG", el), btn = $("#ecoCyP", el), sl = $("#ecoCyS", el);
    function draw() {
      const T = cy.t, L = cy.trail, cnt = { exp: 0, des: 0, con: 0, rec: 0 }, list = D2.series.filter(s => cy.sel.has(s.I.id)), fino = list.length <= 30; let h = "";
      list.forEach(s => { const p = cyAt(s, T); if (!p) return; cnt[cyQ(p)]++;
        if (L > 0) { const tp = []; for (let k = L; k >= 0; k--) { const q = cyAt(s, T - k); if (q && (!tp.length || tp[tp.length - 1] !== q)) tp.push(q); }
          if (tp.length > 1) { if (fino) { for (let i = 1; i < tp.length; i++) h += `<line x1="${X(tp[i - 1].x).toFixed(1)}" y1="${Y(tp[i - 1].y).toFixed(1)}" x2="${X(tp[i].x).toFixed(1)}" y2="${Y(tp[i].y).toFixed(1)}" stroke="${s.I.col}" stroke-width="${(1 + 2 * i / tp.length).toFixed(1)}" stroke-linecap="round" stroke-opacity="${(.12 + .7 * i / tp.length).toFixed(2)}"/>`; }
            else h += `<path d="${tp.map((q, i) => (i ? "L" : "M") + X(q.x).toFixed(1) + " " + Y(q.y).toFixed(1)).join("")}" fill="none" stroke="${s.I.col}" stroke-width="1.6" stroke-opacity=".38" stroke-linejoin="round"/>`; } }
        h += `<g class="eco-cyh" data-id="${s.I.id}" style="cursor:pointer"><circle cx="${X(p.x).toFixed(1)}" cy="${Y(p.y).toFixed(1)}" r="${fino ? 11 : 8}" fill="${s.I.col}" fill-opacity=".25"/><circle cx="${X(p.x).toFixed(1)}" cy="${Y(p.y).toFixed(1)}" r="${fino ? 6 : 4.5}" fill="${s.I.col}" stroke="#fff" stroke-width="1.6"/></g>`;
        if (list.length <= 5) h += `<text x="${(X(p.x) + 10).toFixed(1)}" y="${(Y(p.y) - 8).toFixed(1)}" class="eco-ax" style="fill:${s.I.col};font-weight:700;paint-order:stroke;stroke:rgba(8,13,28,.55);stroke-width:3px">${esc(s.I.nombre.length > 26 ? s.I.nombre.slice(0, 25) + "…" : s.I.nombre)}</text>`; });
      g.innerHTML = h; $("#ecoCyD", svg).textContent = cyFecha(T); $("#ecoCyF", el).textContent = cyFecha(T); sl.value = T;
      $("#ecoCyC", el).innerHTML = Object.keys(QD).map(k => `<span class="eco-cyq" style="--c:${QD[k][1]}"><i></i>${QD[k][0]} <b>${cnt[k]}</b></span>`).join("") + `<span class="eco-hint">${list.length ? "" : "Elige al menos una serie"}</span>`;
    }
    const paint = () => { btn.textContent = cy.playing ? "❚❚" : "▶"; };
    function loop(ts) { if (!el.isConnected || !cy.playing) { S.cyRaf = 0; return; } if (!loop.last) loop.last = ts; loop.acc = (loop.acc || 0) + ts - loop.last; loop.last = ts; const per = 1000 / (5 * cy.speed);
      while (loop.acc >= per) { loop.acc -= per; cy.t++; if (cy.t >= D2.end) { cy.t = D2.end; cy.playing = false; paint(); break; } } draw(); S.cyRaf = cy.playing ? requestAnimationFrame(loop) : 0; }
    function play(on) { cy.playing = on; paint(); if (on) { if (cy.t >= D2.end) cy.t = D2.start; loop.last = 0; loop.acc = 0; S.cyRaf = requestAnimationFrame(loop); } else if (S.cyRaf) { cancelAnimationFrame(S.cyRaf); S.cyRaf = 0; } }
    btn.onclick = () => play(!cy.playing); $("#ecoCyR", el).onclick = () => { cy.t = D2.start; draw(); };
    sl.oninput = () => { cy.t = +sl.value; if (cy.playing) play(false); draw(); };
    el.querySelectorAll("[data-sp]").forEach(b => { b.onclick = () => { cy.speed = +b.dataset.sp; el.querySelectorAll("[data-sp]").forEach(x => x.classList.toggle("on", x === b)); }; });
    el.querySelectorAll("[data-tr]").forEach(b => { b.onclick = () => { cy.trail = +b.dataset.tr; el.querySelectorAll("[data-tr]").forEach(x => x.classList.toggle("on", x === b)); draw(); }; });
    svg.onmousemove = e => { const hd = e.target.closest(".eco-cyh"); if (!hd) { tip(null); return; } const s = D2.byId[hd.dataset.id], p = cyAt(s, cy.t); if (!p) return; const q = QD[cyQ(p)]; tip(`<b>${esc(s.I.nombre)}</b><div class="tp-s">${esc(s.I.area)} · ${cyFecha(cy.t)}</div><div class="tp-v" style="color:${q[1]}">${q[0]}</div><div class="tp-r nu">Nivel vs tendencia: ${nf(p.x, 2)} σ</div><div class="tp-r nu">Impulso: ${nf(p.y, 2)}</div><div class="tp-r nu">${s.I.ciclo === "oficial" ? "Ciclo oficial INEGI" : "Derivado de tendencia-ciclo INEGI"} · clic para abrir</div>`, e); };
    svg.onmouseleave = () => tip(null); svg.onclick = e => { const hd = e.target.closest(".eco-cyh"); if (hd) ir("i:" + hd.dataset.id); };
    // selector de series (casillas con palomita; marcar no vuelve a dibujar la lista)
    const pop = $("#ecoCyPop", el), grupos = AREAS.map(a => ({ a, l: D2.series.filter(s => s.I.area === a.n) })).filter(x => x.l.length);
    pop.innerHTML = `<div class="eco-cypt"><input type="search" id="ecoCyQ" placeholder="Buscar serie…" autocomplete="off"><div class="eco-cyq2"><button type="button" class="eco-chip3" data-q="clave">Clave</button><button type="button" class="eco-chip3" data-q="todas">Todas</button><button type="button" class="eco-chip3" data-q="ciclo" title="Solo series que recorren las 4 fases en los últimos 10 años">Ciclo completo</button><button type="button" class="eco-chip3" data-q="ninguna">Ninguna</button></div></div><div class="eco-cylist">` +
      grupos.map(x => `<div class="eco-cyg" data-a="${esc(x.a.n)}"><div class="eco-cygh" style="--c:${x.a.col}"><i></i><b>${esc(x.a.n)}</b><button type="button" class="eco-lnk" data-ga="${esc(x.a.n)}">marcar / quitar grupo</button></div>${x.l.map(s => `<div class="eco-cyrow" data-id="${s.I.id}" data-n="${esc(norm(s.I.nombre + " " + s.I.grupo))}"><span class="eco-ckb${cy.sel.has(s.I.id) ? " on" : ""}">✓</span><span class="eco-cyn">${esc(s.I.nombre)}</span><em class="eco-cyb ${s.I.ciclo === 'oficial' ? 'ofi' : 'tcb'}" title="${s.I.ciclo === 'oficial' ? 'Ciclo oficial del INEGI (Sistema de Indicadores Cíclicos)' : 'Derivado de la tendencia-ciclo oficial (filtro HP)'} · últimos 10 años: expansión ${s.qc.exp}% · desaceleración ${s.qc.des}% · contracción ${s.qc.con}% · recuperación ${s.qc.rec}%">${s.I.ciclo === "oficial" ? "ciclo oficial" : "deriv. de tend.-ciclo"}</em></div>`).join("")}</div>`).join("") + `</div>`;
    const upd = () => { $("#ecoCyN", el).textContent = nsel(); draw(); };
    pop.onclick = e => { const r = e.target.closest(".eco-cyrow"), q = e.target.closest("[data-q]"), ga = e.target.closest("[data-ga]");
      if (r) { const id = r.dataset.id; cy.sel.has(id) ? cy.sel.delete(id) : cy.sel.add(id); r.querySelector(".eco-ckb").classList.toggle("on", cy.sel.has(id)); upd(); }
      else if (ga) { const rows = [...pop.querySelectorAll(`.eco-cyg[data-a="${CSS.escape(ga.dataset.ga)}"] .eco-cyrow`)], todas = rows.every(x => cy.sel.has(x.dataset.id)); rows.forEach(x => { todas ? cy.sel.delete(x.dataset.id) : cy.sel.add(x.dataset.id); x.querySelector(".eco-ckb").classList.toggle("on", !todas); }); upd(); }
      else if (q) { const k = q.dataset.q; cy.sel = new Set(k === "todas" ? D2.series.map(s => s.I.id) : k === "ciclo" ? D2.series.filter(s => s.cov === "ok").map(s => s.I.id) : k === "clave" ? ["334338", "334339", "334349", "334344"].filter(id => D2.byId[id]) : []); pop.querySelectorAll(".eco-cyrow").forEach(x => x.querySelector(".eco-ckb").classList.toggle("on", cy.sel.has(x.dataset.id))); upd(); } };
    $("#ecoCyQ", el).oninput = e => { const w = norm(e.target.value.trim()).split(/\s+/).filter(Boolean); pop.querySelectorAll(".eco-cyrow").forEach(x => { x.style.display = w.every(t => x.dataset.n.includes(t)) ? "" : "none"; }); pop.querySelectorAll(".eco-cyg").forEach(gp => { gp.style.display = [...gp.querySelectorAll(".eco-cyrow")].some(x => x.style.display !== "none") ? "" : "none"; }); };
    $("#ecoCyB", el).onclick = e => { e.stopPropagation(); pop.style.display = pop.style.display === "none" ? "" : "none"; };
    if (!S._cyDoc) { S._cyDoc = true; document.addEventListener("click", ev => { const p = $("#ecoCyPop"); if (p && p.style.display !== "none" && !ev.target.closest(".eco-cysel")) p.style.display = "none"; }); }
    paint(); draw(); if (cy.playing) play(true);
  }
  function mapaCiclo(el) { return cicloAnimado(el); }

  /* ───────── navegación y árbol ───────── */
  const modo = id => /^(nac|reg|sec|ciu|z:|e:|s:|c:)/.test(id) ? "mon" : "cat";
  const rer = () => { const m = $("#ecoMain2"), st = m.scrollTop; ir(S.sel); m.scrollTop = st; };
  function ir(id) {
    if (id === "home") id = "nac";
    if (!S._noPush && S.sel && S.sel !== id && S.sel !== "home") { S.hist.push(S.sel); if (S.hist.length > 40) S.hist.shift(); }
    S.sel = id; ls.set("eco_sel", id); S.zoom = {}; S.ficha = null; draws = []; tip(null); if (S.cyRaf) { cancelAnimationFrame(S.cyRaf); S.cyRaf = 0; } if (S.cy) S.cy.playing = false; if (S.map) { try { S.map.remove(); } catch (_) { } S.map = null; }
    const main = $("#ecoMain2"), mo = modo(id); if (mo === "mon") S.lastMon = id; else S.lastCat = id;
    if (mo === "mon") {
      if (id === "reg") { main.innerHTML = vistaReg(null); pintaReg(null); S.open.add("R"); }
      else if (id.startsWith("z:")) { const zi = +id.slice(2); if (!ZONAS[zi]) return ir("reg"); S.open.add("R"); S.open.add("z:" + zi); main.innerHTML = vistaReg(zi); pintaReg(zi); }
      else if (id.startsWith("e:")) { const c = id.slice(2); if (!ENTBY[c]) return ir("reg"); S.open.add("R"); S.open.add("z:" + ENTBY[c][5]); main.innerHTML = vistaEst(c); pintaEst(c); }
      else if (id === "ciu") { S.open.add("R"); S.open.add("CI"); main.innerHTML = vistaCiu(); pintaCiu(); }
      else if (id.startsWith("c:")) { const c = id.slice(2); if (!CBY[c]) return ir("ciu"); S.open.add("CI"); main.innerHTML = vistaCiudad(c); pintaCiudad(c); }
      else if (id === "sec") { main.innerHTML = vistaSecciones(); pintaSecciones(); }
      else if (id.startsWith("s:")) { const k = id.slice(2); if (!SECBY[k]) return ir("sec"); S.open.add("SC"); S.open.add("t:" + SECBY[k].t); main.innerHTML = vistaSector(k); pintaSector(k); }
      else { S.sel = "nac"; main.innerHTML = vistaNac(); pintaNac(); }
    } else if (id.startsWith("i:") && BYID[id.slice(2)]) { const I = BYID[id.slice(2)]; S.open.add("a:" + I.area); S.open.add("g:" + I.area + "|" + I.grupo); main.innerHTML = vistaInd(I); pintaInd(I); }
    else if (id.startsWith("g:")) { const [an, gn] = id.slice(2).split("|"), a = AREAS.find(x => x.n === an), g = a && a.g.find(x => x.n === gn); if (!g) return ir("cat"); S.open.add("a:" + an); main.innerHTML = vistaPano(g.n, `<span class="eco-pill" style="--c:${a.col}">${esc(a.n)}</span><span class="eco-pill">${g.i.length} indicadores</span>`, g.i, `<a data-go="home">Panorama</a> › <a data-go="a:${esc(a.n)}">${esc(a.n)}</a>`); pintaPano(g.i); }
    else if (id.startsWith("a:")) { const a = AREAS.find(x => x.n === id.slice(2)); if (!a) return ir("cat"); S.open.add(id); const l = a.g.flatMap(g => g.i); main.innerHTML = vistaPano(a.n, `<span class="eco-pill" style="--c:${a.col}">${a.g.length} grupos</span><span class="eco-pill">${l.length} indicadores</span>`, l, `<a data-go="home">Panorama</a>`); pintaPano(l); }
    else { S.sel = "cat"; main.innerHTML = vistaHome(); draws.push(() => mapaCiclo($("#ecoMapa"))); }
    { const fb = fueraBanner(id); if (fb) main.insertAdjacentHTML("afterbegin", fb); }
    draws.forEach(f => f()); main.scrollTop = 0; arbol(); $("#ecoApp").classList.remove("eco-menu");
    if (EMB) { const h1 = main.querySelector("h1"), wt = $("#ecoWT"); if (wt) wt.textContent = h1 ? h1.textContent : "EconoLab"; const wq = $("#ecoWQ"); if (wq) wq.style.display = S.ficha ? "" : "none"; ecoFp(false); const wb = $("#ecoWB"); if (wb) wb.style.display = S.hist.length ? "" : "none"; }
  }
  function arbolCat() {
    const q = norm(S.q.trim()), ws = q.split(/\s+/).filter(Boolean), F = ctxF(), pasa = I => (!F || !F.giro || (I.sectores && I.sectores.includes(F.giro))) && (!ws.length || ws.every(w => norm(I.nombre + " " + I.descripcion + " " + I.grupo + " " + I.area + " " + I.tema + " " + I.id).includes(w)));
    let h = ctxBar() + `<button type="button" class="eco-n eco-n0${S.sel === "cat" ? " on" : ""}" data-go="cat"><span class="ic">🗂️</span><span class="tx">Catálogo completo</span><span class="bd">${F && F.giro ? IND.filter(pasa).length : IND.length}</span></button>`, any = false;
    AREAS.forEach(a => {
      const gs = a.g.map(g => ({ g, i: g.i.filter(pasa) })).filter(x => x.i.length); if (!gs.length) return; any = true;
      const op = q ? true : S.open.has("a:" + a.n), tot = gs.reduce((s, x) => s + x.i.length, 0);
      h += `<div class="eco-ar" style="--c:${a.col}"><div class="eco-n eco-n1${S.sel === "a:" + a.n ? " on" : ""}"><button type="button" class="tg" data-tg="a:${esc(a.n)}" aria-label="Desplegar">${op ? "▾" : "▸"}</button><button type="button" class="nm" data-go="a:${esc(a.n)}"><i></i><span class="tx">${esc(a.n)}</span><span class="bd">${tot}</span></button></div>`;
      if (op) gs.forEach(({ g, i }) => { const gid = "g:" + a.n + "|" + g.n, go = q ? true : S.open.has(gid); h += `<div class="eco-n eco-n2${S.sel === gid ? " on" : ""}"><button type="button" class="tg" data-tg="${esc(gid)}" aria-label="Desplegar">${go ? "▾" : "▸"}</button><button type="button" class="nm" data-go="${esc(gid)}"><span class="tx">${esc(g.n)}</span><span class="bd">${i.length}</span></button></div>`; if (go) i.forEach(I => { h += `<button type="button" class="eco-n eco-n3${S.sel === "i:" + I.id ? " on" : ""}" data-go="i:${I.id}" title="${esc(I.descripcion)}"><span class="tx">${esc(I.nombre)}</span>${dotE(I)}</button>`; }); });
      h += `</div>`;
    });
    if (!any) h += `<div class="eco-nada">Sin resultados para «${esc(S.q)}».</div>`;
    return h;
  }

  function arbolMon() {
    const q = norm(S.q.trim()), on = id => S.sel === id ? " on" : "", F = ctxF();
    let h = ctxBar() + `<button type="button" class="eco-n eco-n0${on("nac")}" data-go="nac"><span class="ic">🌐</span><span class="tx">Panorama global</span><span class="bd">MX</span></button>`;
    const opR = q || S.open.has("R");
    h += `<div class="eco-ar" style="--c:#22d3ee"><div class="eco-n eco-n1${on("reg")}"><button type="button" class="tg" data-tg="R" aria-label="Desplegar">${opR ? "▾" : "▸"}</button><button type="button" class="nm" data-go="reg"><i></i><span class="tx">Regiones y estados</span><span class="bd">32</span></button></div>`;
    if (opR) ZONAS.forEach((z, i) => { const es = ENT.filter(e => e[5] === i && (!q || norm(e[1] + " " + z[0]).includes(q)) && (!F || !F.ent || e[0] === F.ent)); if (!es.length) return; const zo = q || S.open.has("z:" + i);
      h += `<div class="eco-n eco-n2${on("z:" + i)}" style="--c:${z[1]}"><button type="button" class="tg" data-tg="z:${i}" aria-label="Desplegar">${zo ? "▾" : "▸"}</button><button type="button" class="nm" data-go="z:${i}"><span class="tx">${z[0]}</span><span class="bd">${es.length}</span></button></div>`;
      if (zo) es.forEach(e => { h += `<button type="button" class="eco-n eco-n3${on("e:" + e[0])}" data-go="e:${e[0]}"><span class="tx">${e[1]}</span></button>`; }); });
    h += `</div>`;
    const opC = q || S.open.has("CI"), cs = CIU.filter(c => (!q || norm(c[1] + " " + ENTBY[c[2]][1]).includes(q)) && (!F || !F.ent || c[2] === F.ent || c[0] === F.city)).sort((a, b) => a[1].localeCompare(b[1], "es"));
    if (cs.length) { h += `<div class="eco-ar" style="--c:#34d399"><div class="eco-n eco-n1${on("ciu")}"><button type="button" class="tg" data-tg="CI" aria-label="Desplegar">${opC ? "▾" : "▸"}</button><button type="button" class="nm" data-go="ciu"><i></i><span class="tx">Ciudades</span><span class="bd">${CIU.length}</span></button></div>`;
      if (opC) cs.forEach(c => { h += `<button type="button" class="eco-n eco-n3${on("c:" + c[0])}" data-go="c:${c[0]}"><span class="tx">${esc(c[1])}</span></button>`; }); h += `</div>`; }
    const opS = q || S.open.has("SC");
    h += `<div class="eco-ar" style="--c:#fbbf24"><div class="eco-n eco-n1${on("sec")}"><button type="button" class="tg" data-tg="SC" aria-label="Desplegar">${opS ? "▾" : "▸"}</button><button type="button" class="nm" data-go="sec"><i></i><span class="tx">Sectores económicos</span><span class="bd">${SEC.length}</span></button></div>`;
    if (opS) [1, 2, 3].forEach(t => { const l = SEC.filter(s => s.t === t && (!q || norm(s.n + " " + TIER[t][0]).includes(q)) && (!F || !F.giro || s.k === F.giro)); if (!l.length) return; const to = q || S.open.has("t:" + t);
      h += `<div class="eco-n eco-n2"><button type="button" class="tg" data-tg="t:${t}" aria-label="Desplegar">${to ? "▾" : "▸"}</button><button type="button" class="nm" data-tg="t:${t}"><span class="tx">${TIER[t][2]} ${TIER[t][0]}</span><span class="bd">${l.length}</span></button></div>`;
      if (to) l.forEach(s => { h += `<button type="button" class="eco-n eco-n3${on("s:" + s.k)}" data-go="s:${s.k}"><span class="tx">${s.ic} ${esc(s.n)}<small>Monitor del sector · IGAE</small></span></button>`; if (S.sel === "s:" + s.k || (F && F.giro === s.k)) h += relMenu(s); }); });
    h += `</div>`;
    if (q && h.indexOf('data-go="e:') < 0 && h.indexOf('data-go="s:') < 0 && h.indexOf('data-go="c:') < 0) h += `<div class="eco-nada">Sin resultados para «${esc(S.q)}».</div>`;
    return h;
  }
  function arbol() {
    const mo = modo(S.sel), t = $("#ecoTree"), sc = t.scrollTop;
    $$(".eco-seg button").forEach(b => b.classList.toggle("on", b.dataset.mode === mo)); $("#ecoQ").placeholder = mo === "mon" ? "Buscar estado o sector…" : "Buscar indicador, tema o ID…";
    t.innerHTML = mo === "mon" ? arbolMon() : arbolCat(); t.scrollTop = sc;
  }

  function shell() {
    host.innerHTML = `<div class="eco-app${EMB ? " eco-emb" : ""}" id="ecoApp">
      <header class="eco-top"><button class="eco-burger" id="ecoBurger" type="button" aria-label="Menú">☰</button>
        <div class="eco-logo"><img src="econolab-logo.png" alt="EconoLab"></div>
        <div class="eco-brand"><span>Laboratorio económico · monitoreo de variables coyunturales</span></div><div class="eco-sp"></div>
        <span class="eco-chip" title="Datos descargados de la API del INEGI">Datos: ${esc((D.generado || "").slice(0, 10))}</span>
        ${EMB ? '<button class="gl-back" id="glBack" type="button" title="Volver al menú de GeoLab">← Menú</button><img class="mt-logo" src="metrika-logo.png" alt="Metrika Business Intelligence">' : ""}<button class="eco-theme" id="ecoTema" type="button" aria-label="Cambiar tema"></button><button class="eco-btn" id="ecoCambia" type="button">Cambiar de sistema</button><button class="eco-btn" id="ecoSalir" type="button">Salir</button></header>
      <div class="eco-body"><aside class="eco-side">${glTabs("eco")}<div class="eco-seg"><button type="button" data-mode="mon">📊 Monitores</button><button type="button" data-mode="cat">🗂️ Catálogo</button></div><div class="eco-srch"><input id="ecoQ" type="search" placeholder="Buscar indicador, tema o ID…" autocomplete="off"></div><div class="eco-tree" id="ecoTree"></div><div class="eco-sf">Fuente: INEGI · Banco de Indicadores</div></aside>
      ${EMB ? '<div class="eco-wbar"><button type="button" class="eco-wb" id="ecoWB" style="display:none" aria-label="Regresar a la vista anterior" title="Regresar a la vista anterior">←</button><b id="ecoWT">EconoLab</b><button type="button" class="eco-wq" id="ecoWQ" style="display:none" aria-label="¿De dónde sale cada dato de esta ficha?" title="¿De dónde sale cada dato de esta ficha?">?</button><button type="button" class="eco-wx" id="ecoWX" aria-label="Cerrar ventana" title="Cerrar">✕</button></div>' : ""}<div class="eco-fp" id="ecoFp" hidden></div><main class="eco-main" id="ecoMain2"></main></div><div class="eco-ov" id="ecoOv"></div></div>`;
    tipEl = document.createElement("div"); tipEl.className = "eco-tip"; document.body.appendChild(tipEl);
    $("#ecoBurger", host).onclick = () => $("#ecoApp").classList.toggle("eco-menu"); $("#ecoOv", host).onclick = () => $("#ecoApp").classList.remove("eco-menu");
    $("#ecoSalir", host).onclick = () => window.DL_SALIR && window.DL_SALIR();
    if (EMB) { $("#ecoWB", host).onclick = () => { const pv = S.hist.pop(); if (pv) { S._noPush = true; try { ir(pv); } finally { S._noPush = false; } } };
      $("#ecoWQ", host).onclick = e => { e.stopPropagation(); const p = $("#ecoFp"); ecoFp(p && p.hidden); };
      document.addEventListener("click", e => { const p = $("#ecoFp"); if (p && !p.hidden && (e.target.closest("#ecoFp .eco-fpx") || !e.target.closest("#ecoFp, #ecoWQ"))) ecoFp(false); });
      $("#ecoWX", host).onclick = () => ecoWin(false); document.addEventListener("keydown", ev => { if (ev.key === "Escape") { const p = $("#ecoFp"); if (p && !p.hidden) ecoFp(false); else ecoWin(false); } }); } $("#ecoCambia", host).onclick = () => window.DL_CAMBIAR && window.DL_CAMBIAR();
    const tema = c => { $("#ecoApp").classList.toggle("eco-light", c === "claro"); $("#ecoTema").textContent = c === "claro" ? "🌙 Oscuro" : "☀️ Claro"; $("#ecoTema").title = c === "claro" ? "Cambiar a tema oscuro (aplica a GeoLab, EconoLab y DataLab)" : "Cambiar a tema claro (aplica a GeoLab, EconoLab y DataLab)"; if (!EMB) ls.set("eco_tema", c); };
    tema(EMB ? "oscuro" : ls.get("eco_tema", "oscuro"));
    $("#ecoTema", host).onclick = () => { const nx = $("#ecoApp").classList.contains("eco-light") ? "oscuro" : "claro"; if (EMB) { try { parent.postMessage({ gl: "tema", v: nx === "oscuro" ? "osc" : "claro" }, location.origin); } catch (_) { } } else { tema(nx); rer(); } };
    if (EMB) addEventListener("message", e => { if (e.origin !== location.origin || !e.data || e.data.gl !== "tema") return; const c = e.data.v === "osc" ? "oscuro" : "claro"; if (($("#ecoApp").classList.contains("eco-light") ? "claro" : "oscuro") !== c) { tema(c); if (S.sel) rer(); } });
    $("#ecoQ", host).oninput = e => { S.q = e.target.value; arbol(); };
    host.onclick = e => { if (glIr(e)) return; const ck = e.target.closest(".eco-ck"); if (ck) { const k = ck.dataset.k, st = S.serie, vn = st.vn !== false, va = st.va !== false;
        if (k === "nivel" || k === "anual" || k === "mom") { const cur = { nivel: vn, anual: va, mom: st.vm === true }, nx = Object.assign({}, cur, { [k]: !cur[k] }); if (nx.nivel || nx.anual || nx.mom) { st.vn = nx.nivel; st.va = nx.anual; st.vm = nx.mom; } } else if (k === "fc") st.fc = st.fc === false; else if (k === "og") st.og = st.og === false;
        $$("#ecoMode .eco-ck").forEach(b => b.querySelector(".eco-ckb").classList.toggle("on", ckOn(b.dataset.k))); if (S.curI) drawSerie($("#ecoMain"), S.curI); return; }
      const fb = e.target.closest("#ecoFc"); if (fb) { S.serie.fc = S.serie.fc === false; fb.classList.toggle("on", S.serie.fc !== false); if (S.curI) drawSerie($("#ecoMain"), S.curI); return; } const sb = e.target.closest("[data-sub]"); if (sb) { S.sub = +sb.dataset.sub; return rer(); } const sk = e.target.closest("[data-sk]"); if (sk) { S.sk = sk.dataset.sk; return rer(); } const sm = e.target.closest("[data-mode]"); if (sm) { ir(sm.dataset.mode === "mon" ? (S.lastMon || "nac") : (S.lastCat || "cat")); ecoWin(true); return; } const g = e.target.closest("[data-go]"), t = e.target.closest("[data-tg]"); if (t) { const k = t.dataset.tg; S.open.has(k) ? S.open.delete(k) : S.open.add(k); arbol(); } else if (g) { ir(g.dataset.go); ecoWin(true); } };
    host.onchange = e => { if (e.target.id === "ecoSm") { S.serie.suav = e.target.value; const gk = $("#ecoMode"); if (gk && S.curI) gk.outerHTML = serieChecks(S.curI); if (S.curI) drawSerie($("#ecoMain"), S.curI); } };
    let tm; addEventListener("resize", () => { clearTimeout(tm); tm = setTimeout(() => draws.forEach(f => f()), 120); });
  }

  /* ───────── contexto del proyecto: lugar del pin + giro de interés (los manda el encabezado de GeoLab) ───────── */
  const ctxF = () => S.ctx && S.ctxOn !== false && (S.ctx.ent || S.ctx.giro) ? S.ctx : null;
  /* vista de origen del filtro (giro, ciudad o estado del proyecto) y aviso cuando se está fuera de ella */
  function casaCtx() { const c = S.ctx; if (!c || S.ctxOn === false) return null; if (c.giro) return { id: "s:" + c.giro, n: SECBY[c.giro].ic + " " + SECBY[c.giro].n }; if (c.city) return { id: "c:" + c.city, n: CBY[c.city][1] }; if (c.ent) return { id: "e:" + c.ent, n: ENTBY[c.ent][1] }; return null; }
  function fueraBanner(id) {
    const c = S.ctx, h = casaCtx(); if (!h || !c) return "";
    const ok = id === "nac" || id.startsWith("i:") || id === "sec" || id === "reg" || id === "ciu" || id.startsWith("z:") || id.startsWith("g:") || id.startsWith("a:") || id === "cat" || id === h.id || (c.giro && id === "s:" + c.giro) || (c.city && id === "c:" + c.city) || (c.ent && id === "e:" + c.ent) || (id.startsWith("s:") && !c.giro);
    if (ok) return ""; const q = c.city ? CBY[c.city][1] + ", " + ENTBY[c.ent][1] : c.ent ? ENTBY[c.ent][1] : "", g = c.giro ? SECBY[c.giro].ic + " " + SECBY[c.giro].n : "";
    return `<div class="eco-fuera"><span>⚠️ Estás viendo una vista <b>fuera de tu filtro</b> (${esc([q, g].filter(Boolean).join(" · "))}).</span><button type="button" data-go="${h.id}">↩ Volver a ${esc(h.n)}</button>${c.city ? `<button type="button" data-go="c:${c.city}">📍 ${esc(CBY[c.city][1])}</button>` : ""}${c.ent ? `<button type="button" data-go="e:${c.ent}">📍 ${esc(ENTBY[c.ent][1])}</button>` : ""}</div>`;
  }
  function ctxBar() {
    const c = S.ctx; if (!c || !(c.ent || c.giro)) return "";
    const lug = c.ent ? (c.city ? CBY[c.city][1] + ", " : "") + ENTBY[c.ent][1] : "", gi = c.giro && SECBY[c.giro] ? SECBY[c.giro].ic + " " + SECBY[c.giro].n : "", on = S.ctxOn !== false;
    return `<div class="eco-ctx${on ? " on" : ""}"><div class="eco-ctx-t">${on ? "Filtrado por" : "Filtro desactivado"}</div>${lug ? `<div class="eco-ctx-l">📍 ${c.city ? `<button type="button" class="eco-ctx-go" data-go="c:${c.city}" title="Ver la ciudad">${esc(CBY[c.city][1])} ›</button>, ` : ""}<button type="button" class="eco-ctx-go" data-go="e:${c.ent}" title="Ver el estado">${esc(ENTBY[c.ent][1])} ›</button></div>` : ""}${gi ? `<div class="eco-ctx-l"><button type="button" class="eco-ctx-go" data-go="s:${c.giro}" title="Ver el sector">${esc(gi)} ›</button></div>` : ""}<button type="button" class="eco-ctx-b" data-ctx="tog">${on ? "Ver todo" : "Aplicar filtro"}</button></div>`;
  }
  function resuelveCtx(m) {
    const gname = norm(m.estado || ""), ent = m.ent && ENTBY[m.ent] ? ENTBY[m.ent] : gname ? ENT.filter(e => gname.includes(norm(e[1]))).sort((a, b) => b[1].length - a[1].length)[0] : null;
    let city = null; const names = [m.ciudad, m.municipio].filter(Boolean).map(norm);
    if (isFinite(m.lat) && isFinite(m.lng)) { let best = 1e9; CIU.forEach(c => { const xy = CIU_XY[c[0]]; if (!xy) return; const dy = (xy[0] - m.lat) * 111, dx = (xy[1] - m.lng) * 111 * Math.cos(m.lat * Math.PI / 180), d = Math.hypot(dx, dy); if (d < best && d < 45) { best = d; city = c[0]; } }); }
    if (!city) { const c = CIU.find(c => names.some(n => n && (norm(c[1]).includes(n) || n.includes(norm(c[1]))))); if (c) city = c[0]; }
    let entCode = ent ? ent[0] : (city ? CBY[city][2] : null); if (city && entCode && CBY[city][2] !== entCode) city = null;
    return { ent: entCode, city, giro: SECBY[m.giro] ? m.giro : "" };
  }
  function aplicaCtx(m) {
    const n = resuelveCtx(m), o = S.ctx || {}, lugCambia = n.ent !== o.ent || n.city !== o.city, giroCambia = n.giro !== o.giro;
    S.ctx = n; if (m.go && /^(i:|reg$|ciu$|nac$)/.test(m.go)) { setTimeout(() => { ir(m.go); ecoWin(true); }, 60); } if (!lugCambia && !giroCambia) return; if (n.ent) { S.open.add("R"); S.open.add("z:" + ENTBY[n.ent][5]); S.open.add("CI"); } if (S.ctxOn === false && (n.ent || n.giro)) S.ctxOn = true;
    if (giroCambia && n.giro && !lugCambia) ir("s:" + n.giro);
    else if (lugCambia && n.city) ir("c:" + n.city); else if (lugCambia && n.ent) ir("e:" + n.ent); else arbol();
  }
  window.ecolabInit = function () {
    host = document.getElementById("ecolab-host"); carga(); cargaMon(); cargaCiu(); S.metric = "itaee_yoy"; ["SC", "t:1", "t:2", "t:3"].forEach(k => S.open.add(k)); shell();
    let s = ls.get("eco_sel", "nac"); if (s.startsWith("i:") && !BYID[s.slice(2)]) s = "nac"; ir(s);
    if (EMB) { addEventListener("message", e => { if (e.origin === location.origin && e.data && e.data.gl === "ctx") aplicaCtx(e.data); }); try { parent.postMessage({ gl: "ctx?" }, location.origin); } catch (_) { } }
    host.addEventListener("click", e => { const b = e.target.closest("[data-ctx]"); if (b) { S.ctxOn = S.ctxOn === false; arbol(); } });
  };
})();
