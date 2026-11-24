/* Model Lab · Valuación inmobiliaria por precio de mercado.
   Detecta propiedades publicadas por debajo o por encima del precio que le corresponde a sus características y a su ubicación.
   1) Limpieza exhaustiva de las publicaciones (incompletas, precios espurios, duplicados, ubicaciones dudosas, atípicos).
   2) Modelo hedónico: regresión Ridge del logaritmo del precio con todos los atributos de la propiedad y una tendencia espacial; predicciones con validación cruzada.
   3) Geoestadística: variograma de los residuales (ajuste esférico/exponencial) y kriging ordinario (regresión-kriging) para capturar la dependencia espacial del precio.
   4) Clasificación de cada propiedad según su desviación robusta (z) frente al precio esperado.
   Corre en el navegador; expone window.MLVal. */
(function () {
  "use strict";
  const nz = v => v != null && isFinite(v);
  const tick = () => new Promise(r => setTimeout(r, 0));
  const sum = a => { let s = 0; for (let i = 0; i < a.length; i++) s += a[i]; return s; };
  const mean = a => a.length ? sum(a) / a.length : NaN;
  const sorted = a => a.slice().sort((x, y) => x - y);
  const quant = (s, q) => { if (!s.length) return NaN; const p = (s.length - 1) * q, i = Math.floor(p), f = p - i; return s[i] + (s[Math.min(i + 1, s.length - 1)] - s[i]) * f; };
  const med = a => quant(sorted(a), .5);
  const mad = (a, m) => { const mm = m == null ? med(a) : m; return med(a.map(x => Math.abs(x - mm))); };
  const rng = seed => { let t = (seed || 42) >>> 0; return () => { t += 0x6D2B79F5; let r = Math.imul(t ^ (t >>> 15), 1 | t); r ^= r + Math.imul(r ^ (r >>> 7), 61 | r); return ((r ^ (r >>> 14)) >>> 0) / 4294967296; }; };
  const norm = s => String(s == null ? "" : s).normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/²/g, "2").replace(/³/g, "3").toLowerCase();
  const numV = v => { if (v == null || v === "") return null; if (typeof v === "number") return isFinite(v) ? v : null; let t = String(v).trim().replace(/[$%\s]/g, ""); if (/^-?\d+,\d+$/.test(t)) t = t.replace(",", "."); else t = t.replace(/,/g, ""); const n = Number(t); return t !== "" && isFinite(n) ? n : null; };

  /* ───────── localización de columnas por nombre ───────── */
  function mapeaCols(ds) {
    const C = ds.cols, f = rx => C.findIndex(c => rx.test(norm(c.label || c.name)) || rx.test(norm(c.name)));
    const M = { precio: f(/^precio|^price|valor comercial/), op: f(/^operacion/), tipo: f(/^tipo$|^tipo de /), ciudad: f(/^ciudad$/), titulo: f(/^titulo/), url: f(/^url$|^enlace/), ids: -1,
      rec: f(/recamaras|habitaciones/), ban: f(/^banos$|^banos /), est: f(/estacionamiento/), huesp: f(/huespedes/), camas: f(/^camas/), prec: f(/precision de la ubicacion/), tubi: f(/tipo de ubicacion/), colonia: f(/^colonia|barrio/) };
    M.areas = ["m2 construccion", "m2 totales", "m2 terreno", "m2 total", "superficie", "area"].map(k => C.findIndex(c => norm(c.label || c.name).includes(k))).filter((i, k, a) => i >= 0 && a.indexOf(i) === k);
    return M;
  }
  const esPrecioFuga = (c, y) => c.name !== y && /(^|\b)(precio|price|renta|noche|total pagado|costo)(\b|$)/.test(norm(c.label || c.name));

  /* ───────── limpieza exhaustiva ───────── */
  const LIM = { venta: [150000, 150000000], renta: [1500, 400000], noche: [150, 25000] };
  const xyDe = (lat, lng, lat0, lng0) => [(lng - lng0) * 111320 * Math.cos(lat0 * Math.PI / 180), (lat - lat0) * 110574];

  function limpia(ds, cfg) {
    const n0 = ds.rows.length, C = mapeaCols(ds), iy = ds.cols.findIndex(c => c.name === cfg.y); if (iy < 0) throw new Error("No se encontró la columna de precio en la tabla de datos.");
    const pasos = [], excl = []; let vivos = ds.rows.map((_, i) => i); const quita = (id, titulo, detalle, pred) => { const k = []; const nuevo = []; vivos.forEach(i => { const m = pred(i); if (m) { k.push(i); excl.push({ i, id: ds.ids[i], motivo: titulo + (typeof m === "string" ? ": " + m : "") }); } else nuevo.push(i); }); pasos.push({ id, titulo, detalle, quitadas: k.length, quedan: nuevo.length }); vivos = nuevo; };
    const precio = i => numV(ds.rows[i][iy]), geo = i => [ds.lat[i], ds.lng[i]];
    quita("precio", "Sin precio válido", "Publicaciones sin precio, con precio cero o no numérico.", i => { const p = precio(i); return !(nz(p) && p > 0); });
    quita("coord", "Sin coordenadas válidas", "Latitud o longitud vacías, en (0, 0) o fuera de México.", i => { const [la, ln] = geo(i); return !(nz(la) && nz(ln) && la > 14 && la < 33.5 && ln > -118.5 && ln < -86 && !(Math.abs(la) < 1e-6 && Math.abs(ln) < 1e-6)); });
    /* operación analizada: una sola a la vez (no se mezclan ventas con rentas ni con noches) */
    let op = cfg.op || null; const ops = new Map(); if (C.op >= 0) vivos.forEach(i => { const o = norm(ds.rows[i][C.op]); if (o) ops.set(o, (ops.get(o) || 0) + 1); });
    if (C.op >= 0) { if (!op || !ops.has(op)) op = [...ops.entries()].sort((a, b) => b[1] - a[1])[0] ? [...ops.entries()].sort((a, b) => b[1] - a[1])[0][0] : null; if (op) quita("op", "Otra operación", `Se analiza solo «${op}»: no se mezclan operaciones distintas.`, i => norm(ds.rows[i][C.op]) !== op); }
    const unidad = op === "noche" ? "por noche" : op === "renta" ? "mensual" : op === "venta" ? "total" : "";
    const lim = LIM[op] || null;
    if (lim) quita("rango", "Precio fuera de lo plausible", `Fuera del rango razonable para ${op === "venta" ? "una venta" : op === "renta" ? "una renta mensual" : "una noche"} ($${lim[0].toLocaleString("es-MX")} a $${lim[1].toLocaleString("es-MX")}); suelen ser capturas erróneas, precios simbólicos o montos de otra operación.`, i => { const p = precio(i); return p < lim[0] || p > lim[1] ? "$" + Math.round(p).toLocaleString("es-MX") : false; });
    if (C.prec >= 0) quita("prec", "Ubicación poco precisa", "Coordenadas con precisión de geocodificación muy baja (centro de la ciudad o del municipio): distorsionan el análisis espacial.", i => { const v = numV(ds.rows[i][C.prec]); return nz(v) && v <= 2; });
    /* valores imposibles dentro de una publicación: se anulan (la publicación se conserva si el resto es útil) */
    let corregidos = 0; const areaDe = i => { for (const j of C.areas) { const v = numV(ds.rows[i][j]); if (nz(v) && v >= 12 && v <= (op === "venta" || !op ? 60000 : 20000)) return v; } return null; };
    const Rows = ds.rows.map(r => r.slice());
    vivos.forEach(i => { C.areas.forEach(j => { const v = numV(Rows[i][j]); if (v != null && (v < 12 || v > 100000)) { Rows[i][j] = null; corregidos++; } });
      [[C.rec, 15], [C.ban, 12], [C.est, 20], [C.huesp, 30], [C.camas, 30]].forEach(([j, mx]) => { if (j < 0) return; const v = numV(Rows[i][j]); if (v != null && (v < 0 || v > mx)) { Rows[i][j] = null; corregidos++; } }); });
    pasos.push({ id: "valores", titulo: "Valores imposibles anulados", detalle: "Superficies menores a 12 m² o absurdas, y recámaras, baños, estacionamientos o camas fuera de lo posible: el valor se anula y la publicación se conserva.", quitadas: 0, quedan: vivos.length, corregidos });
    /* duplicados */
    const visto = new Set(); quita("dup", "Publicaciones duplicadas", "Misma ubicación, mismo precio y mismas características, o mismo título y precio.", i => { const a = areaDe(i), k1 = [ds.lat[i].toFixed(4), ds.lng[i].toFixed(4), Math.round(precio(i)), a ? Math.round(a) : "", C.rec >= 0 ? numV(Rows[i][C.rec]) : ""].join("|"), k2 = C.titulo >= 0 && ds.rows[i][C.titulo] ? norm(ds.rows[i][C.titulo]).slice(0, 80) + "|" + Math.round(precio(i)) : null; if (visto.has(k1) || (k2 && visto.has(k2))) return true; visto.add(k1); if (k2) visto.add(k2); return false; });
    /* registros incompletos: al menos 2 de los atributos clave */
    const claves = [C.areas.length ? -2 : -1, C.rec, C.ban, C.huesp, C.est, C.tipo].filter(j => j !== -1);
    if (claves.length >= 2) quita("incompl", "Registros incompletos", "Publicaciones que no describen la propiedad: les faltan casi todos los atributos clave (superficie, recámaras, baños, tipo…).", i => { let k = 0; claves.forEach(j => { if (j === -2) { if (areaDe(i) != null) k++; } else { const v = ds.rows[i][j]; if (v != null && v !== "" && !(typeof v === "number" && !isFinite(v))) k++; } }); return k < 2 ? `${k} de ${claves.length} atributos` : false; });
    /* fuera de la zona de mercado (mal geocodificadas o de otra ciudad) */
    if (vivos.length > 30) { const la0 = med(vivos.map(i => ds.lat[i])), ln0 = med(vivos.map(i => ds.lng[i])), D = vivos.map(i => { const [x, y] = xyDe(ds.lat[i], ds.lng[i], la0, ln0); return Math.hypot(x, y); }), md = med(D), ma = mad(D, md) * 1.4826, tope = Math.max(30000, md + 5 * ma);
      let k = 0; quita("zona", "Fuera de la zona de mercado", `Más de ${Math.round(tope / 1000)} km del centro del mercado: publicaciones de otra ciudad o mal ubicadas.`, i => { const [x, y] = xyDe(ds.lat[i], ds.lng[i], la0, ln0); return Math.hypot(x, y) > tope ? Math.round(Math.hypot(x, y) / 1000) + " km del centro" : false; }); }
    /* atípicos de precio (por tipo de inmueble): z robusto del log(precio por m²) o del log(precio) */
    const gruposOut = (it) => { const key = i => C.tipo >= 0 ? norm(ds.rows[i][C.tipo]) || "otro" : "todos", val = i => { const a = areaDe(i), p = precio(i); return a ? Math.log(p / a) : Math.log(p); }; const G = new Map(); vivos.forEach(i => { const k = key(i); (G.get(k) || G.set(k, []).get(k)).push(val(i)); });
      const gl = vivos.map(val), mG = med(gl), sG = mad(gl, mG) * 1.4826 || 1, est = new Map(); G.forEach((v, k) => { if (v.length >= 10) { const m = med(v), s = mad(v, m) * 1.4826; est.set(k, [m, s > .05 ? s : sG]); } });
      quita("out" + it, it === 1 ? "Precios atípicos" : "Precios atípicos (2.ª pasada)", "Precio (o precio por m²) a más de 3.5 desviaciones robustas de lo normal para su tipo de inmueble.", i => { const e = est.get(key(i)) || [mG, sG], z = (val(i) - e[0]) / e[1]; return Math.abs(z) > 3.5 ? "z = " + z.toFixed(1) : false; }); };
    if (vivos.length > 30) { gruposOut(1); if (vivos.length > 30) gruposOut(2); }
    return { vivos, pasos, excl, op, unidad, Rows, areaDe, C, iy, n0, corregidos };
  }

  /* ───────── álgebra mínima ───────── */
  function resuelve(A, b) {   /* eliminación gaussiana con pivoteo parcial; A es n×n (se modifica) */
    const n = b.length; for (let c = 0; c < n; c++) { let p = c; for (let r = c + 1; r < n; r++) if (Math.abs(A[r][c]) > Math.abs(A[p][c])) p = r; if (Math.abs(A[p][c]) < 1e-12) return null; if (p !== c) { const t = A[p]; A[p] = A[c]; A[c] = t; const tb = b[p]; b[p] = b[c]; b[c] = tb; }
      for (let r = c + 1; r < n; r++) { const f = A[r][c] / A[c][c]; if (!f) continue; for (let k = c; k < n; k++) A[r][k] -= f * A[c][k]; b[r] -= f * b[c]; } }
    const x = new Array(n).fill(0); for (let r = n - 1; r >= 0; r--) { let s = b[r]; for (let k = r + 1; k < n; k++) s -= A[r][k] * x[k]; x[r] = s / A[r][r]; } return x;
  }
  function ridge(X, y, lam) {   /* X ya estandarizada (sin intercepto); y centrada dentro */
    const n = X.length, p = X[0].length, my = mean(y), A = Array.from({ length: p }, () => new Array(p).fill(0)), b = new Array(p).fill(0);
    for (let i = 0; i < n; i++) { const xi = X[i], yi = y[i] - my; for (let j = 0; j < p; j++) { const v = xi[j]; if (!v) continue; b[j] += v * yi; for (let k = j; k < p; k++) A[j][k] += v * xi[k]; } }
    for (let j = 0; j < p; j++) { for (let k = 0; k < j; k++) A[j][k] = A[k][j]; A[j][j] += lam * n; }
    const beta = resuelve(A, b) || new Array(p).fill(0); return { beta, b0: my };
  }
  const predice = (m, x) => { let s = m.b0; for (let j = 0; j < x.length; j++) s += m.beta[j] * x[j]; return s; };

  /* ───────── matriz de atributos ───────── */
  function armaX(ds, L, cfg) {
    const C = L.C, idx = L.vivos, n = idx.length, nombres = [], cols = [], info = [];
    const xs = new Set(cfg.x || ds.cols.map(c => c.name)), usa = (cfg.atributos || "todos") === "tamano" ? (c, j) => j === C.rec || j === C.ban || j === C.huesp || C.areas.includes(j) : () => true;
    ds.cols.forEach((c, j) => {
      if (j === L.iy || !xs.has(c.name) || esPrecioFuga(c, cfg.y) || !usa(c, j)) return; const lab = c.label || c.name, nl = norm(lab);
      if (j === C.titulo || j === C.url || j === C.prec || j === C.tubi || /^id$|^url|enlace|publicad|ciudad|estado|colonia|calle|direccion|^codigo postal/.test(nl)) return;
      const v = idx.map(i => L.Rows[i][j]), nv = v.map(numV), ok = nv.filter(nz).length, lleno = v.filter(x => x !== null && x !== undefined && x !== "").length;
      if (lleno < n * .05) return;
      if (ok >= lleno * .9 && ok) {   /* numérica */
        let a = nv.slice(); if (C.areas.includes(j)) { a = a.map(x => nz(x) && x > 0 ? Math.log(x) : null); }
        const fin = a.filter(nz), mdn = med(fin), sd = Math.sqrt(mean(fin.map(x => (x - mean(fin)) ** 2))) || 1; if (sd < 1e-9) return;
        const skew = (() => { const m = mean(fin); return mean(fin.map(x => ((x - m) / sd) ** 3)); })(); const lg = !C.areas.includes(j) && fin.every(x => x >= 0) && skew > 2 && Math.max(...fin) > 20;
        if (lg) a = a.map(x => nz(x) ? Math.log1p(x) : null); const fin2 = a.filter(nz), m2 = mean(fin2), s2 = Math.sqrt(mean(fin2.map(x => (x - m2) ** 2))) || 1, faltan = a.filter(x => !nz(x)).length;
        cols.push(a.map(x => nz(x) ? (x - m2) / s2 : 0)); nombres.push(lab + (C.areas.includes(j) ? " (ln)" : lg ? " (ln)" : "")); info.push({ tipo: "num", col: lab });
        if (faltan > n * .03 && faltan < n * .97) { cols.push(a.map(x => nz(x) ? 0 : 1)); nombres.push(lab + " · falta el dato"); info.push({ tipo: "falta", col: lab }); }
      } else {   /* categórica */
        const cnt = new Map(); v.forEach(x => { const s = x == null || x === "" ? "sin dato" : String(x).slice(0, 40); cnt.set(s, (cnt.get(s) || 0) + 1); }); const niv = [...cnt.entries()].sort((a, b) => b[1] - a[1]); if (niv.length < 2 || niv.length > 60) return;
        const top = niv.slice(0, 8).map(e => e[0]), ref = top[0], otros = niv.length > 8; top.slice(1).forEach(t => { cols.push(v.map(x => (x == null || x === "" ? "sin dato" : String(x).slice(0, 40)) === t ? 1 : 0)); nombres.push(lab + " = " + t); info.push({ tipo: "cat", col: lab, nivel: t, ref }); });
        if (otros) { cols.push(v.map(x => { const s = x == null || x === "" ? "sin dato" : String(x).slice(0, 40); return top.includes(s) ? 0 : 1; })); nombres.push(lab + " = otros"); info.push({ tipo: "cat", col: lab, nivel: "otros", ref }); }
      }
    });
    /* tendencia espacial (polinomio de 2.º grado en las coordenadas) */
    const la0 = med(idx.map(i => ds.lat[i])), ln0 = med(idx.map(i => ds.lng[i])), XY = idx.map(i => xyDe(ds.lat[i], ds.lng[i], la0, ln0)), sc = Math.max(1, Math.sqrt(mean(XY.map(p => p[0] * p[0] + p[1] * p[1]))));
    const poly = [[p => p[0] / sc, "Ubicación este-oeste"], [p => p[1] / sc, "Ubicación norte-sur"], [p => (p[0] / sc) ** 2, "Ubicación este-oeste (curva)"], [p => (p[1] / sc) ** 2, "Ubicación norte-sur (curva)"], [p => (p[0] / sc) * (p[1] / sc), "Ubicación diagonal"]];
    if (cfg.tendencia !== false) poly.forEach(([f, nm]) => { const a = XY.map(f), m = mean(a), s = Math.sqrt(mean(a.map(x => (x - m) ** 2))) || 1; cols.push(a.map(x => (x - m) / s)); nombres.push(nm); info.push({ tipo: "geo", col: nm }); });
    const X = Array.from({ length: n }, (_, i) => cols.map(c => c[i])); return { X, nombres, info, XY, la0, ln0 };
  }

  /* validación cruzada: Ridge con λ elegido por validación cruzada interna */
  function cvRidge(X, y, K, semilla) {
    const n = X.length, r = rng(semilla), orden = [...Array(n).keys()].sort(() => r() - .5), folds = Array.from({ length: K }, (_, k) => orden.filter((_, i) => i % K === k)), lams = [.002, .01, .05, .2, 1];
    const oof = L => { const o = new Array(n).fill(0); folds.forEach(f => { const te = new Set(f), tr = orden.filter(i => !te.has(i)); const m = ridge(tr.map(i => X[i]), tr.map(i => y[i]), L); f.forEach(i => { o[i] = predice(m, X[i]); }); }); return o; };
    let mejor = null; lams.forEach(L => { const o = oof(L), rm = Math.sqrt(mean(o.map((p, i) => (p - y[i]) ** 2))); if (!mejor || rm < mejor.rm) mejor = { L, o, rm }; });
    const full = ridge(X, y, mejor.L); return { lam: mejor.L, oof: mejor.o, modelo: full };
  }

  /* ───────── variograma y kriging ───────── */
  const esf = (h, c0, c, a) => h <= 0 ? 0 : h >= a ? c0 + c : c0 + c * (1.5 * h / a - .5 * (h / a) ** 3);
  const exp_ = (h, c0, c, a) => h <= 0 ? 0 : c0 + c * (1 - Math.exp(-3 * h / a));
  const MODV = { esferico: esf, exponencial: exp_ };
  function variograma(XY, e, semilla) {
    const n = e.length, r = rng(semilla), pares = []; const total = n * (n - 1) / 2; let maxD = 0;
    const agrega = (i, j) => { const d = Math.hypot(XY[i][0] - XY[j][0], XY[i][1] - XY[j][1]); if (d > 1) { pares.push([d, .5 * (e[i] - e[j]) ** 2]); if (d > maxD) maxD = d; } };
    if (total <= 1.2e6) { for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) agrega(i, j); } else { for (let k = 0; k < 1.2e6; k++) { const i = Math.floor(r() * n); let j = Math.floor(r() * n); if (j === i) j = (j + 1) % n; agrega(i, j); } }
    const ds_ = sorted(pares.map(p => p[0])), lag = Math.max(1500, Math.min(25000, quant(ds_, .45))), nb = 14, w = lag / nb, S = Array.from({ length: nb }, () => ({ s: 0, n: 0, h: 0 }));
    pares.forEach(([d, g]) => { if (d >= lag) return; const b = Math.min(nb - 1, Math.floor(d / w)); S[b].s += g; S[b].n++; S[b].h += d; });
    const bins = S.filter(b => b.n >= 30).map(b => ({ h: b.h / b.n, g: b.s / b.n, n: b.n }));
    if (bins.length < 4) return { bins, ajuste: null, lag, pares: pares.length };
    const gmax = Math.max(...bins.map(b => b.g)), meseta0 = med(bins.slice(-4).map(b => b.g)); let mejor = null;
    Object.keys(MODV).forEach(tipo => { const f = MODV[tipo]; for (let ia = 1; ia <= 24; ia++) { const a = lag * (.12 + ia * .075); for (let ic0 = 0; ic0 <= 10; ic0++) { const c0 = gmax * ic0 / 12; for (let ic = 1; ic <= 12; ic++) { const c = gmax * ic / 8; let sse = 0; bins.forEach(b => { const m = f(b.h, c0, c, a); sse += b.n * ((b.g - m) / Math.max(m, 1e-6)) ** 2; }); if (!mejor || sse < mejor.sse) mejor = { tipo, c0, c, a, sse }; } } } });
    const sill = mejor.c0 + mejor.c; mejor.meseta = sill; mejor.dependencia = sill > 0 ? mejor.c / sill : 0; mejor.rango = mejor.tipo === "exponencial" ? mejor.a : mejor.a; return { bins, ajuste: mejor, lag, pares: pares.length, meseta0 };
  }
  function krigingLOO(XY, e, aj) {
    const n = e.length, sill = aj.c0 + aj.c, f = MODV[aj.tipo], C = h => h <= 0 ? sill : sill - f(h, aj.c0, aj.c, aj.a), rad = aj.a * 1.5, pred = new Array(n).fill(0), sd = new Array(n).fill(Math.sqrt(sill)), kn = new Array(n).fill(0);
    const celda = Math.max(500, rad / 2), G = new Map(); XY.forEach((p, i) => { const k = Math.floor(p[0] / celda) + "," + Math.floor(p[1] / celda); (G.get(k) || G.set(k, []).get(k)).push(i); });
    for (let i = 0; i < n; i++) {
      const cx = Math.floor(XY[i][0] / celda), cy = Math.floor(XY[i][1] / celda), nbh = [], R = Math.ceil(rad / celda);
      for (let dx = -R; dx <= R; dx++) for (let dy = -R; dy <= R; dy++) { const L = G.get((cx + dx) + "," + (cy + dy)); if (!L) continue; L.forEach(j => { if (j === i) return; const d = Math.hypot(XY[i][0] - XY[j][0], XY[i][1] - XY[j][1]); if (d <= rad) nbh.push([d, j]); }); }
      nbh.sort((a, b) => a[0] - b[0]); const m = Math.min(28, nbh.length); if (m < 5) continue; const V = nbh.slice(0, m), A = Array.from({ length: m + 1 }, () => new Array(m + 1).fill(0)), b = new Array(m + 1).fill(0);
      for (let a = 0; a < m; a++) { for (let c = a; c < m; c++) { const d = Math.hypot(XY[V[a][1]][0] - XY[V[c][1]][0], XY[V[a][1]][1] - XY[V[c][1]][1]), v = a === c ? sill : C(d); A[a][c] = v; A[c][a] = v; } A[a][m] = 1; A[m][a] = 1; b[a] = C(V[a][0]); } b[m] = 1;
      const w = resuelve(A, b.slice()); if (!w) continue; let p = 0, vv = sill; for (let a = 0; a < m; a++) { p += w[a] * e[V[a][1]]; vv -= w[a] * b[a]; } vv -= w[m]; pred[i] = p; sd[i] = Math.sqrt(Math.max(vv, 0)); kn[i] = m;
    }
    return { pred, sd, kn };
  }

  /* ───────── análisis completo ───────── */
  const NOMBRES = { val_rk: "Regresión-kriging (atributos + geoestadística)", val_hed: "Modelo hedónico (regresión Ridge del precio)", val_geo: "Geoestadístico (tamaño + variograma y kriging)" };
  async function correr(ds, cfg, onEtapa) {
    const ev = (id, p) => { if (onEtapa) onEtapa({ id, progreso: p || 0 }); }, metodo = cfg.metodo || "val_rk", t0 = Date.now();
    ev("limpieza", 0); await tick(); const L = limpia(ds, cfg), n = L.vivos.length; ev("limpieza", 1);
    if (n < 30) throw new Error(`Solo quedaron ${n} publicaciones útiles después de la limpieza; se necesitan al menos 30. Revisa la operación elegida o carga más publicaciones.`);
    const idx = L.vivos, y = idx.map(i => Math.log(numV(ds.rows[i][L.iy])));
    ev("hedonico", 0); await tick(); const cf = Object.assign({}, cfg, { atributos: metodo === "val_geo" ? "tamano" : "todos", tendencia: metodo !== "val_geo" }), A = armaX(ds, L, cf);
    if (!A.X[0] || !A.X[0].length) throw new Error("No hay atributos utilizables para modelar el precio.");
    const cv = cvRidge(A.X, y, 5, cfg.semilla || 42); ev("hedonico", 1); await tick();
    let e = y.map((v, i) => v - cv.oof[i]), lnPred = cv.oof.slice(), vario = null, krig = null;
    if (metodo !== "val_hed") {
      ev("variograma", 0); await tick(); vario = variograma(A.XY, e, cfg.semilla || 42); ev("variograma", 1); await tick();
      if (vario.ajuste) { ev("kriging", 0); await tick(); krig = krigingLOO(A.XY, e, vario.ajuste); lnPred = lnPred.map((p, i) => p + krig.pred[i]); ev("kriging", 1); await tick(); }
    }
    ev("clasificacion", 0); await tick();
    const r = y.map((v, i) => v - lnPred[i]), sig = mad(r, med(r)) * 1.4826 || Math.sqrt(mean(r.map(x => x * x))) || .1, mr = med(r);
    const rmse = Math.sqrt(mean(r.map(x => x * x))), vy = mean(y.map(v => (v - mean(y)) ** 2)), r2 = 1 - mean(r.map(x => x * x)) / vy, mape = mean(r.map(x => Math.abs(Math.exp(x) - 1))), mdape = med(r.map(x => Math.abs(Math.exp(x) - 1)));
    const MINDIF = cfg.minDif != null ? cfg.minDif : .10, cls = (z, d) => Math.abs(d) < MINDIF || Math.abs(z) < 1 ? "linea" : z <= -2 ? "muy_abajo" : z < 0 ? "abajo" : z >= 2 ? "muy_arriba" : "arriba";
    const C = L.C, filas = idx.map((i, k) => { const p = Math.exp(y[k]), esp = Math.exp(lnPred[k] + mr), dif = p / esp - 1, z = (r[k] - mr) / sig; const a = L.areaDe(i), ci = (j) => j >= 0 ? ds.rows[i][j] : null;
      return { i, id: ds.ids[i], titulo: ci(C.titulo) || "", url: ci(C.url) || "", tipo: ci(C.tipo) || "", ciudad: ci(C.ciudad) || "", colonia: ci(C.colonia) || "", area: a, rec: C.rec >= 0 ? numV(L.Rows[i][C.rec]) : null, ban: C.ban >= 0 ? numV(L.Rows[i][C.ban]) : null, precio: Math.round(p), esperado: Math.round(esp), bajo: Math.round(esp * Math.exp(-1.2816 * sig)), alto: Math.round(esp * Math.exp(1.2816 * sig)), dif: +dif.toFixed(4), z: +z.toFixed(2), clase: cls(z, dif), lat: ds.lat[i], lng: ds.lng[i], sk: krig ? +krig.sd[k].toFixed(3) : null }; });
    const cnt = { muy_abajo: 0, abajo: 0, linea: 0, arriba: 0, muy_arriba: 0 }; filas.forEach(f => { cnt[f.clase]++; });
    /* efecto de cada atributo: cambio % del precio por +1 desviación estándar (o por pertenecer a la categoría) */
    const coefs = A.nombres.map((nm, j) => ({ nombre: nm, tipo: A.info[j].tipo, beta: cv.modelo.beta[j], efecto: Math.exp(cv.modelo.beta[j]) - 1 })).sort((a, b) => Math.abs(b.beta) - Math.abs(a.beta));
    ev("clasificacion", 1);
    const aj = vario && vario.ajuste, kp = [
      { etiqueta: "Publicaciones analizadas", valor: n, formato: "n" }, { etiqueta: "Por debajo del mercado", valor: cnt.muy_abajo + cnt.abajo, formato: "n" }, { etiqueta: "Por encima del mercado", valor: cnt.muy_arriba + cnt.arriba, formato: "n" },
      { etiqueta: "R² (validación cruzada)", valor: +r2.toFixed(3), formato: "n" }, { etiqueta: "Error mediano del precio", valor: +mdape.toFixed(3), formato: "pct" }];
    return { enfoque: "val", tipoY: "regresion", metodo, nombreMetodo: NOMBRES[metodo], y: cfg.y, kpis: kp, creado: new Date().toISOString(), segundos: Math.round((Date.now() - t0) / 100) / 10,
      config: { op: L.op, unidad: L.unidad, minDif: MINDIF, metodo, nAtributos: A.nombres.length, lambda: cv.lam },
      limpieza: { total: L.n0, conserva: n, pasos: L.pasos, corregidos: L.corregidos, excluidas: L.excl.slice(0, 1500), nExcl: L.excl.length },
      modelo: { r2cv: r2, rmseLn: rmse, sigma: sig, mape, mdape, coefs: coefs.slice(0, 30), nCoef: coefs.length, atributos: A.nombres },
      variograma: vario ? { bins: vario.bins, ajuste: aj ? { tipo: aj.tipo, c0: aj.c0, c: aj.c, a: aj.a, meseta: aj.meseta, dependencia: aj.dependencia } : null, lag: vario.lag, pares: vario.pares } : null,
      kriging: krig ? { r2Mejora: null, vecinos: Math.round(mean(krig.kn)) } : null,
      resumen: { conteos: cnt, medianaDif: +med(filas.map(f => f.dif)).toFixed(4), difMin: Math.min(...filas.map(f => f.dif)), difMax: Math.max(...filas.map(f => f.dif)) }, filas };
  }

  /* ═══════════════ interfaz: método, resultados, mapa, tabla y exportación ═══════════════ */
  const CLS = { muy_abajo: ["Muy por debajo del mercado", "#047857", "Oportunidad"], abajo: ["Por debajo del mercado", "#34d399", "Posible oportunidad"], linea: ["En línea con el mercado", "#94a3b8", "Precio coherente"], arriba: ["Por encima del mercado", "#fb923c", "Posible sobreprecio"], muy_arriba: ["Muy por encima del mercado", "#dc2626", "Sobreprecio"] };
  const ORD = ["muy_abajo", "abajo", "linea", "arriba", "muy_arriba"];
  const METODOS = [
    { id: "val_rk", rec: true, nombre: "Regresión-kriging (recomendado)", tag: "Atributos + ubicación", desc: "Primero modela el precio con todos los atributos de la propiedad (superficie, recámaras, baños, tipo…) y una tendencia por zona; después estudia cómo se parecen entre sí los errores de propiedades cercanas (variograma) y los corrige con kriging.",
      v: ["Usa todas las características y también la vecindad", "Detecta zonas que cuestan más o menos de lo que explican los atributos", "Da el precio esperado y un rango"], l: ["Necesita al menos unas 60 publicaciones útiles", "Si faltan atributos, depende más de la ubicación"] },
    { id: "val_hed", nombre: "Modelo hedónico", tag: "Solo atributos", desc: "Regresión Ridge del logaritmo del precio con todas las características y una tendencia suave por zona. Cada atributo recibe un efecto en porcentaje.",
      v: ["Muy fácil de explicar: cuánto sube el precio por cada atributo", "Estable con pocas publicaciones"], l: ["No capta microzonas (una colonia que sube o baja el precio)", "Menos preciso si la ubicación pesa mucho"] },
    { id: "val_geo", nombre: "Geoestadístico (variograma + kriging)", tag: "Tamaño + vecindad", desc: "Toma solo el tamaño de la propiedad como característica y deja que la correlación espacial (variograma) explique el resto: el precio de las propiedades vecinas.",
      v: ["Muy sensible al mercado local", "Aprovecha bien datos con pocos atributos"], l: ["Ignora acabados, tipo y demás atributos", "Requiere publicaciones bien distribuidas en el mapa"] }
  ];
  const fm = v => nz(v) ? "$" + Math.round(v).toLocaleString("es-MX") : "—", fp = (v, d) => nz(v) ? (v > 0 ? "+" : "") + (v * 100).toFixed(d == null ? 0 : d) + " %" : "—";
  const E = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

  /* ── paso «Métodos» para el enfoque de valuación ── */
  function htmlMetodos(H) {
    const S = H.S, M = S.M, ds = S.ds, C = mapeaCols(ds), iy = ds.cols.findIndex(c => c.name === M.y); if (!M.val) M.val = { op: null, minDif: 10 };
    const ops = new Map(); if (C.op >= 0) ds.rows.forEach(r => { const o = norm(r[C.op]); if (o) ops.set(o, (ops.get(o) || 0) + 1); });
    const lista = [...ops.entries()].sort((a, b) => b[1] - a[1]); if (!M.val.op && lista.length) M.val.op = lista[0][0]; if (!M.metodo || !METODOS.some(m => m.id === M.metodo)) { M.metodo = "val_rk"; M.metodoNombre = NOMBRES.val_rk; }
    const NOMOP = { venta: "Venta", renta: "Renta mensual", noche: "Alquiler por noche" };
    return `${H.cab("3", "Método de valuación y limpieza", "Define qué operación se valúa y cómo se calcula el precio esperado. Antes de modelar se limpian las publicaciones automáticamente.")}
      <section class="mo-sx2"><h3>1 · Operación a valuar <small>${H.gN(ds.rows.length, 0)} publicaciones en la tabla</small></h3><p class="mo-mut">No se mezclan operaciones: el precio de una venta, de una renta mensual y de una noche no son comparables. Se analiza una a la vez.</p>
        <div class="mo-chs">${lista.length ? lista.map(([o, n]) => `<button type="button" class="mo-c${M.val.op === o ? " on" : ""}" data-val-op="${E(o)}">${H.ck(M.val.op === o)} ${E(NOMOP[o] || o)} <small>${H.gN(n, 0)}</small></button>`).join("") : '<span class="mo-mut">La tabla no trae una columna de operación: se valúan todas las publicaciones juntas.</span>'}</div>
        <p class="mo-mut">Precio analizado: <b>${E(iy >= 0 ? ds.cols[iy].label : "—")}</b>${M.val.op ? ` · ${E({ venta: "precio total de venta", renta: "renta mensual", noche: "precio por noche" }[M.val.op] || "")}` : ""}.</p></section>
      <section class="mo-sx2"><h3>2 · Método analítico</h3><div class="mo-mg">${METODOS.map(m => `<button type="button" class="mo-mt${M.metodo === m.id ? " on" : ""}" data-val-met="${m.id}"><div class="mo-mh"><div class="t"><b>${E(m.nombre)}</b>${m.rec ? '<span class="mo-rec">Recomendado</span>' : ""}<small>${E(m.tag)}</small></div></div><p>${E(m.desc)}</p>
        <div class="mo-pc2"><div><small>Ventajas</small><ul>${m.v.map(x => `<li>${E(x)}</li>`).join("")}</ul></div><div><small>Alcances y límites</small><ul>${m.l.map(x => `<li>${E(x)}</li>`).join("")}</ul></div></div></button>`).join("")}</div></section>
      <section class="mo-sx2"><h3>3 · Limpieza automática de las publicaciones</h3><p class="mo-mut">Las publicaciones guardadas pueden estar incompletas o traer datos espurios. Estas reglas se aplican siempre, en este orden, y al final verás cuántas quitó cada una y cuáles publicaciones salieron:</p>
        <ol class="mv-reglas"><li><b>Sin precio o sin coordenadas válidas.</b> No se pueden valuar ni ubicar.</li><li><b>Otra operación.</b> Se conserva solo la operación elegida.</li><li><b>Precio fuera de lo plausible</b> para esa operación (por ejemplo ventas menores a $150 mil o rentas de $1 o de $33 millones).</li><li><b>Ubicación poco precisa</b> (centro de la ciudad o del municipio).</li><li><b>Valores imposibles:</b> superficies menores a 12 m², recámaras o baños absurdos; el valor se anula y la publicación se conserva.</li><li><b>Duplicados</b> (misma ubicación, precio y características, o mismo título y precio).</li><li><b>Registros incompletos</b> que casi no describen la propiedad.</li><li><b>Fuera de la zona de mercado:</b> publicaciones mal geocodificadas o de otra ciudad.</li><li><b>Precios atípicos</b> frente a lo normal para su tipo de inmueble (precio por m² con z robusto mayor a 3.5), en dos pasadas.</li></ol>
        <label class="mo-fl">Diferencia mínima para señalar una propiedad como cara o barata <small class="mo-mut">(en %; por debajo de esto se considera «en línea con el mercado»)</small><input class="mo-in mo-num" id="mvMinDif" data-val-in="minDif" type="number" min="1" max="60" step="1" value="${M.val.minDif}"></label></section>
      ${H.pie("enfoque", "modelar", "Continuar a Modelar", !M.metodo)}`;
  }

  /* ── resultados ── */
  const hist = (r, H) => {
    const L = r.limpieza, mx = L.total || 1;
    return `<div class="mo-tw"><table class="mo-t mv-lim"><thead><tr><th>Paso de limpieza</th><th>Qué hace</th><th>Se quitaron</th><th>Quedan</th><th></th></tr></thead><tbody><tr><td><b>Publicaciones originales</b></td><td>Tabla del proyecto</td><td></td><td><b>${H.gN(L.total, 0)}</b></td><td><span class="mv-bar"><i style="width:100%"></i></span></td></tr>${L.pasos.map(p => `<tr class="${p.quitadas ? "" : "mv-nada"}"><td><b>${E(p.titulo)}</b></td><td>${E(p.detalle)}${p.corregidos ? ` <em>(${H.gN(p.corregidos, 0)} valores anulados)</em>` : ""}</td><td>${p.quitadas ? "−" + H.gN(p.quitadas, 0) : "0"}</td><td>${H.gN(p.quedan, 0)}</td><td><span class="mv-bar"><i style="width:${(p.quedan / mx * 100).toFixed(1)}%"></i></span></td></tr>`).join("")}</tbody></table></div>`;
  };
  function svgVario(V) {
    if (!V || !V.bins || V.bins.length < 3) return '<p class="mo-mut">No hubo suficientes pares de propiedades para estimar el variograma.</p>';
    const W = 560, Hh = 260, L = 52, R = 14, T = 14, B = 40, xs = V.bins.map(b => b.h / 1000), maxX = Math.max(...xs, V.lag / 1000) * 1.05, ys = V.bins.map(b => b.g), aj = V.ajuste, maxY = Math.max(...ys, aj ? aj.meseta : 0) * 1.15 || 1, X = v => L + v / maxX * (W - L - R), Y = v => Hh - B - v / maxY * (Hh - B - T);
    let h = `<svg class="mv-svg" viewBox="0 0 ${W} ${Hh}" role="img" aria-label="Variograma empírico y modelo ajustado"><rect x="${L}" y="${T}" width="${W - L - R}" height="${Hh - B - T}" fill="#f8fafc" stroke="#e2e8f0"/>`;
    for (let i = 0; i <= 4; i++) { const v = maxY * i / 4; h += `<line x1="${L}" x2="${W - R}" y1="${Y(v)}" y2="${Y(v)}" stroke="#e2e8f0"/><text x="${L - 6}" y="${Y(v) + 4}" text-anchor="end" class="mv-ax">${v.toFixed(2)}</text>`; }
    for (let i = 0; i <= 5; i++) { const v = maxX * i / 5; h += `<text x="${X(v)}" y="${Hh - B + 16}" text-anchor="middle" class="mv-ax">${v.toFixed(1)}</text>`; }
    h += `<text x="${(L + W - R) / 2}" y="${Hh - 6}" text-anchor="middle" class="mv-ax">Distancia entre propiedades (km)</text><text transform="translate(12 ${(T + Hh - B) / 2}) rotate(-90)" text-anchor="middle" class="mv-ax">Semivarianza del precio</text>`;
    if (aj) { const f = MODV[aj.tipo]; let d = ""; for (let i = 0; i <= 60; i++) { const hm = maxX * 1000 * i / 60; d += (i ? "L" : "M") + X(hm / 1000).toFixed(1) + "," + Y(f(hm, aj.c0, aj.c, aj.a)).toFixed(1); } h += `<line x1="${L}" x2="${W - R}" y1="${Y(aj.meseta)}" y2="${Y(aj.meseta)}" stroke="#a5b4fc" stroke-dasharray="5 4"/><path d="${d}" fill="none" stroke="#4f46e5" stroke-width="2.4"/>`; }
    V.bins.forEach(b => { h += `<circle cx="${X(b.h / 1000)}" cy="${Y(b.g)}" r="${Math.max(3, Math.min(7, 2 + Math.log10(b.n)))}" fill="#06b6d4" fill-opacity=".85" stroke="#fff"><title>${(b.h / 1000).toFixed(2)} km · ${b.g.toFixed(3)} · ${b.n} pares</title></circle>`; });
    return h + "</svg>";
  }
  function barrasEf(r) {
    const C = (r.modelo.coefs || []).slice(0, 12), mx = Math.max(...C.map(c => Math.abs(c.beta)), .01);
    return `<div class="mv-ef">${C.map(c => `<div class="mv-efr"><span title="${E(c.nombre)}">${E(c.nombre)}</span><div class="mv-efb"><i class="${c.beta >= 0 ? "pos" : "neg"}" style="width:${(Math.abs(c.beta) / mx * 50).toFixed(1)}%;${c.beta >= 0 ? "left:50%" : "right:50%"}"></i></div><b class="${c.beta >= 0 ? "pos" : "neg"}">${fp(c.efecto, 0)}</b></div>`).join("")}</div>`;
  }
  const filasVis = (r, st) => { const q = norm(st.q || ""); let L = r.filas.filter(f => (!st.cls || !st.cls.size || st.cls.has(f.clase)) && (!q || norm(f.titulo + " " + f.tipo + " " + f.colonia + " " + f.ciudad + " " + f.id).includes(q))); const sg = st.asc ? 1 : -1; const k = st.orden || "dif", pm = f => f.area ? f.precio / f.area : Infinity; L = L.slice().sort((a, b) => k === "pm2" ? sg * (pm(a) - pm(b)) : k === "precio" ? sg * (a.precio - b.precio) : k === "esperado" ? sg * (a.esperado - b.esperado) : k === "area" ? sg * ((a.area || 0) - (b.area || 0)) : sg * (a.dif - b.dif)); return L; };
  function filaHtml(f, k, H) {
    const c = CLS[f.clase];
    return `<tr class="mv-tr" data-val-foco="${k}"><td class="mv-t1">${f.url ? `<a href="${E(f.url)}" target="_blank" rel="noopener" data-val-nolink="1">${E(f.titulo || f.id)}</a>` : `<b>${E(f.titulo || f.id)}</b>`}<small>${E([f.tipo, f.colonia, f.ciudad].filter(Boolean).join(" · "))}</small></td><td>${f.area ? H.gN(f.area, 0) : "—"}</td><td>${f.rec != null ? f.rec : "—"} / ${f.ban != null ? f.ban : "—"}</td><td><b>${fm(f.precio)}</b></td><td>${fm(f.esperado)}<small>${fm(f.bajo)} – ${fm(f.alto)}</small></td><td><b style="color:${f.dif < 0 ? "#047857" : f.dif > 0 ? "#b45309" : "#64748b"}">${fp(f.dif, 0)}</b><small>z = ${f.z}</small></td><td><span class="mv-chip" style="--c:${c[1]}">${E(c[0])}</span></td></tr>`;
  }
  /* ficha de la publicación: ventana con fotos, datos, descripción y el resultado de la valuación */
  function cierraFicha() { const o = document.getElementById("mvFicha"); if (o) o.remove(); document.removeEventListener("keydown", cierraFicha._k, true); document.body.style.overflow = ""; }
  async function ficha(r, k, H) {
    const f = r.filas[k]; if (!f) return; cierraFicha(); const c = CLS[f.clase], ov = document.createElement("div"); ov.id = "mvFicha"; ov.className = "mv-ov"; document.body.style.overflow = "hidden";
    ov.innerHTML = `<div class="mv-mo" role="dialog" aria-modal="true"><button type="button" class="mv-x" data-val-cierra="1" aria-label="Cerrar">✕</button><div class="mv-fb"><div class="mo-bn" style="padding:40px;text-align:center">Cargando la ficha…</div></div></div>`; document.body.appendChild(ov);
    cierraFicha._k = e => { if (e.key === "Escape") cierraFicha(); }; document.addEventListener("keydown", cierraFicha._k, true);
    const pm = f.area ? f.precio / f.area : null, vr = `<div class="mv-fv" style="--c:${c[1]}"><span class="mv-vd">${E(c[0])}</span><div class="mv-fg"><div><small>Precio publicado</small><b>${fm(f.precio)}</b></div><div><small>Precio esperado</small><b>${fm(f.esperado)}</b></div><div><small>Diferencia</small><b style="color:${c[1]}">${fp(f.dif, 0)}</b></div>${pm ? `<div><small>Precio por m²</small><b>${fm(pm)}</b></div>` : ""}</div><p>Con sus características y su ubicación, el modelo espera un precio de ${fm(f.esperado)} (rango de 80 %: ${fm(f.bajo)} a ${fm(f.alto)}). Esta propiedad se publica ${f.dif < 0 ? "<b>" + Math.round(-f.dif * 100) + " % por debajo</b>" : "<b>" + Math.round(f.dif * 100) + " % por encima</b>"} de ese valor (z = ${f.z}).</p></div>`;
    try { const j = await H.api("/geometrika/modelab/inmuebles/ficha", { id: f.id }), F = j.ficha, fo = (F.fotos || []).filter(Boolean), cuerpo = ov.querySelector(".mv-fb"); if (!cuerpo) return;
      cuerpo.innerHTML = `<div class="mv-gal">${fo.length ? `<div class="mv-big"><img id="mvBig" src="${E(fo[0])}" alt="" referrerpolicy="no-referrer"></div>${fo.length > 1 ? `<div class="mv-th2">${fo.slice(0, 24).map((u, i) => `<img src="${E(u)}" data-val-ph="${i}" class="${i ? "" : "on"}" alt="" referrerpolicy="no-referrer">`).join("")}</div>` : ""}` : '<div class="mv-big mv-sf">Sin fotos</div>'}</div>
        <div class="mv-fd"><div class="mv-fh"><span class="mv-src">${E(F.fuente)}</span>${f.tipo ? `<span class="mv-src">${E(f.tipo)}</span>` : ""}</div><h2>${E(F.titulo || f.titulo || f.id)}</h2><p class="mo-mut">${E(F.ubicacion || "")}${F.publicado ? " · " + E(String(F.publicado).slice(0, 10)) : ""}</p>${vr}
          ${F.pares && F.pares.length ? `<h3>Características</h3><div class="mv-pg">${F.pares.map(([a, b]) => `<div><small>${E(a)}</small><b>${E(b)}</b></div>`).join("")}</div>` : ""}
          ${F.amenidades && F.amenidades.length ? `<h3>Amenidades</h3><div class="mo-chs">${F.amenidades.map(a => `<span class="mv-chip" style="--c:#64748b">${E(a)}</span>`).join("")}</div>` : ""}
          ${F.vendedor ? `<h3>Publicante</h3><p>👤 ${E(F.vendedor)}</p>` : ""}
          ${F.descripcion ? `<h3>Descripción</h3><p class="mv-ds">${E(F.descripcion)}</p>` : ""}
          ${F.url ? `<a class="mv-fi" href="${E(F.url)}" target="_blank" rel="noopener">Abrir la publicación original ↗</a>` : ""}</div>`;
    } catch (e) { const cuerpo = ov.querySelector(".mv-fb"); if (cuerpo) cuerpo.innerHTML = `<div class="mv-fd"><h2>${E(f.titulo || f.id)}</h2>${vr}<p class="mo-mut">No se pudo cargar el detalle de la publicación (${E(e.message)}).</p>${f.url ? `<a class="mv-fi" href="${E(f.url)}" target="_blank" rel="noopener">Abrir la publicación original ↗</a>` : ""}</div>`; }
    ov.addEventListener("click", e => { if (e.target === ov || e.target.closest("[data-val-cierra]")) { cierraFicha(); return; } const t = e.target.closest("[data-val-ph]"); if (t) { const big = ov.querySelector("#mvBig"); if (big) big.src = t.src; ov.querySelectorAll(".mv-th2 img").forEach(x => x.classList.toggle("on", x === t)); } });
  }
  /* miniatura ligera: Mercado Libre sirve la misma foto en 500 px (-O) en lugar de 1200 px (-F), unas 6 veces menos peso */
  const mini = u => /mlstatic\.com/.test(u) ? u.replace(/-F(\.|-)/, "-O$1") : u;
  function cardHtml(f, k, r, F, H) {
    const c = CLS[f.clase], x = F[f.id] || {}, op = { venta: "Venta", renta: "Renta", noche: "Noche" }[r.config.op] || "", pm = f.area ? f.precio / f.area : null, sp = [f.rec != null ? f.rec + " rec." : "", f.ban != null ? f.ban + " baños" : "", f.area ? H.gN(f.area, 0) + " m²" : ""].filter(Boolean).join(" · "), w = Math.min(100, Math.abs(f.dif) * 100 / 1.2);
    return `<div class="mv-cd" data-val-foco="${k}"><div class="mv-cv${x.f ? "" : " nf"}">${x.f ? `<img src="${E(mini(x.f))}" alt="" decoding="async" referrerpolicy="no-referrer" onerror="if(!this.dataset.r){this.dataset.r=1;this.src='${E(x.f)}'}else{this.parentNode.classList.add('nf');this.remove()}">` : ""}<span class="mv-nf">Sin foto</span><div class="mv-bg"><span class="b">${E(f.tipo || "Inmueble")}</span>${op ? `<span class="o">${op}</span>` : ""}</div><span class="mv-vd" style="--c:${c[1]}">${E(c[0])}</span></div>
      <div class="mv-lb"><h4>${E(f.titulo || f.id)}</h4><small>${E([x.s || f.colonia, f.ciudad].filter(Boolean).join(" · "))}</small><small>${E(sp) || "&nbsp;"}</small>${x.h ? `<small>👤 ${E(x.h)}</small>` : ""}
        <div class="mv-pr"><span><b>${fm(f.precio)}</b></span><span>${pm ? `<b>${fm(pm)}</b> <small>/m²</small>` : ""}</span></div>
        <div class="mv-md"><div class="mv-mr"><span>Precio esperado</span><b>${fm(f.esperado)}</b></div><small>Rango 80 %: ${fm(f.bajo)} – ${fm(f.alto)}</small><div class="mv-df" style="--c:${c[1]}"><i style="width:${w.toFixed(0)}%;${f.dif < 0 ? "right:50%" : "left:50%"}"></i><em></em></div><div class="mv-mr"><span>${f.dif < 0 ? "Por debajo del mercado" : "Por encima del mercado"}</span><b style="color:${c[1]}">${fp(f.dif, 0)}</b></div><small>z = ${f.z}</small></div>
        <span class="mv-fi" role="button" tabindex="0" data-val-ficha="${k}">Ver ficha ↗</span></div></div>`;
  }
  function htmlResultados(r, H) {
    const S = H.S, st = S.mv || (S.mv = { cls: new Set(), q: "", orden: "dif", asc: true, vista: 100 }), cn = r.resumen.conteos, n = r.limpieza.conserva, op = { venta: "venta", renta: "renta mensual", noche: "alquiler por noche" }[r.config.op] || "precio", aj = r.variograma && r.variograma.ajuste;
    const kp = [["Publicaciones útiles", H.gN(n, 0), `de ${H.gN(r.limpieza.total, 0)} originales (${Math.round(n / r.limpieza.total * 100)} %)`], ["Por debajo del mercado", H.gN(cn.muy_abajo + cn.abajo, 0), `${H.gN(cn.muy_abajo, 0)} muy por debajo`, "#047857"], ["En línea", H.gN(cn.linea, 0), `${Math.round(cn.linea / n * 100)} % de las útiles`, "#64748b"], ["Por encima del mercado", H.gN(cn.arriba + cn.muy_arriba, 0), `${H.gN(cn.muy_arriba, 0)} muy por encima`, "#dc2626"], ["Confiabilidad (R² validado)", r.modelo.r2cv.toFixed(2), `error mediano del precio ${(r.modelo.mdape * 100).toFixed(0)} %`]];
    const cal = r.modelo.r2cv < .35 ? `<div class="mo-av alto"><div>La confiabilidad es <b>baja</b> (R² ${r.modelo.r2cv.toFixed(2)}): las publicaciones traen pocos atributos o muy variables, así que el «precio esperado» es solo una referencia amplia. Usa con cautela las clasificaciones.</div></div>` : r.modelo.r2cv < .6 ? `<div class="mo-av"><div>La confiabilidad es <b>media</b> (R² ${r.modelo.r2cv.toFixed(2)}). Úsala para priorizar propiedades por revisar, no como avalúo.</div></div>` : "";
    return `<div class="mv-kpis">${kp.map(k => `<div class="mv-k"${k[3] ? ` style="--c:${k[3]}"` : ""}><span>${k[0]}</span><b>${k[1]}</b><small>${k[2]}</small></div>`).join("")}</div>${cal}
      <section class="mo-sx2"><h3>Resumen</h3><p>Se valuó la <b>${E(op)}</b> con el método <b>${E(r.nombreMetodo)}</b>. Después de limpiar, quedaron <b>${H.gN(n, 0)}</b> publicaciones útiles. El modelo estima el precio esperado de cada propiedad según sus características${aj ? " y el precio de las propiedades cercanas" : ""}, y marca como <b>baratas</b> o <b>caras</b> las que se desvían más de lo normal del mercado (diferencia mínima ${Math.round(r.config.minDif * 100)} % y z ≥ 1). Mediana de la diferencia: ${fp(r.resumen.medianaDif, 1)}.</p></section>
      <section class="mo-sx2"><h3>Mapa y tabla de propiedades <small>clic en una fila para ubicarla; el enlace abre la publicación</small></h3>
        <div class="mv-leg">${ORD.map(k => `<button type="button" class="mv-lc${st.cls.has(k) ? " on" : ""}" data-val-cls="${k}" style="--c:${CLS[k][1]}"><i></i>${E(CLS[k][0])} <small>${H.gN(cn[k], 0)}</small></button>`).join("")}<button type="button" class="mo-ghost" data-val-cls="">Todas</button><span class="mo-sp"></span><input id="mvQ" class="mo-in mv-q" data-val-in="q" type="search" placeholder="Buscar por título, tipo, colonia…" value="${E(st.q)}"><button type="button" class="mo-ghost" data-val-csv="1">⬇ Descargar CSV</button></div>
        <div class="mv-bar"><label class="mv-sel">Ordenar: <select class="mo-in" id="mvOrd" data-val-in="ord">${[["dif|1", "Más baratas frente al mercado"], ["dif|0", "Más caras frente al mercado"], ["precio|1", "Precio: menor a mayor"], ["precio|0", "Precio: mayor a menor"], ["pm2|1", "Precio por m²: menor a mayor"], ["pm2|0", "Precio por m²: mayor a menor"], ["area|0", "Superficie: mayor a menor"], ["esperado|0", "Precio esperado: mayor a menor"]].map(([v, t]) => `<option value="${v}"${v === (st.orden || "dif") + "|" + (st.asc ? 1 : 0) ? " selected" : ""}>${t}</option>`).join("")}</select></label><span class="mv-vm"><button type="button" class="mo-ghost${st.modo !== "tabla" ? " on" : ""}" data-val-modo="cards">▦ Tarjetas</button><button type="button" class="mo-ghost${st.modo === "tabla" ? " on" : ""}" data-val-modo="tabla">☰ Tabla</button></span></div>
        <div class="mv-split"><div class="mv-mapw"><div id="mvMap" class="mo-map mv-map"></div></div><div class="mv-tabw">${st.modo === "tabla" ? '<div class="mo-tw mv-tw"><table class="mo-t mv-t"><thead><tr><th>Propiedad</th><th><button type="button" class="mv-th" data-val-ord="area">m²</button></th><th>Rec./Baños</th><th><button type="button" class="mv-th" data-val-ord="precio">Precio publicado</button></th><th><button type="button" class="mv-th" data-val-ord="esperado">Precio esperado</button></th><th><button type="button" class="mv-th" data-val-ord="dif">Diferencia ${st.orden === "dif" ? (st.asc ? "▲" : "▼") : ""}</button></th><th>Clasificación</th></tr></thead><tbody id="mvTb"></tbody></table></div>' : '<div class="mv-cards" id="mvCards"></div>'}<div class="mo-chs" id="mvMas"></div></div></div></section>
      <section class="mo-sx2"><h3>¿Qué se hizo con los datos? <small>limpieza de las publicaciones</small></h3>${hist(r, H)}<p class="mo-mut">Cada paso trabaja sobre lo que dejó el anterior. Al final de esta página puedes ver cuáles publicaciones salieron y por qué.</p></section>
      <section class="mo-sx2"><h3>Qué influye en el precio <small>modelo hedónico · efecto de cada atributo</small></h3><p class="mo-mut">Efecto aproximado en el precio al subir una desviación estándar del atributo (o al pertenecer a la categoría), manteniendo lo demás igual. Se calculó con ${H.gN(r.config.nAtributos, 0)} atributos y penalización Ridge (λ = ${r.config.lambda}).</p>${barrasEf(r)}</section>
      ${r.variograma ? `<section class="mo-sx2"><h3>Dependencia espacial del precio <small>variograma y kriging</small></h3><div class="mv-vg"><div>${svgVario(r.variograma)}<p class="mo-mut">Puntos: semivarianza observada entre pares de propiedades a cada distancia. Línea: modelo ${aj ? E(aj.tipo) : ""} ajustado.</p></div><div class="mv-vgi">${aj ? `<div class="mv-par"><span>Modelo</span><b>${E(aj.tipo)}</b></div><div class="mv-par"><span>Alcance (distancia a la que dejan de parecerse)</span><b>${(aj.a / 1000).toFixed(1)} km</b></div><div class="mv-par"><span>Efecto pepita (variación sin explicar localmente)</span><b>${aj.c0.toFixed(3)}</b></div><div class="mv-par"><span>Meseta (variación total)</span><b>${aj.meseta.toFixed(3)}</b></div><div class="mv-par"><span>Dependencia espacial</span><b>${Math.round(aj.dependencia * 100)} %</b></div><p>${aj.dependencia >= .66 ? "La ubicación pesa mucho: propiedades cercanas tienen precios muy parecidos, aun con los mismos atributos." : aj.dependencia >= .33 ? "Hay dependencia espacial moderada: la vecindad explica una parte del precio que los atributos no captan." : "La dependencia espacial es débil: los atributos explican casi todo el precio."}</p>` : '<p class="mo-mut">No se pudo ajustar un modelo de variograma.</p>'}</div></div></section>` : ""}
      <section class="mo-sx2"><details class="mo-sx"><summary><b>Publicaciones que se excluyeron</b><span>${H.gN(r.limpieza.nExcl, 0)} en total${r.limpieza.nExcl > r.limpieza.excluidas.length ? ` · se muestran ${H.gN(r.limpieza.excluidas.length, 0)}` : ""}</span></summary><div class="mo-tw mv-tw"><table class="mo-t"><thead><tr><th>ID</th><th>Motivo</th></tr></thead><tbody>${r.limpieza.excluidas.slice(0, 600).map(x => `<tr><td>${E(x.id)}</td><td>${E(x.motivo)}</td></tr>`).join("")}</tbody></table></div></details></section>
      <section class="mo-sx2"><h3>Cómo leer este análisis</h3><ul class="mo-rsm"><li>El <b>precio esperado</b> es el precio que le corresponde a una propiedad con esas características y en esa ubicación según el mercado de las publicaciones útiles; el rango es de 80 %.</li><li>Una propiedad <b>por debajo</b> del mercado no es necesariamente una ganga: puede tener defectos que la publicación no menciona (estado, régimen legal, acabados). Revísala en campo.</li><li>La valuación usa precios de <b>publicación</b> (lo que piden), no de cierre; en venta suele haber margen de negociación.</li><li>Las clasificaciones son relativas al conjunto de publicaciones analizado: si cambias la operación, el filtro o cargas más datos, los resultados cambian.</li></ul></section>`;
  }
  /* mapa y tabla enlazados */
  function post(r, H) {
    const S = H.S, st = S.mv; const pinta = () => {
      const L = filasVis(r, st), tb = document.getElementById("mvTb"), cd = document.getElementById("mvCards"), vac = "Ninguna propiedad coincide con el filtro."; if (tb) tb.innerHTML = L.slice(0, st.vista).map(f => filaHtml(f, r.filas.indexOf(f), H)).join("") || `<tr><td colspan="7" class="mo-mut">${vac}</td></tr>`; else if (cd) cd.innerHTML = L.slice(0, st.vista).map(f => cardHtml(f, r.filas.indexOf(f), r, S._mvF || {}, H)).join("") || `<p class="mo-mut">${vac}</p>`; else return;
      const mas = document.getElementById("mvMas"); if (mas) mas.innerHTML = L.length > st.vista ? `<span class="mo-mut">Mostrando ${H.gN(st.vista, 0)} de ${H.gN(L.length, 0)}.</span><button type="button" class="mo-ghost" data-val-mas="1">Mostrar 100 más</button>` : `<span class="mo-mut">${H.gN(L.length, 0)} propiedades.</span>`;
      if (S._mvM) { S._mvM.cap.clearLayers(); const ids = new Set(L.map(f => r.filas.indexOf(f))); S._mvM.pts.forEach((m, k) => { if (ids.has(k)) S._mvM.cap.addLayer(m); }); }
    }; S._mvPinta = pinta;
    H.leaflet(() => { const el = document.getElementById("mvMap"); if (!el || !window.L) return; if (S._mvM && S._mvM.map) { try { S._mvM.map.remove(); } catch (_) { } } const Lf = window.L, map = Lf.map(el, { zoomControl: true }), cap = Lf.layerGroup().addTo(map); Lf.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(map);
      const pts = r.filas.map(f => Lf.circleMarker([f.lat, f.lng], { radius: 5.5, color: "#fff", weight: 1, fillColor: CLS[f.clase][1], fillOpacity: .9 }).bindPopup(`<b>${E(f.titulo || f.id)}</b><br>Publicado: <b>${fm(f.precio)}</b><br>Esperado: ${fm(f.esperado)} (${fp(f.dif, 0)})<br><span style="color:${CLS[f.clase][1]}">${E(CLS[f.clase][0])}</span>${f.url ? `<br><a href="${E(f.url)}" target="_blank" rel="noopener">Abrir la publicación</a>` : ""}`)); S._mvM = { map, cap, pts };
      const b = Lf.latLngBounds(r.filas.map(f => [f.lat, f.lng])); map.fitBounds(b.pad(.1)); setTimeout(() => { map.invalidateSize(); pinta(); }, 120); });
    pinta();
    if (!S._mvF || S._mvFr !== r.creado) { S._mvFr = r.creado; S._mvF = {}; if (H.api) H.api("/geometrika/modelab/inmuebles/fotos", { ids: r.filas.map(f => f.id) }).then(j => { S._mvF = j.items || {}; if (S.sec === "resultados" && S._mvPinta) S._mvPinta(); }).catch(() => { }); }
  }
  function csv(r) {
    const cab = ["id", "titulo", "tipo", "ciudad", "colonia", "m2", "recamaras", "banos", "precio_publicado", "precio_esperado", "rango_bajo", "rango_alto", "diferencia_pct", "z", "clasificacion", "latitud", "longitud", "url"], q = v => { const s = v == null ? "" : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
    return "﻿" + [cab.join(",")].concat(r.filas.map(f => [f.id, f.titulo, f.tipo, f.ciudad, f.colonia, f.area, f.rec, f.ban, f.precio, f.esperado, f.bajo, f.alto, (f.dif * 100).toFixed(1), f.z, CLS[f.clase][0], f.lat, f.lng, f.url].map(q).join(","))).join("\n");
  }
  /* eventos propios de la valuación (devuelven true si el evento era suyo) */
  function clic(e, H) {
    const t = e.target, S = H.S; let b;
    if ((b = t.closest && t.closest("[data-val-op]"))) { S.M.val = S.M.val || {}; S.M.val.op = b.dataset.valOp; H.guarda(); H.reMetodos(); return true; }
    if ((b = t.closest && t.closest("[data-val-met]"))) { S.M.metodo = b.dataset.valMet; S.M.metodoNombre = NOMBRES[S.M.metodo]; S.M.prepOk = true; H.guarda(); H.reMetodos(); return true; }
    if (!S.res || S.res.enfoque !== "val") return false; const r = S.res;
    if ((b = t.closest("[data-val-cls]"))) { const st = S.mv; const k = b.dataset.valCls; if (!k) st.cls.clear(); else if (st.cls.has(k)) st.cls.delete(k); else st.cls.add(k); st.vista = 100; $$("[data-val-cls]").forEach(x => { if (x.dataset.valCls) x.classList.toggle("on", st.cls.has(x.dataset.valCls)); }); S._mvPinta && S._mvPinta(); return true; }
    if ((b = t.closest("[data-val-modo]"))) { S.mv.modo = b.dataset.valModo; S.mv.vista = 100; H.reResultados(); return true; }
    if ((b = t.closest("[data-val-ord]"))) { const st = S.mv, k = b.dataset.valOrd; if (st.orden === k) st.asc = !st.asc; else { st.orden = k; st.asc = true; } H.reResultados(); return true; }
    if ((b = t.closest("[data-val-mas]"))) { S.mv.vista += 100; S._mvPinta && S._mvPinta(); return true; }
    if ((b = t.closest("[data-val-csv]"))) { const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([csv(r)], { type: "text/csv;charset=utf-8" })); a.download = "valuacion-inmuebles.csv"; document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 500); return true; }
    if ((b = t.closest("[data-val-ficha]"))) { ficha(r, +b.dataset.valFicha, H); return true; }
    if ((b = t.closest("[data-val-foco]"))) { if (t.closest("a")) return false; const k = +b.dataset.valFoco, m = S._mvM && S._mvM.pts[k]; if (m && S._mvM.map) { S._mvM.cap.addLayer(m); S._mvM.map.flyTo(m.getLatLng(), Math.max(S._mvM.map.getZoom(), 16), { duration: .5 }); m.openPopup(); $$(".mv-tr.foco").forEach(x => x.classList.remove("foco")); b.classList.add("foco"); } return true; }
    return false;
  }
  function cambia(e, H) {
    const t = e.target; if (t.dataset && t.dataset.valIn === "ord") { const [k, a] = t.value.split("|"), st = H.S.mv; st.orden = k; st.asc = a === "1"; st.vista = 100; H.S._mvPinta && H.S._mvPinta(); return true; }
    if (t.dataset && t.dataset.valIn === "minDif") { const v = Math.max(1, Math.min(60, parseFloat(t.value) || 10)); H.S.M.val = H.S.M.val || {}; H.S.M.val.minDif = v; H.guarda(); return true; }
    return false;
  }
  function entrada(e, H) { const t = e.target; if (t.dataset && t.dataset.valIn === "q") { const st = H.S.mv; if (!st) return false; st.q = t.value; st.vista = 100; H.S._mvPinta && H.S._mvPinta(); return true; } return false; }
  const $$ = s => Array.from(document.querySelectorAll(s));

  window.MLVal = { htmlMetodos, htmlResultados, post, clic, cambia, entrada, correr, limpia, NOMBRES, LIM, mapeaCols, _t: { variograma, krigingLOO, cvRidge, armaX, ridge, resuelve, esf } };
})();
