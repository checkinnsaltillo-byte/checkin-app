/* EcoLab · laboratorio económico: catálogo en árbol (Área › Grupo › Indicador) + tablero interactivo.
   Necesita ecolab-datos.js (window.ECO_DATA, generado por ecolab-descarga.js) y ecolab.css. Se inicia con window.ecolabInit(). */
(function () {
  "use strict";
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
  let host, IND = [], AREAS = [], BYID = {}, S = { sel: "home", q: "", open: new Set(), serie: { mode: "nivel", rango: 120 } }, draws = [], tipEl;

  /* ───────── preparación de datos ───────── */
  function prep(r, k) {
    const ppy = PPY[r.frecuencia] || 1, I = Object.assign({}, r);
    I.ppy = ppy;
    I.s = r.obs.map(([p, v]) => {
      const m = /^(\d{4})(?:\/(\d+))?/.exec(p), yr = +m[1], q = m[2] ? +m[2] : 1;
      return { yr, q, t: yr * ppy + (q - 1), v, l: ppy === 12 ? MES[q - 1] + " " + yr : ppy === 4 ? "T" + q + " " + yr : String(yr) };
    });
    const s = I.s, n = s.length, neg = s.some(o => o.v <= 0);
    I.dif = neg || /Puntos|Porcentaje/.test(r.unidad);
    I.pct = /^Porcentaje/.test(r.unidad);
    const ch = (a, b) => b == null ? null : I.dif ? a - b : b > 0 ? (a / b - 1) * 100 : null;
    s.forEach((o, i) => { o.yoy = i >= ppy ? ch(o.v, s[i - ppy].v) : null; o.mom = i >= 1 && ppy > 1 ? ch(o.v, s[i - 1].v) : null; });
    // reloj económico: desviación vs tendencia (media móvil) y su cambio, ambos estandarizados
    const kk = ppy === 12 ? 12 : ppy === 4 ? 4 : 3, lag = ppy === 12 ? 3 : 1;
    s.forEach((o, i) => { o.dev = i >= kk - 1 ? o.v - s.slice(i - kk + 1, i + 1).reduce((a, b) => a + b.v, 0) / kk : null; });
    const dv = s.map(o => o.dev).filter(v => v != null), sd = Math.sqrt(dv.reduce((a, b) => a + b * b, 0) / (dv.length || 1)) || 1;
    s.forEach(o => { o.x = o.dev == null ? null : o.dev / sd; });
    s.forEach((o, i) => { o.y = i >= lag && o.x != null && s[i - lag].x != null ? o.x - s[i - lag].x : null; });
    const ys = s.map(o => o.y).filter(v => v != null), sy = Math.sqrt(ys.reduce((a, b) => a + b * b, 0) / (ys.length || 1)) || 1;
    s.forEach(o => { if (o.y != null) o.y /= sy; });
    const last = s[n - 1], vals = s.map(o => o.v).sort((a, b) => a - b);
    I.last = last; I.prev = n > 1 ? s[n - 2] : null;
    I.min = vals[0]; I.max = vals[n - 1]; I.pctil = vals.filter(v => v <= last.v).length / n * 100;
    I.minO = s.find(o => o.v === I.min); I.maxO = s.find(o => o.v === I.max);
    I.quad = last.x == null || last.y == null ? null : last.x >= 0 ? (last.y >= 0 ? "exp" : "des") : (last.y >= 0 ? "rec" : "con");
    const now = new Date(), mesesAct = (now.getFullYear() * 12 + now.getMonth()) - (last.yr * 12 + (ppy === 12 ? last.q - 1 : ppy === 4 ? (last.q - 1) * 3 : 0));
    const ok = ppy === 12 ? 4 : ppy === 4 ? 8 : 24;
    I.edad = mesesAct; I.est = mesesAct <= ok ? ["ok", "Al día"] : mesesAct <= ok * 2 ? ["warn", "Rezagada"] : ["old", "Sin actualizar"];
    I.th50 = r.unidad === "Puntos" && I.min < 50 && I.max > 50;
    I.u = r.unidad === "Puntos" ? "pts" : I.pct ? "%" : r.unidad === "Dólares" ? (r.mult === "Millones" ? "mdd" : "USD") : /^Índice/.test(r.unidad) ? r.unidad.replace("Índice base", "índice base ") : /^Pesos/.test(r.unidad) ? (r.mult === "Millones" ? "mdp " : "pesos ") + (/2013/.test(r.unidad) ? "(2013)" : "corrientes") : r.unidad.toLowerCase();
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

  /* ───────── gráfica principal de la serie ───────── */
  function drawSerie(el, I) {
    const W = Math.max(320, el.clientWidth), H = W < 520 ? 280 : 360, m = { l: 62, r: 20, t: 16, b: 34 }, st = S.serie;
    const nmax = st.rango ? Math.min(I.s.length, Math.round(st.rango * I.ppy / 12) || I.s.length) : I.s.length, d = I.s.slice(-Math.max(nmax, 6)), n = d.length;
    const f = st.mode === "anual" ? (o => o.yoy) : (o => o.v), pts = d.map((o, i) => ({ o, i, v: f(o) })).filter(p => p.v != null);
    if (!pts.length) { el.innerHTML = '<div class="eco-vacio">Sin datos suficientes para esta vista.</div>'; return; }
    let lo = Math.min(...pts.map(p => p.v)), hi = Math.max(...pts.map(p => p.v));
    if (st.mode === "anual") { lo = Math.min(lo, 0); hi = Math.max(hi, 0); } else if (I.th50) { lo = Math.min(lo, 50); hi = Math.max(hi, 50); }
    const pad = (hi - lo) * .08 || 1; lo -= pad; hi += pad;
    const X = i => m.l + (n === 1 ? .5 : i / (n - 1)) * (W - m.l - m.r), Y = v => m.t + (1 - (v - lo) / (hi - lo)) * (H - m.t - m.b);
    const tk = ticksN(lo, hi, 5), td = tdec(tk);
    let s = `<defs><linearGradient id="ecoAr" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${I.col}" stop-opacity=".42"/><stop offset="1" stop-color="${I.col}" stop-opacity="0"/></linearGradient>`;
    const base = st.mode === "anual" ? Y(0) : I.th50 ? Y(50) : null;
    if (st.mode !== "anual" && I.th50) { const o = Math.max(0, Math.min(1, (Y(50) - m.t) / (H - m.t - m.b))); s += `<linearGradient id="ecoLn" x1="0" y1="${m.t}" x2="0" y2="${H - m.b}" gradientUnits="userSpaceOnUse"><stop offset="0" stop-color="#34d399"/><stop offset="${o}" stop-color="#34d399"/><stop offset="${o}" stop-color="#fb7185"/><stop offset="1" stop-color="#fb7185"/></linearGradient>`; }
    s += `<filter id="ecoGl" x="-20%" y="-20%" width="140%" height="140%"><feGaussianBlur stdDeviation="3.5" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs>`;
    tk.forEach(v => { s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(v).toFixed(1)}" y2="${Y(v).toFixed(1)}" class="eco-gr"/><text x="${m.l - 8}" y="${(Y(v) + 4).toFixed(1)}" class="eco-ax" text-anchor="end">${nf(v, td)}</text>`; });
    const nl = Math.min(8, Math.max(2, Math.floor((W - m.l) / 90)));
    for (let k = 0; k < nl; k++) { const i = Math.round(k * (n - 1) / (nl - 1)); s += `<text x="${X(i).toFixed(1)}" y="${H - 10}" class="eco-ax" text-anchor="${k === 0 ? "start" : k === nl - 1 ? "end" : "middle"}">${d[i].l}</text>`; }
    if (st.mode === "anual") {
      const bw = Math.max(1.5, Math.min(22, (W - m.l - m.r) / n * .72));
      pts.forEach((p, k) => { const y0 = Y(0), y1 = Y(p.v), up = p.v >= 0; s += `<rect class="eco-bar" x="${(X(p.i) - bw / 2).toFixed(1)}" y="${Math.min(y0, y1).toFixed(1)}" width="${bw.toFixed(1)}" height="${Math.max(1, Math.abs(y1 - y0)).toFixed(1)}" rx="${Math.min(3, bw / 2)}" fill="${up ? "#34d399" : "#fb7185"}" style="transform-origin:center ${up ? "bottom" : "top"};transform-box:fill-box;animation-delay:${Math.min(900, k * 6)}ms"/>`; });
      s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(0).toFixed(1)}" y2="${Y(0).toFixed(1)}" stroke="#94a3b8" stroke-opacity=".6"/>`;
    } else {
      const line = pts.map((p, k) => (k ? "L" : "M") + X(p.i).toFixed(1) + " " + Y(p.v).toFixed(1)).join(""), yb = base != null ? base : H - m.b;
      s += `<path d="${line}L${X(pts[pts.length - 1].i).toFixed(1)} ${yb.toFixed(1)}L${X(pts[0].i).toFixed(1)} ${yb.toFixed(1)}Z" fill="url(#ecoAr)" class="eco-area"/>`;
      if (I.th50) s += `<line x1="${m.l}" x2="${W - m.r}" y1="${Y(50).toFixed(1)}" y2="${Y(50).toFixed(1)}" stroke="#e2e8f0" stroke-opacity=".55" stroke-dasharray="5 5"/><text x="${W - m.r - 4}" y="${(Y(50) - 6).toFixed(1)}" class="eco-ax" text-anchor="end">umbral neutral 50</text>`;
      s += `<path d="${line}" pathLength="1" class="eco-ln" fill="none" stroke="${I.th50 ? "url(#ecoLn)" : I.col}" stroke-width="2.6" stroke-linejoin="round" stroke-linecap="round" filter="url(#ecoGl)"/>`;
      const lp = pts[pts.length - 1]; s += `<circle cx="${X(lp.i).toFixed(1)}" cy="${Y(lp.v).toFixed(1)}" r="5" fill="${I.col}" class="eco-pulse"/><circle cx="${X(lp.i).toFixed(1)}" cy="${Y(lp.v).toFixed(1)}" r="4.5" fill="#fff" stroke="${I.col}" stroke-width="2.5"/>`;
    }
    s += `<g id="ecoCh" style="display:none"><line class="eco-ch" y1="${m.t}" y2="${H - m.b}"/><circle r="5.5" fill="#0a0f1f" stroke="#fff" stroke-width="2.4"/></g><rect id="ecoHit" x="${m.l}" y="${m.t}" width="${W - m.l - m.r}" height="${H - m.t - m.b}" fill="transparent"/>`;
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="${esc(I.nombre)}">${s}</svg>`;
    const g = $("#ecoCh", el), ln = $("line", g), dot = $("circle", g), hit = $("#ecoHit", el);
    hit.onmousemove = hit.ontouchmove = e => {
      const r = hit.getBoundingClientRect(), cx = (e.touches ? e.touches[0].clientX : e.clientX), i = Math.round((cx - r.left) / r.width * (n - 1));
      let best = pts[0]; pts.forEach(p => { if (Math.abs(p.i - i) < Math.abs(best.i - i)) best = p; });
      g.style.display = ""; ln.setAttribute("x1", X(best.i)); ln.setAttribute("x2", X(best.i)); dot.setAttribute("cx", X(best.i)); dot.setAttribute("cy", Y(best.v));
      tip(tipI(I, best.o), e.touches ? e.touches[0] : e);
    };
    hit.onmouseleave = () => { g.style.display = "none"; tip(null); };
  }

  /* ───────── reloj económico ───────── */
  function drawReloj(el, I, big) {
    const W = Math.max(260, Math.min(el.clientWidth, big ? 760 : 520)), H = big ? Math.round(W * .74) : Math.round(W * .86), m = 30;
    const tr = I.s.filter(o => o.x != null && o.y != null).slice(-(big ? 30 : 24));
    if (tr.length < 3) { el.innerHTML = '<div class="eco-vacio">Se necesitan más observaciones para el reloj.</div>'; return; }
    const R = Math.max(1.6, Math.max(...tr.map(o => Math.max(Math.abs(o.x), Math.abs(o.y)))) * 1.15), X = v => m + (v / R + 1) / 2 * (W - 2 * m), Y = v => m + (1 - (v / R + 1) / 2) * (H - 2 * m), cx = X(0), cy = Y(0);
    let s = `<defs><filter id="ecoGl2"><feGaussianBlur stdDeviation="3" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs>`;
    const qd = [["exp", cx, m, W - m, cy], ["des", cx, cy, W - m, H - m], ["con", m, cy, cx, H - m], ["rec", m, m, cx, cy]];
    qd.forEach(([k, x0, y0, x1, y1]) => { s += `<rect x="${x0}" y="${y0}" width="${x1 - x0}" height="${y1 - y0}" fill="${QD[k][1]}" opacity="${I.quad === k ? .2 : .08}"/>`; });
    s += `<line x1="${m}" x2="${W - m}" y1="${cy}" y2="${cy}" class="eco-ch"/><line x1="${cx}" x2="${cx}" y1="${m}" y2="${H - m}" class="eco-ch"/>`;
    [["exp", W - m - 6, m + 16, "end"], ["des", W - m - 6, H - m - 8, "end"], ["con", m + 6, H - m - 8, "start"], ["rec", m + 6, m + 16, "start"]].forEach(([k, x, y, a]) => { s += `<text x="${x}" y="${y}" text-anchor="${a}" class="eco-q" fill="${QD[k][1]}">${QD[k][0].toUpperCase()}</text>`; });
    s += `<text x="${W - m}" y="${cy - 6}" class="eco-ax" text-anchor="end">nivel vs tendencia →</text><text x="${cx + 6}" y="${m - 8}" class="eco-ax">↑ impulso</text>`;
    const path = tr.map((o, i) => (i ? "L" : "M") + X(o.x).toFixed(1) + " " + Y(o.y).toFixed(1)).join("");
    s += `<path d="${path}" pathLength="1" class="eco-ln" fill="none" stroke="${I.col}" stroke-opacity=".75" stroke-width="2.4" stroke-linejoin="round" filter="url(#ecoGl2)"/>`;
    tr.forEach((o, i) => { const l = i === tr.length - 1; s += `<circle class="eco-dotp" data-i="${i}" cx="${X(o.x).toFixed(1)}" cy="${Y(o.y).toFixed(1)}" r="${l ? 8 : 3 + 3 * i / tr.length}" fill="${l ? "#fff" : I.col}" stroke="${I.col}" stroke-width="${l ? 3 : 0}" style="animation-delay:${i * 45}ms"/>`; });
    const lo = tr[tr.length - 1]; s += `<text x="${X(lo.x).toFixed(1)}" y="${(Y(lo.y) - 14).toFixed(1)}" text-anchor="middle" class="eco-q" fill="#fff">${esc(lo.l)}</text>`;
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">${s}</svg>`;
    $$(".eco-dotp", el).forEach(c => { const o = tr[+c.dataset.i], k = o.x >= 0 ? (o.y >= 0 ? "exp" : "des") : (o.y >= 0 ? "rec" : "con"); c.onmousemove = e => tip(`<b>${esc(I.nombre)}</b><div class="tp-s">${esc(o.l)}</div><div class="tp-v" style="color:${QD[k][1]}">${QD[k][0]}</div><div class="tp-r nu">Nivel vs tendencia: ${nf(o.x, 2)} σ</div><div class="tp-r nu">Impulso: ${nf(o.y, 2)}</div>`, e); c.onmouseleave = () => tip(null); });
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
  function cards(list) {
    return `<div class="eco-cards">${list.map((I, k) => `<button type="button" class="eco-card" data-go="i:${I.id}" style="--c:${I.col};animation-delay:${k * 40}ms"><div class="ct"><span>${esc(I.nombre)}</span>${dotE(I)}</div><div class="cg">${esc(I.grupo)}</div><div class="cv">${fv(I, I.last.v)}<small>${esc(I.u === "pts" || I.u === "%" ? "" : I.u)}</small></div>${chipY(I)}<div class="cs">${spark(I, 36, 220, 46)}</div><div class="cp">${esc(I.last.l)} · ${esc(I.frecuencia)}</div></button>`).join("")}</div>`;
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
  function mapaCiclo(el) {
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
    <div class="eco-panel"><div class="eco-ph"><h3>Evolución</h3><div class="eco-tabs" id="ecoMode"><button data-m="nivel" class="${S.serie.mode === "nivel" ? "on" : ""}">Nivel</button><button data-m="anual" class="${S.serie.mode === "anual" ? "on" : ""}">Variación anual</button></div><div class="eco-tabs" id="ecoRng">${rng.map(([t, m]) => `<button data-r="${m}" class="${S.serie.rango === m ? "on" : ""}">${t}</button>`).join("")}</div></div><div id="ecoMain"></div></div>
    <div class="eco-two"><div class="eco-panel"><div class="eco-ph"><h3>Reloj económico</h3><span class="eco-hint">trayectoria reciente: nivel vs tendencia e impulso</span></div><div id="ecoClock"></div></div>
    <div class="eco-panel"><div class="eco-ph"><h3>${I.ppy === 1 ? "Distribución histórica" : "Estacionalidad · año × " + (I.ppy === 12 ? "mes" : "trimestre")}</h3></div><div id="ecoEst"></div></div></div>
    <div class="eco-panel"><div class="eco-ph"><h3>Ficha del indicador</h3></div><dl class="eco-meta">${meta.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join("")}</dl></div>`;
  }
  function pintaInd(I) {
    const el = $("#ecoMain"); draws.push(() => drawSerie($("#ecoMain"), I), () => drawReloj($("#ecoClock"), I), () => drawEstac($("#ecoEst"), I));
    $("#ecoMode").onclick = e => { const b = e.target.closest("button"); if (!b) return; S.serie.mode = b.dataset.m; $$("#ecoMode button").forEach(x => x.classList.toggle("on", x === b)); drawSerie($("#ecoMain"), I); };
    $("#ecoRng").onclick = e => { const b = e.target.closest("button"); if (!b) return; S.serie.rango = +b.dataset.r; $$("#ecoRng button").forEach(x => x.classList.toggle("on", x === b)); drawSerie($("#ecoMain"), I); };
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

  /* ───────── navegación y árbol ───────── */
  function ir(id) {
    S.sel = id; ls.set("eco_sel", id); draws = []; tip(null);
    const main = $("#ecoMain2"); let h = "";
    if (id.startsWith("i:") && BYID[id.slice(2)]) { const I = BYID[id.slice(2)]; S.open.add("a:" + I.area); S.open.add("g:" + I.area + "|" + I.grupo); h = vistaInd(I); main.innerHTML = h; pintaInd(I); }
    else if (id.startsWith("g:")) { const [an, gn] = id.slice(2).split("|"), a = AREAS.find(x => x.n === an), g = a && a.g.find(x => x.n === gn); if (!g) return ir("home"); S.open.add("a:" + an); main.innerHTML = vistaPano(g.n, `<span class="eco-pill" style="--c:${a.col}">${esc(a.n)}</span><span class="eco-pill">${g.i.length} indicadores</span>`, g.i, `<a data-go="home">Panorama</a> › <a data-go="a:${esc(a.n)}">${esc(a.n)}</a>`); pintaPano(g.i); }
    else if (id.startsWith("a:")) { const a = AREAS.find(x => x.n === id.slice(2)); if (!a) return ir("home"); S.open.add(id); const l = a.g.flatMap(g => g.i); main.innerHTML = vistaPano(a.n, `<span class="eco-pill" style="--c:${a.col}">${a.g.length} grupos</span><span class="eco-pill">${l.length} indicadores</span>`, l, `<a data-go="home">Panorama</a>`); pintaPano(l); }
    else { S.sel = "home"; main.innerHTML = vistaHome(); draws.push(() => mapaCiclo($("#ecoMapa"))); }
    draws.forEach(f => f()); main.scrollTop = 0; arbol(); $("#ecoApp").classList.remove("eco-menu");
  }
  function arbol() {
    const q = norm(S.q.trim()), ws = q.split(/\s+/).filter(Boolean), pasa = I => !ws.length || ws.every(w => norm(I.nombre + " " + I.descripcion + " " + I.grupo + " " + I.area + " " + I.tema + " " + I.id).includes(w));
    let h = `<button type="button" class="eco-n eco-n0${S.sel === "home" ? " on" : ""}" data-go="home"><span class="ic">🌐</span><span class="tx">Panorama general</span><span class="bd">${IND.length}</span></button>`, any = false;
    AREAS.forEach(a => {
      const gs = a.g.map(g => ({ g, i: g.i.filter(pasa) })).filter(x => x.i.length); if (!gs.length) return; any = true;
      const op = q ? true : S.open.has("a:" + a.n), tot = gs.reduce((s, x) => s + x.i.length, 0);
      h += `<div class="eco-ar" style="--c:${a.col}"><div class="eco-n eco-n1${S.sel === "a:" + a.n ? " on" : ""}"><button type="button" class="tg" data-tg="a:${esc(a.n)}" aria-label="Desplegar">${op ? "▾" : "▸"}</button><button type="button" class="nm" data-go="a:${esc(a.n)}"><i></i><span class="tx">${esc(a.n)}</span><span class="bd">${tot}</span></button></div>`;
      if (op) gs.forEach(({ g, i }) => { const gid = "g:" + a.n + "|" + g.n, go = q ? true : S.open.has(gid); h += `<div class="eco-n eco-n2${S.sel === gid ? " on" : ""}"><button type="button" class="tg" data-tg="${esc(gid)}" aria-label="Desplegar">${go ? "▾" : "▸"}</button><button type="button" class="nm" data-go="${esc(gid)}"><span class="tx">${esc(g.n)}</span><span class="bd">${i.length}</span></button></div>`; if (go) i.forEach(I => { h += `<button type="button" class="eco-n eco-n3${S.sel === "i:" + I.id ? " on" : ""}" data-go="i:${I.id}" title="${esc(I.descripcion)}"><span class="tx">${esc(I.nombre)}</span>${dotE(I)}</button>`; }); });
      h += `</div>`;
    });
    if (!any) h += `<div class="eco-nada">Sin resultados para «${esc(S.q)}».</div>`;
    const t = $("#ecoTree"), sc = t.scrollTop; t.innerHTML = h; t.scrollTop = sc;
  }

  function shell() {
    host.innerHTML = `<div class="eco-app" id="ecoApp">
      <header class="eco-top"><button class="eco-burger" id="ecoBurger" type="button" aria-label="Menú">☰</button>
        <div class="eco-logo"><svg viewBox="0 0 48 48" width="40" height="40"><defs><linearGradient id="elg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#22d3ee"/><stop offset="1" stop-color="#a78bfa"/></linearGradient></defs><rect width="48" height="48" rx="13" fill="#0f172a" stroke="url(#elg)" stroke-width="2"/><path d="M9 33l8-9 6 5 8-12 8 6" fill="none" stroke="url(#elg)" stroke-width="3.2" stroke-linecap="round" stroke-linejoin="round"/><circle cx="39" cy="23" r="3" fill="#f472b6"/></svg></div>
        <div class="eco-brand"><b>EcoLab</b><span>Laboratorio económico · monitoreo de variables coyunturales</span></div><div class="eco-sp"></div>
        <span class="eco-chip" title="Datos descargados de la API del INEGI">Datos: ${esc((D.generado || "").slice(0, 10))}</span>
        <button class="eco-btn" id="ecoCambia" type="button">Cambiar de sistema</button><button class="eco-btn" id="ecoSalir" type="button">Salir</button></header>
      <div class="eco-body"><aside class="eco-side"><div class="eco-srch"><input id="ecoQ" type="search" placeholder="Buscar indicador, tema o ID…" autocomplete="off"></div><div class="eco-tree" id="ecoTree"></div><div class="eco-sf">Fuente: INEGI · Banco de Indicadores</div></aside>
      <main class="eco-main" id="ecoMain2"></main></div><div class="eco-ov" id="ecoOv"></div></div>`;
    tipEl = document.createElement("div"); tipEl.className = "eco-tip"; document.body.appendChild(tipEl);
    $("#ecoBurger", host).onclick = () => $("#ecoApp").classList.toggle("eco-menu"); $("#ecoOv", host).onclick = () => $("#ecoApp").classList.remove("eco-menu");
    $("#ecoSalir", host).onclick = () => window.DL_SALIR && window.DL_SALIR(); $("#ecoCambia", host).onclick = () => window.DL_CAMBIAR && window.DL_CAMBIAR();
    $("#ecoQ", host).oninput = e => { S.q = e.target.value; arbol(); };
    host.onclick = e => { const g = e.target.closest("[data-go]"), t = e.target.closest("[data-tg]"); if (t) { const k = t.dataset.tg; S.open.has(k) ? S.open.delete(k) : S.open.add(k); arbol(); } else if (g) ir(g.dataset.go); };
    let tm; addEventListener("resize", () => { clearTimeout(tm); tm = setTimeout(() => draws.forEach(f => f()), 120); });
  }
  window.ecolabInit = function () {
    host = document.getElementById("ecolab-host"); carga(); shell();
    let s = ls.get("eco_sel", "home"); if (s.startsWith("i:") && !BYID[s.slice(2)]) s = "home"; ir(s);
  };
})();
