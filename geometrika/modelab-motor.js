/* ============================================================================
 * modelab-motor.js — Motor de análisis estadístico y aprendizaje automático
 * JavaScript puro, sin dependencias. Corre en el navegador (window.MLMotor)
 * y en Node (module.exports).
 *
 * Secciones:
 *  1. Utilidades          2. Estadística y distribuciones   3. Álgebra lineal
 *  4. Lectura y perfilado 5. Catálogos y sugerencias        6. Preprocesamiento
 *  7. Modelos             8. Métricas y validación          9. No supervisados
 * 10. Textos y hallazgos 11. Orquestación (correr)          12. API pública
 * ========================================================================== */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.MLMotor = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  // ==========================================================================
  // 1. UTILIDADES
  // ==========================================================================

  /** Generador pseudoaleatorio reproducible (mulberry32). */
  function mulberry32(semilla) {
    let a = semilla >>> 0;
    return function () {
      a = (a + 0x6D2B79F5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  function crearRng(semilla) {
    const r = mulberry32(semilla == null ? 42 : semilla);
    return {
      next: r,
      int: function (n) { return Math.floor(r() * n); },
      normal: function () {
        let u = 0;
        while (u === 0) u = r();
        const v = r();
        return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
      },
      shuffle: function (a) {
        for (let i = a.length - 1; i > 0; i--) {
          const j = Math.floor(r() * (i + 1));
          const t = a[i]; a[i] = a[j]; a[j] = t;
        }
        return a;
      },
      // k índices distintos de 0..n-1
      muestra: function (n, k) {
        if (k >= n) { const t = []; for (let i = 0; i < n; i++) t.push(i); return t; }
        const a = new Int32Array(n);
        for (let i = 0; i < n; i++) a[i] = i;
        for (let i = 0; i < k; i++) {
          const j = i + Math.floor(r() * (n - i));
          const t = a[i]; a[i] = a[j]; a[j] = t;
        }
        return Array.prototype.slice.call(a.subarray(0, k));
      }
    };
  }

  // Cesión al navegador: cada ~100 ms de cómputo se devuelve el control
  // para que la interfaz pueda redibujarse.
  let _ultimoCeder = Date.now();
  function tk() {
    if (Date.now() - _ultimoCeder > 100) {
      return new Promise(function (r) {
        setTimeout(function () { _ultimoCeder = Date.now(); r(); }, 0);
      });
    }
    return null;
  }
  function cederYa() {
    return new Promise(function (r) {
      setTimeout(function () { _ultimoCeder = Date.now(); r(); }, 0);
    });
  }

  const TEXTOS_FALTANTES = { '': 1, 'na': 1, 'n/a': 1, 'nan': 1, 'null': 1, 'undefined': 1, 'nd': 1, 's/d': 1, '#n/a': 1 };

  function esFaltante(v) {
    if (v === null || v === undefined) return true;
    if (typeof v === 'number') return !isFinite(v);
    if (typeof v === 'string') return TEXTOS_FALTANTES[v.trim().toLowerCase()] === 1;
    return false;
  }

  /** Convierte un valor (número o texto como '1,234.5', '12%', ' 7 ') en número. NaN si no se puede. */
  function parseNum(v) {
    if (typeof v === 'number') return isFinite(v) ? v : NaN;
    if (typeof v === 'boolean' || v === null || v === undefined) return NaN;
    let s = String(v).replace(/[ \s]/g, '');
    if (s === '') return NaN;
    s = s.replace(/^\(([^)]+)\)$/, '-$1');
    s = s.replace(/\$|%|€|MXN|USD/gi, '');
    if (s === '') return NaN;
    if (/^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(s) || /^[+-]?\.\d+$/.test(s)) return Number(s);
    if (/^[+-]?\d{1,3}(,\d{3})+(\.\d+)?$/.test(s)) return Number(s.replace(/,/g, ''));
    if (/^[+-]?\d{1,3}(\.\d{3})+,\d+$/.test(s)) return Number(s.replace(/\./g, '').replace(',', '.'));
    if (/^[+-]?\d+,\d+$/.test(s)) return Number(s.replace(',', '.'));
    return NaN;
  }

  function limpiarTexto(v) { return String(v).replace(/\s+/g, ' ').trim(); }

  /** Formato numérico legible (miles con coma, decimal con punto). */
  function fmt(x, d) {
    if (x === null || x === undefined || typeof x !== 'number' || !isFinite(x)) return 'n/d';
    if (d == null) d = 2;
    const s = Math.abs(x).toFixed(d);
    const partes = s.split('.');
    partes[0] = partes[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    const neg = x < 0 && Number(s) !== 0;
    return (neg ? '-' : '') + partes.join('.');
  }
  function fpct(x, d) { return (x === null || x === undefined || !isFinite(x)) ? 'n/d' : fmt(100 * x, d == null ? 1 : d) + ' %'; }

  /** Reemplaza NaN/Infinity por null y convierte arreglos tipados en arreglos normales. */
  function limpiar(o) {
    if (o === null || o === undefined) return null;
    if (typeof o === 'number') return isFinite(o) ? o : null;
    if (typeof o === 'string' || typeof o === 'boolean') return o;
    if (ArrayBuffer.isView(o)) { const a = new Array(o.length); for (let i = 0; i < o.length; i++) a[i] = isFinite(o[i]) ? o[i] : null; return a; }
    if (Array.isArray(o)) { const a = new Array(o.length); for (let i = 0; i < o.length; i++) a[i] = limpiar(o[i]); return a; }
    if (typeof o === 'object') {
      const r = {};
      for (const k in o) if (Object.prototype.hasOwnProperty.call(o, k) && typeof o[k] !== 'function') r[k] = limpiar(o[k]);
      return r;
    }
    return null;
  }

  function sumaArr(a) { let s = 0; for (let i = 0; i < a.length; i++) s += a[i]; return s; }
  function maxIdx(a) { let m = 0; for (let i = 1; i < a.length; i++) if (a[i] > a[m]) m = i; return m; }
  function unicosArr(a) { return Array.from(new Set(a)); }

  // ==========================================================================
  // 2. ESTADÍSTICA Y DISTRIBUCIONES
  // ==========================================================================

  function media(a) { let s = 0; const n = a.length; for (let i = 0; i < n; i++) s += a[i]; return n ? s / n : NaN; }
  function desvio(a, m) {
    const n = a.length; if (n < 2) return NaN;
    if (m === undefined) m = media(a);
    let s = 0; for (let i = 0; i < n; i++) { const d = a[i] - m; s += d * d; }
    return Math.sqrt(s / (n - 1));
  }
  function ordenado(a) { const f = Float64Array.from(a); f.sort(); return f; }
  function cuantilOrd(s, q) {
    const n = s.length; if (!n) return NaN;
    const pos = (n - 1) * q, lo = Math.floor(pos), hi = Math.ceil(pos);
    return s[lo] + (s[hi] - s[lo]) * (pos - lo);
  }
  function mediana(a) { return cuantilOrd(ordenado(a), 0.5); }
  function sesgoCurtosis(a, m) {
    const n = a.length; if (n < 3) return { sesgo: 0, curtosis: 0 };
    let m2 = 0, m3 = 0, m4 = 0;
    for (let i = 0; i < n; i++) { const d = a[i] - m; const d2 = d * d; m2 += d2; m3 += d2 * d; m4 += d2 * d2; }
    m2 /= n; m3 /= n; m4 /= n;
    if (m2 < 1e-300) return { sesgo: 0, curtosis: 0 };
    return { sesgo: m3 / Math.pow(m2, 1.5), curtosis: m4 / (m2 * m2) - 3 };
  }
  /** Rangos promedio (empates comparten rango). */
  function rangos(a) {
    const n = a.length, idx = new Array(n);
    for (let i = 0; i < n; i++) idx[i] = i;
    idx.sort(function (i, j) { return a[i] - a[j]; });
    const r = new Float64Array(n);
    let i = 0;
    while (i < n) {
      let j = i;
      while (j + 1 < n && a[idx[j + 1]] === a[idx[i]]) j++;
      const rg = (i + j) / 2 + 1;
      for (let k = i; k <= j; k++) r[idx[k]] = rg;
      i = j + 1;
    }
    return r;
  }
  function pearson(a, b) {
    const n = a.length; if (n < 3) return NaN;
    const ma = media(a), mb = media(b);
    let sab = 0, saa = 0, sbb = 0;
    for (let i = 0; i < n; i++) { const x = a[i] - ma, y = b[i] - mb; sab += x * y; saa += x * x; sbb += y * y; }
    if (saa < 1e-300 || sbb < 1e-300) return NaN;
    return sab / Math.sqrt(saa * sbb);
  }

  // --- Funciones especiales -------------------------------------------------
  function lgamma(x) {
    const c = [76.18009172947146, -86.50532032941677, 24.01409824083091, -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5];
    let y = x, tmp = x + 5.5;
    tmp -= (x + 0.5) * Math.log(tmp);
    let ser = 1.000000000190015;
    for (let j = 0; j < 6; j++) ser += c[j] / ++y;
    return -tmp + Math.log(2.5066282746310005 * ser / x);
  }
  function betacf(a, b, x) {
    const FPMIN = 1e-300, qab = a + b, qap = a + 1, qam = a - 1;
    let c = 1, d = 1 - qab * x / qap;
    if (Math.abs(d) < FPMIN) d = FPMIN;
    d = 1 / d;
    let h = d;
    for (let m = 1; m <= 300; m++) {
      const m2 = 2 * m;
      let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
      d = 1 + aa * d; if (Math.abs(d) < FPMIN) d = FPMIN;
      c = 1 + aa / c; if (Math.abs(c) < FPMIN) c = FPMIN;
      d = 1 / d; h *= d * c;
      aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
      d = 1 + aa * d; if (Math.abs(d) < FPMIN) d = FPMIN;
      c = 1 + aa / c; if (Math.abs(c) < FPMIN) c = FPMIN;
      d = 1 / d;
      const del = d * c; h *= del;
      if (Math.abs(del - 1) < 3e-15) break;
    }
    return h;
  }
  /** Beta incompleta regularizada I_x(a,b). */
  function betainc(x, a, b) {
    if (x <= 0) return 0;
    if (x >= 1) return 1;
    const bt = Math.exp(lgamma(a + b) - lgamma(a) - lgamma(b) + a * Math.log(x) + b * Math.log(1 - x));
    if (x < (a + 1) / (a + b + 2)) return bt * betacf(a, b, x) / a;
    return 1 - bt * betacf(b, a, 1 - x) / b;
  }
  /** Gamma incompleta regularizada inferior P(a,x). */
  function gammaP(a, x) {
    if (x <= 0) return 0;
    if (x < a + 1) {
      let ap = a, suma = 1 / a, del = suma;
      for (let n = 0; n < 1000; n++) { ap++; del *= x / ap; suma += del; if (Math.abs(del) < Math.abs(suma) * 1e-16) break; }
      return suma * Math.exp(-x + a * Math.log(x) - lgamma(a));
    }
    let b = x + 1 - a, c = 1e300, d = 1 / b, h = d;
    for (let i = 1; i < 1000; i++) {
      const an = -i * (i - a);
      b += 2;
      d = an * d + b; if (Math.abs(d) < 1e-300) d = 1e-300;
      c = b + an / c; if (Math.abs(c) < 1e-300) c = 1e-300;
      d = 1 / d;
      const del = d * c; h *= del;
      if (Math.abs(del - 1) < 1e-15) break;
    }
    return 1 - Math.exp(-x + a * Math.log(x) - lgamma(a)) * h;
  }
  function normCDF(z) { const p = 0.5 * gammaP(0.5, z * z / 2); return z >= 0 ? 0.5 + p : 0.5 - p; }
  /** Cuantil de la normal estándar (Acklam). */
  function normInv(p) {
    if (p <= 0) return -Infinity; if (p >= 1) return Infinity;
    const a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00];
    const b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01];
    const c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00];
    const d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00];
    const pl = 0.02425;
    let q, r;
    if (p < pl) { q = Math.sqrt(-2 * Math.log(p)); return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
    if (p > 1 - pl) { q = Math.sqrt(-2 * Math.log(1 - p)); return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
    q = p - 0.5; r = q * q;
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
  }
  /** P(T <= t) para t de Student con df grados de libertad. */
  function tCDF(t, df) {
    if (!isFinite(t)) return t > 0 ? 1 : 0;
    const x = df / (df + t * t);
    const p = 0.5 * betainc(x, df / 2, 0.5);
    return t > 0 ? 1 - p : p;
  }
  /** p-valor de dos colas de un estadístico t. */
  function tPvalor(t, df) { if (!isFinite(t)) return 0; return betainc(df / (df + t * t), df / 2, 0.5); }
  function tInv(p, df) {
    if (p === 0.5) return 0;
    if (p < 0.5) return -tInv(1 - p, df);
    let lo = 0, hi = 10;
    while (tCDF(hi, df) < p && hi < 1e7) hi *= 2;
    for (let i = 0; i < 200; i++) { const m = (lo + hi) / 2; if (tCDF(m, df) < p) lo = m; else hi = m; }
    return (lo + hi) / 2;
  }
  function chi2CDF(x, k) { return x <= 0 ? 0 : gammaP(k / 2, x / 2); }
  function chi2Inv(p, k) {
    let lo = 0, hi = Math.max(10, k * 3);
    while (chi2CDF(hi, k) < p && hi < 1e9) hi *= 2;
    for (let i = 0; i < 200; i++) { const m = (lo + hi) / 2; if (chi2CDF(m, k) < p) lo = m; else hi = m; }
    return (lo + hi) / 2;
  }

  // ==========================================================================
  // 3. ÁLGEBRA LINEAL (matrices pequeñas como arreglos de Float64Array)
  // ==========================================================================

  function matriz(n, m) { const A = new Array(n); for (let i = 0; i < n; i++) A[i] = new Float64Array(m); return A; }

  function cholesky(A, n) {
    const L = matriz(n, n);
    for (let i = 0; i < n; i++) {
      for (let j = 0; j <= i; j++) {
        let s = A[i][j];
        for (let k = 0; k < j; k++) s -= L[i][k] * L[j][k];
        if (i === j) {
          if (!(s > 1e-14 * (Math.abs(A[i][i]) + 1e-300))) return null;
          L[i][i] = Math.sqrt(s);
        } else L[i][j] = s / L[j][j];
      }
    }
    return L;
  }
  function resolverChol(L, b, n) {
    const y = new Float64Array(n), x = new Float64Array(n);
    for (let i = 0; i < n; i++) { let s = b[i]; for (let k = 0; k < i; k++) s -= L[i][k] * y[k]; y[i] = s / L[i][i]; }
    for (let i = n - 1; i >= 0; i--) { let s = y[i]; for (let k = i + 1; k < n; k++) s -= L[k][i] * x[k]; x[i] = s / L[i][i]; }
    return x;
  }
  /** Inversa por Gauss-Jordan con pivoteo parcial. null si es singular. */
  function invertir(A, n) {
    const M = matriz(n, 2 * n);
    let escala = 0;
    for (let i = 0; i < n; i++) { for (let j = 0; j < n; j++) { M[i][j] = A[i][j]; escala = Math.max(escala, Math.abs(A[i][j])); } M[i][n + i] = 1; }
    for (let c = 0; c < n; c++) {
      let piv = c;
      for (let r = c + 1; r < n; r++) if (Math.abs(M[r][c]) > Math.abs(M[piv][c])) piv = r;
      if (Math.abs(M[piv][c]) <= 1e-13 * (escala + 1e-300)) return null;
      if (piv !== c) { const t = M[piv]; M[piv] = M[c]; M[c] = t; }
      const d = M[c][c];
      for (let j = 0; j < 2 * n; j++) M[c][j] /= d;
      for (let r = 0; r < n; r++) {
        if (r === c) continue;
        const f = M[r][c];
        if (f === 0) continue;
        for (let j = 0; j < 2 * n; j++) M[r][j] -= f * M[c][j];
      }
    }
    const R = matriz(n, n);
    for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) R[i][j] = M[i][n + j];
    return R;
  }
  /** Resuelve A x = b (A simétrica); si es casi singular agrega regularización mínima creciente. */
  function resolverSim(A, b, n) {
    let tr = 0; for (let i = 0; i < n; i++) tr += Math.abs(A[i][i]);
    tr = tr / Math.max(1, n) || 1;
    const saltos = [0, 1e-12, 1e-10, 1e-8, 1e-6, 1e-4, 1e-2];
    for (let s = 0; s < saltos.length; s++) {
      const B = matriz(n, n);
      for (let i = 0; i < n; i++) { for (let j = 0; j < n; j++) B[i][j] = A[i][j]; B[i][i] += saltos[s] * tr; }
      const L = cholesky(B, n);
      if (L) return { x: resolverChol(L, b, n), jitter: saltos[s] * tr };
    }
    return { x: new Float64Array(n), jitter: Infinity };
  }
  /** Autovalores/autovectores de una matriz simétrica (Jacobi cíclico). vecs[i][k] = componente i del vector k. */
  function jacobi(A0, n) {
    const a = matriz(n, n), v = matriz(n, n);
    for (let i = 0; i < n; i++) { for (let j = 0; j < n; j++) a[i][j] = A0[i][j]; v[i][i] = 1; }
    for (let barrido = 0; barrido < 60; barrido++) {
      let off = 0, diag = 0;
      for (let i = 0; i < n; i++) { diag += a[i][i] * a[i][i]; for (let j = i + 1; j < n; j++) off += a[i][j] * a[i][j]; }
      if (off <= 1e-24 * (diag + off) || off < 1e-300) break;
      for (let p = 0; p < n - 1; p++) {
        for (let q = p + 1; q < n; q++) {
          const apq = a[p][q];
          if (Math.abs(apq) < 1e-300) continue;
          const theta = (a[q][q] - a[p][p]) / (2 * apq);
          const t = (theta >= 0 ? 1 : -1) / (Math.abs(theta) + Math.sqrt(theta * theta + 1));
          const c = 1 / Math.sqrt(t * t + 1), s = t * c;
          for (let k = 0; k < n; k++) { const akp = a[k][p], akq = a[k][q]; a[k][p] = c * akp - s * akq; a[k][q] = s * akp + c * akq; }
          for (let k = 0; k < n; k++) { const apk = a[p][k], aqk = a[q][k]; a[p][k] = c * apk - s * aqk; a[q][k] = s * apk + c * aqk; }
          for (let k = 0; k < n; k++) { const vkp = v[k][p], vkq = v[k][q]; v[k][p] = c * vkp - s * vkq; v[k][q] = s * vkp + c * vkq; }
        }
      }
    }
    const orden = []; for (let i = 0; i < n; i++) orden.push(i);
    orden.sort(function (i, j) { return a[j][j] - a[i][i]; });
    const vals = new Float64Array(n), vecs = matriz(n, n);
    for (let k = 0; k < n; k++) { vals[k] = a[orden[k]][orden[k]]; for (let i = 0; i < n; i++) vecs[i][k] = v[i][orden[k]]; }
    return { vals: vals, vecs: vecs };
  }
  /** Covarianza de las filas indicadas (o todas) de X. */
  function covarianza(X, idx, p) {
    const n = idx ? idx.length : X.length;
    const mu = new Float64Array(p);
    for (let a = 0; a < n; a++) { const r = X[idx ? idx[a] : a]; for (let j = 0; j < p; j++) mu[j] += r[j]; }
    for (let j = 0; j < p; j++) mu[j] /= n;
    const C = matriz(p, p);
    for (let a = 0; a < n; a++) {
      const r = X[idx ? idx[a] : a];
      for (let i = 0; i < p; i++) { const di = r[i] - mu[i]; for (let j = 0; j <= i; j++) C[i][j] += di * (r[j] - mu[j]); }
    }
    const den = Math.max(1, n - 1);
    for (let i = 0; i < p; i++) for (let j = 0; j <= i; j++) { C[i][j] /= den; C[j][i] = C[i][j]; }
    return { mu: mu, C: C };
  }

  /** L-BFGS con búsqueda de línea por retroceso. fg(x,g) llena g y devuelve f. */
  function lbfgs(fg, x0, maxIter, tol) {
    const n = x0.length, m = 8;
    let x = Float64Array.from(x0), g = new Float64Array(n), f = fg(x, g);
    let S = [], Y = [], RHO = [];
    const dot = function (a, b) { let s = 0; for (let i = 0; i < n; i++) s += a[i] * b[i]; return s; };
    for (let it = 0; it < maxIter; it++) {
      let gmax = 0; for (let i = 0; i < n; i++) gmax = Math.max(gmax, Math.abs(g[i]));
      if (gmax < tol) break;
      const q = Float64Array.from(g), alfa = new Array(S.length);
      for (let i = S.length - 1; i >= 0; i--) { alfa[i] = RHO[i] * dot(S[i], q); for (let k = 0; k < n; k++) q[k] -= alfa[i] * Y[i][k]; }
      let gamma = 1;
      if (S.length) { const l = S.length - 1; gamma = dot(S[l], Y[l]) / dot(Y[l], Y[l]); }
      else gamma = 1 / Math.max(1, Math.sqrt(dot(g, g)));
      for (let k = 0; k < n; k++) q[k] *= gamma;
      for (let i = 0; i < S.length; i++) { const b = RHO[i] * dot(Y[i], q); for (let k = 0; k < n; k++) q[k] += S[i][k] * (alfa[i] - b); }
      let d = new Float64Array(n); for (let k = 0; k < n; k++) d[k] = -q[k];
      let dg = dot(d, g);
      if (!(dg < 0)) { for (let k = 0; k < n; k++) d[k] = -g[k]; dg = -dot(g, g); S = []; Y = []; RHO = []; }
      let t = 1, aceptado = false, xn = new Float64Array(n), gn = new Float64Array(n), fn = f;
      for (let ls = 0; ls < 40; ls++) {
        for (let k = 0; k < n; k++) xn[k] = x[k] + t * d[k];
        fn = fg(xn, gn);
        if (isFinite(fn) && fn <= f + 1e-4 * t * dg) { aceptado = true; break; }
        t *= 0.5;
      }
      if (!aceptado) break;
      const s = new Float64Array(n), y = new Float64Array(n);
      for (let k = 0; k < n; k++) { s[k] = xn[k] - x[k]; y[k] = gn[k] - g[k]; }
      const sy = dot(s, y);
      if (sy > 1e-12) { S.push(s); Y.push(y); RHO.push(1 / sy); if (S.length > m) { S.shift(); Y.shift(); RHO.shift(); } }
      const df = Math.abs(f - fn);
      x = Float64Array.from(xn); g = Float64Array.from(gn); f = fn;
      if (df < 1e-14 * (Math.abs(f) + 1e-14)) break;
    }
    return { x: x, f: f };
  }

  // ==========================================================================
  // 4. LECTURA DE DATOS Y PERFILADO
  // ==========================================================================

  /** Lee una columna del dataset: detecta si es numérica (≥90 % de valores convertibles) o categórica. */
  function leerColumna(ds, j) {
    const def = ds.cols[j] || {};
    const name = def.name != null ? String(def.name) : 'col' + j;
    const n = ds.rows.length;
    const num = new Float64Array(n), strs = new Array(n);
    let noFalt = 0, okNum = 0;
    for (let i = 0; i < n; i++) {
      const fila = ds.rows[i];
      const v = fila ? fila[j] : null;
      if (esFaltante(v)) { num[i] = NaN; strs[i] = null; continue; }
      noFalt++;
      const x = parseNum(v);
      num[i] = x;
      strs[i] = limpiarTexto(v);
      if (!isNaN(x)) okNum++;
    }
    const esNum = noFalt === 0 ? true : okNum >= 0.9 * noFalt;
    const col = { j: j, name: name, label: def.label || name, unit: def.unit || '', esNum: esNum, num: null, cat: null, noNumericos: 0 };
    if (esNum) {
      col.noNumericos = noFalt - okNum;
      col.num = num;
    } else {
      // Se unifican variantes de mayúsculas/espacios ('Norte' y 'norte' son la misma categoría).
      const formas = new Map();
      for (let i = 0; i < n; i++) {
        const s = strs[i]; if (s === null) continue;
        const k = s.toLowerCase();
        let m = formas.get(k); if (!m) { m = new Map(); formas.set(k, m); }
        m.set(s, (m.get(s) || 0) + 1);
      }
      const canon = new Map();
      formas.forEach(function (m, k) { let mejor = null, nm = -1; m.forEach(function (c, s) { if (c > nm) { nm = c; mejor = s; } }); canon.set(k, mejor); });
      col.cat = new Array(n);
      for (let i = 0; i < n; i++) col.cat[i] = strs[i] === null ? null : canon.get(strs[i].toLowerCase());
    }
    col.perfil = construirPerfil(col, n);
    return col;
  }

  function leerDataset(ds) {
    if (!ds || !Array.isArray(ds.cols) || !Array.isArray(ds.rows)) throw new Error('Los datos no tienen el formato esperado (se requieren "cols" y "rows").');
    const out = [];
    for (let j = 0; j < ds.cols.length; j++) out.push(leerColumna(ds, j));
    return out;
  }

  function construirPerfil(c, total) {
    const p = { name: c.name, label: c.label, unit: c.unit, filas: total, numerica: c.esNum };
    const nulos = { min: null, max: null, media: null, sd: null, mediana: null, q1: null, q3: null, sesgo: null, curtosis: null, outliersIQR: 0, outliersZ: 0 };
    if (c.esNum) {
      const vals = []; for (let i = 0; i < total; i++) if (!isNaN(c.num[i])) vals.push(c.num[i]);
      const n = vals.length;
      p.n = n; p.faltantes = total - n; p.pctFaltantes = total ? 100 * (total - n) / total : 0;
      if (c.noNumericos) p.noNumericos = c.noNumericos;
      if (!n) {
        Object.assign(p, nulos, { tipo: 'num', unicos: 0, constante: true, categorias: [], muestra: [] });
        return p;
      }
      const s = ordenado(vals);
      const runs = []; let entera = true;
      for (let i = 0; i < n; i++) {
        if (i === 0 || s[i] !== s[i - 1]) runs.push({ v: s[i], n: 1 }); else runs[runs.length - 1].n++;
        if (entera && !Number.isInteger(s[i])) entera = false;
      }
      const unicos = runs.length;
      const m = media(s), sd = n > 1 ? desvio(s, m) : 0;
      const q1 = cuantilOrd(s, 0.25), q3 = cuantilOrd(s, 0.75), iqr = q3 - q1;
      const sc = sesgoCurtosis(s, m);
      let oi = 0, oz = 0;
      for (let i = 0; i < n; i++) {
        if (s[i] < q1 - 1.5 * iqr || s[i] > q3 + 1.5 * iqr) oi++;
        if (sd > 0 && Math.abs((s[i] - m) / sd) > 3) oz++;
      }
      let tipo = 'num';
      if (unicos <= 2) tipo = 'bin';
      else if (entera && unicos <= 10 && n > 50) { tipo = 'cat'; p.ordinal = true; }
      else if (entera) tipo = 'int';
      const cats = runs.slice().sort(function (a, b) { return b.n - a.n; }).slice(0, 12).map(function (r) { return { v: r.v, n: r.n }; });
      const muestra = []; const vistos = new Set();
      for (let i = 0; i < total && muestra.length < 5; i++) { const v = c.num[i]; if (!isNaN(v) && !vistos.has(v)) { vistos.add(v); muestra.push(v); } }
      Object.assign(p, {
        tipo: tipo, unicos: unicos, min: s[0], max: s[n - 1], media: m, sd: sd, mediana: cuantilOrd(s, 0.5), q1: q1, q3: q3,
        sesgo: sc.sesgo, curtosis: sc.curtosis, outliersIQR: oi, outliersZ: oz, constante: unicos <= 1,
        categorias: (tipo === 'bin' || tipo === 'cat') ? cats : [], muestra: muestra
      });
      if (!p.ordinal) p.ordinal = false;
      p.entera = entera;
      return p;
    }
    // Categórica / texto
    const cuenta = new Map(); let n = 0;
    for (let i = 0; i < total; i++) { const s = c.cat[i]; if (s === null) continue; n++; cuenta.set(s, (cuenta.get(s) || 0) + 1); }
    const unicos = cuenta.size;
    p.n = n; p.faltantes = total - n; p.pctFaltantes = total ? 100 * (total - n) / total : 0;
    const lista = []; cuenta.forEach(function (k, v) { lista.push({ v: v, n: k }); });
    lista.sort(function (a, b) { return b.n - a.n; });
    let tipo = 'cat';
    if (unicos > 0.5 * n && unicos > 5) tipo = 'texto';
    else if (unicos <= 2) tipo = 'bin';
    const muestra = []; const vistos = new Set();
    for (let i = 0; i < total && muestra.length < 5; i++) { const v = c.cat[i]; if (v !== null && !vistos.has(v)) { vistos.add(v); muestra.push(v); } }
    Object.assign(p, nulos, { tipo: tipo, unicos: unicos, constante: unicos <= 1, ordinal: false, categorias: tipo === 'texto' ? [] : lista.slice(0, 12), muestra: muestra });
    return p;
  }

  /** Perfil por columna: tipo, faltantes, estadísticos, atípicos y categorías. */
  function perfilar(ds) { return leerDataset(ds).map(function (c) { return c.perfil; }); }

  // ==========================================================================
  // 5. CATÁLOGOS Y SUGERENCIAS (preprocesamiento y métodos)
  // ==========================================================================

  const catalogoPrep = {
    faltantes: [
      { id: 'eliminar_filas', nombre: 'Quitar filas con datos vacíos', desc: 'Se descartan las filas que tengan algún dato vacío. Es lo más limpio si son pocas filas.' },
      { id: 'media', nombre: 'Rellenar con el promedio', desc: 'Los vacíos numéricos se rellenan con el promedio del entrenamiento; en categorías, con la más frecuente.' },
      { id: 'mediana', nombre: 'Rellenar con la mediana', desc: 'Igual que el promedio pero más resistente a valores extremos.' },
      { id: 'moda', nombre: 'Rellenar con el valor más frecuente', desc: 'Se usa el valor que más se repite. Útil en variables de pocas opciones.' },
      { id: 'knn', nombre: 'Rellenar con casos parecidos', desc: 'Cada vacío se estima con el promedio de los 5 casos más parecidos. Más preciso, pero más lento.' },
      { id: 'constante_cero', nombre: 'Rellenar con cero / «(faltante)»', desc: 'Se pone 0 en numéricas y la categoría «(faltante)» en texto. Úsalo si vacío significa «no hay».' },
      { id: 'indicador_y_mediana', nombre: 'Mediana + columna que marca el vacío', desc: 'Se rellena con la mediana y se agrega una columna 0/1 que avisa dónde faltaba el dato (el faltante puede ser informativo).' }
    ],
    outliers: [
      { id: 'ninguno', nombre: 'No tocar valores extremos', desc: 'Se dejan los datos tal como vienen.' },
      { id: 'iqr_recortar', nombre: 'Recortar extremos (regla 1.5·IQR)', desc: 'Los valores muy alejados se llevan al límite de la regla del rango intercuartil (winsorizar).' },
      { id: 'z_recortar', nombre: 'Recortar a ±3 desviaciones', desc: 'Los valores a más de 3 desviaciones estándar del promedio se llevan a ese límite.' },
      { id: 'mad_recortar', nombre: 'Recortar con mediana y MAD', desc: 'Parecido al anterior pero usando mediana, así los propios extremos no distorsionan el límite.' },
      { id: 'eliminar_iqr', nombre: 'Quitar filas con extremos', desc: 'Se eliminan del entrenamiento las filas con algún valor fuera de la regla 1.5·IQR (la prueba no se toca).' }
    ],
    escalado: [
      { id: 'ninguno', nombre: 'Sin escalar', desc: 'Se conservan las unidades originales.' },
      { id: 'estandar', nombre: 'Estandarizar (promedio 0, desviación 1)', desc: 'Todas las variables quedan en la misma escala. Recomendado para la mayoría de métodos.' },
      { id: 'minmax', nombre: 'Llevar a rango 0–1', desc: 'El mínimo pasa a 0 y el máximo a 1.' },
      { id: 'robusto', nombre: 'Escala robusta (mediana y rango intercuartil)', desc: 'Como estandarizar, pero casi no le afectan los valores extremos.' }
    ],
    codificacion: [
      { id: 'onehot', nombre: 'Una columna 0/1 por categoría', desc: 'Cada categoría se vuelve una columna de sí/no (la más frecuente queda como referencia).' },
      { id: 'ordinal', nombre: 'Numerar las categorías', desc: 'Las categorías se reemplazan por 0, 1, 2… Solo tiene sentido si hay un orden natural.' },
      { id: 'frecuencia', nombre: 'Frecuencia de cada categoría', desc: 'Cada categoría se reemplaza por la proporción de casos que tiene. Útil cuando hay muchas categorías.' }
    ],
    transformacion: [
      { id: 'ninguna', nombre: 'Sin transformar', desc: 'No se cambia la forma de las variables.' },
      { id: 'log1p_si_sesgo', nombre: 'Logaritmo si está muy sesgada', desc: 'Las variables con cola larga hacia la derecha se comprimen con log(1+x).' },
      { id: 'raiz_si_sesgo', nombre: 'Raíz cuadrada si está muy sesgada', desc: 'Compresión más suave que el logaritmo para variables con cola larga.' },
      { id: 'yeo_si_sesgo', nombre: 'Yeo-Johnson si está muy sesgada', desc: 'Transformación automática que funciona también con ceros y negativos.' }
    ],
    duplicados: [
      { id: 'ninguno', nombre: 'Conservar filas repetidas', desc: 'No se quita nada.' },
      { id: 'eliminar', nombre: 'Quitar filas repetidas', desc: 'Se eliminan filas idénticas del entrenamiento para que no pesen doble.' }
    ],
    colineales: [
      { id: 'ninguno', nombre: 'Conservar todas', desc: 'No se quitan variables.' },
      { id: 'eliminar_r95', nombre: 'Quitar casi idénticas (r ≥ 0.95)', desc: 'Si dos variables dicen casi lo mismo, se deja solo una.' },
      { id: 'eliminar_r90', nombre: 'Quitar muy parecidas (r ≥ 0.90)', desc: 'Más estricto: también se quita una de cada par muy parecido.' }
    ],
    constantes: [
      { id: 'conservar', nombre: 'Conservar', desc: 'Se mantienen las columnas con un solo valor.' },
      { id: 'eliminar', nombre: 'Quitar columnas de un solo valor', desc: 'No aportan información, así que se descartan.' }
    ],
    formatos: [
      { id: 'siempre', nombre: 'Limpieza de formatos (siempre activa)', desc: 'Se quitan espacios, símbolos ($, %) y separadores de miles; se unifican mayúsculas en categorías.' }
    ],
    columnaFaltantes: [
      { id: 'conservar', nombre: 'Conservar todas las columnas', desc: 'No se descartan columnas por tener vacíos.' },
      { id: 'eliminar_si_>40%', nombre: 'Quitar si falta más del 40 %', desc: 'Una columna con más del 40 % vacío casi no informa.' },
      { id: 'eliminar_si_>60%', nombre: 'Quitar si falta más del 60 %', desc: 'Solo se quitan las columnas con más del 60 % vacío.' }
    ]
  };

  function idValido(cat, id) {
    const l = catalogoPrep[cat] || [];
    for (let i = 0; i < l.length; i++) if (l[i].id === id) return true;
    return false;
  }
  function nombreCat(cat, id) {
    const l = catalogoPrep[cat] || [];
    for (let i = 0; i < l.length; i++) if (l[i].id === id) return l[i].nombre;
    return id;
  }

  const ARBOLES = ['arbol_reg', 'bosque_reg', 'arbol_clf', 'bosque_clf', 'gbm_reg', 'gbm_clf'];
  const ESCALA_OBLIGADA = ['svm_reg', 'svm_clf', 'mlp_reg', 'mlp_clf'];

  /** Decisiones automáticas de preprocesamiento con su razón, a partir de los perfiles. */
  function sugerirPrep(perfiles, ctx) {
    ctx = ctx || {};
    perfiles = perfiles || [];
    const n = ctx.n || (perfiles[0] ? perfiles[0].filas : 0);
    let xs;
    if (ctx.x && ctx.x.length) xs = perfiles.filter(function (p) { return ctx.x.indexOf(p.name) >= 0; });
    else xs = perfiles.filter(function (p) { return p.name !== ctx.y; });
    const met = ctx.metodo || null;
    const esArbol = ARBOLES.indexOf(met) >= 0;
    const usables = xs.filter(function (p) { return p.tipo !== 'texto' && p.n > 0; });
    const continuas = usables.filter(function (p) { return p.numerica && p.tipo !== 'bin' && p.tipo !== 'cat'; });
    const numericas = usables.filter(function (p) { return p.numerica && p.tipo !== 'bin'; });
    const cats = usables.filter(function (p) { return !p.numerica; });
    const g = {}, porColumna = {}, resumen = [];
    const pc = function (name) { if (!porColumna[name]) porColumna[name] = {}; return porColumna[name]; };

    // --- Columnas con muchos faltantes
    const maxFalt = usables.reduce(function (m, p) { return Math.max(m, p.pctFaltantes); }, 0);
    if (maxFalt > 60) g.columnaFaltantes = { id: 'eliminar_si_>60%', razon: 'Hay columnas a las que les falta más del 60 % de los datos; no hay base suficiente para rellenarlas.' };
    else if (maxFalt > 40) g.columnaFaltantes = { id: 'eliminar_si_>40%', razon: 'Hay columnas a las que les falta más del 40 % de los datos; rellenarlas sería inventar casi la mitad de los valores.' };
    else g.columnaFaltantes = { id: 'conservar', razon: 'Ninguna columna tiene más de 40 % de datos vacíos, así que todas se conservan.' };

    // --- Faltantes
    const sumPct = usables.reduce(function (s, p) { return s + (p.pctFaltantes <= 40 ? p.pctFaltantes : 0); }, 0);
    const estFilas = Math.min(1, sumPct / 100);
    const algunSesgo = continuas.some(function (p) { return Math.abs(p.sesgo) > 1; });
    if (sumPct === 0) g.faltantes = { id: 'mediana', razon: 'No hay datos vacíos ahora; se deja la mediana por si llegan datos nuevos con vacíos.' };
    else if (estFilas < 0.03 && n >= 300) g.faltantes = { id: 'eliminar_filas', razon: 'Faltan pocos datos (cerca del ' + fmt(100 * estFilas, 1) + ' % de las filas) y hay muchas filas, así que quitarlas casi no cuesta información.' };
    else if (usables.some(function (p) { return p.pctFaltantes > 15 && p.pctFaltantes <= 40; })) g.faltantes = { id: 'indicador_y_mediana', razon: 'Hay columnas con entre 15 % y 40 % de vacíos: se rellenan con la mediana y se marca dónde faltaba, porque el hecho de faltar puede ser informativo.' };
    else if (numericas.length >= 3 && n >= 100 && n <= 20000) g.faltantes = { id: 'knn', razon: 'Hay varios datos vacíos y varias variables numéricas relacionadas; estimarlos con los casos más parecidos es más preciso que usar un promedio.' };
    else if (algunSesgo) g.faltantes = { id: 'mediana', razon: 'Hay vacíos y algunas variables tienen valores extremos; la mediana se afecta menos que el promedio.' };
    else g.faltantes = { id: 'media', razon: 'Hay pocos vacíos y las variables son bastante simétricas; el promedio es una buena estimación.' };
    cats.forEach(function (p) {
      if (p.faltantes > 0 && (g.faltantes.id === 'media' || g.faltantes.id === 'mediana')) pc(p.name).faltantes = 'moda';
      if (p.faltantes > 0) pc(p.name).razon = 'Es una variable de categorías: los vacíos se rellenan con la categoría más frecuente.';
    });

    // --- Transformación
    const sesgadas = continuas.filter(function (p) { return Math.abs(p.sesgo) > 1; });
    if (esArbol) g.transformacion = { id: 'ninguna', razon: 'Los árboles no se ven afectados por la forma de la distribución, así que no hace falta transformar.' };
    else if (!sesgadas.length) g.transformacion = { id: 'ninguna', razon: 'Ninguna variable tiene un sesgo marcado (todas son razonablemente simétricas).' };
    else if (sesgadas.every(function (p) { return p.min >= 0 && p.sesgo > 0; })) g.transformacion = { id: 'log1p_si_sesgo', razon: sesgadas.length + ' variable(s) tienen cola larga a la derecha (' + sesgadas.slice(0, 3).map(function (p) { return p.label; }).join(', ') + '); el logaritmo las vuelve más simétricas y mejora el ajuste.' };
    else g.transformacion = { id: 'yeo_si_sesgo', razon: sesgadas.length + ' variable(s) están muy sesgadas y algunas tienen ceros o negativos; Yeo-Johnson funciona en todos los casos.' };
    if (!esArbol) sesgadas.forEach(function (p) {
      pc(p.name).transformacion = (p.min >= 0 && p.sesgo > 0) ? 'log1p_si_sesgo' : 'yeo_si_sesgo';
      pc(p.name).razon = 'Sesgo de ' + fmt(p.sesgo, 1) + ' (|sesgo| > 1): distribución muy asimétrica.';
    });

    // --- Valores extremos
    const fuertes = continuas.filter(function (p) { return p.n && 100 * p.outliersIQR / p.n > 2; });
    const conZ = continuas.filter(function (p) { return p.outliersZ > 0; });
    if (met === 'anomalias') g.outliers = { id: 'ninguno', razon: 'Para detectar anomalías los valores extremos son justo lo que se busca; no se recortan.' };
    else if (esArbol) g.outliers = { id: 'ninguno', razon: 'Los árboles dividen por rangos y los valores extremos casi no les afectan.' };
    else if (fuertes.length && n >= 200) g.outliers = { id: 'iqr_recortar', razon: fuertes.length + ' variable(s) tienen más de 2 % de valores extremos (' + fuertes.slice(0, 3).map(function (p) { return p.label; }).join(', ') + '); se llevan al límite razonable para que no dominen el ajuste.' };
    else if (fuertes.length) g.outliers = { id: 'ninguno', razon: 'Hay valores extremos pero son pocos datos (' + n + ' filas); recortarlos quitaría información valiosa. Revísalos a mano.' };
    else if (conZ.length && n >= 100) g.outliers = { id: 'z_recortar', razon: 'Hay algunos valores a más de 3 desviaciones del promedio que podrían distorsionar el modelo; se llevan a ese límite.' };
    else g.outliers = { id: 'ninguno', razon: 'Hay muy pocos valores extremos (menos de 2 %); no se tocan.' };
    fuertes.forEach(function (p) { const q = pc(p.name); if (!q.outliers && g.outliers.id !== 'ninguno') q.outliers = g.outliers.id; });

    // --- Escalado
    if (esArbol) g.escalado = { id: 'ninguno', razon: 'Los árboles no dependen de la escala de las variables.' };
    else if (met === 'correlaciones') g.escalado = { id: 'ninguno', razon: 'La correlación no cambia con la escala.' };
    else if (ESCALA_OBLIGADA.indexOf(met) >= 0) g.escalado = { id: 'estandar', razon: 'Este método (vectores de soporte o red neuronal) se desajusta si unas variables tienen unidades mucho mayores que otras; poner todas en la misma escala es indispensable.' };
    else if (g.outliers.id === 'ninguno' && fuertes.length) g.escalado = { id: 'robusto', razon: 'Quedan valores extremos sin recortar; la escala robusta evita que distorsionen la comparación entre variables.' };
    else g.escalado = { id: 'estandar', razon: 'Pone todas las variables en la misma escala para que ninguna pese más solo por sus unidades (necesario en métodos basados en distancias o penalizaciones).' };

    // --- Codificación
    const maxUnic = cats.reduce(function (m, p) { return Math.max(m, p.unicos); }, 0);
    if (!cats.length) g.codificacion = { id: 'onehot', razon: 'No hay variables de categorías; se deja la opción estándar por si aparecen.' };
    else if (maxUnic > 15) g.codificacion = { id: 'frecuencia', razon: 'Hay variables con muchas categorías (hasta ' + maxUnic + '); una columna por categoría haría el modelo enorme, así que se usa su frecuencia.' };
    else g.codificacion = { id: 'onehot', razon: 'Las variables de categorías tienen pocas opciones; una columna sí/no por categoría no impone un orden falso.' };

    // --- Duplicados
    const todasBajas = usables.length > 0 && usables.every(function (p) { return p.tipo === 'bin' || p.tipo === 'cat' || (p.numerica && p.unicos <= 10); });
    if (met === 'anomalias') g.duplicados = { id: 'ninguno', razon: 'Para anomalías se conservan todas las filas.' };
    else if (todasBajas) g.duplicados = { id: 'ninguno', razon: 'Las variables tienen pocas opciones, así que filas idénticas pueden ser casos distintos; no se quitan.' };
    else g.duplicados = { id: 'eliminar', razon: 'Con variables continuas, dos filas idénticas casi siempre son un error de captura; quitarlas evita que pesen doble.' };

    // --- Colineales
    if (esArbol || met === 'pca' || met === 'correlaciones') g.colineales = { id: 'ninguno', razon: esArbol ? 'Los árboles no se confunden con variables repetidas.' : 'Este método necesita ver todas las variables, incluidas las parecidas.' };
    else if (numericas.length < 3) g.colineales = { id: 'ninguno', razon: 'Hay pocas variables numéricas; no hace falta revisar repeticiones.' };
    else g.colineales = { id: 'eliminar_r95', razon: 'Si dos variables dicen casi lo mismo (correlación ≥ 0.95), el modelo se vuelve inestable; se deja solo una de cada par.' };

    // --- Constantes
    g.constantes = { id: 'eliminar', razon: 'Una columna con un solo valor no distingue entre casos; se descarta.' };

    const orden = ['columnaFaltantes', 'faltantes', 'transformacion', 'outliers', 'duplicados', 'colineales', 'constantes', 'codificacion', 'escalado'];
    const rot = { columnaFaltantes: 'Columnas con vacíos', faltantes: 'Datos vacíos', transformacion: 'Transformación', outliers: 'Valores extremos', duplicados: 'Filas repetidas', colineales: 'Variables repetidas', constantes: 'Columnas constantes', codificacion: 'Categorías', escalado: 'Escala' };
    orden.forEach(function (k) { resumen.push(rot[k] + ': ' + nombreCat(k, g[k].id) + '. ' + g[k].razon); });
    return { global: g, porColumna: porColumna, resumen: resumen };
  }

  // --- Tabla de métodos ------------------------------------------------------
  const META = {
    ols: { nombre: 'Regresión lineal múltiple', familia: 'Modelos lineales', tipo: 'reg', min: 20, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Ajusta una recta (plano) que relaciona las variables con el número a predecir. Dice cuánto cambia el resultado por cada unidad de cada variable y si ese efecto es estadísticamente claro.',
      v: ['Muy fácil de explicar: cada variable tiene un coeficiente', 'Da p-valores e intervalos de confianza', 'Instantáneo'], l: ['Solo capta relaciones en línea recta', 'Sensible a variables repetidas y a valores extremos'] },
    ridge: { nombre: 'Regresión Ridge', familia: 'Modelos lineales', tipo: 'reg', min: 20, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Regresión lineal que «frena» los coeficientes grandes. Funciona bien cuando hay muchas variables o están muy relacionadas entre sí.',
      v: ['Estable con muchas variables o variables repetidas', 'Rápida'], l: ['Solo capta relaciones en línea recta', 'No da p-valores'] },
    lasso: { nombre: 'Regresión Lasso', familia: 'Modelos lineales', tipo: 'reg', min: 30, t: 'bajo', m: 'baja', et: 'segundos en 5 000 filas',
      desc: 'Regresión lineal que lleva a cero las variables poco útiles, dejando un modelo más corto.',
      v: ['Selecciona variables automáticamente', 'Modelo corto y fácil de leer'], l: ['Solo capta relaciones en línea recta', 'Entre variables parecidas elige una al azar'] },
    arbol_reg: { nombre: 'Árbol de decisión (regresión)', familia: 'Árboles', tipo: 'reg', min: 30, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Divide los datos con preguntas sí/no sobre las variables (por ejemplo «¿ingreso > 5 000?») hasta predecir el promedio de cada grupo.',
      v: ['Capta relaciones que no son rectas', 'No necesita escalar variables', 'Se puede explicar con reglas'], l: ['Un solo árbol es inestable: cambia mucho con pocos datos', 'Tiende a sobreajustar si es profundo'] },
    bosque_reg: { nombre: 'Bosque aleatorio (regresión)', familia: 'Árboles', tipo: 'reg', min: 50, t: 'medio', m: 'media', et: 'varios segundos en 5 000 filas',
      desc: 'Promedia cientos de árboles entrenados con variaciones de los datos. Suele ser muy preciso sin mucho ajuste.',
      v: ['Muy buena precisión general', 'Capta curvas e interacciones', 'Robusto a valores extremos'], l: ['Más difícil de explicar', 'No extrapola fuera del rango visto'] },
    knn_reg: { nombre: 'Vecinos más cercanos (regresión)', familia: 'Vecinos', tipo: 'reg', min: 30, t: 'medio', m: 'media', et: 'segundos en 5 000 filas',
      desc: 'Predice con el promedio de los casos más parecidos que ya conoce.',
      v: ['Muy intuitivo', 'No supone ninguna forma de relación'], l: ['Lento con muchos datos', 'Sufre con muchas variables', 'Exige variables en la misma escala'] },
    logistica: { nombre: 'Regresión logística', familia: 'Modelos lineales', tipo: 'clf', min: 30, t: 'bajo', m: 'baja', et: 'menos de 2 segundos en 5 000 filas',
      desc: 'Calcula la probabilidad de cada categoría a partir de las variables. Con dos categorías muestra cuánto sube o baja la probabilidad con cada variable (razones de momios).',
      v: ['Da probabilidades confiables', 'Fácil de explicar', 'Da p-valores (caso de dos categorías)'], l: ['Solo capta fronteras en línea recta', 'Sensible a variables repetidas'] },
    arbol_clf: { nombre: 'Árbol de decisión (clasificación)', familia: 'Árboles', tipo: 'clf', min: 30, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Clasifica con una secuencia de preguntas sí/no sobre las variables.',
      v: ['Se explica con reglas', 'No necesita escalar variables'], l: ['Inestable con pocos datos', 'Tiende a sobreajustar si es profundo'] },
    bosque_clf: { nombre: 'Bosque aleatorio (clasificación)', familia: 'Árboles', tipo: 'clf', min: 50, t: 'medio', m: 'media', et: 'varios segundos en 5 000 filas',
      desc: 'Muchos árboles votan por la categoría. Suele ser el método más preciso sin mucho ajuste.',
      v: ['Muy buena precisión general', 'Capta relaciones complejas', 'Robusto a valores extremos'], l: ['Más difícil de explicar', 'Más lento que los modelos lineales'] },
    knn_clf: { nombre: 'Vecinos más cercanos (clasificación)', familia: 'Vecinos', tipo: 'clf', min: 30, t: 'medio', m: 'media', et: 'segundos en 5 000 filas',
      desc: 'Asigna la categoría más común entre los casos más parecidos.',
      v: ['Muy intuitivo', 'Fronteras flexibles'], l: ['Lento con muchos datos', 'Sufre con muchas variables', 'Exige variables en la misma escala'] },
    naive_bayes: { nombre: 'Bayes ingenuo (gaussiano)', familia: 'Probabilísticos', tipo: 'clf', min: 20, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Supone que las variables son independientes dentro de cada categoría y calcula qué categoría es más probable.',
      v: ['Rapidísimo', 'Funciona con pocos datos'], l: ['Supone variables independientes (casi nunca es cierto)', 'Probabilidades poco calibradas'] },
    lda: { nombre: 'Análisis discriminante lineal', familia: 'Probabilísticos', tipo: 'clf', min: 30, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Busca la combinación de variables que mejor separa las categorías, suponiendo grupos de forma parecida.',
      v: ['Rápido y estable', 'Bueno cuando los grupos son compactos'], l: ['Supone que cada grupo tiene dispersión similar', 'No es ideal con muchas variables de sí/no'] },
    pca: { nombre: 'Componentes principales (PCA)', familia: 'Reducción de dimensiones', tipo: 'nosup', min: 10, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Resume muchas variables en unas pocas «dimensiones» que conservan la mayor parte de la información. Sirve para ver la estructura de los datos en un mapa.',
      v: ['Reduce muchas variables a 2 o 3 ejes', 'Muestra qué variables se mueven juntas'], l: ['Los ejes son combinaciones, menos intuitivos', 'Solo capta relaciones lineales'] },
    kmeans: { nombre: 'Grupos (k-medias)', familia: 'Agrupamiento', tipo: 'nosup', min: 30, t: 'bajo', m: 'baja', et: 'segundos en 5 000 filas',
      desc: 'Divide los casos en grupos parecidos entre sí. Elige automáticamente cuántos grupos conviene usar.',
      v: ['Rápido', 'Resultados fáciles de describir'], l: ['Hay que decidir el número de grupos', 'Forma grupos redondeados', 'Sensible a valores extremos'] },
    jerarquico: { nombre: 'Agrupamiento jerárquico (Ward)', familia: 'Agrupamiento', tipo: 'nosup', min: 20, t: 'medio', m: 'media', et: 'segundos (usa hasta 1 500 filas de muestra)',
      desc: 'Va uniendo los casos más parecidos paso a paso formando un árbol de grupos. Con muchas filas trabaja con una muestra y asigna el resto.',
      v: ['No exige fijar el número de grupos de antemano', 'Grupos compactos'], l: ['Lento con muchas filas (por eso usa muestra)'] },
    dbscan: { nombre: 'Grupos por densidad (DBSCAN)', familia: 'Agrupamiento', tipo: 'nosup', min: 50, t: 'medio', m: 'media', et: 'segundos en 5 000 filas',
      desc: 'Encuentra zonas donde hay muchos casos juntos y marca como «ruido» los casos aislados. Detecta grupos de cualquier forma.',
      v: ['Grupos de forma libre', 'Identifica casos aislados'], l: ['Sensible a la distancia elegida', 'Le cuesta con muchas variables'] },
    correlaciones: { nombre: 'Correlaciones entre variables', familia: 'Relaciones', tipo: 'nosup', min: 10, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Mide qué variables suben o bajan juntas (Pearson para relaciones en línea recta, Spearman para cualquier relación que vaya en un solo sentido).',
      v: ['Muy fácil de leer', 'Detecta variables repetidas'], l: ['Correlación no implica causa', 'Solo variables numéricas'] },
    anomalias: { nombre: 'Detección de casos anómalos', familia: 'Anomalías', tipo: 'nosup', min: 50, t: 'bajo', m: 'baja', et: 'menos de 1 segundo en 5 000 filas',
      desc: 'Marca los casos que se alejan mucho del patrón general, considerando varias variables a la vez (distancia de Mahalanobis robusta).',
      v: ['Considera combinaciones raras de variables', 'Resistente a que los propios extremos la distorsionen'], l: ['Supone una nube de puntos de una sola «masa»'] },
    svm_clf: { nombre: 'Máquina de vectores de soporte (clasificación)', familia: 'Vectores de soporte', tipo: 'clf', min: 100, t: 'medio', m: 'alta', et: 'de segundos a medio minuto; usa como máximo 3 000 filas de aprendizaje',
      desc: 'Traza la frontera que mejor separa las categorías dejando el mayor margen posible; con el núcleo curvo (RBF) puede dibujar fronteras muy flexibles. Entrega probabilidades mediante un ajuste adicional.',
      v: ['Capta fronteras curvas y complejas', 'Muy buena precisión con pocas o medianas cantidades de filas', 'Robusta cuando hay muchas variables'], l: ['Lenta y pesada en memoria con muchas filas (por eso se limita a 3 000 de aprendizaje)', 'Difícil de explicar: no da coeficientes simples', 'Exige variables en la misma escala y ajustar dos parámetros (C y gamma)'] },
    svm_reg: { nombre: 'Regresión por vectores de soporte', familia: 'Vectores de soporte', tipo: 'reg', min: 100, t: 'medio', m: 'alta', et: 'de segundos a medio minuto; usa como máximo 3 000 filas de aprendizaje',
      desc: 'Ajusta una curva suave que deja a los casos dentro de una «banda» de tolerancia; solo los casos fuera de la banda influyen en el resultado. Con el núcleo curvo (RBF) capta relaciones no lineales.',
      v: ['Capta relaciones curvas', 'Poco afectada por unos cuantos valores extremos', 'Buena precisión con filas moderadas'], l: ['Lenta y pesada en memoria con muchas filas (se limita a 3 000 de aprendizaje)', 'Difícil de explicar', 'Exige variables en la misma escala y ajustar tres parámetros (C, ε y gamma)'] },
    mlp_clf: { nombre: 'Red neuronal (clasificación)', familia: 'Redes neuronales', tipo: 'clf', min: 100, t: 'medio', m: 'media', et: 'segundos a decenas de segundos en 3 000 filas',
      desc: 'Una red de neuronas artificiales organizadas en capas aprende combinaciones complejas de las variables para asignar la categoría y calcula la probabilidad de cada una.',
      v: ['Capta relaciones muy complejas e interacciones entre variables', 'Funciona con muchas variables y categorías', 'Da probabilidades por categoría'], l: ['Necesita más datos que otros métodos', 'Difícil de explicar: es una «caja negra»', 'Puede sobreajustar (se frena con parada temprana)', 'Exige variables en la misma escala'] },
    mlp_reg: { nombre: 'Red neuronal (regresión)', familia: 'Redes neuronales', tipo: 'reg', min: 100, t: 'medio', m: 'media', et: 'segundos a decenas de segundos en 3 000 filas',
      desc: 'Una red de neuronas artificiales aprende combinaciones complejas de las variables para predecir un número, incluso con curvas e interacciones.',
      v: ['Capta relaciones muy complejas e interacciones entre variables', 'Funciona con muchas variables', 'Predice con curvas suaves'], l: ['Necesita más datos que otros métodos', 'Difícil de explicar: es una «caja negra»', 'Puede sobreajustar (se frena con parada temprana)', 'Exige variables en la misma escala'] },
    gbm_clf: { nombre: 'Boosting de árboles (clasificación)', familia: 'Árboles', tipo: 'clf', min: 200, t: 'medio', m: 'media', et: 'segundos en 3 000 filas',
      desc: 'Construye árboles pequeños uno tras otro, y cada uno corrige los errores de los anteriores. Suele ser de los métodos más precisos con datos de tabla.',
      v: ['Muy alta precisión en datos de tabla', 'Capta relaciones complejas sin preparar mucho los datos', 'No exige escalar variables ni es sensible a extremos'], l: ['Más difícil de explicar que un solo árbol', 'Puede sobreajustar con pocos datos (se frena con parada temprana)', 'Más lento que los modelos lineales'] },
    gbm_reg: { nombre: 'Boosting de árboles (regresión)', familia: 'Árboles', tipo: 'reg', min: 200, t: 'medio', m: 'media', et: 'segundos en 3 000 filas',
      desc: 'Construye árboles pequeños uno tras otro, y cada uno corrige los errores de los anteriores. Si detecta valores atípicos usa una pérdida robusta para que no lo desvíen.',
      v: ['Muy alta precisión en datos de tabla', 'Capta curvas e interacciones sin preparar mucho los datos', 'No exige escalar variables'], l: ['Más difícil de explicar que un solo árbol', 'No extrapola fuera del rango visto', 'Puede sobreajustar con pocos datos (se frena con parada temprana)'] }
  };
  const ID_SUP = ['ols', 'ridge', 'lasso', 'arbol_reg', 'bosque_reg', 'knn_reg', 'logistica', 'arbol_clf', 'bosque_clf', 'knn_clf', 'naive_bayes', 'lda', 'svm_clf', 'svm_reg', 'mlp_clf', 'mlp_reg', 'gbm_clf', 'gbm_reg'];
  const ID_NOSUP = ['pca', 'kmeans', 'jerarquico', 'dbscan', 'correlaciones', 'anomalias'];

  function tipoRespuestaDe(p) {
    if (!p) return { tipo: null, razon: 'Primero elige la variable que quieres predecir.' };
    if (p.tipo === 'texto') return { tipo: null, razon: '«' + p.label + '» es texto libre (casi todos los valores son distintos); no se puede predecir.' };
    if (p.constante || !p.n) return { tipo: null, razon: '«' + p.label + '» tiene un solo valor o está vacía; no hay nada que predecir.' };
    if (p.tipo === 'bin') return { tipo: 'binaria' };
    if (p.tipo === 'cat') {
      if (p.numerica && p.ordinal) return { tipo: 'multiclase', tambienRegresion: true };
      if (p.unicos > 30) return { tipo: null, razon: '«' + p.label + '» tiene demasiadas categorías (' + p.unicos + '); agrúpalas antes de predecirla.' };
      return { tipo: 'multiclase' };
    }
    return { tipo: 'regresion', poisson: p.tipo === 'int' && p.min >= 0 };
  }

  /** Métodos candidatos para el tipo de respuesta y mezcla de variables. */
  function sugerirMetodos(enfoque, perfilY, perfilesX, n) {
    const X = (perfilesX || []).filter(function (p) { return p && p.tipo !== 'texto' && !p.constante && p.n > 0; });
    if (n == null) n = (perfilY && perfilY.filas) || (X[0] && X[0].filas) || 0;
    const nNum = X.filter(function (p) { return p.numerica; }).length, nCat = X.length - nNum;
    let pEff = 0;
    X.forEach(function (p) { pEff += p.numerica ? 1 : Math.max(1, Math.min(p.unicos, 16) - 1); });
    const nosup = enfoque === 'nosup';
    const ids = nosup ? ID_NOSUP : ID_SUP;
    const info = nosup ? { tipo: 'nosup' } : tipoRespuestaDe(perfilY);
    const lista = [];
    const mezcla = nCat === 0 ? 'todas tus variables son numéricas' : (nNum === 0 ? 'todas tus variables son categorías' : 'mezclas variables numéricas y categorías');

    ids.forEach(function (id) {
      const M = META[id];
      let aplica = true, razon = '';
      let tipoResp = M.tipo === 'reg' ? 'regresion' : (M.tipo === 'clf' ? (info.tipo === 'multiclase' ? 'multiclase' : 'binaria') : 'nosup');
      if (!nosup && !X.length) { aplica = false; razon = 'Elige al menos una variable explicativa.'; }
      else if (nosup && (X.length < 2 && id !== 'anomalias') ) { aplica = false; razon = 'Se necesitan al menos 2 variables utilizables.'; }
      else if (nosup && X.length < 1) { aplica = false; razon = 'Elige al menos una variable.'; }
      else if (!nosup && !info.tipo) { aplica = false; razon = info.razon; }
      else if (!nosup && M.tipo === 'reg' && info.tipo !== 'regresion' && !info.tambienRegresion) { aplica = false; razon = 'Este método predice un número, pero «' + perfilY.label + '» son categorías. Usa un método de clasificación.'; }
      else if (!nosup && M.tipo === 'clf' && info.tipo === 'regresion') { aplica = false; razon = 'Este método predice categorías, pero «' + perfilY.label + '» es un número continuo. Usa un método de regresión.'; }
      else if (n < M.min) { aplica = false; razon = 'Se necesitan al menos ' + M.min + ' filas para este método y tienes ' + n + '.'; }
      else if (id === 'ols' && pEff >= n - 5) { aplica = false; razon = 'Tendrías más variables (≈' + pEff + ' columnas) que observaciones útiles (' + n + '); prueba Ridge o Lasso, que sí lo resisten.'; }
      else if (id === 'lda' && pEff + 3 >= n) { aplica = false; razon = 'Hay casi tantas variables (≈' + pEff + ') como filas (' + n + '); este método no puede estimar la dispersión.'; }
      else if (id === 'anomalias' && n < 5 * Math.max(1, pEff)) { aplica = false; razon = 'Para detectar anomalías con ' + pEff + ' variables se necesitan al menos ' + (5 * pEff) + ' filas.'; }
      else if (id === 'correlaciones' && nNum < 2) { aplica = false; razon = 'Se necesitan al menos 2 variables numéricas.'; }

      if (aplica) {
        if (id === 'ols') razon = 'Punto de partida claro y rápido: ' + mezcla + ' y hay suficientes filas para estimar cada efecto.';
        else if (id === 'ridge') razon = pEff * 10 > n ? 'Tienes muchas variables (≈' + pEff + ') para ' + n + ' filas; Ridge evita que el modelo se pase de ajustado.' : 'Alternativa lineal más estable si hay variables repetidas.';
        else if (id === 'lasso') razon = 'Útil para saber cuáles variables realmente importan: deja en cero las que no aportan.';
        else if (id === 'arbol_reg' || id === 'arbol_clf') razon = 'Sirve si sospechas que el efecto no es una recta (umbrales o saltos) y quieres reglas fáciles de contar.';
        else if (id === 'bosque_reg' || id === 'bosque_clf') razon = 'Suele ser el más preciso cuando la relación es compleja; ' + (nCat ? 'maneja bien tus variables de categorías.' : 'no pide preparar mucho los datos.');
        else if (id === 'knn_reg' || id === 'knn_clf') razon = pEff > 15 ? 'Funciona, pero con ' + pEff + ' variables los «vecinos» dejan de ser tan parecidos; úsalo como comparación.' : 'Útil como comparación: predice con los casos más parecidos.';
        else if (id === 'logistica') razon = (info.tipo === 'binaria' ? 'Dos resultados posibles: da la probabilidad y cuánto pesa cada variable.' : 'Varias categorías: da la probabilidad de cada una.') + ' Rápida y explicable.';
        else if (id === 'naive_bayes') razon = 'Muy rápido y estable aun con pocas filas; las probabilidades suelen ser menos finas.';
        else if (id === 'lda') razon = 'Buena opción si los grupos son compactos y de forma parecida.';
        else if (id === 'svm_clf' || id === 'svm_reg') razon = 'Útil si la relación es curva o las categorías no se separan con una línea recta' + (n > 3000 ? '; con ' + fmt(n, 0) + ' filas aprenderá solo de una muestra de 3 000 por velocidad.' : '.') + ' Compara con modelos más simples.';
        else if (id === 'mlp_clf' || id === 'mlp_reg') razon = n < 500 ? 'Capta relaciones muy complejas, pero con solo ' + n + ' filas una red puede sobreajustar; úsala como comparación.' : 'Capta relaciones muy complejas e interacciones entre variables; conviene si los modelos simples se quedan cortos.';
        else if (id === 'gbm_clf' || id === 'gbm_reg') razon = 'Suele dar de las mejores precisiones en datos de tabla' + (nCat ? ' y maneja bien tus variables de categorías' : '') + '; es la alternativa natural si el bosque aleatorio no basta.';
        else if (id === 'pca') razon = 'Resume tus ' + X.length + ' variables en pocos ejes y muestra cuáles se mueven juntas.';
        else if (id === 'kmeans') razon = 'Forma grupos de casos parecidos y calcula solo cuántos grupos conviene.';
        else if (id === 'jerarquico') razon = n > 1500 ? 'Mismo fin que k-medias, pero con ' + n + ' filas trabajará con una muestra de 1 500 y asignará el resto.' : 'Alternativa a k-medias que muestra cómo se van uniendo los grupos.';
        else if (id === 'dbscan') razon = 'Encuentra grupos de forma libre y separa los casos aislados como «ruido».';
        else if (id === 'correlaciones') razon = 'Primer vistazo: qué variables numéricas suben o bajan juntas.';
        else if (id === 'anomalias') razon = 'Señala los casos raros considerando todas las variables a la vez.';
        if (!nosup && M.tipo === 'reg' && info.poisson) razon += ' Nota: tu respuesta son conteos (enteros ≥ 0); aquí se trata como número continuo. Un modelo de Poisson sería una mejora futura.';
        if (!nosup && M.tipo === 'reg' && info.tambienRegresion) razon += ' Nota: «' + perfilY.label + '» tiene pocos valores enteros; también puede verse como categorías.';
        if (pEff * 10 > n && ['ols', 'logistica'].indexOf(id) >= 0) razon += ' Ojo: hay pocas filas por variable, los resultados pueden ser inestables.';
      }
      lista.push({
        id: id, nombre: M.nombre, familia: M.familia, aplica: aplica, recomendado: false, razon: razon, desc: M.desc,
        ventajas: M.v.slice(), limites: M.l.slice(), recursos: { tiempo: M.t, memoria: M.m, etiqueta: M.et },
        datosMinimos: M.min, tipoRespuesta: tipoResp
      });
    });

    // Un único recomendado: el más adecuado y barato.
    let rec = null;
    const ap = function (id) { for (let i = 0; i < lista.length; i++) if (lista[i].id === id && lista[i].aplica) return true; return false; };
    if (nosup) {
      if (pEff > 12 && ap('pca')) rec = 'pca'; else if (ap('kmeans')) rec = 'kmeans'; else if (ap('pca')) rec = 'pca'; else if (ap('correlaciones')) rec = 'correlaciones';
    } else if (info.tipo === 'regresion') {
      rec = (pEff * 10 > n && ap('ridge')) ? 'ridge' : (ap('ols') ? 'ols' : (ap('ridge') ? 'ridge' : null));
    } else if (info.tipo === 'binaria' || info.tipo === 'multiclase') {
      rec = (pEff * 5 > n && ap('naive_bayes')) ? 'naive_bayes' : (ap('logistica') ? 'logistica' : (ap('naive_bayes') ? 'naive_bayes' : null));
    }
    lista.forEach(function (m) { if (m.id === rec) m.recomendado = true; });
    return lista;
  }

  // ==========================================================================
  // 6. PREPROCESAMIENTO (todo se ajusta SOLO con las filas de entrenamiento)
  // ==========================================================================

  function faltaEn(c, r) { return c.esNum ? isNaN(c.num[r]) : c.cat[r] === null; }
  function esContinua(c) { return c.esNum && (c.perfil.tipo === 'num' || c.perfil.tipo === 'int'); }
  function modaDe(arr) {
    const m = new Map(); let mejor = null, nm = -1;
    for (let i = 0; i < arr.length; i++) m.set(arr[i], (m.get(arr[i]) || 0) + 1);
    m.forEach(function (c, v) { if (c > nm || (c === nm && v < mejor)) { nm = c; mejor = v; } });
    return mejor;
  }

  function yeoJohnson(x, lam) {
    if (x >= 0) return Math.abs(lam) < 1e-9 ? Math.log1p(x) : (Math.pow(x + 1, lam) - 1) / lam;
    return Math.abs(lam - 2) < 1e-9 ? -Math.log1p(-x) : -(Math.pow(1 - x, 2 - lam) - 1) / (2 - lam);
  }
  /** Lambda de Yeo-Johnson por máxima verosimilitud (búsqueda en rejilla + refinamiento). */
  function yeoLambda(vals) {
    let v = vals;
    if (v.length > 2000) { const w = new Float64Array(2000); for (let i = 0; i < 2000; i++) w[i] = vals[Math.floor(i * vals.length / 2000)]; v = w; }
    const n = v.length;
    let sl = 0; for (let i = 0; i < n; i++) sl += (v[i] >= 0 ? 1 : -1) * Math.log1p(Math.abs(v[i]));
    const ll = function (lam) {
      let s = 0, s2 = 0;
      for (let i = 0; i < n; i++) { const t = yeoJohnson(v[i], lam); s += t; s2 += t * t; }
      const m = s / n, va = s2 / n - m * m;
      return -n / 2 * Math.log(Math.max(va, 1e-300)) + (lam - 1) * sl;
    };
    let mejor = 1, bl = -Infinity;
    for (let lam = -2; lam <= 4.0001; lam += 0.25) { const l = ll(lam); if (isFinite(l) && l > bl) { bl = l; mejor = lam; } }
    const c = mejor;
    for (let lam = c - 0.25; lam <= c + 0.2501; lam += 0.05) { const l = ll(lam); if (isFinite(l) && l > bl) { bl = l; mejor = lam; } }
    return mejor;
  }
  function aplicarTrans(v, t) {
    if (t.t === 'log1p') return Math.log1p(Math.max(v, 0));
    if (t.t === 'raiz') return Math.sqrt(Math.max(v, 0));
    return yeoJohnson(v, t.lam);
  }
  function nombreTrans(t) { return t.t === 'log1p' ? 'logaritmo' : (t.t === 'raiz' ? 'raíz cuadrada' : 'Yeo-Johnson'); }

  /** Imputación por vecinos: se guarda una muestra de donantes del entrenamiento. */
  function prepararKNN(model, rows) {
    const feats = model.cm.filter(function (m) { return m.esNum; });
    const F = feats.length, mu = new Float64Array(F), sd = new Float64Array(F);
    feats.forEach(function (m, f) {
      const v = []; rows.forEach(function (r) { const x = m.col.num[r]; if (!isNaN(x)) v.push(x); });
      mu[f] = media(v); const s = desvio(v, mu[f]); sd[f] = s > 1e-12 ? s : 1;
    });
    const cols = model.cm.filter(function (m) { return m.knn; });
    const paso = Math.max(1, Math.floor(rows.length / 1500)), donantes = [];
    for (let a = 0; a < rows.length; a += paso) {
      const r = rows[a], z = new Float64Array(F), raw = new Float64Array(cols.length);
      for (let f = 0; f < F; f++) { const x = feats[f].col.num[r]; z[f] = isNaN(x) ? NaN : (x - mu[f]) / sd[f]; }
      for (let k = 0; k < cols.length; k++) raw[k] = cols[k].col.num[r];
      donantes.push({ z: z, raw: raw });
    }
    cols.forEach(function (m, k) { m.knnPos = k; });
    model.knn = { feats: feats, mu: mu, sd: sd, donantes: donantes, cols: cols };
  }
  function imputarKNN(model, filas) {
    const K = model.knn, F = K.feats.length, nk = K.cols.length, mapa = new Map();
    const NB = 20, bd = new Float64Array(NB), bi = new Int32Array(NB), q = new Float64Array(F);
    for (let a = 0; a < filas.length; a++) {
      const r = filas[a];
      let falta = false;
      for (let k = 0; k < nk; k++) if (isNaN(K.cols[k].col.num[r])) { falta = true; break; }
      if (!falta) continue;
      for (let f = 0; f < F; f++) { const x = K.feats[f].col.num[r]; q[f] = isNaN(x) ? NaN : (x - K.mu[f]) / K.sd[f]; }
      bd.fill(Infinity); bi.fill(-1);
      for (let d = 0; d < K.donantes.length; d++) {
        const z = K.donantes[d].z; let acc = 0, cnt = 0;
        for (let f = 0; f < F; f++) { const u = q[f], w = z[f]; if (u === u && w === w) { const t = u - w; acc += t * t; cnt++; } }
        if (!cnt) continue;
        const dist = acc / cnt;
        if (dist < bd[NB - 1]) {
          let i = NB - 1;
          while (i > 0 && bd[i - 1] > dist) { bd[i] = bd[i - 1]; bi[i] = bi[i - 1]; i--; }
          bd[i] = dist; bi[i] = d;
        }
      }
      const fill = new Float64Array(nk).fill(NaN);
      for (let k = 0; k < nk; k++) {
        if (!isNaN(K.cols[k].col.num[r])) continue;
        let s = 0, c = 0;
        for (let i = 0; i < NB && c < 5; i++) { if (bi[i] < 0) break; const v = K.donantes[bi[i]].raw[k]; if (!isNaN(v)) { s += v; c++; } }
        if (c) fill[k] = s / c;
      }
      mapa.set(r, fill);
    }
    return mapa;
  }

  /** Construye la matriz (sin quitar columnas ni escalar) para las filas dadas. */
  function generarX(model, filas, knnMapa) {
    const P = model.outAll.length, X = new Array(filas.length), cm = model.cm;
    for (let a = 0; a < filas.length; a++) {
      const r = filas[a], row = new Float64Array(P), fill = knnMapa ? knnMapa.get(r) : null;
      for (let q = 0; q < cm.length; q++) {
        const m = cm[q];
        if (m.esNum) {
          let v = m.col.num[r];
          const falta = isNaN(v);
          if (falta) { v = (fill && m.knn && !isNaN(fill[m.knnPos])) ? fill[m.knnPos] : m.imp; }
          if (m.indIdx >= 0) row[m.indIdx] = falta ? 1 : 0;
          if (m.trans) v = aplicarTrans(v, m.trans);
          if (m.clip) v = v < m.clip[0] ? m.clip[0] : (v > m.clip[1] ? m.clip[1] : v);
          row[m.idx] = v;
        } else {
          let s = m.col.cat[r];
          const falta = s === null;
          if (falta) s = m.imp;
          if (m.indIdx >= 0) row[m.indIdx] = falta ? 1 : 0;
          let l = m.nivelIdx.get(s);
          if (l === undefined) l = m.otros !== undefined ? m.otros : -1;
          if (m.codif === 'onehot') { if (l > 0) row[m.dumIdx[l]] = 1; }
          else if (m.codif === 'ordinal') row[m.idx] = l >= 0 ? m.codigos[l] : m.codMed;
          else row[m.idx] = l >= 0 ? m.frec[l] : 0;
        }
      }
      X[a] = row;
    }
    return X;
  }

  /**
   * Ajusta el preprocesamiento con las filas `tr`. Devuelve el modelo de preprocesamiento,
   * las filas de entrenamiento realmente usadas, los pasos aplicados y las columnas descartadas.
   * cols: columnas leídas (leerColumna). op: {g, pc, yVals}
   */
  function ajustarPrep(cols, tr, op) {
    const g = op.g, pc = op.pc || {};
    const pasos = [], descartadas = [];
    let rows = Array.prototype.slice.call(tr);
    const nIni = rows.length;
    const modoF = function (c) { const m = pc[c.name] && pc[c.name].faltantes; return (m && idValido('faltantes', m)) ? m : g.faltantes; };
    const modoO = function (c) { const m = pc[c.name] && pc[c.name].outliers; return (m && idValido('outliers', m)) ? m : g.outliers; };
    const modoT = function (c) { const m = pc[c.name] && pc[c.name].transformacion; return (m && idValido('transformacion', m)) ? m : g.transformacion; };

    // 1) Columnas inutilizables
    const act = [];
    const umbral = g.columnaFaltantes === 'eliminar_si_>60%' ? 60 : (g.columnaFaltantes === 'eliminar_si_>40%' ? 40 : 101);
    for (let q = 0; q < cols.length; q++) {
      const c = cols[q];
      if (c.perfil.tipo === 'texto') { descartadas.push({ name: c.name, razon: 'Es texto libre (casi todos los valores son distintos); no sirve como variable.' }); continue; }
      let obs = 0; const uni = new Set();
      for (let a = 0; a < rows.length; a++) { const r = rows[a]; if (!faltaEn(c, r)) { obs++; if (uni.size < 3) uni.add(c.esNum ? c.num[r] : c.cat[r]); } }
      const pct = rows.length ? 100 * (rows.length - obs) / rows.length : 100;
      if (obs === 0) { descartadas.push({ name: c.name, razon: 'Está vacía en los datos de entrenamiento.' }); continue; }
      if (pct > umbral) { descartadas.push({ name: c.name, razon: 'Le falta el ' + fmt(pct, 0) + ' % de los datos (más del límite de ' + umbral + ' %).' }); continue; }
      if (g.constantes === 'eliminar' && uni.size <= 1) { descartadas.push({ name: c.name, razon: 'Tiene un solo valor, no distingue entre casos.' }); continue; }
      act.push(c);
    }
    if (!act.length) throw new Error('Después de la limpieza no queda ninguna variable utilizable (se descartaron todas por ser texto libre, estar vacías, tener un solo valor o demasiados faltantes).');
    if (descartadas.length) pasos.push('Columnas descartadas: ' + descartadas.length + ' (' + descartadas.slice(0, 6).map(function (d) { return d.name; }).join(', ') + (descartadas.length > 6 ? '…' : '') + ').');

    // 2) Filas repetidas (solo entrenamiento)
    if (g.duplicados === 'eliminar') {
      const vistos = new Set(), keep = [];
      for (let a = 0; a < rows.length; a++) {
        const r = rows[a]; let k = '';
        for (let q = 0; q < act.length; q++) k += (act[q].esNum ? act[q].num[r] : act[q].cat[r]) + '|';
        if (op.yVals) k += op.yVals[r];
        if (!vistos.has(k)) { vistos.add(k); keep.push(r); }
      }
      pasos.push(keep.length < rows.length ? 'Filas repetidas: se quitaron ' + (rows.length - keep.length) + ' filas del entrenamiento.' : 'Filas repetidas: no se encontró ninguna.');
      rows = keep;
    }

    // 3) Filas con datos vacíos (columnas con ese modo)
    const colsDropNA = act.filter(function (c) { return modoF(c) === 'eliminar_filas'; });
    if (colsDropNA.length) {
      const keep = rows.filter(function (r) { return !colsDropNA.some(function (c) { return faltaEn(c, r); }); });
      pasos.push('Datos vacíos: se quitaron ' + (rows.length - keep.length) + ' filas del entrenamiento que tenían celdas vacías' + (colsDropNA.length < act.length ? ' en ' + colsDropNA.length + ' columna(s)' : '') + '.');
      rows = keep;
    }

    // 3b) Filas con extremos (solo entrenamiento)
    const colsEIQR = act.filter(function (c) { return esContinua(c) && modoO(c) === 'eliminar_iqr'; });
    if (colsEIQR.length) {
      const lim = colsEIQR.map(function (c) {
        const v = []; rows.forEach(function (r) { const x = c.num[r]; if (!isNaN(x)) v.push(x); });
        const s = ordenado(v), q1 = cuantilOrd(s, .25), q3 = cuantilOrd(s, .75), i = q3 - q1;
        return [q1 - 1.5 * i, q3 + 1.5 * i];
      });
      const keep = rows.filter(function (r) { return colsEIQR.every(function (c, k) { const x = c.num[r]; return isNaN(x) || lim[k][0] === lim[k][1] || (x >= lim[k][0] && x <= lim[k][1]); }); });
      pasos.push('Valores extremos: se quitaron ' + (rows.length - keep.length) + ' filas del entrenamiento con valores fuera de la regla 1.5·IQR (la prueba no se modifica).');
      rows = keep;
    }
    if (rows.length < 10) throw new Error('Después de limpiar solo quedan ' + rows.length + ' filas de entrenamiento; son muy pocas para ajustar un modelo. Prueba rellenar los datos vacíos en lugar de quitar filas.');

    // 4) Modelos por columna: imputación
    const cm = [], resImp = {};
    act.forEach(function (c) {
      const modo = modoF(c);
      const m = { name: c.name, label: c.label, col: c, esNum: c.esNum, tipo: c.perfil.tipo, modoF: modo, nNA: 0, indicador: false, knn: false, trans: null, clip: null, indIdx: -1 };
      let nNA = 0;
      if (c.esNum) {
        const v = []; rows.forEach(function (r) { const x = c.num[r]; if (isNaN(x)) nNA++; else v.push(x); });
        const s = ordenado(v);
        const med = cuantilOrd(s, 0.5), mu = media(v);
        if (modo === 'media') m.imp = mu; else if (modo === 'moda') m.imp = modaDe(v); else if (modo === 'constante_cero') m.imp = 0; else m.imp = med;
        m.knn = modo === 'knn' && nNA > 0;
      } else {
        const v = []; rows.forEach(function (r) { const x = c.cat[r]; if (x === null) nNA++; else v.push(x); });
        m.imp = modo === 'constante_cero' ? '(faltante)' : modaDe(v);
      }
      m.nNA = nNA;
      m.indicador = modo === 'indicador_y_mediana' && nNA > 0;
      if (nNA > 0 && modo !== 'eliminar_filas') { const k = modo; if (!resImp[k]) resImp[k] = { n: 0, cols: [] }; resImp[k].n += nNA; resImp[k].cols.push(c.name); }
      cm.push(m);
    });
    const nombresModo = { media: 'el promedio', mediana: 'la mediana', moda: 'el valor más frecuente', knn: 'el promedio de los 5 casos más parecidos (categorías: la más frecuente)', constante_cero: 'cero / «(faltante)»', indicador_y_mediana: 'la mediana y se agregó una columna que marca dónde faltaba el dato' };
    Object.keys(resImp).forEach(function (k) {
      pasos.push('Datos vacíos: se rellenaron ' + resImp[k].n + ' celdas del entrenamiento con ' + (nombresModo[k] || k) + ' (' + resImp[k].cols.slice(0, 5).join(', ') + (resImp[k].cols.length > 5 ? '…' : '') + ').');
    });
    if (!Object.keys(resImp).length && !colsDropNA.length) pasos.push('Datos vacíos: no había celdas vacías en el entrenamiento (se guardó el relleno por si aparecen en la prueba).');

    // 5) Transformación y recorte de extremos (sobre valores ya rellenados)
    const trans = [], recortes = [];
    cm.forEach(function (m) {
      if (!(m.esNum && esContinua(m.col))) return;
      const v = new Float64Array(rows.length);
      for (let a = 0; a < rows.length; a++) { const x = m.col.num[rows[a]]; v[a] = isNaN(x) ? m.imp : x; }
      const mt = modoT(m.col);
      if (mt !== 'ninguna') {
        let mn = Infinity; for (let a = 0; a < v.length; a++) if (v[a] < mn) mn = v[a];
        const sc = sesgoCurtosis(v, media(v));
        if (Math.abs(sc.sesgo) > 1) {
          if (mt === 'log1p_si_sesgo' && sc.sesgo > 0 && mn >= 0) m.trans = { t: 'log1p' };
          else if (mt === 'raiz_si_sesgo' && sc.sesgo > 0 && mn >= 0) m.trans = { t: 'raiz' };
          else m.trans = { t: 'yeo', lam: yeoLambda(v) };
          for (let a = 0; a < v.length; a++) v[a] = aplicarTrans(v[a], m.trans);
          trans.push(m.name + ' (' + nombreTrans(m.trans) + ')');
        }
      }
      const mo = modoO(m.col);
      if (mo === 'iqr_recortar' || mo === 'z_recortar' || mo === 'mad_recortar') {
        const s = ordenado(v);
        let lo, hi;
        if (mo === 'iqr_recortar') { const q1 = cuantilOrd(s, .25), q3 = cuantilOrd(s, .75), i = q3 - q1; lo = q1 - 1.5 * i; hi = q3 + 1.5 * i; }
        else if (mo === 'z_recortar') { const mu = media(v), sd = desvio(v, mu); lo = mu - 3 * sd; hi = mu + 3 * sd; }
        else { const med = cuantilOrd(s, .5), d = new Float64Array(v.length); for (let a = 0; a < v.length; a++) d[a] = Math.abs(v[a] - med); const mad = mediana(d); lo = med - 5.189 * mad; hi = med + 5.189 * mad; }
        if (isFinite(lo) && isFinite(hi) && hi > lo) {
          let nc = 0; for (let a = 0; a < v.length; a++) if (v[a] < lo || v[a] > hi) nc++;
          m.clip = [lo, hi];
          if (nc) recortes.push({ name: m.name, n: nc });
        }
      }
    });
    if (trans.length) pasos.push('Transformación: se corrigió el sesgo de ' + trans.slice(0, 6).join(', ') + (trans.length > 6 ? '…' : '') + '.');
    if (recortes.length) {
      const tot = recortes.reduce(function (s, r) { return s + r.n; }, 0);
      pasos.push('Valores extremos: se recortaron ' + tot + ' celdas al límite (' + nombreCat('outliers', g.outliers).toLowerCase() + ') en ' + recortes.slice(0, 5).map(function (r) { return r.name + ' (' + r.n + ')'; }).join(', ') + (recortes.length > 5 ? '…' : '') + '.');
    }

    // 6) Columnas de salida (codificación de categorías)
    const outAll = [];
    const model = { cm: cm, outAll: outAll, colsDropNA: colsDropNA, knn: null, keep: null, esc: null, outCols: null };
    const codifs = [];
    cm.forEach(function (m) {
      if (m.esNum) {
        m.idx = outAll.length;
        outAll.push({ nombre: m.name, label: m.label, origen: m.name, esNum: true, tipoOut: m.tipo === 'bin' ? 'bin' : 'num', escalable: m.tipo !== 'bin' });
      } else {
        const cuenta = new Map();
        rows.forEach(function (r) { let s = m.col.cat[r]; if (s === null) s = m.imp; cuenta.set(s, (cuenta.get(s) || 0) + 1); });
        let lv = Array.from(cuenta.entries()).sort(function (a, b) { return b[1] - a[1] || (a[0] < b[0] ? -1 : 1); });
        let hayOtros = false, sumFrec = 0;
        if (lv.length > 15) { lv = lv.slice(0, 15); hayOtros = true; }
        m.niveles = lv.map(function (e) { return e[0]; });
        m.frec = lv.map(function (e) { sumFrec += e[1] / rows.length; return e[1] / rows.length; });
        m.nivelIdx = new Map(); m.niveles.forEach(function (s, i) { m.nivelIdx.set(s, i); });
        if (hayOtros) { m.niveles.push('(otros)'); m.frec.push(Math.max(0, 1 - sumFrec)); m.otros = m.niveles.length - 1; }
        m.codif = g.codificacion;
        if (m.codif === 'onehot') {
          m.dumIdx = [];
          for (let l = 1; l < m.niveles.length; l++) {
            m.dumIdx[l] = outAll.length;
            outAll.push({ nombre: m.name + '=' + m.niveles[l], label: m.label + ': ' + m.niveles[l], origen: m.name, esNum: false, tipoOut: 'dummy', escalable: false });
          }
          codifs.push(m.name + ' → ' + Math.max(0, m.niveles.length - 1) + ' columna(s) 0/1');
        } else {
          if (m.codif === 'ordinal') {
            const todasNum = m.niveles.every(function (s) { return s === '(otros)' || !isNaN(parseNum(s)); });
            const ord = m.niveles.map(function (s, i) { return i; }).filter(function (i) { return m.niveles[i] !== '(otros)'; });
            ord.sort(function (a, b) { return todasNum ? parseNum(m.niveles[a]) - parseNum(m.niveles[b]) : (m.niveles[a] < m.niveles[b] ? -1 : 1); });
            m.codigos = new Float64Array(m.niveles.length);
            ord.forEach(function (i, k) { m.codigos[i] = k; });
            m.codMed = (ord.length - 1) / 2;
            if (m.otros !== undefined) m.codigos[m.otros] = m.codMed;
          }
          m.idx = outAll.length;
          outAll.push({ nombre: m.name, label: m.label, origen: m.name, esNum: false, tipoOut: m.codif === 'ordinal' ? 'ord' : 'frec', escalable: true });
          codifs.push(m.name + ' → ' + (m.codif === 'ordinal' ? 'números 0, 1, 2…' : 'frecuencia'));
        }
      }
      if (m.indicador) {
        m.indIdx = outAll.length;
        outAll.push({ nombre: m.name + '__faltante', label: m.label + ' (dato faltante)', origen: m.name, esNum: false, tipoOut: 'ind', escalable: false });
      }
    });
    if (codifs.length) pasos.push('Categorías: ' + codifs.slice(0, 6).join('; ') + (codifs.length > 6 ? '…' : '') + '.');
    if (cm.some(function (m) { return m.knn; })) prepararKNN(model, rows);

    // 7) Matriz de entrenamiento sin escalar → variables repetidas → escala
    const P = outAll.length;
    const keepTodo = []; for (let j = 0; j < P; j++) keepTodo.push(j);
    model.keep = keepTodo; model.esc = { c: new Float64Array(P), s: new Float64Array(P).fill(1) };
    const knnTr = model.knn ? imputarKNN(model, rows) : null;
    const Xall = generarX(model, rows, knnTr);
    const n = rows.length;
    let keep = keepTodo.slice();
    const thr = g.colineales === 'eliminar_r95' ? 0.95 : (g.colineales === 'eliminar_r90' ? 0.90 : 0);
    if (thr > 0 && P > 1 && P <= 300) {
      const Z = [], sd = [];
      for (let j = 0; j < P; j++) {
        let s = 0; for (let a = 0; a < n; a++) s += Xall[a][j];
        const mu = s / n; let s2 = 0;
        const z = new Float64Array(n);
        for (let a = 0; a < n; a++) { z[a] = Xall[a][j] - mu; s2 += z[a] * z[a]; }
        const d = Math.sqrt(s2 / Math.max(1, n - 1)); sd.push(d);
        if (d > 1e-12) for (let a = 0; a < n; a++) z[a] /= d * Math.sqrt(Math.max(1, n - 1));
        Z.push(z);
      }
      const R = matriz(P, P), pares = [];
      for (let i = 0; i < P; i++) for (let j = i + 1; j < P; j++) {
        if (sd[i] <= 1e-12 || sd[j] <= 1e-12) continue;
        let s = 0; const zi = Z[i], zj = Z[j]; for (let a = 0; a < n; a++) s += zi[a] * zj[a];
        R[i][j] = R[j][i] = s;
        if (Math.abs(s) >= thr) pares.push([i, j, Math.abs(s)]);
      }
      if (pares.length) {
        const mediaAbs = []; for (let i = 0; i < P; i++) { let s = 0; for (let j = 0; j < P; j++) if (j !== i) s += Math.abs(R[i][j]); mediaAbs.push(s / (P - 1)); }
        pares.sort(function (a, b) { return b[2] - a[2]; });
        const quita = new Set();
        pares.forEach(function (pr) {
          if (quita.has(pr[0]) || quita.has(pr[1])) return;
          const q = mediaAbs[pr[0]] >= mediaAbs[pr[1]] ? pr[0] : pr[1], otro = q === pr[0] ? pr[1] : pr[0];
          quita.add(q);
          descartadas.push({ name: outAll[q].nombre, razon: 'Casi idéntica a «' + outAll[otro].label + '» (correlación ' + fmt(R[pr[0]][pr[1]], 2) + '); se dejó solo una.' });
        });
        keep = keepTodo.filter(function (j) { return !quita.has(j); });
        pasos.push('Variables repetidas: se quitaron ' + quita.size + ' columna(s) casi idénticas a otra (correlación ≥ ' + fmt(thr, 2) + ').');
      }
    }
    if (!keep.length) throw new Error('Después de la limpieza no queda ninguna variable utilizable.');
    model.keep = keep;
    model.outCols = keep.map(function (j) { return outAll[j]; });
    const P2 = keep.length, c = new Float64Array(P2), s = new Float64Array(P2).fill(1);
    const tipoEsc = g.escalado;
    if (tipoEsc !== 'ninguno') {
      for (let k = 0; k < P2; k++) {
        if (!model.outCols[k].escalable) continue;
        const j = keep[k], v = new Float64Array(n);
        for (let a = 0; a < n; a++) v[a] = Xall[a][j];
        if (tipoEsc === 'estandar') { c[k] = media(v); const d = desvio(v, c[k]); s[k] = d > 1e-12 ? d : 1; }
        else if (tipoEsc === 'minmax') { let mn = Infinity, mx = -Infinity; for (let a = 0; a < n; a++) { if (v[a] < mn) mn = v[a]; if (v[a] > mx) mx = v[a]; } c[k] = mn; s[k] = mx - mn > 1e-12 ? mx - mn : 1; }
        else { const o = ordenado(v); c[k] = cuantilOrd(o, .5); const i = cuantilOrd(o, .75) - cuantilOrd(o, .25); s[k] = i > 1e-12 ? i : (desvio(v, media(v)) > 1e-12 ? desvio(v, media(v)) : 1); }
      }
      pasos.push('Escala: ' + nombreCat('escalado', tipoEsc).toLowerCase() + ' en ' + model.outCols.filter(function (o) { return o.escalable; }).length + ' columna(s); calculada solo con el entrenamiento.');
    }
    model.esc = { c: c, s: s };
    model.pasosEscala = tipoEsc;
    return { modelo: model, rows: rows, pasos: pasos, descartadas: descartadas, nIni: nIni };
  }

  /** Aplica un preprocesamiento ya ajustado. Devuelve X (escalada), Xu (sin escalar) y las filas conservadas. */
  function aplicarPrep(model, filas, esTrain) {
    let rows = filas;
    if (!esTrain && model.colsDropNA.length) rows = filas.filter(function (r) { return !model.colsDropNA.some(function (c) { return faltaEn(c, r); }); });
    const knnMapa = model.knn ? imputarKNN(model, rows) : null;
    const Xall = generarX(model, rows, knnMapa);
    const keep = model.keep, P = keep.length, X = new Array(rows.length), Xu = new Array(rows.length);
    for (let a = 0; a < rows.length; a++) {
      const fu = new Float64Array(P), fe = new Float64Array(P), src = Xall[a];
      for (let k = 0; k < P; k++) { const v = src[keep[k]]; fu[k] = v; fe[k] = (v - model.esc.c[k]) / model.esc.s[k]; }
      Xu[a] = fu; X[a] = fe;
    }
    return { X: X, Xu: Xu, filas: rows };
  }

  // ==========================================================================
  // 7. MODELOS
  //    Entrada: X = arreglo de Float64Array (filas). y: Float64Array (regresión)
  //    o Int32Array con índices de clase (clasificación, K clases).
  //    Salida de predecir(): Float64Array (regresión) o arreglo de Float64Array(K) con probabilidades.
  // ==========================================================================

  function estdAjustar(X) {
    const n = X.length, p = n ? X[0].length : 0, mu = new Float64Array(p), sd = new Float64Array(p);
    for (let i = 0; i < n; i++) { const r = X[i]; for (let j = 0; j < p; j++) mu[j] += r[j]; }
    for (let j = 0; j < p; j++) mu[j] /= Math.max(1, n);
    for (let i = 0; i < n; i++) { const r = X[i]; for (let j = 0; j < p; j++) { const d = r[j] - mu[j]; sd[j] += d * d; } }
    for (let j = 0; j < p; j++) { const s = Math.sqrt(sd[j] / Math.max(1, n)); sd[j] = s > 1e-12 ? s : 1; }
    return { mu: mu, sd: sd };
  }
  function estdAplicar(E, X) {
    const p = E.mu.length, out = new Array(X.length);
    for (let i = 0; i < X.length; i++) { const o = new Float64Array(p), r = X[i]; for (let j = 0; j < p; j++) o[j] = (r[j] - E.mu[j]) / E.sd[j]; out[i] = o; }
    return out;
  }
  function softmaxEn(l, K) {
    let m = -Infinity; for (let k = 0; k < K; k++) if (l[k] > m) m = l[k];
    const o = new Float64Array(K); let s = 0;
    for (let k = 0; k < K; k++) { o[k] = Math.exp(l[k] - m); s += o[k]; }
    for (let k = 0; k < K; k++) o[k] /= s;
    return o;
  }

  /** Convierte coeficientes (b0, b) de variables estandarizadas (mu, sd) a las unidades originales; propaga la covarianza. */
  function desescalarCoef(b0, b, cov, mu, sd) {
    const p = b.length, q = p + 1;
    const T = matriz(q, q);
    T[0][0] = 1;
    for (let j = 0; j < p; j++) { T[0][j + 1] = -mu[j] / sd[j]; T[j + 1][j + 1] = 1 / sd[j]; }
    const bo = new Float64Array(q);
    const bb = new Float64Array(q); bb[0] = b0; for (let j = 0; j < p; j++) bb[j + 1] = b[j];
    for (let i = 0; i < q; i++) { let s = 0; for (let j = 0; j < q; j++) s += T[i][j] * bb[j]; bo[i] = s; }
    let se = null, covO = null;
    if (cov) {
      se = new Float64Array(q); covO = matriz(q, q);
      for (let i = 0; i < q; i++) {
        for (let k = 0; k <= i; k++) {
          let v = 0;
          for (let a = 0; a < q; a++) { if (T[i][a] === 0) continue; for (let c = 0; c < q; c++) if (T[k][c] !== 0) v += T[i][a] * cov[a][c] * T[k][c]; }
          covO[i][k] = v; covO[k][i] = v;
        }
        se[i] = Math.sqrt(Math.max(0, covO[i][i]));
      }
    }
    return { b0: bo[0], b: bo.subarray(1), se: se, cov: covO };
  }

  // --- Regresión lineal (mínimos cuadrados) ------------------------------------
  function olsAjustar(X, y) {
    const n = X.length, p = X[0].length, q = p + 1;
    if (n <= q) throw new Error('Hay más variables (' + p + ') que observaciones útiles (' + n + ') para la regresión lineal; usa Ridge o Lasso, o menos variables.');
    const A = matriz(q, q), b = new Float64Array(q), z = new Float64Array(q);
    for (let i = 0; i < n; i++) {
      z[0] = 1; const r = X[i]; for (let j = 0; j < p; j++) z[j + 1] = r[j];
      const yi = y[i];
      for (let a = 0; a < q; a++) { const za = z[a]; b[a] += za * yi; const Aa = A[a]; for (let c = 0; c <= a; c++) Aa[c] += za * z[c]; }
    }
    for (let a = 0; a < q; a++) for (let c = 0; c < a; c++) A[c][a] = A[a][c];
    const sol = resolverSim(A, b, q), beta = sol.x;
    let rss = 0;
    for (let i = 0; i < n; i++) { let s = beta[0]; const r = X[i]; for (let j = 0; j < p; j++) s += beta[j + 1] * r[j]; const e = y[i] - s; rss += e * e; }
    const df = n - q, sigma2 = rss / df;
    const A2 = matriz(q, q);
    for (let i = 0; i < q; i++) { for (let j = 0; j < q; j++) A2[i][j] = A[i][j]; if (isFinite(sol.jitter)) A2[i][i] += sol.jitter; }
    const inv = invertir(A2, q);
    const se = new Float64Array(q), t = new Float64Array(q), pv = new Float64Array(q), cov = matriz(q, q);
    for (let j = 0; j < q; j++) {
      for (let k = 0; k < q; k++) cov[j][k] = inv ? sigma2 * inv[j][k] : NaN;
      se[j] = inv ? Math.sqrt(Math.max(0, sigma2 * inv[j][j])) : NaN;
      t[j] = se[j] > 0 ? beta[j] / se[j] : (beta[j] === 0 ? 0 : (beta[j] > 0 ? Infinity : -Infinity));
      pv[j] = isNaN(se[j]) ? NaN : tPvalor(t[j], df);
    }
    return {
      b0: beta[0], b: beta.subarray(1), beta: beta, se: se, t: t, p: pv, df: df, sigma2: sigma2, cov: cov, rss: rss,
      predecir: function (Xn) { const o = new Float64Array(Xn.length); for (let i = 0; i < Xn.length; i++) { let s = beta[0]; const r = Xn[i]; for (let j = 0; j < p; j++) s += beta[j + 1] * r[j]; o[i] = s; } return o; }
    };
  }

  function lineal(b0, b) {
    const p = b.length;
    return function (Xn) { const o = new Float64Array(Xn.length); for (let i = 0; i < Xn.length; i++) { let s = b0; const r = Xn[i]; for (let j = 0; j < p; j++) s += b[j] * r[j]; o[i] = s; } return o; };
  }

  function ridgeAjustar(X, y, lambda) {
    const n = X.length, p = X[0].length, E = estdAjustar(X), Z = estdAplicar(E, X);
    const ym = media(y); let ys = desvio(y, ym); if (!(ys > 1e-12)) ys = 1;
    const A = matriz(p, p), b = new Float64Array(p);
    for (let i = 0; i < n; i++) {
      const z = Z[i], yc = (y[i] - ym) / ys;
      for (let a = 0; a < p; a++) { b[a] += z[a] * yc; const Aa = A[a]; for (let c = 0; c <= a; c++) Aa[c] += z[a] * z[c]; }
    }
    for (let a = 0; a < p; a++) { b[a] /= n; for (let c = 0; c <= a; c++) { A[a][c] /= n; A[c][a] = A[a][c]; } A[a][a] += lambda; }
    const beta = resolverSim(A, b, p).x;
    const coef = new Float64Array(p); let b0 = ym;
    for (let j = 0; j < p; j++) { coef[j] = beta[j] * ys / E.sd[j]; b0 -= coef[j] * E.mu[j]; }
    return { b0: b0, b: coef, predecir: lineal(b0, coef) };
  }

  function lassoAjustar(X, y, lambda) {
    const n = X.length, p = X[0].length, E = estdAjustar(X);
    const ym = media(y); let ys = desvio(y, ym); if (!(ys > 1e-12)) ys = 1;
    const Zc = [], r = new Float64Array(n);
    for (let j = 0; j < p; j++) { const c = new Float64Array(n); for (let i = 0; i < n; i++) c[i] = (X[i][j] - E.mu[j]) / E.sd[j]; Zc.push(c); }
    for (let i = 0; i < n; i++) r[i] = (y[i] - ym) / ys;
    const beta = new Float64Array(p);
    const norma = new Float64Array(p);
    for (let j = 0; j < p; j++) { let s = 0; for (let i = 0; i < n; i++) s += Zc[j][i] * Zc[j][i]; norma[j] = s / n; }
    for (let it = 0; it < 500; it++) {
      let maxd = 0;
      for (let j = 0; j < p; j++) {
        if (norma[j] < 1e-12) continue;
        const zj = Zc[j]; let s = 0;
        for (let i = 0; i < n; i++) s += zj[i] * r[i];
        const rho = s / n + beta[j] * norma[j];
        const nb = rho > lambda ? (rho - lambda) / norma[j] : (rho < -lambda ? (rho + lambda) / norma[j] : 0);
        const d = nb - beta[j];
        if (d !== 0) { for (let i = 0; i < n; i++) r[i] -= d * zj[i]; beta[j] = nb; if (Math.abs(d) > maxd) maxd = Math.abs(d); }
      }
      if (maxd < 1e-7) break;
    }
    const coef = new Float64Array(p); let b0 = ym;
    for (let j = 0; j < p; j++) { coef[j] = beta[j] * ys / E.sd[j]; b0 -= coef[j] * E.mu[j]; }
    return { b0: b0, b: coef, predecir: lineal(b0, coef) };
  }

  // --- Regresión logística (binaria y multinomial) por L-BFGS ------------------
  function logisticaAjustar(X, yi, K, lambda) {
    const n = X.length, p = X[0].length, q = p + 1;
    const E = estdAjustar(X), Z = estdAplicar(E, X);
    if (K === 2) {
      const fg = function (w, g) {
        g.fill(0); let f = 0;
        for (let i = 0; i < n; i++) {
          const z = Z[i]; let s = w[0]; for (let j = 0; j < p; j++) s += w[j + 1] * z[j];
          const pr = 1 / (1 + Math.exp(-s));
          f += (s > 0 ? s + Math.log1p(Math.exp(-s)) : Math.log1p(Math.exp(s))) - yi[i] * s;
          const e = pr - yi[i]; g[0] += e; for (let j = 0; j < p; j++) g[j + 1] += e * z[j];
        }
        f /= n; for (let k = 0; k < q; k++) g[k] /= n;
        for (let j = 1; j < q; j++) { f += 0.5 * lambda * w[j] * w[j]; g[j] += lambda * w[j]; }
        return f;
      };
      const w = lbfgs(fg, new Float64Array(q), 300, 1e-7).x;
      // Matriz de información para errores estándar
      const H = matriz(q, q), zz = new Float64Array(q);
      for (let i = 0; i < n; i++) {
        let s = w[0]; const z = Z[i]; for (let j = 0; j < p; j++) s += w[j + 1] * z[j];
        const pr = 1 / (1 + Math.exp(-s)), wi = Math.max(pr * (1 - pr), 1e-10);
        zz[0] = 1; for (let j = 0; j < p; j++) zz[j + 1] = z[j];
        for (let a = 0; a < q; a++) for (let c = 0; c <= a; c++) H[a][c] += wi * zz[a] * zz[c];
      }
      for (let a = 0; a < q; a++) { for (let c = 0; c < a; c++) H[c][a] = H[a][c]; if (a > 0) H[a][a] += n * lambda; }
      const cov = invertir(H, q);
      const d = desescalarCoef(w[0], w.subarray(1), cov, E.mu, E.sd);
      return {
        K: 2, b0: d.b0, b: d.b, se: d.se, cov: d.cov,
        predecir: function (Xn) {
          const o = new Array(Xn.length);
          for (let i = 0; i < Xn.length; i++) { let s = d.b0; const r = Xn[i]; for (let j = 0; j < p; j++) s += d.b[j] * r[j]; const pr = 1 / (1 + Math.exp(-s)); o[i] = Float64Array.of(1 - pr, pr); }
          return o;
        }
      };
    }
    const fg = function (w, g) {
      g.fill(0); let f = 0; const l = new Float64Array(K);
      for (let i = 0; i < n; i++) {
        const z = Z[i];
        for (let k = 0; k < K; k++) { let s = w[k * q]; for (let j = 0; j < p; j++) s += w[k * q + j + 1] * z[j]; l[k] = s; }
        const pr = softmaxEn(l, K);
        f -= Math.log(Math.max(pr[yi[i]], 1e-300));
        for (let k = 0; k < K; k++) { const e = pr[k] - (k === yi[i] ? 1 : 0); g[k * q] += e; for (let j = 0; j < p; j++) g[k * q + j + 1] += e * z[j]; }
      }
      f /= n; for (let k = 0; k < g.length; k++) g[k] /= n;
      for (let k = 0; k < K; k++) for (let j = 1; j < q; j++) { const t = k * q + j; f += 0.5 * lambda * w[t] * w[t]; g[t] += lambda * w[t]; }
      return f;
    };
    const w = lbfgs(fg, new Float64Array(K * q), 300, 1e-6).x;
    // Coeficientes en unidades originales de cada clase y relativos a la clase 0
    const B0 = [], BB = [];
    for (let k = 0; k < K; k++) { const d = desescalarCoef(w[k * q], w.subarray(k * q + 1, (k + 1) * q), null, E.mu, E.sd); B0.push(d.b0); BB.push(Float64Array.from(d.b)); }
    return {
      K: K, B0: B0, BB: BB,
      predecir: function (Xn) {
        const o = new Array(Xn.length), l = new Float64Array(K);
        for (let i = 0; i < Xn.length; i++) { const r = Xn[i]; for (let k = 0; k < K; k++) { let s = B0[k]; for (let j = 0; j < p; j++) s += BB[k][j] * r[j]; l[k] = s; } o[i] = softmaxEn(l, K); }
        return o;
      }
    };
  }

  // --- Bayes ingenuo gaussiano --------------------------------------------------
  function nbAjustar(X, yi, K) {
    const n = X.length, p = X[0].length, cnt = new Float64Array(K), mu = [], va = [];
    for (let k = 0; k < K; k++) { mu.push(new Float64Array(p)); va.push(new Float64Array(p)); }
    for (let i = 0; i < n; i++) { cnt[yi[i]]++; for (let j = 0; j < p; j++) mu[yi[i]][j] += X[i][j]; }
    for (let k = 0; k < K; k++) for (let j = 0; j < p; j++) mu[k][j] /= Math.max(1, cnt[k]);
    for (let i = 0; i < n; i++) for (let j = 0; j < p; j++) { const d = X[i][j] - mu[yi[i]][j]; va[yi[i]][j] += d * d; }
    const E = estdAjustar(X);
    for (let k = 0; k < K; k++) for (let j = 0; j < p; j++) va[k][j] = va[k][j] / Math.max(1, cnt[k]) + 1e-3 * E.sd[j] * E.sd[j] + 1e-9;
    const lp = new Float64Array(K); for (let k = 0; k < K; k++) lp[k] = Math.log((cnt[k] + 1) / (n + K));
    return {
      predecir: function (Xn) {
        const o = new Array(Xn.length), l = new Float64Array(K);
        for (let i = 0; i < Xn.length; i++) {
          const r = Xn[i];
          for (let k = 0; k < K; k++) { let s = lp[k]; for (let j = 0; j < p; j++) { const d = r[j] - mu[k][j]; s -= 0.5 * Math.log(va[k][j]) + d * d / (2 * va[k][j]); } l[k] = s; }
          o[i] = softmaxEn(l, K);
        }
        return o;
      }
    };
  }

  // --- Análisis discriminante lineal ----------------------------------------------
  function ldaAjustar(X, yi, K) {
    const n = X.length, p = X[0].length, cnt = new Float64Array(K), mu = [];
    for (let k = 0; k < K; k++) mu.push(new Float64Array(p));
    for (let i = 0; i < n; i++) { cnt[yi[i]]++; for (let j = 0; j < p; j++) mu[yi[i]][j] += X[i][j]; }
    for (let k = 0; k < K; k++) for (let j = 0; j < p; j++) mu[k][j] /= Math.max(1, cnt[k]);
    const S = matriz(p, p), d = new Float64Array(p);
    for (let i = 0; i < n; i++) {
      const m = mu[yi[i]], r = X[i];
      for (let j = 0; j < p; j++) d[j] = r[j] - m[j];
      for (let a = 0; a < p; a++) for (let c = 0; c <= a; c++) S[a][c] += d[a] * d[c];
    }
    let tr = 0;
    for (let a = 0; a < p; a++) { for (let c = 0; c <= a; c++) { S[a][c] /= Math.max(1, n - K); S[c][a] = S[a][c]; } tr += S[a][a]; }
    for (let a = 0; a < p; a++) S[a][a] += 1e-4 * (tr / p) + 1e-9;
    const W = [], c0 = new Float64Array(K);
    for (let k = 0; k < K; k++) {
      const w = resolverSim(S, mu[k], p).x; W.push(w);
      let s = 0; for (let j = 0; j < p; j++) s += w[j] * mu[k][j];
      c0[k] = -0.5 * s + Math.log((cnt[k] + 1) / (n + K));
    }
    return {
      predecir: function (Xn) {
        const o = new Array(Xn.length), l = new Float64Array(K);
        for (let i = 0; i < Xn.length; i++) { const r = Xn[i]; for (let k = 0; k < K; k++) { let s = c0[k]; for (let j = 0; j < p; j++) s += W[k][j] * r[j]; l[k] = s; } o[i] = softmaxEn(l, K); }
        return o;
      }
    };
  }

  // --- Vecinos más cercanos -----------------------------------------------------
  function knnAjustar(X, y, k, K, rng) {
    let idx = null;
    if (X.length > 8000) idx = rng.muestra(X.length, 8000);
    const Xs = idx ? idx.map(function (i) { return X[i]; }) : X, ys = idx ? idx.map(function (i) { return y[i]; }) : y;
    const n = Xs.length, p = Xs[0].length, E = estdAjustar(Xs);
    const T = new Float64Array(n * p);
    for (let i = 0; i < n; i++) for (let j = 0; j < p; j++) T[i * p + j] = (Xs[i][j] - E.mu[j]) / E.sd[j];
    k = Math.max(1, Math.min(k, n));
    const q = new Float64Array(p);
    // Busca los kk vecinos más cercanos de la fila Xn[a] (ordenados de menor a mayor distancia)
    function buscar(fila, kk, bd, bi) {
      for (let j = 0; j < p; j++) q[j] = (fila[j] - E.mu[j]) / E.sd[j];
      bd.fill(Infinity); bi.fill(-1);
      for (let i = 0; i < n; i++) {
        let acc = 0; const off = i * p, tope = bd[kk - 1];
        for (let j = 0; j < p; j++) { const t = q[j] - T[off + j]; acc += t * t; if (acc >= tope) break; }
        if (acc < tope) { let s = kk - 1; while (s > 0 && bd[s - 1] > acc) { bd[s] = bd[s - 1]; bi[s] = bi[s - 1]; s--; } bd[s] = acc; bi[s] = i; }
      }
    }
    function resumen(bi, kk) {
      if (K === 0) { let s = 0; for (let t = 0; t < kk; t++) s += ys[bi[t]]; return s / kk; }
      const pr = new Float64Array(K); for (let t = 0; t < kk; t++) pr[ys[bi[t]]]++;
      for (let c = 0; c < K; c++) pr[c] = (pr[c] + 0.5) / (kk + 0.5 * K);
      return pr;
    }
    return {
      nref: n,
      predecir: function (Xn) {
        const bd = new Float64Array(k), bi = new Int32Array(k), o = new Array(Xn.length);
        for (let a = 0; a < Xn.length; a++) { buscar(Xn[a], k, bd, bi); o[a] = resumen(bi, k); }
        return K === 0 ? Float64Array.from(o) : o;
      },
      // Predicciones para varios valores de k con una sola búsqueda (se usa en la calibración)
      predecirKs: function (Xn, ks) {
        const kmax = Math.min(n, Math.max.apply(null, ks)), bd = new Float64Array(kmax), bi = new Int32Array(kmax);
        const salidas = ks.map(function () { return new Array(Xn.length); });
        for (let a = 0; a < Xn.length; a++) { buscar(Xn[a], kmax, bd, bi); ks.forEach(function (kk, c) { salidas[c][a] = resumen(bi, Math.min(kk, kmax)); }); }
        return K === 0 ? salidas.map(function (x) { return Float64Array.from(x); }) : salidas;
      }
    };
  }

  // --- Árboles CART (histogramas por variable) y bosques --------------------------
  /** Divide cada variable en hasta `maxB` intervalos (a partir de sus valores ordenados). */
  function crearBins(X, maxB) {
    const n = X.length, p = X[0].length, edges = [], nb = new Int32Array(p), Xb = new Uint8Array(n * p);
    const col = new Float64Array(n);
    for (let j = 0; j < p; j++) {
      for (let i = 0; i < n; i++) col[i] = X[i][j];
      const s = Float64Array.from(col).sort(); const u = [];
      for (let i = 0; i < n; i++) if (i === 0 || s[i] !== s[i - 1]) u.push(s[i]);
      const e = [];
      if (u.length <= maxB) { for (let k = 0; k < u.length - 1; k++) e.push((u[k] + u[k + 1]) / 2); }
      else {
        let ult = -1;
        for (let b = 1; b < maxB; b++) { const t = Math.round(b * u.length / maxB); if (t > ult && t >= 1 && t < u.length) { e.push((u[t - 1] + u[t]) / 2); ult = t; } }
      }
      edges.push(Float64Array.from(e)); nb[j] = e.length + 1;
      for (let i = 0; i < n; i++) {
        const x = col[i]; let lo = 0, hi = e.length;
        while (lo < hi) { const m = (lo + hi) >> 1; if (e[m] >= x) hi = m; else lo = m + 1; }
        Xb[i * p + j] = lo;
      }
    }
    return { Xb: Xb, edges: edges, nb: nb, p: p, n: n };
  }

  /** Construye un árbol. K=0 regresión; K>=2 clasificación. idx: muestras (puede repetir). */
  function construirArbol(Bn, y, K, idx, par, rng) {
    const Xb = Bn.Xb, p = Bn.p, nb = Bn.nb, reg = K === 0;
    const feat = [], thr = [], izq = [], der = [], val = [];
    let maxB = 2; for (let j = 0; j < p; j++) if (nb[j] > maxB) maxB = nb[j];
    const cnt = new Float64Array(maxB * (reg ? 1 : K)), sum = new Float64Array(reg ? maxB : 1);
    const tot = new Float64Array(reg ? 1 : K), izqC = new Float64Array(reg ? 1 : K);
    const orden = new Int32Array(p); for (let j = 0; j < p; j++) orden[j] = j;
    const minLeaf = Math.max(1, par.hoja_min), maxDepth = par.profundidad, mtry = Math.max(1, Math.min(p, par.mtry || p));

    function hoja(lo, hi) {
      const n = hi - lo;
      if (reg) { let s = 0; for (let i = lo; i < hi; i++) s += y[idx[i]]; return s / n; }
      const pr = new Float64Array(K);
      for (let i = lo; i < hi; i++) pr[y[idx[i]]]++;
      for (let c = 0; c < K; c++) pr[c] = (pr[c] + 0.5) / (n + 0.5 * K);
      return pr;
    }
    function nodo(lo, hi, prof) {
      const id = feat.length, n = hi - lo;
      feat.push(-1); thr.push(0); izq.push(-1); der.push(-1); val.push(null);
      let S = 0, SS = 0, pura = false;
      if (reg) { for (let i = lo; i < hi; i++) { const v = y[idx[i]]; S += v; SS += v * v; } pura = SS - S * S / n <= 1e-12 * (SS + 1e-300); }
      else { tot.fill(0); for (let i = lo; i < hi; i++) tot[y[idx[i]]]++; let mx = 0; for (let c = 0; c < K; c++) if (tot[c] > mx) mx = tot[c]; pura = mx === n; }
      if (pura || prof >= maxDepth || n < 2 * minLeaf || n < 2) { val[id] = hoja(lo, hi); return id; }
      let base = 0;
      if (reg) base = S * S / n; else for (let c = 0; c < K; c++) base += tot[c] * tot[c] / n;
      let mejor = 1e-10 * (reg ? Math.max(1, SS) : 1), mF = -1, mB = -1, evaluados = 0;
      // orden aleatorio de variables (mezcla parcial)
      for (let t = p - 1; t > 0; t--) { const j = Math.floor(rng.next() * (t + 1)); const tmp = orden[t]; orden[t] = orden[j]; orden[j] = tmp; }
      for (let t = 0; t < p; t++) {
        if (evaluados >= mtry && mF >= 0) break;
        const f = orden[t], B = nb[f];
        if (B < 2) continue;
        evaluados++;
        if (reg) {
          for (let b = 0; b < B; b++) { cnt[b] = 0; sum[b] = 0; }
          for (let i = lo; i < hi; i++) { const r = idx[i], b = Xb[r * p + f]; cnt[b]++; sum[b] += y[r]; }
          let nl = 0, sl = 0;
          for (let b = 0; b < B - 1; b++) {
            nl += cnt[b]; sl += sum[b];
            if (nl < minLeaf) continue;
            const nr = n - nl; if (nr < minLeaf) break;
            const sr = S - sl, gain = sl * sl / nl + sr * sr / nr - base;
            if (gain > mejor) { mejor = gain; mF = f; mB = b; }
          }
        } else {
          for (let b = 0; b < B * K; b++) cnt[b] = 0;
          for (let i = lo; i < hi; i++) { const r = idx[i]; cnt[Xb[r * p + f] * K + y[r]]++; }
          izqC.fill(0); let nl = 0;
          for (let b = 0; b < B - 1; b++) {
            for (let c = 0; c < K; c++) { const v = cnt[b * K + c]; izqC[c] += v; nl += v; }
            if (nl < minLeaf) continue;
            const nr = n - nl; if (nr < minLeaf) break;
            let gl = 0, gr = 0;
            for (let c = 0; c < K; c++) { gl += izqC[c] * izqC[c]; const rr = tot[c] - izqC[c]; gr += rr * rr; }
            const gain = gl / nl + gr / nr - base;
            if (gain > mejor) { mejor = gain; mF = f; mB = b; }
          }
        }
      }
      if (mF < 0) { val[id] = hoja(lo, hi); return id; }
      // partición in situ: bin <= mB a la izquierda
      let i = lo, j = hi - 1;
      while (i <= j) {
        if (Xb[idx[i] * p + mF] <= mB) i++;
        else { const tmp = idx[i]; idx[i] = idx[j]; idx[j] = tmp; j--; }
      }
      if (i === lo || i === hi) { val[id] = hoja(lo, hi); return id; }
      feat[id] = mF; thr[id] = Bn.edges[mF][mB];
      izq[id] = nodo(lo, i, prof + 1);
      der[id] = nodo(i, hi, prof + 1);
      return id;
    }
    nodo(0, idx.length, 0);
    return { feat: feat, thr: thr, izq: izq, der: der, val: val };
  }
  function predecirArbol(T, row) {
    let k = 0;
    while (T.feat[k] >= 0) k = row[T.feat[k]] <= T.thr[k] ? T.izq[k] : T.der[k];
    return T.val[k];
  }

  async function bosqueAjustar(X, y, K, par, rng, esBosque) {
    const n = X.length, p = X[0].length, Bn = crearBins(X, 64), arboles = [];
    const nT = esBosque ? par.arboles : 1;
    for (let t = 0; t < nT; t++) {
      const idx = new Int32Array(n);
      if (esBosque) for (let i = 0; i < n; i++) idx[i] = Math.floor(rng.next() * n); else for (let i = 0; i < n; i++) idx[i] = i;
      arboles.push(construirArbol(Bn, y, K, idx, par, rng));
      const w = tk(); if (w) await w;
    }
    return {
      predecir: function (Xn) {
        const m = Xn.length, T = arboles.length;
        if (K === 0) { const o = new Float64Array(m); for (let i = 0; i < m; i++) { let s = 0; for (let t = 0; t < T; t++) s += predecirArbol(arboles[t], Xn[i]); o[i] = s / T; } return o; }
        const o = new Array(m);
        for (let i = 0; i < m; i++) { const pr = new Float64Array(K); for (let t = 0; t < T; t++) { const v = predecirArbol(arboles[t], Xn[i]); for (let c = 0; c < K; c++) pr[c] += v[c]; } for (let c = 0; c < K; c++) pr[c] /= T; o[i] = pr; }
        return o;
      }
    };
  }

  // --- Máquinas de vectores de soporte (SMO con selección de segundo orden) --------
  const SVM_MAX = 3000;

  /** Matriz de núcleo (Float32, simétrica) de n puntos en Z (arreglo plano n×p). */
  function svmNucleo(Z, n, p, tipo, gamma) {
    const Kd = new Float32Array(n * n), nm = new Float64Array(n);
    for (let i = 0; i < n; i++) { let s = 0; for (let j = 0; j < p; j++) { const v = Z[i * p + j]; s += v * v; } nm[i] = s; }
    for (let i = 0; i < n; i++) {
      const oi = i * p, fi = i * n;
      for (let k = 0; k <= i; k++) {
        const ok = k * p; let d = 0;
        for (let j = 0; j < p; j++) d += Z[oi + j] * Z[ok + j];
        Kd[fi + k] = tipo === 'lineal' ? d : Math.exp(-gamma * Math.max(0, nm[i] + nm[k] - 2 * d));
      }
    }
    // Reflejo del triángulo inferior por bloques (más amable con la memoria caché)
    const B = 32;
    for (let bi = 0; bi < n; bi += B) for (let bk = 0; bk <= bi; bk += B) {
      const ie = Math.min(n, bi + B);
      for (let i = bi; i < ie; i++) { const ke = Math.min(i, bk + B); for (let k = bk; k < ke; k++) Kd[k * n + i] = Kd[i * n + k]; }
    }
    return Kd;
  }

  /**
   * SMO genérico: minimiza ½αᵀQα + pᵀα con 0≤α≤C y Σ yα = 0, donde Q_ij = y_i y_j K(map[i], map[j]).
   * Sirve para clasificación (p = −1) y para regresión ε-SVR (2n variables).
   * Devuelve α y el sesgo b (la función es f(x) = Σ α_i y_i K(x, x_map[i]) + b).
   */
  function svmSMO(Kd, N, map, y, pv, C, tol, maxIter) {
    const L = map.length, a = new Float64Array(L), G = Float64Array.from(pv), dg = new Float64Array(L);
    let ident = L === N;
    for (let t = 0; t < L; t++) { dg[t] = Kd[map[t] * N + map[t]]; if (map[t] !== t) ident = false; }
    const bi = new Float64Array(L), bj = new Float64Array(L);
    const fila = function (m, buf) { if (ident) return Kd.subarray(m * N, m * N + N); const o = m * N; for (let t = 0; t < L; t++) buf[t] = Kd[o + map[t]]; return buf; };
    let iter = 0, Gmax = -Infinity, i = -1;
    for (let t = 0; t < L; t++) {
      const yt = y[t];
      if (yt > 0 ? a[t] < C : a[t] > 0) { const v = -yt * G[t]; if (v > Gmax) { Gmax = v; i = t; } }
    }
    for (; iter < maxIter; iter++) {
      if (i < 0) break;
      const yi = y[i], ki = fila(map[i], bi), Kii = dg[i];
      let Gmin = Infinity, j = -1, mejor = Infinity, vj = 0;
      for (let t = 0; t < L; t++) {
        const yt = y[t];
        if (yt > 0 ? a[t] > 0 : a[t] < C) {
          const v = -yt * G[t];
          if (v < Gmin) Gmin = v;
          const d = Gmax - v;
          if (d > 0) {
            let q = Kii + dg[t] - 2 * ki[t]; if (q < 1e-12) q = 1e-12;
            const obj = -d * d / q;
            if (obj < mejor) { mejor = obj; j = t; vj = v; }
          }
        }
      }
      if (Gmax - Gmin < tol || j < 0) break;
      const yj = y[j], kj = fila(map[j], bj);
      let q = Kii + dg[j] - 2 * ki[j]; if (q < 1e-12) q = 1e-12;
      let d = (Gmax - vj) / q;
      const di = yi > 0 ? C - a[i] : a[i], dj = yj > 0 ? a[j] : C - a[j];
      if (di < d) d = di; if (dj < d) d = dj;
      if (!(d > 0)) break;
      a[i] += yi * d; a[j] -= yj * d;
      Gmax = -Infinity; i = -1;
      for (let t = 0; t < L; t++) {
        const yt = y[t], g = G[t] + yt * d * (ki[t] - kj[t]); G[t] = g;
        if (yt > 0 ? a[t] < C : a[t] > 0) { const v = -yt * g; if (v > Gmax) { Gmax = v; i = t; } }
      }
    }
    // Sesgo: promedio de −yG en las variables libres; si no hay, punto medio del intervalo permitido
    let sl = 0, nl = 0, ub = Infinity, lb = -Infinity;
    for (let t = 0; t < L; t++) {
      const v = -y[t] * G[t];
      if (a[t] > 1e-10 && a[t] < C - 1e-10) { sl += v; nl++; }
      if (y[t] > 0 ? a[t] < C : a[t] > 0) { if (v > lb) lb = v; }
      if (y[t] > 0 ? a[t] > 0 : a[t] < C) { if (v < ub) ub = v; }
    }
    const b = nl ? sl / nl : (isFinite(lb) && isFinite(ub) ? (lb + ub) / 2 : 0);
    return { a: a, b: b, iter: iter };
  }

  /** Escalado de Platt: P(y=1) = 1/(1+exp(−(A·f + B))), ajustado por Newton con objetivos suavizados. */
  function platt(f, pos) {
    const n = f.length; let np = 0; for (let i = 0; i < n; i++) if (pos[i]) np++;
    const nn = n - np, tp = (np + 1) / (np + 2), tn = 1 / (nn + 2);
    let A = 1, B = 0;
    const nll = function (A, B) { let s = 0; for (let i = 0; i < n; i++) { const z = A * f[i] + B, t = pos[i] ? tp : tn; s += (z > 0 ? z + Math.log1p(Math.exp(-z)) : Math.log1p(Math.exp(z))) - t * z; } return s; };
    let fv = nll(A, B);
    for (let it = 0; it < 30; it++) {
      let gA = 0, gB = 0, hAA = 1e-6, hAB = 0, hBB = 1e-6;
      for (let i = 0; i < n; i++) {
        const z = A * f[i] + B, pr = 1 / (1 + Math.exp(-z)), t = pos[i] ? tp : tn, e = pr - t, w = Math.max(pr * (1 - pr), 1e-9);
        gA += e * f[i]; gB += e; hAA += w * f[i] * f[i]; hAB += w * f[i]; hBB += w;
      }
      const det = hAA * hBB - hAB * hAB;
      if (!(Math.abs(det) > 1e-18)) break;
      const dA = -(hBB * gA - hAB * gB) / det, dB = -(-hAB * gA + hAA * gB) / det;
      let paso = 1, mejoro = false;
      for (let k = 0; k < 20; k++) { const f2 = nll(A + paso * dA, B + paso * dB); if (f2 <= fv + 1e-12) { A += paso * dA; B += paso * dB; fv = f2; mejoro = true; break; } paso /= 2; }
      if (!mejoro || Math.abs(gA) + Math.abs(gB) < 1e-7) break;
    }
    return { A: A, B: B };
  }

  /** Muestra estratificada (o simple si cl es null) de `m` posiciones entre n. */
  function submuestra(n, m, cl, rng) {
    if (!cl) return rng.muestra(n, m).sort(function (a, b) { return a - b; });
    const g = new Map();
    for (let i = 0; i < n; i++) { if (!g.has(cl[i])) g.set(cl[i], []); g.get(cl[i]).push(i); }
    const out = [];
    g.forEach(function (l) { rng.shuffle(l); const k = Math.max(1, Math.round(m * l.length / n)); for (let i = 0; i < Math.min(k, l.length); i++) out.push(l[i]); });
    return out.sort(function (a, b) { return a - b; });
  }

  /** Núcleo aplicado a filas nuevas: devuelve una función que evalúa Σ coef·K(x, sv) + b para varias tareas. */
  function svmEmpaquetar(Z, p, E, tareas, tipo, gamma, nota, extra) {
    // Vectores de soporte = unión de los de todas las tareas
    const n = Z.length / p, uso = new Uint8Array(n);
    tareas.forEach(function (t) { for (let i = 0; i < n; i++) if (Math.abs(t.coef[i]) > 1e-9) uso[i] = 1; });
    const U = []; for (let i = 0; i < n; i++) if (uso[i]) U.push(i);
    const nU = U.length, SV = new Float64Array(nU * p), SVn = new Float64Array(nU);
    for (let u = 0; u < nU; u++) { let s = 0; for (let j = 0; j < p; j++) { const v = Z[U[u] * p + j]; SV[u * p + j] = v; s += v * v; } SVn[u] = s; }
    const T = tareas.map(function (t) { const c = new Float64Array(nU); for (let u = 0; u < nU; u++) c[u] = t.coef[U[u]]; return { c: c, b: t.b, platt: t.platt || null }; });
    let W = null;
    if (tipo === 'lineal') W = T.map(function (t) { const w = new Float64Array(p); for (let u = 0; u < nU; u++) for (let j = 0; j < p; j++) w[j] += t.c[u] * SV[u * p + j]; return w; });
    const z = new Float64Array(p), kv = new Float64Array(nU);
    function decisiones(fila, out) {
      let zn = 0; for (let j = 0; j < p; j++) { const v = (fila[j] - E.mu[j]) / E.sd[j]; z[j] = v; zn += v * v; }
      if (tipo === 'lineal') { for (let k = 0; k < T.length; k++) { let s = T[k].b; for (let j = 0; j < p; j++) s += W[k][j] * z[j]; out[k] = s; } return; }
      for (let u = 0; u < nU; u++) { let d = 0; const o = u * p; for (let j = 0; j < p; j++) d += SV[o + j] * z[j]; kv[u] = Math.exp(-gamma * Math.max(0, zn + SVn[u] - 2 * d)); }
      for (let k = 0; k < T.length; k++) { let s = T[k].b; const c = T[k].c; for (let u = 0; u < nU; u++) s += c[u] * kv[u]; out[k] = s; }
    }
    return { nSV: nU, nref: tipo === 'lineal' ? 0 : nU, decisiones: decisiones, T: T, nota: nota };
  }

  /** SVM de clasificación: binaria directa; multiclase uno-contra-resto con escalado de Platt por clase. */
  async function svmClfAjustar(X, yi, K, par, rng, rep, rapido) {
    const n0 = X.length, p = X[0].length;
    let sub = null, nota = '';
    if (n0 > SVM_MAX) { sub = submuestra(n0, SVM_MAX, yi, rng); X = sub.map(function (i) { return X[i]; }); yi = elegirFilas(yi, sub); }
    const n = X.length, E = estdAjustar(X), Z = new Float64Array(n * p);
    for (let i = 0; i < n; i++) for (let j = 0; j < p; j++) Z[i * p + j] = (X[i][j] - E.mu[j]) / E.sd[j];
    const tipo = par.kernel === 'lineal' ? 'lineal' : 'rbf', C = par.C, gamma = par.gamma || 1 / p;
    const Kd = svmNucleo(Z, n, p, tipo, gamma);
    const w0 = tk(); if (w0) await w0;
    const todos = new Int32Array(n); for (let i = 0; i < n; i++) todos[i] = i;
    // Subconjunto de calibración (80 % para ajustar y 20 % para estimar Platt)
    const perm = Array.prototype.slice.call(todos); rng.shuffle(perm);
    const nCal = Math.max(10, Math.round(0.2 * n)), calIdx = perm.slice(0, nCal), calTr = perm.slice(nCal).sort(function (a, b) { return a - b; });
    const nT = K === 2 ? 1 : K, tareas = [];
    const maxIter = tipo === 'lineal' ? 3 * n : (rapido ? 5 * n : Math.min(60000, Math.max(3000, 12 * n))), tol = (tipo === 'lineal' || rapido) ? 1e-2 : 2e-3;
    let iters = 0;
    for (let c = 0; c < nT; c++) {
      const cls = K === 2 ? 1 : c;
      const yv = function (i) { return yi[i] === cls ? 1 : -1; };
      const ys = new Float64Array(n), pv = new Float64Array(n);
      for (let i = 0; i < n; i++) { ys[i] = yv(i); pv[i] = -1; }
      const sol = svmSMO(Kd, n, todos, ys, pv, C, tol, maxIter); iters += sol.iter;
      const coef = new Float64Array(n); for (let i = 0; i < n; i++) coef[i] = sol.a[i] * ys[i];
      // Platt con el modelo ajustado al 80 %
      const mapT = Int32Array.from(calTr), yT = new Float64Array(mapT.length), pT = new Float64Array(mapT.length).fill(-1);
      for (let i = 0; i < mapT.length; i++) yT[i] = ys[mapT[i]];
      const s2 = svmSMO(Kd, n, mapT, yT, pT, C, tol, maxIter); iters += s2.iter;
      const f = new Float64Array(nCal), pos = new Array(nCal);
      for (let k = 0; k < nCal; k++) {
        const r = calIdx[k] * n; let s = s2.b;
        for (let t = 0; t < mapT.length; t++) { const al = s2.a[t]; if (al > 1e-9) s += al * yT[t] * Kd[r + mapT[t]]; }
        f[k] = s; pos[k] = ys[calIdx[k]] > 0;
      }
      tareas.push({ coef: coef, b: sol.b, platt: platt(f, pos) });
      if (rep) rep((c + 1) / nT, 'SVM · clase ' + (c + 1) + ' de ' + nT);
      const w = tk(); if (w) await w;
    }
    const emp = svmEmpaquetar(Z, p, E, tareas, tipo, gamma);
    const tn = tipo === 'lineal' ? 'núcleo lineal' : 'núcleo curvo (RBF)';
    nota = 'núcleo ' + (tipo === 'lineal' ? 'lineal' : 'RBF') + ', ' + emp.nSV + ' vectores de soporte';
    const hall = [];
    if (sub) { nota = 'Como había ' + fmt(n0, 0) + ' filas y este método se vuelve muy lento con muchas, se entrenó con una muestra estratificada de ' + fmt(SVM_MAX, 0) + ' filas; ' + nota; hall.push('Se entrenó con una muestra de ' + fmt(SVM_MAX, 0) + ' de las ' + fmt(n0, 0) + ' filas de aprendizaje (se mantiene la proporción de cada categoría) porque el método se vuelve muy lento con más datos. La evaluación sí usa todas las filas de prueba.'); }
    hall.push('La máquina de vectores de soporte usa ' + tn + ' y se apoya en ' + fmt(emp.nSV, 0) + ' casos clave (' + fpct(emp.nSV / n, 0) + ' de los que usó para aprender); es difícil leer reglas simples de ella, pero las importancias de abajo muestran qué variables pesan.');
    const d = new Float64Array(nT);
    return {
      nref: emp.nref, nota: nota, hallazgos: hall, nSV: emp.nSV, submuestreo: sub ? { de: n0, a: n } : null,
      predecir: function (Xn) {
        const o = new Array(Xn.length);
        for (let i = 0; i < Xn.length; i++) {
          emp.decisiones(Xn[i], d);
          if (K === 2) { const t = emp.T[0].platt, pr = 1 / (1 + Math.exp(-(t.A * d[0] + t.B))); o[i] = Float64Array.of(1 - pr, pr); }
          else { const pr = new Float64Array(K); let s = 0; for (let k = 0; k < K; k++) { const t = emp.T[k].platt; pr[k] = 1 / (1 + Math.exp(-(t.A * d[k] + t.B))) + 1e-9; s += pr[k]; } for (let k = 0; k < K; k++) pr[k] /= s; o[i] = pr; }
        }
        return o;
      }
    };
  }

  /** ε-SVR (regresión por vectores de soporte). */
  async function svmRegAjustar(X, y, par, rng, rep, rapido) {
    const n0 = X.length, p = X[0].length;
    let sub = null;
    if (n0 > SVM_MAX) { sub = submuestra(n0, SVM_MAX, null, rng); X = sub.map(function (i) { return X[i]; }); y = elegirFilas(y, sub); }
    const n = X.length, E = estdAjustar(X), Z = new Float64Array(n * p);
    for (let i = 0; i < n; i++) for (let j = 0; j < p; j++) Z[i * p + j] = (X[i][j] - E.mu[j]) / E.sd[j];
    const ym = media(y); let ysd = desvio(y, ym); if (!(ysd > 1e-12)) ysd = 1;
    const tipo = par.kernel === 'lineal' ? 'lineal' : 'rbf', C = par.C, eps = par.epsilon, gamma = par.gamma || 1 / p;
    const Kd = svmNucleo(Z, n, p, tipo, gamma);
    const w0 = tk(); if (w0) await w0;
    const map = new Int32Array(2 * n), ys = new Float64Array(2 * n), pv = new Float64Array(2 * n);
    for (let i = 0; i < n; i++) {
      const zt = (y[i] - ym) / ysd;
      map[i] = i; map[n + i] = i; ys[i] = 1; ys[n + i] = -1; pv[i] = eps - zt; pv[n + i] = eps + zt;
    }
    const sol = svmSMO(Kd, n, map, ys, pv, C, (tipo === 'lineal' || rapido) ? 1e-2 : 2e-3, tipo === 'lineal' ? 3 * n : (rapido ? 4 * n : Math.min(60000, Math.max(3000, 8 * n))));
    const coef = new Float64Array(n); for (let i = 0; i < n; i++) coef[i] = sol.a[i] - sol.a[n + i];
    if (rep) rep(1, 'SVR · ' + sol.iter + ' iteraciones');
    const emp = svmEmpaquetar(Z, p, E, [{ coef: coef, b: sol.b }], tipo, gamma);
    let nota = 'núcleo ' + (tipo === 'lineal' ? 'lineal' : 'RBF') + ', ' + emp.nSV + ' vectores de soporte';
    const hall = [];
    if (sub) { nota = 'Como había ' + fmt(n0, 0) + ' filas y este método se vuelve muy lento con muchas, se entrenó con una muestra de ' + fmt(SVM_MAX, 0) + ' filas; ' + nota; hall.push('Se entrenó con una muestra de ' + fmt(SVM_MAX, 0) + ' de las ' + fmt(n0, 0) + ' filas de aprendizaje porque el método se vuelve muy lento con más datos. La evaluación sí usa todas las filas de prueba.'); }
    hall.push('La regresión por vectores de soporte usa ' + (tipo === 'lineal' ? 'núcleo lineal' : 'núcleo curvo (RBF)') + ' y se apoya en ' + fmt(emp.nSV, 0) + ' casos clave (' + fpct(emp.nSV / n, 0) + ' de los usados para aprender); ignora errores menores a ' + fmt(eps * ysd, 2) + ' unidades de la respuesta.');
    const d = new Float64Array(1);
    return {
      nref: emp.nref, nota: nota, hallazgos: hall, nSV: emp.nSV, submuestreo: sub ? { de: n0, a: n } : null,
      predecir: function (Xn) { const o = new Float64Array(Xn.length); for (let i = 0; i < Xn.length; i++) { emp.decisiones(Xn[i], d); o[i] = d[0] * ysd + ym; } return o; }
    };
  }

  // --- Red neuronal (perceptrón multicapa) con Adam, L2 y parada temprana ----------
  async function mlpAjustar(X, y, K, par, rng, rep) {
    const n = X.length, p = X[0].length, reg = K === 0, o = reg ? 1 : (K === 2 ? 1 : K);
    const E = estdAjustar(X), Z = new Float64Array(n * p);
    for (let i = 0; i < n; i++) for (let j = 0; j < p; j++) Z[i * p + j] = (X[i][j] - E.mu[j]) / E.sd[j];
    let ym = 0, ysd = 1, yt = null;
    if (reg) { ym = media(y); ysd = desvio(y, ym); if (!(ysd > 1e-12)) ysd = 1; yt = new Float64Array(n); for (let i = 0; i < n; i++) yt[i] = (y[i] - ym) / ysd; }
    const tanhAct = par.act === 'tanh', h1 = par.h, h2 = par.capas === 2 ? Math.max(4, Math.round(par.h / 2)) : 0;
    const sizes = h2 ? [p, h1, h2, o] : [p, h1, o], L = sizes.length - 1;
    // Parámetros en un solo arreglo (pesos + sesgos por capa)
    const offW = [], offB = []; let tot = 0;
    for (let l = 0; l < L; l++) { offW.push(tot); tot += sizes[l] * sizes[l + 1]; offB.push(tot); tot += sizes[l + 1]; }
    const th = new Float64Array(tot), gr = new Float64Array(tot), mA = new Float64Array(tot), vA = new Float64Array(tot);
    for (let l = 0; l < L; l++) {
      const esc = Math.sqrt((tanhAct || l === L - 1 ? 1 : 2) / sizes[l]);
      for (let k = 0; k < sizes[l] * sizes[l + 1]; k++) th[offW[l] + k] = rng.normal() * esc;
    }
    const A = sizes.map(function (s) { return new Float64Array(s); }), dz = sizes.map(function (s) { return new Float64Array(s); });
    function adelante(off) {
      const a0 = A[0]; for (let j = 0; j < p; j++) a0[j] = Z[off + j];
      for (let l = 0; l < L; l++) {
        const ai = A[l], ao = A[l + 1], ni = sizes[l], no = sizes[l + 1], w0 = offW[l], b0 = offB[l];
        for (let q = 0; q < no; q++) ao[q] = th[b0 + q];
        for (let i = 0; i < ni; i++) { const v = ai[i]; if (v === 0) continue; const wo = w0 + i * no; for (let q = 0; q < no; q++) ao[q] += v * th[wo + q]; }
        if (l < L - 1) { if (tanhAct) for (let q = 0; q < no; q++) ao[q] = Math.tanh(ao[q]); else for (let q = 0; q < no; q++) if (ao[q] < 0) ao[q] = 0; }
      }
    }
    // Pérdida de la muestra tras `adelante`; llena dz[L] con el gradiente de la salida
    function perdidaMuestra(i, gradiente) {
      const z = A[L], d = dz[L];
      if (reg) { const e = z[0] - yt[i]; if (gradiente) d[0] = e; return 0.5 * e * e; }
      if (K === 2) { const s = 1 / (1 + Math.exp(-z[0])), t = y[i]; if (gradiente) d[0] = s - t; return -(t === 1 ? Math.log(Math.max(s, 1e-12)) : Math.log(Math.max(1 - s, 1e-12))); }
      const pr = softmaxEn(z, K); if (gradiente) for (let k = 0; k < K; k++) d[k] = pr[k] - (k === y[i] ? 1 : 0);
      return -Math.log(Math.max(pr[y[i]], 1e-12));
    }
    // Partición interna 15 % para parada temprana
    const perm = []; for (let i = 0; i < n; i++) perm.push(i); rng.shuffle(perm);
    const usaVal = n >= 40, nVal = usaVal ? Math.max(5, Math.round(0.15 * n)) : 0;
    const val = perm.slice(0, nVal), trn = Int32Array.from(perm.slice(nVal));
    const nt = trn.length, bs = nt >= 1500 ? 64 : 32, lr = 0.005, l2 = par.l2, maxEp = par.epocas || 120, pac = 10;
    let mejorV = Infinity, mejorEp = 0, mejorTh = null, epUsadas = 0, t = 0, ultimaP = NaN;
    for (let ep = 1; ep <= maxEp; ep++) {
      rng.shuffle(trn);
      let suma = 0;
      for (let b0 = 0; b0 < nt; b0 += bs) {
        const b1 = Math.min(nt, b0 + bs), m = b1 - b0;
        gr.fill(0);
        for (let s = b0; s < b1; s++) {
          const i = trn[s];
          adelante(i * p);
          suma += perdidaMuestra(i, true);
          for (let l = L - 1; l >= 0; l--) {
            const ai = A[l], d = dz[l + 1], ni = sizes[l], no = sizes[l + 1], w0 = offW[l], b0o = offB[l];
            for (let q = 0; q < no; q++) gr[b0o + q] += d[q];
            for (let ii = 0; ii < ni; ii++) { const v = ai[ii]; if (v === 0) continue; const wo = w0 + ii * no; for (let q = 0; q < no; q++) gr[wo + q] += v * d[q]; }
            if (l > 0) {
              const dp = dz[l];
              for (let ii = 0; ii < ni; ii++) {
                let g = 0; const wo = w0 + ii * no; for (let q = 0; q < no; q++) g += th[wo + q] * d[q];
                const a = ai[ii]; dp[ii] = tanhAct ? g * (1 - a * a) : (a > 0 ? g : 0);
              }
            }
          }
        }
        t++;
        const c1 = 1 - Math.pow(0.9, t), c2 = 1 - Math.pow(0.999, t);
        for (let l = 0; l < L; l++) {
          const w0 = offW[l], nw = sizes[l] * sizes[l + 1];
          for (let k = w0; k < w0 + nw; k++) gr[k] += l2 * th[k] * m;
        }
        for (let k = 0; k < tot; k++) {
          const g = gr[k] / m;
          mA[k] = 0.9 * mA[k] + 0.1 * g; vA[k] = 0.999 * vA[k] + 0.001 * g * g;
          th[k] -= lr * (mA[k] / c1) / (Math.sqrt(vA[k] / c2) + 1e-8);
        }
      }
      const pt = suma / nt; ultimaP = pt; epUsadas = ep;
      let pv = pt;
      if (usaVal) { pv = 0; for (let s = 0; s < val.length; s++) { adelante(val[s] * p); pv += perdidaMuestra(val[s], false); } pv /= val.length; }
      if (pv < mejorV - 1e-5) { mejorV = pv; mejorEp = ep; mejorTh = Float64Array.from(th); }
      if (rep) rep(ep / maxEp, 'época ' + ep + ' · pérdida ' + fmt(pt, 3) + (usaVal ? ' (validación ' + fmt(pv, 3) + ')' : ''));
      const w = tk(); if (w) await w;
      if (usaVal && ep - mejorEp >= pac) break;
      if (!isFinite(pt)) break;
    }
    if (mejorTh) th.set(mejorTh);
    const detEp = usaVal ? (epUsadas < maxEp ? 'se detuvo en la época ' + epUsadas + ' porque dejó de mejorar con datos de validación; se conservó la mejor (época ' + mejorEp + ')' : 'llegó al máximo de ' + maxEp + ' épocas; se conservó la mejor (época ' + mejorEp + ')') : 'se entrenó ' + epUsadas + ' épocas (pocos datos para validar)';
    const arq = sizes.slice(1, -1).join(' y ');
    const nota = 'red con ' + (h2 ? '2 capas ocultas (' + arq + ' neuronas)' : '1 capa oculta de ' + arq + ' neuronas') + '; ' + detEp + '.';
    const hall = ['La red neuronal tiene ' + (h2 ? '2 capas ocultas (' + arq + ' neuronas)' : '1 capa oculta de ' + arq + ' neuronas') + ' y ' + detEp + '. Capta relaciones complejas, pero no se puede leer como una fórmula: para saber qué usa, mira las importancias.'];
    return {
      nota: nota, hallazgos: hall, epocas: epUsadas, perdida: ultimaP,
      predecir: function (Xn) {
        const m = Xn.length;
        const sal = reg ? new Float64Array(m) : new Array(m);
        const Zt = new Float64Array(p);
        for (let r = 0; r < m; r++) {
          const fila = Xn[r], a0 = A[0];
          for (let j = 0; j < p; j++) { Zt[j] = (fila[j] - E.mu[j]) / E.sd[j]; a0[j] = Zt[j]; }
          for (let l = 0; l < L; l++) {
            const ai = A[l], ao = A[l + 1], ni = sizes[l], no = sizes[l + 1], w0 = offW[l], b0 = offB[l];
            for (let q = 0; q < no; q++) ao[q] = th[b0 + q];
            for (let i = 0; i < ni; i++) { const v = ai[i]; if (v === 0) continue; const wo = w0 + i * no; for (let q = 0; q < no; q++) ao[q] += v * th[wo + q]; }
            if (l < L - 1) { if (tanhAct) for (let q = 0; q < no; q++) ao[q] = Math.tanh(ao[q]); else for (let q = 0; q < no; q++) if (ao[q] < 0) ao[q] = 0; }
          }
          const z = A[L];
          if (reg) sal[r] = z[0] * ysd + ym;
          else if (K === 2) { const s = 1 / (1 + Math.exp(-z[0])); sal[r] = Float64Array.of(1 - s, s); }
          else sal[r] = softmaxEn(z, K);
        }
        return sal;
      }
    };
  }

  // --- Gradient boosting de árboles pequeños ----------------------------------------
  function hojaIdArbol(T, row) { let k = 0; while (T.feat[k] >= 0) k = row[T.feat[k]] <= T.thr[k] ? T.izq[k] : T.der[k]; return k; }

  async function gbmAjustar(X, y, K, par, rng) {
    const n = X.length, p = X[0].length, reg = K === 0, nc = (reg || K === 2) ? 1 : K;
    const Bn = crearBins(X, 64), lr = par.tasa, rondas = par.rondas;
    const perm = []; for (let i = 0; i < n; i++) perm.push(i); rng.shuffle(perm);
    const usaVal = n >= 60, nVal = usaVal ? Math.max(8, Math.round(0.15 * n)) : 0;
    const val = Int32Array.from(perm.slice(0, nVal)), trn = Int32Array.from(perm.slice(nVal)), nt = trn.length;
    // ¿Hay atípicos en la respuesta? Entonces pérdida de Huber (solo regresión)
    let huber = false;
    if (reg) {
      if (par.perdida === 'huber') huber = true;
      else if (par.perdida !== 'cuadratica') {
        const md = mediana(Array.prototype.slice.call(y)), ad = Array.prototype.slice.call(y).map(function (v) { return Math.abs(v - md); }), mad = 1.4826 * mediana(ad);
        if (mad > 0) { let c = 0; for (let i = 0; i < n; i++) if (ad[i] > 3.5 * mad) c++; huber = c / n >= 0.03; }
      }
    }
    // Predicción inicial
    const F = []; for (let k = 0; k < nc; k++) F.push(new Float64Array(n));
    let F0 = new Float64Array(nc);
    if (reg) { const yy = Array.prototype.slice.call(elegirFilas(y, trn)); F0[0] = huber ? mediana(yy) : media(yy); }
    else if (K === 2) { let c1 = 0; for (let s = 0; s < nt; s++) c1 += y[trn[s]]; const pp = Math.min(0.999, Math.max(0.001, c1 / nt)); F0[0] = Math.log(pp / (1 - pp)); }
    else { const cn = new Float64Array(K); for (let s = 0; s < nt; s++) cn[y[trn[s]]]++; for (let k = 0; k < K; k++) F0[k] = Math.log((cn[k] + 1) / (nt + K)); }
    for (let k = 0; k < nc; k++) F[k].fill(F0[k]);
    const arboles = [], g = new Float64Array(n), hs = new Float64Array(n), lp = new Float64Array(K || 1), Gm = [];
    if (nc > 1) for (let k = 0; k < nc; k++) Gm.push(new Float64Array(n));
    const nsub = Math.max(10, Math.round((par.sub || 1) * nt));
    const hoja_min = Math.max(3, Math.min(10, Math.floor(nt / 40)));
    const perdidaVal = function () {
      let s = 0;
      for (let q = 0; q < nVal; q++) {
        const i = val[q];
        if (reg) { const e = y[i] - F[0][i]; s += e * e; }
        else if (K === 2) { const z = F[0][i]; s += (z > 0 ? z + Math.log1p(Math.exp(-z)) : Math.log1p(Math.exp(z))) - y[i] * z; }
        else { for (let k = 0; k < K; k++) lp[k] = F[k][i]; const sm = softmaxEn(lp, K); s -= Math.log(Math.max(sm[y[i]], 1e-12)); }
      }
      return s / nVal;
    };
    let mejorV = Infinity, mejorR = 0, rondasHechas = 0; const pac = 15;
    for (let r = 1; r <= rondas; r++) {
      // Residuos (gradiente negativo) de todas las filas de entrenamiento
      let delta = 0;
      if (reg && huber) { const ab = new Float64Array(nt); for (let s = 0; s < nt; s++) ab[s] = Math.abs(y[trn[s]] - F[0][trn[s]]); delta = Math.max(1e-9, cuantilOrd(ordenado(ab), 0.9)); }
      for (let s = 0; s < nt; s++) {
        const i = trn[s];
        if (reg) { const e = y[i] - F[0][i]; g[i] = huber ? Math.max(-delta, Math.min(delta, e)) : e; }
        else if (K === 2) { const pb = 1 / (1 + Math.exp(-F[0][i])); g[i] = y[i] - pb; hs[i] = Math.max(pb * (1 - pb), 1e-9); }
      }
      if (nc > 1) for (let s = 0; s < nt; s++) { const i = trn[s]; for (let k = 0; k < K; k++) lp[k] = F[k][i]; const sm = softmaxEn(lp, K); for (let k = 0; k < K; k++) Gm[k][i] = (y[i] === k ? 1 : 0) - sm[k]; }
      const ronda = [];
      for (let k = 0; k < nc; k++) {
        const gk = nc > 1 ? Gm[k] : g;
        const sel = rng.muestra(nt, nsub), idx = new Int32Array(sel.length); for (let a = 0; a < sel.length; a++) idx[a] = trn[sel[a]];
        const T = construirArbol(Bn, gk, 0, idx, { profundidad: par.profundidad, hoja_min: hoja_min, mtry: p }, rng);
        // Valor de cada hoja: regresión = media del residuo; clasificación = paso de Newton
        if (!reg) {
          const num = new Float64Array(T.feat.length), den = new Float64Array(T.feat.length);
          for (let a = 0; a < idx.length; a++) {
            const i = idx[a], h = hojaIdArbol(T, X[i]), rr = gk[i];
            num[h] += rr; den[h] += nc > 1 ? Math.abs(rr) * (1 - Math.abs(rr)) : hs[i];
          }
          for (let h = 0; h < T.feat.length; h++) if (T.feat[h] < 0) { let v = den[h] > 1e-9 ? num[h] / den[h] : 0; if (nc > 1) v *= (K - 1) / K; T.val[h] = Math.max(-6, Math.min(6, v)); }
        }
        for (let h = 0; h < T.feat.length; h++) if (T.feat[h] < 0) T.val[h] = lr * T.val[h];
        ronda.push(T);
        for (let s = 0; s < nt; s++) { const i = trn[s]; F[k][i] += T.val[hojaIdArbol(T, X[i])]; }
        for (let q = 0; q < nVal; q++) { const i = val[q]; F[k][i] += T.val[hojaIdArbol(T, X[i])]; }
      }
      arboles.push(ronda); rondasHechas = r;
      if (usaVal) {
        const pv = perdidaVal();
        if (pv < mejorV - 1e-6) { mejorV = pv; mejorR = r; }
        if (r - mejorR >= pac) break;
      }
      const w = tk(); if (w) await w;
    }
    const usar = usaVal ? Math.max(1, mejorR) : rondasHechas;
    arboles.length = usar;
    const nota = usar + ' árboles de profundidad ' + par.profundidad + (usaVal && usar < rondas ? ' (la parada temprana detuvo el entrenamiento: más rondas ya no mejoraban con datos de validación)' : '') + (huber ? '; pérdida de Huber por atípicos' : '') + '.';
    const hall = ['El modelo suma ' + usar + ' árboles pequeños (profundidad ' + par.profundidad + ') que corrigen los errores de los anteriores' + (usaVal && usar < rondas ? '; se detuvo antes del máximo de ' + rondas + ' porque ya no mejoraba con datos de validación' : '') + '. Suele ser muy preciso, pero no se lee como una regla única.'];
    if (huber) hall.push('Se detectaron valores atípicos en la respuesta, así que se usó una pérdida robusta (Huber) para que no desvíen el modelo.');
    return {
      nota: nota, hallazgos: hall, nArboles: usar, huber: huber,
      predecir: function (Xn) {
        const m = Xn.length, T = arboles.length;
        if (reg) { const o = new Float64Array(m); for (let i = 0; i < m; i++) { let s = F0[0]; for (let t = 0; t < T; t++) s += predecirArbol(arboles[t][0], Xn[i]); o[i] = s; } return o; }
        const o = new Array(m), l = new Float64Array(nc);
        for (let i = 0; i < m; i++) {
          for (let k = 0; k < nc; k++) l[k] = F0[k];
          for (let t = 0; t < T; t++) for (let k = 0; k < nc; k++) l[k] += predecirArbol(arboles[t][k], Xn[i]);
          if (K === 2) { const s = 1 / (1 + Math.exp(-l[0])); o[i] = Float64Array.of(1 - s, s); } else o[i] = softmaxEn(l, K);
        }
        return o;
      }
    };
  }

  // --- Registro de modelos: valores por defecto y rejillas de calibración --------
  function unicosOrd(a) { const r = unicosArr(a).filter(function (x) { return x >= 1; }); return r.sort(function (x, y) { return x - y; }); }
  const MODELOS = {
    ols: { tipo: 'reg', defecto: function () { return {}; }, rejilla: function () { return null; }, ajustar: async function (X, y) { return olsAjustar(X, y); } },
    ridge: { tipo: 'reg', defecto: function () { return { lambda: 0.1 }; }, rejilla: function () { return [0.001, 0.01, 0.1, 1, 10, 100].map(function (l) { return { lambda: l }; }); }, ajustar: async function (X, y, par) { return ridgeAjustar(X, y, par.lambda); } },
    lasso: { tipo: 'reg', defecto: function () { return { lambda: 0.01 }; }, rejilla: function () { return [0.0005, 0.002, 0.01, 0.03, 0.1, 0.3].map(function (l) { return { lambda: l }; }); }, ajustar: async function (X, y, par) { return lassoAjustar(X, y, par.lambda); } },
    arbol_reg: { tipo: 'reg', defecto: function () { return { profundidad: 5, hoja_min: 10 }; }, rejilla: function (c) { return rejillaArbol(c); }, ajustar: async function (X, y, par, c) { return bosqueAjustar(X, y, 0, { profundidad: par.profundidad, hoja_min: par.hoja_min, mtry: X[0].length }, c.rng, false); } },
    bosque_reg: { tipo: 'reg', defecto: function (c) { return { arboles: 100, mtry: Math.max(1, Math.floor(c.p / 3)) }; }, rejilla: function (c) { return rejillaBosque(c, false); }, ajustar: async function (X, y, par, c) { return bosqueAjustar(X, y, 0, { arboles: par.arboles, profundidad: 16, hoja_min: 5, mtry: par.mtry }, c.rng, true); } },
    knn_reg: { tipo: 'reg', evaluarRejilla: evaluarRejillaKnn, defecto: function (c) { return { k: Math.max(1, Math.min(10, Math.floor(c.n / 3))) }; }, rejilla: function (c) { return rejillaKnn(c); }, ajustar: async function (X, y, par, c) { return knnAjustar(X, y, par.k, 0, c.rng); } },
    logistica: { tipo: 'clf', defecto: function () { return {}; }, rejilla: function () { return null; }, ajustar: async function (X, y, par, c) { return logisticaAjustar(X, y, c.K, 1e-3); } },
    arbol_clf: { tipo: 'clf', defecto: function () { return { profundidad: 5, hoja_min: 10 }; }, rejilla: function (c) { return rejillaArbol(c); }, ajustar: async function (X, y, par, c) { return bosqueAjustar(X, y, c.K, { profundidad: par.profundidad, hoja_min: par.hoja_min, mtry: X[0].length }, c.rng, false); } },
    bosque_clf: { tipo: 'clf', defecto: function (c) { return { arboles: 100, mtry: Math.max(1, Math.floor(Math.sqrt(c.p))) }; }, rejilla: function (c) { return rejillaBosque(c, true); }, ajustar: async function (X, y, par, c) { return bosqueAjustar(X, y, c.K, { arboles: par.arboles, profundidad: 16, hoja_min: 3, mtry: par.mtry }, c.rng, true); } },
    knn_clf: { tipo: 'clf', evaluarRejilla: evaluarRejillaKnn, defecto: function (c) { return { k: Math.max(1, Math.min(10, Math.floor(c.n / 3))) }; }, rejilla: function (c) { return rejillaKnn(c); }, ajustar: async function (X, y, par, c) { return knnAjustar(X, y, par.k, c.K, c.rng); } },
    naive_bayes: { tipo: 'clf', defecto: function () { return {}; }, rejilla: function () { return null; }, ajustar: async function (X, y, par, c) { return nbAjustar(X, y, c.K); } },
    lda: { tipo: 'clf', defecto: function () { return {}; }, rejilla: function () { return null; }, ajustar: async function (X, y, par, c) { return ldaAjustar(X, y, c.K); } },
    svm_clf: { tipo: 'clf', defecto: function (c) { return { kernel: 'rbf', C: 1, gamma: redond(1 / c.p) }; }, rejilla: function (c) { return rejillaSvm(c, false); }, ajustar: async function (X, y, par, c) { return svmClfAjustar(X, y, c.K, par, c.rng, c.rep, c.cv); } },
    svm_reg: { tipo: 'reg', defecto: function (c) { return { kernel: 'rbf', C: 1, epsilon: 0.1, gamma: redond(1 / c.p) }; }, rejilla: function (c) { return rejillaSvm(c, true); }, ajustar: async function (X, y, par, c) { return svmRegAjustar(X, y, par, c.rng, c.rep, c.cv); } },
    mlp_clf: { tipo: 'clf', defecto: function (c) { return { capas: c.n >= 1000 ? 2 : 1, h: 16, l2: 0.0001, act: c.n < 300 ? 'tanh' : 'relu' }; }, rejilla: function (c) { return rejillaMlp(c); }, ajustar: async function (X, y, par, c) { return mlpAjustar(X, y, c.K, par, c.rng, c.rep); } },
    mlp_reg: { tipo: 'reg', defecto: function (c) { return { capas: c.n >= 1000 ? 2 : 1, h: 16, l2: 0.0001, act: c.n < 300 ? 'tanh' : 'relu' }; }, rejilla: function (c) { return rejillaMlp(c); }, ajustar: async function (X, y, par, c) { return mlpAjustar(X, y, 0, par, c.rng, c.rep); } },
    gbm_clf: { tipo: 'clf', defecto: function () { return { rondas: 200, profundidad: 3, tasa: 0.1, sub: 0.8 }; }, rejilla: function (c) { return rejillaGbm(c); }, ajustar: async function (X, y, par, c) { return gbmAjustar(X, y, c.K, par, c.rng); } },
    gbm_reg: { tipo: 'reg', defecto: function () { return { rondas: 200, profundidad: 3, tasa: 0.1, sub: 0.8 }; }, rejilla: function (c) { return rejillaGbm(c); }, ajustar: async function (X, y, par, c) { return gbmAjustar(X, y, 0, par, c.rng); } }
  };
  function redond(x) { return Number(x.toPrecision(3)); }
  function rejillaSvm(c, reg) {
    const r = [];
    [1, 10].forEach(function (C) { [0.5, 2].forEach(function (g) { r.push(reg ? { kernel: 'rbf', C: C, epsilon: 0.1, gamma: redond(g / c.p) } : { kernel: 'rbf', C: C, gamma: redond(g / c.p) }); }); });
    if (reg) r.push({ kernel: 'rbf', C: 1, epsilon: 0.3, gamma: redond(1 / c.p) });
    r.push(reg ? { kernel: 'lineal', C: 1, epsilon: 0.1 } : { kernel: 'lineal', C: 1 });
    return r;
  }
  function rejillaMlp(c) {
    const capas = c.n >= 1000 ? 2 : 1, act = c.n < 300 ? 'tanh' : 'relu', r = [];
    [8, 16, 32].forEach(function (h) { [0.0001, 0.01].forEach(function (l2) { r.push({ capas: capas, h: h, l2: l2, act: act }); }); });
    return r;
  }
  function rejillaGbm(c) {
    const r = [];
    [100, 300].forEach(function (t) { [2, 3, 4].forEach(function (d) { [0.05, 0.15].forEach(function (lr) { r.push({ rondas: t, profundidad: d, tasa: lr, sub: 0.8 }); }); }); });
    return r;
  }
  function rejillaArbol(c) {
    const r = [];
    [3, 5, 8, 12].forEach(function (d) { [3, 10, 25].forEach(function (h) { if (h <= c.n / 6 || h === 3) r.push({ profundidad: d, hoja_min: h }); }); });
    return r;
  }
  function rejillaBosque(c, clf) {
    const p = c.p, a = clf ? Math.ceil(Math.sqrt(p)) : Math.ceil(p / 3), b = clf ? Math.ceil(p / 3) : Math.ceil(p / 2);
    const ms = unicosOrd([Math.min(p, Math.max(1, a)), Math.min(p, Math.max(1, b))]);
    const r = [];
    [50, 100].forEach(function (t) { ms.forEach(function (m) { r.push({ arboles: t, mtry: m }); }); });
    return r;
  }
  function rejillaKnn(c) {
    const r = [3, 5, 10, 20, 40].filter(function (k) { return k <= c.n / 3; }).map(function (k) { return { k: k }; });
    return r.length ? r : [{ k: Math.max(1, Math.floor(c.n / 3)) }];
  }
  /** Calibra todos los k de una vez: una sola búsqueda de vecinos por pliegue. */
  async function evaluarRejillaKnn(configs, folds, ctx, esReg) {
    const ks = configs.map(function (c) { return c.k; }), kmax = Math.max.apply(null, ks);
    const suma = new Float64Array(configs.length), preds = configs.map(function () { return []; });
    for (let f = 0; f < folds.length; f++) {
      const mod = knnAjustar(folds[f].Xtr, folds[f].ytr, kmax, ctx.K, ctx.rng);
      const porK = mod.predecirKs(folds[f].Xva, ks);
      porK.forEach(function (pr, c) { preds[c].push(pr); suma[c] += esReg ? rmseDe(folds[f].yva, pr) : logLossDe(folds[f].yva, pr); });
      const w = tk(); if (w) await w;
    }
    return configs.map(function (c, i) { return { score: suma[i] / folds.length, preds: preds[i] }; });
  }
  function ajustarModelo(id, X, y, par, ctx) { return MODELOS[id].ajustar(X, y, par, ctx); }

  /** Predice por trozos para poder ceder el control al navegador. */
  async function predecirAsync(modelo, X) {
    const m = X.length, trozo = modelo.nref && modelo.nref > 500 ? Math.max(20, Math.floor(4e5 / modelo.nref)) : 2000;
    if (m <= trozo) return modelo.predecir(X);
    const partes = [];
    for (let i = 0; i < m; i += trozo) { partes.push(modelo.predecir(X.slice(i, i + trozo))); const w = tk(); if (w) await w; }
    if (partes[0] instanceof Float64Array) { const o = new Float64Array(m); let off = 0; partes.forEach(function (p) { o.set(p, off); off += p.length; }); return o; }
    return [].concat.apply([], partes);
  }

  // ==========================================================================
  // 8. MÉTRICAS Y VALIDACIÓN
  // ==========================================================================

  function metricasReg(real, pred, p) {
    const n = real.length;
    let sse = 0, sae = 0, sb = 0, sm = 0, todosPos = true, mape = 0;
    for (let i = 0; i < n; i++) {
      const e = real[i] - pred[i]; sse += e * e; sae += Math.abs(e); sb += pred[i] - real[i]; sm += real[i];
      if (real[i] > 0) mape += Math.abs(e) / real[i]; else todosPos = false;
    }
    const my = sm / n; let sst = 0; for (let i = 0; i < n; i++) sst += (real[i] - my) * (real[i] - my);
    const r2 = sst > 1e-300 ? 1 - sse / sst : null;
    const r2aj = (r2 !== null && n - p - 1 > 0) ? 1 - (1 - r2) * (n - 1) / (n - p - 1) : null;
    const rmse = Math.sqrt(sse / n), sdy = Math.sqrt(sst / Math.max(1, n - 1));
    return { r2: r2, r2_ajustado: r2aj, rmse: rmse, mae: sae / n, mape: todosPos ? 100 * mape / n : null, sesgo: sb / n, ratio_rmse_sd: sdy > 1e-300 ? rmse / sdy : null };
  }

  /** AUC por rangos (equivale a la probabilidad de que un positivo puntúe más que un negativo). */
  function aucBinaria(score, pos) {
    const n = score.length; let nP = 0; for (let i = 0; i < n; i++) if (pos[i]) nP++;
    const nN = n - nP; if (!nP || !nN) return null;
    const r = rangos(score); let sr = 0; for (let i = 0; i < n; i++) if (pos[i]) sr += r[i];
    return (sr - nP * (nP + 1) / 2) / (nP * nN);
  }
  function curvaROC(score, pos) {
    const n = score.length, idx = new Array(n); for (let i = 0; i < n; i++) idx[i] = i;
    idx.sort(function (a, b) { return score[b] - score[a]; });
    let nP = 0; for (let i = 0; i < n; i++) if (pos[i]) nP++;
    const nN = n - nP; if (!nP || !nN) return { fpr: [0, 1], tpr: [0, 1] };
    const fpr = [0], tpr = [0]; let tp = 0, fp = 0, i = 0;
    while (i < n) {
      let j = i; while (j < n && score[idx[j]] === score[idx[i]]) { if (pos[idx[j]]) tp++; else fp++; j++; }
      fpr.push(fp / nN); tpr.push(tp / nP); i = j;
    }
    if (fpr.length > 200) { const f2 = [], t2 = [], paso = (fpr.length - 1) / 199; for (let k = 0; k < 200; k++) { const q = Math.round(k * paso); f2.push(fpr[q]); t2.push(tpr[q]); } return { fpr: f2, tpr: t2 }; }
    return { fpr: fpr, tpr: tpr };
  }

  function metricasClf(real, probs, K, clases) {
    const n = real.length, M = []; for (let a = 0; a < K; a++) M.push(new Array(K).fill(0));
    let ok = 0, ll = 0; const pred = new Int32Array(n);
    for (let i = 0; i < n; i++) {
      const pr = probs[i], c = maxIdx(pr); pred[i] = c; M[real[i]][c]++; if (c === real[i]) ok++;
      ll -= Math.log(Math.max(pr[real[i]], 1e-15));
    }
    const porClase = {}; let sp = 0, sr = 0, sf = 0, rb = 0, nc = 0;
    for (let c = 0; c < K; c++) {
      let tp = M[c][c], fn = 0, fp = 0;
      for (let k = 0; k < K; k++) { if (k !== c) { fn += M[c][k]; fp += M[k][c]; } }
      const prec = tp + fp > 0 ? tp / (tp + fp) : 0, rec = tp + fn > 0 ? tp / (tp + fn) : 0;
      const f1 = prec + rec > 0 ? 2 * prec * rec / (prec + rec) : 0;
      porClase[clases[c]] = { precision: prec, recall: rec, f1: f1, n: tp + fn };
      if (tp + fn > 0) { sp += prec; sr += rec; sf += f1; rb += rec; nc++; }
    }
    let pe = 0;
    for (let c = 0; c < K; c++) { let fila = 0, col = 0; for (let k = 0; k < K; k++) { fila += M[c][k]; col += M[k][c]; } pe += fila * col; }
    pe /= n * n;
    const po = ok / n;
    let auc = null, roc = null;
    if (K === 2) {
      const sc = new Float64Array(n), pos = new Array(n);
      for (let i = 0; i < n; i++) { sc[i] = probs[i][1]; pos[i] = real[i] === 1; }
      auc = aucBinaria(sc, pos); roc = curvaROC(sc, pos);
    } else {
      let s = 0, c2 = 0; const lista = [];
      const scT = [], posT = [];
      for (let c = 0; c < K; c++) {
        const sc = new Float64Array(n), pos = new Array(n);
        for (let i = 0; i < n; i++) { sc[i] = probs[i][c]; pos[i] = real[i] === c; scT.push(sc[i]); posT.push(pos[i]); }
        const a = aucBinaria(sc, pos); lista.push({ clase: clases[c], auc: a });
        if (a !== null) { s += a; c2++; }
      }
      auc = c2 ? s / c2 : null;
      const micro = curvaROC(Float64Array.from(scT), posT);
      roc = { fpr: micro.fpr, tpr: micro.tpr, porClase: lista, tipo: 'micro' };
    }
    const m = {
      exactitud: po, exactitud_balanceada: nc ? rb / nc : null, precision_macro: nc ? sp / nc : null, recall_macro: nc ? sr / nc : null, f1_macro: nc ? sf / nc : null,
      auc: auc, log_loss: ll / n, kappa: pe < 1 ? (po - pe) / (1 - pe) : null, porClase: porClase
    };
    if (K === 2) { const pc = porClase[clases[1]]; m.precision = pc.precision; m.recall = pc.recall; m.f1 = pc.f1; }
    return { metricas: m, matriz: M, roc: roc, pred: pred };
  }

  function curvaCalibracion(real, probs, K) {
    const B = 10, sp = new Float64Array(B), so = new Float64Array(B), nb = new Float64Array(B);
    for (let i = 0; i < real.length; i++) {
      let s, o;
      if (K === 2) { s = probs[i][1]; o = real[i] === 1 ? 1 : 0; } else { const c = maxIdx(probs[i]); s = probs[i][c]; o = c === real[i] ? 1 : 0; }
      const b = Math.min(B - 1, Math.floor(s * B)); sp[b] += s; so[b] += o; nb[b]++;
    }
    const bins = []; for (let b = 0; b < B; b++) if (nb[b] > 0) bins.push({ p_media: sp[b] / nb[b], frec_obs: so[b] / nb[b], n: nb[b] });
    return { bins: bins };
  }

  /** Diagnóstico de residuos: normalidad (Jarque-Bera), varianza (tipo Breusch-Pagan) y Durbin-Watson. */
  function diagResiduos(real, pred, idxOrig) {
    const n = real.length, e = new Float64Array(n);
    for (let i = 0; i < n; i++) e[i] = real[i] - pred[i];
    const me = media(e), sc = sesgoCurtosis(e, me);
    const jb = n / 6 * (sc.sesgo * sc.sesgo + sc.curtosis * sc.curtosis / 4), pJB = Math.exp(-jb / 2);
    const e2 = new Float64Array(n); for (let i = 0; i < n; i++) e2[i] = e[i] * e[i];
    const r = pearson(pred, e2), lm = isNaN(r) ? 0 : n * r * r;
    const ord = new Array(n); for (let i = 0; i < n; i++) ord[i] = i;
    ord.sort(function (a, b) { return idxOrig[a] - idxOrig[b]; });
    let num = 0, den = 0;
    for (let i = 0; i < n; i++) { den += e[i] * e[i]; if (i > 0) { const d = e[ord[i]] - e[ord[i - 1]]; num += d * d; } }
    return {
      valor: e, sesgoResiduos: sc.sesgo, curtosisResiduos: sc.curtosis,
      normalidad: { prueba: 'Jarque-Bera', estadistico: jb, p: pJB, shapiroLike: pJB },
      heterocedasticidad: { breuschPaganLike: { estadistico: lm, p: 1 - chi2CDF(lm, 1) } },
      durbinWatson: den > 1e-300 ? num / den : null
    };
  }

  /** Reparte posiciones 0..n-1 en entrenamiento y prueba (estratificado si hay clases). */
  function dividir(n, prop, rng, clases) {
    const train = [], test = [];
    if (clases) {
      const g = new Map();
      for (let i = 0; i < n; i++) { if (!g.has(clases[i])) g.set(clases[i], []); g.get(clases[i]).push(i); }
      g.forEach(function (l) {
        rng.shuffle(l);
        let nt = Math.round(prop * l.length); nt = Math.max(1, Math.min(l.length - 1, nt));
        for (let i = 0; i < l.length; i++) (i < nt ? test : train).push(l[i]);
      });
    } else {
      const l = []; for (let i = 0; i < n; i++) l.push(i);
      rng.shuffle(l);
      const nt = Math.max(1, Math.min(n - 1, Math.round(prop * n)));
      for (let i = 0; i < n; i++) (i < nt ? test : train).push(l[i]);
    }
    train.sort(function (a, b) { return a - b; }); test.sort(function (a, b) { return a - b; });
    return { train: train, test: test };
  }
  /** Pliegues de validación cruzada (listas de posiciones de validación). */
  function crearFolds(n, k, rng, clases) {
    const folds = []; for (let i = 0; i < k; i++) folds.push([]);
    let cont = 0;
    if (clases) {
      const g = new Map();
      for (let i = 0; i < n; i++) { if (!g.has(clases[i])) g.set(clases[i], []); g.get(clases[i]).push(i); }
      g.forEach(function (l) { rng.shuffle(l); l.forEach(function (i) { folds[cont % k].push(i); cont++; }); });
    } else {
      const l = []; for (let i = 0; i < n; i++) l.push(i);
      rng.shuffle(l); l.forEach(function (i) { folds[cont % k].push(i); cont++; });
    }
    return folds;
  }

  function promediarMetricas(lista) {
    const med = {}, sd = {};
    if (!lista.length) return { media: med, sd: sd, folds: lista };
    Object.keys(lista[0]).forEach(function (k) {
      if (typeof lista[0][k] === 'object' && lista[0][k] !== null) return;
      const v = lista.map(function (m) { return m[k]; }).filter(function (x) { return typeof x === 'number' && isFinite(x); });
      med[k] = v.length ? media(v) : null; sd[k] = v.length > 1 ? desvio(v) : (v.length ? 0 : null);
    });
    return { media: med, sd: sd, folds: lista };
  }

  // ==========================================================================
  // 9. MÉTODOS NO SUPERVISADOS
  // ==========================================================================

  function dist2(a, b) { let s = 0; for (let j = 0; j < a.length; j++) { const t = a[j] - b[j]; s += t * t; } return s; }

  function pcaDe(Xs, p) {
    const n = Xs.length, cv = covarianza(Xs, null, p), eig = jacobi(cv.C, p);
    const vals = Array.prototype.map.call(eig.vals, function (v) { return Math.max(0, v); });
    const tot = sumaArr(vals) || 1, varianza = vals.map(function (v) { return v / tot; });
    const acumulada = []; let ac = 0; varianza.forEach(function (v) { ac += v; acumulada.push(ac); });
    let n80 = acumulada.findIndex(function (a) { return a >= 0.8 - 1e-12; }) + 1; if (n80 <= 0) n80 = p;
    const nPC = Math.min(3, p), scores = new Array(n);
    for (let i = 0; i < n; i++) {
      const s = new Array(nPC).fill(0);
      for (let c = 0; c < nPC; c++) { let t = 0; for (let j = 0; j < p; j++) t += (Xs[i][j] - cv.mu[j]) * eig.vecs[j][c]; s[c] = t; }
      scores[i] = s;
    }
    const k5 = Math.min(5, p), cargas = [];
    for (let j = 0; j < p; j++) { const pc = []; for (let c = 0; c < k5; c++) pc.push(eig.vecs[j][c] * Math.sqrt(vals[c])); cargas.push({ idxVar: j, pc: pc }); }
    return { varianza: varianza, acumulada: acumulada, cargas: cargas, scores: scores, nComponentes80: n80, autovalores: vals };
  }

  async function kmeansAjustar(Xs, k, rng, nInit) {
    const n = Xs.length, p = Xs[0].length;
    let mejor = null;
    for (let ini = 0; ini < nInit; ini++) {
      // k-means++
      const C = [Float64Array.from(Xs[rng.int(n)])], d2 = new Float64Array(n).fill(Infinity);
      for (let c = 1; c < k; c++) {
        let tot = 0; const u = C[c - 1];
        for (let i = 0; i < n; i++) { const d = dist2(Xs[i], u); if (d < d2[i]) d2[i] = d; tot += d2[i]; }
        let elegido = rng.int(n);
        if (tot > 0) { let r = rng.next() * tot; for (let i = 0; i < n; i++) { r -= d2[i]; if (r <= 0) { elegido = i; break; } } }
        C.push(Float64Array.from(Xs[elegido]));
      }
      const lab = new Int32Array(n).fill(-1);
      for (let it = 0; it < 100; it++) {
        let cambio = 0;
        for (let i = 0; i < n; i++) {
          let b = 0, bd = Infinity;
          for (let c = 0; c < k; c++) { const d = dist2(Xs[i], C[c]); if (d < bd) { bd = d; b = c; } }
          if (lab[i] !== b) { lab[i] = b; cambio++; }
        }
        const S = [], cnt = new Float64Array(k); for (let c = 0; c < k; c++) S.push(new Float64Array(p));
        for (let i = 0; i < n; i++) { cnt[lab[i]]++; const s = S[lab[i]], r = Xs[i]; for (let j = 0; j < p; j++) s[j] += r[j]; }
        for (let c = 0; c < k; c++) {
          if (cnt[c] > 0) { for (let j = 0; j < p; j++) C[c][j] = S[c][j] / cnt[c]; }
          else { // grupo vacío: se reubica en el punto más lejano
            let fi = 0, fd = -1; for (let i = 0; i < n; i++) { const d = dist2(Xs[i], C[lab[i]]); if (d > fd) { fd = d; fi = i; } }
            C[c] = Float64Array.from(Xs[fi]); lab[fi] = c; cambio++;
          }
        }
        if (!cambio) break;
        const w = tk(); if (w) await w;
      }
      let inercia = 0; for (let i = 0; i < n; i++) inercia += dist2(Xs[i], C[lab[i]]);
      if (!mejor || inercia < mejor.inercia) mejor = { labels: lab, centros: C, inercia: inercia };
    }
    return mejor;
  }

  /** Silueta media sobre una muestra de hasta maxN puntos (ignora ruido: etiqueta < 0). */
  function silueta(Xs, labels, maxN, rng) {
    let ids = []; let kmax = 0;
    for (let i = 0; i < labels.length; i++) if (labels[i] >= 0) { ids.push(i); if (labels[i] + 1 > kmax) kmax = labels[i] + 1; }
    if (ids.length > maxN) ids = rng.muestra(ids.length, maxN).map(function (i) { return ids[i]; });
    const m = ids.length, tam = new Float64Array(kmax);
    for (let a = 0; a < m; a++) tam[labels[ids[a]]]++;
    let presentes = 0; for (let c = 0; c < kmax; c++) if (tam[c] > 0) presentes++;
    if (presentes < 2) return null;
    let tot = 0;
    const sums = new Float64Array(kmax);
    for (let a = 0; a < m; a++) {
      sums.fill(0); const i = ids[a], ri = Xs[i];
      for (let b = 0; b < m; b++) { if (a === b) continue; sums[labels[ids[b]]] += Math.sqrt(dist2(ri, Xs[ids[b]])); }
      const li = labels[i];
      if (tam[li] <= 1) continue;
      const A = sums[li] / (tam[li] - 1); let B = Infinity;
      for (let c = 0; c < kmax; c++) if (c !== li && tam[c] > 0) B = Math.min(B, sums[c] / tam[c]);
      const mx = Math.max(A, B); if (mx > 0) tot += (B - A) / mx;
    }
    return tot / m;
  }
  /** Índices de Calinski-Harabasz (más alto = mejor) y Davies-Bouldin (más bajo = mejor). */
  function indicesInternos(Xs, labels) {
    const p = Xs[0].length; let kmax = 0, n = 0;
    for (let i = 0; i < labels.length; i++) if (labels[i] >= 0) { n++; if (labels[i] + 1 > kmax) kmax = labels[i] + 1; }
    const C = [], cnt = new Float64Array(kmax), g = new Float64Array(p);
    for (let c = 0; c < kmax; c++) C.push(new Float64Array(p));
    for (let i = 0; i < labels.length; i++) { if (labels[i] < 0) continue; cnt[labels[i]]++; for (let j = 0; j < p; j++) { C[labels[i]][j] += Xs[i][j]; g[j] += Xs[i][j]; } }
    for (let j = 0; j < p; j++) g[j] /= Math.max(1, n);
    let k = 0; for (let c = 0; c < kmax; c++) if (cnt[c] > 0) { k++; for (let j = 0; j < p; j++) C[c][j] /= cnt[c]; }
    if (k < 2 || n <= k) return { ch: null, db: null };
    let wss = 0, bss = 0; const s = new Float64Array(kmax);
    for (let i = 0; i < labels.length; i++) { if (labels[i] < 0) continue; const d = dist2(Xs[i], C[labels[i]]); wss += d; s[labels[i]] += Math.sqrt(d); }
    for (let c = 0; c < kmax; c++) if (cnt[c] > 0) { bss += cnt[c] * dist2(C[c], g); s[c] /= cnt[c]; }
    let db = 0;
    for (let a = 0; a < kmax; a++) {
      if (!cnt[a]) continue; let mx = 0;
      for (let b = 0; b < kmax; b++) { if (a === b || !cnt[b]) continue; const d = Math.sqrt(dist2(C[a], C[b])); if (d > 0) mx = Math.max(mx, (s[a] + s[b]) / d); }
      db += mx;
    }
    return { ch: wss > 0 ? (bss / (k - 1)) / (wss / (n - k)) : null, db: db / k };
  }

  /** Ward con el algoritmo de cadena de vecinos más cercanos (O(n²)). Devuelve las fusiones ordenadas por altura. */
  async function wardFusiones(Xs) {
    const n = Xs.length, D = new Float64Array(n * n);
    for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) { const d = dist2(Xs[i], Xs[j]); D[i * n + j] = d; D[j * n + i] = d; }
    const tam = new Float64Array(n).fill(1), act = new Uint8Array(n).fill(1), fus = [], cadena = [];
    let quedan = n, pasos = 0;
    while (quedan > 1) {
      if (!cadena.length) { let i = 0; while (!act[i]) i++; cadena.push(i); }
      for (;;) {
        const a = cadena[cadena.length - 1], prev = cadena.length > 1 ? cadena[cadena.length - 2] : -1;
        let best = -1, bd = Infinity;
        if (prev >= 0) { best = prev; bd = D[a * n + prev]; }
        for (let j = 0; j < n; j++) { if (!act[j] || j === a) continue; const d = D[a * n + j]; if (d < bd) { bd = d; best = j; } }
        if (best === prev && prev >= 0) {
          cadena.pop(); cadena.pop();
          const i = prev, j = a, ni = tam[i], nj = tam[j];
          fus.push({ a: i, b: j, h: bd });
          for (let k = 0; k < n; k++) {
            if (!act[k] || k === i || k === j) continue;
            const nk = tam[k];
            const d = ((ni + nk) * D[k * n + i] + (nj + nk) * D[k * n + j] - nk * bd) / (ni + nj + nk);
            D[k * n + i] = d; D[i * n + k] = d;
          }
          tam[i] = ni + nj; act[j] = 0; quedan--;
          break;
        }
        cadena.push(best);
      }
      if (++pasos % 50 === 0) { const w = tk(); if (w) await w; }
    }
    fus.sort(function (x, y) { return x.h - y.h; });
    return fus;
  }
  /** Etiquetas al cortar el dendrograma en k grupos (reproduce las primeras n-k fusiones). */
  function cortarWard(fus, n, k) {
    const padre = new Int32Array(n); for (let i = 0; i < n; i++) padre[i] = i;
    const raiz = function (x) { while (padre[x] !== x) { padre[x] = padre[padre[x]]; x = padre[x]; } return x; };
    for (let m = 0; m < n - k; m++) { const a = raiz(fus[m].a), b = raiz(fus[m].b); if (a !== b) padre[b] = a; }
    const mapa = new Map(), lab = new Int32Array(n);
    for (let i = 0; i < n; i++) { const r = raiz(i); if (!mapa.has(r)) mapa.set(r, mapa.size); lab[i] = mapa.get(r); }
    return lab;
  }

  /** eps automático: «codo» de la curva de distancias al k-ésimo vecino. */
  function epsAuto(Xs, minPts, rng) {
    const n = Xs.length, m = Math.min(n, 800), ids = rng.muestra(n, m), kd = new Float64Array(m);
    const best = new Float64Array(minPts);
    for (let a = 0; a < m; a++) {
      best.fill(Infinity); const q = Xs[ids[a]];
      for (let i = 0; i < n; i++) { if (i === ids[a]) continue; const d = dist2(q, Xs[i]); if (d < best[minPts - 1]) { let s = minPts - 1; while (s > 0 && best[s - 1] > d) { best[s] = best[s - 1]; s--; } best[s] = d; } }
      kd[a] = Math.sqrt(best[minPts - 1]);
    }
    kd.sort();
    let mejor = 0, md = -Infinity; const y0 = kd[0], y1 = kd[m - 1];
    if (y1 - y0 < 1e-12) return Math.max(y1, 1e-6);
    for (let i = 0; i < m; i++) { const d = i / (m - 1) - (kd[i] - y0) / (y1 - y0); if (d > md) { md = d; mejor = i; } }
    return Math.max(kd[mejor], 1e-6);
  }
  async function dbscanAjustar(Xs, eps, minPts) {
    const n = Xs.length, lab = new Int32Array(n).fill(-2), eps2 = eps * eps, enCola = new Uint8Array(n);
    const vecinos = function (i) {
      const r = [], q = Xs[i];
      for (let j = 0; j < n; j++) { let acc = 0; const o = Xs[j]; for (let t = 0; t < q.length; t++) { const d = q[t] - o[t]; acc += d * d; if (acc > eps2) break; } if (acc <= eps2) r.push(j); }
      return r;
    };
    let cl = 0, consultas = 0;
    for (let i = 0; i < n; i++) {
      if (lab[i] !== -2) continue;
      const nb = vecinos(i);
      if (nb.length < minPts) { lab[i] = -1; continue; }
      lab[i] = cl;
      const cola = [];
      nb.forEach(function (j) { if (j !== i && !enCola[j]) { enCola[j] = 1; cola.push(j); } });
      for (let k = 0; k < cola.length; k++) {
        const j = cola[k];
        if (lab[j] === -1) lab[j] = cl;
        if (lab[j] !== -2) continue;
        lab[j] = cl;
        const nb2 = vecinos(j);
        if (nb2.length >= minPts) nb2.forEach(function (u) { if (!enCola[u] && lab[u] < 0) { enCola[u] = 1; cola.push(u); } });
        if (++consultas % 150 === 0) { const w = tk(); if (w) await w; }
      }
      cl++;
    }
    return { labels: lab, k: cl };
  }

  /** Distancia de Mahalanobis robusta (tipo MCD simplificado) con regularización. Xs ya estandarizada. */
  function mahalanobisRobusta(Xs) {
    const n = Xs.length, p = Xs[0].length;
    const med = new Float64Array(p), esc = new Float64Array(p), col = new Float64Array(n);
    for (let j = 0; j < p; j++) {
      for (let i = 0; i < n; i++) col[i] = Xs[i][j];
      const s = ordenado(col); med[j] = cuantilOrd(s, .5);
      const d = new Float64Array(n); for (let i = 0; i < n; i++) d[i] = Math.abs(col[i] - med[j]);
      const mad = 1.4826 * mediana(d); esc[j] = mad > 1e-9 ? mad : (desvio(col) > 1e-9 ? desvio(col) : 1);
    }
    const h = Math.min(n, Math.max(Math.ceil(0.75 * n), p + 2));
    let d2 = new Float64Array(n);
    for (let i = 0; i < n; i++) { let s = 0; for (let j = 0; j < p; j++) { const t = (Xs[i][j] - med[j]) / esc[j]; s += t * t; } d2[i] = s; }
    let mu = med, Sinv = null, S = null;
    const elegir = function () { const o = new Array(n); for (let i = 0; i < n; i++) o[i] = i; o.sort(function (a, b) { return d2[a] - d2[b]; }); return o.slice(0, h); };
    let sub = elegir(), previo = '';
    for (let it = 0; it < 10; it++) {
      const cv = covarianza(Xs, sub, p); mu = cv.mu; S = cv.C;
      let tr = 0; for (let j = 0; j < p; j++) tr += S[j][j];
      for (let j = 0; j < p; j++) S[j][j] += 1e-3 * (tr / p) + 1e-9;
      Sinv = invertir(S, p);
      if (!Sinv) { for (let j = 0; j < p; j++) S[j][j] += 1e-2 * (tr / p + 1); Sinv = invertir(S, p); }
      if (!Sinv) break;
      for (let i = 0; i < n; i++) {
        let s = 0;
        for (let a = 0; a < p; a++) { const da = Xs[i][a] - mu[a]; if (da === 0) continue; let t = 0; for (let b = 0; b < p; b++) t += Sinv[a][b] * (Xs[i][b] - mu[b]); s += da * t; }
        d2[i] = Math.max(0, s);
      }
      sub = elegir();
      const clave = sub.slice().sort(function (a, b) { return a - b; }).join(',');
      if (clave === previo) break; previo = clave;
    }
    // corrección de consistencia: la mediana de d² debe parecerse a la de una chi-cuadrada
    const c = mediana(d2) / chi2Inv(0.5, p);
    if (c > 0 && isFinite(c)) for (let i = 0; i < n; i++) d2[i] /= c;
    return { d2: d2, mu: mu, esc: esc, med: med };
  }

  // ==========================================================================
  // 10. TEXTOS Y HALLAZGOS (español de México, para analistas no programadores)
  // ==========================================================================

  function lista(a) { return a.length <= 1 ? a.join('') : a.slice(0, -1).join(', ') + ' y ' + a[a.length - 1]; }
  function calidadR2(r2) {
    if (r2 === null || r2 === undefined) return 'no se pudo calcular';
    if (r2 >= 0.8) return 'muy bueno'; if (r2 >= 0.6) return 'bueno'; if (r2 >= 0.3) return 'moderado'; if (r2 >= 0.1) return 'débil';
    return 'prácticamente nulo';
  }
  function tonoR2(r2) { return r2 === null || r2 === undefined ? 'neutro' : (r2 >= 0.6 ? 'bueno' : (r2 >= 0.3 ? 'medio' : 'malo')); }
  function tonoPct(x, b, m) { return x === null || x === undefined ? 'neutro' : (x >= b ? 'bueno' : (x >= m ? 'medio' : 'malo')); }

  // ==========================================================================
  // 11. ORQUESTACIÓN
  // ==========================================================================

  function crearEjecutor(onEtapa) {
    const etapas = [];
    const emitir = function (o) { if (typeof onEtapa === 'function') { try { onEtapa(o); } catch (e) { /* la interfaz no debe romper el cálculo */ } } };
    return {
      etapas: etapas, emitir: emitir,
      etapa: async function (id, nombre, fn) {
        const t0 = Date.now();
        emitir({ id: id, nombre: nombre, progreso: 0, detalle: 'Iniciando…' });
        await cederYa();
        const rep = function (p, d) { emitir({ id: id, nombre: nombre, progreso: Math.max(0, Math.min(1, p)), detalle: d || '' }); };
        const det = await fn(rep);
        const detalle = typeof det === 'string' ? det : '';
        emitir({ id: id, nombre: nombre, progreso: 1, detalle: detalle });
        etapas.push({ id: id, nombre: nombre, ms: Date.now() - t0, detalle: detalle });
        await cederYa();
      }
    };
  }

  function configPrep(cfg, perfiles, ctxPrep) {
    const sug = sugerirPrep(perfiles, ctxPrep), g = {}, pc = {};
    Object.keys(sug.global).forEach(function (k) { g[k] = sug.global[k].id; });
    if (cfg.prep && cfg.prep.global) {
      Object.keys(cfg.prep.global).forEach(function (k) { const v = cfg.prep.global[k], id = (v && typeof v === 'object') ? v.id : v; if (idValido(k, id)) g[k] = id; });
      Object.keys(cfg.prep.porColumna || {}).forEach(function (k) { pc[k] = cfg.prep.porColumna[k]; });
    } else if (!cfg.prep) {
      Object.keys(sug.porColumna).forEach(function (k) { pc[k] = sug.porColumna[k]; });
    }
    return { g: g, pc: pc };
  }

  function rmseDe(real, pred) { let s = 0; for (let i = 0; i < real.length; i++) { const e = real[i] - pred[i]; s += e * e; } return Math.sqrt(s / real.length); }
  function logLossDe(real, probs) { let s = 0; for (let i = 0; i < real.length; i++) s -= Math.log(Math.max(probs[i][real[i]], 1e-15)); return s / real.length; }
  function elegirFilas(arr, idx) { const o = new arr.constructor(idx.length); for (let i = 0; i < idx.length; i++) o[i] = arr[idx[i]]; return o; }

  async function evaluarConfig(id, par, folds, ctxBase, esReg, rep) {
    const preds = []; let sc = 0;
    for (let f = 0; f < folds.length; f++) {
      const ctx = Object.assign({}, ctxBase, { n: folds[f].Xtr.length, cv: true });
      const modelo = await ajustarModelo(id, folds[f].Xtr, folds[f].ytr, par, ctx);
      const pr = await predecirAsync(modelo, folds[f].Xva);
      preds.push(pr);
      sc += esReg ? rmseDe(folds[f].yva, pr) : logLossDe(folds[f].yva, pr);
      const w = tk(); if (w) await w;
    }
    return { score: sc / folds.length, preds: preds };
  }

  /** Importancia por permutación (agrupa las columnas 0/1 de una misma variable). */
  async function importanciaPermutacion(modelo, Xte, yte, esReg, grupos, rng, reps, maxFilas, rep) {
    let X = Xte, y = yte;
    if (X.length > maxFilas) { const ids = rng.muestra(X.length, maxFilas); X = ids.map(function (i) { return Xte[i]; }); y = elegirFilas(yte, ids); }
    const perdida = function (pred) { return esReg ? rmseDe(y, pred) : logLossDe(y, pred); };
    const base = perdida(await predecirAsync(modelo, X));
    const Xc = X.map(function (r) { return Float64Array.from(r); }), res = [];
    for (let g = 0; g < grupos.length; g++) {
      const vals = [];
      for (let r = 0; r < reps; r++) {
        const perm = new Array(X.length); for (let i = 0; i < perm.length; i++) perm[i] = i;
        rng.shuffle(perm);
        for (let i = 0; i < Xc.length; i++) for (let c = 0; c < grupos[g].cols.length; c++) Xc[i][grupos[g].cols[c]] = X[perm[i]][grupos[g].cols[c]];
        vals.push(perdida(await predecirAsync(modelo, Xc)) - base);
        for (let i = 0; i < Xc.length; i++) for (let c = 0; c < grupos[g].cols.length; c++) Xc[i][grupos[g].cols[c]] = X[i][grupos[g].cols[c]];
      }
      res.push({ var: grupos[g].nombre, valor: media(vals), sd: vals.length > 1 ? desvio(vals) : 0 });
      rep((g + 1) / grupos.length, 'Importancia de ' + (g + 1) + ' de ' + grupos.length + ' variables');
      const w = tk(); if (w) await w;
    }
    let mx = 0; res.forEach(function (r) { if (r.valor > mx) mx = r.valor; });
    res.forEach(function (r) { r.valorBruto = r.valor; r.valor = mx > 0 ? Math.max(0, r.valor) / mx * 100 : 0; r.sd = mx > 0 ? r.sd / mx * 100 : 0; });
    res.sort(function (a, b) { return b.valor - a.valor; });
    return res;
  }

  function construirCoeficientes(id, modelo, pm, K, clases, base) {
    const cols = pm.outCols, P = cols.length, esc = pm.esc, out = [];
    if (id === 'ols') {
      const d = desescalarCoef(modelo.b0, modelo.b, modelo.cov, esc.c, esc.s);
      const tc = tInv(0.975, modelo.df);
      for (let k = 0; k <= P; k++) {
        const coef = k === 0 ? d.b0 : d.b[k - 1], se = d.se[k], t = se > 0 ? coef / se : NaN, p = se > 0 ? tPvalor(t, modelo.df) : NaN;
        out.push({ var: k === 0 ? '(Intercepto)' : cols[k - 1].label, nombre: k === 0 ? '(Intercepto)' : cols[k - 1].nombre, coef: coef, error: se, t: t, p: p, ic95: [coef - tc * se, coef + tc * se] });
      }
    } else if (id === 'ridge' || id === 'lasso') {
      const d = desescalarCoef(modelo.b0, modelo.b, null, esc.c, esc.s);
      for (let k = 0; k <= P; k++) out.push({ var: k === 0 ? '(Intercepto)' : cols[k - 1].label, nombre: k === 0 ? '(Intercepto)' : cols[k - 1].nombre, coef: k === 0 ? d.b0 : d.b[k - 1], error: null, t: null, p: null, ic95: null });
    } else if (id === 'logistica' && K === 2) {
      const d = desescalarCoef(modelo.b0, modelo.b, modelo.cov, esc.c, esc.s);
      for (let k = 0; k <= P; k++) {
        const coef = k === 0 ? d.b0 : d.b[k - 1], se = d.se ? d.se[k] : null;
        const z = se > 0 ? coef / se : null, p = z !== null ? 2 * (1 - normCDF(Math.abs(z))) : null;
        const ic = se > 0 ? [coef - 1.959964 * se, coef + 1.959964 * se] : null;
        out.push({ var: k === 0 ? '(Intercepto)' : cols[k - 1].label, nombre: k === 0 ? '(Intercepto)' : cols[k - 1].nombre, coef: coef, error: se, t: z, p: p, ic95: ic, oddsRatio: Math.exp(coef), oddsIC95: ic ? [Math.exp(ic[0]), Math.exp(ic[1])] : null });
      }
    } else if (id === 'logistica') {
      for (let c = 1; c < K; c++) {
        const b = new Float64Array(P); for (let j = 0; j < P; j++) b[j] = modelo.BB[c][j] - modelo.BB[0][j];
        const d = desescalarCoef(modelo.B0[c] - modelo.B0[0], b, null, esc.c, esc.s);
        for (let k = 0; k <= P; k++) { const coef = k === 0 ? d.b0 : d.b[k - 1]; out.push({ var: (k === 0 ? '(Intercepto)' : cols[k - 1].label) + ' [' + clases[c] + ' frente a ' + clases[0] + ']', nombre: k === 0 ? '(Intercepto)' : cols[k - 1].nombre, clase: clases[c], coef: coef, error: null, t: null, p: null, ic95: null, oddsRatio: Math.exp(coef) }); }
      }
    }
    return out;
  }

  const COSTOSOS = ['bosque_reg', 'bosque_clf', 'svm_clf', 'svm_reg', 'mlp_clf', 'mlp_reg', 'gbm_clf', 'gbm_reg'];

  async function correrSup(cfg, ds, ej) {
    const t00 = Date.now();
    const lectura = leerDataset(ds), perfiles = lectura.map(function (c) { return c.perfil; }), porNombre = {};
    lectura.forEach(function (c) { porNombre[c.name] = c; });
    const id = cfg.metodo, M = META[id];
    if (!M || M.tipo === 'nosup') throw new Error('El método «' + id + '» no existe o no es de aprendizaje supervisado.');
    const colY = cfg.y ? porNombre[cfg.y] : null;
    if (!colY) throw new Error('Elige la variable a predecir: no se encontró la columna «' + cfg.y + '».');
    const nombresX = (cfg.x && cfg.x.length) ? cfg.x.filter(function (n) { return n !== cfg.y; }) : lectura.filter(function (c) { return c.name !== cfg.y; }).map(function (c) { return c.name; });
    nombresX.forEach(function (nm) { if (!porNombre[nm]) throw new Error('La variable «' + nm + '» no existe en los datos.'); });
    if (!nombresX.length) throw new Error('Elige al menos una variable explicativa.');
    const colsX = nombresX.map(function (nm) { return porNombre[nm]; });
    const info = tipoRespuestaDe(colY.perfil);
    if (!info.tipo) throw new Error(info.razon);
    const esReg = M.tipo === 'reg';
    if (esReg && info.tipo !== 'regresion' && !info.tambienRegresion) throw new Error('«' + colY.label + '» son categorías y el método «' + M.nombre + '» predice números. Elige un método de clasificación.');
    if (!esReg && info.tipo === 'regresion') throw new Error('«' + colY.label + '» es un número continuo y el método «' + M.nombre + '» predice categorías. Elige un método de regresión.');
    const nTot = ds.rows.length;
    const semilla = cfg.semilla == null ? 42 : cfg.semilla, rng = crearRng(semilla), rngM = crearRng(semilla + 1);
    const propPrueba = Math.min(0.5, Math.max(0.05, cfg.prop_prueba == null ? 0.2 : cfg.prop_prueba));
    const nFolds = Math.min(10, Math.max(2, Math.round(cfg.folds == null ? 5 : cfg.folds)));
    const pasosPre = [];

    // --- Variable a predecir
    const validas = []; let yFull, clases = null, K = 0;
    if (esReg) {
      yFull = new Float64Array(nTot).fill(NaN);
      for (let i = 0; i < nTot; i++) { const v = colY.esNum ? colY.num[i] : NaN; if (!isNaN(v)) { yFull[i] = v; validas.push(i); } }
      if (validas.length < nTot) pasosPre.push('Se quitaron ' + (nTot - validas.length) + ' filas que no tenían valor en «' + colY.label + '».');
    } else {
      const etiqueta = function (i) { if (colY.esNum) return isNaN(colY.num[i]) ? null : String(colY.num[i]); return colY.cat[i]; };
      const cnt = new Map();
      for (let i = 0; i < nTot; i++) { const e = etiqueta(i); if (e !== null) cnt.set(e, (cnt.get(e) || 0) + 1); }
      const keep = Array.from(cnt.keys()).filter(function (k) { return cnt.get(k) >= 3; });
      if (keep.length < 2) throw new Error('«' + colY.label + '» necesita al menos 2 categorías con 3 casos o más cada una para poder clasificar.');
      const todasNum = keep.every(function (k) { return !isNaN(Number(k)); });
      keep.sort(function (a, b) { return todasNum ? Number(a) - Number(b) : (a < b ? -1 : (a > b ? 1 : 0)); });
      if (keep.length === 2 && /^(1|si|sí|true|yes|verdadero|s)$/i.test(keep[0])) keep.reverse();
      clases = keep; K = clases.length;
      const mapa = new Map(); clases.forEach(function (c, i) { mapa.set(c, i); });
      yFull = new Int32Array(nTot).fill(-1);
      let sinY = 0, raras = 0;
      for (let i = 0; i < nTot; i++) { const e = etiqueta(i); if (e === null) { sinY++; continue; } if (mapa.has(e)) { yFull[i] = mapa.get(e); validas.push(i); } else raras++; }
      if (sinY) pasosPre.push('Se quitaron ' + sinY + ' filas que no tenían valor en «' + colY.label + '».');
      if (raras) pasosPre.push('Se quitaron ' + raras + ' filas de categorías con menos de 3 casos (no alcanzan para aprender ni evaluar).');
    }
    if (validas.length < 20) throw new Error('Se necesitan al menos 20 filas con valor en «' + colY.label + '» y quedaron ' + validas.length + '.');
    if (validas.length < M.min) throw new Error('El método «' + M.nombre + '» necesita al menos ' + M.min + ' filas y hay ' + validas.length + '.');

    // --- Partición ANTES de ajustar cualquier preprocesamiento
    const sp = dividir(validas.length, propPrueba, rng, esReg ? null : validas.map(function (i) { return yFull[i]; }));
    const trRows = sp.train.map(function (i) { return validas[i]; }), teRows = sp.test.map(function (i) { return validas[i]; });
    if (trRows.length < 10) throw new Error('Quedan muy pocas filas para entrenar (' + trRows.length + ').');
    pasosPre.push('Partición: ' + trRows.length + ' filas para aprender y ' + teRows.length + ' para evaluar (' + (esReg ? 'al azar' : 'estratificada por categoría') + ', semilla ' + semilla + '). El preprocesamiento se calculó solo con las filas de aprendizaje.');

    const cp = configPrep(cfg, perfiles, { enfoque: 'sup', y: cfg.y, x: nombresX, n: validas.length, metodo: id });
    let pa, Atr, Ate, ytr, yte, P, pm;
    await ej.etapa('prep', 'Preparación de los datos', async function (rep) {
      pa = ajustarPrep(colsX, trRows, { g: cp.g, pc: cp.pc, yVals: yFull });
      pm = pa.modelo;
      rep(0.5, 'Aplicando a entrenamiento y prueba');
      Atr = aplicarPrep(pm, pa.rows, true);
      Ate = aplicarPrep(pm, teRows, false);
      if (Ate.X.length < 3) throw new Error('Casi no quedaron filas de prueba después de quitar las que tienen datos vacíos; elige otra forma de rellenar los faltantes.');
      ytr = elegirFilas(yFull, Atr.filas); yte = elegirFilas(yFull, Ate.filas);
      P = pm.outCols.length;
      if (!esReg) {
        const pres = new Set(Array.prototype.slice.call(ytr));
        if (pres.size < 2) throw new Error('Después de la limpieza el entrenamiento quedó con una sola categoría de «' + colY.label + '».');
      }
      if (Ate.X.length < teRows.length) pa.pasos.push('Prueba: se quitaron ' + (teRows.length - Ate.X.length) + ' filas con datos vacíos (no se pueden predecir sin rellenar).');
      return 'Quedaron ' + P + ' variables finales; ' + Atr.X.length + ' filas de entrenamiento y ' + Ate.X.length + ' de prueba.';
    });

    // --- Entrenamiento inicial
    const ctxM = { K: K, rng: rngM, p: P, n: Atr.X.length };
    const parDef = MODELOS[id].defecto(ctxM);
    let modelo, parFinal = parDef;
    await ej.etapa('entrenamiento', 'Entrenamiento del modelo', async function (rep) {
      ctxM.rep = rep;
      try { modelo = await ajustarModelo(id, Atr.X, ytr, parDef, ctxM); } finally { ctxM.rep = null; }
      return 'Se ajustó «' + M.nombre + '» con ' + Atr.X.length + ' filas y ' + P + ' variables' + (Object.keys(parDef).length ? ' usando valores iniciales de hiperparámetros' : '') + (modelo.nota ? ' (' + modelo.nota.replace(/\.$/, '') + ')' : '') + '.';
    });

    // --- Calibración (búsqueda de hiperparámetros por validación cruzada)
    let rejillaRes = [], cvRes = null, mejorIdx = 0, foldsDatos = [];
    await ej.etapa('calibracion', 'Calibración y validación cruzada', async function (rep) {
      const nFoldsUso = COSTOSOS.indexOf(id) >= 0 ? Math.min(nFolds, 3) : nFolds;
      const posFolds = crearFolds(Atr.X.length, nFoldsUso, rng, esReg ? null : ytr);
      for (let f = 0; f < posFolds.length; f++) {
        try {
          const valSet = new Set(posFolds[f]);
          const valRows = posFolds[f].map(function (i) { return Atr.filas[i]; });
          const trnRows = Atr.filas.filter(function (r, i) { return !valSet.has(i); });
          const pf = ajustarPrep(colsX, trnRows, { g: cp.g, pc: cp.pc, yVals: yFull });
          const a = aplicarPrep(pf.modelo, pf.rows, true), b = aplicarPrep(pf.modelo, valRows, false);
          if (!a.X.length || !b.X.length) continue;
          if (!esReg && new Set(Array.prototype.slice.call(elegirFilas(yFull, a.filas))).size < 2) continue;
          foldsDatos.push({ Xtr: a.X, ytr: elegirFilas(yFull, a.filas), Xva: b.X, yva: elegirFilas(yFull, b.filas) });
        } catch (e) { /* un pliegue que falla se omite; se avisa abajo */ }
        rep(0.1 * (f + 1) / posFolds.length, 'Preparando pliegue ' + (f + 1) + ' de ' + posFolds.length);
        const w = tk(); if (w) await w;
      }
      if (foldsDatos.length < 2) return 'No se pudo hacer validación cruzada (muy pocos datos por pliegue); se usan solo los resultados de la prueba.';
      const nTrnF = foldsDatos[0].Xtr.length;
      const ctxG = { K: K, rng: rngM, p: P, n: nTrnF };
      const rej = MODELOS[id].rejilla(ctxG);
      const configs = rej && rej.length ? rej : [parDef];
      let resultados = [];
      if (MODELOS[id].evaluarRejilla && configs.length > 1) { rep(0.3, 'Probando ' + configs.length + ' valores de k'); resultados = await MODELOS[id].evaluarRejilla(configs, foldsDatos, ctxG, esReg); }
      else for (let c = 0; c < configs.length; c++) {
        const r = await evaluarConfig(id, configs[c], foldsDatos, ctxG, esReg, rep);
        resultados.push(r);
        rep(0.1 + 0.9 * (c + 1) / configs.length, 'Probando combinación ' + (c + 1) + ' de ' + configs.length);
      }
      let mj = 0; resultados.forEach(function (r, i) { if (r.score < resultados[mj].score) mj = i; });
      mejorIdx = mj;
      rejillaRes = configs.map(function (p, i) { return { params: p, score: resultados[i].score }; });
      const fm = foldsDatos.map(function (f, i) { return esReg ? metricasReg(f.yva, resultados[mj].preds[i], P) : metricasClf(f.yva, resultados[mj].preds[i], K, clases).metricas; });
      cvRes = promediarMetricas(fm);
      if (rej && rej.length) {
        parFinal = configs[mj];
        if (JSON.stringify(parFinal) !== JSON.stringify(parDef)) { rep(1, 'Reajustando con la mejor combinación'); ctxM.rep = function (p, d) { rep(1, 'Reajustando · ' + d); }; try { modelo = await ajustarModelo(id, Atr.X, ytr, parFinal, ctxM); } finally { ctxM.rep = null; } }
        return 'Se probaron ' + configs.length + ' combinaciones con ' + foldsDatos.length + ' pliegues; mejor: ' + JSON.stringify(parFinal).replace(/[{}"]/g, '').replace(/,/g, ', ') + '.';
      }
      return 'Este método no tiene hiperparámetros que ajustar: se omite la búsqueda y se evalúa con ' + foldsDatos.length + ' pliegues de validación cruzada.';
    });

    // --- Evaluación en el conjunto de prueba
    let mTr, mTe, preds = {}, cm = null, roc = null, calib = null;
    await ej.etapa('evaluacion', 'Evaluación con datos de prueba', async function (rep) {
      const pTr = await predecirAsync(modelo, Atr.X), pTe = await predecirAsync(modelo, Ate.X);
      if (esReg) {
        mTr = metricasReg(ytr, pTr, P); mTe = metricasReg(yte, pTe, P);
        preds = { idx: Ate.filas.slice(), real: Array.from(yte), pred: Array.from(pTe) };
      } else {
        const rTr = metricasClf(ytr, pTr, K, clases), rTe = metricasClf(yte, pTe, K, clases);
        mTr = rTr.metricas; mTe = rTe.metricas;
        cm = { clases: clases.slice(), m: rTe.matriz };
        roc = { fpr: rTe.roc.fpr, tpr: rTe.roc.tpr, auc: mTe.auc, porClase: rTe.roc.porClase, tipo: K === 2 ? 'binaria' : 'micro (todas las categorías juntas)' };
        calib = curvaCalibracion(yte, pTe, K);
        preds = { idx: Ate.filas.slice(), real: Array.from(yte).map(function (c) { return clases[c]; }), pred: Array.from(rTe.pred).map(function (c) { return clases[c]; }), prob: pTe.map(function (p) { return Array.from(p); }), clases: clases.slice() };
      }
      return esReg ? 'R² en prueba: ' + fmt(mTe.r2, 3) + '; error típico (RMSE): ' + fmt(mTe.rmse, 3) + '.' : 'Exactitud en prueba: ' + fpct(mTe.exactitud) + '.';
    });

    // --- Diagnóstico
    let importancias = [], residuos = null, curva = { n: [], entrenamiento: [], validacion: [] }, coeficientes = [];
    await ej.etapa('diagnostico', 'Diagnóstico del modelo', async function (rep) {
      const grupos = [], mapaG = new Map();
      pm.outCols.forEach(function (o, j) {
        if (!mapaG.has(o.origen)) { const g = { nombre: porNombre[o.origen].label, origen: o.origen, cols: [] }; mapaG.set(o.origen, g); grupos.push(g); }
        mapaG.get(o.origen).cols.push(j);
      });
      const costoKnn = id === 'knn_reg' || id === 'knn_clf', costoSvm = id === 'svm_reg' || id === 'svm_clf';
      importancias = await importanciaPermutacion(modelo, Ate.X, yte, esReg, grupos, rngM, (costoKnn || costoSvm) ? 2 : 3, costoKnn ? 100 : (costoSvm ? 200 : 800), function (p, d) { rep(0.6 * p, d); });
      if (esReg) residuos = diagResiduos(yte, await predecirAsync(modelo, Ate.X), Ate.filas);
      coeficientes = construirCoeficientes(id, modelo, pm, K, clases);
      // Curva de aprendizaje con el primer pliegue de validación cruzada
      if (foldsDatos.length) {
        const f0 = foldsDatos[0], nF = f0.Xtr.length;
        for (const fr of [0.2, 0.4, 0.6, 0.8, 1]) {
          const m = Math.max(8, Math.floor(fr * nF));
          if (m > nF) continue;
          try {
            const Xs = f0.Xtr.slice(0, m), ys = f0.ytr.slice(0, m);
            const par = (parFinal.arboles ? Object.assign({}, parFinal, { arboles: Math.min(parFinal.arboles, 30) }) : (parFinal.epocas !== undefined || parFinal.h ? Object.assign({}, parFinal, { epocas: 60 }) : (parFinal.rondas ? Object.assign({}, parFinal, { rondas: Math.min(parFinal.rondas, 150) }) : parFinal)));
            const ctx = { K: K, rng: crearRng(semilla + 7), p: P, n: m };
            const mod = await ajustarModelo(id, Xs, ys, par, ctx);
            const ptr = await predecirAsync(mod, Xs), pva = await predecirAsync(mod, f0.Xva);
            curva.n.push(m);
            curva.entrenamiento.push(esReg ? metricasReg(ys, ptr, P).r2 : metricasClf(ys, ptr, K, clases).metricas.exactitud);
            curva.validacion.push(esReg ? metricasReg(f0.yva, pva, P).r2 : metricasClf(f0.yva, pva, K, clases).metricas.exactitud);
          } catch (e) { /* un punto de la curva que falla se omite */ }
          rep(0.6 + 0.4 * fr, 'Curva de aprendizaje');
          const w = tk(); if (w) await w;
        }
      }
      return 'Importancias por permutación, ' + (esReg ? 'residuos, ' : '') + 'curva de aprendizaje' + (coeficientes.length ? ' y coeficientes.' : '.');
    });

    // --- Textos: hallazgos, KPIs e interpretaciones
    const tipoY = esReg ? 'regresion' : (K === 2 ? 'binaria' : 'multiclase');
    const yL = colY.label, unidad = colY.unit ? ' ' + colY.unit : '';
    const hallazgos = [], advertencias = [];
    const signoDe = function (origen) {
      const c = porNombre[origen]; if (!c || !c.esNum) return 0;
      const a = [], b = [];
      Atr.filas.forEach(function (r, i) { const x = c.num[r]; if (!isNaN(x)) { a.push(x); b.push(esReg ? ytr[i] : (ytr[i] === 1 ? 1 : 0)); } });
      const r = pearson(a, b); return isNaN(r) || Math.abs(r) < 0.1 ? 0 : (r > 0 ? 1 : -1);
    };
    const top = importancias.filter(function (i) { return i.valor > 0; }).slice(0, 3);
    if (esReg) {
      hallazgos.push('El modelo explica cerca del ' + fmt(100 * Math.max(0, mTe.r2 === null ? 0 : mTe.r2), 0) + ' % de la variación de «' + yL + '» en datos que no vio (R² de prueba = ' + fmt(mTe.r2, 2) + '): ajuste ' + calidadR2(mTe.r2) + '. En promedio se equivoca por unos ' + fmt(mTe.mae, 2) + unidad + ' (error típico RMSE ' + fmt(mTe.rmse, 2) + unidad + ').');
      if (mTe.r2 !== null && mTe.r2 < 0) hallazgos.push('Atención: el modelo predice peor que simplemente usar el promedio de «' + yL + '». Con estas variables no hay una relación que aprovechar.');
      if (mTr.r2 !== null && mTe.r2 !== null && mTr.r2 - mTe.r2 > 0.15) hallazgos.push('Posible sobreajuste: acierta mucho más con los datos con los que aprendió (R² ' + fmt(mTr.r2, 2) + ') que con los nuevos (' + fmt(mTe.r2, 2) + '). Prueba un modelo más sencillo o consigue más filas.');
    } else {
      const baseMay = (function () { const c = new Float64Array(K); for (let i = 0; i < yte.length; i++) c[yte[i]]++; return Math.max.apply(null, Array.prototype.slice.call(c)) / yte.length; })();
      hallazgos.push('El modelo clasifica correctamente el ' + fpct(mTe.exactitud, 1) + ' de los casos de prueba' + '; sin modelo, adivinar siempre la categoría más común acertaría ' + fpct(baseMay, 1) + '.');
      if (mTe.exactitud <= baseMay + 0.02) hallazgos.push('Atención: el modelo apenas mejora a «adivinar siempre la categoría más común»; las variables elegidas casi no ayudan a distinguir.');
      if (mTe.auc !== null) hallazgos.push('Capacidad de separar categorías (AUC' + (K > 2 ? ' promedio' : '') + '): ' + fmt(mTe.auc, 2) + ' (' + (mTe.auc >= 0.9 ? 'excelente' : (mTe.auc >= 0.8 ? 'buena' : (mTe.auc >= 0.7 ? 'aceptable' : 'débil'))) + ').');
      const pcs = Object.keys(mTe.porClase).filter(function (k) { return mTe.porClase[k].n >= 3; }).sort(function (a, b) { return mTe.porClase[a].recall - mTe.porClase[b].recall; });
      if (pcs.length > 1 && mTe.porClase[pcs[0]].recall < 0.6) hallazgos.push('La categoría más difícil es «' + pcs[0] + '»: solo se detecta el ' + fpct(mTe.porClase[pcs[0]].recall, 0) + ' de sus casos.');
      const minFrac = Math.min.apply(null, clases.map(function (c) { return mTe.porClase[c].n; })) / yte.length;
      if (minFrac < 0.1) hallazgos.push('Las categorías están desbalanceadas (la menor tiene ' + fpct(minFrac, 0) + ' de los casos); la exactitud puede verse mejor de lo que es, revisa la exactitud balanceada (' + fpct(mTe.exactitud_balanceada, 1) + ').');
      if (mTr.exactitud - mTe.exactitud > 0.1) hallazgos.push('Posible sobreajuste: acierta ' + fpct(mTr.exactitud, 0) + ' en los datos con los que aprendió pero ' + fpct(mTe.exactitud, 0) + ' en los nuevos.');
    }
    if (top.length) {
      const partes = top.map(function (t, i) {
        const o = pm.outCols.filter(function (c) { return porNombre[c.origen] && porNombre[c.origen].label === t.var; })[0];
        const s = o ? signoDe(o.origen) : 0;
        let txt = '«' + t.var + '»';
        if (s !== 0 && (esReg || K === 2)) txt += s > 0 ? (esReg ? ' (a más, tiende a haber más «' + yL + '»)' : ' (a más, mayor probabilidad de «' + clases[1] + '»)') : (esReg ? ' (a más, tiende a haber menos «' + yL + '»)' : ' (a más, menor probabilidad de «' + clases[1] + '»)');
        return txt;
      });
      hallazgos.push('Las variables que más influyen son ' + lista(partes) + '.');
    }
    if (id === 'ols') {
      const sig = coeficientes.filter(function (c) { return c.nombre !== '(Intercepto)' && c.p !== null && c.p < 0.05; });
      hallazgos.push(sig.length ? 'Con 95 % de confianza, ' + sig.length + ' de ' + (coeficientes.length - 1) + ' variables tienen un efecto claro: ' + lista(sig.slice(0, 5).map(function (c) { return c.var + (c.coef > 0 ? ' (+)' : ' (−)'); })) + '.' : 'Ninguna variable tiene un efecto estadísticamente claro (p < 0.05); puede que no haya relación lineal.');
    }
    if (modelo.hallazgos) modelo.hallazgos.forEach(function (h) { hallazgos.push(h); });
    if (id === 'lasso') { const ceros = coeficientes.filter(function (c) { return c.nombre !== '(Intercepto)' && Math.abs(c.coef) < 1e-12; }).length; hallazgos.push('Lasso dejó en cero ' + ceros + ' de ' + P + ' variables por no aportar.'); }
    if (id === 'logistica' && K === 2) {
      const sig = coeficientes.filter(function (c) { return c.nombre !== '(Intercepto)' && c.p !== null && c.p < 0.05; }).slice(0, 3);
      if (sig.length) hallazgos.push('Razones de momios: ' + sig.map(function (c) { return c.var + ' multiplica por ' + fmt(c.oddsRatio, 2) + ' los momios de «' + clases[1] + '» por cada unidad más'; }).join('; ') + '.');
    }
    if (Atr.X.length < 10 * P) { advertencias.push('Hay pocas filas (' + Atr.X.length + ') para ' + P + ' variables; los resultados pueden ser inestables.'); }
    const dropCol = pa.descartadas.filter(function (d) { return d.razon.indexOf('Casi idéntica') === 0; });
    if (dropCol.length) advertencias.push('Se quitaron ' + dropCol.length + ' variable(s) casi idénticas a otras (colinealidad): ' + dropCol.slice(0, 4).map(function (d) { return d.name; }).join(', ') + '.');
    if (esReg && residuos) {
      if (residuos.normalidad.p < 0.01 && Ate.X.length >= 30) advertencias.push('Los errores del modelo no siguen una distribución normal (Jarque-Bera p = ' + fmt(residuos.normalidad.p, 3) + '): los intervalos y p-valores deben tomarse con cautela.');
      if (residuos.heterocedasticidad.breuschPaganLike.p < 0.05 && Ate.X.length >= 30) advertencias.push('El error cambia según el valor predicho (varianza no constante); las predicciones altas o bajas pueden ser menos confiables.');
      if (residuos.durbinWatson !== null && (residuos.durbinWatson < 1.5 || residuos.durbinWatson > 2.5)) advertencias.push('Los errores de filas contiguas se parecen (Durbin-Watson = ' + fmt(residuos.durbinWatson, 2) + '); si tus filas tienen un orden (por ejemplo, fechas), hay estructura que el modelo no capta.');
    }
    if (Ate.X.length < 30) advertencias.push('La prueba tiene solo ' + Ate.X.length + ' filas; las métricas son muy variables.');
    if (cvRes && cvRes.media) {
      const k = esReg ? 'r2' : 'exactitud';
      if (cvRes.media[k] !== null && cvRes.media[k] !== undefined) hallazgos.push('Con validación cruzada (' + foldsDatos.length + ' pliegues) el ' + (esReg ? 'R²' : 'acierto') + ' promedio es ' + (esReg ? fmt(cvRes.media[k], 2) : fpct(cvRes.media[k], 1)) + ' ± ' + (esReg ? fmt(cvRes.sd[k], 2) : fpct(cvRes.sd[k], 1)) + ', ' + ((cvRes.sd[k] || 0) > 0.1 ? 'bastante variable según los datos usados.' : 'consistente entre pliegues.'));
    }
    advertencias.forEach(function (a) { hallazgos.push('Advertencia: ' + a); });

    const kpis = [];
    const mejorVar = importancias.length ? importancias[0].var : null;
    if (esReg) {
      kpis.push({ id: 'r2', etiqueta: 'R² de prueba', valor: mTe.r2, formato: 'dec2', nota: 'Proporción de la variación que explica el modelo (1 = perfecto)', tono: tonoR2(mTe.r2) });
      kpis.push({ id: 'rmse', etiqueta: 'Error típico (RMSE)', valor: mTe.rmse, formato: 'num', nota: 'En unidades de «' + yL + '»' + unidad, tono: mTe.ratio_rmse_sd === null ? 'neutro' : (mTe.ratio_rmse_sd < 0.6 ? 'bueno' : (mTe.ratio_rmse_sd < 0.9 ? 'medio' : 'malo')) });
      kpis.push({ id: 'mae', etiqueta: 'Error promedio (MAE)', valor: mTe.mae, formato: 'num', nota: 'Cuánto se equivoca en promedio', tono: 'neutro' });
      kpis.push({ id: 'ratio', etiqueta: 'Error frente a la variación natural', valor: mTe.ratio_rmse_sd, formato: 'pct', nota: 'RMSE / desviación de «' + yL + '»; menos de 50 % es bueno', tono: mTe.ratio_rmse_sd === null ? 'neutro' : (mTe.ratio_rmse_sd < 0.5 ? 'bueno' : (mTe.ratio_rmse_sd < 0.9 ? 'medio' : 'malo')) });
    } else {
      kpis.push({ id: 'exactitud', etiqueta: '% clasificado correcto', valor: mTe.exactitud, formato: 'pct', nota: 'En datos de prueba', tono: tonoPct(mTe.exactitud, 0.85, 0.7) });
      kpis.push({ id: 'f1', etiqueta: 'F1 (promedio de categorías)', valor: mTe.f1_macro, formato: 'dec2', nota: 'Equilibrio entre acertar y no dejar casos sin detectar', tono: tonoPct(mTe.f1_macro, 0.8, 0.6) });
      kpis.push({ id: 'auc', etiqueta: 'AUC', valor: mTe.auc, formato: 'dec2', nota: '0.5 = azar, 1 = separación perfecta', tono: tonoPct(mTe.auc, 0.85, 0.7) });
      kpis.push({ id: 'kappa', etiqueta: 'Acuerdo más allá del azar (kappa)', valor: mTe.kappa, formato: 'dec2', nota: '0 = azar, 1 = perfecto', tono: tonoPct(mTe.kappa, 0.6, 0.3) });
    }
    if (mejorVar) kpis.push({ id: 'var_top', etiqueta: 'Variable más influyente', valor: mejorVar, formato: 'texto', nota: 'Según importancia por permutación', tono: 'neutro' });
    if (cvRes && cvRes.media && cvRes.media[esReg ? 'r2' : 'exactitud'] !== null && kpis.length < 6) kpis.push({ id: 'cv', etiqueta: esReg ? 'R² en validación cruzada' : 'Exactitud en validación cruzada', valor: cvRes.media[esReg ? 'r2' : 'exactitud'], formato: esReg ? 'dec2' : 'pct', nota: 'Promedio de ' + foldsDatos.length + ' pliegues', tono: 'neutro' });

    const im = { metricas: [], graficas: [] };
    const gap = curva.validacion.length ? curva.entrenamiento[curva.entrenamiento.length - 1] - curva.validacion[curva.validacion.length - 1] : null;
    if (esReg) {
      im.metricas.push({ id: 'r2', texto: 'R² de prueba = ' + fmt(mTe.r2, 2) + ': el modelo explica cerca del ' + fmt(100 * Math.max(0, mTe.r2 || 0), 0) + ' % de la variación de «' + yL + '» en datos nuevos. 1 sería perfecto y 0 equivale a predecir siempre el promedio; negativo es peor que eso.' });
      im.metricas.push({ id: 'r2_ajustado', texto: 'R² ajustado = ' + fmt(mTe.r2_ajustado, 2) + ': como el R² pero descuenta las variables usadas, para que agregar variables inútiles no lo infle.' });
      im.metricas.push({ id: 'rmse', texto: 'RMSE = ' + fmt(mTe.rmse, 2) + unidad + ': es el error típico de una predicción; castiga más los errores grandes. La variación natural de «' + yL + '» (desviación) es ' + fmt(mTe.rmse / (mTe.ratio_rmse_sd || NaN), 2) + '; el modelo debe quedar claramente por debajo.' });
      im.metricas.push({ id: 'mae', texto: 'MAE = ' + fmt(mTe.mae, 2) + unidad + ': en promedio, cada predicción se desvía esa cantidad del valor real.' });
      im.metricas.push({ id: 'mape', texto: mTe.mape === null ? 'El error porcentual no se calcula porque «' + yL + '» tiene valores de cero o negativos.' : 'Error porcentual medio = ' + fmt(mTe.mape, 1) + ' %: en promedio las predicciones se alejan ese porcentaje del valor real.' });
      im.metricas.push({ id: 'sesgo', texto: 'Sesgo medio = ' + fmt(mTe.sesgo, 2) + unidad + ': ' + (Math.abs(mTe.sesgo) < 0.05 * (mTe.rmse || 1) ? 'el modelo no se inclina ni a subestimar ni a sobrestimar.' : (mTe.sesgo > 0 ? 'el modelo tiende a predecir de más.' : 'el modelo tiende a predecir de menos.')) });
      im.metricas.push({ id: 'ratio_rmse_sd', texto: 'Error / variación natural = ' + fpct(mTe.ratio_rmse_sd, 0) + ': ' + (mTe.ratio_rmse_sd !== null && mTe.ratio_rmse_sd < 0.5 ? 'el modelo reduce a menos de la mitad la incertidumbre inicial.' : 'el modelo reduce poco la incertidumbre frente a no usar variables.') });
      im.graficas.push({ id: 'pred_vs_real', texto: 'Cada punto es un caso de prueba: horizontal el valor real, vertical lo que predijo el modelo. Si acertara siempre, todos caerían sobre la diagonal. Hoy el R² es ' + fmt(mTe.r2, 2) + ': ' + (mTe.r2 >= 0.6 ? 'los puntos deben verse bastante pegados a la diagonal.' : 'espera una nube dispersa alrededor de la diagonal.') });
      im.graficas.push({ id: 'residuos', texto: 'Los residuos son los errores (real menos predicho). Deben verse como una nube sin forma alrededor de cero; una curva o un embudo indican que el modelo deja algo sin captar. Varianza constante (Breusch-Pagan) p = ' + fmt(residuos.heterocedasticidad.breuschPaganLike.p, 3) + (residuos.heterocedasticidad.breuschPaganLike.p < 0.05 ? ': el error crece o decrece según el valor predicho.' : ': sin señales de problema.') });
      im.graficas.push({ id: 'normalidad', texto: 'Jarque-Bera p = ' + fmt(residuos.normalidad.p, 3) + ': ' + (residuos.normalidad.p < 0.05 ? 'los errores no son perfectamente normales (hay sesgo o colas largas); afecta sobre todo a p-valores e intervalos de la regresión lineal.' : 'los errores se ven razonablemente normales.') + ' Durbin-Watson = ' + fmt(residuos.durbinWatson, 2) + ' (cerca de 2 = errores sin relación entre filas contiguas).' });
    } else {
      im.metricas.push({ id: 'exactitud', texto: 'Exactitud = ' + fpct(mTe.exactitud, 1) + ': de cada 100 casos de prueba, el modelo acierta la categoría en ' + fmt(100 * mTe.exactitud, 0) + '. Sola puede engañar si una categoría domina.' });
      im.metricas.push({ id: 'exactitud_balanceada', texto: 'Exactitud balanceada = ' + fpct(mTe.exactitud_balanceada, 1) + ': promedio del acierto en cada categoría, para que las categorías pequeñas pesen igual que las grandes.' });
      im.metricas.push({ id: 'f1_macro', texto: 'F1 = ' + fmt(mTe.f1_macro, 2) + ': combina precisión (de lo que marca como X, cuánto es realmente X = ' + fmt(mTe.precision_macro, 2) + ') y cobertura (de lo que es X, cuánto detecta = ' + fmt(mTe.recall_macro, 2) + '), promediado entre categorías.' });
      im.metricas.push({ id: 'auc', texto: 'AUC = ' + fmt(mTe.auc, 2) + ': probabilidad de que un caso de una categoría reciba mayor puntaje que uno de otra. 0.5 es azar y 1 es separación perfecta.' });
      im.metricas.push({ id: 'log_loss', texto: 'Pérdida logarítmica = ' + fmt(mTe.log_loss, 3) + ': mide qué tan buenas son las probabilidades, no solo los aciertos. Menor es mejor; castiga fuerte estar muy seguro y equivocarse.' });
      im.metricas.push({ id: 'kappa', texto: 'Kappa = ' + fmt(mTe.kappa, 2) + ': acuerdo con la realidad descontando lo que se acertaría por azar (0 = azar, 1 = perfecto).' });
      im.graficas.push({ id: 'matriz_confusion', texto: 'Cada fila es la categoría real y cada columna la predicha. La diagonal son los aciertos; los números fuera de ella muestran qué categorías se confunden entre sí.' });
      im.graficas.push({ id: 'roc', texto: 'La curva ROC muestra cuántos casos detecta el modelo (vertical) a cambio de cuántas falsas alarmas (horizontal). Entre más se pegue a la esquina superior izquierda, mejor; la diagonal es azar. Área (AUC) = ' + fmt(mTe.auc, 2) + '.' });
      im.graficas.push({ id: 'calibracion', texto: 'Compara la probabilidad que dice el modelo con la frecuencia real. Si dice «70 %», debería acertar cerca de 7 de cada 10; puntos pegados a la diagonal indican probabilidades confiables.' });
    }
    im.graficas.push({ id: 'importancias', texto: 'Muestra cuánto empeora el modelo si se «revuelven» al azar los valores de cada variable (100 = la más importante). Barras largas indican variables de las que depende; las pequeñas casi no influyen. Las líneas de error muestran la variación entre repeticiones.' });
    im.graficas.push({ id: 'curva_aprendizaje', texto: curva.validacion.length ? 'Muestra cómo mejora el modelo al darle más datos. ' + (gap !== null && gap > 0.15 ? 'La brecha entre lo que acierta con sus datos y con los nuevos es grande (' + fmt(gap, 2) + '): más datos o un modelo más simple ayudarían.' : 'Las dos curvas están cerca, así que el modelo no está sobreajustado.') + ' Si la curva de validación aún sube al final, conseguir más filas mejoraría el resultado.' : 'No hubo datos suficientes para construir la curva de aprendizaje.' });
    if (rejillaRes.length > 1) im.graficas.push({ id: 'hiperparametros', texto: 'Cada punto es una combinación de ajustes probada con validación cruzada; el menor ' + (esReg ? 'error (RMSE)' : 'pérdida logarítmica') + ' es la mejor y se usó para el modelo final.' });

    return limpiar({
      enfoque: 'sup', tipoY: tipoY, metodo: id, nombreMetodo: M.nombre, y: cfg.y, yEtiqueta: yL, clases: clases, semilla: semilla,
      etapas: ej.etapas, tiempoMs: Date.now() - t00,
      prep: { pasos: pasosPre.concat(pa.pasos), nTrain: Atr.X.length, nTest: Ate.X.length, columnasFinales: pm.outCols.map(function (o) { return o.nombre; }), descartadas: pa.descartadas },
      metricas: { entrenamiento: mTr, prueba: mTe, cv: cvRes || { media: {}, sd: {}, folds: [] } },
      importancias: importancias, coeficientes: coeficientes,
      hiperparametros: { elegidos: parFinal, rejilla: rejillaRes, metrica: esReg ? 'RMSE (menor es mejor)' : 'Pérdida logarítmica (menor es mejor)', mejor: rejillaRes.length ? mejorIdx : null },
      predicciones: preds, residuos: residuos, matrizConfusion: cm, roc: roc, calibracion: calib, curvaAprendizaje: curva,
      advertencias: advertencias, kpis: kpis, hallazgos: hallazgos, interpretaciones: im
    });
  }

  // --- No supervisado -----------------------------------------------------------

  function inerciaDe(Xs, lab, k) {
    const p = Xs[0].length, C = [], cnt = new Float64Array(k);
    for (let c = 0; c < k; c++) C.push(new Float64Array(p));
    for (let i = 0; i < Xs.length; i++) { if (lab[i] < 0) continue; cnt[lab[i]]++; for (let j = 0; j < p; j++) C[lab[i]][j] += Xs[i][j]; }
    for (let c = 0; c < k; c++) if (cnt[c]) for (let j = 0; j < p; j++) C[c][j] /= cnt[c];
    let s = 0; for (let i = 0; i < Xs.length; i++) if (lab[i] >= 0) s += dist2(Xs[i], C[lab[i]]);
    return { inercia: s, centros: C };
  }
  function calidadSilueta(s) { return s === null ? 'no calculable' : (s >= 0.5 ? 'buena' : (s >= 0.25 ? 'aceptable' : 'débil')); }

  /** Ordena los grupos por tamaño y arma centroides, perfiles y etiquetas. */
  function armarClusters(labels, Xs, Xu, filas, nTot, outCols) {
    const cnt = new Map();
    for (let i = 0; i < labels.length; i++) if (labels[i] >= 0) cnt.set(labels[i], (cnt.get(labels[i]) || 0) + 1);
    const orden = Array.from(cnt.keys()).sort(function (a, b) { return cnt.get(b) - cnt.get(a) || a - b; });
    const nuevo = new Map(); orden.forEach(function (o, i) { nuevo.set(o, i); });
    const k = orden.length, p = Xs[0].length;
    const lab = new Int32Array(labels.length);
    for (let i = 0; i < labels.length; i++) lab[i] = labels[i] >= 0 ? nuevo.get(labels[i]) : -1;
    const asignacion = new Array(nTot).fill(-1);
    for (let i = 0; i < filas.length; i++) asignacion[filas[i]] = lab[i];
    const tamanos = new Array(k).fill(0), SU = [], SZ = [];
    for (let c = 0; c < k; c++) { SU.push(new Float64Array(p)); SZ.push(new Float64Array(p)); }
    let ruido = 0;
    for (let i = 0; i < lab.length; i++) {
      if (lab[i] < 0) { ruido++; continue; }
      tamanos[lab[i]]++;
      for (let j = 0; j < p; j++) { SU[lab[i]][j] += Xu[i][j]; SZ[lab[i]][j] += Xs[i][j]; }
    }
    const centroides = [], perfil = [], etiquetaSugerida = [];
    for (let c = 0; c < k; c++) {
      const cu = [], z = [];
      for (let j = 0; j < p; j++) { cu.push(SU[c][j] / tamanos[c]); z.push(SZ[c][j] / tamanos[c]); }
      centroides.push(cu);
      const idx = z.map(function (v, j) { return j; }).sort(function (a, b) { return Math.abs(z[b]) - Math.abs(z[a]); });
      let dist = idx.filter(function (j) { return Math.abs(z[j]) >= 0.5; }).slice(0, 4);
      if (!dist.length && idx.length) dist = [idx[0]];
      perfil.push({ cluster: c, n: tamanos[c], variablesDistintivas: dist.map(function (j) { return { var: outCols[j].label, z: z[j] }; }) });
      const altos = dist.filter(function (j) { return z[j] > 0; }).slice(0, 2).map(function (j) { return outCols[j].label; });
      const bajos = dist.filter(function (j) { return z[j] < 0; }).slice(0, 2).map(function (j) { return outCols[j].label; });
      const partes = [];
      if (altos.length) partes.push('Alto en ' + lista(altos));
      if (bajos.length) partes.push((partes.length ? 'bajo' : 'Bajo') + ' en ' + lista(bajos));
      etiquetaSugerida.push(partes.length ? partes.join(', ') : 'Cercano al promedio en todo');
    }
    return { k: k, asignacion: asignacion, tamanos: tamanos, centroides: centroides, perfil: perfil, etiquetaSugerida: etiquetaSugerida, ruido: ruido, labels: lab };
  }

  async function correrNoSup(cfg, ds, ej) {
    const t00 = Date.now();
    const lectura = leerDataset(ds), perfiles = lectura.map(function (c) { return c.perfil; }), porNombre = {};
    lectura.forEach(function (c) { porNombre[c.name] = c; });
    const id = cfg.metodo, M = META[id];
    if (!M || M.tipo !== 'nosup') throw new Error('El método «' + id + '» no existe o no es de aprendizaje no supervisado.');
    const nombresX = (cfg.x && cfg.x.length) ? cfg.x : lectura.filter(function (c) { return c.name !== cfg.y; }).map(function (c) { return c.name; });
    nombresX.forEach(function (nm) { if (!porNombre[nm]) throw new Error('La variable «' + nm + '» no existe en los datos.'); });
    if (!nombresX.length) throw new Error('Elige al menos una variable para el análisis.');
    const colsX = nombresX.map(function (nm) { return porNombre[nm]; });
    const nTot = ds.rows.length, semilla = cfg.semilla == null ? 42 : cfg.semilla, rng = crearRng(semilla);
    if (nTot < M.min) throw new Error('El método «' + M.nombre + '» necesita al menos ' + M.min + ' filas y hay ' + nTot + '.');
    const cp = configPrep(cfg, perfiles, { enfoque: 'nosup', y: null, x: nombresX, n: nTot, metodo: id });

    let pa, pm, A, P, cols;
    await ej.etapa('prep', 'Preparación de los datos', async function (rep) {
      const todas = []; for (let i = 0; i < nTot; i++) todas.push(i);
      pa = ajustarPrep(colsX, todas, { g: cp.g, pc: cp.pc });
      pm = pa.modelo; A = aplicarPrep(pm, pa.rows, true);
      cols = pm.outCols.map(function (o, j) { return j; });
      if (id === 'correlaciones') cols = cols.filter(function (j) { return pm.outCols[j].esNum; });
      P = cols.length;
      if (A.X.length < M.min) throw new Error('El método «' + M.nombre + '» necesita al menos ' + M.min + ' filas completas y quedaron ' + A.X.length + ' después de la limpieza.');
      if (P < 2 && id !== 'anomalias') throw new Error('Este método necesita al menos 2 variables numéricas utilizables y quedaron ' + P + '.');
      return 'Quedaron ' + P + ' variables y ' + A.X.length + ' filas de ' + nTot + '.';
    });
    const outCols = cols.map(function (j) { return pm.outCols[j]; });
    const sel = function (X) { return X.map(function (r) { const o = new Float64Array(cols.length); for (let k = 0; k < cols.length; k++) o[k] = r[cols[k]]; return o; }); };
    const nU = A.X.length, filas = A.filas;
    const Xu = sel(A.Xu), Xe = sel(A.X);
    let Xs = null, pcaR = null;
    await ej.etapa('reduccion', 'Estandarización y componentes', async function (rep) {
      const E = estdAjustar(Xe); Xs = estdAplicar(E, Xe);
      if (P >= 2 && P <= 200) pcaR = pcaDe(Xs, P);
      return pcaR ? 'Variables estandarizadas; los ' + Math.min(3, P) + ' primeros componentes explican ' + fpct(pcaR.acumulada[Math.min(2, P - 1)], 0) + ' de la variación.' : 'Variables estandarizadas.';
    });

    const res = { enfoque: 'nosup', metodo: id, nombreMetodo: M.nombre, semilla: semilla };
    let labelsFin = null, extra = {}, elbow = [], kElegido = null, notaK = '';
    await ej.etapa('ajuste', 'Ajuste del método', async function (rep) {
      if (id === 'pca') {
        if (!pcaR) throw new Error('No se pudo calcular el PCA con ' + P + ' variables.');
        return 'Se calcularon ' + P + ' componentes principales.';
      }
      if (id === 'kmeans') {
        const kMax = Math.max(2, Math.min(10, Math.floor(nU / 10)));
        let k = cfg.k;
        if (k != null && k >= 2) { k = Math.min(Math.round(k), nU - 1); notaK = 'k fijado por ti en ' + k + '.'; }
        else {
          const Xsel = nU > 1500 ? rng.muestra(nU, 1500).map(function (i) { return Xs[i]; }) : Xs;
          for (let kk = 2; kk <= kMax; kk++) {
            const r = await kmeansAjustar(Xsel, kk, rng, 2);
            elbow.push({ k: kk, inercia: r.inercia * nU / Xsel.length, silueta: silueta(Xsel, r.labels, 600, rng) });
            rep(0.8 * (kk - 1) / (kMax - 1), 'Probando ' + kk + ' grupos');
          }
          let mj = null; elbow.forEach(function (e) { if (e.silueta !== null && (mj === null || e.silueta > mj.silueta)) mj = e; });
          k = mj ? mj.k : 2; notaK = 'k elegido automáticamente por silueta (se probó de 2 a ' + kMax + ').';
        }
        kElegido = k;
        const r = await kmeansAjustar(Xs, k, rng, 5);
        labelsFin = r.labels;
        if (!elbow.length) { const r2 = r; elbow.push({ k: k, inercia: r2.inercia, silueta: silueta(Xs, r2.labels, 600, rng) }); }
        return 'k-medias con ' + k + ' grupos. ' + notaK;
      }
      if (id === 'jerarquico') {
        const m = Math.min(nU, 1500), ids = nU > 1500 ? rng.muestra(nU, 1500) : null;
        const Xm = ids ? ids.map(function (i) { return Xs[i]; }) : Xs;
        rep(0.1, 'Construyendo el árbol de uniones');
        const fus = await wardFusiones(Xm);
        const kMax = Math.max(2, Math.min(8, Math.floor(m / 10)));
        let k = cfg.k;
        if (k != null && k >= 2) { k = Math.min(Math.round(k), m - 1); notaK = 'k fijado por ti en ' + k + '.'; }
        for (let kk = 2; kk <= kMax; kk++) {
          const lab = cortarWard(fus, m, kk);
          elbow.push({ k: kk, inercia: inerciaDe(Xm, lab, kk).inercia, silueta: silueta(Xm, lab, 600, rng) });
        }
        if (!(k != null && k >= 2)) { let mj = null; elbow.forEach(function (e) { if (e.silueta !== null && (mj === null || e.silueta > mj.silueta)) mj = e; }); k = mj ? mj.k : 2; notaK = 'k elegido automáticamente por silueta (se probó de 2 a ' + kMax + ').'; }
        kElegido = k;
        const labM = cortarWard(fus, m, k), ine = inerciaDe(Xm, labM, k);
        const lab = new Int32Array(nU).fill(-1);
        if (ids) {
          ids.forEach(function (i, a) { lab[i] = labM[a]; });
          for (let i = 0; i < nU; i++) { if (lab[i] >= 0) continue; let b = 0, bd = Infinity; for (let c = 0; c < k; c++) { const d = dist2(Xs[i], ine.centros[c]); if (d < bd) { bd = d; b = c; } } lab[i] = b; }
          extra.muestraJerarquico = m;
        } else for (let i = 0; i < nU; i++) lab[i] = labM[i];
        labelsFin = lab;
        return 'Ward con ' + k + ' grupos' + (ids ? ' (muestra de ' + m + ' filas; el resto se asignó al grupo más cercano)' : '') + '. ' + notaK;
      }
      if (id === 'dbscan') {
        const minPts = Math.max(4, Math.min(P + 1, 10)), mUso = Math.min(nU, 4000);
        const ids = nU > mUso ? rng.muestra(nU, mUso) : null;
        const Xm = ids ? ids.map(function (i) { return Xs[i]; }) : Xs;
        const eps = epsAuto(Xm, minPts, rng);
        rep(0.3, 'Buscando grupos densos (distancia ' + fmt(eps, 2) + ')');
        const r = await dbscanAjustar(Xm, eps, minPts);
        const lab = new Int32Array(nU).fill(-1);
        if (ids) {
          ids.forEach(function (i, a) { lab[i] = r.labels[a]; });
          const enM = new Uint8Array(nU); ids.forEach(function (i) { enM[i] = 1; });
          for (let i = 0; i < nU; i++) {
            if (enM[i]) continue; let b = -1, bd = eps * eps;
            for (let a = 0; a < ids.length; a++) { if (r.labels[a] < 0) continue; const d = dist2(Xs[i], Xm[a]); if (d <= bd) { bd = d; b = r.labels[a]; } }
            lab[i] = b;
          }
        } else for (let i = 0; i < nU; i++) lab[i] = r.labels[i];
        labelsFin = lab; extra = { eps: eps, minPts: minPts };
        return 'DBSCAN con distancia ' + fmt(eps, 2) + ' y mínimo ' + minPts + ' vecinos: ' + r.k + ' grupo(s).';
      }
      if (id === 'correlaciones') {
        const nV = P, cp1 = [], cs1 = [], colsV = [], colsR = [];
        for (let j = 0; j < nV; j++) { const c = new Float64Array(nU); for (let i = 0; i < nU; i++) c[i] = Xu[i][j]; colsV.push(c); colsR.push(rangos(c)); }
        for (let a = 0; a < nV; a++) { cp1.push(new Array(nV).fill(null)); cs1.push(new Array(nV).fill(null)); }
        for (let a = 0; a < nV; a++) for (let b = a; b < nV; b++) {
          const r = a === b ? 1 : pearson(colsV[a], colsV[b]), s = a === b ? 1 : pearson(colsR[a], colsR[b]);
          cp1[a][b] = cp1[b][a] = isNaN(r) ? null : r; cs1[a][b] = cs1[b][a] = isNaN(s) ? null : s;
        }
        extra.cor = { vars: outCols.map(function (o) { return o.label; }), pearson: cp1, spearman: cs1 };
        const pares = [];
        for (let a = 0; a < nV; a++) for (let b = a + 1; b < nV; b++) {
          const r = cp1[a][b], s = cs1[a][b], fuerza = Math.max(Math.abs(r || 0), Math.abs(s || 0));
          if (fuerza >= 0.5) pares.push({ a: outCols[a].label, b: outCols[b].label, r: r, rho: s, _f: fuerza });
        }
        pares.sort(function (x, y) { return y._f - x._f; });
        extra.cor.paresFuertes = pares.slice(0, 20).map(function (q) { return { a: q.a, b: q.b, r: q.r, rho: q.rho }; });
        return 'Se calcularon correlaciones de Pearson y Spearman entre ' + nV + ' variables.';
      }
      if (id === 'anomalias') {
        const r = mahalanobisRobusta(Xs), umbral = Math.sqrt(chi2Inv(0.99, P));
        const punt = new Array(nTot).fill(null), idxA = [];
        for (let i = 0; i < nU; i++) { punt[filas[i]] = Math.sqrt(r.d2[i]); if (Math.sqrt(r.d2[i]) > umbral) idxA.push(filas[i]); }
        const orden = idxA.slice().sort(function (a, b) { return punt[b] - punt[a]; }).slice(0, 20);
        const posDe = new Map(); filas.forEach(function (f, i) { posDe.set(f, i); });
        const que = orden.map(function (f) {
          const i = posDe.get(f), z = []; for (let j = 0; j < P; j++) z.push((Xs[i][j] - r.mu[j]) / r.esc[j]);
          const o = z.map(function (v, j) { return j; }).sort(function (a, b) { return Math.abs(z[b]) - Math.abs(z[a]); }).slice(0, 3).filter(function (j) { return Math.abs(z[j]) > 1; });
          return { idx: f, vars: o.map(function (j) { return outCols[j].label + (z[j] > 0 ? ' (muy alto)' : ' (muy bajo)'); }) };
        });
        extra.anom = { puntaje: punt, umbral: umbral, idx: idxA, variablesQueMasAportan: que };
        return 'Se marcaron ' + idxA.length + ' casos anómalos de ' + nU + ' (umbral de distancia ' + fmt(umbral, 2) + ').';
      }
      return '';
    });

    let silRes = null, ch = null, db = null, cl = null;
    await ej.etapa('validacion', 'Validación del resultado', async function (rep) {
      if (labelsFin) {
        cl = armarClusters(labelsFin, Xs, Xu, filas, nTot, outCols);
        if (cl.k >= 2) { silRes = silueta(Xs, cl.labels, 1500, rng); const ii = indicesInternos(Xs, cl.labels); ch = ii.ch; db = ii.db; }
        return cl.k >= 2 ? 'Silueta ' + fmt(silRes, 2) + ' (' + calidadSilueta(silRes) + '); Calinski-Harabasz ' + fmt(ch, 0) + '; Davies-Bouldin ' + fmt(db, 2) + '.' : 'Se encontraron ' + cl.k + ' grupo(s): no hay índices de separación que calcular.';
      }
      if (id === 'pca') return 'Varianza acumulada con ' + pcaR.nComponentes80 + ' componente(s): ' + fpct(pcaR.acumulada[pcaR.nComponentes80 - 1], 0) + '.';
      if (id === 'anomalias') return 'Umbral basado en la distribución chi-cuadrada (99 %).';
      return 'Este método no requiere validación adicional.';
    });

    const hallazgos = [], kpis = [], im = { metricas: [], graficas: [] };
    await ej.etapa('interpretacion', 'Interpretación', async function (rep) {
      const nombresPC = outCols.map(function (o) { return o.label; });
      if (pcaR) {
        res.pca = {
          varianza: pcaR.varianza, acumulada: pcaR.acumulada, nComponentes80: pcaR.nComponentes80, scores: pcaR.scores, scoresIdx: filas.slice(),
          cargas: pcaR.cargas.map(function (c) { return { var: nombresPC[c.idxVar], pc: c.pc }; })
        };
      }
      if (id === 'pca') {
        const v = pcaR.varianza, a = pcaR.acumulada, p1 = pcaR.cargas.map(function (c) { return { v: nombresPC[c.idxVar], l: c.pc[0] }; }).sort(function (x, y) { return Math.abs(y.l) - Math.abs(x.l); }).slice(0, 3);
        hallazgos.push('Los 2 primeros componentes explican ' + fpct(a[Math.min(1, P - 1)], 0) + ' de la variación; se necesitan ' + pcaR.nComponentes80 + ' para llegar a 80 %.');
        hallazgos.push('El primer componente (' + fpct(v[0], 0) + ' de la variación) está dominado por ' + lista(p1.map(function (q) { return '«' + q.v + '»' + (q.l >= 0 ? ' (+)' : ' (−)'); })) + '.');
        if (pcaR.nComponentes80 <= Math.max(2, Math.ceil(P / 3))) hallazgos.push('Tus ' + P + ' variables se pueden resumir bien en ' + pcaR.nComponentes80 + ' dimensiones: hay mucha información repetida.');
        else hallazgos.push('La información está repartida entre muchas dimensiones: las variables aportan datos distintos y es difícil resumirlas en pocas.');
        kpis.push({ id: 'n80', etiqueta: 'Componentes para 80 %', valor: pcaR.nComponentes80, formato: 'num', nota: 'De ' + P + ' variables', tono: pcaR.nComponentes80 <= Math.ceil(P / 3) ? 'bueno' : 'medio' });
        kpis.push({ id: 'pc1', etiqueta: 'Varianza del 1.er componente', valor: v[0], formato: 'pct', nota: 'Parte de la variación que resume el eje principal', tono: 'neutro' });
        kpis.push({ id: 'pc2', etiqueta: 'Varianza 1.er + 2.º', valor: a[Math.min(1, P - 1)], formato: 'pct', nota: 'Lo que se ve en un mapa de dos ejes', tono: tonoPct(a[Math.min(1, P - 1)], 0.6, 0.4) });
        im.metricas.push({ id: 'varianza', texto: 'Cada componente es una combinación de variables; su porcentaje indica cuánta de la variación total de los datos resume. Los primeros resumen lo más importante.' });
        im.graficas.push({ id: 'varianza', texto: 'La gráfica de varianza muestra cuánto aporta cada componente; donde la curva acumulada llega a 80 % basta para resumir los datos (aquí: ' + pcaR.nComponentes80 + ' componente(s)).' });
        im.graficas.push({ id: 'cargas', texto: 'Las cargas indican qué variables forman cada componente: valores lejanos de cero pesan mucho, y el signo indica el sentido. Variables con cargas parecidas se mueven juntas.' });
        im.graficas.push({ id: 'scores', texto: 'Cada punto es un caso proyectado en los dos primeros componentes. Casos cercanos se parecen; los alejados son diferentes del resto.' });
      }
      if (cl) {
        const tot = cl.tamanos.reduce(function (s, x) { return s + x; }, 0) || 1;
        res.clusters = {
          k: cl.k, asignacion: cl.asignacion, tamanos: cl.tamanos, centroides: cl.centroides, columnas: outCols.map(function (o) { return o.label; }), perfil: cl.perfil,
          silueta: silRes, indiceCH: ch, indiceDB: db, elbow: elbow, etiquetaSugerida: cl.etiquetaSugerida, ruido: cl.ruido
        };
        if (extra.eps !== undefined) { res.clusters.eps = extra.eps; res.clusters.minPts = extra.minPts; }
        if (extra.muestraJerarquico) res.clusters.muestra = extra.muestraJerarquico;
        if (cl.k === 0) {
          hallazgos.push('No se encontraron grupos con la distancia elegida (todo se consideró ruido). Prueba con k-medias o jerárquico.');
        } else {
          hallazgos.push('Se identificaron ' + cl.k + ' grupo(s) entre ' + tot + ' casos. ' + (notaK || ''));
          cl.perfil.slice(0, 5).forEach(function (pf) { hallazgos.push('Grupo ' + (pf.cluster + 1) + ' (' + pf.n + ' casos, ' + fpct(pf.n / nU, 0) + '): ' + cl.etiquetaSugerida[pf.cluster] + '.'); });
          if (cl.k >= 2) hallazgos.push('La separación entre grupos es ' + calidadSilueta(silRes) + ' (silueta = ' + fmt(silRes, 2) + ').' + (silRes !== null && silRes < 0.25 ? ' Los grupos se traslapan bastante: puede que los datos no formen grupos naturales.' : ''));
        }
        if (cl.ruido > 0) hallazgos.push(cl.ruido + ' casos (' + fpct(cl.ruido / nU, 1) + ') quedaron fuera de todos los grupos (ruido o casos aislados).');
        if (nU < nTot) hallazgos.push('Advertencia: ' + (nTot - nU) + ' filas no se usaron por datos vacíos o limpieza; en la asignación aparecen con -1.');
        const mayor = cl.k ? Math.max.apply(null, cl.tamanos) / nU : null;
        kpis.push({ id: 'k', etiqueta: 'Grupos encontrados', valor: cl.k, formato: 'num', nota: notaK || 'Detectados automáticamente', tono: 'neutro' });
        kpis.push({ id: 'silueta', etiqueta: 'Calidad de separación (silueta)', valor: silRes, formato: 'dec2', nota: 'De -1 a 1; más de 0.5 es buena', tono: silRes === null ? 'neutro' : (silRes >= 0.5 ? 'bueno' : (silRes >= 0.25 ? 'medio' : 'malo')) });
        if (cl.k) kpis.push({ id: 'mayor', etiqueta: 'Grupo más grande', valor: mayor, formato: 'pct', nota: 'Parte de los casos en el mayor grupo', tono: mayor > 0.7 ? 'medio' : 'neutro' });
        if (cl.ruido > 0 || id === 'dbscan') kpis.push({ id: 'ruido', etiqueta: 'Casos fuera de grupos', valor: cl.ruido / nU, formato: 'pct', nota: 'Ruido o casos aislados', tono: cl.ruido / nU > 0.3 ? 'malo' : 'neutro' });
        im.metricas.push({ id: 'silueta', texto: 'Silueta = ' + fmt(silRes, 2) + ': mide qué tan cerca está cada caso de su grupo frente al grupo vecino. Más de 0.5 es buena separación, 0.25–0.5 aceptable y menos de 0.25 débil.' });
        im.metricas.push({ id: 'indiceCH', texto: 'Calinski-Harabasz = ' + fmt(ch, 0) + ': compara la separación entre grupos con su compacidad interna. Sirve para comparar soluciones con distinto número de grupos: mayor es mejor.' });
        im.metricas.push({ id: 'indiceDB', texto: 'Davies-Bouldin = ' + fmt(db, 2) + ': mide el traslape promedio entre grupos. Menor es mejor; cerca de 0 significa grupos bien separados.' });
        im.graficas.push({ id: 'scores', texto: 'Mapa de los casos en dos dimensiones, coloreados por grupo. Si los colores forman nubes separadas, los grupos son claros; si se mezclan, hay traslape.' });
        if (elbow.length) im.graficas.push({ id: 'elbow', texto: 'La gráfica del codo muestra cómo baja la dispersión dentro de los grupos al agregar más grupos. Se busca el punto donde deja de bajar rápido; la silueta ayuda a elegir el mejor.' });
        im.graficas.push({ id: 'perfil', texto: 'El perfil de cada grupo muestra en qué variables se distingue del promedio (en desviaciones estándar): positivo = más alto que el promedio, negativo = más bajo.' });
      }
      if (extra.cor) {
        res.correlaciones = extra.cor;
        const pf = extra.cor.paresFuertes;
        if (pf.length) {
          hallazgos.push('Se encontraron ' + pf.length + ' pares de variables con relación fuerte (≥ 0.5). La más alta: «' + pf[0].a + '» y «' + pf[0].b + '» (Pearson ' + fmt(pf[0].r, 2) + ').');
          pf.filter(function (q) { return Math.abs(q.r || 0) >= 0.9; }).slice(0, 3).forEach(function (q) { hallazgos.push('«' + q.a + '» y «' + q.b + '» casi dicen lo mismo (r = ' + fmt(q.r, 2) + '); quizá sobra una al modelar.'); });
          const nl = pf.filter(function (q) { return q.rho !== null && q.r !== null && Math.abs(q.rho) - Math.abs(q.r) > 0.15; })[0];
          if (nl) hallazgos.push('«' + nl.a + '» y «' + nl.b + '» tienen una relación constante pero no en línea recta (Spearman ' + fmt(nl.rho, 2) + ' frente a Pearson ' + fmt(nl.r, 2) + ').');
        } else hallazgos.push('No hay pares de variables con relación fuerte (todas por debajo de 0.5): aportan información distinta.');
        kpis.push({ id: 'pares', etiqueta: 'Pares con relación fuerte', valor: pf.length, formato: 'num', nota: '|correlación| ≥ 0.5', tono: 'neutro' });
        if (pf.length) kpis.push({ id: 'max', etiqueta: 'Correlación más alta', valor: pf[0].r, formato: 'dec2', nota: '«' + pf[0].a + '» con «' + pf[0].b + '»', tono: 'neutro' });
        im.metricas.push({ id: 'pearson', texto: 'Pearson mide qué tanto dos variables suben o bajan juntas en línea recta: 1 = suben juntas, -1 = una sube cuando la otra baja, 0 = sin relación lineal.' });
        im.metricas.push({ id: 'spearman', texto: 'Spearman usa solo el orden de los valores; capta relaciones que van siempre en un sentido aunque no sean rectas y le afectan menos los valores extremos.' });
        im.graficas.push({ id: 'correlaciones', texto: 'El mapa de calor muestra la correlación de cada par: tonos fuertes indican relación alta. Correlación no significa que una variable cause la otra.' });
      }
      if (extra.anom) {
        res.anomalias = extra.anom;
        const na = extra.anom.idx.length;
        hallazgos.push(na ? 'Se marcaron ' + na + ' casos (' + fpct(na / nU, 1) + ') como anómalos: se alejan del patrón general más que el 99 % esperado.' : 'No se detectaron casos anómalos con el umbral usado.');
        if (extra.anom.variablesQueMasAportan.length && extra.anom.variablesQueMasAportan[0].vars.length) hallazgos.push('El caso más raro (fila ' + (extra.anom.variablesQueMasAportan[0].idx + 1) + ') se distingue por: ' + extra.anom.variablesQueMasAportan[0].vars.join(', ') + '.');
        if (na / nU > 0.1) hallazgos.push('Advertencia: más de 10 % de los casos salen anómalos; quizá los datos tienen varios grupos distintos y conviene segmentarlos antes.');
        kpis.push({ id: 'n_anom', etiqueta: 'Casos anómalos', valor: na, formato: 'num', nota: fpct(na / nU, 1) + ' de los casos', tono: na / nU > 0.1 ? 'medio' : 'neutro' });
        kpis.push({ id: 'umbral', etiqueta: 'Umbral de distancia', valor: extra.anom.umbral, formato: 'dec2', nota: 'Distancia de Mahalanobis a partir de la cual un caso es raro', tono: 'neutro' });
        im.metricas.push({ id: 'puntaje', texto: 'El puntaje es la distancia de Mahalanobis robusta: cuántas «desviaciones» se aleja un caso del centro, tomando en cuenta cómo se relacionan las variables entre sí. Un caso puede ser raro por una combinación inusual aunque cada valor sea normal.' });
        im.graficas.push({ id: 'anomalias', texto: 'Cada barra o punto es un caso; los que superan la línea del umbral (' + fmt(extra.anom.umbral, 2) + ') son anómalos. Revísalos: pueden ser errores de captura o casos legítimos poco frecuentes.' });
      }
      if (pa.pasos.length) hallazgos.push('Preparación aplicada: ' + pa.pasos.length + ' paso(s) (ver detalle en preparación).');
      return 'Se generaron ' + hallazgos.length + ' hallazgos.';
    });

    res.etapas = ej.etapas; res.tiempoMs = Date.now() - t00;
    res.prep = { pasos: pa.pasos, nTrain: nU, nTest: 0, columnasFinales: outCols.map(function (o) { return o.nombre; }), descartadas: pa.descartadas };
    res.kpis = kpis; res.hallazgos = hallazgos; res.interpretaciones = im;
    return limpiar(res);
  }

  // ==========================================================================
  // 12. API PÚBLICA
  // ==========================================================================

  /** Ejecuta el análisis completo; se resuelve con los resultados (o se rechaza con un error en español). */
  function correr(cfg, ds, onEtapa) {
    return (async function () {
      cfg = cfg || {};
      const ej = crearEjecutor(onEtapa);
      if (cfg.enfoque === 'nosup') return correrNoSup(cfg, ds, ej);
      if (cfg.enfoque === 'sup') return correrSup(cfg, ds, ej);
      throw new Error('Indica el enfoque del análisis: «sup» (predecir una variable) o «nosup» (explorar sin variable objetivo).');
    })();
  }

  return {
    version: '1.0',
    perfilar: perfilar,
    catalogoPrep: catalogoPrep,
    sugerirPrep: sugerirPrep,
    sugerirMetodos: sugerirMetodos,
    correr: correr,
    // Funciones internas expuestas solo para las pruebas
    _t: {
      parseNum: parseNum, tCDF: tCDF, tPvalor: tPvalor, tInv: tInv, normCDF: normCDF, normInv: normInv, chi2CDF: chi2CDF, chi2Inv: chi2Inv, betainc: betainc,
      olsAjustar: olsAjustar, jacobi: jacobi, cholesky: cholesky, invertir: invertir, crearRng: crearRng, leerDataset: leerDataset,
      ajustarPrep: ajustarPrep, aplicarPrep: aplicarPrep, rangos: rangos, aucBinaria: aucBinaria, yeoJohnson: yeoJohnson, fmt: fmt
    }
  };
});
