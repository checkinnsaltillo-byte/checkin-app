/* GeoMetrika: acceso por contraseña. Agrega el token (x-gm-token) a las peticiones protegidas y, si falta o venció, pide la contraseña. */
(function () {
  if (window.__gmAuth) return; window.__gmAuth = 1;
  var API = 'https://api.check-inn.mx', G = /^https:\/\/api\.check-inn\.mx\/(mercado\/(maqueta|ambiente)|geometrika)\//, X = /\/geometrika\/acceso|\/mercado\/maqueta\/fachada\/img|\/mercado\/ambiente\/aire\/tile/;
  function tok() { try { var t = JSON.parse(localStorage.getItem('gmTok') || 'null'); return t && t.exp > Date.now() ? t.token : ''; } catch (e) { return ''; } }
  window.gmGuarda = function (t) { try { if (t && t.token) localStorage.setItem('gmTok', JSON.stringify({ token: t.token, exp: t.exp })); } catch (e) { } };
  var f0 = window.fetch.bind(window), pend = null;
  function pide() {
    if (pend) return pend;
    pend = new Promise(function (ok) {
      if (window.GM_NOCANCEL) {   /* maquetas: no se escribe contraseña aquí; el acceso viene de haber iniciado sesión en GeoMetrika */
        var q = document.createElement('div'); q.style.cssText = 'position:fixed;inset:0;z-index:2147483000;background:rgba(15,23,42,.6);display:flex;align-items:center;justify-content:center;font:13px "JetBrains Mono",ui-monospace,Menlo,monospace';
        q.innerHTML = '<div style="background:#fcfcfb;border:1px solid #dcdcda;border-top:2px solid #111;width:min(92vw,380px);padding:24px 24px 20px;color:#111"><div style="font-weight:600;font-size:14px;margin-bottom:6px">Acceso restringido</div><div style="color:#555;font-size:12px;line-height:1.6;margin-bottom:16px">Esta información es privada. Inicia sesión en GeoMetrika y vuelve a abrir esta liga.</div><a href="/geometrika/" style="display:inline-block;font:inherit;padding:10px 16px;background:#111;color:#fff;text-decoration:none;border-radius:6px">Iniciar sesión en GeoMetrika</a></div>';
        document.body.appendChild(q); return ok(false);
      }
      var d = document.createElement('div'); d.style.cssText = 'position:fixed;inset:0;z-index:2147483000;background:rgba(15,23,42,.55);display:flex;align-items:center;justify-content:center;font:13px "JetBrains Mono",ui-monospace,Menlo,monospace';
      d.innerHTML = '<div style="background:#fcfcfb;border:1px solid #dcdcda;border-top:2px solid #111;width:min(92vw,360px);padding:22px 22px 18px;color:#111"><div style="font-weight:600;font-size:14px;margin-bottom:4px">GeoMetrika</div><div style="color:#6b6b6b;font-size:12px;margin-bottom:14px">Escribe la contraseña para ver la información.</div><input type="password" autocomplete="current-password" placeholder="Contraseña" style="width:100%;box-sizing:border-box;font:inherit;padding:10px 12px;border:1px solid #dcdcda;background:#f3f3f2;outline:0;border-radius:0"><div class="e" style="color:#b91c1c;font-size:11.5px;min-height:16px;margin:6px 0 4px"></div><div style="display:flex;gap:8px;justify-content:flex-end"><button class="c" style="font:inherit;padding:9px 14px;border:1px solid #d6d6d4;background:#f6f6f5;cursor:pointer;border-radius:0">Cancelar</button><button class="g" style="font:inherit;padding:9px 16px;border:0;background:#111;color:#fff;cursor:pointer;border-radius:6px">Entrar</button></div></div>';
      document.body.appendChild(d); var i = d.querySelector('input'), e = d.querySelector('.e'), g = d.querySelector('.g'), c = d.querySelector('.c'); if (window.GM_NOCANCEL) c.style.display = 'none'; setTimeout(function () { i.focus(); }, 50);
      function fin(v) { d.remove(); pend = null; ok(v); }
      function entra() { var v = i.value.trim(); if (!v) { e.textContent = 'Escribe la contraseña.'; return; } g.disabled = true; g.textContent = 'Verificando…';
        f0(API + '/geometrika/acceso', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clave: v }) }).then(function (r) { return r.json(); }).then(function (j) { if (j && j.ok) { window.gmGuarda(j); fin(true); } else { e.textContent = (j && j.error) || 'Contraseña incorrecta.'; g.disabled = false; g.textContent = 'Entrar'; i.select(); } }).catch(function () { e.textContent = 'No se pudo conectar. Intenta de nuevo.'; g.disabled = false; g.textContent = 'Entrar'; }); }
      g.onclick = entra; c.onclick = function () { fin(false); }; i.onkeydown = function (ev) { if (ev.key === 'Enter') entra(); };
    });
    return pend;
  }
  window.gmPideClave = pide;
  window.fetch = function (u, o) {
    var url = typeof u === 'string' ? u : (u && u.url) || ''; if (!G.test(url) || X.test(url)) return f0(u, o);
    var enviado = '';
    function go() { var t = tok(); enviado = t;  var h = new Headers((o && o.headers) || (u && u.headers) || {}); if (t) h.set('x-gm-token', t); if (typeof u === 'string' || u instanceof URL) return f0(u, Object.assign({}, o || {}, { headers: h })); return f0(new Request(u, { headers: h })); }
    return go().then(function (r) { if (r.status !== 401) return r; return r.clone().json().catch(function () { return null; }).then(function (j) { if (!(j && j.gm)) return r; if (tok() && tok() !== enviado) return go(); return pide().then(function (ok) { return ok ? go() : r; }); }); });
  };
})();
