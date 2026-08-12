#!/usr/bin/env node
// Snapshot horario del backend: baja todos los alojamientos y escribe
// public/guia/data/<HouseId>.json + public/guia/data/index.json.
// El frontend lee estos JSON estáticos desde el mismo dominio
// www.check-inn.mx (Fastly, no Google Cloud) para evitar bloqueos ISP
// móviles a *.run.app / IPs de Google Cloud.
//
// Uso: node scripts/sync-guias.js
// Falla con exit != 0 si el backend no responde (para que el Action
// no borre los JSONs previos ni haga commit vacío).

'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');

const BACKEND = process.env.BACKEND_URL || 'https://ticket-vision-957627511957.us-central1.run.app';
const OUT_DIR = path.join(__dirname, '..', 'public', 'guia', 'data');

function fetchJson(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: timeoutMs || 30000 }, (res) => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        reject(new Error('HTTP ' + res.statusCode + ' de ' + url));
        return;
      }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
        catch (e) { reject(e); }
      });
    });
    req.on('timeout', () => { req.destroy(new Error('timeout')); });
    req.on('error', reject);
  });
}

async function main() {
  console.log('[sync-guias] backend:', BACKEND);
  console.log('[sync-guias] out dir:', OUT_DIR);
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const data = await fetchJson(BACKEND + '/alojamientos-list', 60000);
  const rows = Array.isArray(data.rows) ? data.rows : [];
  if (!rows.length) throw new Error('backend devolvió 0 rows — abortando para no borrar JSONs previos');
  console.log('[sync-guias] rows recibidos:', rows.length);

  const generatedAt = new Date().toISOString();
  let written = 0;
  const summary = [];

  for (const row of rows) {
    const id = String(row.HouseId || row.HouseID || row.ID || '').trim();
    if (!id) continue;
    const payload = { ok: true, generatedAt, rows: [row] };
    const outFile = path.join(OUT_DIR, id + '.json');
    fs.writeFileSync(outFile, JSON.stringify(payload));
    written++;
    summary.push({ id, name: row.HouseName || '' });
  }

  // Index para debugging / futura navegación
  fs.writeFileSync(
    path.join(OUT_DIR, 'index.json'),
    JSON.stringify({ ok: true, generatedAt, count: written, items: summary }, null, 2)
  );

  console.log('[sync-guias] archivos escritos:', written);
}

main().catch((e) => {
  console.error('[sync-guias] ERROR:', e.message);
  process.exit(1);
});
