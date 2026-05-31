#!/usr/bin/env python3
"""
Envía Web Push notifications a usuarios suscritos del Portal Check-inn.

Requisitos:
    pip install pywebpush requests

Uso:
    # Mandar a UN usuario por phoneKey
    python3 push_sender.py --phone +528115569120 --title "Tu reserva está lista" \
                           --body "Pasa por la recepción a recoger tus llaves."

    # Mandar a TODOS los suscritos
    python3 push_sender.py --all --title "Mantenimiento programado" \
                           --body "Mañana el portal estará en mantenimiento de 2am a 4am."

    # Aumentar el badge rojo del ícono a N
    python3 push_sender.py --phone +52... --title "Aviso" --body "..." --badge 3

Variables de entorno (o edita CONFIG abajo):
    VAPID_PRIVATE_PEM   ruta al .pem (default: otros/vapid_private.pem)
    VAPID_SUBJECT       mailto: o https:// que te identifica como sender
    WEB_APP_URL         endpoint del Apps Script (mismo que usa el frontend)
"""
import argparse, json, os, sys, time, requests
from pywebpush import webpush, WebPushException

# ─── CONFIG ───────────────────────────────────────────────────────────────────
WEB_APP_URL = os.environ.get(
    "WEB_APP_URL",
    "https://script.google.com/macros/s/AKfycbwqMfC6tITLXlhEwYzQ5mKzw-KD6-nV7XVKIuekj6pK4Po50oRfVKClZeHcr-si3ppB/exec"
)
VAPID_PRIVATE_PEM = os.environ.get("VAPID_PRIVATE_PEM", os.path.join(os.path.dirname(__file__), "vapid_private.pem"))
VAPID_SUBJECT = os.environ.get("VAPID_SUBJECT", "mailto:correodeandrescarreon@gmail.com")
# ──────────────────────────────────────────────────────────────────────────────


def _normalize_phone(s):
    """Quita '+' y todo lo no-numérico para comparar phoneKeys robustamente."""
    return "".join(c for c in str(s) if c.isdigit())


def list_subscriptions(phone_key=None):
    # Trae todas y filtra localmente — Sheets puede haber guardado el phoneKey
    # como número (sin '+') al recibirlo del frontend.
    r = requests.get(WEB_APP_URL, params={"action": "list_push_subscriptions"},
                     allow_redirects=True, timeout=30)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"list_push_subscriptions falló: {data}")
    rows = data.get("rows", [])
    # Coerce todo a string (Sheets devuelve números como int)
    for row in rows:
        for k in list(row.keys()):
            row[k] = "" if row[k] is None else str(row[k])
    if phone_key:
        target = _normalize_phone(phone_key)
        rows = [r for r in rows if _normalize_phone(r.get("phoneKey", "")) == target]
    return rows


def unregister(endpoint):
    """Borra suscripción del backend cuando el servidor de push responde 404/410."""
    try:
        requests.post(
            WEB_APP_URL,
            data=json.dumps({"action": "unregister_push_subscription", "endpoint": endpoint}),
            headers={"Content-Type": "text/plain;charset=utf-8"},
            allow_redirects=True, timeout=30
        )
    except Exception as e:
        print(f"  [warn] no se pudo limpiar la suscripción muerta: {e}", file=sys.stderr)


def send_one(sub, payload):
    """sub es un dict de la hoja con endpoint, p256dh, auth."""
    endpoint = sub["endpoint"]
    subscription_info = {
        "endpoint": endpoint,
        "keys": {"p256dh": sub["p256dh"], "auth": sub["auth"]},
    }
    try:
        webpush(
            subscription_info=subscription_info,
            data=json.dumps(payload),
            vapid_private_key=VAPID_PRIVATE_PEM,   # path to .pem
            vapid_claims={"sub": VAPID_SUBJECT},
            ttl=86400,
        )
        return True, None
    except WebPushException as ex:
        status = ex.response.status_code if ex.response is not None else None
        # 404 / 410 = suscripción muerta — limpiar del backend
        if status in (404, 410):
            unregister(endpoint)
            return False, f"endpoint muerto ({status}) — eliminado"
        return False, f"WebPushException {status}: {ex}"
    except Exception as ex:
        return False, f"error: {ex}"


def list_pending():
    r = requests.get(WEB_APP_URL, params={"action": "list_pending_notifications"},
                     allow_redirects=True, timeout=30)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"list_pending_notifications falló: {data}")
    return data.get("rows", [])


def mark_processed(notification_id, status, error=""):
    try:
        requests.post(
            WEB_APP_URL,
            data=json.dumps({
                "action": "mark_notification_processed",
                "id": notification_id,
                "status": status,
                "error": error,
            }),
            headers={"Content-Type": "text/plain;charset=utf-8"},
            allow_redirects=True, timeout=30,
        )
    except Exception as e:
        print(f"  [warn] no se pudo marcar procesada: {e}", file=sys.stderr)


def drain_queue():
    """Procesa todas las notificaciones pendientes de la hoja Notifications_Queue."""
    pending = list_pending()
    print(f"Pendientes en cola: {len(pending)}")
    if not pending:
        return 0, 0
    sent, failed = 0, 0
    for n in pending:
        target = (n.get("target") or "").strip()
        category = (n.get("category") or "general").strip()
        payload = {
            "title": n.get("title", ""),
            "body": n.get("body", ""),
            "url": n.get("url") or "./",
        }
        if n.get("badge"):
            try: payload["badgeCount"] = int(n["badge"])
            except: pass
        if n.get("tag"):
            payload["tag"] = n["tag"]
        # Resolver destinatarios
        if target == "ALL" or target == "":
            subs = list_subscriptions(None)
        else:
            subs = list_subscriptions(target)
        # Filtrar por categoría (si la suscripción tiene categorías listadas)
        def matches_cat(sub):
            cats = (sub.get("categories") or "").strip()
            if not cats: return True  # sin filtro = recibe todo
            allowed = {c.strip().lower() for c in cats.split(",")}
            return category.lower() in allowed
        subs = [s for s in subs if matches_cat(s)]
        if not subs:
            mark_processed(n["id"], "sent", "0 destinatarios elegibles")
            print(f"  · [{n['id'][:8]}] ningún destinatario elegible (target={target}, cat={category})")
            continue
        local_sent = 0
        last_err = ""
        for sub in subs:
            ok, err = send_one(sub, payload)
            if ok: local_sent += 1
            else: last_err = err
        if local_sent > 0:
            mark_processed(n["id"], "sent", f"{local_sent}/{len(subs)} enviados")
            sent += 1
            print(f"  ✓ [{n['id'][:8]}] {local_sent}/{len(subs)} entregados (cat={category})")
        else:
            mark_processed(n["id"], "failed", last_err or "0 enviados")
            failed += 1
            print(f"  ✗ [{n['id'][:8]}] fallido: {last_err}")
        time.sleep(0.05)
    return sent, failed


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--phone", help="phoneKey objetivo, ej. +528115569120")
    g.add_argument("--all", action="store_true", help="enviar a TODAS las suscripciones")
    g.add_argument("--drain", action="store_true", help="procesar la cola Notifications_Queue (todos los pendientes)")
    p.add_argument("--title", help="Título de la notificación (no requerido con --drain)")
    p.add_argument("--body", help="Cuerpo de la notificación (no requerido con --drain)")
    p.add_argument("--url", default="./", help="URL que abre al hacer click (default: ./)")
    p.add_argument("--badge", type=int, default=None, help="Número del badge rojo en el ícono")
    p.add_argument("--tag", default=None, help="Tag para reemplazar notificaciones del mismo tipo")
    args = p.parse_args()

    if args.drain:
        if not os.path.exists(VAPID_PRIVATE_PEM):
            print(f"ERROR: no encuentro {VAPID_PRIVATE_PEM}", file=sys.stderr)
            sys.exit(1)
        sent, failed = drain_queue()
        print(f"\nResumen drain: {sent} notificaciones enviadas, {failed} fallidas.")
        return

    if not args.title or not args.body:
        print("ERROR: --title y --body son requeridos (excepto en --drain)", file=sys.stderr)
        sys.exit(2)

    if not os.path.exists(VAPID_PRIVATE_PEM):
        print(f"ERROR: no encuentro {VAPID_PRIVATE_PEM}", file=sys.stderr)
        sys.exit(1)

    targets = list_subscriptions(None if args.all else args.phone)
    print(f"Suscripciones objetivo: {len(targets)}")
    if not targets:
        print("Nada que enviar.")
        return

    payload = {"title": args.title, "body": args.body, "url": args.url}
    if args.badge is not None:
        payload["badgeCount"] = args.badge
    if args.tag:
        payload["tag"] = args.tag

    sent, failed = 0, 0
    for sub in targets:
        ok, err = send_one(sub, payload)
        label = sub.get("phoneKey", "?") + " · " + sub.get("endpoint", "")[-30:]
        if ok:
            print(f"  ✓ {label}")
            sent += 1
        else:
            print(f"  ✗ {label} → {err}")
            failed += 1
        time.sleep(0.05)
    print(f"\nResumen: {sent} enviados, {failed} fallidos.")


if __name__ == "__main__":
    main()
