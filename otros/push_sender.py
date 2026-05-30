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


def list_subscriptions(phone_key=None):
    params = {"action": "list_push_subscriptions"}
    if phone_key:
        params["phoneKey"] = phone_key
    r = requests.get(WEB_APP_URL, params=params, allow_redirects=True, timeout=30)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"list_push_subscriptions falló: {data}")
    return data.get("rows", [])


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
    with open(VAPID_PRIVATE_PEM, "r") as f:
        vapid_pem = f.read()
    try:
        webpush(
            subscription_info=subscription_info,
            data=json.dumps(payload),
            vapid_private_key=vapid_pem,
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


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--phone", help="phoneKey objetivo, ej. +528115569120")
    g.add_argument("--all", action="store_true", help="enviar a TODAS las suscripciones")
    p.add_argument("--title", required=True, help="Título de la notificación")
    p.add_argument("--body", required=True, help="Cuerpo de la notificación")
    p.add_argument("--url", default="./", help="URL que abre al hacer click (default: ./)")
    p.add_argument("--badge", type=int, default=None, help="Número del badge rojo en el ícono")
    p.add_argument("--tag", default=None, help="Tag para reemplazar notificaciones del mismo tipo")
    args = p.parse_args()

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
