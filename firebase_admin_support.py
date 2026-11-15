"""Shared, lazy Firebase Admin SDK initialization."""
import json
import os
import threading

_lock = threading.Lock()
_app = None


def _get_app():
    global _app
    if _app is not None:
        return _app

    raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()
    database_url = os.environ.get("FIREBASE_DB_URL", "").strip()
    if not raw or not database_url:
        raise RuntimeError("Firebase Admin is not configured")

    try:
        import firebase_admin
        from firebase_admin import credentials
    except ImportError as exc:
        raise RuntimeError("firebase-admin is not installed") from exc

    with _lock:
        if _app is not None:
            return _app
        try:
            _app = firebase_admin.get_app()
        except ValueError:
            try:
                service_account = json.loads(raw)
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_JSON is invalid JSON") from exc
            _app = firebase_admin.initialize_app(
                credentials.Certificate(service_account),
                {"databaseURL": database_url},
            )
    return _app


def database_ref(path):
    from firebase_admin import db

    root = os.environ.get("FIREBASE_ROOT", "kino88").strip().strip("/") or "kino88"
    suffix = str(path or "").strip("/")
    full_path = "/".join(part for part in (root, suffix) if part)
    return db.reference(full_path, app=_get_app())


def verify_id_token(token):
    from firebase_admin import auth

    return auth.verify_id_token(token, app=_get_app())


def create_custom_token(uid, claims=None):
    from firebase_admin import auth

    token = auth.create_custom_token(uid, developer_claims=claims, app=_get_app())
    return token.decode("utf-8") if isinstance(token, bytes) else token
