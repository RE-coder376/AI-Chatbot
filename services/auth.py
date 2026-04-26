"""
services/auth.py — Password hashing, validation utilities, request extraction.

Standalone — no app-level module variables, safe to import anywhere.
"""
import re
import hmac
import hashlib
import secrets

from fastapi import HTTPException, Request

PASSWORD_HASH_PREFIX = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 260_000


def _validate_db_name(name: str) -> str:
    """Reject path traversal and invalid characters in DB names."""
    name = (name or "").strip()
    if not name or not re.match(r'^[a-zA-Z0-9_\-]+$', name) or ".." in name:
        raise HTTPException(status_code=400, detail="Invalid database name")
    return name


def _is_password_hash(value: str) -> bool:
    return isinstance(value, str) and value.startswith(f"{PASSWORD_HASH_PREFIX}$")


def _hash_password(password: str) -> str:
    """Hash a plain-text password using PBKDF2-SHA256."""
    password = str(password or "")
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PASSWORD_HASH_ITERATIONS,
    ).hex()
    return f"{PASSWORD_HASH_PREFIX}${PASSWORD_HASH_ITERATIONS}${salt}${digest}"


def _password_matches(candidate: str, stored: str) -> bool:
    """Compare plaintext or pbkdf2 password values in constant time."""
    candidate = str(candidate or "")
    stored = str(stored or "")
    if not candidate or not stored:
        return False
    if _is_password_hash(stored):
        try:
            _, rounds_s, salt, expected = stored.split("$", 3)
            digest = hashlib.pbkdf2_hmac(
                "sha256",
                candidate.encode("utf-8"),
                salt.encode("utf-8"),
                int(rounds_s),
            ).hex()
            return hmac.compare_digest(digest, expected)
        except Exception:
            return False
    return hmac.compare_digest(candidate.encode(), stored.encode())


def _extract_password(request: Request, fallback: str = "") -> str:
    """Extract password from Authorization: Bearer <pass> header, or fallback."""
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return fallback
