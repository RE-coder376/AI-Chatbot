"""
services/llm_keys.py — LLM key rotation, health tracking, and provider selection.
Self-contained state. No dependency on app.py globals.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import random
import threading
from collections import deque
from pathlib import Path
from typing import Dict

from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI as _BaseChatOpenAI

from services.config import KEYS_FILE, _atomic_write_json

logger = logging.getLogger(__name__)

def _tag_llm(llm, provider: str, model: str):
    try:
        setattr(llm, "_provider_name", provider)
        setattr(llm, "_model_name", model)
    except Exception:
        pass
    return llm


class _CompatChatOpenAI(_BaseChatOpenAI):
    """ChatOpenAI subclass that reverts max_completion_tokens → max_tokens
    for providers that don't support the OpenAI o1-style parameter."""
    def _get_request_payload(self, input_, *, stop=None, **kwargs):
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        if "max_completion_tokens" in payload:
            payload["max_tokens"] = payload.pop("max_completion_tokens")
        return payload


_llm_key_lock  = threading.Lock()   # prevents key-selection race under concurrent requests

KEY_HEALTH_FILE = Path("key_health.json")
_key_status: Dict[str, dict] = {}
_key_org_map: Dict[str, str] = {}   # api_key -> org_id (populated from 429 errors)
_org_cooldown: Dict[str, float] = {} # org_id -> cooldown_until timestamp
_provider_cooldown: Dict[str, float] = {}  # provider -> cooldown_until timestamp
_key_rpm_window: Dict[str, deque] = {}  # api_key -> deque of request timestamps (last 60s)
# Soft RPM limits — rotate before hitting the real limit (leaves ~5 req buffer)
_KEY_RPM_SOFT_LIMIT: Dict[str, int] = {
    "groq": 25,        # real limit ~30/min
    "gemini": 12,      # real limit ~15/min
    "cerebras": 25,
    "sambanova": 25,
    "mistral": 25,
    "openai": 25,
}

def _cooldown_provider(provider: str, seconds: int = 90):
    try:
        p = str(provider or "").strip().lower()
        if not p:
            return
        _provider_cooldown[p] = time.time() + max(30, int(seconds))
    except Exception:
        pass


def _load_key_health():
    """Load persisted key cooldowns from disk on startup."""
    global _key_status, _org_cooldown, _key_org_map
    if KEY_HEALTH_FILE.exists():
        try:
            data = json.loads(KEY_HEALTH_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "key_status" in data:
                _key_status   = data.get("key_status", {})
                _org_cooldown = data.get("org_cooldown", {})
                _key_org_map  = data.get("key_org_map", {})
            else:
                _key_status = data  # legacy format
        except Exception as e:
            logger.warning(f"_load_key_health: could not parse {KEY_HEALTH_FILE}: {e}")


def _save_key_health():
    """Persist key cooldowns to disk so they survive restarts."""
    try:
        _atomic_write_json(KEY_HEALTH_FILE, {
            "key_status":   _key_status,
            "org_cooldown": _org_cooldown,
            "key_org_map":  _key_org_map,
        })
    except Exception as e:
        logger.warning(f"_save_key_health: could not write {KEY_HEALTH_FILE}: {e}")


def _mark_key_failed(api_key: str, error_str: str):
    """Mark a key as rate-limited. TPD errors get a until-midnight cooldown.
    Also extracts org_id from the error and cools ALL keys from that org."""
    import re
    from datetime import datetime, timezone, timedelta
    now = time.time()
    err_lower = error_str.lower()
    is_tpd = ("per day" in err_lower or "tokens per day" in err_lower or "tpd" in err_lower
              or "daily" in err_lower or "perday" in err_lower or "limit: 0" in err_lower
              or "per_day" in err_lower)

    if is_tpd:
        utc_now = datetime.now(timezone.utc)
        midnight = (utc_now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        cooldown_until = midnight.timestamp() + 120  # +2min buffer
        logger.warning(f"Key ...{api_key[-4:]} TPD exhausted — cooldown until midnight UTC")

        # Daily quota is shared per ACCOUNT (org), so one TPD signal means every sibling
        # key on that account is also dead until midnight. Cool the whole group at once,
        # keyed off the synthetic `org` field in keys.json — provider-agnostic, so it
        # works for Gemini/Cerebras/Sambanova too (they never emit an org id in errors).
        org_group = None
        try:
            if KEYS_FILE.exists():
                all_keys = json.loads(KEYS_FILE.read_text(encoding="utf-8"))
                for entry in all_keys:
                    if entry.get("key") == api_key:
                        org_group = entry.get("org")
                        break
                if org_group:
                    _org_cooldown[org_group] = cooldown_until
                    cascaded = 0
                    for entry in all_keys:
                        k = entry.get("key", "")
                        if k and entry.get("org") == org_group:
                            _key_org_map[k] = org_group
                            cascaded += 1
                    logger.warning(f"Account {org_group} TPD exhausted — cooled {cascaded} sibling keys until midnight UTC")
        except Exception as _ce:
            logger.warning(f"TPD account cascade failed: {_ce}")

        # Also honor the real org id when Groq returns one (extra safety; groups by the
        # provider-issued id in addition to our synthetic account group).
        org_match = re.search(r'organization\s+[`\']?(org_\w+)[`\']?', error_str)
        if org_match:
            org_id = org_match.group(1)
            _key_org_map[api_key] = org_id
            _org_cooldown[org_id] = cooldown_until

        if not org_group:
            logger.warning(f"Key ...{api_key[-4:]} daily quota exhausted (no account group) — per-key cooldown only")
        _save_key_health()
    elif "invalid_api_key" in err_lower or "invalid api key" in err_lower or "authentication" in err_lower:
        # Key is permanently invalid — disable it in keys.json
        cooldown_until = now + 86400 * 365  # 1 year = effectively permanent
        try:
            if KEYS_FILE.exists():
                all_keys = json.loads(KEYS_FILE.read_text(encoding="utf-8"))
                for entry in all_keys:
                    if entry.get('key') == api_key:
                        entry['status'] = 'inactive'
                KEYS_FILE.write_text(json.dumps(all_keys, indent=2), encoding="utf-8")
                logger.warning(f"Key ...{api_key[-4:]} is invalid — marked inactive in keys.json")
        except Exception as ex:
            logger.warning(f"Could not disable invalid key: {ex}")
    else:
        cooldown_until = now + 65  # TPM: 65s cooldown

    s = _key_status.get(api_key, {})
    s["cooldown_until"] = cooldown_until
    s["tokens"] = 0
    s["last_used"] = now
    _key_status[api_key] = s
    _save_key_health()


def any_key_ready() -> bool:
    """Fast check: returns True if at least one key is off cooldown."""
    now = time.time()
    try:
        keys = json.loads(KEYS_FILE.read_text(encoding="utf-8")) if KEYS_FILE.exists() else []
        actives = [k for k in keys if k.get('status') == 'active']
        for ev, prov in [("GROQ_API_KEY","groq"),("GEMINI_API_KEY","gemini"),("CEREBRAS_API_KEY","cerebras"),("SAMBANOVA_API_KEY","sambanova"),("OPENAI_API_KEY","openai")]:
            v = os.getenv(ev)
            if v and not any(k.get('key') == v for k in actives):
                actives.append({"key": v, "provider": prov, "label": f"Env {prov}"})
        for k in actives:
            prov = str(k.get('provider', '') or '').lower()
            if prov and now < _provider_cooldown.get(prov, 0):
                continue
            s = _key_status.get(k['key'], {})
            if now >= s.get("cooldown_until", 0):
                org_id = _key_org_map.get(k['key'])
                if not org_id or now >= _org_cooldown.get(org_id, 0):
                    return True
        return False
    except:
        return True  # assume ready if check fails


# Per-model context window budgets (chars, conservative — leaves room for system prompt + history)
# Cerebras llama3.1-8b: 8K tokens ≈ 6000 chars usable for context
_CEREBRAS_MAX_CONTEXT_CHARS = 6000
# Groq free tier returns HTTP 413 when request payload is too large (~50k chars context)
_GROQ_MAX_CONTEXT_CHARS = 20000


def _peek_provider() -> str:
    """Return the provider string of the healthiest available key (without creating LLM object)."""
    try:
        actives = []
        if KEYS_FILE.exists():
            keys = json.loads(KEYS_FILE.read_text(encoding="utf-8"))
            actives = [k for k in keys if k.get('status') == 'active']
        for ev, prov in [("GROQ_API_KEY","groq"),("GEMINI_API_KEY","gemini"),("CEREBRAS_API_KEY","cerebras"),("SAMBANOVA_API_KEY","sambanova"),("OPENAI_API_KEY","openai")]:
            v = os.getenv(ev)
            if v and not any(k.get('key') == v for k in actives):
                actives.append({"key": v, "provider": prov, "label": f"Env {prov}"})
        if not actives:
            return 'groq'
        now = time.time()
        def _score(k):
            prov = str(k.get('provider', '') or '').lower()
            if prov and now < _provider_cooldown.get(prov, 0):
                return (-1, 0, 0)
            s = _key_status.get(k['key'], {})
            if now < s.get("cooldown_until", 0): return (-1, 0, 0)
            org_id = _key_org_map.get(k['key'])
            if org_id and now < _org_cooldown.get(org_id, 0): return (-1, 0, 0)
            return (s.get("tokens", 6000), -s.get("last_used", 0), 0)
        chosen = sorted(actives, key=_score, reverse=True)[0]
        return chosen.get('provider', 'groq')
    except:
        return 'groq'


def get_fresh_llm(avoid_providers=None):
    """Multi-provider key rotation: Groq (llama-3.3-70b) + OpenAI (gpt-4o-mini) fallback.

    avoid_providers: providers already failed in THIS request — ranked last so a
    single exhausted provider/org (e.g. one Groq account out of daily quota) is
    skipped on the very next retry instead of burning the budget on its sibling
    keys. Avoided-but-healthy still outrank cooled-down keys (usable last resort)."""
    avoid = {str(p).lower() for p in (avoid_providers or [])}
    try:
        actives = []
        if KEYS_FILE.exists():
            keys = json.loads(KEYS_FILE.read_text(encoding="utf-8"))
            actives = [k for k in keys if k.get('status') == 'active']

        for ev, prov in [("GROQ_API_KEY","groq"),("GEMINI_API_KEY","gemini"),("CEREBRAS_API_KEY","cerebras"),("SAMBANOVA_API_KEY","sambanova"),("OPENAI_API_KEY","openai")]:
            v = os.getenv(ev)
            if v and not any(k.get('key') == v for k in actives):
                actives.append({"key": v, "provider": prov, "label": f"Env {prov}"})

        if not actives: return None

        # Pre-load org membership from keys.json 'org' field (no need to wait for a failure)
        for k in actives:
            if k.get("org") and not _key_org_map.get(k["key"]):
                _key_org_map[k["key"]] = k["org"]

        now = time.time()

        _PROV_TIER = {'groq': 4, 'gemini': 4, 'sambanova': 3, 'mistral': 3, 'openai': 3, 'cerebras': 1}

        def key_health_score(k):
            s = _key_status.get(k['key'], {})
            prov = str(k.get('provider', '') or '').lower()
            _skip = (-1, -1, 0, 0, random.random())
            if prov and now < _provider_cooldown.get(prov, 0):
                return _skip
            if now < s.get("cooldown_until", 0):
                return _skip
            org_id = _key_org_map.get(k['key'])
            if org_id and now < _org_cooldown.get(org_id, 0):
                return _skip
            # Proactive RPM check — skip key if it's near its per-minute limit
            prov = k.get('provider', 'groq')
            soft_limit = _KEY_RPM_SOFT_LIMIT.get(prov, 25)
            rpm_dq = _key_rpm_window.get(k['key'], deque())
            recent_reqs = sum(1 for t in rpm_dq if now - t < 60)
            if recent_reqs >= soft_limit:
                return _skip
            tier = _PROV_TIER.get(prov, 2)
            tokens = s.get("tokens", 6000)
            # avoid_rank: providers already failed this request rank below fresh ones
            # but still above cooled (-1) keys.
            avoid_rank = 0 if prov in avoid else 1
            return (avoid_rank, tier, tokens, -s.get("last_used", 0), random.random())

        with _llm_key_lock:
            healthiest = sorted(actives, key=key_health_score, reverse=True)
            chosen = healthiest[0]
            key_val = chosen['key']
            provider = chosen.get('provider', 'groq')
            # Update last_used inside lock so concurrent requests get different keys
            status = _key_status.get(key_val, {"tokens": 6000, "requests": 14400, "last_used": 0})
            status["last_used"] = now
            _key_status[key_val] = status
            # Track RPM — append timestamp, trim entries older than 60s
            rpm_dq = _key_rpm_window.setdefault(key_val, deque())
            rpm_dq.append(now)
            while rpm_dq and now - rpm_dq[0] > 60:
                rpm_dq.popleft()

        if provider == 'openai':
            from langchain_openai import ChatOpenAI
            return _tag_llm(ChatOpenAI(
                api_key=key_val,
                model="gpt-4o-mini",
                temperature=0,
                max_retries=0,
                max_tokens=512,
                request_timeout=8,
            ), "openai", "gpt-4o-mini")
        elif provider == 'gemini':
            from langchain_google_genai import ChatGoogleGenerativeAI
            # max_retries=0 + transport="rest" disables google-genai SDK's internal
            # exponential-backoff retry loop (which was wasting 30-40s per exhausted key
            # before our own rotation code could run)
            return _tag_llm(ChatGoogleGenerativeAI(
                google_api_key=key_val,
                model="gemini-2.0-flash-lite",
                temperature=0,
                max_retries=0,
                max_output_tokens=512,
                timeout=11,
            ), "gemini", "gemini-2.0-flash-lite")
        elif provider == 'cerebras':
            return _tag_llm(_CompatChatOpenAI(
                api_key=key_val,
                model="llama3.1-8b",
                base_url="https://api.cerebras.ai/v1",
                temperature=0,
                max_retries=0,
                max_tokens=512,
                request_timeout=8,
            ), "cerebras", "llama3.1-8b")
        elif provider == 'sambanova':
            return _tag_llm(_CompatChatOpenAI(
                api_key=key_val,
                model="Meta-Llama-3.3-70B-Instruct",
                base_url="https://api.sambanova.ai/v1",
                temperature=0,
                max_retries=0,
                max_tokens=512,
                request_timeout=8,
            ), "sambanova", "Meta-Llama-3.3-70B-Instruct")
        elif provider == 'mistral':
            return _tag_llm(_CompatChatOpenAI(
                api_key=key_val,
                model="mistral-small-latest",
                base_url="https://api.mistral.ai/v1",
                temperature=0,
                max_retries=0,
                max_tokens=512,
                request_timeout=8,
            ), "mistral", "mistral-small-latest")
        else:
            return _tag_llm(ChatGroq(
                api_key=key_val,
                model="llama-3.3-70b-versatile",
                temperature=0,
                max_retries=0,
                max_tokens=512,
                request_timeout=7,
            ), "groq", "llama-3.3-70b-versatile")
    except Exception as e:
        logger.error(f"LLM Key Selection Error: {e}")
        return None

