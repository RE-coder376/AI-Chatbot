from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import requests

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
JUDGE_MODEL = "llama-3.3-70b-versatile"
CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
ALLOWED_FAILURE_SOURCES = {
    "none",
    "retrieval_incomplete",
    "prompt_overconstraint",
    "answer_generation_drift",
    "scope_mismatch",
    "question_bad",
}

EVALS_DIR = Path(__file__).resolve().parent
JUDGE_CACHE_DIR = EVALS_DIR / "state" / "judge_cache"


@dataclass
class JudgeVerdict:
    faithfulness_score: float | None = None
    answer_relevance_score: float | None = None
    likely_failure_source: str = "none"
    reason: str = ""
    error: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _coerce_keys(api_key: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if not api_key:
        return []
    if isinstance(api_key, (list, tuple)):
        raw = api_key
    else:
        raw = str(api_key).replace("\n", ",").split(",")
    keys = []
    for key in raw:
        k = str(key or "").strip()
        if not k or k in keys:
            continue
        keys.append(k)
    return keys


def _cache_key(question: str, context: str, answer: str, reference: str) -> str:
    payload = {
        "model": JUDGE_MODEL,
        "question": question,
        "context": context,
        "answer": answer,
        "reference": reference,
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _cache_path(cache_key: str) -> Path:
    return JUDGE_CACHE_DIR / f"{cache_key}.json"


def _read_cache(cache_key: str) -> JudgeVerdict | None:
    path = _cache_path(cache_key)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    created_at = float(payload.get("created_at") or 0)
    if time.time() - created_at > CACHE_TTL_SECONDS:
        return None
    verdict = payload.get("verdict") or {}
    return _normalize_verdict(verdict)


def _write_cache(cache_key: str, verdict: JudgeVerdict) -> None:
    JUDGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at": time.time(),
        "verdict": verdict.to_dict(),
    }
    _cache_path(cache_key).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _clamp_score(value) -> float | None:
    try:
        score = float(value)
    except Exception:
        return None
    if score < 0:
        score = 0.0
    if score > 1:
        score = 1.0
    return round(score, 3)


def _normalize_failure_source(value: str) -> str:
    source = str(value or "").strip().lower() or "none"
    return source if source in ALLOWED_FAILURE_SOURCES else "none"


def _normalize_verdict(payload: dict) -> JudgeVerdict:
    return JudgeVerdict(
        faithfulness_score=_clamp_score(payload.get("faithfulness_score")),
        answer_relevance_score=_clamp_score(payload.get("answer_relevance_score")),
        likely_failure_source=_normalize_failure_source(payload.get("likely_failure_source")),
        reason=str(payload.get("reason") or "").strip(),
        error=str(payload.get("error") or "").strip(),
    )


def _request_payload(question: str, context: str, answer: str, reference: str) -> dict:
    system_prompt = (
        "You are a strict RAG evaluation judge. "
        "Return JSON only. "
        "Score whether the answer is faithful to the retrieved context and relevant to the user's question. "
        "Do not judge retrieval ranking. "
        "Use likely_failure_source only from: "
        "none, retrieval_incomplete, prompt_overconstraint, answer_generation_drift, scope_mismatch, question_bad."
    )
    user_prompt = (
        "Evaluate this chatbot answer.\n\n"
        f"Question:\n{question}\n\n"
        f"Retrieved Context:\n{context}\n\n"
        f"Reference Answer:\n{reference}\n\n"
        f"Chatbot Answer:\n{answer}\n\n"
        "Return a JSON object with exactly these keys:\n"
        "- faithfulness_score: number from 0 to 1\n"
        "- answer_relevance_score: number from 0 to 1\n"
        "- likely_failure_source: one of none, retrieval_incomplete, prompt_overconstraint, answer_generation_drift, scope_mismatch, question_bad\n"
        "- reason: short explanation\n"
        "- error: empty string unless you cannot judge\n"
    )
    return {
        "model": JUDGE_MODEL,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }


def _should_rotate(status_code: int, payload: dict | None) -> bool:
    if status_code in {429, 500, 502, 503, 504, 529}:
        return True
    message = str((payload or {}).get("error", {}).get("message") or "").lower()
    err_type = str((payload or {}).get("error", {}).get("type") or "").lower()
    return "rate limit" in message or "temporarily unavailable" in message or "overloaded" in err_type


def judge_answer(question: str, context: str, answer: str, reference: str, api_key) -> JudgeVerdict:
    keys = _coerce_keys(api_key)
    if not keys:
        return JudgeVerdict(error="judge_disabled")

    cache_key = _cache_key(question, context, answer, reference)
    cached = _read_cache(cache_key)
    if cached is not None:
        return cached

    payload = _request_payload(question, context, answer, reference)
    last_error = "judge_unavailable"
    for key in keys:
        try:
            response = requests.post(
                f"{GROQ_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=90,
            )
        except requests.RequestException as exc:
            last_error = f"judge_request_failed: {type(exc).__name__}"
            continue

        body = None
        try:
            body = response.json()
        except Exception:
            body = None

        if response.status_code != 200:
            if _should_rotate(response.status_code, body):
                last_error = f"judge_retryable_http_{response.status_code}"
                continue
            last_error = f"judge_http_{response.status_code}"
            continue

        try:
            content = ((body or {}).get("choices") or [{}])[0].get("message", {}).get("content", "")
            verdict = _normalize_verdict(json.loads(content or "{}"))
        except Exception:
            last_error = "judge_parse_error"
            continue

        _write_cache(cache_key, verdict)
        return verdict

    return JudgeVerdict(error=last_error)
