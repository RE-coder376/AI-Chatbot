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
    confidence: float | None = None
    reason: str = ""
    exact_failure_step: str = ""
    root_cause_note: str = ""
    fix_hint: str = ""
    self_check_status: str = "single_pass"
    self_check_note: str = ""
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


def _cache_key(
    question: str,
    context: str,
    answer: str,
    reference: str,
    prompt_context: str = "",
    retrieval_metrics: dict | None = None,
    retrieval_diagnosis: str = "",
    workflow_trace: str = "",
    guard_decisions: dict | None = None,
    answer_artifacts: dict | None = None,
) -> str:
    payload = {
        "model": JUDGE_MODEL,
        "question": question,
        "context": context,
        "answer": answer,
        "reference": reference,
        "prompt_context": prompt_context,
        "retrieval_metrics": retrieval_metrics or {},
        "retrieval_diagnosis": retrieval_diagnosis,
        "workflow_trace": workflow_trace,
        "guard_decisions": guard_decisions or {},
        "answer_artifacts": answer_artifacts or {},
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
        confidence=_clamp_score(payload.get("confidence")),
        reason=str(payload.get("reason") or "").strip(),
        exact_failure_step=str(payload.get("exact_failure_step") or "").strip(),
        root_cause_note=str(payload.get("root_cause_note") or "").strip(),
        fix_hint=str(payload.get("fix_hint") or "").strip(),
        self_check_status=str(payload.get("self_check_status") or "single_pass").strip() or "single_pass",
        self_check_note=str(payload.get("self_check_note") or "").strip(),
        error=str(payload.get("error") or "").strip(),
    )


def _request_payload(
    question: str,
    context: str,
    answer: str,
    reference: str,
    prompt_context: str = "",
    retrieval_metrics: dict | None = None,
    retrieval_diagnosis: str = "",
    workflow_trace: str = "",
    guard_decisions: dict | None = None,
    answer_artifacts: dict | None = None,
) -> dict:
    system_prompt = (
        "You are a strict RAG evaluation judge. "
        "Return JSON only. "
        "Score whether the answer is faithful to the retrieved context and relevant to the user's question. "
        "You are also a workflow debugger: use the provided trace to identify the exact branch or guard that most likely caused the final answer shape. "
        "Do not judge retrieval ranking beyond the supplied deterministic signals. "
        "Use likely_failure_source only from: "
        "none, retrieval_incomplete, prompt_overconstraint, answer_generation_drift, scope_mismatch, question_bad."
    )
    user_prompt = (
        "Evaluate this chatbot answer.\n\n"
        f"Question:\n{question}\n\n"
        f"Retrieved Context:\n{context}\n\n"
        f"Reference Answer:\n{reference}\n\n"
        f"System Prompt Snapshot:\n{prompt_context or '(not provided)'}\n\n"
        f"Deterministic Retrieval Signals:\n{json.dumps(retrieval_metrics or {}, ensure_ascii=True)}\n\n"
        f"Deterministic Retrieval Diagnosis:\n{retrieval_diagnosis or 'none'}\n\n"
        f"Answer Generation Workflow Trace:\n{workflow_trace or '(not provided)'}\n\n"
        f"Guard Decisions:\n{json.dumps(guard_decisions or {}, ensure_ascii=True)}\n\n"
        f"Answer Artifacts:\n{json.dumps(answer_artifacts or {}, ensure_ascii=True)}\n\n"
        f"Chatbot Answer:\n{answer}\n\n"
        "If the workflow trace, guard decisions, or answer artifacts clearly show a guard, fallback, source suppression, rewrite, post-processing branch, or answer-class toggle, use that evidence directly instead of hedging. "
        "Only blame retrieval when the trace and deterministic retrieval signals support that conclusion. "
        "Return a JSON object with exactly these keys:\n"
        "- faithfulness_score: number from 0 to 1\n"
        "- answer_relevance_score: number from 0 to 1\n"
        "- likely_failure_source: one of none, retrieval_incomplete, prompt_overconstraint, answer_generation_drift, scope_mismatch, question_bad\n"
        "- confidence: number from 0 to 1 for how certain you are about the root cause\n"
        "- reason: short explanation\n"
        "- exact_failure_step: short machine-friendly step name like guard:sparse_kb_context_miss, answer_class_transition, validator_rewrite, retrieval, generation_output\n"
        "- root_cause_note: explain the most likely prompt, retrieval, or generation issue\n"
        "- fix_hint: short concrete fix direction\n"
        "- self_check_status: set to single_pass\n"
        "- self_check_note: empty string\n"
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


def _recheck_payload(
    question: str,
    context: str,
    answer: str,
    reference: str,
    first_pass: JudgeVerdict,
    prompt_context: str = "",
    retrieval_metrics: dict | None = None,
    retrieval_diagnosis: str = "",
    workflow_trace: str = "",
    guard_decisions: dict | None = None,
    answer_artifacts: dict | None = None,
) -> dict:
    system_prompt = (
        "You are verifying a prior RAG evaluation verdict. "
        "Return JSON only. "
        "Confirm the verdict only if the evidence clearly supports it; otherwise correct it. "
        "Use likely_failure_source only from: "
        "none, retrieval_incomplete, prompt_overconstraint, answer_generation_drift, scope_mismatch, question_bad."
    )
    user_prompt = (
        "Re-check this prior verdict against the evidence.\n\n"
        f"Question:\n{question}\n\n"
        f"Retrieved Context:\n{context}\n\n"
        f"Reference Answer:\n{reference}\n\n"
        f"System Prompt Snapshot:\n{prompt_context or '(not provided)'}\n\n"
        f"Deterministic Retrieval Signals:\n{json.dumps(retrieval_metrics or {}, ensure_ascii=True)}\n\n"
        f"Deterministic Retrieval Diagnosis:\n{retrieval_diagnosis or 'none'}\n\n"
        f"Answer Generation Workflow Trace:\n{workflow_trace or '(not provided)'}\n\n"
        f"Guard Decisions:\n{json.dumps(guard_decisions or {}, ensure_ascii=True)}\n\n"
        f"Answer Artifacts:\n{json.dumps(answer_artifacts or {}, ensure_ascii=True)}\n\n"
        f"Chatbot Answer:\n{answer}\n\n"
        f"First-Pass Verdict:\n{json.dumps(first_pass.to_dict(), ensure_ascii=True)}\n\n"
        "If the first-pass verdict is solid, confirm it. "
        "If the evidence points somewhere else, correct it and explain what evidence forced the correction. "
        "Return a JSON object with exactly these keys:\n"
        "- faithfulness_score\n"
        "- answer_relevance_score\n"
        "- likely_failure_source\n"
        "- confidence\n"
        "- reason\n"
        "- exact_failure_step\n"
        "- root_cause_note\n"
        "- fix_hint\n"
        "- self_check_status: confirmed or revised\n"
        "- self_check_note: short confirmation/correction note\n"
        "- error\n"
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


def _post_json(api_key: str, payload: dict) -> tuple[int, dict | None, str]:
    try:
        response = requests.post(
            f"{GROQ_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=90,
        )
    except requests.RequestException as exc:
        return 0, None, f"judge_request_failed: {type(exc).__name__}"

    body = None
    try:
        body = response.json()
    except Exception:
        body = None
    if response.status_code != 200:
        return response.status_code, body, ""
    return response.status_code, body, ""


def _strongest_failure_step(guard_decisions: dict | None, answer_artifacts: dict | None, retrieval_diagnosis: str = "") -> tuple[str, str]:
    decisions = guard_decisions or {}
    artifacts = answer_artifacts or {}
    exit_guard = str(decisions.get("exit_guard") or "").strip()
    if exit_guard:
        if exit_guard == "sparse_kb_context_miss":
            return "guard:sparse_kb_context_miss", "retrieval_incomplete"
        if exit_guard in {"general_oos", "coding_scope", "translation", "persona_lock", "off_topic_pre_retrieval"}:
            return f"guard:{exit_guard}", "scope_mismatch"
        return f"guard:{exit_guard}", "prompt_overconstraint"
    if bool(decisions.get("validator_rewrite_triggered")):
        return "validator_rewrite", "answer_generation_drift"
    if bool(decisions.get("answer_class_changed")) and str(decisions.get("raw_answer_class") or "") == "ANSWER":
        return "answer_class_transition", "answer_generation_drift"
    suppressed = str(decisions.get("source_suppressed_reason") or "").strip()
    if suppressed == "idk" and str(decisions.get("cleaned_answer_class") or "") == "IDK":
        return "source_suppression:idk", "prompt_overconstraint"
    if retrieval_diagnosis in {"retrieval_incomplete", "retrieval_miss", "weak_top_k"}:
        return "retrieval", "retrieval_incomplete"
    raw_answer = str(artifacts.get("raw_llm_answer") or "").strip()
    cleaned_answer = str(artifacts.get("cleaned_answer") or "").strip()
    if raw_answer and cleaned_answer:
        return "generation_output", "answer_generation_drift"
    return "", "none"


def _recalibrate_verdict(
    verdict: JudgeVerdict,
    retrieval_diagnosis: str = "",
    guard_decisions: dict | None = None,
    answer_artifacts: dict | None = None,
) -> JudgeVerdict:
    evidence_step, evidence_source = _strongest_failure_step(guard_decisions, answer_artifacts, retrieval_diagnosis=retrieval_diagnosis)
    if evidence_step and not verdict.exact_failure_step:
        verdict.exact_failure_step = evidence_step
    if evidence_source != "none" and verdict.likely_failure_source == "none":
        verdict.likely_failure_source = evidence_source
    if evidence_step == "answer_class_transition" and verdict.likely_failure_source != "answer_generation_drift":
        verdict.likely_failure_source = "answer_generation_drift"
        verdict.reason = verdict.reason or "The answer changed class after generation."
        verdict.root_cause_note = verdict.root_cause_note or "The raw model answer looked usable, but a later cleanup/reclassification step flipped it away from an ANSWER."
        verdict.fix_hint = verdict.fix_hint or "Inspect the cleanup and answer-class rules that run after the LLM output is produced."
    if evidence_step.startswith("guard:") and verdict.likely_failure_source in {"none", "answer_generation_drift"}:
        verdict.likely_failure_source = evidence_source
    if evidence_step:
        base_conf = verdict.confidence if verdict.confidence is not None else 0.72
        verdict.confidence = round(max(base_conf, 0.88), 3)
    elif verdict.confidence is None:
        verdict.confidence = 0.65
    return verdict


def judge_answer(
    question: str,
    context: str,
    answer: str,
    reference: str,
    api_key,
    prompt_context: str = "",
    retrieval_metrics: dict | None = None,
    retrieval_diagnosis: str = "",
    workflow_trace: str = "",
    guard_decisions: dict | None = None,
    answer_artifacts: dict | None = None,
) -> JudgeVerdict:
    keys = _coerce_keys(api_key)
    if not keys:
        return JudgeVerdict(error="judge_disabled")

    cache_key = _cache_key(
        question,
        context,
        answer,
        reference,
        prompt_context=prompt_context,
        retrieval_metrics=retrieval_metrics,
        retrieval_diagnosis=retrieval_diagnosis,
        workflow_trace=workflow_trace,
        guard_decisions=guard_decisions,
        answer_artifacts=answer_artifacts,
    )
    cached = _read_cache(cache_key)
    if cached is not None:
        return cached

    payload = _request_payload(
        question,
        context,
        answer,
        reference,
        prompt_context=prompt_context,
        retrieval_metrics=retrieval_metrics,
        retrieval_diagnosis=retrieval_diagnosis,
        workflow_trace=workflow_trace,
        guard_decisions=guard_decisions,
        answer_artifacts=answer_artifacts,
    )
    last_error = "judge_unavailable"
    for key in keys:
        status_code, body, request_error = _post_json(key, payload)
        if request_error:
            last_error = request_error
            continue
        if status_code != 200:
            if _should_rotate(status_code, body):
                last_error = f"judge_retryable_http_{status_code}"
                continue
            last_error = f"judge_http_{status_code}"
            continue

        try:
            content = ((body or {}).get("choices") or [{}])[0].get("message", {}).get("content", "")
            verdict = _normalize_verdict(json.loads(content or "{}"))
        except Exception:
            last_error = "judge_parse_error"
            continue

        verdict = _recalibrate_verdict(
            verdict,
            retrieval_diagnosis=retrieval_diagnosis,
            guard_decisions=guard_decisions,
            answer_artifacts=answer_artifacts,
        )
        recheck_payload = _recheck_payload(
            question,
            context,
            answer,
            reference,
            verdict,
            prompt_context=prompt_context,
            retrieval_metrics=retrieval_metrics,
            retrieval_diagnosis=retrieval_diagnosis,
            workflow_trace=workflow_trace,
            guard_decisions=guard_decisions,
            answer_artifacts=answer_artifacts,
        )
        re_status, re_body, re_error = _post_json(key, recheck_payload)
        if not re_error and re_status == 200:
            try:
                re_content = ((re_body or {}).get("choices") or [{}])[0].get("message", {}).get("content", "")
                re_verdict = _normalize_verdict(json.loads(re_content or "{}"))
                re_verdict = _recalibrate_verdict(
                    re_verdict,
                    retrieval_diagnosis=retrieval_diagnosis,
                    guard_decisions=guard_decisions,
                    answer_artifacts=answer_artifacts,
                )
                verdict = re_verdict
            except Exception:
                verdict.self_check_status = "error"
                verdict.self_check_note = "Judge re-check response could not be parsed."
        else:
            verdict.self_check_status = "error"
            verdict.self_check_note = f"Judge re-check unavailable ({re_error or f'http_{re_status}'})"
        _write_cache(cache_key, verdict)
        return verdict

    return JudgeVerdict(error=last_error)
