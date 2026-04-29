from pathlib import Path
import tempfile

from evals import llm_judge


def _scratch_dir() -> Path:
    path = Path("tests") / "_tmp" / next(tempfile._get_candidate_names())
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_judge_answer_skips_gracefully_without_key():
    verdict = llm_judge.judge_answer("q", "ctx", "ans", "ref", "")

    assert verdict.error == "judge_disabled"
    assert verdict.likely_failure_source == "none"


def test_judge_answer_uses_cache(monkeypatch):
    scratch = _scratch_dir()
    monkeypatch.setattr(llm_judge, "JUDGE_CACHE_DIR", scratch / "judge_cache")
    verdict = llm_judge.JudgeVerdict(
        faithfulness_score=0.81,
        answer_relevance_score=0.73,
        likely_failure_source="answer_generation_drift",
        confidence=0.9,
        exact_failure_step="answer_class_transition",
        reason="Cached verdict",
    )
    cache_key = llm_judge._cache_key("q", "ctx", "ans", "ref")
    llm_judge._write_cache(cache_key, verdict)

    class ShouldNotRun:
        def __call__(self, *args, **kwargs):
            raise AssertionError("network should not run on cache hit")

    monkeypatch.setattr(llm_judge.requests, "post", ShouldNotRun())

    cached = llm_judge.judge_answer("q", "ctx", "ans", "ref", "test-key")

    assert cached.faithfulness_score == 0.81
    assert cached.answer_relevance_score == 0.73
    assert cached.likely_failure_source == "answer_generation_drift"
    assert cached.exact_failure_step == "answer_class_transition"


def test_judge_answer_rotates_keys_on_rate_limit(monkeypatch):
    scratch = _scratch_dir()
    monkeypatch.setattr(llm_judge, "JUDGE_CACHE_DIR", scratch / "judge_cache")

    calls = []

    class FakeResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append(headers.get("Authorization", ""))
        if len(calls) == 1:
            return FakeResponse(429, {"error": {"type": "rate_limit_error", "message": "slow down"}})
        return FakeResponse(
            200,
            {
                "choices": [
                    {
                        "message": {
                            "content": llm_judge.json.dumps(
                                {
                                    "faithfulness_score": 0.9,
                                    "answer_relevance_score": 0.8,
                                    "likely_failure_source": "none",
                                    "confidence": 0.61,
                                    "reason": "Grounded answer.",
                                    "exact_failure_step": "",
                                    "root_cause_note": "",
                                    "fix_hint": "",
                                    "self_check_status": "confirmed",
                                    "self_check_note": "consistent",
                                    "error": "",
                                }
                            )
                        }
                    }
                ]
            },
        )

    monkeypatch.setattr(llm_judge.requests, "post", fake_post)

    verdict = llm_judge.judge_answer("q", "ctx", "ans", "ref", "key-one,key-two")

    assert len(calls) == 3
    assert verdict.faithfulness_score == 0.9
    assert verdict.answer_relevance_score == 0.8
    assert verdict.error == ""
    assert verdict.self_check_status == "confirmed"


def test_judge_answer_payload_includes_workflow_trace(monkeypatch):
    captured = {}
    scratch = _scratch_dir()
    monkeypatch.setattr(llm_judge, "JUDGE_CACHE_DIR", scratch / "judge_cache")

    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": llm_judge.json.dumps(
                                {
                                    "faithfulness_score": 1,
                                    "answer_relevance_score": 1,
                                    "likely_failure_source": "prompt_overconstraint",
                                    "confidence": 0.95,
                                    "reason": "trace points to a guard",
                                    "exact_failure_step": "guard:scope_declined",
                                    "root_cause_note": "scope guard fired after retrieval",
                                    "fix_hint": "relax scope guard when context is strong",
                                    "self_check_status": "confirmed",
                                    "self_check_note": "trace agrees",
                                    "error": "",
                                }
                            )
                        }
                    }
                ]
            }

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["payload"] = json
        return FakeResponse()

    monkeypatch.setattr(llm_judge.requests, "post", fake_post)

    verdict = llm_judge.judge_answer(
        "q",
        "ctx",
        "ans",
        "ref",
        "key-one",
        prompt_context="prompt snapshot",
        retrieval_metrics={"average_precision": 1.0},
        retrieval_diagnosis="weak_top_k",
        workflow_trace="guard_exit: {\"guard\": \"scope_declined\"}",
        guard_decisions={"exit_guard": "scope_declined"},
        answer_artifacts={"system_prompt_final": "full prompt", "raw_llm_answer": "no"},
    )

    user_content = captured["payload"]["messages"][1]["content"]
    assert "Answer Generation Workflow Trace" in user_content
    assert "scope_declined" in user_content
    assert "Guard Decisions" in user_content
    assert "Answer Artifacts" in user_content
    assert verdict.likely_failure_source == "prompt_overconstraint"


def test_judge_recalibrates_to_answer_class_transition(monkeypatch):
    scratch = _scratch_dir()
    monkeypatch.setattr(llm_judge, "JUDGE_CACHE_DIR", scratch / "judge_cache")

    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": llm_judge.json.dumps(
                                {
                                    "faithfulness_score": 0.4,
                                    "answer_relevance_score": 0.4,
                                    "likely_failure_source": "none",
                                    "confidence": 0.2,
                                    "reason": "",
                                    "exact_failure_step": "",
                                    "root_cause_note": "",
                                    "fix_hint": "",
                                    "self_check_status": "confirmed",
                                    "self_check_note": "",
                                    "error": "",
                                }
                            )
                        }
                    }
                ]
            }

    monkeypatch.setattr(llm_judge.requests, "post", lambda *args, **kwargs: FakeResponse())

    verdict = llm_judge.judge_answer(
        "q",
        "ctx",
        "ans",
        "ref",
        "key-one",
        retrieval_diagnosis="weak_top_k",
        guard_decisions={
            "raw_answer_class": "ANSWER",
            "cleaned_answer_class": "IDK",
            "answer_class_changed": True,
            "source_suppressed_reason": "idk",
        },
        answer_artifacts={"raw_llm_answer": "usable answer", "cleaned_answer": "I don't know"},
    )

    assert verdict.likely_failure_source == "answer_generation_drift"
    assert verdict.exact_failure_step == "answer_class_transition"
    assert verdict.confidence is not None and verdict.confidence >= 0.88
