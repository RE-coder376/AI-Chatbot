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
                                    "reason": "Grounded answer.",
                                    "root_cause_note": "",
                                    "fix_hint": "",
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

    assert len(calls) == 2
    assert verdict.faithfulness_score == 0.9
    assert verdict.answer_relevance_score == 0.8
    assert verdict.error == ""
