from pathlib import Path

from evals import eval_v1


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(eval_v1.json.dumps(payload, indent=2), encoding="utf-8")


def test_collect_seed_items_filters_generic_noise_and_keeps_curated(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    db_dir = tmp_path / "databases" / "tenant_a"

    _write_json(
        db_dir / "faqs.json",
        [{"question": "What is AgentFactory?", "answer": "An AI agent learning platform."}],
    )
    _write_json(
        db_dir / "analytics.json",
        {
            "questions": {
                "What is the weather today?": 8,
                "What is AgentFactory?": 5,
                "How does the curriculum work?": 4,
            },
            "history": [
                {"q": "What is the capital of Japan?"},
                {"q": "How does the curriculum work?"},
            ],
        },
    )
    _write_json(
        db_dir / "knowledge_gaps.json",
        [
            {"question": "What is 2+2?"},
            {"question": "Who teaches the course?"},
        ],
    )

    items = eval_v1._collect_seed_items("tenant_a", 10)
    questions = [item.q for item in items]

    assert "What is AgentFactory?" in questions
    assert "How does the curriculum work?" in questions
    assert "Who teaches the course?" in questions
    assert "What is the weather today?" not in questions
    assert "What is the capital of Japan?" not in questions
    assert "What is 2+2?" not in questions


def test_collect_eval_items_keeps_only_grounded_candidates(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    db_dir = tmp_path / "databases" / "tenant_b"

    _write_json(
        db_dir / "analytics.json",
        {
            "questions": {
                "How does the refund policy work?": 5,
                "What is your CEO's favorite color?": 4,
                "Compare standard and express shipping.": 3,
            }
        },
    )

    def fake_preflight(_base_url, _password, item):
        if "refund" in item.q.lower():
            item.retrieve_doc_count = 3
            item.retrieve_context_length = 420
        elif "shipping" in item.q.lower():
            item.retrieve_doc_count = 2
            item.retrieve_context_length = 260
        else:
            item.retrieve_doc_count = 0
            item.retrieve_context_length = 0
        return item

    monkeypatch.setattr(eval_v1, "_preflight_retrieve", fake_preflight)

    items = eval_v1._collect_eval_items("http://example.test", "pw", "tenant_b", 10)
    questions = [item.q for item in items]

    assert "How does the refund policy work?" in questions
    assert "Compare standard and express shipping." in questions
    assert "What is your CEO's favorite color?" not in questions


def test_faq_items_still_need_retrieval_support(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    db_dir = tmp_path / "databases" / "tenant_c"

    _write_json(
        db_dir / "faqs.json",
        [{"question": "What is your refund policy?", "answer": "Refunds are allowed within 7 days."}],
    )

    def fake_preflight(_base_url, _password, item):
        item.retrieve_doc_count = 0
        item.retrieve_context_length = 0
        return item

    monkeypatch.setattr(eval_v1, "_preflight_retrieve", fake_preflight)

    items = eval_v1._collect_eval_items("http://example.test", "pw", "tenant_c", 5)
    assert items == []


def test_grade_answer_fails_long_but_ungrounded_answer():
    item = eval_v1.EvalItem(
        q="How does the refund policy work?",
        expect="ANSWER",
        source="analytics",
        reference_answer="Refunds are allowed within 7 days for unused items with proof of purchase.",
        retrieve_doc_count=3,
        retrieve_context_length=240,
        retrieve_context_preview="Refunds are allowed within 7 days for unused items with proof of purchase.",
    )

    status, reason, overlap = eval_v1._grade_answer(
        item,
        "Our company also runs luxury travel tours and celebrity meetups across Europe all summer long.",
    )

    assert status == "FAIL"
    assert "align closely enough" in reason
    assert overlap == 0.0


def test_build_summary_does_not_fake_idk_score():
    results = [
        {"retrieval_status": "PASS", "answer_status": "PASS", "idk_status": "SKIP", "overall_status": "PASS"},
        {"retrieval_status": "FAIL", "answer_status": "FAIL", "idk_status": "SKIP", "overall_status": "FAIL"},
    ]

    summary = eval_v1._build_summary(results)

    assert summary["overall_score"] == 5.0
    assert summary["retrieval_score"] == 5.0
    assert summary["answer_score"] == 5.0
    assert summary["idk_score"] is None
    assert summary["score_meanings"]["idk"] == "Not exercised in this run."


def test_finalize_selection_uses_stable_core_and_rotates_discovery(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    core_pool = [
        eval_v1.EvalItem(q="What is AgentFactory?", source="faq", frequency=1000, difficulty="easy", candidate_key="faq::1"),
        eval_v1.EvalItem(q="How does the curriculum work?", source="analytics", frequency=20, difficulty="medium", candidate_key="analytics::1"),
        eval_v1.EvalItem(q="Who teaches the course?", source="analytics", frequency=10, difficulty="hard", candidate_key="analytics::2"),
    ]
    discovery_pool = [
        eval_v1.EvalItem(q="What are the key details about Chapter 19?", source="chunk_topic", difficulty="easy", candidate_key="chunk::1"),
        eval_v1.EvalItem(q="Summarize Chapter 20.", source="chunk_topic", difficulty="medium", candidate_key="chunk::2"),
        eval_v1.EvalItem(q="How does Chapter 21 work?", source="chunk_topic", difficulty="hard", candidate_key="chunk::3"),
    ]

    first = eval_v1._finalize_selection(core_pool, discovery_pool, 4, "tenant_rotate")
    second = eval_v1._finalize_selection(core_pool, discovery_pool, 4, "tenant_rotate")

    first_core = [item.q for item in first if item.selection_bucket == "stable_core"]
    second_core = [item.q for item in second if item.selection_bucket == "stable_core"]
    first_discovery = [item.q for item in first if item.selection_bucket == "rotating_discovery"]
    second_discovery = [item.q for item in second if item.selection_bucket == "rotating_discovery"]

    assert first_core == second_core
    assert first_discovery != second_discovery
    assert len(first_discovery) == 2
    assert len(second_discovery) == 2
