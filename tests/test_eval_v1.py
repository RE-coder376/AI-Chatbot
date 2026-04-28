from pathlib import Path
import tempfile

from evals import eval_v1


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(eval_v1.json.dumps(payload, indent=2), encoding="utf-8")


def _scratch_dir() -> Path:
    return Path(tempfile.mkdtemp())


def test_collect_seed_items_filters_generic_noise_and_keeps_curated(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.chdir(temp_path)
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    monkeypatch.setattr(eval_v1, "_load_document_rows", lambda _db_name: [])
    db_dir = temp_path / "databases" / "tenant_a"

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


def test_collect_eval_items_keeps_only_grounded_candidates(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.chdir(temp_path)
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    monkeypatch.setattr(eval_v1, "_load_document_rows", lambda _db_name: [])
    db_dir = temp_path / "databases" / "tenant_b"

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


def test_faq_items_still_need_retrieval_support(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.chdir(temp_path)
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    monkeypatch.setattr(eval_v1, "_load_document_rows", lambda _db_name: [])
    db_dir = temp_path / "databases" / "tenant_c"

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
    assert "faithful enough" in reason or "align closely enough" in reason
    assert overlap == 0.0


def test_grade_answer_passes_grounded_answer():
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
        "Refunds are allowed within 7 days for unused items, and you need proof of purchase.",
    )

    assert status == "PASS"
    assert reason is None
    assert overlap >= 0.12


def test_grade_retrieval_rewards_good_top_rank():
    item = eval_v1.EvalItem(
        q="How do refunds work?",
        reference_answer="Refunds are allowed within 7 days for unused items with proof of purchase.",
        retrieve_context_preview="Refunds are allowed within 7 days for unused items with proof of purchase.",
        retrieve_sources=["https://example.test/refunds"],
    )

    result = eval_v1._grade_retrieval(
        item,
        [
            {
                "source": "https://example.test/refunds",
                "preview": "Refunds are allowed within 7 days for unused items with proof of purchase.",
            },
            {
                "source": "https://example.test/about",
                "preview": "Our company was founded in 2020 and serves online businesses.",
            },
        ],
        expected_source="https://example.test/refunds",
    )

    assert result["status"] == "PASS"
    assert result["average_precision"] >= 0.9
    assert result["first_relevant_rank"] == 1


def test_grade_retrieval_fails_when_relevant_chunk_is_buried():
    item = eval_v1.EvalItem(
        q="How do refunds work?",
        reference_answer="Refunds are allowed within 7 days for unused items with proof of purchase.",
        retrieve_context_preview="Refunds are allowed within 7 days for unused items with proof of purchase.",
        retrieve_sources=["https://example.test/refunds"],
    )

    result = eval_v1._grade_retrieval(
        item,
        [
            {"source": "https://example.test/about", "preview": "We are a growing company with a remote team."},
            {"source": "https://example.test/blog", "preview": "Read our latest marketing trends and AI updates."},
            {
                "source": "https://example.test/refunds",
                "preview": "Refunds are allowed within 7 days for unused items with proof of purchase.",
            },
        ],
        expected_source="https://example.test/refunds",
    )

    assert result["status"] == "FAIL"
    assert result["diagnosis"] == "weak_top_k"
    assert result["average_precision"] < 0.55


def test_grade_answer_classifies_refuse_consistently():
    item = eval_v1.EvalItem(
        q="What is the weather today?",
        expect="REFUSE",
        source="analytics",
    )

    status, reason, _ = eval_v1._grade_answer(
        item,
        "I can't help with that because it falls outside my scope. I specialize in helping with the tenant knowledge base.",
    )

    assert status == "PASS"
    assert reason is None


def test_grade_answer_classifies_idk_consistently():
    item = eval_v1.EvalItem(
        q="What is your CEO's favorite color?",
        expect="IDK",
        source="analytics",
    )

    status, reason, _ = eval_v1._grade_answer(
        item,
        "I don't have that information in my knowledge base right now.",
    )

    assert status == "PASS"
    assert reason is None


def test_grade_answer_fails_unfaithful_supported_sounding_answer():
    item = eval_v1.EvalItem(
        q="What is the refund policy?",
        expect="ANSWER",
        source="faq",
        reference_answer="Refunds are allowed within 7 days for unused items with proof of purchase.",
        retrieve_doc_count=3,
        retrieve_context_length=240,
        retrieve_context_preview="Refunds are allowed within 7 days for unused items with proof of purchase.",
    )

    status, reason, _ = eval_v1._grade_answer(
        item,
        "Refunds are only available within 30 days, and opened items are always accepted without proof of purchase.",
    )

    assert status == "FAIL"
    assert "faithful enough" in reason


def test_answer_metrics_penalize_irrelevant_rambling():
    item = eval_v1.EvalItem(
        q="How do I register for the program?",
        expect="ANSWER",
        source="faq",
        reference_answer="Create an account, verify your email, and complete the checkout form.",
        retrieve_doc_count=2,
        retrieve_context_length=200,
        retrieve_context_preview="Create an account, verify your email, and complete the checkout form.",
    )

    metrics = eval_v1._answer_metrics(
        item,
        "Our team cares deeply about student success and community values. We believe in practical learning and innovation.",
    )

    assert metrics["answer_relevance"] < 0.5


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


def test_finalize_selection_uses_stable_core_and_rotates_discovery(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.chdir(temp_path)
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    monkeypatch.setattr(eval_v1, "_load_document_rows", lambda _db_name: [])

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


def test_chunk_state_file_is_module_relative():
    path = eval_v1._chunk_state_file("tenant_x")

    assert path.parent == eval_v1.EVALS_DIR / "state"
    assert path.name == "tenant_x_rotation.json"


def test_preflight_retrieve_raises_on_owner_auth_failure(monkeypatch):
    class FakeResponse:
        status_code = 403

    monkeypatch.setattr(eval_v1.requests, "post", lambda *args, **kwargs: FakeResponse())

    item = eval_v1.EvalItem(q="What is AgentFactory?")

    try:
        eval_v1._preflight_retrieve("http://example.test", "wrong", item)
        assert False, "Expected EvalAuthError"
    except eval_v1.EvalAuthError as exc:
        assert "Owner auth failed" in str(exc)


def test_load_document_rows_prefers_chroma_then_falls_back(monkeypatch):
    chroma_rows = [("doc-a", "alpha")]
    sqlite_rows = [("doc-b", "beta")]

    monkeypatch.setattr(eval_v1, "_load_document_rows_via_chroma", lambda _db_name: chroma_rows)
    monkeypatch.setattr(eval_v1, "_load_document_rows_via_sqlite", lambda _db_name: sqlite_rows)
    assert eval_v1._load_document_rows("tenant_x") == chroma_rows

    monkeypatch.setattr(eval_v1, "_load_document_rows_via_chroma", lambda _db_name: [])
    assert eval_v1._load_document_rows("tenant_x") == sqlite_rows


def test_doc_qa_fallback_filters_malformed_prompt_blobs(monkeypatch):
    monkeypatch.setattr(
        eval_v1,
        "_load_document_rows",
        lambda _db_name: [
            (
                "doc-1",
                'Q: [The question as an employee would ask it]** A: [Your verified plain-language answer from /policy-lookup]',
            ),
            (
                "doc-2",
                "Q: What is AgentFactory? A: AgentFactory is an AI agent learning platform.",
            ),
        ],
    )

    items = eval_v1._load_doc_qa_fallbacks("tenant_x", 10)

    assert [item.q for item in items] == ["What is AgentFactory?"]
    assert items[0].expect == "ANSWER"


def test_doc_qa_fallback_marks_missing_info_as_idk(monkeypatch):
    monkeypatch.setattr(
        eval_v1,
        "_load_document_rows",
        lambda _db_name: [
            (
                "doc-1",
                "Q: Can TaskManager integrate with Jira? A: I searched the documentation but couldn't find information about Jira integration and escalated this to support.",
            ),
        ],
    )

    items = eval_v1._load_doc_qa_fallbacks("tenant_x", 10)

    assert len(items) == 1
    assert items[0].expect == "IDK"


def test_chunk_topic_candidates_are_filtered_and_not_stuck_on_first_rows(monkeypatch):
    rows = [
        ("1", "Meeting notes: use these codes while taking notes. D: decision. A: action."),
        ("2", "def build_agent(): return workflow"),
        ("3", "Chapter 19: File Processing Workflows explains how files are transformed, validated, and routed through an agent workflow with structured outputs and review steps."),
        ("4", "Computation and Data Extraction covers pulling structured values from messy text and converting them into reliable fields for downstream use.", "https://example.test/computation-and-data-extraction"),
    ]
    monkeypatch.setattr(eval_v1, "_load_document_rows", lambda _db_name: rows)
    monkeypatch.setattr(eval_v1, "_candidate_row_order", lambda rows: rows)

    items = eval_v1._load_chunk_topic_candidates("tenant_x", 1)
    questions = [item.q for item in items]

    assert questions
    assert all("Meeting notes" not in q for q in questions)
    assert all("def build_agent" not in q for q in questions)
    assert any("Chapter 19" in q or "Computation and Data Extraction" in q for q in questions)


def test_chunk_topic_candidates_filter_transfer_log_topics(monkeypatch):
    rows = [
        ("1", "objects: 100% (245/245), 89.42 KiB | 1.29 MiB/s, done. Create LEARNING-SPEC.md with your notes."),
    ]
    monkeypatch.setattr(eval_v1, "_load_document_rows", lambda _db_name: rows)
    monkeypatch.setattr(eval_v1, "_candidate_row_order", lambda rows: rows)

    items = eval_v1._load_chunk_topic_candidates("tenant_x", 10)

    assert items == []


def test_database_dir_is_project_relative():
    expected = eval_v1.EVALS_DIR.parent / "databases"
    assert eval_v1.DATABASES_DIR == expected


def test_collect_eval_items_end_to_end_with_seeded_sources(monkeypatch):
    monkeypatch.setattr(
        eval_v1,
        "_collect_seed_items",
        lambda _db_name, _desired_count: [
            eval_v1.EvalItem(
                q="What is AgentFactory?",
                source="faq",
                difficulty="easy",
                reference_answer="AgentFactory is an AI agent learning platform.",
                frequency=1000,
                candidate_key="faq::agentfactory",
            ),
            eval_v1.EvalItem(
                q="How does the curriculum work?",
                source="analytics",
                difficulty="medium",
                reference_answer="The curriculum is delivered in guided chapters with projects.",
                frequency=10,
                candidate_key="analytics::curriculum",
            ),
            eval_v1.EvalItem(
                q="Summarize Chapter 24.",
                source="chunk_topic",
                difficulty="hard",
                reference_answer="Chapter 24 is about building a proactive AI employee.",
                frequency=2,
                candidate_key="chunk::24",
            ),
        ],
    )

    def fake_preflight(_base_url, _password, item):
        item.retrieve_doc_count = 3
        item.retrieve_context_length = 300
        item.retrieve_context_preview = item.reference_answer
        item.retrieve_sources = ["seeded-source"]
        return item

    monkeypatch.setattr(eval_v1, "_preflight_retrieve", fake_preflight)
    monkeypatch.setattr(eval_v1, "_save_rotation_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(eval_v1, "_load_rotation_state", lambda _db_name: {"cursor": 0})

    items = eval_v1._collect_eval_items("http://example.test", "ownerpw", "tenant_seeded", 3)

    assert len(items) == 3
    assert {item.source for item in items} == {"faq", "analytics", "chunk_topic"}
    assert all(item.retrieve_doc_count > 0 for item in items)
