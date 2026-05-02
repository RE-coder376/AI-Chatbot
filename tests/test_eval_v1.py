from pathlib import Path
import tempfile

from evals import eval_v1
from evals.llm_judge import JudgeVerdict


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(eval_v1.json.dumps(payload, indent=2), encoding="utf-8")


def _scratch_dir() -> Path:
    path = Path("tests") / "_tmp" / next(tempfile._get_candidate_names())
    path.mkdir(parents=True, exist_ok=True)
    return path


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


def test_tenant_scope_tokens_infers_from_sources_when_config_missing(monkeypatch):
    monkeypatch.setattr(eval_v1, "get_config", lambda _db_name="": {"business_name": "", "topics": [], "business_description": ""})
    monkeypatch.setattr(
        eval_v1,
        "_load_document_rows",
        lambda _db_name: [
            ("1", "Laptop product details and pricing.", "https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops"),
            ("2", "Tablet specs and prices.", "https://webscraper.io/test-sites/e-commerce/allinone/computers/tablets"),
        ],
    )

    tokens = eval_v1._tenant_scope_tokens("mystery_store")

    assert "mystery" in tokens or "store" in tokens
    assert "webscraper" in tokens
    assert "laptop" in tokens or "tablet" in tokens


def test_is_tenant_relevant_question_does_not_treat_course_words_as_global_scope():
    store_scope = {"store", "webscraper", "laptops", "phones", "electronics"}
    assert not eval_v1._is_tenant_relevant_question(
        "What does Give Your Employee An Identity cover?",
        "store",
        store_scope,
    )


def test_extract_chunk_topic_handles_product_chunks():
    text = (
        'Product: IdeaTab A3500L Black Price: $88.99 RAM: 8GB Display: 7" IPS '
        'OS: Android 4.2 Full specs: IdeaTab A3500L Black, 7" IPS, Quad-Core 1.2GHz, 8GB, Android 4.2'
    )

    topic = eval_v1._extract_chunk_topic(text, "https://webscraper.io/test-sites/e-commerce/allinone/computers/tablets")
    question = eval_v1._make_chunk_question(topic, text)

    assert topic == "IdeaTab A3500L Black"
    assert question == "What is the pricing for IdeaTab A3500L Black?"


def test_load_gap_candidates_filters_cross_tenant_pollution(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    db_dir = temp_path / "databases" / "agentfactory"
    _write_json(
        db_dir / "config.json",
        {
            "business_name": "AgentFactory by Panaversity",
            "topics": "AI agent development, courses, pricing, curriculum, team",
            "business_description": "Platform for learning to build AI agents.",
        },
    )
    _write_json(
        db_dir / "knowledge_gaps.json",
        [
            {"question": "What is spec-driven development?"},
            {"question": "What premium fountain pens do you stock?"},
            {"question": "What educational toys do you have for a 2-year-old under 3000 PKR?"},
        ],
    )

    items = eval_v1._load_gap_candidates("agentfactory")

    assert [item.q for item in items] == ["What is spec-driven development?"]


def test_load_doc_qa_fallbacks_filters_cross_tenant_embedded_qa(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    db_dir = temp_path / "databases" / "agentfactory"
    _write_json(
        db_dir / "config.json",
        {
            "business_name": "AgentFactory by Panaversity",
            "topics": "AI agent development, courses, pricing, curriculum, team",
            "business_description": "Platform for learning to build AI agents.",
        },
    )
    monkeypatch.setattr(
        eval_v1,
        "_load_document_rows",
        lambda _db_name: [
            ("1", "Q: What is AgentFactory? A: AgentFactory is an AI agent learning platform."),
            ("2", "Q: What premium fountain pens do you stock? A: We carry luxury fountain pens and journals."),
        ],
    )

    items = eval_v1._load_doc_qa_fallbacks("agentfactory", 10)

    assert [item.q for item in items] == ["What is AgentFactory?"]


def test_subscription_questions_are_not_globally_filtered(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.chdir(temp_path)
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    monkeypatch.setattr(eval_v1, "_load_document_rows", lambda _db_name: [])
    db_dir = temp_path / "databases" / "tenant_subs"

    _write_json(
        db_dir / "analytics.json",
        {"questions": {"How do I cancel my subscription plan?": 5}},
    )

    items = eval_v1._collect_seed_items("tenant_subs", 10)

    assert [item.q for item in items] == ["How do I cancel my subscription plan?"]


def test_load_analytics_candidates_filters_cross_tenant_pollution(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    db_dir = temp_path / "databases" / "agentfactory"

    _write_json(
        db_dir / "config.json",
        {
            "business_name": "AgentFactory by Panaversity",
            "topics": "AI agent development, courses, pricing, curriculum, team",
            "business_description": "Platform for learning to build AI agents.",
        },
    )
    _write_json(
        db_dir / "analytics.json",
        {
            "questions": {
                "What is AgentFactory?": 5,
                "What courses are available?": 4,
                "Which laptop has the most RAM under $700?": 9,
                "What are the top 3 highest rated anime of all time on MAL?": 7,
            }
        },
    )

    items = eval_v1._load_analytics_candidates("agentfactory")
    questions = [item.q for item in items]

    assert "What is AgentFactory?" in questions
    assert "What courses are available?" in questions
    assert "Which laptop has the most RAM under $700?" not in questions
    assert "What are the top 3 highest rated anime of all time on MAL?" not in questions


def test_is_tenant_relevant_eval_question_rejects_cross_tenant_queries(monkeypatch):
    temp_path = _scratch_dir()
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", temp_path / "databases")
    db_dir = temp_path / "databases" / "agentfactory"
    _write_json(
        db_dir / "config.json",
        {
            "business_name": "AgentFactory by Panaversity",
            "topics": "AI agent development, courses, pricing, curriculum, team",
            "business_description": "Platform for learning to build AI agents.",
        },
    )

    assert eval_v1._is_tenant_relevant_eval_question("What is AgentFactory?", "agentfactory") is True
    assert eval_v1._is_tenant_relevant_eval_question("What premium fountain pens do you stock?", "agentfactory") is False


def test_collect_eval_items_keeps_only_grounded_candidates(monkeypatch):
    temp_path = _scratch_dir().resolve()
    monkeypatch.chdir(temp_path)
    monkeypatch.setattr(eval_v1, "DATABASES_DIR", (temp_path / "databases").resolve())
    monkeypatch.setattr(eval_v1, "_load_document_rows", lambda _db_name: [])
    db_dir = eval_v1.DATABASES_DIR / "tenant_b"
    _write_json(
        db_dir / "config.json",
        {
            "business_name": "Tenant B Logistics",
            "topics": "refund policy, shipping, delivery, returns",
            "business_description": "Support help for refunds and shipping questions.",
        },
    )

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
            item.retrieve_context_preview = "The refund policy allows refunds within 7 days and requires proof of purchase."
        elif "shipping" in item.q.lower():
            item.retrieve_doc_count = 2
            item.retrieve_context_length = 260
            item.retrieve_context_preview = "Compare standard and express shipping by delivery speed and cost."
        else:
            item.retrieve_doc_count = 0
            item.retrieve_context_length = 0
            item.retrieve_context_preview = ""
        return item

    monkeypatch.setattr(eval_v1, "_preflight_retrieve", fake_preflight)

    items = eval_v1._collect_eval_items("http://example.test", "pw", "tenant_b", 10)
    questions = [item.q for item in items]

    assert "How does the refund policy work?" in questions
    assert "Compare standard and express shipping." in questions
    assert "What is your CEO's favorite color?" not in questions


def test_is_grounded_requires_some_preview_relevance():
    item = eval_v1.EvalItem(
        q="How do refunds work?",
        source="analytics",
        retrieve_doc_count=5,
        retrieve_context_length=500,
        retrieve_context_preview="Our company values teamwork, innovation, and long-term learning culture.",
    )

    assert eval_v1._is_grounded(item) is False


def test_is_grounded_accepts_relevant_preview():
    item = eval_v1.EvalItem(
        q="How do refunds work?",
        source="analytics",
        retrieve_doc_count=5,
        retrieve_context_length=500,
        retrieve_context_preview="Refunds are allowed within 7 days for unused items with proof of purchase.",
    )

    assert eval_v1._is_grounded(item) is True


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


def test_grade_retrieval_fails_when_context_is_incomplete():
    item = eval_v1.EvalItem(
        q="How does the refund policy work?",
        reference_answer="Refunds are allowed within 7 days. Unused items require proof of purchase. Damaged goods need support approval first.",
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
        ],
        expected_source="https://example.test/refunds",
    )

    assert result["status"] == "FAIL"
    assert result["diagnosis"] == "retrieval_incomplete"
    assert result["context_recall"] < 0.8


def test_grade_retrieval_passes_product_url_hit_even_with_noisy_reference():
    item = eval_v1.EvalItem(
        q="What is the price of Daler Rowney Turpentine Oil?",
        expect="ANSWER",
        source="chunk_topic",
        reference_answer=(
            "Name: Daler Rowney Turpentine Oil Category: Turpentine Oil "
            "Daler Rowney Turpentine Oil Rs.2,045 You may also like Car Stationery set 5 in one Rs.450 "
            "M&G Stationery Tape 97322 Rs.560 Wooden Pen Stand Rs.4,085"
        ),
        retrieve_context_preview="Daler Rowney Turpentine Oil Rs.2,045 Dilutes oil color and cleans brushes.",
        retrieve_sources=["https://www.thestationerycompany.pk/products/daler-rowney-turpentine-oil"],
    )

    result = eval_v1._grade_retrieval(
        item,
        [
            {
                "source": "https://www.thestationerycompany.pk/products/daler-rowney-turpentine-oil",
                "preview": "Daler Rowney Turpentine Oil Rs.2,045 Dilutes oil color and cleans brushes.",
            }
        ],
        expected_source="https://www.thestationerycompany.pk/products/daler-rowney-turpentine-oil",
    )

    assert result["hit"] is True
    assert result["context_recall"] == 0.0
    assert result["status"] == "PASS"
    assert result["diagnosis"] == "retrieval_ok"


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


def test_grade_answer_accepts_short_pricing_answer_when_judge_is_perfect():
    item = eval_v1.EvalItem(
        q="What is the price of Daler Rowney Turpentine Oil?",
        expect="ANSWER",
        source="chunk_topic",
        reference_answer="Daler Rowney Turpentine Oil Rs.2,045",
        retrieve_doc_count=2,
        retrieve_context_length=160,
        retrieve_context_preview="Daler Rowney Turpentine Oil Rs.2,045 Dilutes oil color to a thin wash.",
        retrieve_sources=["https://www.thestationerycompany.pk/products/daler-rowney-turpentine-oil"],
    )
    verdict = JudgeVerdict(
        faithfulness_score=1.0,
        answer_relevance_score=1.0,
        likely_failure_source="none",
    )

    status, reason, _ = eval_v1._grade_answer(item, "Rs.2,045", judge_verdict=verdict)

    assert status == "PASS"
    assert reason is None


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


def test_statement_supported_rejects_wrong_numeric_claim():
    ok, score = eval_v1._statement_supported(
        "Refunds are allowed within 3 days for unused items.",
        ["Refunds are allowed within 7 days for unused items with proof of purchase."],
    )

    assert ok is False
    assert score == 0.0


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


def test_answer_metrics_include_context_recall():
    item = eval_v1.EvalItem(
        q="How does the refund policy work?",
        expect="ANSWER",
        source="faq",
        reference_answer="Refunds are allowed within 7 days. Proof of purchase is required. Support approval is needed for damaged goods.",
        retrieve_doc_count=2,
        retrieve_context_length=200,
        retrieve_context_preview="Refunds are allowed within 7 days. Proof of purchase is required.",
    )

    metrics = eval_v1._answer_metrics(item, "Refunds are allowed within 7 days and require proof of purchase.")

    assert metrics["context_recall"] is not None
    assert metrics["context_recall"] < 1.0


def test_statement_relevant_is_stricter_for_long_domain_overlap():
    ok, score = eval_v1._statement_relevant(
        "The course learning dashboard offers a friendly community experience for members.",
        "How do I cancel my subscription plan?",
        "Cancel a subscription from the billing settings page.",
    )

    assert ok is False
    assert score < 0.35


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


def test_build_summary_includes_failure_breakdown():
    results = [
        {"retrieval_status": "FAIL", "answer_status": "SKIP", "idk_status": "SKIP", "overall_status": "FAIL", "retrieval_diagnosis": "weak_top_k"},
        {"retrieval_status": "PASS", "answer_status": "FAIL", "idk_status": "SKIP", "overall_status": "FAIL", "answer_diagnosis": "grounded_but_answered_idk"},
    ]

    summary = eval_v1._build_summary(results)

    assert summary["failure_breakdown"] == {
        "weak_top_k": 1,
        "grounded_but_answered_idk": 1,
    }


def test_build_summary_includes_failure_source_mix():
    results = [
        {
            "retrieval_status": "PASS",
            "answer_status": "FAIL",
            "idk_status": "SKIP",
            "overall_status": "FAIL",
            "answer_diagnosis": "answer_not_faithful",
            "judge": {"likely_failure_source": "prompt_overconstraint", "root_cause_note": "Tier 3 IDK rule is too aggressive.", "fix_hint": "Relax scope guard for grounded answers."},
        },
        {
            "retrieval_status": "PASS",
            "answer_status": "FAIL",
            "idk_status": "SKIP",
            "overall_status": "FAIL",
            "answer_diagnosis": "answer_irrelevant",
            "judge": {"likely_failure_source": "answer_generation_drift", "root_cause_note": "Answer ignored the main question focus.", "fix_hint": "Tighten response-format prompt."},
        },
    ]

    summary = eval_v1._build_summary(results)

    assert summary["failure_source_mix"] == {
        "prompt_overconstraint": 1,
        "answer_generation_drift": 1,
    }
    assert summary["root_cause_mix"] == {
        "Tier 3 IDK rule is too aggressive.": 1,
        "Answer ignored the main question focus.": 1,
    }
    assert summary["fix_hint_mix"] == {
        "Relax scope guard for grounded answers.": 1,
        "Tighten response-format prompt.": 1,
    }


def test_grade_answer_uses_judge_scores_when_available():
    item = eval_v1.EvalItem(
        q="What is the refund policy?",
        expect="ANSWER",
        source="faq",
        reference_answer="Refunds are allowed within 7 days for unused items with proof of purchase.",
        retrieve_doc_count=3,
        retrieve_context_length=240,
        retrieve_context_preview="Refunds are allowed within 7 days for unused items with proof of purchase.",
    )

    verdict = JudgeVerdict(
        faithfulness_score=0.92,
        answer_relevance_score=0.88,
        likely_failure_source="none",
    )
    metrics = eval_v1._answer_metrics(
        item,
        "You have a week to return unused items, and proof of purchase is required.",
        judge_verdict=verdict,
    )

    assert metrics["judge_used"] is True
    assert metrics["faithfulness"] == 0.92
    assert metrics["answer_relevance"] == 0.88
    assert metrics["deterministic_faithfulness"] != metrics["faithfulness"] or metrics["deterministic_answer_relevance"] != metrics["answer_relevance"]


def test_grade_answer_falls_back_when_judge_errors():
    item = eval_v1.EvalItem(
        q="What is the refund policy?",
        expect="ANSWER",
        source="faq",
        reference_answer="Refunds are allowed within 7 days for unused items with proof of purchase.",
        retrieve_doc_count=3,
        retrieve_context_length=240,
        retrieve_context_preview="Refunds are allowed within 7 days for unused items with proof of purchase.",
    )

    verdict = JudgeVerdict(error="judge_http_429")
    metrics = eval_v1._answer_metrics(
        item,
        "Refunds are allowed within 7 days for unused items, and you need proof of purchase.",
        judge_verdict=verdict,
    )

    assert metrics["judge_used"] is False
    assert metrics["faithfulness"] == metrics["deterministic_faithfulness"]
    assert metrics["answer_relevance"] == metrics["deterministic_answer_relevance"]


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


def test_make_chunk_question_prefers_process_wording_for_step_chunks():
    q = eval_v1._make_chunk_question(
        "Spec And Build Your First Tool",
        "Step 1: write the spec. Step 2: review it. Step 3: build and verify the tool.",
    )

    assert q == "What is the process for Spec And Build Your First Tool?"


def test_make_chunk_question_uses_prerequisite_wording_when_reference_supports_it():
    q = eval_v1._make_chunk_question(
        "Chapter 65: Enable File Checkpointing",
        "Chapter 65: Enable File Checkpointing. Prerequisites include a configured storage backend and an enabled checkpoint worker before you start the setup.",
    )

    assert q == "What are the prerequisites for Chapter 65: Enable File Checkpointing?"


def test_make_chunk_question_skips_quiz_like_topics():
    q = eval_v1._make_chunk_question(
        "Chapter Quiz",
        "Prompt 1: answer the quiz questions and review your work.",
    )

    assert q == ""


def test_trim_seed_pool_does_not_force_minimum_of_ten():
    items = [
        eval_v1.EvalItem(q="What is AgentFactory?", source="faq", frequency=1000, difficulty="easy"),
        eval_v1.EvalItem(q="How does the curriculum work?", source="analytics", frequency=10, difficulty="medium"),
        eval_v1.EvalItem(q="What does Chapter 1 cover?", source="chunk_topic", frequency=2, difficulty="hard"),
    ]

    trimmed = eval_v1._trim_seed_pool(items, 3)

    assert 3 <= len(trimmed) <= 6


def test_chunk_reference_answer_prefers_clean_semantic_sentences():
    ref = eval_v1._chunk_reference_answer(
        "Refund Policy",
        """
        What you are learning: this exercise builds judgment.
        The refund policy allows refunds within 7 days for unused items with proof of purchase.
        Customers must contact support before returning damaged goods.
        Prompt 3: compare your answer with a partner.
        """,
    )

    assert "refund policy allows refunds within 7 days" in ref.lower()
    assert "prompt 3" not in ref.lower()
    assert "what you are learning" not in ref.lower()


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
