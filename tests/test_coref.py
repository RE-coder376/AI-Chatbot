from services.coref import _names_from_answer, resolve_followup


def test_policy_prose_is_not_extracted_as_product():
    """An answer about logistics/policy must not yield a product name — else a
    following "is it in stock?" binds the pronoun to "return policy"."""
    assert _names_from_answer("Our return policy is available at: https://x/refund-policy") == []
    assert _names_from_answer("Shipping is available across the country.") == []


def test_real_product_names_still_extracted():
    assert "Astronaut Space Boy Glitter Water" in _names_from_answer(
        "- Astronaut Space Boy Glitter Water - Rs.5,900")
    assert "Wish For Women By Chopard" in " ".join(_names_from_answer(
        "The Wish For Women By Chopard is priced at Rs.9250."))


def test_pronoun_falls_back_through_policy_turn_to_product():
    history = [
        {"role": "user", "content": "do you carry the Astronaut Space Boy Glitter Water?"},
        {"role": "assistant", "content": "Yes — we carry astronaut space boy glitter water. "
                                          "- Astronaut Space Boy Glitter Water - Rs.5,900"},
        {"role": "user", "content": "whats your return policy?"},
        {"role": "assistant", "content": "Our return policy is available at: https://x/policies/refund-policy"},
    ]
    out = resolve_followup("is it in stock right now?", history).lower()
    assert "astronaut space boy glitter water" in out
    assert "return policy" not in out


def test_intervening_product_binds_most_recent():
    history = [
        {"role": "user", "content": "do you have the Magnetic Train?"},
        {"role": "assistant", "content": "Yes — we carry Magnetic Train. - Magnetic Train - Rs.1,000"},
        {"role": "user", "content": "and do you also stock the Wall Climbing Car?"},
        {"role": "assistant", "content": "Yes — we carry Wall Climbing Car. - Wall Climbing Car - Rs.2,000"},
    ]
    out = resolve_followup("how much is it?", history).lower()
    assert "wall climbing car" in out
