def test_loader_javascript_is_public_and_isolated(client):
    response = client.get("/widget-loader.js")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/javascript")
    script = response.text
    assert "document.currentScript" in script
    assert "data-db" in script
    assert "attachShadow" in script
    assert "__AI_CHAT_WIDGETS__" in script
    assert "/widget-bootstrap/" in script
    assert "addEventListener('click'" in script
    assert "onclick=" not in script


def test_widget_bootstrap_returns_stable_route(client, two_tenants):
    response = client.get("/widget-bootstrap/a")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["db"] == "a"
    assert payload["widget_url"] == "/w/a"
    assert payload["primary_color"].startswith("#")
    assert "widget_key" not in payload


def test_widget_bootstrap_rejects_missing_database(client):
    response = client.get("/widget-bootstrap/not_a_real_database")

    assert response.status_code == 404


def test_stable_widget_route_redirects_to_current_key(client, two_tenants):
    response = client.get("/w/a", follow_redirects=False)

    assert response.status_code == 302, response.text
    assert response.headers["location"] == "/widget-chat?key=wk_a"
    assert response.headers["content-security-policy"] == "frame-ancestors *"
    assert "x-frame-options" not in response.headers


def test_stable_widget_route_rejects_missing_database(client):
    response = client.get("/w/not_a_real_database", follow_redirects=False)

    assert response.status_code == 404


def test_admin_embed_code_is_one_line_and_does_not_pin_raw_key(client, two_tenants):
    response = client.get(
        "/admin/embed-code",
        headers={"Authorization": "Bearer clientA", "X-Admin-DB": "a"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    snippet = payload["snippet"]
    assert snippet == payload["embed_code"]
    assert snippet == '<script src="https://testserver/widget-loader.js" data-db="a" defer></script>'
    assert "wk_a" not in snippet
    assert "widget-chat?key=" not in snippet
    assert payload["install_url"] == "https://testserver/w/a"
