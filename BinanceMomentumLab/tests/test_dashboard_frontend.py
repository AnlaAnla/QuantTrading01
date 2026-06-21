from pathlib import Path


def test_dashboard_is_native_and_contains_required_realtime_domains() -> None:
    html = (Path(__file__).parents[1] / "web" / "index.html").read_text(encoding="utf-8")

    assert "react" not in html.lower()
    assert "node_modules" not in html
    assert "/ws/dashboard" in html
    assert "/api/paper/pause" in html
    assert "/api/paper/resume" in html
    assert "/api/emergency-stop" in html
    assert "/api/paper/reset" in html
    assert 'id="equity-line"' in html
    assert 'id="features-body"' in html
    assert 'id="signals-body"' in html
    assert 'id="orders-body"' in html
    assert 'id="positions-body"' in html
    assert 'id="error-list"' in html


def test_dangerous_actions_require_phrase_and_backend_confirmation() -> None:
    html = (Path(__file__).parents[1] / "web" / "index.html").read_text(encoding="utf-8")

    assert "二次确认" in html
    assert 'value.trim() !== "确认"' in html
    assert "{ confirm: true }" in html
    assert "API Secret" not in html
