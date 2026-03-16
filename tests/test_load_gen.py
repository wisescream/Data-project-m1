import re

import load_gen


def test_headers_use_contract_compliant_user_id():
    headers = load_gen._headers("normal_user")
    assert re.fullmatch(r"usr_[a-z0-9]{8}", headers["X-User-Id"])


def test_bot_headers_keep_stable_session():
    headers = load_gen._headers("bot", stable_session=True)
    assert headers["X-Session-Id"] == "bot-fixed-session"
