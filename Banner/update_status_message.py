#!/usr/bin/env python3
"""
Update the Strategy Library "System Status" banner message.

Keeps the original banner content and sets one status line with fresh info
(e.g. a dbt run summary). Re-running the script replaces that status line
in place rather than piling up a new one each time. Prefers mstrio-py;
falls back to plain REST API calls if mstrio-py is not installed.

Config via environment variables (auto-loaded from a .env file placed next
to this script, no extra dependency required; copy .env.example to .env
and fill in credentials):
    MSTR_BASE_URL        Strategy Library base URL, e.g. https://host/StrategyLibrary
    MSTR_USERNAME        login username
    MSTR_PASSWORD        login password
    MSTR_LOGIN_MODE      1 = Standard (default), 16 = LDAP
    MSTR_APPLICATION_ID  Application (Library config) object ID to update
                          (defaults to the "BAM" app id seen in the HAR capture)

Usage (e.g. as a dbt on-run-end hook via `run_operation` shell/python call):
    python update_status_message.py "dbt run finished: 42 models, 0 errors"
"""

import html
import os
import re
import sys

DEFAULT_APPLICATION_ID = "903807551E2D4A9F906715426DBA0385"


def load_env_file(path: str | None = None) -> None:
    """Minimal .env loader (no external dependency). Reads KEY=VALUE lines
    from a .env file next to this script and sets them in os.environ,
    without overwriting variables already set in the real environment."""
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(
            f"Missing required environment variable: {name}\n"
            "Copy .env.example to .env in this script's directory and fill "
            "in your credentials, then re-run."
        )
    return value

STATUS_LINE_ID = "mstr-status-line"

# Matches the div's opening tag plus its first <p>...</p> block, i.e. the
# original banner message.
FIRST_PARAGRAPH_RE = re.compile(r"^(<div[^>]*>)(.*?</p>)", re.DOTALL)


def set_status_line(top_content: str, new_line: str) -> str:
    """Return top_content with new_line as the only status line, keeping
    just the original first paragraph. Always rebuilds as
    original_paragraph + status_paragraph, so re-running never accumulates
    extra lines even if older runs previously left stray ones behind.

    new_line may contain '\\n' to produce multiple visual lines within the
    single status paragraph; each line is escaped individually and joined
    with <br> so line breaks render correctly. The first line is bolded."""
    escaped_lines = [html.escape(line) for line in new_line.split("\n")]
    if escaped_lines:
        escaped_lines[0] = f"<strong>{escaped_lines[0]}</strong>"
    escaped = "<br>".join(escaped_lines)
    new_paragraph = f'<p id="{STATUS_LINE_ID}" style="text-align:left">{escaped}</p>'

    if not top_content:
        return (
            '<div class="htmleditor-container-top-editor" '
            'style="padding:10px 20px;color:#29313b;background-color:#ffffff">'
            f"{new_paragraph}</div>"
        )

    match = FIRST_PARAGRAPH_RE.match(top_content.strip())
    if match:
        div_open, original_paragraph = match.groups()
        return f"{div_open}{original_paragraph}{new_paragraph}</div>"

    # Doesn't look like the expected div/paragraph shape; append rather
    # than risk dropping content we don't understand.
    idx = top_content.rfind("</div>")
    if idx == -1:
        return top_content + new_paragraph
    return top_content[:idx] + new_paragraph + top_content[idx:]


def update_via_mstrio(new_line: str, application_id: str) -> None:
    from mstrio.api import applications
    from mstrio.connection import Connection

    conn = Connection(
        base_url=require_env("MSTR_BASE_URL"),
        username=require_env("MSTR_USERNAME"),
        password=require_env("MSTR_PASSWORD"),
        login_mode=int(os.environ.get("MSTR_LOGIN_MODE", 1)),
    )
    try:
        app = applications.get_application(conn, application_id).json()
        status = app.setdefault("systemStatus", {})
        status["enabled"] = True
        status["enableTopContent"] = True
        status["topContent"] = set_status_line(status.get("topContent", ""), new_line)
        applications.update_application(conn, application_id, app)
    finally:
        conn.close()


def update_via_rest(new_line: str, application_id: str) -> None:
    import requests

    base_url = require_env("MSTR_BASE_URL").rstrip("/")
    session = requests.Session()

    login = session.post(
        f"{base_url}/api/auth/login",
        json={
            "username": require_env("MSTR_USERNAME"),
            "password": require_env("MSTR_PASSWORD"),
            "loginMode": int(os.environ.get("MSTR_LOGIN_MODE", 1)),
        },
    )
    login.raise_for_status()
    headers = {
        "X-MSTR-AuthToken": login.headers["X-MSTR-AuthToken"],
        "Content-Type": "application/json;charset=utf-8",
    }

    try:
        resp = session.get(
            f"{base_url}/api/v2/applications/{application_id}",
            headers=headers,
            params={"outputFlag": ["INCLUDE_LOCALE", "INCLUDE_ACL"]},
        )
        resp.raise_for_status()
        app = resp.json()

        status = app.setdefault("systemStatus", {})
        status["enabled"] = True
        status["enableTopContent"] = True
        status["topContent"] = set_status_line(status.get("topContent", ""), new_line)

        put = session.put(
            f"{base_url}/api/v2/applications/{application_id}",
            headers=headers,
            json=app,
        )
        put.raise_for_status()
    finally:
        session.delete(f"{base_url}/api/auth/login", headers=headers)


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit('Usage: python update_status_message.py "<new status line>"')
    new_line = sys.argv[1]
    application_id = os.environ.get("MSTR_APPLICATION_ID", DEFAULT_APPLICATION_ID)

    try:
        update_via_mstrio(new_line, application_id)
        print("Status message updated via mstrio-py.")
    except ImportError:
        update_via_rest(new_line, application_id)
        print("Status message updated via REST API (mstrio-py not installed).")


if __name__ == "__main__":
    main()
