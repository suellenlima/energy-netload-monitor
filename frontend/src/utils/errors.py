from typing import Any

import streamlit as st


def parse_error_response(resp, fallback: str) -> str:
    try:
        payload = resp.json()
    except Exception:
        return fallback
    if isinstance(payload, dict) and payload.get("detail"):
        return str(payload["detail"])
    return fallback


def show_error(message: str, location: str = "main") -> None:
    if location == "sidebar":
        st.sidebar.error(message)
    else:
        st.error(message)