# backend/app/ai.py
import os
from dotenv import load_dotenv

load_dotenv(override=True)

try:
    from openai import OpenAI
except Exception as e:
    raise RuntimeError(
        "OpenAI SDK not installed. Run: pip install --upgrade openai"
    ) from e

_client = None

def _get_client():
    global _client
    if _client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set in backend/.env")
        _client = OpenAI(api_key=api_key)
    return _client

DEFAULT_MODEL = os.getenv("FOLLOWUP_MODEL", "gpt-4o-mini")

def complete_chat(system: str, user: str, *, model: str | None = None,
                  temperature: float = 0.25, max_tokens: int = 400) -> str:
    """
    Thin wrapper around Chat Completions. Returns a plain string.
    """
    client = _get_client()
    m = model or DEFAULT_MODEL
    resp = client.chat.completions.create(
        model=m,
        temperature=temperature,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system.strip()},
            {"role": "user", "content": user.strip()},
        ],
    )
    return (resp.choices[0].message.content or "").strip()

def classify_text(user_text: str, labels: list[str], *, model: str | None = None) -> dict:
    """
    Returns {"label": <UPPER>, "confidence": float}.
    Falls back to simple keyword rules if parsing fails.
    """
    system = "You are a concise, high-precision classifier for client support emails. Only output JSON."
    prompt = f"""
Classify the client's message into ONE of: {", ".join(labels)}.

Message:
\"\"\"{user_text.strip()}\"\"\"

Return ONLY JSON: {{"label": "<one_label>", "confidence": 0.0-1.0}}
"""
    out = complete_chat(system, prompt, model=model, temperature=0.0, max_tokens=120)
    import json
    try:
        data = json.loads(out)
        return {
            "label": str(data.get("label", "OTHER")).upper(),
            "confidence": float(data.get("confidence", 0.7)),
        }
    except Exception:
        # Lightweight fallbacks
        t = (user_text or "").lower()
        if any(k in t for k in ["stop", "unsubscribe", "do not contact"]):
            return {"label": "DNC", "confidence": 0.99}
        if "wrong number" in t:
            return {"label": "WRONG_NUMBER", "confidence": 0.9}
        if any(k in t for k in ["uploaded", "already sent", "i sent", "i attached"]):
            return {"label": "ALREADY_UPLOADED", "confidence": 0.8}
        if any(k in t for k in ["later", "tomorrow", "next week", "when i can"]):
            return {"label": "WILL_UPLOAD_LATER", "confidence": 0.75}
        if any(k in t for k in ["help", "can't upload", "cannot upload", "error", "trouble", "issue"]):
            return {"label": "NEED_HELP", "confidence": 0.8}
        return {"label": "OTHER", "confidence": 0.5}

def rewrite_reply(context: str, *, model: str | None = None,
                  temperature: float = 0.3, max_tokens: int = 320) -> str:
    """
    Write a short, friendly, helpful law-firm intake/ops email based on the given context.
    """
    system = (
        "You write short, friendly, professional emails on behalf of a law firm's intake/operations team. "
        "Do NOT give legal advice. Keep it under ~110 words. Be concrete and helpful, use bullets when listing items. "
        "If troubleshooting upload problems, give 2–3 high-impact steps; if they fail, state you’ll loop in the team. "
        "Include the secure upload link if present in context. Avoid sounding like a bot."
    )
    return complete_chat(system, context, model=model, temperature=temperature, max_tokens=max_tokens)