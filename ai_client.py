import os
from dotenv import load_dotenv
from openai import OpenAI
from anthropic import Anthropic

# Load variables from .env (OPENAI_API_KEY, ANTHROPIC_API_KEY)
load_dotenv()

# --- Clients ---
openai_client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)

claude_client = Anthropic(
    api_key=os.getenv("ANTHROPIC_API_KEY")
)


def ask_chatgpt(prompt: str) -> str:
    """Call ChatGPT and return text."""
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4.1-mini",  # or "gpt-4.1" if you prefer
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        # For the new OpenAI SDK, message content is a string or list.
        return resp.choices[0].message.content
    except Exception as e:
        return f"ChatGPT error: {e!r}"


def ask_claude(prompt: str) -> str:
    """Call Claude and return text."""
    try:
        resp = claude_client.messages.create(
            # Use a widely-available model ID
            model="claude-3-haiku-20240307",
            max_tokens=200,
            messages=[
                {"role": "user", "content": prompt}
            ],
        )
        # Anthropic responses come back as a list of content blocks
        return resp.content[0].text
    except Exception as e:
        return f"Claude error: {e!r}"


if __name__ == "__main__":
    print("Testing ChatGPT...")
    print(ask_chatgpt("Say hello from ChatGPT in one short sentence."))

    print("\nTesting Claude...")
    print(ask_claude("Say hello from Claude in one short sentence."))

