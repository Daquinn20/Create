from anthropic import Anthropic

client = Anthropic(api_key="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")

response = client.messages.create(
    model="claude-3-haiku-20240307",
    max_tokens=300,
    messages=[
        {"role": "user", "content": "Write a 2 sentence summary of the S&P 500."}
    ]
)

print(response.content[0].text)



