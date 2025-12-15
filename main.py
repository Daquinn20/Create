import os
import anthropic

# 1. Setup Client
api_key = os.getenv("ANTHROPIC_API_KEY")
if not api_key:
    print("Error: API Key missing.")
    exit()

client = anthropic.Anthropic(api_key=api_key)

# 2. Define Model (Newest Sonnet 4.5)
model_id = "claude-sonnet-4-5-20250929"

print(f"--- Connected to {model_id} ---")
print("Type 'quit' to exit.\n")

# 3. Chat Loop
while True:
    try:
        user_input = input("You: ")
        if user_input.lower() in ['quit', 'exit']:
            print("Goodbye!")
            break

        message = client.messages.create(
            model=model_id,
            max_tokens=1024,
            messages=[{"role": "user", "content": user_input}]
        )
        print(f"Claude: {message.content[0].text}\n")

    except Exception as e:
        print(f"Error: {e}")