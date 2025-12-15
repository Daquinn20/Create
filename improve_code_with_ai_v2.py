"""
AI Code Improver - IMPROVED VERSION
Saves output to a file to avoid truncation
"""

import os
from openai import OpenAI
from anthropic import Anthropic
from datetime import datetime

def read_file(filepath):
    """Read the contents of a Python file"""
    # Try different encodings to handle special characters
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']

    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue

    # If all encodings fail, read as binary and decode with errors='ignore'
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        return f.read()

def improve_with_chatgpt(code, instructions):
    """Use ChatGPT to improve the code"""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    prompt = f"""I have this Python code:

```python
{code}
```

Please improve it based on these instructions: {instructions}

Provide the complete improved code."""

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "user", "content": prompt}
        ],
        max_tokens=4096
    )

    return response.choices[0].message.content

def improve_with_claude(code, instructions):
    """Use Claude to improve the code"""
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    prompt = f"""I have this Python code:

```python
{code}
```1

Please improve it based on these instructions: {instructions}

Provide the complete improved code."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return message.content[0].text

def extract_code_from_response(response):
    """Extract code blocks from AI response"""
    # Look for code between ```python and ```
    if "```python" in response:
        start = response.find("```python") + 9
        end = response.find("```", start)
        if end != -1:
            return response[start:end].strip()

    # If no python code block, look for any code block
    if "```" in response:
        start = response.find("```") + 3
        # Skip language identifier if present
        if response[start:start+10].strip().isalpha():
            start = response.find("\n", start) + 1
        end = response.find("```", start)
        if end != -1:
            return response[start:end].strip()

    # If no code blocks, return the whole response
    return response

def main():
    print("=" * 60)
    print("AI Code Improver - Enhanced Version")
    print("=" * 60)

    # Get the file path
    filepath = input("\nEnter the path to your Python file (e.g., Growth Screen.py): ").strip()

    # Read the code
    try:
        code = read_file(filepath)
        print(f"\n✓ Successfully read {filepath}")
        print(f"  Code is {len(code)} characters long\n")
    except FileNotFoundError:
        print(f"✗ Error: Could not find file '{filepath}'")
        return

    # Get improvement instructions
    print("What would you like to improve? (in plain English)")
    print("Examples:")
    print("  - 'Add error handling'")
    print("  - 'Make it faster'")
    print("  - 'Add comments and documentation'")
    print("  - 'Fix any bugs and improve readability'")
    instructions = input("\nYour instructions: ").strip()

    # Choose AI
    print("\nWhich AI would you like to use?")
    print("1. ChatGPT (GPT-4)")
    print("2. Claude (Sonnet)")
    choice = input("Enter 1 or 2: ").strip()

    # Get improvements
    print("\n⏳ AI is analyzing and improving your code...")
    print("   This may take 30-60 seconds for large files...\n")

    try:
        if choice == "1":
            result = improve_with_chatgpt(code, instructions)
            ai_name = "ChatGPT"
        else:
            result = improve_with_claude(code, instructions)
            ai_name = "Claude"

        # Save full response to a text file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        response_file = f"ai_response_{timestamp}.txt"
        with open(response_file, 'w', encoding='utf-8') as f:
            f.write(f"{'='*60}\n")
            f.write(f"{ai_name}'s Response\n")
            f.write(f"{'='*60}\n\n")
            f.write(result)

        print(f"✓ Full response saved to: {response_file}")

        # Try to extract just the code
        improved_code = extract_code_from_response(result)

        # Save the improved code
        print("\nWould you like to save the improved code?")
        print("1. Yes, save to a new file")
        print("2. Yes, overwrite the original file")
        print("3. No, just show me a preview")
        save_choice = input("Enter 1, 2, or 3: ").strip()

        if save_choice == "1":
            output_file = input("Enter output filename (e.g., Growth Screen_improved.py): ").strip()
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(improved_code)
            print(f"\n✓ Improved code saved to: {output_file}")
            print(f"✓ Full AI response saved to: {response_file}")

        elif save_choice == "2":
            confirm = input(f"⚠️  This will overwrite {filepath}. Are you sure? (yes/no): ").strip().lower()
            if confirm == "yes":
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(improved_code)
                print(f"\n✓ Original file updated: {filepath}")
                print(f"✓ Full AI response saved to: {response_file}")
            else:
                print("\n✗ Cancelled. No files were modified.")

        else:
            # Show preview (first 50 lines)
            lines = improved_code.split('\n')
            preview_lines = min(50, len(lines))
            print("\n" + "="*60)
            print(f"Preview (first {preview_lines} lines):")
            print("="*60)
            print('\n'.join(lines[:preview_lines]))
            if len(lines) > preview_lines:
                print(f"\n... ({len(lines) - preview_lines} more lines)")
            print("\n" + "="*60)
            print(f"Full response saved to: {response_file}")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        return

if __name__ == "__main__":
    main()
