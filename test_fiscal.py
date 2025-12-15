import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Try to get the key
fiscal_key = os.getenv('FISCAL_API_KEY')

print("=" * 50)
print("TESTING FISCAL.AI API KEY")
print("=" * 50)

if fiscal_key:
    print(f"✅ API Key Found!")
    print(f"📋 Key starts with: {fiscal_key[:15]}...")
    print(f"📏 Key length: {len(fiscal_key)} characters")
else:
    print("❌ API Key NOT Found!")
    print("⚠️  Check your .env file")

print("=" * 50)
```

**Run it!** (Green play button)

---

## **What You Should See:** ✅
```
==================================================
TESTING FISCAL.AI API KEY
==================================================
✅ API Key Found!
📋 Key starts with: sk-fiscal-abc1...
📏 Key length: 45 characters
==================================================