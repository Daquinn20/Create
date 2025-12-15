# innovation_stack_daily.py
# Daily Innovation Stack - PDF report with AI summaries and email delivery
# Run with: python innovation_stack_daily.py

import feedparser
from datetime import datetime
import pytz
import os
import json
import smtplib
import re
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, HRFlowable
from reportlab.lib.colors import HexColor
from openai import OpenAI
import anthropic

# ===== LOAD CONFIGURATION =====
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

EMAIL_CONFIG = {
    "sender_email": config.get("email_address"),
    "sender_password": config.get("password"),
    "recipient_email": config.get("email_recipient"),
    "smtp_server": config.get("smtp_server", "smtp.gmail.com"),
    "smtp_port": config.get("smtp_port", 587)
}

LOGO_PATH = config.get("logo_path", os.path.join(os.path.dirname(__file__), "company_logo.png"))
OUTPUT_DIR = os.path.dirname(__file__)

# API Keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Initialize AI clients
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None

# Original feeds (your perfect stack)
# NOTE: Benedict Evans, a16z Blog, and First Round Review feeds are currently broken/blocked
# but kept here for reference in case they become available again
FEEDS = {
    # --- Your Original Feeds ---
    "Benedict Evans":        "https://www.ben-evans.com/newsletter/rss",      # Currently broken
    "Stratechery":           "https://stratechery.com/feed/",
    "Not Boring":            "https://www.notboring.co/feed",
    "a16z Blog":             "https://a16z.com/feed/",                        # Currently broken
    "First Round Review":    "https://review.firstround.com/feed",            # Currently broken
    # --- Additional Working Feeds ---
    "Lenny's Newsletter":    "https://www.lennysnewsletter.com/feed",
    "Paul Graham":           "http://www.aaronsw.com/2002/feeds/pgessays.rss",
    "Seth Godin":            "https://seths.blog/feed/atom/",
    "MIT Tech Review":       "https://www.technologyreview.com/feed/",
    "TechCrunch":            "https://techcrunch.com/feed/",
}


def clean_html(text):
    """Remove HTML tags from text."""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def fetch_article_content(url):
    """Fetch and extract main content from an article URL."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Remove script, style, nav, footer elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            element.decompose()

        # Try to find article content
        article = soup.find('article') or soup.find('main') or soup.find('div', class_=re.compile('content|article|post'))

        if article:
            text = article.get_text(separator=' ', strip=True)
        else:
            text = soup.get_text(separator=' ', strip=True)

        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        # Limit to first 5000 characters for summarization
        return text[:5000] if len(text) > 5000 else text

    except Exception as e:
        print(f"    Error fetching article content: {e}")
        return None


def summarize_with_openai(title, content):
    """Summarize article content using OpenAI GPT-4."""
    if not openai_client or not content:
        return None

    try:
        prompt = f"""Summarize this article in ONE concise paragraph (3-5 sentences). Focus on the key insights, main argument, and actionable takeaways for a business/tech executive.

Article Title: {title}

Article Content:
{content}

Provide only the summary paragraph, no preamble."""

        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"    OpenAI error: {e}")
        return None


def summarize_with_claude(title, content):
    """Summarize article content using Claude."""
    if not anthropic_client or not content:
        return None

    try:
        prompt = f"""Summarize this article in ONE concise paragraph (3-5 sentences). Focus on the key insights, main argument, and actionable takeaways for a business/tech executive.

Article Title: {title}

Article Content:
{content}

Provide only the summary paragraph, no preamble."""

        response = anthropic_client.messages.create(
            model="claude-3-5-haiku-latest",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except Exception as e:
        print(f"    Claude error: {e}")
        return None


def get_ai_summary(title, url, use_openai_first=True):
    """Get AI summary of an article, with fallback between providers."""
    print(f"    Fetching article content...")
    content = fetch_article_content(url)

    if not content:
        return "Unable to fetch article content for summarization."

    print(f"    Generating AI summary...")

    if use_openai_first:
        summary = summarize_with_openai(title, content)
        if not summary:
            summary = summarize_with_claude(title, content)
    else:
        summary = summarize_with_claude(title, content)
        if not summary:
            summary = summarize_with_openai(title, content)

    return summary or "Summary unavailable."


def fetch_entries():
    """Fetch and return all entries from RSS feeds with AI summaries."""
    all_entries = []

    for name, url in FEEDS.items():
        print(f"Fetching {name}...")
        try:
            feed = feedparser.parse(url)

            for entry in feed.entries[:5]:  # Top 5 per source
                title = entry.title
                link = entry.link

                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    pub_date = datetime(*entry.published_parsed[:6])
                elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                    pub_date = datetime(*entry.updated_parsed[:6])
                else:
                    pub_date = datetime.now()

                pub_date = pub_date.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone("America/New_York"))

                print(f"  Processing: {title[:50]}...")

                # Get AI summary
                ai_summary = get_ai_summary(title, link)

                all_entries.append({
                    "date": pub_date,
                    "source": name,
                    "title": title,
                    "link": link,
                    "summary": ai_summary
                })
        except Exception as e:
            print(f"  Error fetching {name}: {e}")

    all_entries.sort(key=lambda x: x["date"], reverse=True)
    return all_entries  # Return all articles


def create_pdf(entries, output_path):
    """Generate a PDF report with logo and AI-summarized entries."""
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.5*inch,
        bottomMargin=0.5*inch
    )

    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=HexColor('#1a365d'),
        spaceAfter=6,
        alignment=1
    )

    date_style = ParagraphStyle(
        'DateStyle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=HexColor('#666666'),
        alignment=1,
        spaceAfter=20
    )

    source_style = ParagraphStyle(
        'SourceStyle',
        parent=styles['Heading2'],
        fontSize=10,
        textColor=HexColor('#2563eb'),
        spaceBefore=16,
        spaceAfter=2
    )

    article_title_style = ParagraphStyle(
        'ArticleTitle',
        parent=styles['Heading3'],
        fontSize=13,
        textColor=HexColor('#1e293b'),
        spaceAfter=6,
        leading=16
    )

    summary_style = ParagraphStyle(
        'SummaryStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=HexColor('#374151'),
        spaceAfter=6,
        leading=14,
        firstLineIndent=0
    )

    link_style = ParagraphStyle(
        'LinkStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=HexColor('#2563eb'),
        spaceAfter=12
    )

    story = []

    # Add logo if exists
    if os.path.exists(LOGO_PATH):
        img = Image(LOGO_PATH)
        img_width = 2.5 * inch
        aspect = img.imageHeight / img.imageWidth
        img.drawWidth = img_width
        img.drawHeight = img_width * aspect
        img.hAlign = 'CENTER'
        story.append(img)
        story.append(Spacer(1, 0.2*inch))

    # Title
    story.append(Paragraph("Daily Innovation Stack", title_style))
    story.append(Paragraph(
        f"Updated: {datetime.now().strftime('%A, %B %d, %Y at %I:%M %p')} ET",
        date_style
    ))

    # Subtitle
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=HexColor('#6b7280'),
        alignment=1,
        spaceAfter=15
    )
    story.append(Paragraph("AI-Powered Article Summaries from Top Innovation Sources", subtitle_style))

    # Horizontal line
    story.append(HRFlowable(width="100%", thickness=1, color=HexColor('#e2e8f0')))
    story.append(Spacer(1, 0.15*inch))

    # Entries
    for i, entry in enumerate(entries, 1):
        story.append(Paragraph(
            f"<b>{i}.</b> [{entry['date'].strftime('%b %d')}] <b>{entry['source']}</b>",
            source_style
        ))
        story.append(Paragraph(entry['title'], article_title_style))

        if entry['summary']:
            # Escape any problematic characters for reportlab
            safe_summary = entry['summary'].replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            story.append(Paragraph(safe_summary, summary_style))

        story.append(Paragraph(
            f"<link href='{entry['link']}'>Read full article →</link>",
            link_style
        ))

    # Footer
    story.append(Spacer(1, 0.3*inch))
    story.append(HRFlowable(width="100%", thickness=1, color=HexColor('#e2e8f0')))
    story.append(Spacer(1, 0.1*inch))

    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=9,
        textColor=HexColor('#94a3b8'),
        alignment=1
    )
    story.append(Paragraph("End of today's innovation stack. Go build something.", footer_style))

    doc.build(story)
    print(f"\nPDF created: {output_path}")
    return output_path


def send_email(pdf_path):
    """Send the PDF via email."""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_CONFIG['sender_email']
    msg['To'] = EMAIL_CONFIG['recipient_email']
    msg['Subject'] = f"Daily Innovation Stack - {datetime.now().strftime('%B %d, %Y')}"

    body = """Good morning!

Your Daily Innovation Stack is attached with AI-powered summaries of the latest articles from your curated innovation sources.

Each article includes a one-paragraph summary so you can quickly decide what's worth a deeper read.

Best,
Innovation Stack Bot
"""
    msg.attach(MIMEText(body, 'plain'))

    # Attach PDF
    with open(pdf_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())

    encoders.encode_base64(part)
    filename = os.path.basename(pdf_path)
    part.add_header('Content-Disposition', f'attachment; filename= {filename}')
    msg.attach(part)

    # Send email
    try:
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        server.send_message(msg)
        server.quit()
        print(f"Email sent successfully to {EMAIL_CONFIG['recipient_email']}")
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False


def main():
    print("=" * 60)
    print("DAILY INNOVATION STACK (AI-Powered)")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Check API keys
    if not OPENAI_API_KEY and not ANTHROPIC_API_KEY:
        print("WARNING: No AI API keys found. Summaries will be unavailable.")
    else:
        providers = []
        if OPENAI_API_KEY:
            providers.append("OpenAI")
        if ANTHROPIC_API_KEY:
            providers.append("Claude")
        print(f"AI Providers: {', '.join(providers)}")

    print()

    # Fetch entries with AI summaries
    entries = fetch_entries()

    if not entries:
        print("No entries found. Exiting.")
        return

    print(f"\nProcessed {len(entries)} articles with AI summaries.\n")

    # Create PDF
    date_str = datetime.now().strftime('%Y-%m-%d')
    pdf_filename = f"Innovation_Stack_{date_str}.pdf"
    pdf_path = os.path.join(OUTPUT_DIR, pdf_filename)

    create_pdf(entries, pdf_path)

    # Send email
    print("\nSending email...")
    send_email(pdf_path)

    print("\nDone!")


if __name__ == "__main__":
    main()
