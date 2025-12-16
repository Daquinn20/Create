"""
Innovation Stack Dashboard
Interactive Streamlit dashboard for AI-powered article summaries from top innovation sources
Matches the format of Innovation_stack_daily.py
"""
import streamlit as st
import feedparser
from datetime import datetime
import pytz
import os
import re
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI
import anthropic
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, HRFlowable
from reportlab.lib.colors import HexColor
import io
from pathlib import Path

load_dotenv()

st.set_page_config(
    page_title="Innovation Stack",
    page_icon="💡",
    layout="wide"
)

# API Keys
def get_api_key(key_name):
    key = os.getenv(key_name)
    if key:
        return key
    try:
        return st.secrets.get(key_name)
    except:
        return None

OPENAI_API_KEY = get_api_key("OPENAI_API_KEY")
ANTHROPIC_API_KEY = get_api_key("ANTHROPIC_API_KEY")

# RSS Feeds - same as original
FEEDS = {
    "Stratechery": "https://stratechery.com/feed/",
    "Not Boring": "https://www.notboring.co/feed",
    "Lenny's Newsletter": "https://www.lennysnewsletter.com/feed",
    "Paul Graham": "http://www.aaronsw.com/2002/feeds/pgessays.rss",
    "Seth Godin": "https://seths.blog/feed/atom/",
    "MIT Tech Review": "https://www.technologyreview.com/feed/",
    "TechCrunch": "https://techcrunch.com/feed/",
}


def fetch_article_content(url):
    """Fetch and extract main content from an article URL."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            element.decompose()

        article = soup.find('article') or soup.find('main') or soup.find('div', class_=re.compile('content|article|post'))
        text = article.get_text(separator=' ', strip=True) if article else soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:5000] if len(text) > 5000 else text
    except:
        return None


def summarize_with_claude(title, content):
    """Summarize with Claude."""
    if not ANTHROPIC_API_KEY or not content:
        return None
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt = f"""Summarize this article in ONE concise paragraph (3-5 sentences). Focus on key insights and actionable takeaways.

Article Title: {title}
Content: {content}

Provide only the summary paragraph."""

        response = client.messages.create(
            model="claude-3-5-haiku-latest",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except:
        return None


def summarize_with_openai(title, content):
    """Summarize with OpenAI."""
    if not OPENAI_API_KEY or not content:
        return None
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"""Summarize this article in ONE concise paragraph (3-5 sentences). Focus on key insights and actionable takeaways.

Article Title: {title}
Content: {content}

Provide only the summary paragraph."""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300
        )
        return response.choices[0].message.content.strip()
    except:
        return None


def get_ai_summary(title, url):
    """Get AI summary with fallback."""
    content = fetch_article_content(url)
    if not content:
        return "Unable to fetch article content."

    # Try Claude first, then OpenAI
    summary = summarize_with_claude(title, content)
    if not summary:
        summary = summarize_with_openai(title, content)

    return summary or "Summary unavailable."


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_all_entries(selected_feeds, articles_per_feed):
    """Fetch entries from all RSS feeds with AI summaries."""
    all_entries = []

    for name in selected_feeds:
        url = FEEDS.get(name)
        if not url:
            continue

        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:articles_per_feed]:
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    pub_date = datetime(*entry.published_parsed[:6])
                elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                    pub_date = datetime(*entry.updated_parsed[:6])
                else:
                    pub_date = datetime.now()

                pub_date = pub_date.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone("America/New_York"))

                # Get AI summary
                summary = get_ai_summary(entry.title, entry.link)

                all_entries.append({
                    "date": pub_date,
                    "source": name,
                    "title": entry.title,
                    "link": entry.link,
                    "summary": summary
                })
        except Exception as e:
            continue

    all_entries.sort(key=lambda x: x["date"], reverse=True)
    return all_entries


def create_pdf(entries):
    """Generate PDF matching original Innovation_stack_daily.py format."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.5*inch,
        bottomMargin=0.5*inch
    )

    styles = getSampleStyleSheet()

    # Custom styles matching original
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

    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=HexColor('#6b7280'),
        alignment=1,
        spaceAfter=15
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
        leading=14
    )

    link_style = ParagraphStyle(
        'LinkStyle',
        parent=styles['Normal'],
        fontSize=8,
        textColor=HexColor('#2563eb'),
        spaceAfter=12
    )

    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=9,
        textColor=HexColor('#94a3b8'),
        alignment=1
    )

    story = []

    # Add logo if exists
    logo_path = Path(__file__).parent / "company_logo.png"
    if logo_path.exists():
        try:
            img = Image(str(logo_path))
            img_width = 2.5 * inch
            aspect = img.imageHeight / img.imageWidth
            img.drawWidth = img_width
            img.drawHeight = img_width * aspect
            img.hAlign = 'CENTER'
            story.append(img)
            story.append(Spacer(1, 0.2*inch))
        except:
            pass

    # Title
    story.append(Paragraph("Daily Innovation Stack", title_style))
    story.append(Paragraph(
        f"Updated: {datetime.now().strftime('%A, %B %d, %Y at %I:%M %p')} ET",
        date_style
    ))
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
    story.append(Paragraph("End of today's innovation stack. Go build something.", footer_style))

    doc.build(story)
    buffer.seek(0)
    return buffer


# ============ MAIN APP ============
st.title("💡 Daily Innovation Stack")
st.markdown("*AI-Powered Article Summaries from Top Innovation Sources*")

# Check API keys
if not OPENAI_API_KEY and not ANTHROPIC_API_KEY:
    st.error("Missing API keys. Add OPENAI_API_KEY or ANTHROPIC_API_KEY to your secrets.")
    st.stop()

# Sidebar settings
st.sidebar.header("Settings")
selected_feeds = st.sidebar.multiselect(
    "Select Sources",
    options=list(FEEDS.keys()),
    default=list(FEEDS.keys())
)
articles_per_feed = st.sidebar.slider("Articles per source", 1, 5, 3)

if st.sidebar.button("🔄 Refresh Data", type="primary"):
    st.cache_data.clear()
    st.rerun()

# Auto-fetch and display
if selected_feeds:
    with st.spinner("Fetching articles and generating AI summaries... This may take a minute."):
        entries = fetch_all_entries(tuple(selected_feeds), articles_per_feed)

    if entries:
        st.success(f"Processed {len(entries)} articles with AI summaries")

        # Display entries
        st.header("📰 Today's Innovation Stack")

        for i, entry in enumerate(entries, 1):
            with st.expander(f"**{i}. [{entry['source']}]** {entry['title']}", expanded=(i <= 5)):
                st.caption(f"📅 {entry['date'].strftime('%B %d, %Y at %I:%M %p')} ET")
                st.write("---")
                st.write(entry['summary'])
                st.markdown(f"[Read full article →]({entry['link']})")

        # Download PDF
        st.divider()
        st.subheader("📥 Download Innovation Stack")

        pdf = create_pdf(entries)
        st.download_button(
            "📄 Download PDF",
            pdf,
            file_name=f"Innovation_Stack_{datetime.now().strftime('%Y-%m-%d')}.pdf",
            mime="application/pdf"
        )
    else:
        st.warning("No articles found. Try selecting different sources.")
else:
    st.info("👈 Select sources from the sidebar to generate your Innovation Stack")

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; color: #94a3b8; padding: 1rem;">
    <p><i>End of today's innovation stack. Go build something.</i></p>
</div>
""", unsafe_allow_html=True)
