#!/usr/bin/env python3
"""
Daily Note Generator - Fetches emails and creates professional daily summaries
"""

import imaplib
import email
from email.header import decode_header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
import json
import os
import re
import smtplib
from bs4 import BeautifulSoup
from pathlib import Path
from anthropic import Anthropic
from openai import OpenAI
import yfinance as yf
import requests
import pandas as pd
import pytz
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from dotenv import load_dotenv


class DailyNoteGenerator:
    def __init__(self, config_path='config.json'):
        """Initialize with configuration file and environment variables"""
        # Load environment variables from .env file
        load_dotenv()

        with open(config_path, 'r') as f:
            self.config = json.load(f)

        # Override config with environment variables (env takes priority)
        self.config['FMP_API_KEY'] = os.getenv('FMP_API_KEY', self.config.get('FMP_API_KEY', ''))
        self.config['OPENAI_API_KEY'] = os.getenv('OPENAI_API_KEY', self.config.get('OPENAI_API_KEY', ''))
        self.config['ANTHROPIC_API_KEY'] = os.getenv('ANTHROPIC_API_KEY', self.config.get('ANTHROPIC_API_KEY', ''))
        self.config['NEWSAPI_KEY'] = os.getenv('NEWSAPI_KEY', self.config.get('NEWSAPI_KEY', ''))
        self.config['ALPHAVANTAGE_API_KEY'] = os.getenv('ALPHAVANTAGE_API_KEY', self.config.get('ALPHAVANTAGE_API_KEY', ''))
        self.config['POLYGON_API_KEY'] = os.getenv('POLYGON_API_KEY', self.config.get('POLYGON_API_KEY', ''))

        self.imap_server = self.config.get('imap_server', 'imap.gmail.com')
        self.email_address = self.config['email_address']
        self.password = self.config['password']
        self.target_senders = self.config['target_senders']
        self.output_dir = self.config.get('output_dir', '.')

    def connect_to_gmail(self):
        """Connect to Gmail via IMAP"""
        print(f"Connecting to {self.imap_server}...")
        mail = imaplib.IMAP4_SSL(self.imap_server)
        mail.login(self.email_address, self.password)
        mail.select('inbox')
        return mail

    def clean_html(self, html_content):
        """Convert HTML to clean text"""
        if not html_content:
            return ""

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script, style, meta, and other non-content elements
            for element in soup(["script", "style", "meta", "link", "noscript", "head"]):
                element.decompose()

            # Get text and clean it up
            text = soup.get_text(separator=' ', strip=True)

            # Clean up whitespace
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            text = '\n'.join(lines)

            # Remove excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'\n\s*\n', '\n', text)

            return text.strip()
        except Exception as e:
            print(f"Error cleaning HTML: {e}")
            return html_content[:500]  # Return first 500 chars as fallback

    def decode_email_subject(self, subject):
        """Decode email subject"""
        if subject is None:
            return "No Subject"
        decoded_parts = decode_header(subject)
        subject_text = ""
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                subject_text += part.decode(encoding or 'utf-8', errors='ignore')
            else:
                subject_text += part
        return subject_text

    def extract_email_body(self, msg):
        """Extract the email body (prefer text, fallback to HTML)"""
        text_body = ""
        html_body = ""

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))

                if "attachment" not in content_disposition:
                    if content_type == "text/plain":
                        try:
                            text_body = part.get_payload(decode=True).decode('utf-8', errors='ignore').strip()
                        except:
                            pass
                    elif content_type == "text/html":
                        try:
                            html_body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                        except:
                            pass
        else:
            content_type = msg.get_content_type()
            try:
                payload = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
                if content_type == "text/plain":
                    text_body = payload.strip()
                elif content_type == "text/html":
                    html_body = payload
            except:
                pass

        # Check if text_body actually contains HTML tags
        if text_body and ('<!DOCTYPE' in text_body or '<html' in text_body or '<HTML' in text_body):
            # Text part contains HTML, clean it
            return self.clean_html(text_body)
        # Prefer text, but if text is empty/short, use cleaned HTML
        elif text_body and len(text_body) > 50:
            return text_body
        elif html_body:
            return self.clean_html(html_body)
        elif text_body:
            return text_body
        else:
            return "No content available"

    def summarize_with_ai(self, subject, body, sender):
        """Use AI to generate a concise summary of the newsletter content"""
        use_ai = self.config.get('use_ai_summary', True)
        ai_provider = self.config.get('ai_provider', 'anthropic')  # 'anthropic' or 'openai'

        if not use_ai:
            # Return first 500 chars if AI is disabled
            return body[:500] + "..." if len(body) > 500 else body

        try:
            # Prepare the prompt
            prompt = f"""Please provide a concise, professional summary of this newsletter email. Focus on:
1. Key headlines or main topics
2. Important data points, stock movements, or financial metrics
3. Actionable insights or recommendations

Newsletter: {sender}
Subject: {subject}

Content:
{body[:4000]}

Provide a summary in 2-4 bullet points, maximum 200 words."""

            if ai_provider == 'anthropic':
                return self._summarize_with_anthropic(prompt)
            elif ai_provider == 'openai':
                return self._summarize_with_openai(prompt)
            else:
                print(f"Unknown AI provider: {ai_provider}, using raw content")
                return body[:500] + "..." if len(body) > 500 else body

        except Exception as e:
            print(f"Error generating AI summary: {str(e)}")
            # Fallback to raw content if AI fails
            return body[:500] + "..." if len(body) > 500 else body

    def _summarize_with_anthropic(self, prompt):
        """Generate summary using Anthropic Claude"""
        api_key = self.config.get('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in config")

        client = Anthropic(api_key=api_key)

        message = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=300,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        return message.content[0].text

    def _summarize_with_openai(self, prompt):
        """Generate summary using OpenAI GPT"""
        api_key = self.config.get('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in config")

        client = OpenAI(api_key=api_key)

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=300,
            messages=[
                {"role": "system", "content": "You are a professional financial newsletter summarizer."},
                {"role": "user", "content": prompt}
            ]
        )

        return response.choices[0].message.content

    def fetch_global_markets_data(self):
        """Fetch global markets overview using FMP API"""
        print("Fetching global markets data from FMP...")

        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            print("FMP_API_KEY not found in config")
            return None

        markets_data = {
            'indices': {},
            'fx': {},
            'commodities': {},
            'crypto': {},
            'treasuries': {}
        }

        # Define futures symbols for yfinance
        indices_symbols = {
            'S&P 500': 'ES=F',
            'NASDAQ': 'NQ=F',
            'Russell 2000': 'RTY=F',
            'Nikkei 225': 'NIY=F'
        }

        fx_symbols = {
            'EUR/USD': 'EURUSD',
            'USD/JPY': 'USDJPY',
            'USD/CNY': 'USDCNY'
        }

        commodity_symbols = {
            'WTI Crude': 'CLUSD',
            'Gold': 'GCUSD',
            'Copper': 'HGUSD',
            'Lumber': 'LBSUSD',
            'Corn': 'ZCUSD'
        }

        crypto_symbols = {
            'Bitcoin': 'BTCUSD',
            'Ethereum': 'ETHUSD',
            'Solana': 'SOLUSD',
            'Sui': 'SUIUSD'
        }

        # For treasuries, we'll use yfinance as FMP doesn't have good treasury data
        treasury_tickers = {
            'US 30Y': '^TYX',
            'US 10Y': '^TNX',
            'US 2Y': '^IRX',  # Using 3M as proxy for 2Y
            'US 3M': '^IRX'
        }

        try:
            # Fetch indices futures using yfinance
            for name, symbol in indices_symbols.items():
                markets_data['indices'][name] = self._get_ticker_data(symbol)

            # Fetch FX
            for name, symbol in fx_symbols.items():
                markets_data['fx'][name] = self._get_fmp_forex_quote(symbol, api_key)

            # Fetch commodities
            for name, symbol in commodity_symbols.items():
                markets_data['commodities'][name] = self._get_fmp_quote(symbol, api_key)

            # Fetch crypto
            for name, symbol in crypto_symbols.items():
                markets_data['crypto'][name] = self._get_fmp_crypto_quote(symbol, api_key)

            # Fetch treasuries using yfinance
            for name, ticker in treasury_tickers.items():
                markets_data['treasuries'][name] = self._get_ticker_data(ticker)

        except Exception as e:
            print(f"Error fetching global markets data: {str(e)}")

        return markets_data

    def _get_fmp_quote(self, symbol, api_key):
        """Get quote from FMP for indices and commodities"""
        try:
            url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={api_key}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    quote = data[0]
                    return {
                        'price': quote.get('price', 0),
                        'change': quote.get('change', 0),
                        'change_pct': quote.get('changesPercentage', 0)
                    }
        except Exception as e:
            print(f"Error fetching FMP quote for {symbol}: {str(e)}")

        return {'price': None, 'change': None, 'change_pct': None}

    def _get_fmp_forex_quote(self, symbol, api_key):
        """Get forex quote from FMP"""
        try:
            url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={api_key}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    quote = data[0]
                    return {
                        'price': quote.get('price', 0),
                        'change': quote.get('change', 0),
                        'change_pct': quote.get('changesPercentage', 0)
                    }
        except Exception as e:
            print(f"Error fetching FMP forex for {symbol}: {str(e)}")

        return {'price': None, 'change': None, 'change_pct': None}

    def _get_fmp_crypto_quote(self, symbol, api_key):
        """Get crypto quote from FMP"""
        try:
            url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={api_key}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    quote = data[0]
                    return {
                        'price': quote.get('price', 0),
                        'change': quote.get('change', 0),
                        'change_pct': quote.get('changesPercentage', 0)
                    }
        except Exception as e:
            print(f"Error fetching FMP crypto for {symbol}: {str(e)}")

        return {'price': None, 'change': None, 'change_pct': None}

    def _get_ticker_data(self, ticker):
        """Get current price and change for a ticker using yfinance (for futures/treasuries)"""
        try:
            t = yf.Ticker(ticker)
            # Use 5d to ensure we get data even over weekends
            hist = t.history(period='5d')

            if len(hist) >= 2:
                current = hist['Close'].iloc[-1]
                previous = hist['Close'].iloc[-2]
                change = current - previous
                change_pct = (change / previous) * 100 if previous != 0 else 0

                return {
                    'price': current,
                    'change': change,
                    'change_pct': change_pct
                }
            elif len(hist) == 1:
                current = hist['Close'].iloc[-1]
                return {
                    'price': current,
                    'change': 0,
                    'change_pct': 0
                }
        except Exception as e:
            print(f"Error fetching {ticker}: {str(e)}")

        return {'price': None, 'change': None, 'change_pct': None}

    def format_global_markets(self, markets_data):
        """Format global markets data with consolidated main table plus separate FX and Crypto tables"""
        output = "## Global Markets\n\n"

        # Main table with Category, Item, Price/Rate, Yield, Daily Δ%
        output += "| Category | Item | Price/Rate | Yield | Daily Δ% |\n"
        output += "|:---------|:-----|----------:|------:|---------:|\n"

        # Index Futures
        first_index = True
        for name, data in markets_data['indices'].items():
            if data['price']:
                category = "Index Futures" if first_index else ""
                output += f"| {category} | {name} | {data['price']:,.2f} | - | {data['change_pct']:+.2f}% |\n"
                first_index = False

        # Fixed Income (Treasuries)
        first_treasury = True
        for name, data in markets_data['treasuries'].items():
            if data['price']:
                category = "Fixed Income" if first_treasury else ""
                output += f"| {category} | {name} | - | {data['price']:.2f}% | {data['change_pct']:+.2f}% |\n"
                first_treasury = False

        # Commodities
        first_commodity = True
        for name, data in markets_data['commodities'].items():
            if data['price']:
                category = "Commodities" if first_commodity else ""
                output += f"| {category} | {name} | ${data['price']:,.2f} | - | {data['change_pct']:+.2f}% |\n"
                first_commodity = False

        output += "\n"

        # Combined FX and Crypto table
        output += "| Category | Item | Price/Rate | Daily Δ% |\n"
        output += "|:---------|:-----|----------:|---------:|\n"

        # Foreign Exchange
        first_fx = True
        for name, data in markets_data['fx'].items():
            if data['price']:
                category = "Foreign Exchange" if first_fx else ""
                output += f"| {category} | {name} | {data['price']:.4f} | {data['change_pct']:+.2f}% |\n"
                first_fx = False

        # Cryptocurrency
        first_crypto = True
        for name, data in markets_data['crypto'].items():
            if data['price']:
                category = "Crypto Currencies" if first_crypto else ""
                output += f"| {category} | {name} | ${data['price']:,.2f} | {data['change_pct']:+.2f}% |\n"
                first_crypto = False

        output += "\n---\n\n"
        return output

    def fetch_market_news(self):
        """Fetch market-moving news from multiple sources"""
        news_items = []

        try:
            # Fetch SEC 8-K filings FIRST - official M&A source
            print("Fetching SEC 8-K filings for M&A...")
            sec_news = self._fetch_sec_8k_filings()
            news_items.extend(sec_news)

            # Fetch M&A news from FMP - highest priority
            print("Fetching M&A news from FMP...")
            ma_news = self._fetch_ma_news()
            news_items.extend(ma_news)

            # Fetch from Google News - fastest breaking news
            print("Fetching Google News...")
            google_news = self._fetch_google_news()
            news_items.extend(google_news)

            # Fetch from Benzinga - fast pre-market news
            print("Fetching Benzinga news...")
            benzinga_news = self._fetch_benzinga_news()
            news_items.extend(benzinga_news)

            # Fetch from NewsAPI - aggregates 80+ sources
            print("Fetching NewsAPI headlines...")
            newsapi_news = self._fetch_newsapi()
            news_items.extend(newsapi_news)

            # Fetch from Alpha Vantage
            print("Fetching Alpha Vantage news...")
            alphavantage_news = self._fetch_alphavantage_news()
            news_items.extend(alphavantage_news)

            # Fetch from Polygon.io
            print("Fetching Polygon.io news...")
            polygon_news = self._fetch_polygon_news()
            news_items.extend(polygon_news)

            # Fetch from Yahoo Finance
            print("Fetching market news from Yahoo Finance...")
            yahoo_news = self._fetch_yahoo_finance_news()
            news_items.extend(yahoo_news)

            # Fetch from FMP
            print("Fetching market news from FMP...")
            fmp_news = self._fetch_fmp_news()
            news_items.extend(fmp_news)

            # Fetch from Reuters
            print("Fetching headlines from Reuters...")
            reuters_news = self._fetch_reuters_news()
            news_items.extend(reuters_news)

            # Fetch from Seeking Alpha (via FMP)
            print("Fetching headlines from Seeking Alpha...")
            sa_news = self._fetch_seeking_alpha_news()
            news_items.extend(sa_news)

            # Fetch from Bloomberg (via FMP)
            print("Fetching headlines from Bloomberg...")
            bloomberg_news = self._fetch_bloomberg_news()
            news_items.extend(bloomberg_news)

            # Fetch press releases for M&A announcements
            print("Fetching press releases...")
            press_releases = self._fetch_press_releases()
            news_items.extend(press_releases)

            print(f"Found {len(news_items)} market news items")

        except Exception as e:
            print(f"Error fetching market news: {str(e)}")

        return news_items

    def _fetch_yahoo_finance_news(self):
        """Fetch news from Yahoo Finance for major indices"""
        news_items = []
        tickers = ['^GSPC', '^DJI', '^IXIC']  # S&P 500, Dow Jones, NASDAQ

        try:
            for ticker_symbol in tickers:
                ticker = yf.Ticker(ticker_symbol)
                ticker_news = ticker.news

                for item in ticker_news[:3]:  # Top 3 per index
                    news_items.append({
                        'title': item.get('title', 'No title'),
                        'publisher': item.get('publisher', 'Yahoo Finance'),
                        'link': item.get('link', ''),
                        'source': 'Yahoo Finance'
                    })
        except Exception as e:
            print(f"Error fetching Yahoo Finance news: {str(e)}")

        return news_items[:5]  # Return top 5

    def _fetch_fmp_news(self):
        """Fetch general market news from FMP API"""
        news_items = []
        api_key = self.config.get('FMP_API_KEY')

        if not api_key:
            print("FMP_API_KEY not found in config")
            return news_items

        try:
            # Get general news
            url = f"https://financialmodelingprep.com/api/v3/fmp/articles?page=0&size=5&apikey={api_key}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                for item in data.get('content', []):
                    news_items.append({
                        'title': item.get('title', 'No title'),
                        'publisher': 'FMP',
                        'link': item.get('url', ''),
                        'source': 'FMP',
                        'content': item.get('content', '')[:500]
                    })
        except Exception as e:
            print(f"Error fetching FMP news: {str(e)}")

        return news_items[:5]  # Return top 5

    def _fetch_reuters_news(self):
        """Fetch news from Reuters Markets and World/Geopolitics sections"""
        news_items = []
        ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'buy', 'deal', 'billion']

        try:
            import feedparser

            # Reuters RSS feeds for Markets and World news
            feeds = [
                ('https://www.reuters.com/markets/?format=rss', 'Reuters Markets'),
                ('https://www.reuters.com/world/?format=rss', 'Reuters World'),
                ('https://www.reuters.com/business/?format=rss', 'Reuters Business'),
                ('https://www.reutersagency.com/feed/?taxonomy=best-topics&post_type=best', 'Reuters Agency')
            ]

            for feed_url, source_name in feeds:
                try:
                    feed = feedparser.parse(feed_url)
                    for entry in feed.entries[:5]:
                        title = entry.get('title', 'No title')
                        is_ma = any(kw in title.lower() for kw in ma_keywords)
                        news_items.append({
                            'title': title,
                            'publisher': source_name,
                            'link': entry.get('link', ''),
                            'source': 'M&A NEWS' if is_ma else source_name,
                            'is_ma': is_ma
                        })
                except Exception as e:
                    continue

        except Exception as e:
            print(f"Error fetching Reuters news: {str(e)}")
        return news_items[:15]

    def _fetch_seeking_alpha_news(self):
        """Fetch Seeking Alpha news via FMP"""
        news_items = []
        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            return news_items

        try:
            url = f"https://financialmodelingprep.com/api/v4/general_news?page=0&apikey={api_key}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                for item in data[:5]:
                    news_items.append({
                        'title': item.get('title', 'No title'),
                        'publisher': item.get('site', 'Seeking Alpha'),
                        'link': item.get('url', ''),
                        'source': 'Market News'
                    })
        except Exception as e:
            print(f"Error fetching Seeking Alpha news: {str(e)}")
        return news_items

    def _fetch_bloomberg_news(self):
        """Fetch Bloomberg-style market news via FMP press releases"""
        news_items = []
        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            return news_items

        try:
            # Get stock market news - increased limit to catch more M&A
            url = f"https://financialmodelingprep.com/api/v3/stock_news?limit=50&apikey={api_key}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'buy ', 'buying', 'deal', 'billion']

                # First add any M&A related news with priority
                for item in data:
                    title = item.get('title', '').lower()
                    if any(kw in title for kw in ma_keywords):
                        news_items.append({
                            'title': item.get('title', 'No title'),
                            'publisher': item.get('site', 'Market News'),
                            'link': item.get('url', ''),
                            'source': 'M&A NEWS',
                            'is_ma': True
                        })

                # Then add regular news
                for item in data[:10]:
                    title = item.get('title', '').lower()
                    if not any(kw in title for kw in ma_keywords):  # Avoid duplicates
                        news_items.append({
                            'title': item.get('title', 'No title'),
                            'publisher': item.get('site', 'Market News'),
                            'link': item.get('url', ''),
                            'source': 'Financial News'
                        })
        except Exception as e:
            print(f"Error fetching market news: {str(e)}")
        return news_items[:15]

    def _fetch_ma_news(self):
        """Fetch M&A (Mergers & Acquisitions) news from FMP"""
        news_items = []
        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            return news_items

        try:
            # FMP Mergers & Acquisitions RSS feed
            url = f"https://financialmodelingprep.com/api/v4/mergers-acquisitions-rss-feed?page=0&apikey={api_key}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                for item in data[:10]:  # Get top 10 M&A news
                    news_items.append({
                        'title': f"M&A: {item.get('companyName', '')} - {item.get('targetedCompanyName', '')} ({item.get('transactionType', 'Deal')})",
                        'publisher': 'FMP M&A',
                        'link': item.get('link', ''),
                        'source': 'M&A NEWS',
                        'is_ma': True
                    })

            # Also check ticker-specific news for major tech companies that often do M&A
            ma_tickers = ['IBM', 'MSFT', 'GOOGL', 'AAPL', 'META', 'AMZN', 'ORCL', 'CRM', 'ADBE', 'CSCO', 'INTC', 'AMD', 'NVDA', 'AVGO']
            ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'buy', 'deal', 'billion', 'purchase']

            for ticker in ma_tickers:
                ticker_url = f"https://financialmodelingprep.com/api/v3/stock_news?tickers={ticker}&limit=5&apikey={api_key}"
                try:
                    ticker_response = requests.get(ticker_url, timeout=5)
                    if ticker_response.status_code == 200:
                        ticker_data = ticker_response.json()
                        for item in ticker_data:
                            title = item.get('title', '').lower()
                            if any(kw in title for kw in ma_keywords):
                                news_items.append({
                                    'title': item.get('title', 'No title'),
                                    'publisher': item.get('site', 'M&A News'),
                                    'link': item.get('url', ''),
                                    'source': 'M&A NEWS',
                                    'is_ma': True
                                })
                except:
                    continue

        except Exception as e:
            print(f"Error fetching M&A news: {str(e)}")

        # Remove duplicates based on title
        seen_titles = set()
        unique_news = []
        for item in news_items:
            title_key = item['title'][:50].lower()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_news.append(item)

        return unique_news[:15]

    def _fetch_press_releases(self):
        """Fetch press releases which often contain M&A announcements"""
        news_items = []
        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            return news_items

        try:
            # FMP Press Releases endpoint
            url = f"https://financialmodelingprep.com/api/v3/press-releases?limit=20&apikey={api_key}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Filter for M&A related press releases
                ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'purchase', 'deal', 'transaction', 'combine', 'join']
                for item in data:
                    title = item.get('title', '').lower()
                    text = item.get('text', '').lower()
                    # Check if M&A related
                    if any(kw in title or kw in text for kw in ma_keywords):
                        news_items.append({
                            'title': item.get('title', 'No title'),
                            'publisher': item.get('symbol', 'Press Release'),
                            'link': '',
                            'source': 'Press Release',
                            'is_ma': True
                        })
        except Exception as e:
            print(f"Error fetching press releases: {str(e)}")

        return news_items[:10]  # Return top 10 M&A-related

    def _fetch_google_news(self):
        """Fetch breaking news from Google News RSS - fastest source for breaking news"""
        news_items = []
        ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'buy', 'deal', 'billion', 'purchase']

        try:
            import feedparser

            # Google News RSS feeds for business/finance
            feeds = [
                'https://news.google.com/rss/search?q=stock+market+merger+acquisition&hl=en-US&gl=US&ceid=US:en',
                'https://news.google.com/rss/search?q=M%26A+deal+announcement&hl=en-US&gl=US&ceid=US:en',
                'https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB?hl=en-US&gl=US&ceid=US:en'  # Business
            ]

            for feed_url in feeds:
                try:
                    feed = feedparser.parse(feed_url)
                    for entry in feed.entries[:10]:
                        title = entry.get('title', '')
                        # Check if M&A related
                        is_ma = any(kw in title.lower() for kw in ma_keywords)
                        news_items.append({
                            'title': title,
                            'publisher': entry.get('source', {}).get('title', 'Google News'),
                            'link': entry.get('link', ''),
                            'source': 'M&A NEWS' if is_ma else 'Google News',
                            'is_ma': is_ma
                        })
                except Exception as e:
                    continue

        except Exception as e:
            print(f"Error fetching Google News: {str(e)}")

        return news_items[:15]

    def _fetch_sec_8k_filings(self):
        """Fetch SEC 8-K filings - official source for M&A announcements"""
        news_items = []

        try:
            import feedparser

            # SEC EDGAR RSS feeds for recent filings
            feeds = [
                'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&company=&dateb=&owner=include&count=40&output=atom',
                'https://www.sec.gov/rss/news/press.xml'  # SEC press releases
            ]

            ma_keywords = ['acqui', 'merger', 'agreement', 'purchase', 'business combination', 'asset purchase', 'definitive', 'transaction']

            for feed_url in feeds:
                try:
                    feed = feedparser.parse(feed_url)
                    for entry in feed.entries[:30]:
                        title = entry.get('title', '')
                        summary = entry.get('summary', entry.get('description', ''))
                        combined_text = (title + ' ' + str(summary)).lower()

                        # Check if M&A related
                        if any(kw in combined_text for kw in ma_keywords):
                            # Extract company name from title
                            company = title.split(' - ')[0] if ' - ' in title else title[:60]
                            news_items.append({
                                'title': f"SEC Filing: {company}",
                                'publisher': 'SEC EDGAR',
                                'link': entry.get('link', ''),
                                'source': 'M&A NEWS',
                                'is_ma': True
                            })
                except Exception as e:
                    continue

        except Exception as e:
            print(f"Error fetching SEC 8-K filings: {str(e)}")

        return news_items[:10]

    def _fetch_benzinga_news(self):
        """Fetch news from Benzinga - known for fast pre-market news"""
        news_items = []
        api_key = self.config.get('BENZINGA_API_KEY')

        # Benzinga requires API key, try RSS as fallback
        try:
            import feedparser

            # Benzinga RSS feeds
            feeds = [
                'https://www.benzinga.com/feed',
                'https://www.benzinga.com/topic/m-a/feed'
            ]

            ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'buy', 'deal', 'billion']

            for feed_url in feeds:
                try:
                    feed = feedparser.parse(feed_url)
                    for entry in feed.entries[:10]:
                        title = entry.get('title', '')
                        is_ma = any(kw in title.lower() for kw in ma_keywords)
                        news_items.append({
                            'title': title,
                            'publisher': 'Benzinga',
                            'link': entry.get('link', ''),
                            'source': 'M&A NEWS' if is_ma else 'Benzinga',
                            'is_ma': is_ma
                        })
                except:
                    continue

        except Exception as e:
            print(f"Error fetching Benzinga news: {str(e)}")

        return news_items[:10]

    def _fetch_newsapi(self):
        """Fetch from NewsAPI.org - aggregates 80+ sources including WSJ, Bloomberg, CNBC"""
        news_items = []
        api_key = self.config.get('NEWSAPI_KEY')

        if not api_key:
            # Try without API key using top headlines
            return news_items

        try:
            ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'buy', 'deal', 'billion']

            # Search for M&A news
            url = f"https://newsapi.org/v2/everything?q=merger+OR+acquisition+OR+buyout&language=en&sortBy=publishedAt&pageSize=20&apiKey={api_key}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                for article in data.get('articles', []):
                    title = article.get('title', '')
                    is_ma = any(kw in title.lower() for kw in ma_keywords)
                    news_items.append({
                        'title': title,
                        'publisher': article.get('source', {}).get('name', 'NewsAPI'),
                        'link': article.get('url', ''),
                        'source': 'M&A NEWS' if is_ma else 'NewsAPI',
                        'is_ma': is_ma
                    })

            # Also fetch top business headlines
            url2 = f"https://newsapi.org/v2/top-headlines?category=business&country=us&pageSize=10&apiKey={api_key}"
            response2 = requests.get(url2, timeout=10)

            if response2.status_code == 200:
                data2 = response2.json()
                for article in data2.get('articles', []):
                    title = article.get('title', '')
                    is_ma = any(kw in title.lower() for kw in ma_keywords)
                    news_items.append({
                        'title': title,
                        'publisher': article.get('source', {}).get('name', 'NewsAPI'),
                        'link': article.get('url', ''),
                        'source': 'M&A NEWS' if is_ma else 'Business News',
                        'is_ma': is_ma
                    })

        except Exception as e:
            print(f"Error fetching NewsAPI: {str(e)}")

        return news_items[:15]

    def _fetch_alphavantage_news(self):
        """Fetch from Alpha Vantage News API"""
        news_items = []
        api_key = self.config.get('ALPHAVANTAGE_API_KEY')

        if not api_key:
            return news_items

        try:
            ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'buy', 'deal', 'billion']

            # Alpha Vantage news sentiment endpoint
            url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics=mergers_and_acquisitions&apikey={api_key}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                for article in data.get('feed', [])[:15]:
                    title = article.get('title', '')
                    is_ma = any(kw in title.lower() for kw in ma_keywords)
                    news_items.append({
                        'title': title,
                        'publisher': article.get('source', 'Alpha Vantage'),
                        'link': article.get('url', ''),
                        'source': 'M&A NEWS' if is_ma else 'Alpha Vantage',
                        'is_ma': is_ma
                    })

        except Exception as e:
            print(f"Error fetching Alpha Vantage news: {str(e)}")

        return news_items[:10]

    def _fetch_polygon_news(self):
        """Fetch from Polygon.io news API"""
        news_items = []
        api_key = self.config.get('POLYGON_API_KEY')

        if not api_key:
            return news_items

        try:
            ma_keywords = ['acqui', 'merger', 'buyout', 'takeover', 'buy', 'deal', 'billion']

            # Polygon news endpoint
            url = f"https://api.polygon.io/v2/reference/news?limit=20&apiKey={api_key}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                for article in data.get('results', []):
                    title = article.get('title', '')
                    is_ma = any(kw in title.lower() for kw in ma_keywords)
                    news_items.append({
                        'title': title,
                        'publisher': article.get('publisher', {}).get('name', 'Polygon'),
                        'link': article.get('article_url', ''),
                        'source': 'M&A NEWS' if is_ma else 'Polygon',
                        'is_ma': is_ma
                    })

        except Exception as e:
            print(f"Error fetching Polygon news: {str(e)}")

        return news_items[:10]

    def fetch_premarket_movers(self, portfolio_tickers=None):
        """Fetch pre-market movers from S&P 500 + Disruption Index

        Calculates: (premarket_price - last_trading_day_close) / last_trading_day_close
        Only includes stocks with >3% change
        Uses actual pre-market trade prices (not quote endpoint which may show stale prices)
        """
        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            print("FMP_API_KEY not found in config")
            return []

        try:
            print("Fetching pre-market movers (>3% change)...")

            movers = []
            seen_symbols = set()

            # Step 1: Fetch S&P 500 constituents
            print("Fetching S&P 500 constituents...")
            sp500_symbols = []
            try:
                sp500_url = f"https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={api_key}"
                response = requests.get(sp500_url, timeout=10)
                if response.status_code == 200:
                    sp500_stocks = response.json()
                    sp500_symbols = [stock['symbol'] for stock in sp500_stocks]
                    print(f"Got {len(sp500_symbols)} S&P 500 symbols")
            except Exception as e:
                print(f"Error fetching S&P 500 list: {e}")

            # Combine S&P 500 + Disruption Index symbols
            all_symbols = list(set(sp500_symbols + (portfolio_tickers or [])))
            print(f"Total symbols to check: {len(all_symbols)}")

            # Step 2: Get stock names from quote endpoint
            print("Fetching stock names...")
            stock_names = {}  # symbol -> name

            for i in range(0, len(all_symbols), 50):
                batch = all_symbols[i:i+50]
                symbols_str = ','.join(batch)
                try:
                    quote_url = f"https://financialmodelingprep.com/api/v3/quote/{symbols_str}?apikey={api_key}"
                    response = requests.get(quote_url, timeout=15)
                    if response.status_code == 200:
                        for item in response.json():
                            symbol = item.get('symbol', '')
                            name = item.get('name', symbol)
                            if symbol:
                                stock_names[symbol] = name
                except Exception as e:
                    pass

            print(f"Got names for {len(stock_names)} symbols")

            # Step 3: Get ACTUAL pre-market prices from batch-pre-post-market-trade endpoint
            print("Fetching actual pre-market trade prices...")
            premarket_prices = {}  # symbol -> premarket_price

            for i in range(0, len(all_symbols), 50):
                batch = all_symbols[i:i+50]
                symbols_str = ','.join(batch)
                try:
                    premarket_url = f"https://financialmodelingprep.com/api/v4/batch-pre-post-market-trade/{symbols_str}?apikey={api_key}"
                    response = requests.get(premarket_url, timeout=15)
                    if response.status_code == 200:
                        data = response.json()
                        if data and isinstance(data, list):
                            for trade in data:
                                symbol = trade.get('symbol', '')
                                price = trade.get('price', 0)
                                if symbol and price > 0:
                                    premarket_prices[symbol] = price
                except Exception as e:
                    print(f"Error fetching pre-market batch: {e}")

            print(f"Got pre-market prices for {len(premarket_prices)} symbols")

            # Step 4: Get quote data with previousClose for initial filtering
            print("Fetching quote data for previous closes...")
            quote_prev_closes = {}  # symbol -> previousClose from quote endpoint

            symbols_with_premarket = list(premarket_prices.keys())
            for i in range(0, len(symbols_with_premarket), 50):
                batch = symbols_with_premarket[i:i+50]
                symbols_str = ','.join(batch)
                try:
                    quote_url = f"https://financialmodelingprep.com/api/v3/quote/{symbols_str}?apikey={api_key}"
                    response = requests.get(quote_url, timeout=15)
                    if response.status_code == 200:
                        for item in response.json():
                            symbol = item.get('symbol', '')
                            prev_close = item.get('previousClose', 0)
                            if symbol and prev_close > 0:
                                quote_prev_closes[symbol] = prev_close
                                if symbol not in stock_names:
                                    stock_names[symbol] = item.get('name', symbol)
                except:
                    pass

            print(f"Got quote previous closes for {len(quote_prev_closes)} symbols")

            # Step 5: Find potential movers (>2.5% based on quote previousClose)
            # Then verify with historical data for accurate previous close
            potential_movers = []
            for symbol in symbols_with_premarket:
                premarket_price = premarket_prices.get(symbol, 0)
                quote_prev = quote_prev_closes.get(symbol, 0)
                if premarket_price > 0 and quote_prev > 0:
                    rough_change = abs((premarket_price - quote_prev) / quote_prev * 100)
                    if rough_change >= 2.5:  # Use 2.5% threshold for initial filter
                        potential_movers.append(symbol)

            print(f"Found {len(potential_movers)} potential movers to verify with historical data")

            # Step 6: Get HISTORICAL closes only for potential movers (accurate previous close)
            print("Fetching historical closes for potential movers...")
            historical_closes = {}  # symbol -> last_trading_day_close

            # Get today's date to skip if it appears in historical data
            from datetime import datetime, date
            today_str = date.today().strftime('%Y-%m-%d')
            print(f"Today's date: {today_str} - will skip this date in historical data")

            for symbol in potential_movers:
                try:
                    # Get last 5 days of data to ensure we have the most recent trading day
                    hist_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?timeseries=5&apikey={api_key}"
                    response = requests.get(hist_url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if data and 'historical' in data and len(data['historical']) > 0:
                            # Find the most recent trading day BEFORE today
                            # historical data is sorted by date descending (most recent first)
                            for hist_entry in data['historical']:
                                hist_date = hist_entry.get('date', '')
                                # Skip today's date (market hasn't closed yet)
                                if hist_date == today_str:
                                    continue
                                hist_close = hist_entry.get('close', 0)
                                if hist_close > 0:
                                    historical_closes[symbol] = hist_close
                                    # Debug: print first few
                                    if len(historical_closes) <= 5:
                                        print(f"  {symbol}: previous close date={hist_date}, close=${hist_close:.2f}")
                                    break  # Use the first valid entry before today
                except:
                    pass

            print(f"Got historical closes for {len(historical_closes)} potential movers")

            # Step 7: Calculate pre-market change for potential movers using accurate historical close
            print("Calculating pre-market changes with verified historical data...")

            for symbol in potential_movers:
                if symbol in seen_symbols:
                    continue

                premarket_price = premarket_prices.get(symbol, 0)
                prev_close = historical_closes.get(symbol, 0)
                name = stock_names.get(symbol, symbol)

                if premarket_price <= 0 or prev_close <= 0:
                    continue

                # Calculate: (premarket_price - yesterday_close) / yesterday_close
                change = premarket_price - prev_close
                change_pct = (change / prev_close) * 100

                # Filter criteria:
                # - Must have >= 3% change
                # - Price must be > $5 (filter penny stocks)
                # - Change must be realistic (< 50% to filter bad data)
                if (abs(change_pct) >= 3 and
                    abs(change_pct) < 50 and
                    premarket_price >= 5):

                    seen_symbols.add(symbol)
                    movers.append({
                        'symbol': symbol,
                        'name': name,
                        'price': premarket_price,
                        'previous_close': prev_close,
                        'change': change,
                        'change_pct': change_pct,
                        'volume': 0,
                        'direction': 'UP' if change_pct > 0 else 'DOWN'
                    })

            # Sort by absolute change percentage
            movers.sort(key=lambda x: abs(x['change_pct']), reverse=True)

            # Limit to top 25 movers
            movers = movers[:25]

            print(f"Found {len(movers)} significant pre-market movers (>3%)")
            for m in movers[:5]:
                print(f"  {m['symbol']}: {m['change_pct']:.2f}% (${m['previous_close']:.2f} -> ${m['price']:.2f})")

            return movers

        except Exception as e:
            print(f"Error fetching pre-market movers: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def read_portfolio_tickers(self, excel_path):
        """Read ticker symbols from the Disruption Index Excel file"""
        try:
            df = pd.read_excel(excel_path, header=None)
            # Extract tickers from second column, skip first 2 rows (headers)
            tickers = df.iloc[2:, 1].dropna().str.upper().tolist()
            # Remove "SYMBOL" if it's in the list
            tickers = [t for t in tickers if t != 'SYMBOL']
            print(f"Loaded {len(tickers)} tickers from portfolio")
            return tickers
        except Exception as e:
            print(f"Error reading portfolio tickers: {str(e)}")
            return []

    def fetch_earnings_calendar(self, tickers):
        """Fetch upcoming earnings dates for portfolio tickers"""
        if not tickers:
            return []

        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            print("FMP_API_KEY not found in config")
            return []

        try:
            print("Fetching earnings calendar for portfolio...")
            from datetime import datetime, timedelta

            # Get earnings for next 14 days
            today = datetime.now()
            end_date = today + timedelta(days=14)

            from_date = today.strftime('%Y-%m-%d')
            to_date = end_date.strftime('%Y-%m-%d')

            url = f"https://financialmodelingprep.com/api/v3/earning_calendar?from={from_date}&to={to_date}&apikey={api_key}"
            response = requests.get(url, timeout=15)

            earnings = []
            if response.status_code == 200:
                data = response.json()
                ticker_set = set(t.upper() for t in tickers)

                for item in data:
                    symbol = item.get('symbol', '').upper()
                    if symbol in ticker_set:
                        earnings.append({
                            'symbol': symbol,
                            'date': item.get('date', ''),
                            'time': item.get('time', 'TBD'),  # BMO (Before Market Open) or AMC (After Market Close)
                            'eps_estimate': item.get('epsEstimated'),
                            'revenue_estimate': item.get('revenueEstimated')
                        })

                # Sort by date
                earnings.sort(key=lambda x: x['date'])

            print(f"Found {len(earnings)} upcoming earnings for portfolio tickers")
            return earnings

        except Exception as e:
            print(f"Error fetching earnings calendar: {str(e)}")
            return []

    def format_earnings_calendar(self, earnings):
        """Format earnings calendar as markdown grouped by date"""
        if not earnings:
            return ""

        from datetime import datetime

        output = "## Upcoming Portfolio Earnings (Next 2 Weeks)\n\n"

        # Group earnings by date
        earnings_by_date = {}
        for item in earnings[:20]:
            date = item['date']
            if date not in earnings_by_date:
                earnings_by_date[date] = []
            earnings_by_date[date].append(item)

        # Format each date group
        for date in sorted(earnings_by_date.keys()):
            # Parse and format date nicely
            try:
                dt = datetime.strptime(date, '%Y-%m-%d')
                formatted_date = dt.strftime('%A, %B %d')  # e.g., "Monday, December 09"
            except:
                formatted_date = date

            output += f"### {formatted_date}\n\n"
            output += "| Ticker | Company | Time | EPS Est. | Revenue Est. |\n"
            output += "|:-------|:--------|:----:|--------:|-------------:|\n"

            for item in earnings_by_date[date]:
                symbol = item['symbol']
                time = item['time'].upper() if item['time'] else 'TBD'
                time_display = "Before Open" if time == "BMO" else "After Close" if time == "AMC" else time
                eps = f"${item['eps_estimate']:.2f}" if item['eps_estimate'] else '-'

                # Format revenue
                if item['revenue_estimate']:
                    rev_val = item['revenue_estimate']
                    if rev_val >= 1e9:
                        rev = f"${rev_val/1e9:.1f}B"
                    else:
                        rev = f"${rev_val/1e6:.0f}M"
                else:
                    rev = '-'

                output += f"| **{symbol}** | | {time_display} | {eps} | {rev} |\n"

            output += "\n"

        output += "---\n\n"
        return output

    def fetch_sector_performance(self):
        """Fetch sector performance for heatmap"""
        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            print("FMP_API_KEY not found in config")
            return []

        try:
            print("Fetching sector performance...")

            url = f"https://financialmodelingprep.com/api/v3/sectors-performance?apikey={api_key}"
            response = requests.get(url, timeout=10)

            sectors = []
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    sector = item.get('sector', '')
                    change_pct = item.get('changesPercentage', '0%')
                    # Convert string percentage to float
                    try:
                        change_val = float(change_pct.replace('%', ''))
                    except:
                        change_val = 0

                    sectors.append({
                        'sector': sector,
                        'change_pct': change_val
                    })

                # Sort by performance (best to worst)
                sectors.sort(key=lambda x: x['change_pct'], reverse=True)

            print(f"Found {len(sectors)} sectors")
            return sectors

        except Exception as e:
            print(f"Error fetching sector performance: {str(e)}")
            return []

    def format_sector_heatmap(self, sectors):
        """Format sector performance as a visual heatmap"""
        if not sectors:
            return ""

        output = "## Sector Performance\n\n"

        # Create visual heatmap using text
        output += "```\n"
        for item in sectors:
            sector = item['sector'][:20].ljust(20)
            change = item['change_pct']

            # Create bar visualization
            bar_length = min(abs(int(change * 2)), 20)
            if change >= 0:
                bar = "+" + ("█" * bar_length)
                output += f"{sector} {bar.ljust(22)} +{change:.2f}%\n"
            else:
                bar = ("█" * bar_length) + "-"
                output += f"{sector} {bar.rjust(22)} {change:.2f}%\n"

        output += "```\n\n"

        # Also add table format
        output += "| Sector | Change |\n"
        output += "|:-------|-------:|\n"
        for item in sectors:
            indicator = "🟢" if item['change_pct'] >= 0 else "🔴"
            output += f"| {item['sector']} | {item['change_pct']:+.2f}% |\n"

        output += "\n---\n\n"
        return output

    def fetch_portfolio_news(self, tickers):
        """Fetch news for specific portfolio tickers from ALL available sources"""
        if not tickers:
            return []

        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            print("FMP_API_KEY not found in config")
            return []

        try:
            print(f"Fetching news for {len(tickers)} portfolio tickers...")
            news_items = []
            ticker_set = set(t.upper() for t in tickers)

            # 1. FMP Stock News - ticker-specific (limit to 100 tickers)
            print("  - Fetching from FMP Stock News...")
            for ticker in list(tickers)[:100]:
                news_url = f"https://financialmodelingprep.com/api/v3/stock_news?tickers={ticker}&limit=3&apikey={api_key}"
                try:
                    response = requests.get(news_url, timeout=5)
                    if response.status_code == 200:
                        news_data = response.json()
                        for item in news_data:
                            news_items.append({
                                'ticker': ticker,
                                'title': item.get('title', ''),
                                'text': item.get('text', '')[:200],
                                'url': item.get('url', ''),
                                'published': item.get('publishedDate', ''),
                                'source': item.get('site', 'FMP')
                            })
                except:
                    continue

            # 2. FMP General Stock News - filter for portfolio tickers
            print("  - Fetching from FMP General News...")
            try:
                general_url = f"https://financialmodelingprep.com/api/v3/stock_news?limit=100&apikey={api_key}"
                response = requests.get(general_url, timeout=10)
                if response.status_code == 200:
                    for item in response.json():
                        symbol = item.get('symbol', '').upper()
                        if symbol in ticker_set:
                            news_items.append({
                                'ticker': symbol,
                                'title': item.get('title', ''),
                                'text': item.get('text', '')[:200],
                                'url': item.get('url', ''),
                                'published': item.get('publishedDate', ''),
                                'source': item.get('site', 'FMP General')
                            })
            except:
                pass

            # 3. FMP Press Releases - filter for portfolio tickers
            print("  - Fetching from FMP Press Releases...")
            try:
                pr_url = f"https://financialmodelingprep.com/api/v3/press-releases?limit=50&apikey={api_key}"
                response = requests.get(pr_url, timeout=10)
                if response.status_code == 200:
                    for item in response.json():
                        symbol = item.get('symbol', '').upper()
                        if symbol in ticker_set:
                            news_items.append({
                                'ticker': symbol,
                                'title': item.get('title', ''),
                                'text': item.get('text', '')[:200] if item.get('text') else '',
                                'url': '',
                                'published': item.get('date', ''),
                                'source': 'Press Release'
                            })
            except:
                pass

            # 4. FMP Earnings Surprises news
            print("  - Fetching from FMP Earnings Surprises...")
            try:
                earnings_url = f"https://financialmodelingprep.com/api/v3/earnings-surprises?apikey={api_key}"
                response = requests.get(earnings_url, timeout=10)
                if response.status_code == 200:
                    for item in response.json()[:50]:
                        symbol = item.get('symbol', '').upper()
                        if symbol in ticker_set:
                            surprise = item.get('surprisePercentage', 0)
                            direction = "beat" if surprise > 0 else "missed"
                            news_items.append({
                                'ticker': symbol,
                                'title': f"{symbol} {direction} earnings by {abs(surprise):.1f}%",
                                'text': f"Actual: {item.get('actualEarningResult', 'N/A')}, Est: {item.get('estimatedEarning', 'N/A')}",
                                'url': '',
                                'published': item.get('date', ''),
                                'source': 'Earnings'
                            })
            except:
                pass

            # 5. Google News RSS - search for portfolio tickers
            print("  - Fetching from Google News RSS...")
            try:
                import feedparser
                # Search for top portfolio tickers
                top_tickers = list(tickers)[:20]  # Limit to avoid too many requests
                for ticker in top_tickers:
                    feed_url = f'https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en'
                    try:
                        feed = feedparser.parse(feed_url)
                        for entry in feed.entries[:3]:
                            news_items.append({
                                'ticker': ticker,
                                'title': entry.get('title', ''),
                                'text': entry.get('summary', '')[:200] if entry.get('summary') else '',
                                'url': entry.get('link', ''),
                                'published': entry.get('published', ''),
                                'source': 'Google News'
                            })
                    except:
                        continue
            except:
                pass

            # 6. Benzinga RSS - filter for portfolio tickers
            print("  - Fetching from Benzinga RSS...")
            try:
                import feedparser
                feed = feedparser.parse('https://www.benzinga.com/feed')
                for entry in feed.entries[:30]:
                    title = entry.get('title', '')
                    # Check if any portfolio ticker is mentioned in title
                    for ticker in tickers:
                        if ticker.upper() in title.upper() or f"${ticker.upper()}" in title.upper():
                            news_items.append({
                                'ticker': ticker,
                                'title': title,
                                'text': entry.get('summary', '')[:200] if entry.get('summary') else '',
                                'url': entry.get('link', ''),
                                'published': entry.get('published', ''),
                                'source': 'Benzinga'
                            })
                            break
            except:
                pass

            # 7. Reuters RSS - filter for portfolio tickers
            print("  - Fetching from Reuters RSS...")
            try:
                import feedparser
                reuters_feeds = [
                    'https://www.reuters.com/markets/?format=rss',
                    'https://www.reuters.com/business/?format=rss'
                ]
                for feed_url in reuters_feeds:
                    try:
                        feed = feedparser.parse(feed_url)
                        for entry in feed.entries[:20]:
                            title = entry.get('title', '')
                            for ticker in tickers:
                                if ticker.upper() in title.upper():
                                    news_items.append({
                                        'ticker': ticker,
                                        'title': title,
                                        'text': entry.get('summary', '')[:200] if entry.get('summary') else '',
                                        'url': entry.get('link', ''),
                                        'published': entry.get('published', ''),
                                        'source': 'Reuters'
                                    })
                                    break
                    except:
                        continue
            except:
                pass

            # 8. SEC EDGAR RSS - filter for portfolio tickers
            print("  - Fetching from SEC EDGAR...")
            try:
                import feedparser
                feed = feedparser.parse('https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&company=&dateb=&owner=include&count=40&output=atom')
                for entry in feed.entries[:30]:
                    title = entry.get('title', '')
                    for ticker in tickers:
                        if ticker.upper() in title.upper():
                            news_items.append({
                                'ticker': ticker,
                                'title': f"SEC Filing: {title}",
                                'text': entry.get('summary', '')[:200] if entry.get('summary') else '',
                                'url': entry.get('link', ''),
                                'published': entry.get('updated', ''),
                                'source': 'SEC EDGAR'
                            })
                            break
            except:
                pass

            # 9. FMP Analyst Estimates (for recent changes)
            print("  - Fetching from FMP Analyst Estimates...")
            try:
                for ticker in list(tickers)[:30]:
                    est_url = f"https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?limit=1&apikey={api_key}"
                    try:
                        response = requests.get(est_url, timeout=5)
                        if response.status_code == 200:
                            data = response.json()
                            if data:
                                item = data[0]
                                news_items.append({
                                    'ticker': ticker,
                                    'title': f"{ticker} Analyst Est: Rev ${item.get('estimatedRevenueAvg', 0)/1e9:.1f}B, EPS ${item.get('estimatedEpsAvg', 0):.2f}",
                                    'text': f"High: ${item.get('estimatedEpsHigh', 0):.2f}, Low: ${item.get('estimatedEpsLow', 0):.2f}",
                                    'url': '',
                                    'published': item.get('date', ''),
                                    'source': 'Analyst Estimates'
                                })
                    except:
                        continue
            except:
                pass

            # 10. FMP Social Sentiment
            print("  - Fetching from FMP Social Sentiment...")
            try:
                for ticker in list(tickers)[:20]:
                    sent_url = f"https://financialmodelingprep.com/api/v4/social-sentiment?symbol={ticker}&apikey={api_key}"
                    try:
                        response = requests.get(sent_url, timeout=5)
                        if response.status_code == 200:
                            data = response.json()
                            if data:
                                item = data[0]
                                sentiment = item.get('sentiment', 0)
                                sentiment_label = "Bullish" if sentiment > 0.1 else "Bearish" if sentiment < -0.1 else "Neutral"
                                news_items.append({
                                    'ticker': ticker,
                                    'title': f"{ticker} Social Sentiment: {sentiment_label} ({sentiment:.2f})",
                                    'text': f"Social volume trending on {item.get('source', 'social media')}",
                                    'url': '',
                                    'published': item.get('date', ''),
                                    'source': 'Social Sentiment'
                                })
                    except:
                        continue
            except:
                pass

            # Remove duplicates based on title
            seen_titles = set()
            unique_news = []
            for item in news_items:
                title_key = item['title'][:60].lower()
                if title_key not in seen_titles and item['title']:
                    seen_titles.add(title_key)
                    unique_news.append(item)

            print(f"Found {len(unique_news)} news items for portfolio tickers")
            return unique_news[:150]  # Return top 150 unique items

        except Exception as e:
            print(f"Error fetching portfolio news: {str(e)}")
            return []

    def fetch_portfolio_upgrades_downgrades(self, tickers):
        """Fetch analyst upgrades and downgrades for portfolio tickers"""
        if not tickers:
            return []

        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            print("FMP_API_KEY not found in config")
            return []

        try:
            print(f"Fetching upgrades/downgrades for {len(tickers)} portfolio tickers...")
            upgrades_downgrades = []

            # FMP has an upgrades-downgrades-rss-feed endpoint
            # We'll fetch recent upgrades/downgrades and filter for our tickers
            url = f"https://financialmodelingprep.com/api/v4/upgrades-downgrades-rss-feed?page=0&apikey={api_key}"

            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()

                # Filter for our tickers only
                ticker_set = set(t.upper() for t in tickers)
                for item in data:
                    symbol = item.get('symbol', '').upper()
                    if symbol in ticker_set:
                        upgrades_downgrades.append({
                            'ticker': symbol,
                            'company': item.get('company', ''),
                            'grade': item.get('newGrade', ''),
                            'previous_grade': item.get('previousGrade', ''),
                            'action': item.get('gradingCompany', ''),
                            'analyst': item.get('gradingCompany', ''),
                            'published': item.get('publishedDate', '')
                        })

            print(f"Found {len(upgrades_downgrades)} upgrades/downgrades for portfolio tickers")
            return upgrades_downgrades[:20]  # Limit to 20 most recent

        except Exception as e:
            print(f"Error fetching upgrades/downgrades: {str(e)}")
            return []

    def summarize_portfolio_news(self, news_items, upgrades_downgrades=None):
        """Use AI to summarize portfolio-specific news in bullet point format"""
        if not news_items and not upgrades_downgrades:
            return "No significant news for portfolio holdings."

        # Group news by ticker
        news_by_ticker = {}
        for item in news_items:
            ticker = item['ticker']
            if ticker not in news_by_ticker:
                news_by_ticker[ticker] = []
            news_by_ticker[ticker].append(item)

        # Prepare news text for AI
        news_text = ""
        for ticker, items in news_by_ticker.items():
            news_text += f"\n\n{ticker}:\n"
            for item in items[:2]:  # Top 2 news per ticker
                news_text += f"- {item['title']} (Source: {item['source']})\n"

        # Add upgrades/downgrades if available
        upgrades_text = ""
        if upgrades_downgrades:
            upgrades_text = "\n\nAnalyst Upgrades/Downgrades:\n"
            for item in upgrades_downgrades[:15]:
                prev_grade = item.get('previous_grade') or ''
                new_grade = item.get('grade') or ''

                # Determine if upgrade or downgrade based on common rating terms
                upgrade_terms = ['buy', 'outperform', 'overweight', 'strong buy']
                downgrade_terms = ['sell', 'underperform', 'underweight', 'reduce']

                action_type = "rating change"
                if any(term in new_grade.lower() for term in upgrade_terms) and not any(term in prev_grade.lower() for term in upgrade_terms):
                    action_type = "upgraded"
                elif any(term in new_grade.lower() for term in downgrade_terms) or (prev_grade and 'buy' in prev_grade.lower() and 'hold' in new_grade.lower()):
                    action_type = "downgraded"

                upgrades_text += f"- {item['ticker']} ({item.get('company', '')}): {action_type} by {item.get('analyst', 'analyst')} from {prev_grade or 'N/A'} to {new_grade or 'N/A'}\n"

        prompt = f"""Analyze the following news about portfolio holdings in the Disruption/Innovation Index.
Focus ONLY on the most significant and market-moving news. Ignore minor updates.

Provide a concise summary of the most important company-specific developments, INCLUDING any analyst upgrades or downgrades.

IMPORTANT: Format your response as 4-7 clear bullet points using this exact format:
• **[TICKER]**: [Brief headline/development and its significance]

For upgrades/downgrades, use format:
• **[TICKER]**: Upgraded/Downgraded by [Analyst] to [New Rating] from [Previous Rating]

Maximum 200 words total. Start each point with a bullet (•) symbol.
Only include tickers with truly significant news (major earnings, acquisitions, product launches, regulatory changes, analyst upgrades/downgrades, etc.).

Portfolio News:
{news_text}

{upgrades_text}"""

        try:
            ai_provider = self.config.get('ai_provider', 'openai')

            if ai_provider == 'anthropic':
                return self._summarize_with_anthropic(prompt)
            else:
                return self._summarize_with_openai(prompt)
        except Exception as e:
            print(f"Error summarizing portfolio news: {str(e)}")
            return "Portfolio news summary unavailable."

    def fetch_economic_calendar(self):
        """Fetch US economic calendar for today from FMP"""
        api_key = self.config.get('FMP_API_KEY')
        if not api_key:
            print("FMP_API_KEY not found in config")
            return []

        today = datetime.now().strftime('%Y-%m-%d')

        try:
            print(f"Fetching economic calendar for {today}...")
            url = f"https://financialmodelingprep.com/api/v3/economic_calendar?from={today}&to={today}&apikey={api_key}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                # Filter for US events only
                us_events = [event for event in data if event.get('country') == 'US']
                print(f"Found {len(us_events)} US economic events")
                return us_events
            else:
                print(f"Error fetching economic calendar: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error fetching economic calendar: {str(e)}")
            return []

    def format_economic_calendar(self, events):
        """Format economic calendar events as markdown"""
        if not events:
            return ""

        output = "## US Economic Calendar\n\n"
        output += "| Time | Event | Actual | Estimate | Previous |\n"
        output += "|:-----|:------|-------:|---------:|---------:|\n"

        for event in sorted(events, key=lambda x: x.get('date', '')):
            time = event.get('date', '').split('T')[1][:5] if 'T' in event.get('date', '') else 'TBD'
            name = event.get('event', 'N/A')
            actual = event.get('actual', '-')
            estimate = event.get('estimate', '-')
            previous = event.get('previous', '-')

            output += f"| {time} | {name} | {actual} | {estimate} | {previous} |\n"

        output += "\n---\n\n"
        return output

    def summarize_market_news(self, news_items, premarket_movers=None):
        """Use AI to identify and summarize market-moving news including pre-market movers"""
        if not news_items and not premarket_movers:
            return "No major market news available."

        # Prepare news for AI
        news_text = "\n\n".join([
            f"- {item['title']} (Source: {item['source']})"
            for item in news_items
        ]) if news_items else ""

        # Add pre-market movers if available
        movers_text = ""
        if premarket_movers:
            movers_text = "\n\nPre-Market Movers (>3% change):\n"
            for mover in premarket_movers[:10]:
                direction = "↑" if mover['direction'] == 'UP' else "↓"
                movers_text += f"- {mover['symbol']} ({mover['name']}): {direction} {mover['change_pct']:+.2f}% at ${mover['price']:.2f}\n"

        prompt = f"""Analyze the following market news and pre-market activity. Provide a concise summary of the most important market-moving events.

Focus on:
1. MERGERS AND ACQUISITIONS - Any M&A announcements are HIGH PRIORITY and must be included
2. Major pre-market movers and what's driving them
3. Key market headlines and trends
4. Significant company news that could impact markets
5. Economic data or policy changes

IMPORTANT: Format your response as 4-7 clear bullet points using this exact format:
• [Brief headline/topic]: [Concise explanation]

M&A news should always be listed first if present. Maximum 250 words total. Start each point with a bullet (•) symbol.

News Headlines:
{news_text}

{movers_text}"""

        try:
            ai_provider = self.config.get('ai_provider', 'openai')

            if ai_provider == 'anthropic':
                return self._summarize_with_anthropic(prompt)
            else:
                return self._summarize_with_openai(prompt)
        except Exception as e:
            print(f"Error summarizing market news: {str(e)}")
            return "Market news summary unavailable."

    def fetch_emails_from_senders(self, days_back=1):
        """Fetch emails from specific senders within the last N days"""
        mail = self.connect_to_gmail()

        # Calculate date for search
        since_date = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")

        emails_data = []

        for sender in self.target_senders:
            print(f"Fetching emails from: {sender}")

            # Search for emails from this sender since the date
            search_criteria = f'(FROM "{sender}" SINCE {since_date})'
            status, messages = mail.search(None, search_criteria)

            if status != 'OK':
                print(f"Error searching for emails from {sender}")
                continue

            email_ids = messages[0].split()
            print(f"Found {len(email_ids)} emails from {sender}")

            for email_id in email_ids:
                try:
                    status, msg_data = mail.fetch(email_id, '(RFC822)')

                    if status != 'OK':
                        continue

                    # Parse email
                    msg = email.message_from_bytes(msg_data[0][1])

                    # Extract details
                    subject = self.decode_email_subject(msg['Subject'])
                    from_addr = msg['From']
                    date_str = msg['Date']
                    body = self.extract_email_body(msg)

                    # Generate AI summary
                    print(f"  Summarizing: {subject[:50]}...")
                    summary = self.summarize_with_ai(subject, body, from_addr)

                    emails_data.append({
                        'sender': from_addr,
                        'subject': subject,
                        'date': date_str,
                        'summary': summary,
                        'body': body
                    })

                except Exception as e:
                    print(f"Error processing email {email_id}: {str(e)}")
                    continue

        mail.close()
        mail.logout()

        return emails_data

    def generate_daily_note(self, emails_data, market_news_summary=None, global_markets_text=None, economic_calendar_text=None, portfolio_news_summary=None, sector_heatmap_text=None, earnings_calendar_text=None, weekend_mode=False):
        """Generate a professional daily note from emails"""
        today = datetime.now().strftime("%Y-%m-%d")

        # Create header
        if weekend_mode:
            note = f"""# Weekend Brief - Week Ahead Preview
## {datetime.now().strftime("%B %d, %Y")}

---

"""
        else:
            note = f"""# Daily Brief - {datetime.now().strftime("%B %d, %Y")}

---

"""

        # Add global markets section if available
        if global_markets_text:
            note += global_markets_text

        # Add sector heatmap if available
        if sector_heatmap_text:
            note += sector_heatmap_text

        # Add summary section
        if not weekend_mode:
            note += f"""## Summary
Total updates received: {len(emails_data)} items

"""

        # Add market news summary if available
        if market_news_summary:
            note += f"""### Market-Moving News

{market_news_summary}

---

"""
        else:
            note += "---\n\n"

        # Add Disruption/Innovation Index portfolio news if available
        if portfolio_news_summary:
            note += f"""## Disruption/Innovation Index News

{portfolio_news_summary}

---

"""

        # Group by sender
        emails_by_sender = {}
        # Advertisement/promotion keywords to filter out
        ad_keywords = ['sale', 'discount', 'offer', 'promo', 'black friday', 'cyber monday',
                       'limited time', 'last chance', 'don\'t miss', 'subscribe', 'webinar',
                       'sign up', 'register now', 'free trial', 'special offer', '% off']

        for email_data in emails_data:
            sender = email_data['sender']
            subject = email_data.get('subject', '').lower()

            # Filter out advertisements and promotions from any source
            if any(keyword in subject for keyword in ad_keywords):
                continue

            if sender not in emails_by_sender:
                emails_by_sender[sender] = []
            emails_by_sender[sender].append(email_data)

        # Add each sender's emails
        for sender, emails in emails_by_sender.items():
            # Extract display name from email (remove email address)
            display_name = sender
            if '<' in sender and '>' in sender:
                display_name = sender.split('<')[0].strip().strip('"')

            note += f"## {display_name}\n\n"

            for email_item in emails:
                note += f"### {email_item['subject']}\n"
                note += f"*Received: {email_item['date']}*\n\n"
                note += f"{email_item['summary']}\n\n"
                note += "---\n\n"

        # Add upcoming earnings calendar (just before economic calendar)
        if earnings_calendar_text:
            note += earnings_calendar_text

        # Add economic calendar at bottom if available
        if economic_calendar_text:
            note += economic_calendar_text

        # Add footer
        note += f"\n\n*Generated on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}*\n"

        return note, today

    def save_note(self, note_content, date_str):
        """Save the daily note to a file"""
        output_path = Path(self.output_dir) / f"daily_note_{date_str}.md"

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(note_content)

        print(f"\nDaily note saved to: {output_path}")
        return output_path

    def generate_pdf(self, emails_data, market_news_summary, global_markets_data, economic_events, date_str, portfolio_news_summary=None, sector_data=None, earnings_data=None, weekend_mode=False, premarket_movers=None):
        """Generate PDF with all 5 global markets in ONE horizontal row"""
        pdf_path = Path(self.output_dir) / f"{'weekend' if weekend_mode else 'daily'}_brief_{date_str}.pdf"

        doc = SimpleDocTemplate(str(pdf_path), pagesize=letter,
                               rightMargin=0.5*inch, leftMargin=0.5*inch,
                               topMargin=0.5*inch, bottomMargin=0.5*inch)

        story = []
        styles = getSampleStyleSheet()

        # Custom styles
        title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'],
                                     fontSize=24, textColor=colors.HexColor('#2c3e50'),
                                     spaceAfter=12, alignment=TA_CENTER)

        heading_style = ParagraphStyle('CustomHeading', parent=styles['Heading2'],
                                       fontSize=14, textColor=colors.HexColor('#34495e'),
                                       spaceAfter=10, spaceBefore=15)

        tagline_style = ParagraphStyle('Tagline', parent=styles['Normal'],
                                       fontSize=10, textColor=colors.HexColor('#555555'),
                                       alignment=TA_CENTER, fontName='Helvetica-Oblique',
                                       spaceAfter=20)

        # Add company logo if it exists (3x larger)
        logo_path = self.config.get('logo_path', '')
        if logo_path and os.path.exists(logo_path):
            from reportlab.platypus import Image
            try:
                # Load image at 3x size and maintain aspect ratio
                logo = Image(logo_path, width=6.0*inch, height=2.0*inch, kind='proportional')
                logo.hAlign = 'CENTER'
                story.append(logo)
                story.append(Spacer(1, 0.08*inch))
            except Exception as e:
                print(f"Could not add logo to PDF: {e}")

        # Add tagline below logo
        story.append(Paragraph("Precision Analysis for Informed Investment Decisions", tagline_style))
        story.append(Spacer(1, 0.08*inch))

        # Title
        if weekend_mode:
            story.append(Paragraph(f"Weekend Brief - Week Ahead Preview", title_style))
            story.append(Paragraph(f"{datetime.now().strftime('%B %d, %Y')}", tagline_style))
        else:
            story.append(Paragraph(f"Daily Brief - {datetime.now().strftime('%B %d, %Y')}", title_style))
        story.append(Spacer(1, 0.12*inch))

        # GLOBAL MARKETS - 3 centered tables, compact to fit on page 1
        if global_markets_data:
            story.append(Paragraph("Global Markets", heading_style))

            # Compact table style for all tables
            compact_style = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (1, -1), 'LEFT'),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
            ])

            # Table 1: Main Markets (Index Futures, Fixed Income, Commodities)
            main_table_data = [['Category', 'Item', 'Price/Rate', 'Yield', 'Δ%']]

            # Index Futures
            first_index = True
            for name, data in global_markets_data['indices'].items():
                if data['price']:
                    category = "Index Futures" if first_index else ""
                    main_table_data.append([category, name, f"{data['price']:,.2f}", "-", f"{data['change_pct']:+.2f}%"])
                    first_index = False

            # Fixed Income
            first_treasury = True
            for name, data in global_markets_data['treasuries'].items():
                if data['price']:
                    category = "Fixed Income" if first_treasury else ""
                    main_table_data.append([category, name, "-", f"{data['price']:.2f}%", f"{data['change_pct']:+.2f}%"])
                    first_treasury = False

            # Commodities
            first_commodity = True
            for name, data in global_markets_data['commodities'].items():
                if data['price']:
                    category = "Commodities" if first_commodity else ""
                    main_table_data.append([category, name, f"${data['price']:,.2f}", "-", f"{data['change_pct']:+.2f}%"])
                    first_commodity = False

            main_table = Table(main_table_data, colWidths=[1.1*inch, 1.1*inch, 1.0*inch, 0.7*inch, 0.8*inch])
            main_table.setStyle(compact_style)
            main_table.hAlign = 'CENTER'
            story.append(main_table)
            story.append(Spacer(1, 0.1*inch))

            # Table 2: Combined FX and Crypto (same format as main table)
            fx_crypto_table_data = [['Category', 'Item', 'Price/Rate', 'Δ%']]

            # Foreign Exchange
            first_fx = True
            for name, data in global_markets_data['fx'].items():
                if data['price']:
                    category = "Foreign Exchange" if first_fx else ""
                    fx_crypto_table_data.append([category, name, f"{data['price']:.4f}", f"{data['change_pct']:+.2f}%"])
                    first_fx = False

            # Crypto Currencies
            first_crypto = True
            for name, data in global_markets_data['crypto'].items():
                if data['price']:
                    category = "Crypto Currencies" if first_crypto else ""
                    fx_crypto_table_data.append([category, name, f"${data['price']:,.2f}", f"{data['change_pct']:+.2f}%"])
                    first_crypto = False

            # Match width of main table (1.1 + 1.1 + 1.0 + 0.7 + 0.8 = 4.7 inches)
            fx_crypto_table = Table(fx_crypto_table_data, colWidths=[1.1*inch, 1.1*inch, 1.5*inch, 1.0*inch])
            fx_crypto_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (1, -1), 'LEFT'),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
            ]))
            fx_crypto_table.hAlign = 'CENTER'
            story.append(fx_crypto_table)

            story.append(Spacer(1, 0.1*inch))

        # SECTOR PERFORMANCE
        if sector_data:
            story.append(Paragraph("Sector Performance", heading_style))
            sector_table_data = [['Sector', 'Change']]
            for item in sector_data:
                change_str = f"{item['change_pct']:+.2f}%"
                sector_table_data.append([item['sector'], change_str])

            sector_table = Table(sector_table_data, colWidths=[4*inch, 1.5*inch])
            sector_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
            ]))
            story.append(sector_table)
            story.append(Spacer(1, 0.15*inch))

        # SUMMARY SECTION (now on page 2)
        if not weekend_mode:
            story.append(Paragraph("Summary", heading_style))
            story.append(Paragraph(f"Total updates received: {len(emails_data)} items", styles['Normal']))
            story.append(Spacer(1, 0.1*inch))

        if market_news_summary:
            story.append(Paragraph("<b>Market-Moving News</b>", styles['Normal']))
            story.append(Spacer(1, 0.05*inch))

            # Split bullet points and format each one on a separate line
            bullet_points = [line.strip() for line in market_news_summary.split('\n') if line.strip() and line.strip().startswith('•')]

            bullet_style = ParagraphStyle('BulletPoint', parent=styles['Normal'],
                                         fontSize=10, leftIndent=10, spaceAfter=8,
                                         leading=14)

            for bullet in bullet_points:
                story.append(Paragraph(bullet, bullet_style))

            story.append(Spacer(1, 0.15*inch))

        # PRE-MARKET MOVERS section (after Market-Moving News)
        if premarket_movers and not weekend_mode:
            story.append(Paragraph("<b>Pre-Market Movers</b>", styles['Normal']))
            story.append(Spacer(1, 0.05*inch))

            # Create table for pre-market movers
            movers_table_data = [['Symbol', 'Company', 'Price', 'Change', '% Change']]
            for mover in premarket_movers:
                change_val = mover.get('change', 0)
                change_pct = mover.get('change_pct', 0)
                change_str = f"+${change_val:.2f}" if change_val >= 0 else f"-${abs(change_val):.2f}"
                pct_str = f"+{change_pct:.2f}%" if change_pct >= 0 else f"{change_pct:.2f}%"
                company_name = mover.get('name', '')[:25]  # Truncate long names
                movers_table_data.append([
                    mover.get('symbol', ''),
                    company_name,
                    f"${mover.get('price', 0):.2f}",
                    change_str,
                    pct_str
                ])

            movers_table = Table(movers_table_data, colWidths=[0.8*inch, 2.5*inch, 1*inch, 1*inch, 1*inch])
            movers_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                ('TOPPADDING', (0, 0), (-1, 0), 6),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
            ]))
            story.append(movers_table)
            story.append(Spacer(1, 0.15*inch))

        # KITE EVOLUTION FUND - Portfolio News
        if portfolio_news_summary:
            story.append(Paragraph("Disruption/Innovation Index News", heading_style))
            story.append(Spacer(1, 0.05*inch))

            # Split bullet points and format each one on a separate line
            portfolio_bullets = [line.strip() for line in portfolio_news_summary.split('\n') if line.strip() and line.strip().startswith('•')]

            portfolio_bullet_style = ParagraphStyle('PortfolioBulletPoint', parent=styles['Normal'],
                                                   fontSize=10, leftIndent=10, spaceAfter=8,
                                                   leading=14)

            for bullet in portfolio_bullets:
                story.append(Paragraph(bullet, portfolio_bullet_style))

            story.append(Spacer(1, 0.15*inch))

        # NEWSLETTER UPDATES
        emails_by_sender = {}
        # Advertisement/promotion keywords to filter out
        ad_keywords = ['sale', 'discount', 'offer', 'promo', 'black friday', 'cyber monday',
                       'limited time', 'last chance', 'don\'t miss', 'subscribe', 'webinar',
                       'sign up', 'register now', 'free trial', 'special offer', '% off']

        for email_data in emails_data:
            sender = email_data['sender']
            subject = email_data.get('subject', '').lower()

            # Filter out advertisements and promotions from any source
            if any(keyword in subject for keyword in ad_keywords):
                continue

            if sender not in emails_by_sender:
                emails_by_sender[sender] = []
            emails_by_sender[sender].append(email_data)

        for sender, emails in emails_by_sender.items():
            display_name = sender
            if '<' in sender and '>' in sender:
                display_name = sender.split('<')[0].strip().strip('"')

            story.append(Paragraph(display_name, heading_style))

            for email_item in emails:
                story.append(Paragraph(f"<b>{email_item['subject']}</b>", styles['Normal']))
                story.append(Paragraph(f"<i>{email_item['date']}</i>", styles['Normal']))
                story.append(Spacer(1, 0.05*inch))
                story.append(Paragraph(email_item['summary'], styles['Normal']))
                story.append(Spacer(1, 0.15*inch))

        # UPCOMING EARNINGS (just before Economic Calendar)
        if earnings_data:
            story.append(Paragraph("Upcoming Portfolio Earnings", heading_style))

            # Group earnings by date
            from datetime import datetime as dt_cls
            earnings_by_date = {}
            for item in earnings_data[:15]:
                date = item['date']
                if date not in earnings_by_date:
                    earnings_by_date[date] = []
                earnings_by_date[date].append(item)

            # Create table for each date
            for date in sorted(earnings_by_date.keys()):
                try:
                    dt_obj = dt_cls.strptime(date, '%Y-%m-%d')
                    formatted_date = dt_obj.strftime('%A, %B %d')
                except:
                    formatted_date = date

                # Date header
                date_style = ParagraphStyle('DateHeader', parent=styles['Normal'],
                                           fontSize=10, fontName='Helvetica-Bold',
                                           textColor=colors.HexColor('#2c3e50'),
                                           spaceBefore=6, spaceAfter=3)
                story.append(Paragraph(formatted_date, date_style))

                # Build table data
                table_data = [['Ticker', 'Time', 'EPS Est.', 'Rev Est.']]
                for item in earnings_by_date[date]:
                    symbol = item['symbol']
                    time = item['time'].upper() if item['time'] else 'TBD'
                    time_display = "Before Open" if time == "BMO" else "After Close" if time == "AMC" else time
                    eps = f"${item['eps_estimate']:.2f}" if item['eps_estimate'] else '-'

                    if item['revenue_estimate']:
                        rev_val = item['revenue_estimate']
                        if rev_val >= 1e9:
                            rev = f"${rev_val/1e9:.1f}B"
                        else:
                            rev = f"${rev_val/1e6:.0f}M"
                    else:
                        rev = '-'

                    table_data.append([symbol, time_display, eps, rev])

                earnings_table = Table(table_data, colWidths=[1.2*inch, 1.5*inch, 1*inch, 1.2*inch])
                earnings_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 5),
                    ('TOPPADDING', (0, 0), (-1, 0), 5),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
                ]))
                story.append(earnings_table)
                story.append(Spacer(1, 0.08*inch))

            story.append(Spacer(1, 0.1*inch))

        # ECONOMIC CALENDAR
        if economic_events:
            story.append(Paragraph("US Economic Calendar", heading_style))

            cal_data = [['Time', 'Event', 'Actual', 'Estimate', 'Previous']]
            for event in sorted(economic_events, key=lambda x: x.get('date', '')):
                time_str = event.get('date', '').split('T')[1][:5] if 'T' in event.get('date', '') else 'TBD'
                cal_data.append([
                    time_str,
                    event.get('event', 'N/A')[:40],
                    str(event.get('actual', '-')),
                    str(event.get('estimate', '-')),
                    str(event.get('previous', '-'))
                ])

            cal_table = Table(cal_data, colWidths=[0.8*inch, 3.5*inch, 0.8*inch, 0.8*inch, 0.8*inch])
            cal_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
            ]))
            story.append(cal_table)

        # Build PDF
        doc.build(story)
        print(f"\nPDF generated: {pdf_path}")
        return pdf_path

    def send_email(self, note_content, date_str, pdf_path=None):
        """Send the daily note via email"""
        if not self.config.get('send_email', False):
            print("Email sending is disabled in config")
            return

        recipient = self.config.get('email_recipient', self.email_address)
        smtp_server = self.config.get('smtp_server', 'smtp.gmail.com')
        smtp_port = self.config.get('smtp_port', 587)
        logo_path = self.config.get('logo_path', '')

        print(f"\nSending email to {recipient}...")

        try:
            # Create message container
            msg = MIMEMultipart('related')
            msg['Subject'] = f"Daily Brief - {datetime.now().strftime('%B %d, %Y')}"
            msg['From'] = self.email_address
            msg['To'] = recipient

            # Create alternative container for text and HTML
            msg_alternative = MIMEMultipart('alternative')
            msg.attach(msg_alternative)

            # Add plain text version
            text_part = MIMEText(note_content, 'plain', 'utf-8')
            msg_alternative.attach(text_part)

            # Create simple HTML version with just message
            simple_message = f"Please see your Daily Brief for {datetime.now().strftime('%B %d, %Y')}"
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <body style="font-family: Arial, sans-serif; padding: 20px;">
                <p style="font-size: 16px; color: #333;">{simple_message}</p>
                <p style="font-size: 14px; color: #666; margin-top: 20px;">The full report is attached as a PDF.</p>
            </body>
            </html>
            """
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg_alternative.attach(html_part)

            # Attach logo if provided
            if logo_path and os.path.exists(logo_path):
                with open(logo_path, 'rb') as img_file:
                    img_data = img_file.read()
                    image = MIMEImage(img_data)
                    image.add_header('Content-ID', '<company_logo>')
                    image.add_header('Content-Disposition', 'inline', filename='company_logo.png')
                    msg.attach(image)

            # Attach PDF if provided
            if pdf_path and os.path.exists(pdf_path):
                with open(pdf_path, 'rb') as pdf_file:
                    pdf_attachment = MIMEApplication(pdf_file.read(), _subtype='pdf')
                    pdf_attachment.add_header('Content-Disposition', 'attachment',
                                             filename=f"daily_brief_{date_str}.pdf")
                    msg.attach(pdf_attachment)

            # Connect and send
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(self.email_address, self.password)
                server.send_message(msg)

            print(f"Email sent successfully to {recipient}")

        except Exception as e:
            print(f"Error sending email: {str(e)}")
            raise

    def _create_html_email(self, note_content, has_logo=False):
        """Convert markdown note to HTML email format with proper table and grid layout"""
        import html as html_escape_module

        # Convert markdown tables to HTML tables
        lines = note_content.split('\n')
        html_lines = []
        in_table = False
        table_html = []

        for line in lines:
            # Handle div markers
            if line.startswith('<div'):
                html_lines.append(line)
                continue
            elif line.startswith('</div>'):
                html_lines.append(line)
                continue
            elif line == '&nbsp;':
                html_lines.append('<div style="margin: 30px 0;"></div>')
                continue

            # Detect markdown table start
            if '|' in line and not in_table:
                in_table = True
                table_html = ['<table style="width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 0.9em;">']

            if in_table:
                if '|' in line:
                    cells = [cell.strip() for cell in line.split('|')[1:-1]]
                    if all(set(cell) <= set('-:| ') for cell in cells):
                        # Header separator row, skip
                        continue
                    row_html = '<tr>'
                    for i, cell in enumerate(cells):
                        if i == 0:
                            row_html += f'<td style="padding: 2px 6px; border-bottom: 1px solid #e0e0e0; font-weight: 600;">{html_escape_module.escape(cell)}</td>'
                        else:
                            row_html += f'<td style="padding: 2px 6px; border-bottom: 1px solid #e0e0e0; text-align: right;">{html_escape_module.escape(cell)}</td>'
                    row_html += '</tr>'
                    table_html.append(row_html)
                elif not line.strip():
                    # End of table
                    table_html.append('</table>')
                    html_lines.append('\n'.join(table_html))
                    in_table = False
                    table_html = []
                continue

            # Regular markdown conversion
            if line.startswith('# '):
                # Center the main Daily Brief title
                html_lines.append(f'<h1 style="color: #2c3e50; font-family: Arial, sans-serif; margin: 20px 0 10px 0; text-align: center; font-size: 2em;">{html_escape_module.escape(line[2:])}</h1>')
            elif line.startswith('## '):
                html_lines.append(f'<h2 style="color: #34495e; font-family: Arial, sans-serif; border-bottom: 2px solid #3498db; padding-bottom: 8px; margin: 25px 0 15px 0; font-size: 1.4em;">{html_escape_module.escape(line[3:])}</h2>')
            elif line.startswith('### '):
                html_lines.append(f'<h3 style="color: #2c5aa0; font-family: Arial, sans-serif; margin: 18px 0 10px 0; font-size: 1.15em;">{html_escape_module.escape(line[4:])}</h3>')
            elif line.startswith('*') and line.endswith('*'):
                html_lines.append(f'<p style="color: #7f8c8d; font-style: italic; font-size: 0.9em; margin: 5px 0;">{html_escape_module.escape(line[1:-1])}</p>')
            elif line.startswith('---'):
                html_lines.append('<hr style="border: none; border-top: 2px solid #ddd; margin: 25px 0;">')
            elif line.strip():
                html_lines.append(f'<p style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 10px 0; font-size: 1em;">{html_escape_module.escape(line)}</p>')
            else:
                html_lines.append('<br>')

        html_body = '\n'.join(html_lines)

        # Create full HTML email with grid layout
        logo_html = ''
        if has_logo:
            logo_html = '<div style="text-align: center;"><img src="cid:company_logo" alt="Company Logo" style="max-width: 280px; margin-bottom: 10px;"><p style="font-family: Arial, sans-serif; font-size: 14px; color: #555; margin-top: 5px; margin-bottom: 20px; font-style: italic;">Precision Analysis for Informed Investment Decisions</p></div>'

        html_email = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    background-color: white;
                    padding: 40px;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .markets-grid {{
                    margin: 10px 0 20px 0;
                }}
                .markets-row {{
                    display: grid;
                    gap: 8px;
                    margin-bottom: 15px;
                }}
                .markets-row.compact-row {{
                    max-height: 192px;
                    gap: 6px;
                    margin-bottom: 10px;
                }}
                .markets-row.row-2col {{
                    grid-template-columns: 1fr 1fr;
                }}
                .markets-row.row-3col {{
                    grid-template-columns: 1fr 1fr 1fr;
                }}
                .markets-row.row-5col {{
                    grid-template-columns: 1fr 1fr 1fr 1fr 1fr;
                }}
                .markets-row.row-1col {{
                    grid-template-columns: 1fr;
                }}
                .market-section {{
                    background: #fafafa;
                    padding: 12px;
                    border-radius: 4px;
                    border: 1px solid #e0e0e0;
                }}
                .market-section.compact {{
                    padding: 6px;
                    border-radius: 3px;
                }}
                .market-section.center-single {{
                    max-width: 500px;
                    margin: 0 auto;
                }}
                .market-section table {{
                    background: white;
                    width: 100%;
                    font-size: 11px;
                }}
                .market-section.compact table {{
                    font-size: 8px;
                    margin: 0;
                }}
                .market-section th,
                .market-section td {{
                    padding: 2px 6px !important;
                    white-space: nowrap;
                }}
                .market-section.compact th,
                .market-section.compact td {{
                    padding: 1px 3px !important;
                    line-height: 1.2;
                }}
                .market-section h3 {{
                    font-size: 13px;
                    margin: 0 0 6px 0;
                    font-weight: 600;
                }}
                .market-section.compact h3 {{
                    font-size: 9px;
                    margin: 0 0 3px 0;
                    font-weight: 600;
                    line-height: 1.2;
                }}
                @media only screen and (max-width: 768px) {{
                    .markets-row {{
                        grid-template-columns: 1fr !important;
                    }}
                    .market-section.center-single {{
                        max-width: 100%;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                {logo_html}
                {html_body}
            </div>
        </body>
        </html>
        """

        return html_email

    def run(self, weekend_mode=False):
        """Main execution method"""
        print("=" * 60)
        if weekend_mode:
            print("Weekend Brief Generator - Week Ahead Preview")
        else:
            print("Daily Note Generator")
        print("=" * 60)

        try:
            # Fetch global markets data
            global_markets_data = self.fetch_global_markets_data()
            global_markets_text = None

            if global_markets_data:
                global_markets_text = self.format_global_markets(global_markets_data)

            # Fetch sector performance
            print("\nFetching sector performance...")
            sector_data = self.fetch_sector_performance()
            sector_heatmap_text = self.format_sector_heatmap(sector_data) if sector_data else None

            # Fetch market news
            print("\nFetching market-moving news...")
            market_news = self.fetch_market_news()

            # Load portfolio tickers early (needed for pre-market movers)
            portfolio_tickers = []
            portfolio_excel_path = r"C:\Users\daqui\PycharmProjects\PythonProject1\Disruption Index.xlsx"
            if os.path.exists(portfolio_excel_path):
                print("\nFetching Kite Evolution Fund portfolio data...")
                portfolio_tickers = self.read_portfolio_tickers(portfolio_excel_path)

            # Fetch pre-market movers (skip on weekends) - includes portfolio tickers
            premarket_movers = []
            if not weekend_mode:
                premarket_movers = self.fetch_premarket_movers(portfolio_tickers)

            market_news_summary = None
            if market_news or premarket_movers:
                print("Generating AI summary of market news and pre-market movers...")
                market_news_summary = self.summarize_market_news(market_news, premarket_movers)

            # Fetch economic calendar
            economic_events = self.fetch_economic_calendar()
            economic_calendar_text = self.format_economic_calendar(economic_events) if economic_events else None

            # Fetch portfolio data for Kite Evolution Fund
            portfolio_news_summary = None
            earnings_calendar_text = None
            earnings_data = []
            if portfolio_tickers:
                # Fetch earnings calendar for portfolio
                earnings_data = self.fetch_earnings_calendar(portfolio_tickers)
                earnings_calendar_text = self.format_earnings_calendar(earnings_data) if earnings_data else None

                # Fetch portfolio news
                portfolio_news = self.fetch_portfolio_news(portfolio_tickers)
                portfolio_upgrades = self.fetch_portfolio_upgrades_downgrades(portfolio_tickers)
                if portfolio_news or portfolio_upgrades:
                    print("Generating AI summary of portfolio news and analyst ratings...")
                    portfolio_news_summary = self.summarize_portfolio_news(portfolio_news, portfolio_upgrades)

            # Fetch emails (skip on weekend brief)
            emails = []
            if not weekend_mode:
                print("\nFetching emails...")
                emails = self.fetch_emails_from_senders(days_back=1)

            if not emails and not weekend_mode:
                print("\nNo emails found from specified senders.")
                # Still generate note with just market news if available
                if market_news_summary or global_markets_text:
                    print("Generating daily note with market data only...")

            # Generate note
            print(f"\nGenerating {'weekend' if weekend_mode else 'daily'} note...")
            note, date_str = self.generate_daily_note(
                emails, market_news_summary, global_markets_text,
                economic_calendar_text, portfolio_news_summary,
                sector_heatmap_text, earnings_calendar_text, weekend_mode
            )

            # Save note
            self.save_note(note, date_str)

            # Generate PDF
            print("\nGenerating PDF...")
            pdf_path = self.generate_pdf(
                emails, market_news_summary, global_markets_data,
                economic_events, date_str, portfolio_news_summary,
                sector_data, earnings_data, weekend_mode, premarket_movers
            )

            # Send email if configured
            self.send_email(note, date_str, pdf_path)

            print("\n" + "=" * 60)
            print(f"{'Weekend' if weekend_mode else 'Daily'} note generation completed successfully!")
            print("=" * 60)

        except Exception as e:
            print(f"\nError: {str(e)}")
            raise


if __name__ == "__main__":
    import sys

    generator = DailyNoteGenerator()

    # Check for weekend mode flag
    weekend_mode = '--weekend' in sys.argv or '-w' in sys.argv

    # Auto-detect weekend (Saturday=5, Sunday=6)
    if not weekend_mode:
        from datetime import datetime
        today = datetime.now().weekday()
        if today in [5, 6]:  # Saturday or Sunday
            weekend_mode = True
            print("Weekend detected - running in Weekend Brief mode")

    generator.run(weekend_mode=weekend_mode)
