FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD gunicorn company_report_backend:app --bind 0.0.0.0:${PORT:-5001}
