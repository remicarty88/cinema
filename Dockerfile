FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Та же команда запуска, что и в Procfile: сайт (gunicorn).
CMD ["sh", "-c", "gunicorn server:app --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 16 --worker-class gthread --timeout 300"]
