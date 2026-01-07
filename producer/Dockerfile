FROM python:3.10-slim

# Prevents Python from writing pyc files and enables unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better layer caching)
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# Run as non-root for security
RUN useradd -m appuser
USER appuser

CMD ["python", "app.py"]
