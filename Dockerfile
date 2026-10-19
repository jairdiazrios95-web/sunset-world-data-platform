FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY sunset /app/sunset
COPY warehouse /app/warehouse
COPY config /app/config
COPY data/sample /app/data/sample
CMD ["python", "-m", "sunset.quality", "--input", "data/sample", "--scope", "full"]
