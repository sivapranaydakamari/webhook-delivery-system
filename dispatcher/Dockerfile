FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# The dispatcher is a plain Python worker (no HTTP server needed)
CMD ["python", "-u", "main.py"]
