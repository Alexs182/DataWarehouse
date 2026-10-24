FROM python:3.12-slim
RUN useradd -r -u 10001 app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config ./config
COPY connectors ./connectors
COPY transforms ./transforms
COPY main.py ./main.py
ENV PYTHONUNBUFFERED=1
USER 10001
ENTRYPOINT ["python3", "main.py"]