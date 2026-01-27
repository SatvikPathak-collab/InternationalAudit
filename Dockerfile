FROM python:3.13-slim-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV STREAMLIT_SERVER_FILE_WATCHER_TYPE=none

EXPOSE 80

EXPOSE 8001

CMD ["streamlit", "run", "frontend-streamlit/main.py", "--server.address", "0.0.0.0", "--server.port", "80"]
