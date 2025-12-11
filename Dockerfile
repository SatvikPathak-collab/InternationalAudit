FROM python:3.10-slim-bullseye

RUN apt-get update

RUN pip3 install --upgrade pip

COPY . .

RUN pip install -r ./requirements.txt

ENV STREAMLIT_SERVER_FILE_WATCHER_TYPE=none

EXPOSE 80

EXPOSE 8001

CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0", "--server.port", "80"]
