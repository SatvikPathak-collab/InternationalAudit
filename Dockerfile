FROM python:3.10-slim

RUN apt-get update

RUN pip3 install --upgrade pip

COPY . .

RUN pip install -r ./requirements.txt

EXPOSE 80

EXPOSE 8001

CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0", "--server.port", "80"]
