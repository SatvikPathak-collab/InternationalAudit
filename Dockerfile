FROM python:3.10-slim-buster

# Install system packages
RUN apt-get update && apt-get install -y build-essential

# Upgrade pip
RUN pip install --upgrade pip

# Copy project files
COPY . /app
WORKDIR /app

# Install dependencies
RUN pip install -r requirements.txt

# Expose ports
EXPOSE 8000
EXPOSE 8001

# Run Streamlit
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8000"]
