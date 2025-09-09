# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.11-slim

# Install Java (using default available version) and ps command
RUN apt-get update \
    && apt-get install -y default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app
COPY . /app
ENV PYTHONPATH=/app/src
WORKDIR /app/src

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
# Create output directory and ensure appuser owns it
RUN mkdir -p /app/output && chown appuser:appuser /app/output
USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "-m", "parquet_benchmark.benchmark"]
ENTRYPOINT ["python", "-m", "parquet_benchmark.benchmark"]
