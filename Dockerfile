FROM python:3

WORKDIR /usr/src/app/

COPY requirements.txt ./
COPY src/ ./src
COPY config/ ./config

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "src/engine.py", "config/"]