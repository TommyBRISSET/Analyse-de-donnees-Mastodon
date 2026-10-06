FROM apache/spark:3.5.6-python3
USER root
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY tests ./tests
ENV PYTHONPATH=/app TZ=UTC PYTHONUNBUFFERED=1
RUN mkdir -p /checkpoints /home/spark && chown -R 185:185 /app /checkpoints /home/spark
USER 185
