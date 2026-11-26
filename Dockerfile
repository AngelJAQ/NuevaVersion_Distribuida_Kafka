FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# El comando concreto (router / nodo de cálculo / cliente) lo define docker-compose.yml
CMD ["python", "router.py"]
