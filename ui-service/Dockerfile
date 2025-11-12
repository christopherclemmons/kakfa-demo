FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install kafka-python fastapi uvicorn[standard]
CMD ["python", "app.py"]
