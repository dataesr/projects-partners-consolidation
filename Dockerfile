FROM python:3.9-slim
WORKDIR /app
COPY . /app/
ENV PYTHONPATH=/app
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000
CMD ["python", "project/server/main/projects_partners_flask_app.py"]
