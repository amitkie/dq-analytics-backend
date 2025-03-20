# Use official Python image from DockerHub
FROM python:3.12-slim

# Set environment variables to improve performance and logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies required for psycopg2-binary and other packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Upgrade pip to avoid dependency resolution issues
RUN pip install --upgrade pip

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port FastAPI will run on
EXPOSE 8001

# Command to run the FastAPI app using Uvicorn
CMD ["uvicorn", "app.backendApi.index:app", "--host", "0.0.0.0", "--port", "8001"]
