# Use official Python image from DockerHub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port FastAPI will run on
EXPOSE 8001

# Command to run the FastAPI app using Uvicorn
CMD ["uvicorn", "app.backendApi.index:app", "--host", "0.0.0.0", "--port", "8001"]
