# Use a lightweight Python base image to keep the container size small
FROM python:3.10-slim

# Set the main working directory inside the container
WORKDIR /app

# Copy only the requirements first to leverage Docker layer caching
COPY requirements.txt .

# Install dependencies without storing cache to reduce the final image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the machine learning models and the application code
COPY ./models ./models
COPY ./api ./api

# Document the port the application will listen on
EXPOSE 8000

# Start the FastAPI application via Uvicorn, allowing external connections (0.0.0.0)
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]