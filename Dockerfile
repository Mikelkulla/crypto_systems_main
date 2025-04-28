# Use the latest Python runtime as the base image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy all project files into the container
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port for Cloud Run
EXPOSE 8080

# Command to run the Flask app
CMD ["waitress-serve", "--host=0.0.0.0", "--port=8080", "--threads=4", "app:app"]