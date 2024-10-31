# Use the official Python image
FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code and JSON file
COPY streaming-app-standalone.py .
COPY optimal_route.json .

# Expose the Streamlit port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "streaming-app-standalone.py", "--server.port=8501", "--server.address=0.0.0.0"]
