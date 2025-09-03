# Dockerfile
FROM python:3.8-slim

# Set working directory in the container...
WORKDIR /app/local

# Copy the requirements file...
COPY requirements.txt /app/local/

# Install dependencies...
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application...
COPY . /app/local/

# Set the command to run the Python script...
CMD ["python3", "data_preprocessing_operator.py"]
