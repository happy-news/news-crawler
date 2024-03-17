# Use an official Python runtime as a base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-root user and switch to it
RUN useradd -u 10001 -m myuser
USER 10001

# Run crawler.py when the container launches
CMD ["python", "crawler.py"]
