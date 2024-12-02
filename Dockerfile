# Use the official Python image from the Docker Hub
FROM python:3.13

# Set the working directory in the container
WORKDIR /Ec2Deploy

# Copy the current directory contents into the container at /app
COPY . .

# Install required Python packages
RUN app.py  # If you have a requirements file

# Run the application
CMD ["python", "app.py"]
