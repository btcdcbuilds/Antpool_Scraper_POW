FROM mcr.microsoft.com/playwright/python:v1.35.0-focal

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Install Playwright browsers
RUN python -m playwright install chromium

# Make start script executable
RUN chmod +x start.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Command to run when container starts
CMD ["./start.sh"]
