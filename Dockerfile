# KOSPI/KOSDAQ Stock Data MCP Server
# Requires Kakao login credentials via environment variables

# Use Playwright's official Docker image for browser support
FROM mcr.microsoft.com/playwright/python:v1.49.0-noble

# Set working directory
WORKDIR /app

# Install uv for fast dependency management
RUN pip install uv

# Copy dependency files first for caching
COPY pyproject.toml requirements.txt ./

# Install dependencies
RUN uv pip install --system -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium

# Copy application code
COPY kospi_kosdaq_stock_server.py krx_data_client.py ./

# Create non-root user for security
RUN groupadd -r -g 1001 app && \
    useradd -r -u 1001 -g app -m -d /home/app -s /bin/bash app && \
    chown -R app:app /app

# Create directory for session cookies
RUN mkdir -p /home/app && chown -R app:app /home/app

# Switch to non-root user
USER app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV HOME=/home/app

# Entry point
ENTRYPOINT ["python", "-m", "kospi_kosdaq_stock_server"]
