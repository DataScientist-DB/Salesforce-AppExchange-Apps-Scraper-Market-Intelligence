FROM apify/actor-python:3.11

# Set working directory for your Actor
WORKDIR /usr/src/app

# Copy all project files
COPY . ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# ✅ Install Playwright browser (Chromium) with all dependencies
RUN python -m playwright install --with-deps chromium

# Start the Actor
CMD ["python", "main.py"]
