FROM python:3.13-slim

WORKDIR /app

# # Install system dependencies including curl and gpg
# RUN apt-get update && apt-get install -y \
#     wget \
#     curl \
#     gnupg \
#     unzip \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# # Variables de version
# ENV CHROME_VERSION=124.0.6367.91
# ENV CHROMEDRIVER_VERSION=124.0.6367.91

# # Install Chrome
# RUN curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg \
#     && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
#     && apt-get update \
#     && apt-get install -y google-chrome-stable \
#     && rm -rf /var/lib/apt/lists/*

# # Install ChromeDriver (version fixe)
# RUN wget -q "https://storage.googleapis.com/chrome-for-testing-public/136.0.7103.49/linux64/chromedriver-linux64.zip" \
#     && unzip chromedriver_linux64.zip -d /usr/local/bin \
#     && rm chromedriver_linux64.zip \
#     && chmod +x /usr/local/bin/chromedriver

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Run the collector service
CMD ["python", "main.py"]
