FROM apify/actor-python:3.11

WORKDIR /usr/src/app
COPY . ./

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN python -m pip install --no-cache-dir -r requirements.txt

# Playwright + Chromium
RUN python -m playwright install --with-deps chromium

CMD ["python", "main.py"]