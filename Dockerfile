# Многоэтапная сборка для минимального образа
FROM python:3.11-slim-bookworm AS builder

WORKDIR /app

# Устанавливаем только необходимые системные зависимости для сборки
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY requirements.txt .

# Создаем виртуальное окружение и устанавливаем зависимости
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir --no-build-isolation -r requirements.txt

# Финальный образ
FROM python:3.11-alpine3.18

# Устанавливаем минимальный набор системных зависимостей
RUN apk add --no-cache \
    ffmpeg \
    ca-certificates \
    curl \
    tzdata \
    && update-ca-certificates

# Копируем виртуальное окружение из builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Копируем исходный код
COPY src/ /app/src/
COPY entrypoint.sh /app/

# Настройка рабочей директории и прав
WORKDIR /app
RUN chmod +x /app/entrypoint.sh

# Настройка переменных окружения
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONOPTIMIZE=1

# Создаем пользователя
RUN addgroup -S appuser && adduser -S appuser -G appuser \
    && chown -R appuser:appuser /app

USER appuser

# Healthcheck для мониторинга состояния
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python3 -c "import sys, os; sys.exit(0 if os.path.exists('/tmp/healthcheck') else 1)" || exit 1

# Graceful shutdown через entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
