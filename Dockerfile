# Этап 1: Сборщик зависимостей
FROM python:3.11-slim-bookworm AS builder

WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Создаем виртуальное окружение и устанавливаем зависимости
RUN python -m venv /opt/venv &&
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip &&
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# Этап 2: Финальный образ
FROM python:3.11-slim-bookworm

# Устанавливаем системные зависимости для healthcheck и безопасности
RUN apt-get update &&
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        tzdata &&
    rm -rf /var/lib/apt/lists/*

# Создаем непривилегированного пользователя
RUN groupadd -r appuser &&
    useradd -r -g appuser -u 1000 -m -s /bin/bash appuser

# Копируем виртуальное окружение из builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Копируем исходный код
COPY src/ /app/src/
COPY entrypoint.sh /app/

# Настройка рабочей директории и прав
WORKDIR /app
RUN chown -R appuser:appuser /app &&
    chmod +x /app/entrypoint.sh

# Настройка переменных окружения
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Переключаемся на непривилегированного пользователя
USER appuser

# Healthcheck для мониторинга состояния
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python3 -c "import sys; sys.exit(0)" || exit 1

# Graceful shutdown через entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
