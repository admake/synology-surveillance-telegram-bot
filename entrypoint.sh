#!/bin/sh
set -e

# Создаем PID файл
PIDFILE="/tmp/app.pid"

# Graceful shutdown
graceful_shutdown() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Received shutdown signal" >&2

    if [ -f "$PIDFILE" ]; then
        PID=$(cat "$PIDFILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "Sending SIGTERM to PID: $PID" >&2
            kill -TERM "$PID"

            # Ожидание завершения
            timeout=15
            while kill -0 "$PID" 2>/dev/null && [ $timeout -gt 0 ]; do
                sleep 1
                timeout=$((timeout - 1))
            done

            # Принудительное завершение при необходимости
            if kill -0 "$PID" 2>/dev/null; then
                echo "Process not terminated, sending SIGKILL" >&2
                kill -KILL "$PID"
            fi
        fi
        rm -f "$PIDFILE"
    fi

    # Очистка временных файлов
    rm -f /tmp/healthcheck /tmp/*.mp4 2>/dev/null || true

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Shutdown complete" >&2
}

# Регистрация обработчиков сигналов
trap 'graceful_shutdown' TERM INT

# Проверка обязательных переменных
required_vars="SYNO_IP SYNO_PASS TG_TOKEN TG_CHAT_ID"
missing_vars=""

for var in $required_vars; do
    eval "value=\$$var"
    if [ -z "$value" ]; then
        missing_vars="$missing_vars $var"
    fi
done

if [ -n "$missing_vars" ]; then
    echo "ERROR: Missing required environment variables:" >&2
    for var in $missing_vars; do
        echo "  - $var" >&2
    done
    exit 1
fi

# Проверка доступности ffmpeg
if ! command -v ffmpeg >/dev/null 2>&1; then
    echo "ERROR: ffmpeg not found in PATH" >&2
    exit 1
fi

# Создание директорий
mkdir -p /data /tmp/synology_cache
chmod 777 /tmp /tmp/synology_cache 2>/dev/null || true

echo "Starting Surveillance Station Bot (Optimized)" >&2
echo "Camera ID: ${CAMERA_ID:-1}, Check interval: ${CHECK_INTERVAL:-10}s" >&2

# Запуск приложения в фоне
python3 -u /app/src/main.py &
PYTHON_PID=$!

# Сохранение PID
echo $PYTHON_PID >"$PIDFILE"

# Ожидание завершения
wait $PYTHON_PID
EXIT_CODE=$?

# Очистка
rm -f "$PIDFILE"

echo "Application exited with code: $EXIT_CODE" >&2
exit $EXIT_CODE
