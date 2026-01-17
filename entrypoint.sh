#!/bin/bash
set -e

# Функция для graceful shutdown
graceful_shutdown() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Received SIGTERM signal, initiating graceful shutdown..." >&2

    # Отправляем SIGTERM дочернему процессу Python
    if [ -n "$python_pid" ]; then
        kill -TERM "$python_pid" 2>/dev/null &&
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Sent SIGTERM to Python process (PID: $python_pid)" >&2

        # Ждем завершения (максимум 25 секунд)
        timeout=25
        while kill -0 "$python_pid" 2>/dev/null && [ $timeout -gt 0 ]; do
            sleep 1
            timeout=$((timeout - 1))
        done

        # Если процесс еще жив, отправляем SIGKILL
        if kill -0 "$python_pid" 2>/dev/null; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - Process not terminated, sending SIGKILL" >&2
            kill -KILL "$python_pid" 2>/dev/null
        fi

        wait "$python_pid" 2>/dev/null
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Application shutdown complete" >&2
    exit 0
}

# Ловим сигналы остановки
trap 'graceful_shutdown' SIGTERM SIGINT

# Проверяем обязательные переменные окружения
required_vars=("SYNO_IP" "SYNO_PASS" "TG_TOKEN" "TG_CHAT_ID")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -gt 0 ]; then
    echo "ERROR: Missing required environment variables:" >&2
    printf '  - %s\n' "${missing_vars[@]}" >&2
    echo "Please check your docker-compose or .env file" >&2
    exit 1
fi

# Создаем директорию для данных если не существует
mkdir -p /data

echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting Surveillance Station Telegram Bot" >&2
echo "$(date '+%Y-%m-%d %H:%M:%S') - Camera ID: ${CAMERA_ID:-1}, Check interval: ${CHECK_INTERVAL:-30}s" >&2

# Запускаем основной скрипт в фоновом режиме
python3 -u /app/src/main.py &
python_pid=$!

# Ждем завершения процесса Python и обрабатываем ошибки
if wait "$python_pid"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Python script exited successfully" >&2
    exit 0
else
    exit_code=$?
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Python script exited with code: $exit_code" >&2
    exit $exit_code
fi
