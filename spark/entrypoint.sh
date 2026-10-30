#!/bin/sh
set -e

host="${S3_HOST:-object-storage}"
port="${S3_PORT:-9000}"

echo "Waiting for object storage at ${host}:${port}..."
i=0
while [ "$i" -lt 60 ]; do
  if python3 -c "import socket; socket.create_connection(('${host}', int('${port}')), 2).close()" 2>/dev/null; then
    echo "Object storage is reachable."
    exec "$@"
  fi
  i=$((i + 1))
  sleep 1
done

echo "Timed out waiting for object storage at ${host}:${port}" >&2
exit 1
