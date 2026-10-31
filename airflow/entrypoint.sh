#!/usr/bin/env bash
set -e

SOCK=/var/run/docker.sock

if [ -S "$SOCK" ]; then
  GID="$(stat -c '%g' "$SOCK")"
  if [ "$GID" != "0" ]; then
    GROUP="$(getent group "$GID" | cut -d: -f1 || true)"
    if [ -z "$GROUP" ]; then
      GROUP=dockerhost
      groupadd -g "$GID" "$GROUP"
    fi
    usermod -aG "$GROUP" airflow
  fi
fi

# Drop privileges and run the original Airflow entrypoint
exec gosu airflow /entrypoint "$@"