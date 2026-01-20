
#!/bin/bash

pid=$(ps -ef | grep '[/opt/venv/bin/]uvicorn app.main:app' | head -n 1 | awk '{print $2}')

if [ -n "$pid" ]; then
  echo "KILLING process $pid (app.main:app)!"
  kill -9 "$pid"
else
  echo "No app.main:app process found!"
fi

BASE_DIR=$(dirname $(realpath "$0"))
export PYTHONPATH=$BASE_DIR:$PYTHONPATH

uvicorn app.main:app --host 0.0.0.0 --port 8739
