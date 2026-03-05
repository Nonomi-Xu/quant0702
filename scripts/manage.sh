#!/bin/bash

cd ~/quant

case "$1" in
  start)
    echo "启动Dagster服务..."
    sudo systemctl start dagster-webserver
    sudo systemctl start dagster-daemon
    ;;
  stop)
    echo "停止Dagster服务..."
    sudo systemctl stop dagster-webserver
    sudo systemctl stop dagster-daemon
    ;;
  restart)
    echo "重启Dagster服务..."
    sudo systemctl restart dagster-webserver
    sudo systemctl restart dagster-daemon
    ;;
  status)
    sudo systemctl status dagster-webserver
    sudo systemctl status dagster-daemon
    ;;
  logs)
    sudo journalctl -u dagster-webserver -f
    ;;
  test)
    source venv/bin/activate
    python -c "from quant_code.duckdb_io import DuckDBResource; db=DuckDBResource(); print('✅ 连接成功'); db.close()"
    ;;
  *)
    echo "用法: $0 {start|stop|restart|status|logs|test}"
    exit 1
esac