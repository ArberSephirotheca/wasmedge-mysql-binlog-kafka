wasmedge --env "SQL_USERNAME=root" --env "SQL_PASSWORD=password" --env "SQL_PORT=3306" --env "SQL_HOSTNAME=mysql" --env "SQL_DATABASE=mysql" --env "KAFKA_URL=localhost:9092" --env "SLEEP_TIME=10000"  target/wasm32-wasi/debug/mysql-binlog-kafka.wasm
