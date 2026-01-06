# Syslog UDP receiver to DuckDB

## Build and run the app

```
go build syslog-to-duckdb.go
sudo ./syslog-to-duckdb.go
```

## Configure rsyslog on your system to send logs

Run:

```
sudo nano /etc/rsyslog.conf
```

Add:

```
# UDP server
module(load="imudp")
input(type="imudp" port="514")
*.* @127.0.0.1:514
```

Run:

```
sudo systemctl restart rsyslog
```

Test:

```
logger "My testing log"
```

## Read the logs from syslog.db

```
python3 read-db.py
```
