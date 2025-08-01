CONFIG_FILE="/etc/pgbouncer/pgbouncer.ini"

# Buat section jika belum ada
crudini --set "$CONFIG_FILE" pgbouncer pool_mode transaction
crudini --set "$CONFIG_FILE" pgbouncer max_client_conn 1000
crudini --set "$CONFIG_FILE" pgbouncer default_pool_size 40
crudini --set "$CONFIG_FILE" pgbouncer max_db_connections 40
crudini --set "$CONFIG_FILE" pgbouncer reserve_pool_size 10
crudini --set "$CONFIG_FILE" pgbouncer reserve_pool_timeout 3

# Performance tuning
crudini --set "$CONFIG_FILE" pgbouncer server_lifetime 3600
crudini --set "$CONFIG_FILE" pgbouncer server_idle_timeout 600
crudini --set "$CONFIG_FILE" pgbouncer query_timeout 30
crudini --set "$CONFIG_FILE" pgbouncer query_wait_timeout 120
crudini --set "$CONFIG_FILE" pgbouncer client_idle_timeout 0
crudini --set "$CONFIG_FILE" pgbouncer client_login_timeout 60

# Logging
crudini --set "$CONFIG_FILE" pgbouncer log_connections 0
crudini --set "$CONFIG_FILE" pgbouncer log_disconnections 0
crudini --set "$CONFIG_FILE" pgbouncer log_pooler_errors 1
crudini --set "$CONFIG_FILE" pgbouncer log_stats 1
crudini --set "$CONFIG_FILE" pgbouncer stats_period 60

# Memory optimization
crudini --set "$CONFIG_FILE" pgbouncer pkt_buf 4096
crudini --set "$CONFIG_FILE" pgbouncer listen_backlog 512
crudini --set "$CONFIG_FILE" pgbouncer sbuf_loopcnt 5
crudini --set "$CONFIG_FILE" pgbouncer tcp_defer_accept 45
crudini --set "$CONFIG_FILE" pgbouncer tcp_keepalive 1
crudini --set "$CONFIG_FILE" pgbouncer tcp_keepcnt 3
crudini --set "$CONFIG_FILE" pgbouncer tcp_keepidle 600
crudini --set "$CONFIG_FILE" pgbouncer tcp_keepintvl 30

