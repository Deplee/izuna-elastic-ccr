#!/bin/bash

# Настройка Elasticsearch для оптимальной производительности репликации
# Предполагается, что кластер уже установлен и запущен

# Параметры подключения
ES_HOST="localhost:9200"
ES_USER="admin"
ES_PASS="admin"

# Функция для выполнения curl-запросов к ES
es_request() {
    local method=$1
    local endpoint=$2
    local data=$3
    
    curl -X $method "http://$ES_HOST/$endpoint" \
        -H "Content-Type: application/json" \
        -u "$ES_USER:$ES_PASS" \
        ${data:+-d "$data"}
}

echo "Настройка кластера Elasticsearch..."

# 1. Настройка памяти и кучи
echo "Настройка параметров памяти..."
cat > elasticsearch.yml << EOF
# Системные настройки
bootstrap.memory_lock: true
cluster.name: replication-cluster
node.name: replication-node

# Настройки памяти и производительности
indices.memory.index_buffer_size: 30%
indices.queries.cache.size: 15%
indices.fielddata.cache.size: 20%
indices.recovery.max_bytes_per_sec: 100mb
indices.recovery.concurrent_streams: 5

# Настройки транспорта и HTTP
transport.tcp.compress: true
http.compression: true
http.compression_level: 3

# Настройки безопасности
xpack.security.enabled: true
xpack.security.transport.ssl.enabled: true

# Настройки для репликации
replication.handler.concurrent_reads: 5
thread_pool.write.size: 30
thread_pool.write.queue_size: 1000
EOF

# 2. Настройка JVM
cat > jvm.options << EOF
-Xms4g
-Xmx4g
-XX:+UseG1GC
-XX:G1ReservePercent=25
-XX:InitiatingHeapOccupancyPercent=30
-XX:+HeapDumpOnOutOfMemoryError
EOF

# 3. Настройка безопасности
echo "Настройка безопасности..."
es_request PUT "_security/user/admin" '{
  "password" : "admin",
  "roles" : [ "superuser" ],
  "full_name" : "Administrator"
}'

# 4. Оптимизация для репликации
echo "Применение оптимизаций для репликации..."
es_request PUT "_cluster/settings" '{
  "persistent": {
    "indices.recovery.max_bytes_per_sec": "100mb",
    "indices.recovery.concurrent_streams": 5,
    "cluster.routing.allocation.node_concurrent_recoveries": 5,
    "cluster.routing.allocation.cluster_concurrent_rebalance": 5
  }
}'

# 5. Настройка индексов по умолчанию
echo "Настройка шаблонов индексов..."
es_request PUT "_template/default_template" '{
  "index_patterns": ["*"],
  "settings": {
    "number_of_shards": 5,
    "number_of_replicas": 1,
    "refresh_interval": "30s",
    "index.merge.scheduler.max_thread_count": 4,
    "index.translog.durability": "async",
    "index.translog.sync_interval": "30s",
    "lifecycle.name": "default_policy"
  }
}'

# 6. Настройка политики ILM
echo "Настройка политики жизненного цикла индексов..."
es_request PUT "_ilm/policy/default_policy" '{
  "policy": {
    "phases": {
      "hot": {
        "min_age": "0ms",
        "actions": {
          "rollover": {
            "max_age": "30d",
            "max_size": "50gb",
            "max_docs": 100000000
          },
          "set_priority": {
            "priority": 100
          }
        }
      },
      "warm": {
        "min_age": "30d",
        "actions": {
          "shrink": {
            "number_of_shards": 1
          },
          "forcemerge": {
            "max_num_segments": 1
          },
          "set_priority": {
            "priority": 50
          },
          "allocate": {
            "number_of_replicas": 1
          }
        }
      },
      "cold": {
        "min_age": "60d",
        "actions": {
          "set_priority": {
            "priority": 0
          },
          "allocate": {
            "number_of_replicas": 0
          },
          "freeze": {}
        }
      },
      "delete": {
        "min_age": "90d",
        "actions": {
          "delete": {
            "delete_searchable_snapshot": true
          }
        }
      }
    }
  }
}'

# 7. Проверка статуса кластера
echo "Проверка статуса кластера..."
es_request GET "_cluster/health"

echo "Настройка завершена. Пожалуйста, перезапустите Elasticsearch для применения изменений."