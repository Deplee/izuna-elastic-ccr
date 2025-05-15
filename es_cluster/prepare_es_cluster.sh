#!/bin/bash

# Параметры подключения
ES_HOST="localhost:9200"
ES_USER="admin"
ES_PASS="admin"

# Функция для выполнения curl-запросов к ES
es_request() {
    local method=$1
    local endpoint=$2
    local data=$3
    
    response=$(curl -s -w "\n%{http_code}" -X $method "http://$ES_HOST/$endpoint" \
        -H "Content-Type: application/json" \
        -u "$ES_USER:$ES_PASS" \
        ${data:+-d "$data"})
    
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed \$d)
    
    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
        echo "$body"
    else
        echo "Error: HTTP $http_code"
        echo "Response: $body"
        return 1
    fi
}

# Функция для управления индексом
function manage_index() {
    local action=$1
    local index_name=$2
    
    case $action in
        "close")
            es_request "POST" "$index_name/_close" ""
            ;;
        "read_only")
            es_request "PUT" "$index_name/_settings" '{"index.blocks.write":true}'
            ;;
        "read_write")
            es_request "PUT" "$index_name/_settings" '{"index.blocks.write":false}'
            ;;
        "forcemerge_docs")
            es_request "POST" "$index_name/_forcemerge?only_expunge_deletes=true" ""
            ;;
        "forcemerge_segments")
            es_request "POST" "$index_name/_forcemerge?max_num_segments=1" ""
            ;;
        "refresh")
            es_request "POST" "$index_name/_refresh" ""
            ;;
        "segments_info")
            es_request "GET" "$index_name/_stats/segments" ""
            ;;
        "deleted_docs_info")
            es_request "GET" "_cat/indices/$index_name?v&h=index,docs.count,docs.deleted" ""
            ;;
        *)
            echo "Unknown action: $action"
            exit 1
            ;;
    esac
}

# Функция для управления кластером
function manage_cluster() {
    local action=$1
    
    case $action in
        "health")
            es_request "GET" "_cluster/health" ""
            ;;
        "settings")
            es_request "GET" "_cluster/settings" ""
            ;;
        "state")
            es_request "GET" "_cluster/state" ""
            ;;
        "stats")
            es_request "GET" "_cluster/stats" ""
            ;;
        *)
            echo "Unknown cluster action: $action"
            exit 1
            ;;
    esac
}

# Функция для управления политиками и шаблонами
function manage_policies() {
    local action=$1
    
    case $action in
        "create_cleanup_policy")
            # Создание политики очистки
            cleanup_policy='{
                "policy": {
                    "phases": {
                        "hot": {
                            "min_age": "0ms",
                            "actions": {
                                "set_priority": {
                                    "priority": 100
                                }
                            }
                        },
                        "warm": {
                            "min_age": "2d",
                            "actions": {
                                "forcemerge": {
                                    "max_num_segments": 1
                                }
                            }
                        },
                        "cold": {
                            "min_age": "4d",
                            "actions": {
                                "allocate": {
                                    "number_of_replicas": 0,
                                    "require": {
                                        "data": "cold"
                                    }
                                },
                                "set_priority": {
                                    "priority": 0
                                }
                            }
                        }
                    }
                }
            }'
            es_request "PUT" "_ilm/policy/cleanup_policy" "$cleanup_policy"
            ;;
        "create_template")
            # Создание шаблона индекса с высоким приоритетом
            template='{
                "index_patterns": ["*"],
                "template": {
                    "settings": {
                        "index.lifecycle.name": "cleanup_policy",
                        "index.lifecycle.rollover_alias": "test"
                    }
                },
                "priority": 500
            }'
            es_request "PUT" "_index_template/template_with_cleanup_policy" "$template"
            ;;
        "apply_policy_to_all")
            # Применение политики ко всем существующим индексам
            all_indices=$(es_request "GET" "_cat/indices?format=json" "" | jq -r '.[].index')
            for index in $all_indices; do
                es_request "PUT" "$index/_settings" '{"index.lifecycle.name":"cleanup_policy"}'
            done
            ;;
        "list_policies")
            es_request "GET" "_ilm/policy" ""
            ;;
        "list_templates")
            es_request "GET" "_index_template" ""
            ;;
        *)
            echo "Unknown policy action: $action"
            exit 1
            ;;
    esac
}

# Основная функция
if [ $# -lt 1 ]; then
    echo "Usage: $0 <function> [arguments...]"
    echo "Available functions:"
    echo "  manage_index <action> <index_name>"
    echo "  manage_cluster <action>"
    echo "  manage_policies <action>"
    exit 1
fi

function_name=$1
shift

case $function_name in
    "manage_index")
        if [ $# -ne 2 ]; then
            echo "Usage: $0 manage_index <action> <index_name>"
            exit 1
        fi
        manage_index "$1" "$2"
        ;;
    "manage_cluster")
        if [ $# -ne 1 ]; then
            echo "Usage: $0 manage_cluster <action>"
            exit 1
        fi
        manage_cluster "$1"
        ;;
    "manage_policies")
        if [ $# -ne 1 ]; then
            echo "Usage: $0 manage_policies <action>"
            exit 1
        fi
        manage_policies "$1"
        ;;
    *)
        echo "Unknown function: $function_name"
        exit 1
        ;;
esac

# Примеры использования:
# ./prepare_es_cluster.sh
# manage_index "read_only" "my-index"      # Перевести в режим только для чтения
# manage_index "forcemerge_segments" "my-index"  # Выполнить forcemerge
# manage_index "read_write" "my-index"     # Вернуть в режим чтения-записи
# manage_cluster "health"                  # Проверить статус кластера
# manage_cluster "settings"                # Получить настройки кластера
# manage_policies "create_cleanup_policy"  # Создать политику очистки
# manage_policies "create_template"        # Создать шаблон индекса
# manage_policies "apply_policy_to_all"    # Применить политику ко всем индексам
# manage_policies "list_policies"          # Показать все политики
# manage_policies "list_templates"         # Показать все шаблоны
