from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import argparse
import warnings

def analyze_elasticsearch_indices(host, username, password, index_pattern='*'):
    """
    Анализирует индексы в Elasticsearch кластере с проверкой best practices
    """
    try:
        # Подключение к Elasticsearch с аутентификацией
        es = Elasticsearch(
            hosts=[host],
            basic_auth=(username, password),
            verify_certs=False,
            timeout=30
        )
        
        if not es.ping():
            print("Ошибка: Не удалось подключиться к Elasticsearch")
            return

        # Получаем информацию о кластере
        cluster_info = es.info()
        cluster_health = es.cluster.health()
        nodes_stats = es.nodes.stats()
        
        print("\n=== Общая информация о кластере ===")
        print(f"Имя кластера: {cluster_info['cluster_name']}")
        print(f"Версия Elasticsearch: {cluster_info['version']['number']}")
        print(f"Статус кластера: {cluster_health['status']}")
        print(f"Количество нод: {cluster_health['number_of_nodes']}")
        indices = es.cat.indices(index=index_pattern, format='json', h='index,docs.count')
        print(f"Количество индексов: {len(indices)}")
        total_docs = sum(int(idx.get('docs.count', 0)) for idx in indices)
        print(f"Количество документов: {total_docs}")
        
        # Получение списка всех индексов с детальной информацией
        indices = es.cat.indices(index=index_pattern, format='json', h='index,health,status,pri,rep,store.size,docs.count,creation.date')
        aliases = es.cat.aliases(format='json')
        
        print(f"\n=== Анализ индексов на соответствие best practices (шаблон: {index_pattern}) ===\n")
        
        total_docs = 0
        total_size = 0
        issues_found = 0
        
        for idx in sorted(indices, key=lambda x: x['index']):
            index_name = idx['index']
            health = idx.get('health', 'unknown')
            status = idx.get('status', 'unknown')
            primaries = idx.get('pri', '0')
            replicas = idx.get('rep', '0')
            size = idx.get('store.size', '0b')
            docs = idx.get('docs.count', '0')
            creation_date = idx.get('creation.date', '')
            
            # Конвертируем размер и дату создания
            try:
                size_bytes = parse_size(size)
                size_readable = sizeof_fmt(size_bytes)
            except:
                size_readable = size
                size_bytes = 0
                
            try:
                creation_date_dt = datetime.fromtimestamp(int(creation_date)/1000)
                age_days = (datetime.now() - creation_date_dt).days
            except:
                creation_date_dt = "unknown"
                age_days = 0
            
            # Проверка best practices
            issues = []
            
            # 1. Проверка количества реплик
            if int(replicas) < 1:
                issues.append("Нет реплик (риск потери данных)")
            
            # 2. Проверка размера шардов
            try:
                shard_size = size_bytes / int(primaries) if int(primaries) > 0 else 0
                if shard_size > 50 * 1024 * 1024 * 1024:  # 50GB
                    issues.append(f"Слишком большие шарды: {sizeof_fmt(shard_size)}")
            except:
                pass
            
            # 3. Проверка устаревших индексов
            if age_days > 365:
                issues.append(f"Очень старый индекс: {age_days} дней")
            
            # 4. Проверка на наличие алиасов
            has_alias = any(a['alias'] == index_name for a in aliases)
            if not has_alias:
                issues.append("Нет алиаса (усложняет реиндексацию)")
            
            # 5. Проверка naming convention
            if not any(delim in index_name for delim in ['-', '_']):
                issues.append("Нестандартное имя индекса (рекомендуется использовать разделители)")

            # 6. Проверка mapping'а
            try:
                mapping = es.indices.get_mapping(index=index_name)
                props = mapping[index_name]['mappings'].get('properties', {})
                fields_count = len(props)
                if fields_count > 1000:
                    issues.append(f"Слишком много полей в индексе: {fields_count}")
                # Dynamic mapping
                if mapping[index_name]['mappings'].get('dynamic', True):
                    issues.append("Включен dynamic mapping (может привести к неожиданному росту mapping'а)")
                # text без keyword
                for field, desc in props.items():
                    if desc.get('type') == 'text' and 'fields' not in desc:
                        issues.append(f"Поле '{field}' типа text без keyword subfield (агрегации невозможны)")
                        break
            except Exception as e:
                issues.append(f"Не удалось получить mapping: {str(e)}")

            # 7. Проверка refresh_interval и number_of_shards
            try:
                settings = es.indices.get_settings(index=index_name)
                idx_settings = settings[index_name]['settings']['index']
                refresh_interval = idx_settings.get('refresh_interval', '1s')
                if refresh_interval == '1s':
                    issues.append("refresh_interval по умолчанию (1s) — можно увеличить для bulk загрузки")
                # Количество шардов относительно размера
                if size_bytes > 0 and int(primaries) > 0:
                    if size_bytes / int(primaries) < 1024*1024*1024:  # <1GB на шард
                        issues.append("Слишком мало данных на шард (<1GB)")
            except Exception as e:
                issues.append(f"Не удалось получить settings: {str(e)}")

            # 8. Проверка deprecated features
            try:
                mapping = es.indices.get_mapping(index=index_name)
                if '_all' in mapping[index_name]['mappings']:
                    issues.append("Используется устаревшее поле _all")
                if '_type' in mapping[index_name]['mappings']:
                    issues.append("Используется устаревшее поле _type")
            except:
                pass

            # 9. Проверка write alias
            try:
                write_aliases = [a['alias'] for a in aliases if a.get('is_write_index') == 'true' and a['index'] == index_name]
                if not write_aliases:
                    issues.append("Нет write alias для индекса (рекомендуется для rollover)")
            except:
                pass

            # 10. Проверка количества сегментов
            try:
                segments = es.indices.segments(index=index_name)
                seg_count = 0
                for shard in segments['indices'][index_name]['shards'].values():
                    for seg in shard:
                        seg_count += len(seg['segments'])
                if seg_count > int(primaries) * 50:
                    issues.append(f"Слишком много сегментов: {seg_count}")
            except:
                pass

            # 10.1. Проверка на помеченные на удаление документы
            try:
                stats = es.indices.stats(index=index_name, metric='docs')
                deleted_docs = stats['indices'][index_name]['primaries']['docs']['deleted']
                total_docs_in_index = stats['indices'][index_name]['primaries']['docs']['count']
                if total_docs_in_index + deleted_docs > 0:
                    deleted_ratio = deleted_docs / (total_docs_in_index + deleted_docs)
                    if deleted_ratio > 0.1:  # Более 10% помечено на удаление
                        issues.append(f"Много помеченных на удаление документов: {deleted_docs} ({deleted_ratio:.1%})")
            except Exception as e:
                issues.append(f"Не удалось получить статистику deleted docs: {str(e)}")

            # 11. Проверка неиспользуемых индексов (не обновлялся > 90 дней)
            try:
                stats = es.indices.stats(index=index_name, metric='indexing')
                last_index_time = 0
                for shard in stats['indices'][index_name]['total']['indexing']['index_time_in_millis'], stats['indices'][index_name]['total']['indexing']['throttle_time_in_millis']:
                    if shard and shard > last_index_time:
                        last_index_time = shard
                if last_index_time:
                    last_index_dt = datetime.now() - timedelta(milliseconds=last_index_time)
                    if (datetime.now() - last_index_dt).days > 90:
                        issues.append("Индекс не обновлялся более 90 дней")
            except:
                pass

            # 12. Проверка системных индексов
            if index_name.startswith('.'):
                if int(primaries) > 1 or int(replicas) > 1:
                    issues.append("Системный индекс с избыточным числом шардов или реплик")
            
            # Вывод информации об индексе
            print(f"Индекс: {index_name}")
            print(f"  • Здоровье: {health}, Статус: {status}, Документов: {docs}, Размер: {size_readable}")
            print(f"  • Шарды: {primaries} первичных, {replicas} реплик, Создан: {creation_date_dt} ({age_days} дней назад)")
            
            if issues:
                issues_found += len(issues)
                print("  • Проблемы:")
                for issue in issues:
                    print(f"    - {issue}")
            else:
                print("  • Соответствует best practices")
            
            print("")
            
            try:
                total_docs += int(docs)
                total_size += size_bytes
            except:
                pass
        
        # Анализ шаблонов индексов
        try:
            templates = es.indices.get_template()
            print("\n=== Анализ шаблонов индексов ===")
            for name, template in templates.items():
                print(f"\nШаблон: {name}")
                if 'settings' in template:
                    print("  Настройки:")
                    print(f"    • Количество шардов: {template['settings'].get('index.number_of_shards', 'не указано')}")
                    print(f"    • Количество реплик: {template['settings'].get('index.number_of_replicas', 'не указано')}")
                
                # Проверка на наличие ILM политики
                if 'settings' in template and 'index.lifecycle.name' in template['settings']:
                    print(f"  • Используется ILM политика: {template['settings']['index.lifecycle.name']}")
                else:
                    print("  • Внимание: не используется ILM политика")
                    issues_found += 1
        except Exception as e:
            print(f"\nНе удалось получить шаблоны индексов: {str(e)}")
        
        # Общие рекомендации
        print("\n=== Рекомендации ===")
        if issues_found == 0:
            print("Все индексы соответствуют best practices!")
        else:
            print(f"Найдено {issues_found} проблемных мест в индексах")
        
        # Рекомендации по кластеру
        print("\nОбщие рекомендации по кластеру:")
        if cluster_health['status'] != 'green':
            print(f"- Статус кластера {cluster_health['status']}. Рекомендуется привести к green")
        
        if int(cluster_health['number_of_nodes']) < 3:
            print("- Маловато нод для отказоустойчивости (рекомендуется минимум 3)")
        
        # Проверка JVM heap
        for node_id, node in nodes_stats['nodes'].items():
            jvm_max = node['jvm']['mem']['heap_max_in_bytes']
            jvm_used = node['jvm']['mem']['heap_used_in_bytes']
            usage_percent = (jvm_used / jvm_max) * 100
            if usage_percent > 75:
                print(f"- Высокое использование JVM heap на ноде {node['name']}: {usage_percent:.1f}%")

        # Проверка ILM политик
        try:
            ilm_policies = es.ilm.get_lifecycle()
            if ilm_policies:
                print(f"\nНайдено ILM-политик: {len(ilm_policies)}")
                print("Список ILM-политик:")
                for pol in ilm_policies:
                    print(f"  - {pol}")
            else:
                print("\nВнимание: не найдено ни одной ILM-политики!")
                print("Рекомендуется настроить ILM для управления жизненным циклом индексов.")
        except Exception as e:
            print(f"\nОшибка при получении ILM-политик: {str(e)}")

        # Проверка индексов без ILM
        try:
            indices_settings = es.indices.get_settings(index='*', filter_path='*.settings.index.lifecycle.name')
            indices_without_ilm = [idx for idx, val in indices_settings.items() if 'lifecycle' not in val['settings']['index'] or not val['settings']['index']['lifecycle'].get('name')]
            if indices_without_ilm:
                print(f"\nВнимание: {len(indices_without_ilm)} индексов без ILM-политики!")
                for idx in indices_without_ilm[:10]:
                    print(f"  - {idx}")
                if len(indices_without_ilm) > 10:
                    print("  ...")
                print("Рекомендуется назначить ILM-политику для всех индексов.")
            else:
                print("\nВсе индексы используют ILM-политику.")
        except Exception as e:
            print(f"\nОшибка при анализе ILM-политик у индексов: {str(e)}")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")

def parse_size(size_str):
    """Конвертирует строку размера (например, 5.2gb) в байты"""
    units = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4}
    size_str = size_str.lower().strip()
    num = float(size_str[:-2]) if size_str[-2:] in units else float(size_str[:-1])
    unit = size_str[-2:] if size_str[-2:] in units else size_str[-1:]
    return int(num * units[unit])

def sizeof_fmt(num, suffix='B'):
    """Конвертирует размер в байтах в человеко-читаемый формат"""
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    
    parser = argparse.ArgumentParser(description='Анализ индексов Elasticsearch с проверкой best practices')
    parser.add_argument('--host', required=True, help='URL Elasticsearch сервера (например, http://localhost:9200)')
    parser.add_argument('--username', required=True, help='Имя пользователя для аутентификации')
    parser.add_argument('--password', required=True, help='Пароль для аутентификации')
    parser.add_argument('--index', required=False, default='*', help='Имя или шаблон индекса для анализа (например, log-*)')
    
    args = parser.parse_args()
    
    analyze_elasticsearch_indices(args.host, args.username, args.password, args.index)
