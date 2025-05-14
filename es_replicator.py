import logging
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers
from elasticsearch import exceptions as es_exceptions
import time
import re
import hashlib
import argparse
import yaml
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('es_full_replication.log'),
        logging.StreamHandler()
    ]
)

class ESFullReplicator:
    def __init__(self, source_config: Dict[str, Any], target_config: Dict[str, Any]):
        # Удаляем timeout из конфигов, если он есть
        source_config = {k: v for k, v in source_config.items() if k != 'timeout'}
        target_config = {k: v for k, v in target_config.items() if k != 'timeout'}
        
        # Убедимся, что используем basic_auth вместо http_auth
        if 'http_auth' in source_config:
            source_config['basic_auth'] = source_config.pop('http_auth')
        if 'http_auth' in target_config:
            target_config['basic_auth'] = target_config.pop('http_auth')

        # Базовые параметры для обоих клиентов
        common_config = {
            'verify_certs': False,
            'request_timeout': 300,  # Увеличиваем таймаут до 5 минут
            'retry_on_timeout': True,
            'max_retries': 3,
            'http_compress': True,
            'headers': {
                'Accept': 'application/vnd.elasticsearch+json; compatible-with=8',
                'Content-Type': 'application/json'
            }
        }
        
        # Создаем клиенты с явным указанием параметров
        self.source_es = Elasticsearch(
            hosts=source_config['hosts'],
            basic_auth=source_config['basic_auth'],
            **common_config
        )
        
        self.target_es = Elasticsearch(
            hosts=target_config['hosts'],
            basic_auth=target_config['basic_auth'],
            **common_config
        )
        
        self.scroll_time = "60m"  # Увеличенное время для больших индексов
        self.batch_size = 10000   # Максимальный размер batch в Elasticsearch
        self.max_workers = 100    # Увеличиваем количество потоков для документов
        self.max_index_workers = 5  # Количество параллельных индексов
        self.exclude_patterns = [
            r'^\.',              # Системные индексы
            r'_ilm_history.*',   # Индексы управления жизненным циклом
            r'_watcher_history.*',
            r'_security_audit_log.*'
        ]
        self.timestamp_field = None  # Определяется автоматически
        self.checksum_field = "_doc_hash"
        self.last_sync_time = None
        self.doc_count_threshold = 1000000  # Порог для логирования прогресса
        
        self._check_connections()
        self._init_last_sync_time()

    def _check_connections(self) -> None:
        """Проверка соединения с кластерами"""
        for conn, name in [(self.source_es, "source"), (self.target_es, "target")]:
            try:
                # Используем прямой GET запрос
                response = conn.perform_request(
                    method='GET',
                    path='/',
                    headers={'Accept': '*/*'}
                )
                
                # В новой версии клиента response - это уже словарь с данными
                info = response
                logging.info(f"Connected to {name} cluster (v{info['version']['number']}, cluster: {info['cluster_name']})")
                
            except Exception as e:
                logging.error(f"Connection error to {name} cluster: {str(e)}")
                # Добавляем больше информации об ошибке
                if hasattr(e, 'response'):
                    logging.error(f"Response body: {e.response}")
                raise

    def _init_last_sync_time(self) -> None:
        """Инициализация времени последней синхронизации"""
        try:
            if self.target_es.indices.exists(index=".replication_meta"):
                doc = self.target_es.get(index=".replication_meta", id="last_sync")['_source']
                self.last_sync_time = datetime.fromisoformat(doc['timestamp'])
                logging.info(f"Last sync time loaded: {self.last_sync_time}")
        except Exception:
            self.last_sync_time = datetime.now() - timedelta(days=1)
            logging.info("No previous sync time found, using 24 hours ago")

    def _save_sync_time(self) -> None:
        """Сохранение времени последней синхронизации"""
        try:
            if not self.target_es.indices.exists(index=".replication_meta"):
                self.target_es.indices.create(index=".replication_meta")
            
            self.target_es.index(
                index=".replication_meta",
                id="last_sync",
                body={"timestamp": datetime.now().isoformat()}
            )
        except Exception as e:
            logging.error(f"Failed to save sync time: {str(e)}")

    def _should_replicate(self, index_name: str) -> bool:
        """Определяем, нужно ли реплицировать индекс"""
        return not any(re.match(pattern, index_name) for pattern in self.exclude_patterns)

    def _detect_timestamp_field(self, index: str) -> Optional[str]:
        """Автоматическое определение поля с timestamp"""
        try:
            mappings = self.source_es.indices.get_mapping(index=index)
            properties = mappings[index]['mappings'].get('properties', {})
            
            # Ищем поле типа date
            for field, props in properties.items():
                if props.get('type') == 'date':
                    return field
            
            # Проверяем вложенные поля
            for field, props in properties.items():
                if props.get('type') == 'object' and 'properties' in props:
                    for subfield, subprops in props['properties'].items():
                        if subprops.get('type') == 'date':
                            return f"{field}.{subfield}"
            
            return None
        except Exception as e:
            logging.warning(f"Failed to detect timestamp field for {index}: {str(e)}")
            return None

    def get_all_indices(self) -> List[str]:
        """Получаем список всех индексов для репликации"""
        try:
            indices = self.source_es.cat.indices(format='json', h='index,creation.date')
            return [idx['index'] for idx in indices if self._should_replicate(idx['index'])]
        except Exception as e:
            logging.error(f"Error getting indices list: {str(e)}")
            raise

    def get_missing_indices(self) -> List[str]:
        """Получаем индексы, отсутствующие в target кластере"""
        try:
            source_indices = set(idx['index'] for idx in 
                               self.source_es.cat.indices(format='json', h='index')
                               if self._should_replicate(idx['index']))
            
            target_indices = set(idx['index'] for idx in 
                               self.target_es.cat.indices(format='json', h='index'))
            
            return list(source_indices - target_indices)
        except Exception as e:
            logging.error(f"Error getting missing indices: {str(e)}")
            raise

    def replicate_all_indices(self, continuous: bool = False, interval: int = 3600) -> None:
        """Основной цикл репликации всех индексов"""
        try:
            while True:
                start_time = datetime.now()
                indices = self.get_all_indices()
                
                if not indices:
                    logging.warning("No indices found for replication")
                    time.sleep(interval)
                    continue
                
                logging.info(f"Starting parallel replication of {len(indices)} indices")
                
                total_stats = {
                    'processed': 0,
                    'updated': 0,
                    'failed': 0,
                    'checked': 0,
                    'mismatches': 0
                }
                
                # Используем ThreadPoolExecutor для параллельной репликации индексов
                with ThreadPoolExecutor(max_workers=self.max_index_workers) as index_executor:
                    index_futures = []
                    
                    for index in indices:
                        future = index_executor.submit(self._replicate_single_index, index)
                        index_futures.append((index, future))
                    
                    # Обрабатываем результаты по мере завершения
                    for index, future in index_futures:
                        try:
                            stats = future.result()
                            for k in total_stats:
                                total_stats[k] += stats.get(k, 0)
                            logging.info(f"Completed replication of index: {index}")
                        except Exception as e:
                            logging.error(f"Failed to replicate index {index}: {str(e)}")
                            total_stats['failed'] += 1
                
                duration = (datetime.now() - start_time).total_seconds()
                logging.info(
                    f"Parallel replication completed. Documents: {total_stats['processed']} "
                    f"(updated: {total_stats['updated']}), Errors: {total_stats['failed']}, "
                    f"Time: {duration:.2f} sec"
                )
                
                self._save_sync_time()
                
                if not continuous:
                    break
                
                logging.info(f"Next iteration in {interval//60} minutes...")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logging.info("Replication stopped by user")
        except Exception as e:
            logging.error(f"Critical error: {str(e)}")
            raise

    def _replicate_single_index(self, index: str) -> Dict[str, int]:
        """Обработка отдельного индекса"""
        try:
            self.timestamp_field = self._detect_timestamp_field(index)
            if self.timestamp_field:
                logging.info(f"Using timestamp field: {self.timestamp_field} for index {index}")
            return self.replicate_index(index)
        except Exception as e:
            logging.error(f"Error replicating index {index}: {str(e)}")
            return {'processed': 0, 'updated': 0, 'failed': 1}

    def replicate_missing_indices(self) -> None:
        """Репликация только отсутствующих индексов"""
        try:
            indices = self.get_missing_indices()
            
            if not indices:
                logging.info("No missing indices found - all indices are in sync")
                return
            
            logging.info(f"Found {len(indices)} missing indices: {', '.join(indices)}")
            
            total_stats = {
                'processed': 0,
                'updated': 0,
                'failed': 0
            }
            
            for index in indices:
                try:
                    # Автоматически определяем timestamp поле для каждого индекса
                    self.timestamp_field = self._detect_timestamp_field(index)
                    if self.timestamp_field:
                        logging.info(f"Using timestamp field: {self.timestamp_field} for index {index}")
                    
                    # Для отсутствующих индексов делаем полную репликацию
                    stats = self.replicate_index(index)
                    for k in total_stats:
                        total_stats[k] += stats.get(k, 0)
                except Exception as e:
                    logging.error(f"Error replicating index {index}: {str(e)}")
                    total_stats['failed'] += 1
            
            logging.info(
                f"Replication completed. Documents: {total_stats['processed']}, "
                f"Errors: {total_stats['failed']}"
            )
            
        except Exception as e:
            logging.error(f"Critical error: {str(e)}")
            raise

    def replicate_index(self, source_index: str, target_index: Optional[str] = None) -> Dict[str, int]:
        target_index = target_index or source_index
        stats = {'processed': 0, 'updated': 0, 'failed': 0}
        
        try:
            # Временно отключаем refresh
            self.target_es.indices.put_settings(
                index=target_index,
                body={"index": {"refresh_interval": "-1"}}
            )
            
            if not self.source_es.indices.exists(index=source_index):
                logging.warning(f"Source index {source_index} doesn't exist")
                return stats
            
            # Получаем полное определение индекса
            index_settings = self.source_es.indices.get_settings(index=source_index)
            index_mappings = self.source_es.indices.get_mapping(index=source_index)
            
            # Создаем индекс в target с теми же настройками
            if not self.target_es.indices.exists(index=target_index):
                self._create_target_index(target_index, index_settings, index_mappings)
            
            # Определяем общее количество документов
            total_docs = self.source_es.count(index=source_index)['count']
            if total_docs == 0:
                logging.info(f"Index {source_index} is empty")
                return stats
            
            logging.info(f"Starting replication of {source_index} -> {target_index} ({total_docs} documents)")
            
            # Используем scan для эффективного чтения больших объемов
            query = {"query": {"match_all": {}}}
            
            # Логируем прогресс для больших индексов
            log_interval = max(10000, total_docs // 100)  # Логировать каждые 1% или 10k документов
            
            def process_batch(batch):
                actions = []
                for doc in batch:
                    try:
                        doc_hash = self._generate_doc_hash(doc['_source'])
                        action = {
                            "_op_type": "index",
                            "_index": target_index,
                            "_id": doc["_id"],
                            "_source": {**doc["_source"], self.checksum_field: doc_hash}
                        }
                        actions.append(action)
                    except Exception as e:
                        logging.warning(f"Error processing doc {doc['_id']}: {str(e)}")
                        stats['failed'] += 1
                return actions

            processed = 0
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for batch in batch_iterator(
                    helpers.scan(
                        client=self.source_es,
                        index=source_index,
                        query=query,
                        size=self.batch_size,
                        scroll=self.scroll_time,
                        preserve_order=True,
                        request_timeout=300
                    ),
                    self.batch_size
                ):
                    actions = process_batch(batch)
                    if actions:
                        future = executor.submit(
                            helpers.bulk,
                            client=self.target_es.options(
                                request_timeout=300,
                                max_retries=5,
                                retry_on_timeout=True
                            ),
                            actions=actions,
                            stats_only=True,
                            raise_on_error=False,
                            chunk_size=len(actions)
                        )
                        futures.append(future)
                        
                        processed += len(actions)
                        if processed % log_interval == 0:
                            logging.info(f"Progress: {processed}/{total_docs} ({processed/total_docs:.1%})")
                
                # Ожидаем завершения всех операций
                for future in futures:
                    try:
                        success, failed = future.result()
                        stats['processed'] += success
                        stats['updated'] += success
                        stats['failed'] += failed
                    except Exception as e:
                        logging.error(f"Bulk operation failed: {str(e)}")
                        stats['failed'] += len(actions)
            
            logging.info(f"Completed: {source_index} -> {target_index} | Success: {stats['processed']}, Failed: {stats['failed']}")
            
            # Восстанавливаем refresh_interval после завершения
            self.target_es.indices.put_settings(
                index=target_index,
                body={"index": {"refresh_interval": "1s"}}
            )
            
            return stats
            
        except es_exceptions.NotFoundError:
            logging.warning(f"Index {source_index} not found")
            return stats
        except Exception as e:
            logging.error(f"Error replicating index {source_index}: {str(e)}")
            raise

    def _create_target_index(self, target_index: str, 
                           settings: Dict[str, Any], 
                           mappings: Dict[str, Any]) -> None:
        """Создание целевого индекса с сохранением всех оригинальных настроек"""
        try:
            # Удаляем системные настройки, которые нельзя копировать
            index_name = list(settings.keys())[0]
            index_settings = settings[index_name]['settings']
            
            # Выводим оригинальные настройки для отладки
            logging.info(f"Original settings for {target_index}: {index_settings}")
            
            # Список системных настроек, которые нужно удалить
            system_settings = [
                'uuid', 'version', 'creation_date', 'provided_name',
                'history.uuid', 'history.retention.lease.period',
                'history.retention.lease.period.unit'
            ]
            
            # Удаляем системные настройки из всех уровней
            if 'index' in index_settings:
                # Удаляем всю секцию history
                if 'history' in index_settings['index']:
                    logging.info(f"Removing history settings: {index_settings['index']['history']}")
                    del index_settings['index']['history']
                
                # Удаляем остальные системные настройки
                for setting in system_settings:
                    if setting in index_settings['index']:
                        logging.info(f"Removing system setting: {setting}")
                        del index_settings['index'][setting]
            
            # Выводим очищенные настройки для отладки
            logging.info(f"Cleaned settings for {target_index}: {index_settings}")
            
            # Добавляем поле для хеша документа
            mappings[index_name]['mappings'].setdefault('properties', {})
            mappings[index_name]['mappings']['properties'][self.checksum_field] = {
                "type": "keyword",
                "doc_values": False,
                "index": False
            }
            
            # Создаем индекс
            self.target_es.indices.create(
                index=target_index,
                body={
                    "settings": index_settings,
                    "mappings": mappings[index_name]['mappings']
                }
            )
            logging.info(f"Created target index {target_index} with original settings")
        except Exception as e:
            logging.error(f"Error creating target index {target_index}: {str(e)}")
            # Выводим полные настройки при ошибке
            logging.error(f"Failed settings: {index_settings}")
            raise

    def verify_replication(self, indices: List[str]) -> Dict[str, int]:
        """Проверка расхождений между кластерами"""
        stats = {'checked': 0, 'mismatches': 0, 'missing_indices': 0, 'count_mismatches': 0}
        
        for index in indices:
            try:
                # Используем GET запрос вместо HEAD для проверки существования индекса
                try:
                    target_info = self.target_es.indices.get(index=index)
                    source_info = self.source_es.indices.get(index=index)
                except es_exceptions.NotFoundError:
                    stats['mismatches'] += 1
                    stats['missing_indices'] += 1
                    logging.warning(f"Index {index} missing in target cluster")
                    continue
                
                logging.info(f"Verifying index {index}...")
                
                # Сравниваем количество документов
                source_count = self.source_es.count(index=index)['count']
                target_count = self.target_es.count(index=index)['count']
                
                if source_count != target_count:
                    diff = abs(source_count - target_count)
                    stats['mismatches'] += diff
                    stats['count_mismatches'] += diff
                    logging.warning(
                        f"Document count mismatch for {index}: "
                        f"source={source_count}, target={target_count} (diff: {diff})"
                    )
                
                # Проверяем хеши документов выборочно
                sample_size = min(1000, source_count)
                if sample_size == 0:
                    continue
                
                # Получаем случайную выборку документов
                query = {
                    "size": sample_size,
                    "query": {
                        "function_score": {
                            "functions": [{"random_score": {}}],
                            "score_mode": "sum"
                        }
                    },
                    "_source": [self.checksum_field]
                }
                
                try:
                    source_sample = {
                        hit['_id']: hit['_source'].get(self.checksum_field)
                        for hit in self.source_es.search(index=index, body=query)['hits']['hits']
                    }
                    
                    target_sample = {
                        hit['_id']: hit['_source'].get(self.checksum_field)
                        for hit in self.target_es.search(index=index, body=query)['hits']['hits']
                    }
                    
                    # Сравниваем хеши
                    for doc_id in source_sample:
                        if doc_id not in target_sample:
                            stats['mismatches'] += 1
                            logging.debug(f"Document {doc_id} missing in target index {index}")
                        elif source_sample[doc_id] != target_sample[doc_id]:
                            stats['mismatches'] += 1
                            logging.debug(f"Document {doc_id} content mismatch in index {index}")
                        stats['checked'] += 1
                    
                    logging.info(
                        f"Verification complete for {index}: checked {stats['checked']} docs, "
                        f"found {stats['mismatches']} mismatches"
                    )
                    
                except Exception as e:
                    logging.error(f"Error sampling documents from {index}: {str(e)}")
                    continue
                
            except Exception as e:
                logging.error(f"Error verifying index {index}: {str(e)}")
        
        # Сводная статистика
        if stats['missing_indices'] > 0:
            logging.warning(f"Missing indices in target: {stats['missing_indices']}")
        if stats['count_mismatches'] > 0:
            logging.warning(f"Document count mismatches: {stats['count_mismatches']}")
        if stats['mismatches'] > 0:
            logging.warning(f"Total mismatches found: {stats['mismatches']}")
        
        return stats

    def _generate_doc_hash(self, doc_source: Dict[str, Any]) -> str:
        """Генерирует хеш содержимого документа"""
        doc_str = str(sorted(doc_source.items())).encode('utf-8')
        return hashlib.sha256(doc_str).hexdigest()

def load_config(config_path: str) -> Dict[str, Any]:
    """Загрузка конфигурации из YAML-файла"""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading config file: {str(e)}")
        raise

def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Elasticsearch Full Replicator')
    
    # Основные параметры
    parser.add_argument('--config', help='Path to YAML config file')
    parser.add_argument('--missing-only', action='store_true', 
                       help='Replicate only missing indices')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify replication status without copying data')
    
    # Параметры source кластера
    parser.add_argument('--source-hosts', help='Source ES hosts (comma-separated)')
    parser.add_argument('--source-user', help='Source ES username')
    parser.add_argument('--source-password', help='Source ES password')
    
    # Параметры target кластера
    parser.add_argument('--target-hosts', help='Target ES hosts (comma-separated)')
    parser.add_argument('--target-user', help='Target ES username')
    parser.add_argument('--target-password', help='Target ES password')
    
    # Параметры репликации
    parser.add_argument('--continuous', action='store_true', 
                       help='Enable continuous replication')
    parser.add_argument('--interval', type=int, default=3600,
                       help='Sync interval in seconds (default: 3600)')
    parser.add_argument('--timestamp-field', 
                       help='Custom timestamp field for incremental sync')
    parser.add_argument('--exclude-patterns', 
                       help='Comma-separated index patterns to exclude')
    
    return parser.parse_args()

def merge_configs(file_config: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    """Объединение конфигов из файла и аргументов"""
    config = file_config.copy() if file_config else {}
    
    # Удаляем timeout из конфигурации, если он есть
    if 'source' in config and 'timeout' in config['source']:
        del config['source']['timeout']
    if 'target' in config and 'timeout' in config['target']:
        del config['target']['timeout']
    
    # Source cluster
    if 'source' not in config:
        config['source'] = {}
    
    if args.source_hosts:
        config['source']['hosts'] = args.source_hosts.split(',')
    if args.source_user and args.source_password:
        config['source']['basic_auth'] = (args.source_user, args.source_password)
    
    # Target cluster
    if 'target' not in config:
        config['target'] = {}
    
    if args.target_hosts:
        config['target']['hosts'] = args.target_hosts.split(',')
    if args.target_user and args.target_password:
        config['target']['basic_auth'] = (args.target_user, args.target_password)
    
    # Replication settings
    if 'replication' not in config:
        config['replication'] = {}
    
    if args.continuous:
        config['replication']['continuous'] = args.continuous
    if args.interval:
        config['replication']['interval'] = args.interval
    if args.timestamp_field:
        config['replication']['timestamp_field'] = args.timestamp_field
    if args.exclude_patterns:
        config['replication']['exclude_patterns'] = args.exclude_patterns.split(',')
    
    return config

def main():
    try:
        args = parse_args()
        
        # Загрузка конфигурации
        file_config = load_config(args.config) if args.config else None
        config = merge_configs(file_config, args)
        
        # Проверка обязательных параметров
        if 'source' not in config or 'hosts' not in config['source']:
            raise ValueError("Source cluster hosts must be specified")
        if 'target' not in config or 'hosts' not in config['target']:
            raise ValueError("Target cluster hosts must be specified")
        
        # Инициализация репликатора
        replicator = ESFullReplicator(
            source_config=config['source'],
            target_config=config['target']
        )
        
        # Настройка параметров репликации
        if 'replication' in config:
            if 'timestamp_field' in config['replication']:
                replicator.timestamp_field = config['replication']['timestamp_field']
            if 'exclude_patterns' in config['replication']:
                replicator.exclude_patterns.extend(config['replication']['exclude_patterns'])
        
        # Определение режима работы
        if args.verify_only:
            # Режим только проверки
            logging.info("Running in verification-only mode")
            
            # Получаем все индексы для проверки
            indices = replicator.get_all_indices()
            if not indices:
                logging.warning("No indices found for verification")
                return
            
            logging.info(f"Starting verification of {len(indices)} indices")
            
            # Выполняем проверку
            verify_stats = replicator.verify_replication(indices)
            
            # Логируем итоговую статистику
            logging.info(
                "Verification completed:\n"
                f"  Indices checked: {len(indices)}\n"
                f"  Documents checked: {verify_stats['checked']}\n"
                f"  Missing indices: {verify_stats['missing_indices']}\n"
                f"  Document count mismatches: {verify_stats['count_mismatches']}\n"
                f"  Content mismatches: {verify_stats['mismatches'] - verify_stats['count_mismatches']}\n"
                f"  Total mismatches found: {verify_stats['mismatches']}"
            )
            
            # Выход с кодом ошибки, если найдены расхождения
            if verify_stats['mismatches'] > 0:
                logging.error("Verification failed - mismatches found")
                exit(1)
            else:
                logging.info("Verification successful - no mismatches found")
                exit(0)
                
        elif args.missing_only:
            # Режим репликации только отсутствующих индексов
            logging.info("Running in missing-indices-only mode")
            replicator.replicate_missing_indices()
        else:
            # Стандартный режим работы (полная репликация)
            continuous = config.get('replication', {}).get('continuous', False)
            interval = config.get('replication', {}).get('interval', 3600)
            
            if continuous:
                logging.info(f"Running in continuous replication mode (interval: {interval} seconds)")
            else:
                logging.info("Running in one-time replication mode")
            
            replicator.replicate_all_indices(continuous=continuous, interval=interval)
            
    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
        exit(0)
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()
