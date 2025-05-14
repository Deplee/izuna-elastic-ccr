# Найти индексы, где >10% документов удалено
curl -s -u admin:pwd  "http://localhost:9200/_cat/segments?v&h=index,docs.count,docs.deleted" \
  | awk '{ if ($3 > 0 && ($3 / $2) > 0.1) print $1 }' \
  | xargs -I{} curl -u admin:pwd -XPOST "http://localhost:9200/{}/_forcemerge?only_expunge_deletes=true
