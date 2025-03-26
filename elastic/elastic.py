from elasticsearch import Elasticsearch


def main():
    # --------------------------------------------------
    # 1. Подключение к Elasticsearch
    # --------------------------------------------------
    
    # Если вы используете Elasticsearch без безопасности (xpack.security.enabled=false)
    es = Elasticsearch(
        "http://localhost:9200",
        verify_certs=False,
        # Если включен Basic Auth (при включённом security), то укажите:
        # http_auth=("elastic", "MY_SECURE_PASSWORD"),
        # ssl_show_warn=False,
    )

    # Проверяем соединение
    if es.ping():
        print("Подключение к Elasticsearch установлено.")
    else:
        print("Ошибка подключения к Elasticsearch.")
        return

    index_name = "my_index_example"

    # --------------------------------------------------
    # 2. Создание индекса (с простым маппингом)
    # --------------------------------------------------
    # Зададим минимальный маппинг с парой полей: title (text) и tags (keyword).
    mapping = {
        "mappings": {
            "properties": {
                "title": {
                    "type": "text"
                },
                "tags": {
                    "type": "keyword"
                }
            }
        }
    }

    # Удалим индекс, если он уже есть (для чистоты примера)
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Старый индекс {index_name} удалён.")

    # Создадим новый индекс
    es.indices.create(index=index_name, body=mapping)
    print(f"Индекс {index_name} создан.")

    # --------------------------------------------------
    # 3. Индексирование (добавление) документа
    # --------------------------------------------------
    doc_body = {
        "title": "Как работать с Elasticsearch",
        "tags": ["search", "elasticsearch", "python"]
    }

    # Если не указывать ID, он будет сгенерирован автоматически
    response = es.index(index=index_name, body=doc_body)
    doc_id = response["_id"]
    print(f"Документ добавлен в индекс {index_name} с ID: {doc_id}")

    # --------------------------------------------------
    # 4. Поиск документа по ключевому слову
    # --------------------------------------------------
    # Ищем документы, у которых в поле 'title' встречается слово "Elasticsearch"
    query = {
        "query": {
            "match": {
                "title": "Elasticsearch"
            }
        }
    }

    search_result = es.search(index=index_name, body=query)
    print("Результаты поиска:")
    for hit in search_result["hits"]["hits"]:
        print(f"  ID: {hit['_id']}, source: {hit['_source']}")

    # --------------------------------------------------
    # 5. Обновление документа
    # --------------------------------------------------
    update_body = {
        "doc": {
            "title": "Как эффективно работать с Elasticsearch"
        }
    }
    es.update(index=index_name, id=doc_id, body=update_body)

    updated_doc = es.get(index=index_name, id=doc_id)
    print("Обновлённый документ:", updated_doc["_source"])

    # --------------------------------------------------
    # 6. Удаление документа
    # --------------------------------------------------
    es.delete(index=index_name, id=doc_id)
    print(f"Документ с ID {doc_id} удалён.")

    # --------------------------------------------------
    # 7. Удаление индекса (по желанию)
    # --------------------------------------------------
    # es.indices.delete(index=index_name)
    # print(f"Индекс {index_name} удалён.")

if __name__ == "__main__":
    main()
