import json
import logging
from datetime import datetime
from typing import Dict

from clickhouse_con.settings import kafka_settings

from kafka import KafkaConsumer

from clickhouse_con.ch_classes import DataChange
from clickhouse_con.save_data import SaveData
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_consumer")


class KafkaJsonConsumer:

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start_consuming(self):
        # читаем сообщения
        try:
            for msg in self.consumer:
                self.process_message(msg.value)
                # Подтверждаем обработку сообщения
                self.consumer.commit()
        except Exception as e:
            logger.error(f"Ошибка при чтении из Kafka: {e}")
        finally:
            self.consumer.close()

    def process_message(self, message: Dict):
        """Обработка одного сообщения."""
        try:
            if not self._validate_message(message):
                logger.error("Неверный формат сообщения: отсутствуют необходимые поля")
                return

            transformed_data = self._transform_data(message)
            self._save_to_database(transformed_data)

        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}")

    def _validate_message(self, message: Dict) -> bool:
        """Проверка наличия обязательных полей."""
        required_fields = ['id', 'name', 'event', 'time', 'is_something']
        return all(field in message for field in required_fields)

    def _transform_data(self, data: Dict) -> Dict:
        """Пример трансформации входящих данных. Можно дополнять/менять логику."""
        return {
            'id':           str(data['id']),
            'name':         str(data['name']),
            'event':        str(data['event']),
            'time':         str(data['time']),
            'is_something': str(data['is_something']),
        }

    def _save_to_database(self, data: Dict):
        """Подготовка данных и сохранение в ClickHouse."""
        change_data = DataChange(data).get_change_data()
    
        table_name = datetime.now().strftime("%Y_%m_%d__%H")
    
        SaveData({table_name: change_data}).create_and_save()
        logger.info(f"Сохраняем в БД: {data}")

consumer = KafkaJsonConsumer(
        bootstrap_servers=kafka_settings.bootstrap_servers,
        topic=kafka_settings.topic,
        group_id=kafka_settings.group_id
    )
consumer.start_consuming()