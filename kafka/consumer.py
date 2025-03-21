from kafka import KafkaConsumer
import json
import logging
from typing import Dict
from clickhouse_con.ch_classes import DataChange
from clickhouse_con.save_data import SaveData
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_consumer")

class KafkaJsonConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
    def process_message(self, message: Dict):
        try:
            if not self._validate_message(message):
                logger.error("Invalid message format")
                return
            
            transformed_data = self._transform_data(message)
            
            self._save_to_database(transformed_data)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _validate_message(self, message: Dict) -> bool:
        # проверка доступности всех данных
        required_fields = ['id', 'name', 'event', 'time', 'is_something']
        return all(field in message for field in required_fields)

    def _transform_data(self, data: Dict):
        return {
            'id': str(data['id']),
            'name': str(data['name']),
            'event': str(data['event']),
            'time': str(data['event']),
            'is_something': str(data['is_something']),
        }

    def _save_to_database(self, data: Dict):
        change_data = DataChange(data).get_change_data()
        SaveData({datetime.now().table_name.strftime("%Y-%m-%d %H").replace("-","_").replace(" ","__").replace(":", "_"): 
                  change_data}).create_and_save()

    def start_consuming(self):
        try:
            for message in self.consumer:
                self.process_message(message.value)
                # Подтверждение обработки сообщения
                self.consumer.commit()
        finally:
            self.consumer.close()

config = {
    "bootstrap_servers": "localhost:9092",
    "topic": "json_data_topic",
    "group_id": "json_consumer_group"
}

consumer = KafkaJsonConsumer(**config)
consumer.start_consuming()