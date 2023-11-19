from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
from sqlalchemy import create_engine, Column, Integer, String, DateTime, inspect  # Updated import
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime
import logging


# Configure the logger
logging.basicConfig(level=logging.INFO)  # Set your desired logging level

# Oracle database configuration
# DEFINE THE DATABASE CREDENTIALS
user = 'system'
password = 'tiger'
host = 'localhost'
port = 1521
service_name = 'orcl'  # Replace with your Oracle service name

DATABASE_URI = f"oracle+cx_oracle://{user}:{password}@{host}:{port}/{service_name}"
engine = create_engine(DATABASE_URI)
Base = declarative_base()

class Log(Base):
    __tablename__ = 'logs'
    id = Column(Integer, primary_key=True)
    level = Column(String(50))
    message = Column(String(255))
    resourceId = Column(String(50))
    timestamp = Column(DateTime)
    traceId = Column(String(50))
    spanId = Column(String(50))
    commit = Column(String(50))
    parentResourceId = Column(String(50))

# Create an inspector
inspector = inspect(engine)

# Check if the table already exists
if not inspector.has_table('logs'):
    # If the table doesn't exist, create it
    Base.metadata.create_all(engine)
    print("Table 'logs' created successfully.")
else:
    print("Table 'logs' already exists.")

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'kafka1:19092',  # Replace with your Kafka broker(s)
    'group.id': 'log_consumer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Read from the beginning of the topic on first start
}

def consume_and_store_logs():
    consumer = Consumer(conf)
    consumer.subscribe(['log-ingestor'])  # Replace 'log_topic' with your actual topic

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll every 1 second for new messages

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                # Process the message and store in Oracle
                log_data = json.loads(msg.value().decode('utf-8'))

                Session = sessionmaker(bind=engine)
                session = Session()

                # Convert ISO 8601 timestamp to Oracle DateTime format
                parsed_timestamp = datetime.strptime(log_data.get('timestamp'), '%Y-%m-%dT%H:%M:%SZ')

                new_log = Log(
                    level=log_data.get('level'),
                    message=log_data.get('message'),
                    resourceId=log_data.get('resourceId'),
                    timestamp=parsed_timestamp,
                    traceId=log_data.get('traceId'),
                    spanId=log_data.get('spanId'),
                    commit=log_data.get('commit'),
                    parentResourceId=log_data.get('metadata', {}).get('parentResourceId')
                )

                session.add(new_log)
                session.commit()
                session.close()

                logging.info(f'Logs stored in db: {log_data}')

    except KeyboardInterrupt:
        logging.info(f'Aborted by user')
        sys.stderr.write('%% Aborted by user\n')

    finally:
        consumer.close()

if __name__ == '__main__':
    consume_and_store_logs()
