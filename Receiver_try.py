import paho.mqtt.client as mqtt
import mysql.connector
import json

# MQTT setup
broker = "broker.emqx.io"
port = 1883
data_topic = "try"
ack_topic = "try_ack"
heartbeat_topic = "try_heartbeat"

# Database connection
def create_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="database2"
    )

# Create the data table if it doesn't exist
def create_data_table():
    db_connection = create_db_connection()
    cursor = db_connection.cursor()

    create_data_table_query = """
    CREATE TABLE IF NOT EXISTS received_data (
        _id VARCHAR(255) PRIMARY KEY,
        data_machine_id INT,
        data_machine_status BOOLEAN,
        data_shot_count INT,
        data_shot_status INT,
        data_status VARCHAR(50),
        updated_on DATETIME
    )
    """
    cursor.execute(create_data_table_query)
    db_connection.commit()
    db_connection.close()

# Store received data in the database
def store_received_data(data):
    db_connection = create_db_connection()
    cursor = db_connection.cursor()
    
    insert_data_query = """
    INSERT INTO received_data (_id, data_machine_id, data_machine_status, data_shot_count, 
    data_shot_status, data_status, updated_on)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    data_machine_id = VALUES(data_machine_id),
    data_machine_status = VALUES(data_machine_status),
    data_shot_count = VALUES(data_shot_count),
    data_shot_status = VALUES(data_shot_status),
    data_status = VALUES(data_status),
    updated_on = VALUES(updated_on)
    """
    cursor.execute(insert_data_query, (
        data['_id'], data['data.machine_id'], data['data.machine_status'],
        data['data.shot_count'], data['data.shot_status'],
        data['data.status'], data['updated_on']
    ))
    db_connection.commit()
    db_connection.close()

# MQTT client setup
client = mqtt.Client()

# Callback for when the client receives a connection response
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe([(data_topic, 1), (heartbeat_topic, 1)])
    else:
        print(f"Failed to connect, return code {rc}")

# Handle incoming data and heartbeat messages
def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        
        # Handle heartbeat messages
        if message.topic == heartbeat_topic:
            heartbeat_id = payload.get('_id')
            if heartbeat_id:
                # Send acknowledgment for heartbeat
                ack_message = json.dumps({"_id": heartbeat_id, "ack": "received"})
                client.publish(ack_topic, ack_message, qos=1)
                print("Heartbeat acknowledged.")
        
        # Handle data messages
        elif message.topic == data_topic:
            # Store received data in the database
            data_id = payload.get('_id')
            if data_id:
                store_received_data(payload)
                
                # Send acknowledgment for the data
                ack_message = json.dumps({"_id": data_id, "ack": "received"})
                client.publish(ack_topic, ack_message, qos=1)
                print(f"Data received and acknowledged for _id: {data_id}")
    
    except json.JSONDecodeError as e:
        print(f"Failed to decode message: {e}")

client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, port)
client.loop_forever()
