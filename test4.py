import pandas as pd
import datetime
import time
import asyncio
import json
import mysql.connector
import gmqtt

# Load the CSV file
file_path = "Asite1_m1_modified.csv"
df = pd.read_csv(file_path)

# Drop the 'data.downtime_status' column if it exists
if 'data.downtime_status' in df.columns:
    df.drop(columns=['data.downtime_status'], inplace=True)

# MQTT setup
broker = "broker.emqx.io"
port = 1883
data_topic = "try"
ack_topic = "try_ack"
heartbeat_topic = "try_heartbeat"

# In-memory tracking and constants
heartbeat_interval = 10
unacknowledged_ids = set()
received_acknowledgments_set = set()
receiver_online = False

# Database connection
def create_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="database2"
    )

# Create tables if they do not exist
def create_tables_if_not_exists():
    db_connection = create_db_connection()
    cursor = db_connection.cursor()

    create_data_table = """
    CREATE TABLE IF NOT EXISTS mqtt_trying (
        _id VARCHAR(255) PRIMARY KEY,
        data_machine_id INT,
        data_machine_status BOOLEAN,
        data_shot_count INT,
        data_shot_status INT,
        data_status VARCHAR(50),
        updated_on DATETIME
    )
    """
    cursor.execute(create_data_table)

    create_ack_table = """
    CREATE TABLE IF NOT EXISTS acknowledgments (
        _id VARCHAR(255) PRIMARY KEY,
        message JSON
    )
    """
    cursor.execute(create_ack_table)

    create_ack_db_table = """
    CREATE TABLE IF NOT EXISTS acknowledgment_db (
        _id VARCHAR(255) PRIMARY KEY,
        data_machine_id INT,
        data_machine_status BOOLEAN,
        data_shot_count INT,
        data_shot_status INT,
        data_status VARCHAR(50),
        updated_on DATETIME
    )
    """
    cursor.execute(create_ack_db_table)
    
    db_connection.commit()
    db_connection.close()

# Store unsent data in acknowledgment database
def store_in_acknowledgment_db(data):
    db_connection = create_db_connection()
    cursor = db_connection.cursor()
    sql_insert_ack_db = """
        INSERT INTO acknowledgment_db (_id, data_machine_id, data_machine_status, data_shot_count, 
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
    cursor.execute(sql_insert_ack_db, (
        data['_id'], data['data.machine_id'], data['data.machine_status'],
        data['data.shot_count'], data['data.shot_status'],
        data['data.status'], data['updated_on']
    ))
    db_connection.commit()
    db_connection.close()
    unacknowledged_ids.add(data['_id'])

# Insert data into main database table
def insert_into_db(row):
    db_connection = create_db_connection()
    cursor = db_connection.cursor()
    sql_insert_query = """
        INSERT INTO mqtt_trying (_id, data_machine_id, data_machine_status, data_shot_count, 
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
    cursor.execute(sql_insert_query, (
        row['_id'], row['data.machine_id'], row['data.machine_status'],
        row['data.shot_count'], row['data.shot_status'],
        row['data.status'], row['updated_on']
    ))
    db_connection.commit()
    db_connection.close()

# MQTT client setup with reconnect handling
mqtt_client = gmqtt.Client("mqtt_client_{}".format(datetime.datetime.now().timestamp()))

# MQTT connection handling
async def reconnect_mqtt():
    max_retries = 5
    delay = 2  # Start with a 2-second delay
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Reconnecting attempt {attempt}...")
            await mqtt_client.connect(broker, port)
            print("Reconnected successfully!")
            return
        except Exception as e:
            print(f"Reconnect failed (attempt {attempt}): {e}")
            await asyncio.sleep(delay)
            delay *= 2  # Exponential backoff
    print("Max reconnection attempts reached. Exiting...")

mqtt_client.on_connect = lambda client, flags, result, properties: asyncio.create_task(handle_connect(client, flags, result, properties))
mqtt_client.on_disconnect = lambda client, packet, exc=None: asyncio.create_task(reconnect_mqtt())

async def handle_connect(client, flags, result, properties):
    print("Connected to MQTT Broker!")
    client.subscribe(ack_topic)

# Handle acknowledgment messages, including heartbeat acknowledgments
async def on_ack_message(client, topic, packet, qos, properties):
    global receiver_online
    ack_data = json.loads(packet.decode())
    ack_id = ack_data.get('_id')

    if ack_id == "heartbeat" and ack_data.get('ack') == 'received':
        receiver_online = True
        print("Heartbeat acknowledged, receiver is online.")
    elif ack_data.get('ack') == 'received' and ack_id:
        received_acknowledgments_set.add(ack_id)
        print(f"Acknowledgment received for data _id: {ack_id}")

mqtt_client.on_message = on_ack_message

async def safe_publish(topic, message, retries=3):
    for attempt in range(retries):
        try:
            mqtt_client.publish(topic, message, qos=1)
            print(f"Published to {topic}: {message}")
            return
        except Exception as e:
            print(f"Publish failed (attempt {attempt+1}): {e}")
            await asyncio.sleep(2)

async def send_current_data():
    while not df.empty:
        current_time = datetime.datetime.now().time()
        
        for index, row in df.iterrows():
            target_time = datetime.datetime.strptime(row['Time'], '%I:%M:%S %p').time()
            if (current_time.hour == target_time.hour and
                current_time.minute == target_time.minute and
                current_time.second == target_time.second):

                combined_datetime = f"{row['Date']} {row['Time']}"
                row['updated_on'] = combined_datetime
                row_data = row.drop(['Date', 'Time']).to_dict()
                message = json.dumps(row_data)
                
                await safe_publish(data_topic, message)
                
                start_time = time.time()
                acknowledgment_received = False
                while time.time() - start_time < 2:
                    if row_data['_id'] in received_acknowledgments_set:
                        acknowledgment_received = True
                        print(f"Acknowledgment received for data _id {row_data['_id']}")
                        break
                    await asyncio.sleep(0.1)

                if not acknowledgment_received:
                    print(f"No acknowledgment for data _id {row_data['_id']}, storing in acknowledgment_db")
                    store_in_acknowledgment_db(row_data)

                insert_into_db(row_data)
                df.drop(index, inplace=True)
                break

        await asyncio.sleep(0.5)

async def send_heartbeat():
    while True:
        heartbeat_message = json.dumps({"_id": "heartbeat", "ack": "ping"})
        await safe_publish(heartbeat_topic, heartbeat_message)
        await asyncio.sleep(heartbeat_interval)

async def resend_unsent_data():
    global receiver_online
    while True:
        if receiver_online:
            receiver_online = False
            db_connection = create_db_connection()
            cursor = db_connection.cursor()
            cursor.execute("SELECT * FROM acknowledgment_db")
            unsent_messages = cursor.fetchall()
            
            for row in unsent_messages:
                data = {
                    "_id": row[0],
                    "data.machine_id": row[1],
                    "data.machine_status": row[2],
                    "data.shot_count": row[3],
                    "data.shot_status": row[4],
                    "data.status": row[5],
                    "updated_on": row[6].isoformat()
                }
                message = json.dumps(data)
                
                await safe_publish(data_topic, message)

                start_time = time.time()
                acknowledgment_received = False
                while time.time() - start_time < 2:
                    if data['_id'] in received_acknowledgments_set:
                        acknowledgment_received = True
                        print(f"Acknowledgment received for data _id {data['_id']}, deleting from acknowledgment_db")
                        cursor.execute("DELETE FROM acknowledgment_db WHERE _id = %s", (data['_id'],))
                        db_connection.commit()
                        break
                    await asyncio.sleep(0.1)

        await asyncio.sleep(heartbeat_interval)

async def main():
    await mqtt_client.connect(broker, port)
    await asyncio.gather(
        send_current_data(),
        send_heartbeat(),
        resend_unsent_data()
    )
if __name__ == "__main__":
    asyncio.run(main())
    create_tables_if_not_exists()

