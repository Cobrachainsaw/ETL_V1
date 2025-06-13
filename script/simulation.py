import asyncio
import json
import os
from pathlib import Path
from dotenv import load_dotenv
import numpy as np
import wfdb
import datetime
from kafka import KafkaProducer

# --- Configuration ---
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecg-signals")

# --- ECG Data Processing ---
def load_ecg_data(file_path):
    all_signals = []
    all_labels = []
    for file in Path(file_path).glob("*.dat"):
        record_name = file.stem
        data, _ = wfdb.rdsamp(str(file.parent / record_name))
        data = data[:, 0]  # First lead
        annot = wfdb.rdann(str(file.parent / record_name), 'atr')

        segmented_signals = [
            data[max(0, peak - 100):min(len(data), peak + 100)]
            for peak in annot.sample
        ]

        # Pad if less than 200
        segmented_signals = [
            np.pad(sig, (0, 200 - len(sig)), mode="edge") if len(sig) < 200 else sig
            for sig in segmented_signals
        ]
        labels = annot.symbol[:len(segmented_signals)]
        all_signals.extend(segmented_signals)
        all_labels.extend(labels)

    return all_signals, all_labels

# --- Async Data Publisher to Kafka ---
async def generate_and_publish_data(signals, labels):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all'  # Ensure write acknowledgment
    )

    count = 0
    while True:
        try:
            data = {
                "timestamp": datetime.datetime.now().isoformat(),
                "ecg_signal": signals[count].tolist(),
                "label": labels[count]
            }

            producer.send(KAFKA_TOPIC, value=data)
            print(f"📤 Published to Kafka: {data['timestamp']}")

            count = (count + 1) % len(signals)
            await asyncio.sleep(5)

        except Exception as e:
            print(f"❌ Error publishing to Kafka: {e}")
            await asyncio.sleep(3)

# --- Main ---
if __name__ == "__main__":
    file_path = Path("C:/Users/vinay/Downloads/mit-bih-arrhythmia-database-1.0.0/mit-bih-arrhythmia-database-1.0.0")
    signals, labels = load_ecg_data(file_path)
    print(f"📊 Loaded {len(signals)} ECG segments")
    asyncio.run(generate_and_publish_data(signals, labels))
