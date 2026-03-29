# include/data_generator.py
import json
import csv
import random
import os
from datetime import datetime, timedelta
# Nomes dos satélites (Dimensão simulada)
SATELLITES = ["SENTINEL-1A", "SENTINEL-1B", "CUBESAT-X", "CUBESAT-Y"]
# Pastas locais onde o Airflow vai enxergar
INBOUND_DIR = os.path.join(os.path.dirname(__file__), "inbound")
os.makedirs(INBOUND_DIR, exist_ok=True)
def generate_telemetry_json(num_records=100):
    """Gera dados fakes de bateria em formato JSON"""
    data = []
    for _ in range(num_records):
        record = {
            "satellite_id": random.choice(SATELLITES),
            "voltage": round(random.uniform(3.0, 4.2), 2),  # Voltagem da bateria
            "temperature_c": round(random.uniform(-40.0, 85.0), 1),
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(1, 60))).isoformat()
        }
        # Inserindo um pouco de "sujeira" (NaN ou nulos) para simular falha no sensor, como manda a premissa
        if random.random() < 0.05:
            record["voltage"] = "NaN"
            
        data.append(record)
    file_path = os.path.join(INBOUND_DIR, f"telemetry_{datetime.now().strftime('%Y%H%M%S')}.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"[OK] Gerado arquivo de telemetria JSON: {file_path}")
def generate_position_csv(num_records=100):
    """Gera dados fakes de geolocalização em formato CSV"""
    file_path = os.path.join(INBOUND_DIR, f"position_{datetime.now().strftime('%Y%H%M%S')}.csv")
    
    with open(file_path, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["satellite_id", "latitude", "longitude", "altitude_km", "timestamp"])
        
        for _ in range(num_records):
            timestamp = (datetime.now() - timedelta(minutes=random.randint(1, 60))).isoformat()
            
            # Simulando altitude quebrada com vírgula para forçar que seu dado Staging seja TEXT (como manda a premissa)
            alt_km = str(round(random.uniform(400.0, 600.0), 2)).replace('.', ',')
            
            writer.writerow([
                random.choice(SATELLITES),
                round(random.uniform(-90.0, 90.0), 4),
                round(random.uniform(-180.0, 180.0), 4),
                alt_km,
                timestamp
            ])
            
    print(f"[OK] Gerado arquivo de posição CSV: {file_path}")
if __name__ == "__main__":
    print("Iniciando simulação de sinais da AeroSpace Insights...")
    generate_telemetry_json()
    generate_position_csv()
    print("Concluído!")