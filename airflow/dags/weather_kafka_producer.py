import requests
import json
from kafka import KafkaProducer
from datetime import datetime
import time
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "last_state.json")

def load_last_state():
    """Đọc trạng thái cuối cùng từ file JSON"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_last_state(state):
    """Lưu trạng thái cuối cùng vào file JSON"""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def round_time_15m(time_str):
    # time_str: "2026-04-12T07:10"
    dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
    # Làm tròn về mốc 15 phút gần nhất (00, 15, 30, 45)
    minute = (dt.minute // 15) * 15
    dt = dt.replace(minute=minute, second=0)
    return dt.strftime("%Y-%m-%dT%H:%M")

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "weather-topic"

# Danh sách 20 địa điểm tiêu biểu (Loại trừ Hà Nội)
LOCATIONS = [
    {"name": "Ho Chi Minh City", "lat": 10.8231, "lon": 106.6297},
    {"name": "Da Nang", "lat": 16.0544, "lon": 108.2022},
    {"name": "Hai Phong", "lat": 20.8449, "lon": 106.6881},
    {"name": "Can Tho", "lat": 10.0452, "lon": 105.7469},
    {"name": "Bien Hoa", "lat": 10.9575, "lon": 106.8427},
    {"name": "Nha Trang", "lat": 12.2388, "lon": 109.1967},
    {"name": "Hue", "lat": 16.4637, "lon": 107.5909},
    {"name": "Vung Tau", "lat": 10.3460, "lon": 107.0843},
    {"name": "Da Lat", "lat": 11.9404, "lon": 108.4583},
    {"name": "Quy Nhon", "lat": 13.7767, "lon": 109.2243},
    {"name": "Nam Dinh", "lat": 20.4231, "lon": 106.1681},
    {"name": "Rach Gia", "lat": 10.0125, "lon": 105.0801},
    {"name": "Phan Thiet", "lat": 10.9333, "lon": 108.1000},
    {"name": "Long Xuyen", "lat": 10.3759, "lon": 105.4325},
    {"name": "Ca Mau", "lat": 9.1769, "lon": 105.1501},
    {"name": "Buon Ma Thuot", "lat": 12.6667, "lon": 108.0500},
    {"name": "Thai Nguyen", "lat": 21.5939, "lon": 105.8442},
    {"name": "Vinh", "lat": 18.6733, "lon": 105.6922},
    {"name": "Pleiku", "lat": 13.9833, "lon": 108.0000},
    {"name": "Phu Quoc", "lat": 10.2167, "lon": 103.9667}
]

def call_openmeteo():
    # Chuẩn bị danh sách Lat/Lon cho Batch Request
    lats = ",".join([str(loc["lat"]) for loc in LOCATIONS])
    lons = ",".join([str(loc["lon"]) for loc in LOCATIONS])
    last_sent_timestamps = load_last_state()
    print(f"Loaded last sent timestamps: {last_sent_timestamps}")
    data_count = 0

    # 1. Weather API (Batch)
    weather_url = "https://api.open-meteo.com/v1/forecast"
    weather_params = {
        "latitude": lats,
        "longitude": lons,
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,"
                   "cloud_cover,wind_speed_10m,wind_direction_10m,wind_gusts_10m,uv_index,weather_code",
        "timezone": "Asia/Ho_Chi_Minh"
    }
    
    # 2. Air Quality API (Batch)
    aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    aq_params = {
        "latitude": lats,
        "longitude": lons,
        "current": "european_aqi,us_aqi,pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone"
    }

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(3, 9, 2), 
        acks=1
    )

    try:
        print(f"[{datetime.now()}] Fetching data for {len(LOCATIONS)} locations...")
        
        # Gọi API - Open-meteo sẽ trả về một LIST các object nếu gửi nhiều tọa độ
        w_res = requests.get(weather_url, params=weather_params, timeout=30).json()
        aq_res = requests.get(aq_url, params=aq_params, timeout=30).json()

        # Vì gửi batch nên Open-Meteo trả về mảng kết quả ứng với thứ tự gửi đi
        # Nếu chỉ gửi 1 điểm nó trả về dict, nếu gửi nhiều nó trả về list các dict
        for i in range(len(LOCATIONS)):
            # Xử lý trường hợp Open-Meteo trả về 1 object (khi list chỉ có 1) hoặc nhiều
            w_current = w_res[i]["current"] if isinstance(w_res, list) else w_res["current"]
            aq_current = aq_res[i]["current"] if isinstance(aq_res, list) else aq_res["current"]
            
            city_name = LOCATIONS[i]["name"]
            timestamp = round_time_15m(w_res[i]["current"]["time"]) if isinstance(w_res, list) else round_time_15m(w_res["current"]["time"])

            if last_sent_timestamps.get(city_name) == timestamp:
                print(f"  -> Skipping {city_name} (timestamp {timestamp} already sent)")
                continue

            payload = {
                "timestamp": datetime.now().isoformat(),
                "location": LOCATIONS[i]["name"],
                "latitude": LOCATIONS[i]["lat"],
                "longitude": LOCATIONS[i]["lon"],
                **w_current,
                **aq_current
            }

            producer.send(TOPIC, value=payload)
            last_sent_timestamps[city_name] = timestamp
            data_count += 1
            # In đầu và cuối
            if i == 0 or i == len(LOCATIONS) - 1:
                print(f"  -> Prepared data for {LOCATIONS[i]['name']}")

        if data_count > 0:
            producer.flush()
            print(f"[{datetime.now()}] Successfully sent {len(LOCATIONS)} messages to Kafka.")
            save_last_state(last_sent_timestamps)
        else: print(f"[{datetime.now()}] No new data to send. All timestamps are up-to-date.")
        
        return True

    except Exception as e:
        print(f"Error fetching/sending data: {e}")
        return False
    finally:
        producer.close()

if __name__ == "__main__":
    print("Weather Batch Producer started. Press Ctrl+C to stop.")
    while True:
        call_openmeteo()
        print("Waiting 5 minutes for next batch...")
        time.sleep(300)