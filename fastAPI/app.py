import os
import numpy as np
import onnxruntime as rt
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List

# ------------------------------
# 1. Load ONNX model (chỉ một lần khi khởi động)
# ------------------------------
MODEL_PATH = os.getenv("MODEL_PATH", "/home/spark/spark/ML/air_quality_model.onnx")

if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"ONNX model not found at {MODEL_PATH}")

session = rt.InferenceSession(MODEL_PATH)
input_name = session.get_inputs()[0].name
output_name = session.get_outputs()[0].name

print(f"ONNX model loaded from {MODEL_PATH}")
print(f"Input name: {input_name}, Output name: {output_name}")

# ------------------------------
# 2. Định nghĩa request/response schemas
# ------------------------------
class Features(BaseModel):
    pm2_5: float = Field(..., example=12.5, description="PM2.5 concentration")
    pm10: float = Field(..., example=35.2, description="PM10 concentration")
    co: float = Field(..., example=0.8, description="Carbon monoxide")
    no2: float = Field(..., example=15.2, description="Nitrogen dioxide")
    so2: float = Field(..., example=5.1, description="Sulfur dioxide")
    o3: float = Field(..., example=32.1, description="Ozone")

class PredictionResponse(BaseModel):
    prediction: float
    status: str

class BatchPredictionResponse(BaseModel):
    predictions: List[float]
    status: str

# ------------------------------
# 3. FastAPI app
# ------------------------------
app = FastAPI(
    title="Air Quality Prediction API (ONNX)",
    description="Predict AQI using ONNX model without Spark",
    version="1.0"
)

@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": True}

@app.post("/predict", response_model=PredictionResponse)
def predict(features: Features):
    try:
        # Chuyển đổi input thành numpy array (shape: 1 x 6)
        input_data = np.array([[
            features.pm2_5,
            features.pm10,
            features.co,
            features.no2,
            features.so2,
            features.o3
        ]], dtype=np.float32)
        
        # Run inference
        pred = session.run([output_name], {input_name: input_data})[0][0][0]
        return PredictionResponse(prediction=float(pred), status="success")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/predict_batch", response_model=BatchPredictionResponse)
def predict_batch(features_list: List[Features]):
    try:
        # Chuyển đổi batch input thành numpy array (n x 6)
        input_data = np.array([
            [f.pm2_5, f.pm10, f.co, f.no2, f.so2, f.o3] for f in features_list
        ], dtype=np.float32)
        
        # Run inference
        predictions = session.run([output_name], {input_name: input_data})[0].flatten()
        return BatchPredictionResponse(predictions=predictions.tolist(), status="success")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Optional: endpoint để dự đoán từ JSON raw (tương tự /predict)
@app.post("/predict_raw")
def predict_raw(data: dict):
    try:
        required = ["pm2_5", "pm10", "co", "no2", "so2", "o3"]
        input_data = np.array([[data[k] for k in required]], dtype=np.float32)
        pred = session.run([output_name], {input_name: input_data})[0][0][0]
        return {"prediction": float(pred)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))