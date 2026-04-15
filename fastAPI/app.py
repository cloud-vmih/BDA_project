import os
import numpy as np
import onnxruntime as rt
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List
from fastapi.middleware.cors import CORSMiddleware


# ------------------------------
# 1. Load ONNX model (chỉ một lần khi khởi động)
# ------------------------------
MODEL_PATH = os.getenv("MODEL_PATH", "/home/spark/spark/ML/air_quality_model_v2.onnx")

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

# CORS Middleware: Cho phép frontend gọi API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],    # cho phép tất cả các nguồn
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": True}

@app.post("/predict", response_model=PredictionResponse)
def predict(features: Features):
    try:
        feed = {
            "pm2_5": np.array([[features.pm2_5]], dtype=np.float32),
            "pm10": np.array([[features.pm10]], dtype=np.float32),
            "co": np.array([[features.co]], dtype=np.float32),
            "no2": np.array([[features.no2]], dtype=np.float32),
            "so2": np.array([[features.so2]], dtype=np.float32),
            "o3": np.array([[features.o3]], dtype=np.float32),
        }

        outputs = session.run(None, feed)

        prediction = int(outputs[0][0])

        return PredictionResponse(prediction=prediction,status="success")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
