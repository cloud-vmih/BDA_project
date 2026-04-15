import numpy as np
import onnxruntime as rt

model_path = "spark/ML/air_quality_model.onnx"
sess = rt.InferenceSession(model_path)

# Lấy tên các đầu vào (sẽ là ['pm2_5', 'pm10', 'co', 'no2', 'so2', 'o3'])
input_names = [inp.name for inp in sess.get_inputs()]
print("Input names:", input_names)

# Tạo 3 mẫu dữ liệu (6 cột)
samples = np.array([
    [12.5, 35.2, 0.8, 15.2, 5.1, 32.1],
    [5.0, 10.0, 0.2, 5.0, 2.0, 15.0],
    [80.0, 120.0, 3.0, 60.0, 25.0, 80.0]
], dtype=np.float32)

# Tạo feed dict: mỗi cột gán vào đúng tên input
feed = {}
for i, name in enumerate(input_names):
    feed[name] = samples[:, i].reshape(-1, 1)  # shape (3,1)

# Dự đoán
predictions = sess.run(None, feed)[0].flatten()

# In kết quả
for i, pred in enumerate(predictions):
    print(f"Mẫu {i+1}: {samples[i]} -> AQI = {pred:.4f}")