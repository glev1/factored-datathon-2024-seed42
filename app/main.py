from fastapi import FastAPI, Request
from app.utils import preprocess, postprocess, predict

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Welcome to the ML API"}

@app.post("/predict")
async def make_prediction(request: Request):
    input_data = await request.json()
    processed_data = preprocess(input_data)
    prediction = predict(processed_data)
    result = postprocess(prediction)
    return result
