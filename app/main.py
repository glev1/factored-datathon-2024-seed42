from fastapi import FastAPI, Request
from app.model import load_model, predict
from app.utils import preprocess, postprocess

app = FastAPI()

# Load the model when the application starts
model = load_model()

@app.get("/")
def read_root():
    return {"message": "Welcome to the ML API"}

@app.post("/predict")
async def make_prediction(request: Request):
    input_data = await request.json()
    processed_data = preprocess(input_data)
    prediction = predict(model, processed_data)
    result = postprocess(prediction)
    return result
