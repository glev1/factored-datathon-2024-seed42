from fastapi import FastAPI, Depends, HTTPException
from utils import preprocess, postprocess, predict
from auth import create_access_token, verify_token, Token, authenticate_user

app = FastAPI()

@app.post("/token", response_model=Token)
async def login(form_data: dict):
    username = form_data.get("username")
    password = form_data.get("password")

    # Authenticate the user against the PostgreSQL database
    user = await authenticate_user(username, password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    access_token = create_access_token(data={"sub": user['username']})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/")
def read_root():
    return {"message": "Welcome to the ML API"}

@app.post("/predict", dependencies=[Depends(verify_token)])
async def make_prediction(request: dict):
    input_data = request
    processed_data = preprocess(input_data)
    prediction = predict(processed_data)
    result = postprocess(prediction)
    return result
