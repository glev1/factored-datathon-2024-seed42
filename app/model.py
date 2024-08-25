import joblib

def load_model():
    model = joblib.load("models/my_model.pkl")
    return model

def predict(model, data):
    return model.predict([data])
