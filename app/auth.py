import os
import jwt
import bcrypt
from datetime import datetime, timedelta
from fastapi import HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from database import get_db_connection

# JWT Configuration
SECRET_KEY = os.getenv("AUTH_SECRET_KEY") #TODO get from secret manager
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class Token(BaseModel):
    access_token: str
    token_type: str

async def authenticate_user(username: str, password: str):
    conn = await get_db_connection()
    user = await conn.fetchrow("SELECT * FROM users WHERE username = $1", username)
    await conn.close()

    if user is None:
        return False

    # Validate the password
    if not bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
        return False

    return user

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now() + expires_delta
    else:
        expire = datetime.now() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
    

def hash_password(password: str) -> str:
    
	import bcrypt
	hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
	
	return hashed_password.decode('utf-8')


if __name__ == "__main__":
    
	my_pass = "mypass1234"
     
	hashed_pass = hash_password(my_pass)

	print(hashed_pass) 

