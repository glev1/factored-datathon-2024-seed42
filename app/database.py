import asyncpg
import os

# Environment variables for DB connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost/users")

if not DATABASE_URL:
	raise Exception("No DB detected.")

async def get_db_connection():
    conn = await asyncpg.connect(DATABASE_URL)
    return conn

async def close_db_connection(conn):
    await conn.close()
