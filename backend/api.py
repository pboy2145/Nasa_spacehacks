from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response 
from pydantic import BaseModel

import pandas as pd
import json
import io
import uuid
import time
import sqlite3
import os

# Initialize FastAPI app
app = FastAPI(
    title="AI CSV Generation API",
    description="Receives JSON data from Streamlit, converts it to CSV, and provides a fetch endpoint."
)


# --- SQLITE PERSISTENCE ---
DB_PATH = os.environ.get("DB_PATH", "processed_data.db")
EXPIRATION_TIME_SECONDS = 300  # Data expires in 5 minutes (for demonstration purposes)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS processed_data (
            data_id TEXT PRIMARY KEY,
            csv_data TEXT NOT NULL,
            expiry REAL NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

init_db()
# --- END SQLITE PERSISTENCE ---

# Pydantic model for incoming data (from Streamlit)
class DataPayload(BaseModel):
    json_data_string: str

# Helper function for JSON cleanup 
def clean_json_output(json_string):
    """Strips markdown fences and parses the JSON string."""
    if json_string is None:
        return ""
    
    # Check for markdown code fence (```)
    stripped = json_string.strip()
    if stripped.startswith('```'):
        parts = stripped.split('```')
        # We expect [start, content, end] or [start, content]
        if len(parts) > 1:
            content = parts[1].strip()
            # Remove optional language tag (e.g., 'json')
            if content.lower().startswith('json'):
                return content[len('json'):].strip()
            return content
        
    return json_string

@app.post("/process_ai_data")
async def process_ai_data(payload: DataPayload):
    """
    Receives JSON from Streamlit, processes it, stores the CSV internally, 
    and returns a unique ID for fetching.
    """
    json_string = payload.json_data_string
    data_id = str(uuid.uuid4()) # Generate a unique ID (the 'Key' for fetching)
    
    # 1. Clean and parse the incoming JSON string
    try:
        clean_json_string = clean_json_output(json_string)
        # Attempt to load the JSON array
        data = json.loads(clean_json_string)

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON payload received from client. Failed to parse. Check model output format."
        )

    # 2. Convert the JSON data (list of dicts) to a Pandas DataFrame
    try:
        df = pd.json_normalize(data)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Could not normalize JSON data into tabular format: {e}"
        )
    
    # 3. Convert DataFrame to CSV in memory
    csv_in_memory = df.to_csv(index=False)
    
    # 4. Store the CSV data with its expiration time in SQLite
    expiry = time.time() + EXPIRATION_TIME_SECONDS
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO processed_data (data_id, csv_data, expiry) VALUES (?, ?, ?)", (data_id, csv_in_memory, expiry))
    conn.commit()
    conn.close()

    # Log to the terminal for confirmation
    print("-" * 50)
    print(f"--- DATA PROCESSED AND STORED ---")
    print(f"Unique Data ID (Key): {data_id}")
    print(f"Rows processed: {len(df)}")
    print(f"CSV Header Preview:\n{csv_in_memory.splitlines()[0]}")
    print("-" * 50)

    # Return the details the 'other AI' needs to fetch the data
    fetch_url = f"https://elegant-consideration-production.up.railway.app/fetch_data/{data_id}"

    return JSONResponse(
        status_code=200,
        content={
            "status": "success",
            "message": "Data successfully processed and stored.",
            "data_id": data_id,
            "fetch_endpoint": fetch_url, # The URL the other bot uses
            "column_count": len(df.columns)
        }
    )

@app.get("/fetch_data/{data_id}")
async def fetch_data(data_id: str, format: str = "csv"):
    """
    Endpoint for the downstream AI to fetch the processed data using the unique ID (key).
    """
    # Fetch from SQLite
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT csv_data, expiry FROM processed_data WHERE data_id = ?", (data_id,))
    row = c.fetchone()
    conn.close()
    if not row or row[1] < time.time():
        raise HTTPException(
            status_code=404,
            detail="Data ID (Key) not found or has expired. Please run the generation again."
        )
    csv_data = row[0]
    if format.lower() == "json":
        df = pd.read_csv(io.StringIO(csv_data))
        return JSONResponse(
            status_code=200,
            content={"data": df.to_dict(orient="records")}
        )
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment;filename=data_{data_id}.csv"}
    )
