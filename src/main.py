from fastapi import FastAPI, HTTPException
from typing import List, Tuple
from datetime import date

from q1_memory import q1_memory
from q1_time import q1_time
from q2_memory import q2_memory
from q2_time import q2_time
from q3_memory import q3_memory
from q3_time import q3_time

app = FastAPI()

@app.get("/q1/time", response_model=List[Tuple[date, str]])
async def get_q1_time(file_path: str):
    try:
        result = q1_time(file_path)
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/q1/memory", response_model=List[Tuple[date, str]])
async def get_q1_memory(file_path: str):
    try:
        result = q1_memory(file_path)
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/q2/time", response_model=List[Tuple[str, int]])
async def get_q2_time(file_path: str):
    try:
        result = q2_time(file_path)
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/q2/memory", response_model=List[Tuple[str, int]])
async def get_q2_memory(file_path: str, batch_size: int = 1000):
    try:
        result = q2_memory(file_path, batch_size=batch_size)
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/q3/time", response_model=List[Tuple[str, int]])
async def get_q3_time(file_path: str):
    try:
        result = q3_time(file_path)
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/q3/memory", response_model=List[Tuple[str, int]])
async def get_q3_memory(file_path: str):
    try:
        result = q3_memory(file_path)
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
