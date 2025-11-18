import uvicorn
from _core_route import app

if __name__ == "__main__":
    host = "0.0.0.0"
    port = 4079
    uvicorn.run("_core_route:app", host=host, port=port, reload=True,  workers=4)