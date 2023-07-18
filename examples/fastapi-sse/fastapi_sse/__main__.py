import logging

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        app="fastapi_sse.app:app",
        host="localhost",
        port=8000,
        log_level=logging.INFO,
        reload=True,
        debug=True,
    )
