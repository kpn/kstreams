import logging

import uvicorn


def main():
    uvicorn.run(
        app="fastapi_webapp.app:app",
        host="localhost",
        port=8000,
        log_level=logging.INFO,
        reload=True,
        debug=True,
    )


if __name__ == "__main__":
    main()
