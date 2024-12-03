from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.service.controller_service.crawl_controller import CrawlController

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"message": "Hello, World!"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}


if __name__ == '__main__':
    # uvicorn.run(
    #     "src.main:app",
    #     host=SERVER_HOST,
    #     port=SERVER_PORT,
    #     reload=True
    # )

    # 6. Crawl_Data:
    # 6.1 Khởi tạo đối tượng CrawlController
    c = CrawlController()
    c.get_config()
