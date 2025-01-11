from fastapi import FastAPI, Depends, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from database import init_db, get_async_db, Product
from parser import run_parser
from apscheduler.schedulers.asyncio import AsyncIOScheduler


app = FastAPI()
scheduler = AsyncIOScheduler()
is_parsing = False
active_connections = []

@app.websocket("/api/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)

async def notify_clients(message: str):
    for connection in active_connections:
        await connection.send_text(message)

@app.on_event("startup")
async def on_startup():
    await init_db()

@app.get("/api/products")
async def get_all_products(db: AsyncSession = Depends(get_async_db)):
    result = await db.execute(select(Product).order_by(func.random()).limit(10))
    products = result.scalars().all()
    return products

@app.get("/api/products/{id}")
async def get_product_by_id(id: int, db: AsyncSession = Depends(get_async_db)):
    result = await db.execute(select(Product).filter(Product.id == id))
    product = result.scalar_one_or_none()
    if product is None:
        return JSONResponse(status_code=404, content={"message": "Товар не найден"})
    return product


@app.post("/api/products")
async def create_product(request: Request, db: AsyncSession = Depends(get_async_db)):
    data = await request.json()
    new_product = Product(
        name=data["name"],
        category=data["category"],
        price=data["price"]
    )


    db.add(new_product)
    await db.commit()
    await db.refresh(new_product)

    await notify_clients(f"Создан новый товар: {new_product.id}, Название: {new_product.name}")

    return {
        "message": "Товар успешно создан",
        "product": {
            "id": new_product.id,
            "name": new_product.name,
            "category": new_product.category,
            "price": new_product.price
        }
    }

@app.put("/api/products/{id}")
async def update_product(id: int, request: Request, db: AsyncSession = Depends(get_async_db)):
    result = await db.execute(select(Product).filter(Product.id == id))
    product = result.scalar_one_or_none()
    if product is None:
        return JSONResponse(status_code=404, content={"message": "Товар не найден"})

    data = await request.json()
    product.name = data.get("name", product.name)
    product.category = data.get("category", product.category)
    product.price = data.get("price", product.price)
    try:
        db.add(product)
        await db.commit()
        await db.refresh(product)
        await notify_clients(f"Обновлен товар: {product.id}, Название: {product.name}")
    except:
        return JSONResponse(status_code=400, content={"message": "Некорректный запрос"})

    return {"message": "Товар успешно обновлен", "product": {
        "id": product.id,
        "name": product.name,
        "category": product.category,
        "price": product.price
    }}

@app.delete("/api/products/{id}")
async def delete_product(id: int, db: AsyncSession = Depends(get_async_db)):
    result = await db.execute(select(Product).filter(Product.id == id))
    product = result.scalar_one_or_none()
    if product is None:
        return JSONResponse(status_code=404, content={"message": "Товар не найден"})

    await db.delete(product)
    await db.commit()
    await notify_clients(f"Удален товар: {product.id}, Название: {product.name}")
    return {"result": "Товар удален успешно"}

@app.post("/api/start-parser")
async def start_parser():
    global is_parsing
    if not is_parsing:
        is_parsing = True
        scheduler.add_job(run_parser, 'interval', minutes=1)
        scheduler.start()
        await notify_clients("Парсер запущен")
        return {"message": "Парсер запущен"}
    else:
        return {"message": "Парсер уже работает"}

@app.post("/api/stop-parser")
async def stop_parser():
    global is_parsing
    if is_parsing:
        is_parsing = False
        scheduler.remove_all_jobs()
        await notify_clients("Парсер остановлен")
        return {"message": "Парсер остановлен"}
    else:
        return {"message": "Парсер не был запущен"}
