import random
import asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from database import async_session, Product

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
}

category_name = {
    "1": "televizory",
    "2": "smartfony",
    "3": "noutbuki",
    "4": "kofemashiny"
}

async def fetch_and_save_products(session: AsyncSession, category: str, page: int):
    query = """
    query GetSubcategoryProductsFilter($subcategoryProductsFilterInput: CatalogFilter_ProductsFilterInput!) {
        productsFilter(filter: $subcategoryProductsFilterInput) {
            record {
                products {
                    id
                    name
                    price {
                        current
                    }
                    category {
                        name
                    }
                }
            }
        }
    }
    """

    variables = {
        "subcategoryProductsFilterInput": {
            "categorySlug": category,
            "pagination": {"page": page, "perPage": 20},
            "sorting": {"id": "", "direction": "SORT_DIRECTION_DESC"},
        }
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                url='https://www.citilink.ru/graphql/',
                headers=headers,
                json={"query": query, "variables": variables},
                timeout=10.0
            )

            if response.status_code == 429:
                return

            data = response.json()
            products = data['data']['productsFilter']['record']['products']

            for product in products:
                new_product = Product(
                    name=product['name'],
                    category=product['category']['name'],
                    price=float(product['price']['current']) if product['price']['current'].isdigit() else 0
                )
                session.merge(new_product)
            await session.commit()
        except Exception as e:
            print(f"Ошибка при запросе категории '{category}': {e}")

async def run_parser():
    async with async_session() as session:
        tasks = []
        for _, category in category_name.items():
            page = random.randint(1, 10)
            tasks.append(fetch_and_save_products(session, category, page))
        await asyncio.gather(*tasks)
