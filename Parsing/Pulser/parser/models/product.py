from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import TEXT, INTEGER
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()

class Product(Base):

    __tablename__ = "pulser_products"
    __table_args__ = {'schema': 'marketplaces'}

    source_id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    title: Mapped[str] = mapped_column(TEXT)
    category_id: Mapped[int] = mapped_column(INTEGER)
    price: Mapped[int] = mapped_column(INTEGER)
    product_url: Mapped[str] = mapped_column(TEXT)

    def __repr__(self):
        return f"Product(source_id={self.source_id!r}, " \
               f"title={self.title!r}, " \
               f"category_id={self.category_id!r}, " \
               f"price={self.price!r}, " \
               f"product_url={self.product_url!r})"

    @staticmethod
    async def fill_products(source_id : int, title : str, category_id : int, price : int, product_url : str):
        return Product(source_id=source_id, title=title, category_id=category_id, price=price, product_url=product_url)


