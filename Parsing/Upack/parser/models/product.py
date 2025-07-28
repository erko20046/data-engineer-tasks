from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import TEXT, INTEGER, NUMERIC
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()


class Product(Base):
    __tablename__ = "upack_products"
    __table_args__ = {'schema': 'marketplaces'}

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True, autoincrement=True)
    source_code: Mapped[str] = mapped_column(TEXT, nullable=True)
    title: Mapped[str] = mapped_column(TEXT)
    category_id: Mapped[int] = mapped_column(INTEGER)
    product_url: Mapped[str] = mapped_column(TEXT)
    per_price: Mapped[float] = mapped_column(NUMERIC(10, 2))
    quantity_per_box: Mapped[int] = mapped_column(INTEGER)
    quantity_per_pack: Mapped[int] = mapped_column(INTEGER)
    min_quantity: Mapped[int] = mapped_column(INTEGER)
    min_batch_price: Mapped[float] = mapped_column(NUMERIC(10, 2))