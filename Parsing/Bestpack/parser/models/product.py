from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import TEXT, INTEGER, NUMERIC
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()

class Product(Base):

    __tablename__ = "bestpack_products"
    __table_args__ = {'schema': 'marketplaces'}

    product_id: Mapped[int] = mapped_column(INTEGER, primary_key=True, autoincrement=True)
    source_code: Mapped[str] = mapped_column(TEXT, nullable=True)
    title: Mapped[str] = mapped_column(TEXT)
    category_id: Mapped[int] = mapped_column(INTEGER)
    overall_pack_price: Mapped[float] = mapped_column(NUMERIC(10,2))
    overall_box_price: Mapped[float] = mapped_column(NUMERIC(10,2))
    per_price: Mapped[float] = mapped_column(NUMERIC(10,2))
    per_discount_price: Mapped[float] = mapped_column(NUMERIC(10,2), nullable=True)
    product_url: Mapped[str] = mapped_column(TEXT)
    product_url_hash: Mapped[str] = mapped_column(TEXT)
