from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import TEXT, INTEGER, BIGINT
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()

class Picture(Base):

    __tablename__ = "bestpack_pictures"
    __table_args__ = {'schema': 'marketplaces'}

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    product_url_hash: Mapped[str] = mapped_column(TEXT)
    image_url: Mapped[str] = mapped_column(TEXT)
    path: Mapped[str] = mapped_column(TEXT)