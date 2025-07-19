from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import BIGINT, TEXT, INTEGER
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()

class Characteristic(Base):

    __tablename__ = "bestpack_characteristics"
    __table_args__ = {'schema': 'marketplaces'}

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    product_url_hash: Mapped[str] = mapped_column(TEXT)
    characteristic: Mapped[str] = mapped_column(TEXT)
    value: Mapped[str] = mapped_column(TEXT)