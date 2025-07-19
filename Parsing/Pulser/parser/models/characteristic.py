from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import BIGINT, TEXT, INTEGER
from sqlalchemy.orm import Mapped, mapped_column

Base = declarative_base()

class Characteristic(Base):

    __tablename__ = "pulser_characteristics"
    __table_args__ = {'schema': 'marketplaces'}

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(INTEGER)
    characteristic: Mapped[str] = mapped_column(TEXT)
    value: Mapped[str] = mapped_column(TEXT)

    def __repr__(self):
        return  f"Characteristic(id={self.id!r}, " \
                f"source_id={self.source_id!r}, " \
                f"characteristic={self.characteristic!r}, " \
                f"value={self.value!r})"


    @staticmethod
    async def fill_characteristic(source_id:int, characteristic: str, value: str):
        return Characteristic(source_id=source_id, characteristic=characteristic, value=value)