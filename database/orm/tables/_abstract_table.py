from database.orm import ORM
from abc import ABC


class _AbstractTable(ABC):
    def __init__(self, orm: ORM):
        self._orm = orm
        self._table = None

    def _check_existing_id(self, row_id: int = None):
        if row_id is not None:
            existing_row = self._orm.execute(self._table.select().where(row_id == self._table.c.id)).first()

            if existing_row is not None:
                for column, value in existing_row.items():
                    if 'id' == column:
                        return value

        return None
