from database.orm.tables._abstract_table import _AbstractTable
from sqlalchemy.sql import and_
from datetime import datetime


class ChecksUrlset(_AbstractTable):
    def _check_existing_check(self, url: int, check: str):
        existing_row = self._orm.execute(self._table.select().where(and_(
            url == self._table.c.url,
            check == self._table.c.check
        ))).first()

        if existing_row is not None:
            for column, value in existing_row.items():
                if 'id' == column:
                    return value

        return None

    def add(
            self,
            urlset: str,
            url: int,
            check: str,
            valid: bool,
            value: str = '',
            diff: str = '',
            error: str = ''
    ) -> int:
        self._table = self._orm.tables.table_urlset_checks(urlset)

        now = datetime.utcnow()

        result = self._orm.execute(self._table.insert().values(
            created=now,
            last_checked=now,
            url=url,
            check=check,
            valid=valid,
            value=value,
            diff=diff,
            error=error
        ))

        insert_id = result.inserted_primary_key[0]

        return insert_id
