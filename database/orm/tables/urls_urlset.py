from database.orm.tables._abstract_table import _AbstractTable
from sqlalchemy.sql import and_


class UrlsUrlset(_AbstractTable):
    def _check_existing_url(self, protocol: str, domain: str, path: str, query: str):
        existing_row = self._orm.execute(self._table.select().where(and_(
            protocol == self._table.c.protocol,
            domain == self._table.c.domain,
            path == self._table.c.path,
            query == self._table.c.query,
        ))).first()

        if existing_row is not None:
            for column, value in existing_row.items():
                if 'id' == column:
                    return value

        return None

    def add(self, urlset: str, protocol: str, domain: str, path: str, query: str, row_id: int = None) -> int:
        self._table = self._orm.tables.table_urlset_urls(urlset)

        existing_row_id = self._check_existing_id(row_id)

        if existing_row_id is not None:
            return existing_row_id

        existing_row_id = self._check_existing_url(protocol, domain, path, query)

        if existing_row_id is not None:
            return existing_row_id

        self._orm.execute(
            self._table.insert().values(id=row_id, protocol=protocol, domain=domain, path=path, query=query)
        )

        existing_row_id = self._check_existing_url(protocol, domain, path, query)

        if existing_row_id is None:
            raise Exception("URL not found after insert.")

        return existing_row_id
