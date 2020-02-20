from sqlalchemy import Integer
from sqlalchemy.dialects.mysql import INTEGER

UnsignedInt = Integer()
UnsignedInt = UnsignedInt.with_variant(INTEGER(unsigned=True), 'mysql')
