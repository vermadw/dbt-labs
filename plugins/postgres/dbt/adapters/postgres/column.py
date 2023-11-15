from dbt.adapters.base import Column


class PostgresColumn(Column):
    _DTYPE_ARRAY_TO_DATA_TYPE = {"stringarray": "text[]"}

    @property
    def data_type(self):
        # on postgres, do not convert 'text' or 'varchar' to 'varchar()'
        if self.dtype.lower() == "text" or (
            self.dtype.lower() == "character varying" and self.char_size is None
        ):
            return self.dtype

        if self.dtype.lower() in self._DTYPE_ARRAY_TO_DATA_TYPE:
            return self._DTYPE_ARRAY_TO_DATA_TYPE[self.dtype.lower()]

        return super().data_type
