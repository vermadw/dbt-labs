from dbt.adapters.base import Column


class PostgresColumn(Column):
    TYPE_LABELS = {
        "STRING": "TEXT",
        "DATETIME": "TIMESTAMP",
        "DATETIMETZ": "TIMESTAMPTZ",
        "STRINGARRAY": "TEXT[]",
        "INTEGERARRAY": "INT[]",
        "DECIMALARRAY": "DECIMAL[]",
        "BOOLEANARRAY": "BOOL[]",
        "DATEARRAY": "DATE[]",
        "DATETIMEARRAY": "TIMESTAMP[]",
        "DATETIMETZARRAY": "TIMESTAMPTZ[]",
    }

    @property
    def data_type(self):
        # on postgres, do not convert 'text' or 'varchar' to 'varchar()'
        if self.dtype.lower() == "text" or (
            self.dtype.lower() == "character varying" and self.char_size is None
        ):
            return self.dtype

        return super().data_type
