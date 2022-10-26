from dataclasses import dataclass, field
from tabulate import tabulate
import datetime


@dataclass
class TableView():
    header: list
    metadata: list = field(repr=False)
    table: list = field(repr=False)
    name: str = field(default=None)
    display_cols: list = field(default=None, repr=False)
    _datetime_format: str = field(default='%b %d, %Y - %H:%M', repr=False)

    def print(self):
        LOCAL_TIMEZONE = datetime.datetime.now().astimezone().tzinfo

        formatted_table = []

        if self.display_cols is not None:
            col_idx_select = []
            formatted_header = []
            for idx, item in enumerate(self.header):
                if item in self.display_cols:
                    col_idx_select.append(idx)
                    formatted_header.append(item)
            for row in self.table:
                formatted_row = []
                for idx, item in enumerate(row):
                    if idx in col_idx_select:
                        if isinstance(item, datetime.datetime):
                            item = item.astimezone(LOCAL_TIMEZONE).strftime(
                                self._datetime_format)

                        formatted_row.append(item)

                formatted_table.append(formatted_row)
        else:
            formatted_header = self.header
            for row in self.table:
                formatted_row = []
                for idx, item in enumerate(row):
                    if isinstance(item, datetime.datetime):
                        item = item.astimezone(LOCAL_TIMEZONE).strftime(
                            self._datetime_format)

                    formatted_row.append(item)

                formatted_table.append(formatted_row)

        return tabulate(formatted_table, formatted_header, tablefmt="pretty")
