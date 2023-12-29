from collections import UserDict
from dataclasses import dataclass

import pandas


@dataclass
class CDMCodeTable:
    name: str
    table: pandas.DataFrame


class CDMCodeTables(UserDict):
    def __init__(self, cdm_tables_dict: dict[str, CDMCodeTable]):
        UserDict.__init__(self)
        self.update(cdm_tables_dict)
