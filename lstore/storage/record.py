
class Record:
    """
    Data record (not metadata). RID gets populated by page directory.
    :param rid:
    :param key:
    :param columns: Array of data values
    """

    def __init__(self, key, columns, rid=None):
        self.key = key
        self.columns = columns

        self.rid = rid

    def __repr__(self) -> str:
        return str(self.columns)