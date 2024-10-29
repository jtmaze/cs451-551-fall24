from lstore.table import Table

from lstore.index_types.index_config import IndexConfig

class Database():

    def __init__(self):
        self.tables = dict()
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key_index, index_config=None):
        """
        # Creates a new table
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        :param index_config
        """
        if index_config is None:
            index_config = IndexConfig()

        table = Table(name, num_columns, key_index, index_config)
        self.tables[name] = table  # Add the table to the database's table dictionary.
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        """
        # Deletes the specified table
        """
        if name in self.tables:
            del self.tables[name]
        else:
            raise Exception(f"Table '{name}' does not exist.")

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        """
        # Returns table with the passed name
        """
        if name in self.tables:
            return self.tables[name]
        else:
            raise Exception(f"Table '{name}' does not exist.")
