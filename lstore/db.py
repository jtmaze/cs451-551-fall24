from lstore.table import Table
import os

from lstore.index_types.index_config import IndexConfig

class Database():

    def __init__(self):
        self.tables = dict()
        self.path = None

    # Not required for milestone1
    def open(self, path):
        """
        Opens the database at the specified path, initializing tables and loading
        metadata if applicable. Sets up directories and prepares storage.

        :param path: The filesystem path where the database is stored.
        """
        self.path = path

        # Create directory for database files if it doesn't exist
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        # Optional: Load metadata and initialize tables from storage files
        # self._load_metadata()

    def close(self):
        """
        Closes the database by ensuring all in-memory data is safely flushed to disk.
        """
        for table in self.tables.values():
            # Ensures any dirty pages are written to disk
            table.flush_pages()
        # Clear tables in memory
        self.tables.clear()
        # Clear the database path reference
        self.path = None

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
