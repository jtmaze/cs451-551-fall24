import json

from lstore.table import Table
import os

from lstore.storage.rid import RID
from lstore.storage.buffer.page_table import PageTable

from lstore.index_types.index_config import IndexConfig

class Database():

    def __init__(self):
        self.metadata_file = "metadata.json"
        self.tables = dict()
        self.path = "./CS451"

        self._set_uid_gen_path(self.path)

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

        pages_path = os.path.join(self.path, "pages")
        if not os.path.exists(pages_path):
            os.makedirs(pages_path)

        # Load metadata from file to recreate tables if metadata exists
        metadata_path = os.path.join(self.path, self.metadata_file)
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as meta_file:
                metadata: dict = json.load(meta_file)
            for table_name, table_info in metadata.get("tables", {}).items():
                self._restore_table(table_name, table_info)

        # Overwrite database path for UID generators
        self._set_uid_gen_path(path)

    def close(self):
        """
        Closes the database by ensuring all in-memory data is safely flushed to disk.
        """
        for table in self.tables.values():
            # Ensures any dirty pages are written to disk
            table.flush_pages()

        # Save metadata about tables
        self._save_metadata()
        self.tables.clear()
        self.path = None

    def reconstruct_indices(self):
        """
        Reconstructs the indices for each table in the database.
        This function is called after restoring tables to ensure
        all indices are accurate and up-to-date.
        """
        for table in self.tables.values():
            table.reconstruct_index()

    def _save_metadata(self):
        """
        Saves the metadata for the database to the metadata file.
        """
        metadata = {"tables": dict()}

        for table_name, table in self.tables.items():
            indices = table.index.indices
            index_cols = [i for i in range(len(indices)) if indices[i] is not None]

            metadata["tables"][table_name] = {
                "num_columns": table.num_columns,
                "key_index": table.key,
                "index_cols": index_cols,
                # Save additional table settings as needed
            }

        metadata_path = os.path.join(self.path, self.metadata_file)
        with open(metadata_path, 'w') as meta_file:
            json.dump(metadata, meta_file)
        print("Metadata saved successfully.")

    def _restore_table(self, name, table_info):
        """
        Restores a table using the metadata loaded from disk.
        """
        num_columns = table_info.get("num_columns")
        key_index = table_info.get("key_index")
        index_config = IndexConfig()  # Customize this if you have saved index details

        table = Table(name, num_columns, key_index, self.path, index_config)
        self.tables[name] = table  # Recreate the table in the database

        # Reconstruct the index
        table.reconstruct_index(table_info.get("index_cols"))

        print(f"Restored and indexed table '{name}' with {num_columns} columns.")

    def _set_uid_gen_path(self, path):
        RID.initialize_uid_gen(path)
        PageTable.initialize_uid_gen(path)

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

        table = Table(name, num_columns, key_index, self.path, index_config)
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
