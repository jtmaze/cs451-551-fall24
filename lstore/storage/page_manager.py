# page_manager.py
class PageManager:
    def __init__(self):
        self.current_page_id = 0  # Start from 0 or load the last used ID from metadata if restoring

    def generate_page_id(self):
        """
        Generates a unique page_id for each new page.
        """
        page_id = self.current_page_id
        self.current_page_id += 1
        return page_id
