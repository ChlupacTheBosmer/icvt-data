import sqlite3

class frameDatabase:
    def __init__(self, database_path):
        self.database_path = database_path
        self.create_metadata_database()

    def create_metadata_database(self):
        # Connect to the database (or create if it doesn't exist)
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Create the Frames table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Frames (
            id INTEGER PRIMARY KEY,
            video_id TEXT,
            timestamp TEXT,
            roi_number INTEGER,
            frame_number INTEGER,
            visit_number INTEGER,
            x1 INTEGER,
            y1 INTEGER,
            x2 INTEGER,
            y2 INTEGER,
            frame_path TEXT,
            low_fps_video_frame_number INTEGER
        )
        ''')

        # Commit changes and close the connection
        conn.commit()
        conn.close()

    def add_database_entry(self, video_id, timestamp, roi_number, frame_number,
                           visit_number, x1, y1, x2, y2, frame_path, low_fps_video_frame_number: int = -1):
        # Connect to the database
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Insert a new frame entry
        insert_query = '''
        INSERT INTO Frames (video_id, timestamp, roi_number, frame_number,
                            visit_number, x1, y1, x2, y2, frame_path, low_fps_video_frame_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        data = (video_id, timestamp, roi_number, frame_number, visit_number,
                x1, y1, x2, y2, frame_path, low_fps_video_frame_number)
        cursor.execute(insert_query, data)

        # Commit changes and close the connection
        conn.commit()
        conn.close()

    def execute_sql_script(self, script_path, params=None):

        with open(script_path, "r") as f:
            sql_query = f.read()

        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        if params is not None:
            cursor.execute(sql_query, params)
        else:
            cursor.execute(sql_query)

        result = cursor.fetchall()

        conn.close()
        return result

    def update_column_value(self, table_name, column_name, new_value, condition_column, condition_value):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        update_query = f'''
        UPDATE {table_name}
        SET {column_name} = ?
        WHERE {condition_column} = ?;
        '''

        data = (new_value, condition_value)

        cursor.execute(update_query, data)

        conn.commit()
        conn.close()

# Example usage
if __name__ == "__main__":
    db = frameDatabase("my_metadata.db")
    db.add_database_entry("video123", "2023-08-24", 1, 1, 1, 10, 20, 30, 40, "path_to_frame.jpg", 1)