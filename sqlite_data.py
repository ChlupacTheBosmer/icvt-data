import sqlite3
import json

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
            low_fps_video_frame_number INTEGER,
            detection BOOLEAN,
            number_of_visitors INTEGER,
            bounding_boxes TEXT,
            detection_confs TEXT,
            visitor_classes TEXT
        )
        ''')

        # Commit changes and close the connection
        conn.commit()
        conn.close()

    def add_database_entry(self, video_id, timestamp, roi_number, frame_number,
                           visit_number, x1, y1, x2, y2, frame_path, low_fps_video_frame_number: int = -1,
                           detection: bool = False, number_of_visitors: int = -1, bounding_boxes=None,
                           detection_confs=None, visitor_classes=None):

        # Process default parameters
        if bounding_boxes is None:
            bounding_boxes = []
        if detection_confs is None:
            detection_confs = []
        if visitor_classes is None:
            visitor_classes = []

        # Serialize lists
        bounding_boxes = json.dumps(bounding_boxes)
        detection_confs = json.dumps(detection_confs)
        visitor_classes = json.dumps(visitor_classes)

        # Connect to the database
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Insert a new frame entry
        insert_query = '''
        INSERT INTO Frames (video_id, timestamp, roi_number, frame_number,
                            visit_number, x1, y1, x2, y2, frame_path, low_fps_video_frame_number, detection, 
                            number_of_visitors, bounding_boxes, detection_confs, visitor_classes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        data = (video_id, timestamp, roi_number, frame_number, visit_number,
                x1, y1, x2, y2, frame_path, low_fps_video_frame_number, detection,
                number_of_visitors, bounding_boxes, detection_confs, visitor_classes)
        cursor.execute(insert_query, data)

        last_inserted_id = cursor.lastrowid

        # Commit changes and close the connection
        conn.commit()
        conn.close()
        return last_inserted_id

    def add_multiple_entries(self, entries):
        # Connect to the database
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Serialize values in each entry
        serialized_entries = []
        for entry in entries:
            video_id, timestamp, roi_number, frame_number, visit_number, x1, y1, x2, y2, frame_path, low_fps_video_frame_number, detection, number_of_visitors, bounding_boxes, detection_confs, visitor_classes = entry

            # Serialize lists
            bounding_boxes = json.dumps(bounding_boxes)
            detection_confs = json.dumps(detection_confs)
            visitor_classes = json.dumps(visitor_classes)

            serialized_entry = (video_id, timestamp, roi_number, frame_number, visit_number, x1, y1, x2, y2, frame_path,
                                low_fps_video_frame_number, detection, number_of_visitors, bounding_boxes,
                                detection_confs, visitor_classes)
            serialized_entries.append(serialized_entry)

        # Use a transaction to insert multiple entries
        try:
            cursor.executemany('''
                INSERT INTO Frames (video_id, timestamp, roi_number, frame_number,
                                    visit_number, x1, y1, x2, y2, frame_path, low_fps_video_frame_number, detection, 
                                    number_of_visitors, bounding_boxes, detection_confs, visitor_classes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', serialized_entries)

            conn.commit()  # Commit the transaction
        except sqlite3.Error:
            conn.rollback()  # Rollback if an error occurs
        finally:
            conn.close()

    def update_detection_to_last_column(self, entries):
        # Connect to the database
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Update values in each entry
        for entry in entries:
            frame_number, detection, number_of_visitors, bounding_boxes, detection_confs, visitor_classes, roi_number = entry

            # Serialize lists
            bounding_boxes = json.dumps(bounding_boxes)
            detection_confs = json.dumps(detection_confs)
            visitor_classes = json.dumps(visitor_classes)

            # Update the database row
            try:
                cursor.execute('''
                    UPDATE Frames
                    SET detection = ?,
                        number_of_visitors = ?,
                        bounding_boxes = ?,
                        detection_confs = ?,
                        visitor_classes = ?
                    WHERE frame_number = ? AND roi_number = ?
                ''', (detection, number_of_visitors, bounding_boxes, detection_confs, visitor_classes, frame_number,
                      roi_number))

                conn.commit()  # Commit the transaction
            except sqlite3.Error:
                conn.rollback()  # Rollback if an error occurs

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

    def update_column_value(self, table_name, column_name, new_value, *args):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        condition_columns = args[::2]
        condition_values = args[1::2]
        condition_format = " AND ".join([f"{col} = ?" for col in condition_columns])

        update_query = f'''
        UPDATE {table_name}
        SET {column_name} = ?
        WHERE {condition_format};
        '''

        data = (new_value,) + tuple(condition_values)

        cursor.execute(update_query, data)

        conn.commit()
        conn.close()

    # def update_detection_info(self, id, detection_info):
    #
    #         # Extract the values from the dictionary
    #         detection, num_visitors, boxes, confs, classes = detection_info
    #
    #         # Serialize the lists as JSON strings
    #         serialized_boxes = json.dumps(boxes)
    #         serialized_confs = json.dumps(confs)
    #         serialized_classes = json.dumps(classes)
    #
    #         # Update the database columns with both low_fps_video_frame_number and roi_number conditions
    #         condition_args = ('id', id)
    #
    #         self.update_column_value('Frames', 'detection', detection, *condition_args)
    #         self.update_column_value('Frames', 'number_of_visitors', num_visitors, *condition_args)
    #         self.update_column_value('Frames', 'bounding_boxes', serialized_boxes, *condition_args)
    #         self.update_column_value('Frames', 'detection_confs', serialized_confs, *condition_args)
    #         self.update_column_value('Frames', 'visitor_classes', serialized_classes, *condition_args)

    def update_detection_info(self, object_detection_metadata, roi_number):
        for frame_number, detection_info in object_detection_metadata.items():
            low_fps_frame_number = frame_number  # Assuming the dictionary key is same as low_fps_video_frame_number

            # Extract the values from the dictionary
            detection, num_visitors, boxes, confs, classes = detection_info

            # Serialize the lists as JSON strings
            serialized_boxes = json.dumps(boxes)
            serialized_confs = json.dumps(confs)
            serialized_classes = json.dumps(classes)

            # Update the database columns with both low_fps_video_frame_number and roi_number conditions
            condition_args = ('low_fps_video_frame_number', low_fps_frame_number, 'roi_number', roi_number)

            self.update_column_value('Frames', 'detection', detection, *condition_args)
            self.update_column_value('Frames', 'number_of_visitors', num_visitors, *condition_args)
            self.update_column_value('Frames', 'bounding_boxes', serialized_boxes, *condition_args)
            self.update_column_value('Frames', 'detection_confs', serialized_confs, *condition_args)
            self.update_column_value('Frames', 'visitor_classes', serialized_classes, *condition_args)

# Example usage
if __name__ == "__main__":
    db = frameDatabase("my_metadata.db")
    db.add_database_entry("video123", "2023-08-24", 1, 1, 1, 10, 20, 30, 40, "path_to_frame.jpg", 1)