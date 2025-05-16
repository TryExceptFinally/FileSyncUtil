import pydicom

from src.database import DatabaseConnector
from src.dicom.exceptions import DicomError, TagNotFoundError


class MakstorRepository:
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector

    def get_volume_path(self, share_uid: int) -> str:
        with self.db_connector as db:
            result = db.execute(
                f"""select share_path from shares where share_uid={share_uid}"""
            ).fetchone()
            return result[0] if result else None

    def get_image_by_id(self, uid: int) -> tuple | None:
        with self.db_connector as db:
            result = db.execute(
                f"""select image_uid, image_path from images where image_uid={uid}"""
            ).fetchone()
            return result

    def get_image_by_uid(self, uid_in_file: str) -> tuple | None:
        with self.db_connector as db:
            result = db.execute(
                f"""select image_uid, image_path from images 
                where images_uid_in_file='{uid_in_file}'"""
            ).fetchone()
            return result

    def update_image(self, image_id: int, share_uid: int, image_path: str):
        with self.db_connector as db:
            db.execute(
                f"""update images 
                set share_uid={share_uid}, image_path='{image_path}' 
                where image_uid={image_id}"""
            )
