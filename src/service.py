from src.database import DatabaseConnector


def get_volume_path(db_connector: DatabaseConnector, share_uid: int) -> str:
    with db_connector as db:
        result = db.execute(
            f"""select share_path from shares where share_uid={share_uid}"""
        ).fetchone()
        return result[0] if result else None


def get_image_name_by_uid(db_connector: DatabaseConnector, uid: int) -> str | None:
    with db_connector as db:
        result = db.execute(
            f"""select image_name from images where image_uid={uid}"""
        ).fetchone()
        return result[0] if result else None


def get_image_uid_by_name(db_connector: DatabaseConnector, name: str) -> int:
    with db_connector as db:
        result = db.execute(
            f"""select image_uid from images where image_name='{name}'"""
        ).fetchone()
        return result[0] if result else None


def update_image(db_connector: DatabaseConnector, image_uid: int, share_uid: int, image_path: str):
    with db_connector as db:
        db.execute(
            f"""update images 
            set share_uid={share_uid}, image_path='{image_path}' 
            where image_uid='{image_uid}'"""
        )
