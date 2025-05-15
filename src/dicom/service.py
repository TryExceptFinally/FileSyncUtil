from src.dicom.repository import DicomRepository


class DicomService:
    def __init__(self, file_path: str):
        self.repository = DicomRepository(file_path)

    def get_image_uid(self):
        return self.repository.get_tag('00080018')
