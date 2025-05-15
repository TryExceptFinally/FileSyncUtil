import pydicom

from src.dicom.exceptions import DicomError, TagNotFoundError


class DicomRepository:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file = self.load_dicom_file()

    def load_dicom_file(self):
        try:
            return pydicom.dcmread(self.file_path)
        except Exception as e:
            raise DicomError(f'Ошибка при загрузке DICOM файла: {e}')

    def get_tag(self, tag: str):
        if tag in self.file:
            return self.file[tag].value
        else:
            raise TagNotFoundError(f'Тег {tag} не найден в DICOM файле.')
