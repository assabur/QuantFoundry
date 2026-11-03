import os.path
from os import listdir
from os.path import isfile, join
from typing import List


class ConfigHandler:
    """
    This class is used to read configuration files from local
    It can check if the path is a folder or a file.
    In case of the folder, it will read all the files in the folder and return a list of strings
    In case of the file, it will read a single file and return a list of strings
    """

    def __init__(self):
        pass

    @staticmethod
    def check_if_folder_local(path):
        """
        Check if the path is a folder or a file
        """
        return os.path.isdir(path)

    @staticmethod
    def list_of_files(path) -> List[str]:
        """
        List all the files in the folder
        """
        files = [f for f in listdir(path) if isfile(join(path, f))]
        return sorted(file for file in files if file.endswith((".yaml", ".yml")))

    def read_as_string_local(self, path, encoding: str = "utf-8") -> List[str]:
        """
        List all the files in the folder and read them as string
        """
        if not self.check_if_folder_local(path):
            return [open(path, encoding=encoding).read()]
        return [open(join(path, file), encoding=encoding).read() for file in self.list_of_files(path)]
