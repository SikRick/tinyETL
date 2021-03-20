"""
This module has classes and methods to supportstaging ie; loading a specified flatfile to a specified database
"""
__author__ = "SikRick"

import findspark

class Stage:
    """
    Stage is a class built to handle staging the flatfile to a specified database.
    """

    def __init__(self, file_path:str, table_name:str, database:str, replace:bool=False, delimiter:chr=",", headers:bool=True):
        """
        params 
        ------
        file_path : specify the path of the file to stage <might move to cloud storage>
        table_name : specify the name of the staged table
        database : specify the database <might change to separate db config>
        replace : flag to specify to delete exisitng stages tables
        delimiter : specifying the delimiter of the file, defaulted to ','
        headers : flag to specify if headers are present in the file
        """
        self.file_path = file_path
        self.table_name = table_name
        self.database = database
        self.replace = replace
        self.delimiter = delimiter
        self.headers = headers

    