"""
This module has classes and methods to supportstaging ie; loading a specified flatfile to a specified database

R 0.1 : only reads local files, only allows csv files
"""
__author__ = "SikRick"

import findspark
findspark.init()
from pyspark.sql import SparkSession


class Stage:
    """
    Stage is a class built to handle staging the flatfile to a specified database.
    """

    def __init__(self, file_path:str, table_name:str, database:str, replace:bool=False, delimiter:chr=",", header:bool=True, session:str="training"):
        """
        params 
        ------
        file_path : specify the path of the file to stage <might move to cloud storage>
        table_name : specify the name of the staged table
        database : specify the database <might change to separate db config>
        replace : flag to specify to delete exisitng stages tables
        delimiter : specifying the delimiter of the file, defaulted to ','
        headers : flag to specify if headers are present in the file
        session : SparkSession name ; defaulted to training <might change in future releases>
        """
        self.file_path = file_path
        self.table_name = table_name
        self.database = database
        self.replace = replace
        self.delimiter = delimiter
        self.header = header
        self.session = session

    def load_to_staging_tables(self):
        spark = SparkSession.builder.appName(self.session).getOrCreate()
        try:
            print(f"staging started for file {self.file_path}")
            df = spark.read.options(header=self.header, delimiter=self.delimiter).csv(self.file_path)
            try:
                spark.sql(f"use {self.database}")
            except Exception:
                print("database not found...creating..")
                spark.sql(f"create database {self.database}")
            df.write.saveAsTable(f"{self.database}.{self.table_name}")
            print("data staging completed")
        except FileNotFoundError:
            print("Error finding the specified file!!!")