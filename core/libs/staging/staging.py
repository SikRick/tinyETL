"""
This module has classes and methods to supportstaging ie; loading a specified flatfile to a specified database

R 0.1 : only reads local files, only allows csv files
"""
__author__ = "SikRick"

import findspark
findspark.init()
import os
from pyspark.sql import SparkSession


class Stage:
    """
    Stage is a class built to handle staging the flatfile to a specified database.
    """

    def __init__(self, file_path:str, table_name:str, database:str, replace:bool=False, delimiter:chr=",", header:bool=True, validations:dict={}, transaformations:dict={}, session:str="training"):
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
        self.transformations = transaformations
        self.validations = validations
        self.header = header
        self.session = session
        self.staged = False

    def load_to_staging_table(self):
        """
        Dumps the given flatfile to a staging table
        """
        self.spark = SparkSession.builder.config("spark.sql.warehouse.dir", os.environ["HOME"]+"/tinyETL").appName(self.session).getOrCreate()
        try:
            print(f"staging started for file {self.file_path}")
            df = self.spark.read.options(header=self.header, delimiter=self.delimiter).csv(self.file_path)
            try:
                self.spark.catalog.setCurrentDatabase(self.database)
            except Exception:
                print("database not found...creating..")
                self.spark.sql(f"create database {self.database}")
            df.write.saveAsTable(f"{self.database}.{self.table_name}")
            self.staged = True
            print("data staging completed")
        except FileNotFoundError:
            print("Error finding the specified file!!!")
        return self

    def apply_validations(self):
        rejects_df = None
        if self.staged:
            for validation_name, validation in self.validations.items():
                query = validation["query"]
                flag = validation["flag"]
                print(f"Running validation {validation_name} : TableName -> {self.table_name}")
                if rejects_df:
                    #check if rejects_df is initialized
                    current_df = self.spark.sql(query)
                    rejects_df = rejects_df.union(current_df)
                else:
                    print("initializing error table")
                    rejects_df = self.spark.sql(query)
            rejects_df.write.saveAsTable(f"{self.database}.{self.table_name}_Rejects")
        else:
            print("staging not completed")
            pass
        print(f"validations ran for table : {self.table_name}")
        return self

            
    def generate_transformations(self):
        pass