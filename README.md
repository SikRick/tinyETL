<h1>A light weight ETL platform written in python [Read wiki for more info]</h1>

<h3>Usage</h3>
<ul>
    <li>staging : Stage(file_path="/home/aj/Projects/tinyETL/core/tests/datasets/BankChurners.csv", table_name="Credit", database="tinyetl").load_to_staging_table()</li>
    <li>validations : Stage(file_path="/home/aj/Projects/tinyETL/core/tests/datasets/BankChurners.csv", table_name="Credit", database="tinyetl_validations", validations=validations).load_to_staging_table().apply_validations()</li>
</ul>
