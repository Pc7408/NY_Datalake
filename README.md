NewYorker Datalake Design:


This project provides solution for NewYorker Datalake design.

Datalake consists of three layers of data : Raw,Cleaned,Insight (Aggregated)

Raw Layer: This layer has all json files loaded from source to datalake environment. 

Cleaned Layer:This layer has all cleaned and formatted data loaded from previous raw layer.

Insight(Aggregated):This layer has aggregation like stars on business per week and no of checkins of a business on collected and cleaned JSON data.

All these layers consist of databases with respective names for doing further analysis.

There is a script Datalake_Solution_run.sh, use it to run python file .
Datalake_Solution_run.sh requires 5 arguments that is file path for business,checkin,review,tip and users json files.

Acknowledgements
Code is designed on Databricks Apache Spark and Python.

Authored by https://github.com/Pc7408

🔗 Links
https://github.com/Pc7408
