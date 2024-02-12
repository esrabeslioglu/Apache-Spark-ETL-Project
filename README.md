# Apache-Spark-ETL-Project

This project takes the data science employee salaries data and after making some transforms, loads it into a table in a database which is created in Google Cloud Platform.

This project was made in databricks environment.


1. The file ds_salaries.csv is uploaded to DBFS of Databricks. 
2. To see the DBFS option in catalog section, DBFS File Browser option should be enabled like below.

![image](https://github.com/esrabeslioglu/Apache-Spark-ETL-Project/assets/52747952/5fca00b3-19cb-492d-82b5-b2245565e20f)

3. We create a compute like below.

![image](https://github.com/esrabeslioglu/Apache-Spark-ETL-Project/assets/52747952/f84238db-55df-41fe-a033-b0aae44e5dea)

4. After creating the notebook as shown below, we choose the cluster from top right of the screen.

![image](https://github.com/esrabeslioglu/Apache-Spark-ETL-Project/assets/52747952/214836ef-c56a-484b-a281-2b93dc53c6b8)

![image](https://github.com/esrabeslioglu/Apache-Spark-ETL-Project/assets/52747952/7b4116a3-d050-4f2a-8cf8-c4665e5c58db)

The code is in data folder. To load the final transformed data to a database we need below information of the database:

* The Public IP Adress and port
* Driver
* User and password

DBeaver which is a database tool can be downloaded to see whether the table is loaded.

On DBeaver, we can add the database. Steps below can be followed:
- Create new database connection
- Choose PostgreSQL
- Paste the Public IP address of the database to the host
- Enter the user and password which are given while creating the database on GCP.

![image](https://github.com/esrabeslioglu/Apache-Spark-ETL-Project/assets/52747952/45eb4136-48fc-435d-a420-7e62d199b9f4)





