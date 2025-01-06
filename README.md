# luban
Repository for Luban take home assignment. 


Architecture Diagram: 

Tools Used and Why: 

Data Processing: Databricks
* Allow processing of batch and real-time data within the same notebook. 
Offers Data catalog 
Delta Tables which offers ACID transactions hence data warehousing opportunities. 
Has a Data Flow feature to enable easy data orchestration set up. 
Data engineer and Data scientist can easily collaborate within the same databricks workspace. 

Data visualization: PowerBI 
Versatile data visualization tool that can be directly connected to Databricks Tables.



Tables , fields and Description : 

A Data catalog "luban_cat_wks" was created in Databricks and Schema/Database created "ethereum" . Within this Database the following tables were created. 

Blocks Table. 
block_number. int
timestamp.  timestamp
transactions_count. int
gas_used. bigint
gas_limit  bigint
base_fee_per_gas. bigint
difficulty. bigint



Transactions 
eth_gas_processed





Considerations: 
* For the purpose of this assignment, only batch data ETL was set up.
* The Dataflow Job for automating data ingestion and processing will be created and disabled for now as it consumes money to run the job and clusters.
* 
