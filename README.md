# Luban Take-Home Assignment 🚀

## Overview 📝
This repository contains the implementation of the Luban take-home assignment, focusing on processing and analyzing Ethereum blockchain data.

## Architecture 🏗️
[Architecture Diagram to be added]

## Technology Stack 🛠️

### Data Processing: Databricks 💫
- Unified platform for batch and real-time data processing
- Built-in Data Catalog for metadata management
- Delta Lake support with ACID transactions for reliable data warehousing
- Visual Data Flow feature for streamlined data orchestration
- Collaborative workspace for Data Engineers and Data Scientists

### Data Visualization: Power BI 📊
- Versatile visualization capabilities
- Direct integration with Databricks tables
- Rich interactive dashboards

## Data Model 📦

### Data Catalog Structure 📚
- Catalog: `luban_cat_wks`
- Schema/Database: `ethereum`

### Tables and Schema 🗃️

#### Blocks Table ⛓️
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| block_number | INT | Unique identifier for each block |
| timestamp | TIMESTAMP | Block creation timestamp |
| transactions_count | INT | Number of transactions in the block |
| gas_used | BIGINT | Total gas used in the block |
| gas_limit | BIGINT | Maximum gas limit for the block |
| base_fee_per_gas | BIGINT | Base fee per gas unit |
| difficulty | BIGINT | Mining difficulty of the block |

#### Transactions Table 💸
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| block_number | INT | Reference to the block |
| hash | STRING | Transaction hash |
| from_address | STRING | Sender's address |
| to_address | STRING | Recipient's address |
| value_wei | DECIMAL(38,0) | Transaction value in Wei |
| value_eth | DOUBLE | Transaction value in ETH |
| gas_price | BIGINT | Price per gas unit |
| gas | BIGINT | Gas used in transaction |

#### eth_gas_processed View 🔍
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| block_number | INT | Reference to the block |
| hash | STRING | Transaction hash |
| from_address | STRING | Sender's address |
| to_address | STRING | Recipient's address |
| value_wei | DECIMAL(38,0) | Transaction value in Wei |
| value_eth | DOUBLE | Transaction value in ETH |
| gas_price | BIGINT | Price per gas unit |
| gas | BIGINT | Gas used in transaction |
| block_timestamp | TIMESTAMP | Timestamp of the block |
| block_date | DATE | Date of the block |
| block_hour | INT | Hour of the block creation |
| block_minute | INT | Minute of the block creation |
| gas_category_str | STRING | Gas category as string |
| gas_category_int | STRING | Gas category as integer |



## Data Flow Process ⚡
1. Ingestion pipeline populates the `blocks` and `transactions` tables
2. The `eth_gas_processed` view is generated from the transactions table
3. Block tracking mechanism:
   - Utilizes `block_tracker.json` to store the latest processed block number
   - Prevents duplicate block processing
   - Maintains processing continuity

## Implementation Notes 📋

### Current Limitations ⚠️
- Implementation focuses on batch data ETL
- Dataflow Job automation is configured but disabled to manage costs
- Cluster usage is optimized for development purposes

### Future Enhancements 🔮
[To be added based on project roadmap]

## Setup and Usage 🔧
[To be added: Instructions for setting up and running the project]

