# Databricks notebook source
# Accessing FileStore
data_path = "/FileStore/ethereum_data/block_tracker.json"

# Read the JSON file into a DataFrame
df = spark.read.json(f"dbfs:{data_path}")
# display(df)

# COMMAND ----------

# Widgets for parameters
# dbutils.widgets.text("infura_api_key", "", "Infura API Key")
dbutils.widgets.text("blocks_per_fetch", "500", "Number of blocks to fetch")
dbutils.widgets.text("data_path", "/FileStore/ethereum_data", "Data Path")

# COMMAND ----------

# read infura_api_key stored in azure secrets from azure key vault
INFURA_API_KEY = dbutils.secrets.get(scope = "luban-scope", key = "infura-api-key")

# COMMAND ----------

from web3 import Web3
from datetime import datetime
import time
from typing import List, Dict
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType, DoubleType, DecimalType
from pyspark.sql.functions import from_unixtime, col

# COMMAND ----------

# Get parameters
# INFURA_API_KEY = dbutils.widgets.get("infura_api_key")
BLOCKS_PER_FETCH = int(dbutils.widgets.get("blocks_per_fetch"))
DATA_PATH = dbutils.widgets.get("data_path")

# COMMAND ----------

# DBTITLE 1, Ethereum Data Pipeline Class
from decimal import Decimal

# Schema Definitions to ensure schema consistency

block_schema = StructType([
    StructField("block_number", IntegerType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("transactions_count", IntegerType(), False),
    StructField("gas_used", LongType(), False),
    StructField("gas_limit", LongType(), False),
    StructField("base_fee_per_gas", LongType(), True),
    StructField("difficulty", LongType(), False)
])

transaction_schema = StructType([
    StructField("block_number", IntegerType(), False),
    StructField("hash", StringType(), False),
    StructField("from_address", StringType(), False),
    StructField("to_address", StringType(), True),
    StructField("value_wei", DecimalType(38, 0), False),  # Changed to DecimalType
    StructField("value_eth", DoubleType(), False),
    StructField("gas_price", LongType(), False),
    StructField("gas", LongType(), False)
])


class EthereumDatabricksPipeline:
    def __init__(self, infura_api_key: str, data_path: str):
        self.w3 = Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{infura_api_key}"))
        self.data_path = data_path
        self.tracker_path = f"{data_path}/block_tracker.json"
        
        # Verify connection
        if not self.w3.is_connected():
            raise Exception("Failed to connect to Ethereum network")
        
        # Initialize block tracker
        self.processed_blocks = self.load_processed_blocks()
        
    def load_processed_blocks(self) -> Dict:
        """Load the record of processed blocks from tracker file."""
        try:
            tracker_content = dbutils.fs.head(self.tracker_path)
            return json.loads(tracker_content)
        except:
            return {
                'last_processed_block': 0,
                'processed_blocks': []
            }

    def save_processed_blocks(self):
        """Save the record of processed blocks to tracker file."""
        dbutils.fs.put(self.tracker_path, json.dumps(self.processed_blocks), True)

    def get_block_data(self, block_number: int) -> tuple:
        """Fetch block data for a given block number."""
        block = self.w3.eth.get_block(block_number, full_transactions=True)
        
        block_data = {
            'block_number': block_number,
            'timestamp': datetime.fromtimestamp(block.timestamp),
            'transactions_count': len(block.transactions),
            'gas_used': block.gasUsed,
            'gas_limit': block.gasLimit,
            'base_fee_per_gas': getattr(block, 'baseFeePerGas', 0),
            'difficulty': block.difficulty,
        }
        
        return block_data, block.transactions

    def process_transactions(self, block_number: int, transactions: List) -> List[Dict]:
        """Process transaction data from a block."""
        processed_txs = []
        
        for tx in transactions:
            if isinstance(tx, (bytes, str)):
                continue
                
            tx_dict = {
                'block_number': block_number,
                'hash': tx.hash.hex(),
                'from_address': tx['from'],
                'to_address': tx.to if tx.to else None,
                'value_wei': Decimal(tx.value),  # Convert to Decimal
                'value_eth': float(self.w3.from_wei(tx.value, 'ether')),
                'gas_price': tx.gasPrice,  # Keep as integer
                'gas': tx.gas,
            }
            processed_txs.append(tx_dict)
            
        return processed_txs

    def fetch_blocks_range(self, start_block: int, end_block: int) -> tuple:
        """Fetch data for a range of blocks, skipping already processed blocks."""
        blocks_data = []
        all_transactions = []
        
        print(f"Fetching blocks {start_block} to {end_block}...")
        
        for block_num in range(start_block, end_block + 1):
            if str(block_num) in self.processed_blocks['processed_blocks']:
                print(f"Skipping already processed block {block_num}")
                continue
                
            try:
                block_data, transactions = self.get_block_data(block_num)
                processed_txs = self.process_transactions(block_num, transactions)
                
                blocks_data.append(block_data)
                all_transactions.extend(processed_txs)
                
                self.processed_blocks['processed_blocks'].append(str(block_num))
                self.processed_blocks['last_processed_block'] = max(
                    block_num,
                    self.processed_blocks['last_processed_block']
                )
                
                if block_num % 100 == 0:
                    print(f"Processed block {block_num}")
                    self.save_processed_blocks()
                    
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error processing block {block_num}: {str(e)}")
                continue
        
        self.save_processed_blocks()
        return blocks_data, all_transactions

# COMMAND ----------

# DBTITLE 1, Data Processing and Storage Functions
def save_to_delta(blocks_data: List[Dict], transactions_data: List[Dict], timestamp: str):
    """Save the fetched data to Delta tables."""
    if not blocks_data:
        print("No new data to save")
        return
    
    # Convert to Spark DataFrames
    blocks_df = spark.createDataFrame(blocks_data, schema=block_schema)
    transactions_df = spark.createDataFrame(transactions_data, schema=transaction_schema)
    
    # Save as Delta tables with timestamp partition
    blocks_df.write.format("delta").mode("append").partitionBy("block_number").saveAsTable("ethereum.blocks")
    transactions_df.write.format("delta").mode("append").partitionBy("block_number").saveAsTable("ethereum.transactions")
    
    # Create views for analysis
    spark.sql("REFRESH TABLE ethereum.blocks")
    spark.sql("REFRESH TABLE ethereum.transactions")

# COMMAND ----------

# DBTITLE 1, Main Execution
# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS ethereum")

# Initialize pipeline
pipeline = EthereumDatabricksPipeline(INFURA_API_KEY, DATA_PATH)

# Get current block number
latest_block = pipeline.w3.eth.block_number

# Calculate start block
start_block = max(
    pipeline.processed_blocks['last_processed_block'] + 1,
    latest_block - BLOCKS_PER_FETCH
)

if start_block <= latest_block:
    # Fetch data
    blocks_data, transactions_data = pipeline.fetch_blocks_range(start_block, latest_block)
    
    # Save to Delta tables
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_to_delta(blocks_data, transactions_data, timestamp)
    
    # Run analysis

    print(f"\nSummary:")
    print(f"Processed {len(blocks_data)} new blocks")
    print(f"Collected {len(transactions_data)} new transactions")
else:
    print("No new blocks to process")