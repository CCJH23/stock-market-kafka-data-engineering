# Stock Market Kafka Data Engineering Project

Kafka project by simulating a stock market API, storing data in Amazon S3, crawling data using Amazon Glue, creating a data catalogue and querying data using Amazon Athena

## Architecture Diagram

![Architecture Diagram](https://github.com/CCJH23/stock-market-kafka-data-engineering/blob/63e183b4ba0bc38f83a5dfa00b9e4a8658bf8760/img/ArchitectureDiagram.png)

## Technologies Used

1. Apache Kafka
2. Amazon S3
3. Amazon Glue
4. Amazon Athena

## Data from CSV File

![Data from CSV File](https://github.com/CCJH23/stock-market-kafka-data-engineering/blob/77a21985ea8787f90ba5b083f82e3582563ad727/img/DataBefore.png)

## Data in Amazon Athena After Crawling by Amazon Glue

![Data in Amazon Athena After Crawling by Amazon Glue](https://github.com/CCJH23/stock-market-kafka-data-engineering/blob/77a21985ea8787f90ba5b083f82e3582563ad727/img/DataInAthena.png)

## Process

1. Install Apache Kafka in Amazon EC2
   1. Install Java because Apache Kafka and Apache Zookeeper requires a Java environment to work
   2. Install Kafka
   3. Configure Kafka to run on the public IP address of EC2 instead of the private one currently so that I can access Kafka from my local Jupyter notebook
   4. Start Zookeeper
   5. Start Kafka
   6. Create a topic in Kafka
   7. Start Kafka producer
   8. Start Kafka consumer
2. Simulate a stock market API (producer)
   1. Read sampleStockMarketData csv file using Pandas in Jupyter notebook as a dataframe
   2. Initialise Kafka producer
   3. Create a loop that randomly picks a row from the dataframe and sends it using the producer
3. Create a Kafka consumer that receives this data and stores the data into Amazon S3
4. Use Amazon Glue crawler to crawl through the data in the S3 bucket to understand the schema of the data and create a data catalog
5. Query the data in Amazon Athena using SQL
