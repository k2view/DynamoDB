
# DynamoDB Connector
          
## Introduction

This DynamoDB extension utilizes AWS Java SDK and DynamoDB's PartiQL query language to perform CRUD operations on a DynamoDB table.  

It provides two main classes:
1. DynamoDBIoProvider - The integration point with Fabric.
2. DynamoDBIoSession - The core functionality which is developed on top of AWS SDK.


## Authentication: 
Any of the methods mentioned in [Default Credential Provider Chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default) is natively supported. 
As for what is recommended, **Instance profile credentials** is preferred when running on EC2 instances, and **Amazon ECS container credentials** in the case of container. When running locally, **The default credential profiles file** is the easiest way.


## How to Use
1. Create a Fabric interface of type DynamoDB:
   - Specify the AWS region (If you don’t explicitly set it, the AWS SDK consults the [Default Region Provider Chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment) to try and determine the region to use).

2. Use the Broadway actor DbCommand; set the "interface" input as the DynamoDB interface that you've created.
    - See the actor description for more info on how to use it.
    - The sql command syntax should match the PartiQL query language's syntax.
      - Note that in PartiQL, unlike common SQL languages, there is no LIMIT clause. As a workaround, we added some logic to support "LIMIT {number}" **ONLY** at the end of a statement - so if needed, use it with caution.

## Batches and Transactions
If batch input in DbCommand is set to:
1. True:
   - In Transaction:
       - The statements will get executed sequentially, in batches of size 25, which is the maximum batch size that DynamoDB allows (as of now). If you wish, for some reason, to decrease the batch size, you can specify the BATCH_SIZE in the "Data" section of the interface (e.g. { BATCH_SIZE: 10}).
       - **Recommended; But note that it won't be a real transaction**, as rollbacks in case of a failure aren't supported
   - Outside of a transaction:
       - Not allowed (because the last batch has to be executed on the transaction commit).
2. False:
   - In Transaction:
       - Statements will get executed in a single batch at commit; Either all statements are successful, or none. 
       - **Max allowed number of statements in this case is 100** (as of now), which is a limitation of DynamoDB SDK ([More info](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.multiplestatements.transactions.html)). 
   - Outside of a transaction:
       - Statements will be executed sequentially, 1 by 1.
       - **Not recommended** due to the amount of API calls.


### Change Log
[Open change log file](/api/k2view/dynamodb-connector/0.0.1/file/CHANGELOG.md)

### License
[Open license file](/api/k2view/dynamodb-connector/0.0.1/file/LICENSE.txt)

