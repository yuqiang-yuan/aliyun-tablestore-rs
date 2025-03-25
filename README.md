# Aliyun Tablestore (OTS) Rust SDK

This is a Rust SDK for the Aliyun Tablestore (OTS) service. Aliyun tablestore is a fully managed NoSQL database service that provides high-performance, scalable, and cost-effective storage for large amounts of structured data. It supports various data models such as key-value, wide-column, and document, and offers features like strong consistency, global distribution, and automatic sharding.

There operations are implemented:

- Wide-column table
  - Table operations
    - Create table
    - Describe table
    - List tables
    - Update table
    - Delete table
    - Compute table split points by size
  - Defined column operations
    - Add column
    - Delete column
  - Row operations
    - Get row
    - Get range
    - Put row
    - Update row
    - Delete row
    - Batch get row
    - Batch write row
