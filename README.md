# Aliyun Tablestore (OTS) Rust SDK

[![Crates.io Version](https://img.shields.io/crates/v/aliyun-tablestore-rs?_ts_=20250411)](https://crates.io/crates/aliyun-tablestore-rs)
![Crates.io MSRV](https://img.shields.io/crates/msrv/aliyun-tablestore-rs?_ts_=20250411)
[![docs.rs](https://img.shields.io/docsrs/aliyun-tablestore-rs)](https://docs.rs/aliyun-tablestore-rs)
[![Crates.io License](https://img.shields.io/crates/l/aliyun-tablestore-rs?_ts_=20250411)](https://github.com/yuqiang-yuan/aliyun-tablestore-rs?tab=License-1-ov-file)

[English](https://github.com/yuqiang-yuan/aliyun-tablestore-rs) | [中文](https://github.com/yuqiang-yuan/aliyun-tablestore-rs/blob/dev/README.zh-CN.md)

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
  - Data operations
    - Get row
    - Get range
    - Put row
    - Update row
    - Delete row
    - Batch get row
    - Batch write row
    - Bulk import
    - Bulk export
  - Index
    - Create index
    - Drop index
  - Search Index
    - Create search index
    - Delete search index
    - Describe search Index
    - List search Index
    - Search using index
    - Parallel scan
    - Compute splits
- Time series table
  - Table operations
    - Create table
    - Describe table
  - Data operations
    - Get data
    - Put data
    - Query meta
    - Update meta


TODO List:

- [ ] Test group by date histogram
- [ ] Test group by geo grid
- [ ] Test group by geo distance
- [ ] Test group by composite
- [ ] Test search with routing values.
