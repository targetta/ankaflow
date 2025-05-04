# Motherduck Integration Guide

This guide explains how to integrate and manipulate Motherduck data in your Duckflow pipelines. Since Motherduck requires native network access, all pipeline runs using Motherduck must execute on a server or environment with internet connectivity.

---

## 1. Prerequisites

- **Duckflow** installed and configured.
- Access to a Motherduck account and dataset.
- Valid **Motherduck API token** set as an environment variable.

```bash
export motherduck_token=your_real_token_here
```

---

## 2. Pipeline YAML Example

Below is a minimal pipeline demonstrating how to attach a Motherduck endpoint, query system tables, and retrieve sample data.

```yaml
- name: First step
  kind: self
  query: select 42 as meaning

- name: Attach Motherduck
  kind: self
  query: >
    attach 'md:';
    select table_catalog, table_schema, table_name
      from INFORMATION_SCHEMA.tables;
  show: 5

- name: Show sample data
  kind: self
  query: >
    select *
      from sample_data.nyc.taxi
      limit 10;
  show: 5
```

### Explanation

1. **API Token:** Duckflow reads `motherduck_token` from the environment.  
2. **`attach 'md:'`:** Establishes the connection to Motherduck using the DSN prefix `md:`.  
3. **System Catalog Query:** Lists available tables in `INFORMATION_SCHEMA.tables`.  
4. **Data Query:** Pulls the first 10 rows from `sample_data.nyc.taxi`.

---

## 3. Running the Pipeline

Execute your pipeline on a server with network access:

```bash
duct run pipeline.yaml
```

You should see output sections like:

```
---- Attach Motherduck
  table_catalog | table_schema | table_name
  … (5 rows shown) …

---- Show sample data
  VendorID | tpep_pickup_datetime | … 
  … (10 rows shown) …
```

---

## 4. Writing Data to Motherduck

To create or overwrite tables in Motherduck, use SQL DDL statements:

```yaml
- name: Create reporting table
  kind: self
  query: >
    attach 'md:';
    create or replace table analytics.daily_counts as
      select date_trunc('day', tpep_pickup_datetime) as day,
             count(*) as rides
        from sample_data.nyc.taxi
       group by day;
```

> **Tip:** Confirm write permissions and dataset names before running DDL.

---

## 5. Advanced Data Manipulations

### 5.1. Inserting Additional Rows

```yaml
- name: Append special events
  kind: self
  query: >
    attach 'md:';
    insert into analytics.daily_counts(day, rides)
      values('2023-12-31', 1234);
```

### 5.2. Updating Records

```yaml
- name: Correct ride count
  kind: self
  query: >
    attach 'md:';
    update analytics.daily_counts
       set rides = rides + 10
     where day = '2023-12-31';
```

### 5.3. Cleaning Up

```yaml
- name: Drop temp table
  kind: self
  query: >
    attach 'md:';
    drop table if exists analytics.daily_counts_temp;
```

---

## 6. Best Practices

- **Chunk large queries** to avoid timeouts.  
- **Use `show: N`** to preview results without overwhelming the logs.  
- **Modularize** repeated `attach 'md:'` calls by creating a top-level stage for connection.  
- **Secure your token**: do not commit `motherduck_token` to source control.

---

## 7. Troubleshooting

- **Authentication errors** → Verify `motherduck_token` is correct and exported.  
- **Network failures** → Ensure outbound connectivity to Motherduck endpoints.  
- **Permission denied** → Check your account’s dataset ACLs.

---

Happy querying with Motherduck in Duckflow!
