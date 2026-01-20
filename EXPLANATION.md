  A configuration-driven ETL tool that extracts data from CSV files, transforms it, aggregates it, and loads it into database tables - all defined in YAML files (no Python code changes needed for new systems).


  How

  CSV File → Read & Transform → Aggregate → Load to DB
     ↓              ↓                ↓            ↓
  source_file_a  trim/cast    group by     Product/Customer/
                      upper_case   first/last   Address/Phone tables



Data Flow with Code References

  1. ENTRY POINT: execute_copy_data_task()
     etl/workflows.py:180-355
     ├─ Loads YAML configs via ConfigLoader
     └─ Returns when data is loaded

  2. LOAD CONFIGS: get_config_loader()
     etl/config_loader.py:27-166
     ├─ load_source_file()      → configs/sources/{system}/{source}.yaml
     ├─ load_aggregation()      → configs/aggregations/{system}/{agg}.yaml
     └─ load_copydata_task()    → configs/copydata/{system}/{task}.yaml

  3. READ SOURCE FILE (line 246)
     df = pd.read_csv(source_file_path)
     └─ Gets path from source_file_a.yaml

  4. APPLY TRANSFORMATIONS (line 249)
     etl/transformations.py:112-146
     transformer.apply_transformations(df, source_columns)
     └─ trim, cast, upper_case, etc.

  5. APPLY ROW FILTERS (line 252)
     etl/transformations.py:149-162
     apply_row_filters(df, row_filters)
     └─ not_null, equals, etc.

  6. APPLY AGGREGATIONS (line 256) [if source_type='aggregated_dataset']
     etl/transformations.py:165-185
     apply_aggregations(df, group_by, aggregated_columns)
     └─ group_by + first/last/min/max/count/sum/avg

  7. MAP COLUMNS (line 262-269)
     Uses column_mappings from copydata YAML
     └─ source → destination column mapping

  8. LOAD TO DATABASE (line 292-349)
     conn, cur = client_db(database)  # database.py:4-7
     └─ INSERT OR REPLACE / INSERT OR IGNORE
