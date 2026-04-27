import dlt

dlt.create_streaming_table(name = 'dim_stores')

dlt.create_auto_cdc_flow(
target = "dim_stores",
  source = "stores_enr_view",
  keys = ["store_id"],
  sequence_by = "updated_date",
  ignore_null_updates = None,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None
  )