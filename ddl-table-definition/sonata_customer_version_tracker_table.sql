CREATE TABLE partner_customer_loan_db.sonata_customer_version_tracker_table_v6 (
    customer_id bigint ,
	loan_account_no bigint,
	
    address1 text,
	address2 text,
	address_version bigint,
	max_address_version bigint,
	
	branch_id text,
	branch_id_version bigint,
	max_branch_id_version bigint,
	
    center_id text,
	center_id_version bigint,
	max_center_id_version bigint,
	
	hub_id text,
	hub_id_version bigint,
	max_hub_id_version bigint,
	
	loan_sanction_id text,
	loan_version bigint,
	max_loan_version bigint,
	
	phone_number text,
	phone_version bigint,
	max_phone_version bigint,
	
	pincode text,
	pincode_version bigint,
	max_pincode_version bigint,
	
	dob date,
	dob_version bigint,
	max_dob_version bigint,
	
	sfo_id text,
	sfo_id_version bigint,
	max_sfo_id_version bigint,
	
	nominee_relation text,
	nominee_relation_version bigint,
	max_nominee_relation_version bigint,
	
	annual_income double,
	annual_income_version bigint,
	max_annual_income_version bigint,
	
	marital_status text,
	marital_status_version bigint,
	max_marital_status_version bigint,
	
	
	
	PRIMARY KEY (customer_id, loan_account_no)
	
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 100
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';