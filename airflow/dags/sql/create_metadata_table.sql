CREATE TABLE IF NOT EXISTS metadata.table_metadata (
    id SERIAL PRIMARY KEY,
    table_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    source_data_last_updated TIMESTAMP WITH TIME ZONE,
    source_metadata_last_updated TIMESTAMP WITH TIME ZONE,
    updated_data_available BOOLEAN DEFAULT NULL,
    updated_metadata_available BOOLEAN DEFAULT NULL,
    data_pulled_this_check BOOLEAN DEFAULT NULL,
    time_of_check TIMESTAMP WITH TIME ZONE,
    metadata_json JSONB
);