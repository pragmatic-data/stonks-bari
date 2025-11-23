-- 1. Creation of the schema for the Landing Tables
CREATE SCHEMA IF NOT EXISTS STONKS_BARI_DEV.LAND_IB
COMMENT = 'Landing table schema for CSV files from Interactive Brokers.';

-- 2. Creation of the File Format to read the files for the Landing Tables
CREATE FILE FORMAT IF NOT EXISTS STONKS_BARI_DEV.LAND_IB.IB_CSV__FF
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    COMPRESSION = 'AUTO'
    ERROR_ON_COLUMN_COUNT_MISMATCH = True
    EMPTY_FIELD_AS_NULL = True
    NULL_IF = ('', '\\N')
;


-- 3. Creation of the Stage holding the files for the Landing Tables
CREATE STAGE IF NOT EXISTS STONKS_BARI_DEV.LAND_IB.IB_CSV__STAGE
        FILE_FORMAT = STONKS_BARI_DEV.LAND_IB.IB_CSV__FF
        DIRECTORY = ( ENABLE = true )
        COMMENT = 'Stage for CSV files from Interactive Brokers.'
;
