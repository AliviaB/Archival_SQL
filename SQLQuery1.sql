CREATE MASTER KEY 
IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name='ArchStorageCredential')
	BEGIN
		   DROP DATABASE SCOPED CREDENTIAL ArchStorageCredential 
		
	END

	 CREATE DATABASE SCOPED CREDENTIAL ArchStorageCredential
						WITH
						IDENTITY = 'SHARED ACCESS SIGNATURE',
						SECRET = 'H43/44bEdmowePRVhHiHAAa3Vp1W77t4kMLepnqDauwLLp6cdkvhrweOtptfbylL/npQpi4ZZc2DwMtr9CtA8Q==';

IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name='ArchStorageSource')
	BEGIN
		  DROP EXTERNAL DATA SOURCE ArchStorageSource
			
	END

	CREATE DATABASE SCOPED CREDENTIAL MyCredentials WITH IDENTITY = 'SHARED ACCESS SIGNATURE',SECRET = 'QLYMgmSXMklt%2FI1U6DcVrQixnlU5Sgbtk1qDRakUBGs%3D';

CREATE EXTERNAL DATA SOURCE ArchStorageSource
		WITH 
		(
			TYPE = BLOB_STORAGE,
			LOCATION = 'https://archivalblobstore.blob.core.windows.net/sqldb-archival',
			CREDENTIAL = ArchStorageCredential
		);

		select count(1) from dataArchival.SAPBW_BW_TD_OTIF

		BULK INSERT dataArchival.SAPBW_BW_TD_OTIF
FROM 'dataArchival.SAPBW_BW_TD_OTIF/20180201/file.csv'
WITH (DATA_SOURCE = 'ArchStorageSource',Format='CSV');

select top 10 * from dataArchival.SAPBW_BW_TD_OTIF
dataArchival.SAPBW_BW_TD_OTIF/20180201/file.csv