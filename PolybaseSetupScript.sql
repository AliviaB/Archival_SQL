/******* One Time script for setup Per SQL Data Warehouse *************
Desc : Create EXTERNAL DATA SOURCE,DATABASE SCOPED CREDENTIAL,EXTERNAL FILE FORMAT
Author :Alivia Banerjee
Create Date:20190313
Last Modified :20190313

*****************************************/


	IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name='ArchStorageSource')
	BEGIN
		  DROP EXTERNAL DATA SOURCE ArchStorageSource
			
	END
	IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name='ArchStorageCredential')
	BEGIN
		   DROP DATABASE SCOPED CREDENTIAL ArchStorageCredential 
		
	END
	IF EXISTS ( SELECT * FROM sys.external_file_formats WHERE name='ArchivalFileFormat')
	BEGIN
		   DROP EXTERNAL FILE FORMAT ArchivalFileFormat
		
	END
	------------------------------------------------------------------------------------

	-------- A:Create DB scoped credential for datalake storage account access----------

		 CREATE DATABASE SCOPED CREDENTIAL ArchStorageCredential
						WITH
						IDENTITY = 'user',
						SECRET = 's2H1bV2EGmgqJFF7dncSwr36GW9nEhIy7tJRqPCGvkTPqmg+B9l0Y9lkEH0hev3BNnlCPo4bQNs4CajMHuhT2w==';
		            


		
	---------end of A--------------------------------------------------------------------------------


	-------------- B:Create external data source for azure datalake store v2 container level----------

		CREATE EXTERNAL DATA SOURCE ArchStorageSource
		WITH 
		(
			TYPE = HADOOP,
			LOCATION = 'abfss://dataarchival@coats0archival0adls0prod.dfs.core.windows.net/',
			CREDENTIAL = ArchStorageCredential
		);

		
	-----------------End of  B-------------------------------------------------------------------------


	--------------C: Create External file format-------------------------------------------------------

		CREATE EXTERNAL FILE FORMAT ArchivalFileFormat
						WITH
						(
						    FORMAT_TYPE = DelimitedText,
						    FORMAT_OPTIONS 
						    (
						        FIELD_TERMINATOR = '⯑'
						        ,STRING_DELIMITER = '0x7E0x7E'
						        ,USE_TYPE_DEFAULT = False
						    )
						   
						);
			
	-----------------End of C----------------------------------------------------------------------------------