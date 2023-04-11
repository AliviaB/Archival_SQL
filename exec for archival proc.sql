

------------------exec for archival proc----------------

exec dataArchival.usp_loadDataToBlob @sourceTableName='DWH.WIP_DATA_DAILY',@ArchSourceTSField ='CurrentDate',
 @StartDate ='2018-08-08 00:00:00',
 @EndDate ='2018-08-08 23:59:59',@ExportLocation='DWH.WIP_DATA_DAILY/20180808a'

 --- add one more exec with a interval table
 
 
 
 
 
 
 
 
 
 
 
 
 
 select GETDATE()
drop external table   [dbo].[Example_Exported]
 select * from [dbo].[Example_Exported]

 select * from DWH.WIP_DATA_DAILY where currentdate between '2018-08-07' and '2018-08-07'

OPEN MASTER KEY DECRYPTION BY PASSWORD = 'password'

drop DATABASE SCOPED CREDENTIAL TestStorageCredential
drop MASTER KEY ENCRYPTION BY PASSWORD = 'CoatsPassword1'

select top 100 * from [DWH].[WIP_DATA_DAILY] where CurrentDate='2018-08-09'

SELECT * FROM sys.external_data_sources;

select * from sys.database_scoped_credentials where name='TestStorageCredential'
declare @sql nvarchar(max),@sourceTableName varchar(500)='DWH.WIP_DATA_DAILY',@sourceRowCount int
SELECT @sql=N'SELECT @sourceRowCountOut=COUNT(1) 
						 FROM '+ @sourceTableName
						--	+ 
						--' WHERE ' +@ArchSourceTSField +' BETWEEN '''+ convert(varchar(10), @StartDate, 120)+''' AND '''+convert(varchar(10), @EndDate, 120)+''''
	EXECUTE sp_executesql @sql, N'@sourceRowCountOut int OUTPUT',  @sourceRowCountOut=@sourceRowCount OUTPUT; 

	select @sourceRowCount

	select @sql='SELECT @sourceRowCountOut=COUNT(1) 
						 FROM '+ @sourceTableName