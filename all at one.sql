Azure Data Warehouse.
Partitioned table:dataArchival.TSrcTblPartFactIOTEventDowntimeWinding

--------- purge CTAS
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

exec dataArchival.usp_purgeDataCTAS 
@InputParam		='SourceTableName=Arch.TestDWHWIP_DATA_DAILY#|#ArchSourceTSField=CurrentDate#|#StartDate=2018-10-06#|#EndDate=2018-10-10#|#',
@execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as '@execStatus',
		@statusMessage as '@statusMessage'
---- archival
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

EXEC	 [dataArchival].[usp_loadDataToBlob]
		@inputParam				  = N'SourceTableName=dataArchival.Testt_Credit_Control#|#ArchSourceTSField=CALDAY#|#StartDate=2014-10-01#|#EndDate=2014-10-31#|#ExportLocation=dataArchival.Testt_Credit_Control/20141001#|#',
		@execStatus = @execStatus OUTPUT,
		@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'

 --- Purge normal
 
 DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)
	 exec dataArchival.usp_purgeData 
 @InputParam ='SourceTableName=dataArchival.Testt_Credit_Control#|#ArchSourceTSField=CALDAY#|#StartDate=2014-10-01#|#EndDate=2014-10-01#|#ArchFrequency=DAY#|#',
 @execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

		SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'
 ---- purge partition
  DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)
  
  exec dataArchival.usp_purgeDataPartitionedTable 
	@InputParam	= 'SourceTableName=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1#|#ArchSourceTSField=intervalDateLocal#|#ArchPeriod=2018-03-01#|#',
	@execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

	SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'

----- Restore

DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)


exec dataArchival.usp_restoreDataToDW 
 @InputParam='SourceTableName=Arch.TSrcTblPartFactIOTEventDowntimeWinding#|#ExportLocation=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding/20180401#|#',
 @execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as '@execStatus',
		@statusMessage as '@statusMessage'
		
		
		
		
		
		
		
		
		
		
		
		

DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

exec dataArchival.usp_purgeDataCTAS 
@SourceTableName	='Arch.TestDWHWIP_DATA_DAILY',
@ArchSourceTSField = 'CurrentDate',

@StartDate			= '2018-08-08',
@EndDate			= '2018-08-08',
@execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as '@execStatus',
		@statusMessage as '@statusMessage'

------------------------------------------------------
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

exec dataArchival.usp_purgeDataCTAS 
@SourceTableName	='dataArchival.Testt_Credit_Control',
@ArchSourceTSField = 'CALDAY',

@StartDate			= '2014-12-01',
@EndDate			= '2014-12-31',
@execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as '@execStatus',
		@statusMessage as '@statusMessage'


Output Parameter: ExecStatus(varchar)(Success/failure),StatusMessage(varchar(255))
---- archival
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

EXEC	 [dataArchival].[usp_loadDataToBlob]
		@inputParam				  = N'dataArchival.Testt_Credit_Control#|#CALDAY#|#2014-11-01#|#2014-11-30#|#dataArchival.Testt_Credit_Control/20141101#|#',
		@execStatus = @execStatus OUTPUT,
		@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'


 
 exec dataArchival.usp_loadDataToBlob 
 @sourceTableName='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding',
 @ArchSourceTSField ='intervalDateLocal',
 @StartDate ='2018-04-01',
 @EndDate ='2018-04-30',@ExportLocation='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding/20180401'--21779
 
 
 --- Purge normal
 
 DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)
	 exec dataArchival.usp_purgeData 
 @InputParam ='dataArchival.Testt_Credit_Control#|#CALDAY#|#2014-11-01#|#2014-11-01#|#DAY#|#',
 @execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

		SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'
 












		
 
 
 SELECT sourceRowCountOut=COUNT(1) FROM dataArchival.Testt_Credit_Control WHERE CALDAY between '2014-09-01' AND  '2014-09-30' --18495
 
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)
	 exec dataArchival.usp_purgeData 
 @sourceTableName ='dataArchival.Testt_Credit_Control' ,
 @ArchSourceTSField ='CALDAY',
 @StartDate='2014-09-01' ,
 @EndDate='2014-09-30',
 @ArchFrequency= 'MONTH',
 @execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

		SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'

 exec dataArchival.usp_purgeData 
 @sourceTableName ='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding' ,
 @ArchSourceTSField ='intervalDateLocal', 
 @StartDate='2018-04-01' ,
 @EndDate='2018-04-30',
 @ArchFrequency= 'MONTH'

 select count(1)  from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding where  intervalDateLocal >= '2018-04-01' and intervalDateLocal< ='2018-04-30'

exec dataArchival.usp_purgeData 
 @sourceTableName ='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1' ,
 @ArchSourceTSField ='intervalDateLocal', 
 @StartDate='2018-05-05 00:00:00' ,
 @EndDate='2018-05-05 23:59:59',
 @ArchFrequency= 'DAY'
 
 
 ---- purge partition
 select count(1) from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1 where  intervalDateLocal >= '2018-05-01' and intervalDateLocal< ='2018-05-31'--20405
  
  exec dataArchival.usp_purgeDataPartitionedTable 
	@SourceTableName	= 'dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1',
	@ArchSourceTSField	= 'intervalDateLocal',
	@ArchFrequency 		= 'MM',
	@ArchPeriod			= '2018-05-01'
 
 select count(1) from dataArchival.FactIOTEventPerfWinding where  intervalDateLocal >= '2018-05-01' and intervalDateLocal< ='2018-05-31'--7266
 
 exec dataArchival.usp_purgeDataPartitionedTable 
	@SourceTableName	= 'dataArchival.FactIOTEventPerfWinding',
	@ArchSourceTSField	= 'intervalDateLocal',
	@ArchFrequency 		='MM',
	@ArchPeriod			= '2018-05-01'









exec dataArchival.usp_restoreDataToDW 
 @sourceTableName='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding',
 @ArchSourceTSField ='intervalDateLocal',
 @StartDate ='2018-04-01',
 @EndDate ='2018-04-30',@ExportLocation='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding/20180401'
 
 
 
 	exec dataArchival.usp_restoreDataToDW 
	@sourceTableName='dataArchival.Testt_Credit_Control',
	@ArchSourceTSField ='CALDAY',
    @StartDate ='2014-09-01',
    @EndDate ='2014-09-30',
	@ExportLocation='dataArchival.Testt_Credit_Control/20140901'