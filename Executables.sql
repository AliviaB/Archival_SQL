/*                                                   Azure sql data warehouse: datalakedwdv01          */
-- Partitioned table:dataArchival.TSrcTblPartFactIOTEventDowntimeWinding
select count(1) from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding where intervalDateLocal between '2018-05-01' and '2018-05-31'
select count(1) from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding where intervalDateLocal between '2018-04-01' and '2018-04-30'

dataArchival.BW_TD_FAIR july august 2018
-- Archive
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

EXEC	 [dataArchival].[usp_loadDataToBlob]
		@inputParam	= N'SourceTableName=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding#|#ArchSourceTSField=intervalDateLocal#|#StartDate=2018-04-01#|#EndDate=2018-04-30#|#ExportLocation=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding/20180401#|#',
		@execStatus = @execStatus OUTPUT,
		@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'
		
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

EXEC	 [dataArchival].[usp_loadDataToBlob]
		@inputParam	= N'SourceTableName=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding#|#ArchSourceTSField=intervalDateLocal#|#StartDate=2018-05-01#|#EndDate=2018-05-31#|#ExportLocation=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding/20180501#|#',
		@execStatus = @execStatus OUTPUT,
		@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'		
		
-- Purge partition
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)
  
  exec dataArchival.usp_purgeDataPartitionedTable 
	@InputParam	= 'SourceTableName=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding#|#ArchSourceTSField=intervalDateLocal#|#ArchPeriod=2018-05-01#|#',
	@execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

	SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'		


---- Restore
 DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)


exec dataArchival.usp_restoreDataToDW 
 @InputParam='SourceTableName=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding#|#ExportLocation=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding/20180401#|#',
 @execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as '@execStatus',
		@statusMessage as '@statusMessage'
		
 DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)


exec dataArchival.usp_restoreDataToDW 
 @InputParam='SourceTableName=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding#|#ExportLocation=dataArchival.TSrcTblPartFactIOTEventDowntimeWinding/20180501#|#',
 @execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as '@execStatus',
		@statusMessage as '@statusMessage'



-- Non Partitioned table(DWH.t_Credit_Control)

select count(1) from dataArchival.Testt_Credit_Control where CALDAY between '2014-11-01' and '2014-11-30'
--Archive

DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

EXEC	 [dataArchival].[usp_loadDataToBlob]
		@inputParam	= N'SourceTableName=dataArchival.Testt_Credit_Control#|#ArchSourceTSField=CALDAY#|#StartDate=2014-11-01#|#EndDate=2014-11-30#|#ExportLocation=dataArchival.Testt_Credit_Control/20141101#|#',
		@execStatus = @execStatus OUTPUT,
		@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'
		
-- PURGE
 
 DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)
	 exec dataArchival.usp_purgeData 
 @InputParam ='SourceTableName=dataArchival.Testt_Credit_Control#|#ArchSourceTSField=CALDAY#|#StartDate=2014-11-01#|#EndDate=2014-11-30#|#ArchFrequency=MONTH#|#',
 @execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

		SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'	
-- Restore
DECLARE	
@execStatus varchar(10),
@statusMessage varchar(128)

exec dataArchival.usp_restoreDataToDW 
 @InputParam='SourceTableName=dataArchival.Testt_Credit_Control#|#ExportLocation=dataArchival.Testt_Credit_Control/20141101#|#',
 @execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as '@execStatus',
		@statusMessage as '@statusMessage'	
--------- purge CTAS
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

exec dataArchival.usp_purgeDataCTAS 
@InputParam		='SourceTableName=dataArchival.Testt_Credit_Control#|#ArchSourceTSField=CALDAY#|#StartDate=2014-10-01#|#EndDate=2014-10-31#|#',
@execStatus = @execStatus OUTPUT,
@statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as '@execStatus',
		@statusMessage as '@statusMessage'	

/*                                                   Azure sql DB : CoatsDB          */	
-- archive PARTITION
select count(1) from dataArchival.SalesSource where SalesDate between '2014-01-01' and '2014-01-31'
Source:dataArchival.SalesSource
ArchTSField:SalesDate
Start:2014-01-01
end: 2014-01-31
file:dataArchival.SalesSource/20140101

---- Purge PARTITION
 DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

exec dataArchival.usp_purgeDataPartition 
 @InputParam ='SourceTableName=dataArchival.SalesSource#|#ArchSourceTSField=SalesDate#|#ArchPeriod=2014-01-01#|#',
 @execStatus = @execStatus OUTPUT,
 @statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'	

--- Restore		
 DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

exec dataArchival.usp_loadDataToDB 
 @InputParam ='SourceTableName=dataArchival.SalesSource#|#ExportLocation=dataArchival.SalesSource/20140101/file.txt#|#',
 @execStatus = @execStatus OUTPUT,
 @statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'



											/*  non partition **/

	select count(1) from dataArchival.SAPBW_BW_TD_OTIF where cal_day between '2018-03-01' and '2018-03-31'
-- archive
Source:dataArchival.SAPBW_BW_TD_OTIF
ArchTSField:Cal_Day
Start:2018-03-01
end: 2018-03-31
file:dataArchival.SAPBW_BW_TD_OTIF/20180301
-- purge batch
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

exec dataArchival.usp_purgeDataBatch 
 @InputParam ='SourceTableName=dataArchival.SAPBW_BW_TD_OTIF#|#ArchSourceTSField=Cal_Day#|#StartDate=2018-03-01#|#EndDate=2018-03-31#|#',
 @execStatus = @execStatus OUTPUT,
 @statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'	

--- Restore		
DECLARE	
		@execStatus varchar(10),
		@statusMessage varchar(128)

exec dataArchival.usp_loadDataToDB 
 @InputParam ='SourceTableName=dataArchival.SAPBW_BW_TD_OTIF#|#ExportLocation=dataArchival.SAPBW_BW_TD_OTIF/20180301/file.txt#|#',
 @execStatus = @execStatus OUTPUT,
 @statusMessage = @statusMessage OUTPUT

SELECT	@execStatus as N'@execStatus',
		@statusMessage as N'@statusMessage'		
		
---------------------------------------------------------------------------------------------------------
