/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [TableId]
      ,[SourceId]
      ,[SourceTableName]
      ,[SourceSchemaName]
      ,[ArchFrequency]
      ,[ArchRetentionPeriod]
      ,[PurgeRetentionPeriod]
      ,[ArchSourceTSField]
      ,[PartitionedInd]
      ,[ActiveInd]
      ,[ArchIntervalLast]
      ,[PurgeIntervalLast]
  FROM [dataArchival].[ArchivalMaster]

  update dataArchival.ArchivalMaster set ArchIntervalLast='2017-01-01' where Tableid=2
   update dataArchival.ArchivalMaster set ArchIntervalLast='2017-10-01' where Tableid=3
   update dataArchival.ArchivalMaster set ArchIntervalLast='2017-01-01' where Tableid=6

    update dataArchival.ArchivalMaster set PurgeRetentionPeriod=6 where Tableid=6

   select top 1 * from dataArchival.SalesSource

 select top 1 * from dataArchival.

 update dataArchival.ArchivalMaster set PurgeIntervalLast='2017-01-01' where Tableid=2
   update dataArchival.ArchivalMaster set PurgeIntervalLast='2019-01-01' where Tableid=3
   update dataArchival.ArchivalMaster set PurgeIntervalLast='2017-01-01' where Tableid=6

   exec dataArchival.usp_getDatafrArchival @SourceTableName='dataArchival.SalesSource',
@ArchSourceTSField='SalesDate',
@StartDate='2014-01-01',
@EndDate='2014-01-31'
file:dataArchival.SalesSource/20140101

select top 1 calday from SAPBW.BW_TD_BILLITEM with (nolock) where calday = '2018-09-30'
  drop index idx1 on sapbw.bw_td_billitem

update statistics SAPBW.BW_TD_BILLITEM  idx1

SELECT * FROM sys.dm_db_index_physical_stats  
    (DB_ID(N'CoatsDB'), OBJECT_ID(N'SAPBW.BW_TD_BILLITEM'), NULL, NULL , 'DETAILED'); 
	
	 DBCC SQLPERF(LOGSPACE) 
	 SELECT name AS 'Database Name', log_backup_time AS 'last log backup time' --2019-05-22 08:07:22.330
FROM sys.databases AS s CROSS APPLY sys.dm_db_log_stats(s.database_id)
WHERE name = 'CoatsDB'
	 SELECT name, [log_reuse_wait_desc] FROM sys.databases WHERE name = 'CoatsDB'


