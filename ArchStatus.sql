/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [TableId]
      ,[ArchInterval]
      ,[ArchStatus]
      ,[PurgeStatus]
      ,[RestoreStatus]
	  ,RestoreRequestInd
  FROM [dataArchival].[ArchivalStatus] order by TableId,ArchInterval

  truncate table [dataArchival].[ArchivalStatus]
  declare @tblID int =1 ,@archivalIntrval varchar(20)='2017-02-01',@archStatus varchar(1)='0'
 

   MERGE dataArchival.ArchivalStatus
    AS target  
    USING (SELECT @tblID,@archivalIntrval,@archStatus) AS source (TableID, ArchInterval,ArchStatus)  
    ON (target.TableId = source.TableId and target.ArchInterval=source.ArchInterval)  
    WHEN MATCHED THEN   
        UPDATE SET ArchStatus = source.ArchStatus  
WHEN NOT MATCHED THEN  
    INSERT (TableID, ArchInterval,ArchStatus)  
    VALUES (source.TableId, source.ArchInterval,source.ArchStatus);  

	update dataArchival.ArchivalStatus set RestoreRequestInd='0',RestoreStatus='1' where TableId=6 and ArchInterval='2018-07-01'
   