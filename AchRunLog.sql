/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [TableId]
      ,[ArchInterval]
      ,[ActivityType]
      ,[RunStartDatetime]
      ,[RunEndDatetime]
      ,[RunStatus]
      ,[RunMessage]
  FROM [dataArchival].[ArchivalRunLog]order by RunStartDatetime desc

  truncate table [dataArchival].[ArchivalRunLog]

