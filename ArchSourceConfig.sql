/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [SourceId]
      ,[SourceUniqueName]
      ,[ServerUrl]
      ,[DataBaseName]
      ,[DBType]
  FROM [dataArchival].[ArchivalSourceConfig]