
 select count(1) from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1

 	exec dataArchival.usp_restoreDataToDW @sourceTableName='dataArchival.TestDWHWIP_DATA_DAILY',@ArchSourceTSField ='CurrentDate',
 @StartDate ='2018-08-08 00:00:00',
 @EndDate ='2018-08-08 23:59:59',@ExportLocation='DWH.WIP_DATA_DAILY/20180808a'


 	exec dataArchival.usp_restoreDataToDW @sourceTableName='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1',@ArchSourceTSField ='intervalDateLocal',
 @StartDate ='2018-05-01',
 @EndDate ='2018-05-31',@ExportLocation='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1/20180501'



