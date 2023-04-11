exec dataArchival.usp_purgeData @sourceTableName ='dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1' ,
@ArchSourceTSField ='intervalDateLocal', @StartDate='2018-05-04 00:00:00' ,@EndDate='2018-05-04 23:59:59',@ArchFrequency= 'DAY'

delete from   dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1 WHERE intervalDateLocal 
in (select top 10000 intervalDateLocal from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1 where intervalDateLocal  BETWEEN '2018-03-01' AND '2018-04-01')

delete d from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1 d
(
 SELECT
        *,
        RN = ROW_NUMBER() OVER(ORDER BY (SELECT NULL))
    FROM dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1
	where intervalDateLocal  BETWEEN '2018-03-01' AND '2018-10-01'
)A where A.RN <=10000


delete dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1
from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1 d,dataArchival.TSrcTblPartFactIOTEventDowntimeWinding2 d2
where d.intervalDateLocal=d2.intervalDateLocal

select intervalDateLocal
                     from dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1
                     group by intervalDateLocal
                     limit 1 offset 1