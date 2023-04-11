exec dataArchival.usp_purgeDataPartitionedTable 
@SourceTableName	= 'dataArchival.TSrcTblPartFactIOTEventDowntimeWinding1',
@ArchSourceTSField	= 'intervalDateLocal',
@ArchFrequency 		= 'MM',
@ArchPeriod			= '2018-03-01'

exec dataArchival.usp_purgeDataPartitionedTable 
@SourceTableName	= 'dataArchival.FactIOTEventPerfWinding',
@ArchSourceTSField	= 'intervalDateLocal',
@ArchFrequency 		='MM',
@ArchPeriod			= '2018-01-01'





SELECT 
			 targetPartitionNm= p.partition_Number  ,
			 prv_right.value,
			 arowCount = p.rows 
			  
	    FROM  sys.partitions                  AS p
		JOIN  sys.indexes                     AS i
			    ON i.object_id = p.object_id
			   AND i.index_id = p.index_id
		JOIN  sys.data_spaces                 AS ds
		       ON ds.data_space_id = i.data_space_id
		JOIN  sys.partition_schemes           AS ps
			   ON ps.data_space_id = ds.data_space_id
		JOIN  sys.partition_functions         AS pf
			   ON pf.function_id = ps.function_id
		LEFT JOIN sys.partition_range_values AS prv_right
			   ON ps.function_id = prv_right.function_id
			  AND prv_right.boundary_id = p.partition_number 
		WHERE
			  OBJECTPROPERTY(p.object_id, 'ISMSShipped') = 0
			  and p.object_id = 1255323882
			  and prv_right.value=convert(datetime,'2018-03-01')