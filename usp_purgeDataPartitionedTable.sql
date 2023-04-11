/******* Purging Procedure Partition*************
Proc Name: usp_purgeDataPartitionedTable
Schema : dataArchival
Author :Alivia Banerjee
Create Date:20190308
Last Modified :20190308

*****************************************/

IF OBJECT_ID (N'dataArchival.usp_purgeDataPartitionedTable', N'P') IS NOT NULL 
DROP PROC dataArchival.usp_purgeDataPartitionedTable
GO
CREATE PROC dataArchival.usp_purgeDataPartitionedTable 
(
	
	@InputParam				 varchar(1000),
	@execStatus              varchar(10) out,
	@statusMessage           varchar(1000) out
)
AS
BEGIN
	DECLARE @SourceTableName		varchar(500),
			@ArchSourceTSField		varchar(200),
			@ArchPeriod				varchar(10),
			@str					varchar(128)
	
	
	

	DECLARE  @sql varchar(1000),@objectId int,@targetPartitionNm int, @SinkTable varchar(500),@distribution int,
			 @indextype int,@indexColumn varchar(50),@distributionColumn varchar(50), @Desc varchar(100),@Schema_name varchar(50),
			 @limit int,@i int=1,@text varchar(800),@xact_state int
	----- parameter preparing			
	SELECT @str=substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	
	SELECT @SourceTableName= substring(@str,CharIndex('=',@str)+1,len(@str));
	SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @ArchSourceTSField= substring(@str,CharIndex('=',@str)+1,len(@str));
		
	SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @ArchPeriod =substring(@str,CharIndex('=',@str)+1,len(@str));
-- select @SourceTableName,@ArchSourceTSField,@StartDate,@EndDate,@ExportLocation


		SELECT @Schema_name  = s.name
	      FROM    sys.objects    o  
	      JOIN    sys.schemas     s    
	        ON     o.[schema_id]   = s.[schema_id]
	     WHERE  o.object_id = OBJECT_ID (N'dataArchival.usp_purgeDataPartitionedTable', N'P')
	  
		 SELECT @SinkTable = @Schema_name+'.'+
							 substring(@sourceTableName,CharIndex('.',@sourceTableName)+1,(len(@sourceTableName)-CharIndex('.',@sourceTableName)))+'Sink'--- add dataArchival Schema
		 -- SELECT @SinkTable
		 --------------- Get Object Id
			  SELECT  @objectId = OBJECT_ID(@SourceTableName)
			  

-------------- Find partition number from source table
	 SELECT 
			 @targetPartitionNm= p.partition_Number  
			  
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
			  AND prv_right.boundary_id = p.partition_number - 1
		WHERE
			  OBJECTPROPERTY(p.object_id, 'ISMSShipped') = 0
			  and p.object_id = @objectId
			  and prv_right.value=Convert(datetime,@ArchPeriod)

		
		------------------ Create Sink table to switch partition to-------------------------------
	  

		SELECT @sql =' CREATE TABLE '+@SinkTable +
		         ' WITH  ( '+' DISTRIBUTION = '
				 
-------------------------------- Distribution query------
		SELECT @distribution= distribution_policy,@Desc = distribution_policy_desc
		FROM  sys.pdw_table_distribution_properties td
		WHERE  td.object_id=@objectId

		IF (@distribution =2) -- For HASH distribution
		BEGIN
			SELECT @distributionColumn=c.name  
			FROM sys.columns c 
			INNER JOIN sys.pdw_column_distribution_properties d 
			ON c.column_id=d.column_id and C.Object_id=d.Object_id
			INNER JOIN sys.pdw_table_distribution_properties td
			ON d.object_id=td.object_id
			WHERE  d.object_id=@objectId and distribution_ordinal=1
	
			SELECT @sql= @sql+@Desc+' ( '+@distributionColumn+ ' ), ' --- adding distribution column name

		END 
		ELSE
		BEGIN
			SELECT @sql= @sql+@Desc+' ,' -- for Replication/Round_Robin distribution
		END
	-------------------------------- INDEX query------	
	SELECT @indextype = type,@Desc=type_desc +' INDEX '
	FROM 	sys.indexes 
	WHERE OBJECT_ID=@objectId

	IF (@indextype =1)
	BEGIN
		IF ((SELECT count(1) from sys.index_columns where object_id = @objectId)>1)---- multi column index
		BEGIN
			
			 CREATE table #tempColumn( columnId int, ColumnName varchar(50))
			 INSERT into #tempColumn
			 SELECT c.column_id, c.name
			 FROM sys.index_columns ic 
			 JOIN sys.columns c  
			 ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
			 WHERE ic.[object_id] =@objectId
			 
			 SELECT @limit= max(columnId) FROM #tempColumn
			 SELECT @text=''
				WHILE (@i<=@limit) 
				BEGIN
					SELECT @indexColumn = ColumnName FROM #tempColumn WHERE columnid=@i
					SELECT @text = @text +@indexColumn+','
					
					DELETE FROM #tempColumn WHERE columnid=@i
					IF( @i=@limit )
					
					SELECT @text = LEFT(@text, LEN(@text) - 1) 
					SELECT @i = MIN(columnid) FROM #tempColumn
					
				END 
				SELECT @sql= @sql+@Desc +'('+@text+' )'
				 
		
		END
		
		ELSE 
		BEGIN
			SELECT @indexColumn = c.name
			FROM sys.index_columns ic 
			JOIN sys.columns c  
			ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
			WHERE ic.[object_id] =@objectId
			SELECT @sql= @sql+@Desc +'('+@indexColumn+' )'
		END	
    END
    ELSE
	BEGIN
	    SELECT @sql= @sql+@Desc
	END
	-------------------  rest of table creation query
   
			SELECT @sql =@sql +')
			 as SELECT * FROM '+@SourceTableName +' WHERE 1=2'
			
			-- SELECT @sql
			
                    BEGIN TRY
						----- create sink table
						EXEC(@sql)
						---- switch partition
						Select @sql = 'ALTER TABLE '+@SourceTableName+' SWITCH PARTITION '+convert(varchar,@targetPartitionNm) +' TO '+@SinkTable
			
						EXEC (@sql)
			
						----- drop sink table 
						SELECT @sql =' DROP TABLE '+@SinkTable
			
						EXEC (@sql)

						SELECT @execStatus=0,@statusMessage='Partition switched successfully';
					END TRY 
					BEGIN CATCH
					

					SELECT   @execStatus=1,
					         @statusMessage='Partition switch failed :'+ERROR_MESSAGE() 
					END CATCH;
	        
		--SELECT 'No Rows available to purge'





END


