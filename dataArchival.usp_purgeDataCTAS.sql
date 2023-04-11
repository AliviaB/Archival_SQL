/******* Purging Procedure CTAS*************
Proc Name: usp_purgeDataCTAS
Schema : dataArchival
Author :Alivia Banerjee
Create Date:20190325
Last Modified :20190325

*****************************************/

IF OBJECT_ID (N'dataArchival.usp_purgeDataCTAS', N'P') IS NOT NULL 
DROP PROC dataArchival.usp_purgeDataCTAS
GO
CREATE PROC dataArchival.usp_purgeDataCTAS 
(
	@InputParam				 varchar(1000),
	@ExecStatus              varchar(10) out,
	@StatusMessage           varchar(128) out

)
AS
BEGIN
		
	 DECLARE    @sql varchar(1000),
				@tsql varchar(1000),
				@objectId int, 
				@SinkTable varchar(500),
				@distribution int,
				@indextype int,
				@indexColumn varchar(50),
				@distributionColumn varchar(50),
				@Desc varchar(100),
				@Schema_name varchar(50),
			    @limit int,
				@i int=1,
				@text varchar(800),
				@sourceSchema varchar(50),
				@TableName varchar(128),
				@IntrTableName varchar(128),
				@IntrTableNameWtSch varchar(128),
				@SourceTableName		varchar(500),-- schemaname.tableName
	            @ArchSourceTSField		varchar(200),
	            @StartDate				Datetime,
	            @EndDate				DateTime,
				@str                    varchar(128)
        ----- parameter preparing			
	    SELECT @str=substring(@InputParam,0,CharIndex('#|#',@InputParam))
	    SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	    
	    SELECT @SourceTableName= substring(@str,CharIndex('=',@str)+1,len(@str));
	    
	    SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	    SELECT @ArchSourceTSField= substring(@str,CharIndex('=',@str)+1,len(@str));
	    SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	    SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	    SELECT @StartDate= convert(datetime,substring(@str,CharIndex('=',@str)+1,len(@str)));
	    SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	    SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	    SELECT @EndDate= convert(datetime,substring(@str,CharIndex('=',@str)+1,len(@str)));
	    
		-- select @SourceTableName,@ArchSourceTSField,@StartDate,@EndDate
	-------------------------------------------------------	
	   SELECT @Schema_name  = s.name
	        FROM    sys.objects    o  
	        JOIN    sys.schemas     s    
	          ON     o.[schema_id]   = s.[schema_id]
	       WHERE  o.object_id = OBJECT_ID (N'dataArchival.usp_purgeDataCTAS', N'P')

		SELECT @TableName = substring(@sourceTableName,CharIndex('.',@sourceTableName)+1,(len(@sourceTableName)-CharIndex('.',@sourceTableName)))
	    SELECT @IntrTableName = @TableName+'_ArchInter'
	    SELECT @SinkTable = @Schema_name+'.'+@TableName+'_Sink'--- add Archival Schema
		SELECT @sourceSchema =substring(@sourceTableName,0,CharIndex('.',@sourceTableName))
	    SELECT @IntrTableNameWtSch=@Schema_name+'.'+@IntrTableName
	    --------------- Get Object Id
	   	  SELECT  @objectId = OBJECT_ID(@SourceTableName)




------------------------- A:Preapare Sink table to store data----------------------------------------
	  

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

	IF (@indextype =1) --clustured index
	BEGIN
		IF ((SELECT count(1) from sys.index_columns where object_id = @objectId)>1)---- multi column clustured index
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
			  SELECT @sql= @sql+@Desc +'('+@text+' )' -- adding index columns
				 
		
		END
		
		ELSE 
		BEGIN  ---- single column clustured index
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
			 as ((SELECT * FROM '+@IntrTableNameWtSch +' WHERE '
			 + @ArchSourceTSField  +' < '''+ convert(varchar(10), @StartDate, 120)+''''
			 +') UNION ALL (SELECT * FROM '+@IntrTableNameWtSch +' WHERE
			  '+ @ArchSourceTSField+' > '''+convert(varchar(10), @EndDate, 120)+'''))'
			

                   

				-----------------------  Rename Sorce table to intermediate table
				
				   BEGIN TRY
						SELECT @tsql = ' RENAME OBJECT '+@sourceTableName+' TO '+@IntrTableName
						--SELECT @tsql
						EXEC(@tsql)
						----------------------- Transfer to dataArchival Schema
						SELECT @tsql = 'ALTER SCHEMA '+@Schema_name+' TRANSFER OBJECT::'+@sourceSchema+'.'+@IntrTableName;
						--SELECT @tsql
						EXEC(@tsql)
				   END TRY 
				   BEGIN CATCH
						SELECT  @ExecStatus=1, @StatusMessage=convert(varchar,ERROR_NUMBER() )
												+': Failed to Rename Source :'+ERROR_MESSAGE()   
					
					END CATCH;
					--------------------------Create sink table
				   BEGIN TRY
						--SELECT @sql
						EXEC(@sql)
                   END TRY
				   BEGIN CATCH
					
						SELECT  @ExecStatus=1, @StatusMessage= convert(varchar,ERROR_NUMBER() )
						                         +': Failed to Create table :'+ERROR_MESSAGE()   
					
				   END CATCH;
						------------------------- Drop intermediate table
                   BEGIN TRY
						SELECT @sql = 'DROP TABLE '+@IntrTableNameWtSch
						--SELECT @sql
						EXEC (@sql)
                   END TRY
			       BEGIN CATCH
					 		SELECT  @ExecStatus=1, @StatusMessage= convert(varchar,ERROR_NUMBER() )
						                            +': Failed to Drop Intrim table :'+ERROR_MESSAGE()   
					
				   END CATCH;  
						------------------------- Rename Sink To Source (Table renamed to archival schema)
                   BEGIN TRY
						SELECT @sql =' RENAME OBJECT '+@SinkTable+' TO '+@TableName
						--SELECT @sql
						EXEC (@sql)

						-------------------------- transfer to original schema
						SELECT @sql ='ALTER SCHEMA '+@sourceSchema+' TRANSFER OBJECT::'+@Schema_name+'.'+@TableName;
						EXEC (@sql)
						--SELECT @sql
						SELECT @ExecStatus=0, @StatusMessage='Rows purged successfully'

					END TRY 
					BEGIN CATCH
					

					SELECT   @ExecStatus=1,@StatusMessage=convert(varchar,ERROR_NUMBER() )+': Failed to Rename to Source :'+ERROR_MESSAGE()   
					
					END CATCH;
	    --    IF @@TRANCOUNT >0
			  --BEGIN
					-- PRINT 'COMMIT';
					-- COMMIT TRAN;
			  --END





END


