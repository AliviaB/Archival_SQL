/******* Restore Procedure*************
Proc Name: usp_restoreDataToDW
Schema : dataArchival
Author :Alivia Banerjee
Create Date:20190306
Last Modified :20190326

*****************************************/

IF OBJECT_ID (N'dataArchival.usp_restoreDataToDW', N'P') IS NOT NULL 
DROP PROC dataArchival.usp_restoreDataToDW
GO
CREATE PROC dataArchival.usp_restoreDataToDW 
(
	-- @SourceTableName		varchar(500),
	-- @ExportLocation         varchar(500),
	@InputParam				 varchar(1000),
	@ExecStatus              varchar(10) out,
	@StatusMessage           varchar(1000) out
)
AS
BEGIN
	-----------------Declare-------------------------
	DECLARE @object_id              INT,
			@Schema_name            VARCHAR(50),
			@sql                    VARCHAR(max),
			@ExternalTableName      VARCHAR(500),
			@xact_state 			INT,
			@msg 					VARCHAR(500),
			@SourceTableName		VARCHAR(500),
			@ExportLocation         VARCHAR(500),
			@str					VARCHAR(128)
	   ----- parameter preparing			
	SELECT @str=substring(@InputParam,0,CharIndex('#|#',@InputParam))
	
	SELECT @SourceTableName= substring(@str,CharIndex('=',@str)+1,len(@str));
	
	SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @ExportLocation =substring(@str,CharIndex('=',@str)+1,len(@str));
	--select @SourceTableName,@ExportLocation	
	-------------------------------------------------------	

	SELECT @Schema_name  = s.name
	FROM    sys.objects    o  
	JOIN    sys.schemas     s    
	ON     o.[schema_id]   = s.[schema_id]
	WHERE  o.object_id = OBJECT_ID (N'dataArchival.usp_restoreDataToDW', N'P')

	SELECT @ExternalTableName = @Schema_name +'.'+
	substring(@SourceTableName,CharIndex('.',@SourceTableName)+1,(len(@SourceTableName)-CharIndex('.',@SourceTableName)))+'_Restore'

	-------- Clear EXTERNAL TABLE first--------------
	IF OBJECT_ID(@ExternalTableName	) IS NOT NULL 
	BEGIN
			SELECT @sql ='DROP EXTERNAL TABLE '+@ExternalTableName	
			
				BEGIN TRY
					EXEC (@sql)
                END TRY
				BEGIN CATCH
										
					SELECT  @ExecStatus=1, @StatusMessage= convert(varchar,ERROR_NUMBER() )
						                         +': Failed to drop external table :'+ERROR_MESSAGE();
			  END CATCH;
			 
			
			

	END
 
	
	-------------- Create External table to load data from ADLS AND Final Insert into table----------------------------------------------

		
       BEGIN TRY
				SELECT @sql='CREATE EXTERNAL TABLE '+@ExternalTableName+
							' WITH 
							  (
							    LOCATION = '''+@ExportLocation+''',
							    DATA_SOURCE = ArchStorageSource,
							    FILE_FORMAT = ArchivalFileFormat,
								REJECT_TYPE = VALUE,
								REJECT_VALUE = 100,
								REJECTED_ROW_LOCATION=''/Reject_Directory''
							   )
							  as SELECT * FROM '+@SourceTableName+' WHERE 1=2;'
				--select @sql
				EXEC(@sql);
		
	  END TRY 
	  BEGIN CATCH
					SELECT  @ExecStatus=1, @StatusMessage= convert(varchar,ERROR_NUMBER() )
						                         +': Creating External Table failed :'+ERROR_MESSAGE()
	  END CATCH;
	  BEGIN TRY
				SELECT @sql = ' INSERT INTO '+@SourceTableName
							 +' SELECT * from '+@ExternalTableName

				EXEC(@sql)

				SELECT @ExecStatus=0, @StatusMessage='Import Successful'
	  END TRY 
	  BEGIN CATCH
					SELECT  @ExecStatus=1, @StatusMessage= convert(varchar,ERROR_NUMBER() )
						                         +': Data Import failed :'+ERROR_MESSAGE()
	  END CATCH;

END



