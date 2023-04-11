/******* Archival Procedure*************
Proc Name: usp_loadDataToBlob
Schema : dataArchival
Author :Alivia Banerjee
Create Date:20190304
Last Modified :20190305

*****************************************/

IF OBJECT_ID (N'dataArchival.usp_loadDataToBlob', N'P') IS NOT NULL 
DROP PROC dataArchival.usp_loadDataToBlob
GO
CREATE PROC dataArchival.usp_loadDataToBlob 
(
	@InputParam				 varchar(1000),
	@ExecStatus              varchar(10) out,
	@StatusMessage           varchar(1000) out
)--  'SourceTableName=dataArchival.Testt_Credit_Control#|#ArchSourceTSField=CALDAY#|#StartDate=2014-09-01#|#EndDate=2014-09-01#|#ExportLocation=dataArchival.Testt_Credit_Control/20140901#|#'
AS
BEGIN
	--- Declare block
	DECLARE 
			@SourceTableName		varchar(500),
	        @ArchSourceTSField		varchar(200),
	        @StartDate				datetime,
	        @EndDate				datetime,
	        @ExportLocation         varchar(500),
			@sql 					varchar(max),
			@Schema_name            VARCHAR(50),			
	        @sourceRowCount 		int, 
			@destinationRowCount 	int,
			@destinationRowCountOut int,
	        @nsql 					nvarchar(max),
			@sourceRowCountOut 		int,
			@ExternalTableName 		varchar(500),
			@ExternalTableNameSchm 		varchar(500),
			@xact_state 			int,
			@msg 					varchar(500),
			@str					varchar(100),
			@schema_id				int
			
    ----- parameter preparing			
	SELECT @str=substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @SourceTableName= substring(@str,CharIndex('=',@str)+1,len(@str));
	SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @ArchSourceTSField= substring(@str,CharIndex('=',@str)+1,len(@str));
	SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @StartDate= convert(datetime,substring(@str,CharIndex('=',@str)+1,len(@str)));
	SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @EndDate= convert(datetime,substring(@str,CharIndex('=',@str)+1,len(@str)));
	SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @ExportLocation =substring(@str,CharIndex('=',@str)+1,len(@str));
-- select @SourceTableName,@ArchSourceTSField,@StartDate,@EndDate,@ExportLocation	
	-------------------------------------------------------	
		SELECT @Schema_name  = s.name,
			   @schema_id = o.[schema_id]
	FROM    sys.objects    o  
	JOIN    sys.schemas     s    
	ON     o.[schema_id]   = s.[schema_id]
	WHERE  o.object_id = OBJECT_ID (N'dataArchival.usp_loadDataToBlob', N'P')
	
	SELECT @ExternalTableName= 	substring(@SourceTableName,CharIndex('.',@SourceTableName)+1,(len(@SourceTableName)-CharIndex('.',@SourceTableName)))+'_Arch'
	SELECT @ExternalTableNameSchm = @Schema_name+'.'+ @ExternalTableName
	--SELECT @ExternalTableName
	-------- Clear all components first--------------
	IF EXISTS (SELECT * FROM sys.external_tables WHERE name=@ExternalTableName AND schema_id = @schema_id)
	BEGIN
			SELECT @sql ='DROP EXTERNAL TABLE '+@ExternalTableNameSchm	
			--BEGIN TRAN
				BEGIN TRY
					EXEC (@sql)
                END TRY
				BEGIN CATCH
					SET @xact_state = XACT_STATE();

					IF @@TRANCOUNT > 0
					BEGIN
					    PRINT 'ROLLBACK';
					    ROLLBACK TRAN;
					END

					-- SELECT  ERROR_NUMBER()    AS ErrNumber
					-- ,       ERROR_SEVERITY()  AS ErrSeverity
					-- ,       ERROR_STATE()     AS ErrState
					-- ,       ERROR_PROCEDURE() AS ErrProcedure
					       
					
					select @ExecStatus=1,@StatusMessage =convert(varchar(10),ERROR_NUMBER())+':Dropping external table failed '+ERROR_MESSAGE();
			    END CATCH;
			  IF @@TRANCOUNT >0
			  BEGIN
					 PRINT 'COMMIT';
					-- COMMIT TRAN;
			  END
			
			--SELECT @xact_state AS TransactionState;

	END
 

	-------------- D: Create External table for Final Load to ADLS -------------------------------------------------------------------

			SELECT @sql =  'CREATE EXTERNAL TABLE '+@ExternalTableNameSchm+
							' WITH 
								(
								     LOCATION ='''+@ExportLocation+''',
								     DATA_SOURCE = ArchStorageSource,
								     FILE_FORMAT = ArchivalFileFormat,
									 REJECT_TYPE = VALUE,
									 REJECT_VALUE = 100,
									 REJECTED_ROW_LOCATION=''/Reject_Directory''
								)
								AS
							    SELECT  *
							      FROM   '+ @SourceTableName
							+ '  WHERE ' +@ArchSourceTSField 
							+' BETWEEN '''+ convert(varchar(10), @StartDate, 120)
							+'''   AND '''+convert(varchar(10), @EndDate, 120)+''''
			--SELECT @sql

			SELECT @nsql='SELECT @sourceRowCountOut=COUNT(1) 
						    FROM '+ @SourceTableName
					   + ' WHERE ' +@ArchSourceTSField 
					  +' BETWEEN '''+ convert(varchar(10), @StartDate, 120)
					  +'''   AND '''+convert(varchar(10), @EndDate, 120)+''''
			--BEGIN TRAN
				BEGIN TRY
					EXEC (@sql);
                    
					EXECUTE sp_executesql @nsql, N'@sourceRowCountOut int OUTPUT',  @sourceRowCountOut=@sourceRowCount OUTPUT;  
					
					SELECT @nsql='SELECT @destinationRowCountOut=count(1) from '+ @ExternalTableNameSchm  

						EXECUTE sp_executesql @nsql, N'@destinationRowCountOut int OUTPUT',  @destinationRowCountOut=@destinationRowCount OUTPUT; 

					--select @sourceRowCount,@destinationRowCount
	

---------------------------------validation and success msg---------------------
					if (@sourceRowCount = @destinationRowCount)

					 SELECT  @ExecStatus=0,@StatusMessage ='Export successful'
						ELSE
						BEGIN
							SELECT @ExecStatus=1,@StatusMessage  ='Validation failed:Source count don''t matches with target count '
							  
						 END

				END TRY 
			    BEGIN CATCH
					--SET @xact_state = XACT_STATE();

					--IF @@TRANCOUNT > 0
					--BEGIN
					--    PRINT 'ROLLBACK';
					--    ROLLBACK TRAN;
					--END

					-- SELECT  ERROR_NUMBER()    AS ErrNumber
					-- ,       ERROR_SEVERITY()  AS ErrSeverity
					-- ,       ERROR_STATE()     AS ErrState
					-- ,       ERROR_PROCEDURE() AS ErrProcedure
					-- ,       ' '+ERROR_MESSAGE()   AS ErrMessage
					-- ;
					SELECT @ExecStatus=1,@StatusMessage =convert(varchar(10),ERROR_NUMBER())+':Export failed '+ERROR_MESSAGE();
			  END CATCH;
			  --IF @@TRANCOUNT >0
			  --BEGIN
					-- PRINT 'COMMIT';
				 --  --  COMMIT TRAN;
			  --END
			
			--SELECT @xact_state AS TransactionState;
	
	
	 
	
	

	
END 
----------------------------------------------------------------------------------------------------------------------

