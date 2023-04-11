/******* Purging Procedure*************
Proc Name: usp_purgeData
Schema : dataArchival
Author :Alivia Banerjee
Create Date:20190306
Last Modified :20190306

*****************************************/

IF OBJECT_ID (N'dataArchival.usp_purgeData', N'P') IS NOT NULL 
DROP PROC dataArchival.usp_purgeData
GO
CREATE PROC dataArchival.usp_purgeData 
(
	@SourceTableName		varchar(500),
	@ArchSourceTSField		varchar(200),
	@StartDate				datetime,
	@EndDate				datetime,--- included
	@ArchFrequency			varchar(10)
)
AS
BEGIN
--- Declare block
	DECLARE  @sourceRowCount int, @loopLimit int , @loopLimitOut int,@nsql nvarchar(1000),
			@sourceRowCountOut int,@objectID int,@batchCntrTS datetime,@xact_state int,@sql varchar(1000),@batchCntrNxtTS datetime;

	if @sourceTableName is null
	BEGIN
		raiserror('Table Name is Null', 16, 1)
	  
	END

	SELECT @nsql='SELECT @sourceRowCountOut=COUNT(1) 
						 FROM '+ @sourceTableName
							+ 
						' WHERE ' +@ArchSourceTSField +' >= '''+ convert(varchar(10), @StartDate, 120)+''' AND '+@ArchSourceTSField +' <= '''+convert(varchar(10), @EndDate, 120)+''''
	
	EXECUTE sp_executesql @nsql, N'@sourceRowCountOut int OUTPUT',  @sourceRowCountOut=@sourceRowCount OUTPUT;  

	
	if (@sourceRowCount>0)
     BEGIN
		SET @batchCntrTS= @StartDate
		SET @batchCntrNxtTS= dateadd(hh,1,@batchCntrTS)

			WHILE(@batchCntrTS<=@EndDate)
			BEGIN
			    
					SELECT @sql ='DELETE FROM '+ @sourceTableName
									+' WHERE '
									
					SELECT @sql = @sql + CASE WHEN 	@ArchFrequency ='MONTH'
													THEN ' CONVERT(date ,'+@ArchSourceTSField +') = CONVERT(date ,'''+convert(varchar(20), @batchCntrTS, 120)+''')'
                                                 WHEN @ArchFrequency ='DAY'
												    THEN @ArchSourceTSField +' >= convert(datetime,'''+ convert(varchar(20), @batchCntrTS, 120)+''') AND '+
														 @ArchSourceTSField +' < convert(datetime,'''+ convert(varchar(20),@batchCntrNxtTS, 120)+''')'
										  END;
					
					--SELECT @sql
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

						SELECT  ERROR_NUMBER()    AS ErrNumber
						,       ERROR_SEVERITY()  AS ErrSeverity
						,       ERROR_STATE()     AS ErrState
						,       ERROR_PROCEDURE() AS ErrProcedure
						,       'Deleting rows failed'+ERROR_MESSAGE()   AS ErrMessage
						;
						break;
					END CATCH;
			  IF @@TRANCOUNT >0
			  BEGIN
					 PRINT 'COMMIT';
					 COMMIT TRAN;
			  END
                  
				SELECT @batchCntrTS =  CASE WHEN  @ArchFrequency ='MONTH'
												THEN   dateadd(dd,1 , @batchCntrTS)
											WHEN @ArchFrequency ='DAY'
											    THEN   dateadd(hh,1,@batchCntrTS)
											END	
				SELECT @batchCntrNxtTS = DATEADD(hh,1,@batchCntrTS)

	          END

			  
			
	  
		 
	  
			SELECT 'Rows purged successfully'
	
	 END
	ELSE
		SELECT 'No Rows available to purge'





END


