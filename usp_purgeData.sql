/******* Purging Procedure*************
Proc Name: usp_purgeData
Schema : dataArchival
Author :Alivia Banerjee
Create Date:20190306
Last Modified :20190319

*****************************************/

IF OBJECT_ID (N'dataArchival.usp_purgeData', N'P') IS NOT NULL 
DROP PROC dataArchival.usp_purgeData
GO
CREATE PROC dataArchival.usp_purgeData 
(
	@InputParam				 varchar(1000),
	@execStatus              varchar(10) out,
	@statusMessage           varchar(1000) out
)
AS
BEGIN
--- Declare block
	DECLARE  @sourceRowCount     int, 
	         @loopLimit          int , 
			 @loopLimitOut       int,
			 @nsql               nvarchar(1000),
			 @sourceRowCountOut  int,
			 @objectID           int,
			 @batchCntrTS        datetime,
			 @xact_state         int,
			 @sql                varchar(1000),
			 @batchCntrNxtTS     datetime;
			
	DECLARE @SourceTableName		varchar(500),
	        @ArchSourceTSField		varchar(200),
	        @StartDate				datetime,
	        @EndDate				datetime,--- included
	        @ArchFrequency			varchar(10),
			@str					varchar(128)

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
	SELECT @InputParam =substring(@InputParam,len(@str)+4,len(@InputParam))
	SELECT @str = substring(@InputParam,0,CharIndex('#|#',@InputParam))
	SELECT @ArchFrequency =substring(@str,CharIndex('=',@str)+1,len(@str));
-- select @SourceTableName,@ArchSourceTSField,@StartDate,@EndDate,@ArchFrequency
	
			

		SET @batchCntrTS= @StartDate
		SELECT @batchCntrNxtTS= CASE WHEN  @ArchFrequency ='MONTH'
												THEN   dateadd(dd,1 , @batchCntrTS)
											WHEN @ArchFrequency ='DAY'
											    THEN   dateadd(hh,1,@batchCntrTS)
											END

			WHILE(@batchCntrTS<=@EndDate)
			BEGIN
			    
					SELECT @sql ='DELETE FROM '+ @sourceTableName
									+' WHERE ' +  @ArchSourceTSField +' >= convert(datetime,'''+ convert(varchar(20), @batchCntrTS, 120)+''') AND '+
												  @ArchSourceTSField +' < convert(datetime,''' + convert(varchar(20),@batchCntrNxtTS, 120)+''')'
										 
					
					--SELECT @sql
              BEGIN TRAN
                    BEGIN TRY
					    EXEC (@sql)
						SELECT @execStatus =0,@statusMessage ='Rows purged successfully'
                    END TRY
				    BEGIN CATCH
						--SET @xact_state = XACT_STATE();

						IF @@TRANCOUNT > 0
						BEGIN
						   -- PRINT 'ROLLBACK';
						    ROLLBACK TRAN;
						END

						
						SELECT @execStatus =1 , @statusMessage =convert(varchar(10),ERROR_NUMBER())+':Deleting rows failed '+ERROR_MESSAGE();
						
						break;
					END CATCH;
			  IF @@TRANCOUNT >0
			  BEGIN
					 --PRINT 'COMMIT';
					 COMMIT TRAN;
			  END
                  
				SELECT @batchCntrTS =  CASE WHEN  @ArchFrequency ='MONTH'
												THEN   dateadd(dd,1 , @batchCntrTS)
											WHEN @ArchFrequency ='DAY'
											    THEN   dateadd(hh,1,@batchCntrTS)
										END	
				SELECT @batchCntrNxtTS = CASE WHEN  @ArchFrequency ='MONTH'
												THEN   dateadd(dd,1 , @batchCntrTS)
											  WHEN @ArchFrequency ='DAY'
											    THEN   dateadd(hh,1,@batchCntrTS)
										 END

	          END

			  
			
	  
		 
	  
			
	
	




END


