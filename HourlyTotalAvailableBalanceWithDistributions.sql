USE [DataWareHouse_Workspace]
GO
ALTER PROCEDURE [DBO].[spHourlyTotalUserBalances] (@BaseDay DATE) AS
DROP TABLE IF EXISTS #TempDateHours,#ReadyToUpdateData_Transactions,/*#ReadyToUpdateData_MerchantTransactions,*/#A,#B,#C
CREATE TABLE #TempDateHours (HourlyDateTime DATETIME);
DECLARE  @inc		AS INT  = 1
--		 @m			AS INT  = 1,
--		 @BaseDay   AS DATE		= '2017-11-16'--CAST(GETDATE() as DATE)--'2019-09-14' --Enter this while inputing data!

		--IF DAY(@BaseDay) = 1-- OPEN IF YOU USE dateadd(day, 1, eomonth(@BaseDay, -@m)) FOR @StartDateParameter
		--   BEGIN
		--   SET @m = @m + 1
		--   END
/*1- First you need to create a dummy hourly table to input all hour for given date interval that basing User by User*/
DECLARE	 @StartDateParameter AS DATETIME = DATEADD(DAY,-1,cast(@BaseDay as DATETIME))--dateadd(day, 1, eomonth(@BaseDay, -@m))
DECLARE  @StartDate			 AS DATETIME = DATEADD(DAY,-1,cast(@StartDateParameter as DATETIME))
		WHILE @StartDate <= @BaseDay
			BEGIN
				IF(@inc <= 23)
					BEGIN
					INSERT INTO #TempDateHours
						SELECT
							DATEADD(hour,@inc,@StartDate) HourlyDateTime
						SET @inc = @inc + 1
					END
				ELSE
					BEGIN
						SET @inc = 0
						SET @StartDate = DATEADD(DAY,1,@StartDate)
					END
			END

			SET @StartDate = @StartDateParameter
			;
DELETE FROM #TempDateHours WHERE @BaseDay   < HourlyDateTime
DELETE FROM #TempDateHours WHERE @StartDate > HourlyDateTime;
;
WITH DailyLastBalanceOfUserUpdating AS
	(
	SELECT
		L.UserIdentifier
	   ,L.Currency
	   ,AvailableBalance AvailableBalanceEod
	   ,ROW_NUMBER() OVER (PARTITION BY L.UserIdentifier,L.Currency,CAST(L.CreatedAt AS DATE) ORDER BY L.CreatedAt DESC) Ranker
	   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,L.CreatedAt))/24)/24, dateadd(day, 0, datediff(day, -1, L.CreatedAt))) ContributedEndOfDayHour
	   ,L.CreatedAt LastTxDate
	FROM (select UserIdentifier,Currency,CreatedAt,AvailableBalance from Transactions2020Before.dbo.FACT_Transactions With (Nolock) 
			union all
		  select UserIdentifier,Currency,CreatedAt,AvailableBalance from		DWH_DataWareHouse_Workspace.dbo.FACT_Transactions With (Nolock)) L
	JOIN DataWareHouse_Workspace.[DataWareHouse_Workspace\skacar].FACT_DailyEodUserBalances DEO With (Nolock) ON L.UserIdentifier = DEO.UserIdentifier AND L.Currency = DEO.Currency
	WHERE CreatedAt >= DATEADD(DAY,-2,@StartDate) and CreatedAt < DATEADD(DAY,-1,@BaseDay)
	)
	UPDATE DEO
	SET  DEO.AvailableBalanceEod	 = DLO.AvailableBalanceEod
		,DEO.ContributedEndOfDayHour = DLO.ContributedEndOfDayHour
		,DEO.LastTxDate				 = DLO.LastTxDate
	FROM DailyLastBalanceOfUserUpdating DLO With (Nolock)
	JOIN DataWareHouse_Workspace.[DataWareHouse_Workspace\skacar].FACT_DailyEodUserBalances DEO With (Nolock) ON DLO.UserIdentifier = DEO.UserIdentifier AND DLO.Currency = DEO.Currency
	WHERE Ranker = 1
;
WITH DailyLastBalanceOfUserNewComerEntries AS
	(
	SELECT
		L.UserIdentifier
	   ,L.Currency
	   ,AvailableBalance AvailableBalanceEod
	   ,ROW_NUMBER() OVER (PARTITION BY L.UserIdentifier,L.Currency,CAST(L.CreatedAt AS DATE) ORDER BY L.CreatedAt DESC) Ranker
	   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,L.CreatedAt))/24)/24, dateadd(day, 0, datediff(day, -1, L.CreatedAt))) ContributedEndOfDayHour
	   ,L.CreatedAt LastTxDate
	FROM (select UserIdentifier,Currency,CreatedAt,AvailableBalance from Transactions2020Before.dbo.FACT_Transactions With (Nolock) 
			union all
		  select UserIdentifier,Currency,CreatedAt,AvailableBalance from		DWH_DataWareHouse_Workspace.dbo.FACT_Transactions With (Nolock)) L
	LEFT JOIN DataWareHouse_Workspace.[DataWareHouse_Workspace\skacar].FACT_DailyEodUserBalances DEO With (Nolock) ON L.UserIdentifier = DEO.UserIdentifier AND L.Currency = DEO.Currency
	WHERE CreatedAt >= DATEADD(DAY,-2,@StartDate) and CreatedAt < DATEADD(DAY,-1,@BaseDay)
		 AND DEO.UserIdentifier IS NULL AND DEO.Currency IS NULL		  
	)
	INSERT INTO DataWareHouse_Workspace.[DataWareHouse_Workspace\skacar].FACT_DailyEodUserBalances
	select UserIdentifier,Currency,AvailableBalanceEod,ContributedEndOfDayHour,LastTxDate
	from DailyLastBalanceOfUserNewComerEntries
	where Ranker = 1
;
WITH TransactionsData_CTE/*AvailableBalanceCTE_Transactions*/ AS
	(
	SELECT
		UserIdentifier
	   ,Currency
	   ,AvailableBalance
	   , IIF(DATEPART(HOUR,CreatedAt) = 23
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,CreatedAt))), dateadd(day, 0, datediff(day, -1, CreatedAt))) 
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,CreatedAt))), dateadd(day, 0, datediff(day,  0, CreatedAt))
						)) ContributedDateHour
	   ,ROW_NUMBER() OVER (PARTITION BY UserIdentifier,Currency,IIF(DATEPART(HOUR,CreatedAt) = 23
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,CreatedAt))), dateadd(day, 0, datediff(day, -1, CreatedAt))) 
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,CreatedAt))), dateadd(day, 0, datediff(day,  0, CreatedAt))
						   )
						   ) ORDER BY CreatedAt DESC) Ranker
	   ,CAST(1 AS bigint) HourlyUserCountConstant
	FROM (select UserIdentifier,Currency,CreatedAt,AvailableBalance from Transactions2020Before.dbo.FACT_Transactions With (Nolock) 
			union all
		  select UserIdentifier,Currency,CreatedAt,AvailableBalance from		DWH_DataWareHouse_Workspace.dbo.FACT_Transactions With (Nolock)) L
	WHERE CreatedAt > @StartDate and CreatedAt <= @BaseDay
	),UserBalanceData_CTE AS
	(
		SELECT
		LC.UserIdentifier
	   ,LC.Currency
	   ,AvailableBalanceEod [AvailableBalance]
	   ,ContributedEndOfDayHour
	   ,CAST(1 as bigint) Ranker
	   ,CAST(NULL AS bigint) HourlyUserCountConstant
	FROM TransactionsData_CTE Lc 
	left JOIN DataWareHouse_Workspace.[DataWareHouse_Workspace\skacar].FACT_DailyEodUserBalances UB With (Nolock) ON Lc.Currency = UB.Currency AND Lc.UserIdentifier = UB.UserIdentifier
	), AvailableBalanceCTE_Transactions AS
	(
	SELECT * FROM TransactionsData_CTE
	UNION ALL
	SELECT * FROM UserBalanceData_CTE
	)
	,CrossJoinedDummyData_Transactions AS
	(
			SELECT 
				DH.HourlyDateTime [DateHour], UserIdentifier,Currency
			FROM (SELECT DISTINCT UserIdentifier,Currency FROM AvailableBalanceCTE_Transactions WHERE ContributedDateHour >= @StartDate AND ContributedDateHour < @BaseDay AND Ranker = 1) x
			CROSS JOIN #TempDateHours DH WITH (NOLOCK)
			WHERE DH.HourlyDateTime >= @StartDate AND DH.HourlyDateTime <= @BaseDay
	), CombiningDummyDataWithFundamental_Transactions AS
	(
			SELECT
				 CAR.DateHour
				,CAR.UserIdentifier
				,CAR.Currency
				,SK.AvailableBalance
				,HourlyUserCountConstant
			FROM CrossJoinedDummyData_Transactions CAR
	LEFT JOIN  AvailableBalanceCTE_Transactions SK ON SK.ContributedDateHour = CAR.DateHour AND  SK.UserIdentifier = CAR.UserIdentifier AND SK.Currency = CAR.Currency AND Ranker = 1
	)

	select * 
	INTO #ReadyToUpdateData_Transactions
	from CombiningDummyDataWithFundamental_Transactions order by [DateHour]

	
		UPDATE  #ReadyToUpdateData_Transactions
			SET AvailableBalance = 0
		FROM  #ReadyToUpdateData_Transactions
		WHERE AvailableBalance is null AND DateHour = @StartDate--dateadd(hour,1,@StartDate)

		WHILE (SELECT COUNT(DateHour) FROM  #ReadyToUpdateData_Transactions WHERE AvailableBalance IS NULL AND DateHour >= @StartDate AND DateHour <= @BaseDay) != 0
		BEGIN
		UPDATE R1
		set R1.AvailableBalance = R2.AvailableBalance    
		from  #ReadyToUpdateData_Transactions R1
		join  #ReadyToUpdateData_Transactions R2 on dateadd(HOUR,1,R2.DateHour) = R1.DateHour AND R1.UserIdentifier = R2.UserIdentifier AND R1.Currency = R2.Currency
		where  R1.AvailableBalance is null AND R2.AvailableBalance IS NOT NULL and R1.DateHour >= @StartDate AND R1.DateHour <= @BaseDay and R2.DateHour >= @StartDate AND R2.DateHour <= @BaseDay
		end
;
		WITH OutOfTxDayTotalBalance AS
		(
		SELECT Currency
		,SUM(AvailableBalanceEod) TotalBalanceOfOutOfDayUsers
		FROM DataWareHouse_Workspace.[DataWareHouse_Workspace\skacar].FACT_DailyEodUserBalances with (Nolock)
		WHERE ContributedEndOfDayHour <= @StartDate
		GROUP BY Currency
		)
		SELECT L.DateHour,L.Currency,SUM(l.AvailableBalance)+MAX(TotalBalanceOfOutOfDayUsers) TotalUserBalance
			,ISNULL( SUM(CASE WHEN HourlyUserCountConstant = 1 THEN l.AvailableBalance END),0)		TotalBalanceOfDailyActiveUsers
	--		COALESCE(SUM(CASE WHEN HourlyUserCountConstant = 1 THEN l.AvailableBalance END)*1.0/	(NULLIF(SUM(HourlyUserCountConstant),0)),0) AvgAvailableBalanceOfDailyActiveUsers

		INTO #A
		FROM #ReadyToUpdateData_Transactions L
		JOIN OutOfTxDayTotalBalance O ON L.Currency = O.Currency
		group by l.DateHour,L.Currency
		order by l.Currency,l.DateHour

;With SpreadOfDistributionsActiveUsersOnTime AS
		(
			SELECT
				UserIdentifier
			   ,Currency
			   ,DateHour
			   ,AvailableBalance
			   ,NTILE(4) OVER (PARTITION BY Currency, DateHour ORDER BY AvailableBalance) Quartiles
			FROM #ReadyToUpdateData_Transactions WHERE HourlyUserCountConstant = 1
		)
select
	 DateHour
	,Currency
--	,MIN(AvailableBalance)																							 MinimumActiveUsersOnTime
	,MAX(CASE WHEN Quartiles = 1 THEN AvailableBalance END)															 [1QuartileActiveUsersOnTime]
	,MAX(CASE WHEN Quartiles = 2 THEN AvailableBalance END)															 MedianActiveUsersOnTime
	,AVG(AvailableBalance)																							 MeanActiveUsersOnTime
	,MAX(CASE WHEN Quartiles = 3 THEN AvailableBalance END)															 [3QuartileActiveUsersOnTime]
	,MAX(CASE WHEN Quartiles = 3 THEN AvailableBalance END) - MAX(CASE WHEN Quartiles = 1 THEN AvailableBalance END) IQRActiveUsersOnTime
--	,MAX(AvailableBalance)																							 MaximumActiveUsersOnTime
	,STDEV(AvailableBalance)																						 StandardDeviationActiveUsersOnTime
	,COUNT(AvailableBalance)																						 UUActiveUsersOnTime
	,SUM(AvailableBalance)																							 TotalAvailableBalanceActiveUsersOnTime
INTO #B
from SpreadOfDistributionsActiveUsersOnTime
group by DateHour,Currency

;With SpreadOfDistributionsActiveUsersOnDay AS
		(
			SELECT
				UserIdentifier
			   ,Currency
			   ,DateHour
			   ,AvailableBalance
			   ,NTILE(4) OVER (PARTITION BY Currency, DateHour ORDER BY AvailableBalance) Quartiles
			FROM #ReadyToUpdateData_Transactions
		)
select
	 DateHour
	,Currency
--	,MIN(AvailableBalance)																							 MinimumActiveUsersOnDay
	,MAX(CASE WHEN Quartiles = 1 THEN AvailableBalance END)															 [1QuartileActiveUsersOnDay]
	,MAX(CASE WHEN Quartiles = 2 THEN AvailableBalance END)															 MedianActiveUsersOnDay
	,AVG(AvailableBalance)																							 MeanActiveUsersOnDay
	,MAX(CASE WHEN Quartiles = 3 THEN AvailableBalance END)															 [3QuartileActiveUsersOnDay]
	,MAX(CASE WHEN Quartiles = 3 THEN AvailableBalance END) - MAX(CASE WHEN Quartiles = 1 THEN AvailableBalance END) IQRActiveUsersOnDay
--	,MAX(AvailableBalance)																							 MaximumActiveUsersOnDay
	,STDEV(AvailableBalance)																						 StandardDeviationActiveUsersOnDay
	,COUNT(AvailableBalance)																						 UUActiveUsersOnDay
	,SUM(AvailableBalance)																							 TotalAvailableBalanceActiveUsersOnDay
INTO #C
from SpreadOfDistributionsActiveUsersOnDay
group by DateHour,Currency

--INSERT INTO DataWareHouse_Workspace.[DataWareHouse_Workspace\skacar].FACT_HourlyTotalUserBalances
select a.*
	--	,MinimumActiveUsersOnTime
		,[1QuartileActiveUsersOnTime]
		,MedianActiveUsersOnTime
		,MeanActiveUsersOnTime
		,[3QuartileActiveUsersOnTime]
		,IQRActiveUsersOnTime
--		,MaximumActiveUsersOnTime
		,StandardDeviationActiveUsersOnTime
		,UUActiveUsersOnTime
		,TotalAvailableBalanceActiveUsersOnTime
--		,MinimumActiveUsersOnDay
		,[1QuartileActiveUsersOnDay]
		,MedianActiveUsersOnDay
		,MeanActiveUsersOnDay
		,[3QuartileActiveUsersOnDay]
		,IQRActiveUsersOnDay
--		,MaximumActiveUsersOnDay
		,StandardDeviationActiveUsersOnDay
		,UUActiveUsersOnDay
		,TotalAvailableBalanceActiveUsersOnDay
--INTO DataWareHouse_Workspace.[DataWareHouse_Workspace\skacar].FACT_HourlyTotalUserBalances
from #A a 
JOIN #C c on c.Currency = a.Currency and c.DateHour = a.DateHour
left join #B b on b.Currency = a.Currency AND b.DateHour = a.DateHour
where a.DateHour > @StartDate
