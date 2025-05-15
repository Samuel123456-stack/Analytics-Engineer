--Cliente: Telecomunicações Oi -->
--Call Centers: Empresa Prestadora de Serviços (EPS)

--Descrição do cenário: A Oi envia, diariamente uma certa quantidade de mailings, constando as seguintes informações:
    --ID do mailing
    --Número de leads para discagem
    --Nome da Eps (Call Center) que irá coletar os mailings e realizar chamadas com base no tipo de alavanca: Aquisição, Rentabilização, Blindagem, Pesquisa, Retenção e Relacionamento
    --ID da campanha
    --Nome do produto a ser vendido
    --Chave como formato de uso exclusivo para atendentes que fazem a discagem de leads. É interpretado como um recurso que contabiliza a devolutiva das chamadas,
    --abolindo redundâncias, caso sejam encontradas.

--Objetivo: Aprimorar a estrutura da query, melhorando a efetividade da eliminação de duplicidades, usando como critério o "cod_chave".
--Resultado: Mais congruência e melhor eficácia na integridade dos dados, mobilizando mais agilidade de modo que previna interceptações indevidas 
--através do paralelismo de transações simultâneas, e redução imprescindível de custos operacionais.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--#####################################
--1. Cria tabela temporária
--#####################################

SET NOCOUNT ON

DECLARE @TabelaControleBaseB VARCHAR(MAX)
DECLARE @COD_MAILING VARCHAR(20)
DECLARE @MAX_DATE DATETIME
DECLARE @MIN_DATE DATETIME
--DECLARE @QtdeRepetidos INT

IF OBJECT_ID('TempDB..#BaseB') IS NOT NULL
	DROP TABLE #BaseB

CREATE TABLE #BaseB 
(
DbmId					uniqueidentifier,
DataInput				datetime,
EpsId					uniqueidentifier,
ValidationKey			varchar	(55),
cod_mailing				varchar	(12),
cod_chave				varchar (50),	
ddd_telefone_discado	bigint,	
cod_retorno				bigint,	
nome_produto			varchar	(30),
dt_evento				date,	
hr_evento_ini			time,	
hr_evento_fim			time,	
matr_atend				varchar	(50),
matr_sup				varchar	(50),
cdBaseB int identity(1,1) NOT NULL
);


--#############################
--2. Coleta chaves duplicadas
--#############################


IF EXISTS(SELECT * FROM tempdb..sysobjects WHERE name = 'TMP_BaseB_Duplicados')
			DROP TABLE TempDB..TMP_BaseB_Duplicados --Remove da Base

	SELECT 
		cod_mailing, 
		MIN(DataMinima) DataMinima, 
		MAX(DataMaxima) DataMaxima, 
		SUM(QtdDuplicados) QtdDuplicados
	INTO  TempDB..TMP_BaseB_Duplicados 
	FROM (
		-- Obtenção dos Mailing Duplicados
		SELECT 
      cod_mailing, 
			MIN(DataInput) DataMinima, 
			MAX(DataInput) DataMaxima, 
			COUNT(*) QtdDuplicados
		FROM BaseB_Consolidado WITH (NOLOCK)
		-- Somente Mailings recebidos no dia anterior.
		WHERE DataInput BETWEEN CAST(GETDATE() -1 AS date) AND CAST(GETDATE() AS date)
		GROUP BY cod_mailing, cod_chave,dt_evento,hr_evento_ini,hr_evento_fim
		HAVING COUNT(*) > 1
		) a
		GROUP BY cod_mailing


DECLARE chave_cursor CURSOR FOR
SELECT cod_mailing, DataMinima, DataMaxima
FROM tempdb..TMP_BaseB_Duplicados
ORDER BY cod_mailing

OPEN chave_cursor
FETCH NEXT FROM chave_cursor INTO @cod_mailing, @min_date, @max_date

WHILE @@FETCH_STATUS = 0
BEGIN

--########################
--3. MAPEIA AS BASEB_MV
--########################

DECLARE @Statement NVARCHAR(max)
DECLARE @StrMinDate VARCHAR(1000)
DECLARE @StrMaxDate VARCHAR(1000)


	SELECT @TabelaControleBaseB = TabelaControleBaseB FROM Engine WHERE cod_mailing = @cod_mailing
	
	SET @StrMinDate = CONVERT(VARCHAR(30), @Min_Date, 109)
	SET @StrMaxDate = CONVERT(VARCHAR(30), @Max_Date, 109)

	EXEC ('INSERT INTO #BaseB (DbmId,DataInput,EpsId,ValidationKey,cod_mailing,cod_chave,ddd_telefone_discado,cod_retorno,
			nome_produto,dt_evento,hr_evento_ini,hr_evento_fim,matr_atend,matr_sup)
			SELECT 
				DbmId,
				DataInput,
				EpsId,
				ValidationKey,
				cod_mailing,
				cod_chave,
				ddd_telefone_discado,
				cod_retorno,
				nome_produto,
				dt_evento,
				hr_evento_ini,
				hr_evento_fim,
				matr_atend,
				matr_sup 
			FROM ' + @TabelaControleBaseB + ' 
			WHERE DataInput >= ''' + @Min_Date + ''' AND DataInput <= ''' + @Max_Date + ''''
		)

--#################################
--4. EXCLUI AS CHAVES DUPLICADAS
--#################################

	SET @Statement = N'
	;WITH CTE AS (
		SELECT 
			cod_chave,
			ROW_NUMBER() OVER (PARTITION BY cod_mailing, cod_chave ORDER BY (SELECT 0)) AS Duplicado_QTD
		FROM ' + @TabelaControleBaseB + ' WHERE DataInput BETWEEN ''' + @StrMinDate + ''' AND ''' + @StrMaxDate + '''
	)
	
		DELETE FROM CTE WHERE Duplicado_QTD > 1;
	
	'

	EXEC(@Statement)
	
	DELETE FROM BaseB_Consolidado  
	WHERE cod_mailing = @cod_mailing AND
		DataInput >= @Min_Date AND 
		DataInput < @Max_Date 

	 

	FETCH NEXT FROM chave_cursor INTO @cod_mailing, @min_date, @max_date

END 


CLOSE chave_cursor
DEALLOCATE chave_cursor
