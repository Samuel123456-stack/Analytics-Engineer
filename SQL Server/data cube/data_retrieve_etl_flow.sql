--CUBO DE DADOS
--Definição: 
  --O cubo de dados é uma estrutura multidimensional amplamente utilizada em sistemas de análise de dados, como Data Warehousing 
  --e Business Intelligence. Ele organiza informações em dimensões (como tempo, local, produto) e medidas (como vendas, lucros, quantidades), 
  --permitindo análises e consultas rápidas e complexas.
    
  --A principal característica do cubo de dados é sua capacidade de consolidar dados agregados de forma eficiente, 
  --proporcionando uma visão holística das informações. Essa estrutura possibilita a exploração dos dados sob diferentes perspectivas, 
  --facilitando a identificação de padrões, tendências e insights valiosos para a tomada de decisões estratégicas.

--Objetivo: Esquematizar uma estrutura de alta flexibilidade, provisionando um grau de liberdade ao cliente em alocar atributos
--categóricos e quantitativos em diferentes quadrantes, simulando uma planilha dinâmica em virtude das transposições em formato
--de (linha, coluna), facilitar a personalização dos templates.

--Clientes: Indústrias do ramo alimentício.

--TECNOLOGIAS UTILIZADAS

--Ferramentas de análise: Microsoft SQL Server 2019, SSIS (ETL)

--Resultados: 
  --1. Cenários com relatórios dinâmicos que viabilizam a extração de insights valiosos em diferentes formatos.
  --2. Aumento da satisfação do cliente pelo serviço prestado.

--------------------------------------------------------------------------------------------------------------------------------------

--1. Padroniza a clonagem da tabela de ciclo (de vendas) e verifica a existência de um novo registro

SELECT 
    CASE 
        WHEN EXISTS (SELECT * FROM Auxiliar.CicloProcesso WHERE Id = (
			SELECT
				MAX(Id)
			FROM CicloProcesso
		)) 
        THEN 1
        ELSE 0
    END AS Resultado
--------------------------------------------------------------------------------------------------------------

--2. Adiciona na tabela auxiliar o novo ciclo criado na tabela original.

DECLARE @columns NVARCHAR(MAX)
DECLARE @sql NVARCHAR(MAX)

SELECT @columns = 
	RTRIM(
		STUFF(
				(
					SELECT 
						    ', ' + QUOTENAME(column_name)
					FROM INFORMATION_SCHEMA.COLUMNS
					WHERE SCHEMA_NAME() = CURRENT_USER
					AND table_name = 'CicloProcesso'
					AND table_schema = 'Auxiliar'
					AND LOWER(is_nullable) = 'no'
							FOR xml PATH ('')
				), 1, 1, ''
			)
)


SET @sql = N'INSERT INTO Auxiliar.CicloProcesso (' + @columns + ') ' +
			'SELECT 
				Id, 
				Nome, 
				CriadoEm, 
				ModificadoEm, 
				Ativo, 
				CriticaBaseCalculo, 
				CanalId, 
				CicloMesAnoId ' +
			'FROM CicloProcesso ' +
			'WHERE Id = (
				SELECT 
					MAX(Id) 
				FROM CicloProcesso
			)';

EXEC sp_executesql @sql;

-----------------------------------------------------------------------------------------

--3. Coleta o ciclo recém cadastrado, juntamente com o mês e ano (periodo de inclusão)

SELECT
    MAX(t.CicloProcessoId) [CicloProcessoId],
	  CONCAT(LEFT(CAST(MesAno AS int), 1) + 1, RIGHT(MesAno, 4)) [MesAno]
FROM (
	SELECT
		  Id [CicloProcessoId],
		  Nome,
		  [MesAno] = RTRIM(MONTH(dtInclusao)) + LTRIM(DATEPART(YEAR, dtInclusao))
	FROM Auxiliar.CicloProcesso
	WHERE Id = (
		SELECT
			  MAX(Id)
		FROM Auxiliar.CicloProcesso 
	)
) t
GROUP BY CONCAT(
			LEFT(
				CAST(MesAno AS int), 
				1
			) + 1, 
			RIGHT(MesAno, 4)
		), CanalId

------------------------------------------------------------------------------------------------------------

CREATE PROCEDURE usp_mapeamento_tabelaHierarquia_4niveis        
(        
 @ID_Ciclo int,        
 @MesAno int        
)        
        
 AS        
  BEGIN        
        
   SET NOCOUNT ON        
           
   DECLARE @sql_fetchtables_engine nvarchar(max)        
   DECLARE @tables_controller nvarchar(max)        
   DECLARE @table_hierarchy nvarchar(55)        
   DECLARE @table_hierarchy2 nvarchar(55)        
   DECLARE @table_hierarchy3 nvarchar(55)        
   DECLARE @set_final_query nvarchar(max)        
           
   SET @tables_controller = N'        
    SELECT        
     cd.tb_critica_desdobramento,        
     canal_dist.tb_canal_distribuicao,        
     m.tb_material,        
     r.tb_regional,        
     f.tb_filial,        
     mc.tb_meta_consenso,        
  c.tb_cliente,    
  cdh1.tb_hierarquia,    
  cdh2.tb_hierarquia_2,    
  cdh3.tb_hierarquia_3    
    '        
        
   SET @table_hierarchy = N'''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_CriticaDesdobramentoHierarquia'''        
   SET @table_hierarchy2 = N'''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_CriticaDesdobramentoHierarquia2'''        
   SET @table_hierarchy3 = N'''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_CriticaDesdobramentoHierarquia3'''        
        
           
   SET @sql_fetchtables_engine = N'        
    FROM (        
     SELECT        
      OBJECT_NAME(object_id) [tb_critica_desdobramento]        
     FROM sys.objects        
     WHERE name = ''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_CriticaDesdobramento''        
    ) cd JOIN (        
     SELECT        
      OBJECT_NAME(object_id) [tb_canal_distribuicao]        
     from sys.objects        
     WHERE name = ''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_CanalDistribuicao''        
    ) canal_dist        
     ON        
      SUBSTRING(cd.tb_critica_desdobramento, 3, 5) = SUBSTRING(canal_dist.tb_canal_distribuicao, 3, 5)        
    JOIN (        
     SELECT        
      OBJECT_NAME(object_id) [tb_material]        
     FROM sys.objects        
     WHERE name = ''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_Material''        
    ) m        
     ON        
      SUBSTRING(canal_dist.tb_canal_distribuicao, 3, 5) = SUBSTRING(m.tb_material, 3, 5)        
    JOIN (        
     SELECT        
      OBJECT_NAME(object_id) [tb_regional]        
     FROM sys.objects        
     WHERE name = ''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_Diretoria''        
    ) r        
     ON        
      SUBSTRING(m.tb_material, 3, 5) = SUBSTRING(r.tb_regional, 3, 5)        
    JOIN (        
     SELECT        
      OBJECT_NAME(object_id) [tb_filial]        
     FROM sys.objects        
     WHERE name = ''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_Filial''        
    ) f        
     ON        
      SUBSTRING(r.tb_regional, 3, 5) = SUBSTRING(f.tb_filial, 3, 5)        
    JOIN (        
     SELECT        
      OBJECT_NAME(object_id) [tb_meta_consenso]        
     FROM sys.objects        
     WHERE name = ''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_MetaConsenso''        
    ) mc        
     ON        
      SUBSTRING(f.tb_filial, 3, 5) = SUBSTRING(mc.tb_meta_consenso, 3, 5)        
 JOIN (      
  SELECT      
   OBJECT_NAME(object_id) [tb_cliente]      
  FROM sys.objects      
  WHERE name = ''__' + CONVERT(varchar(5), @ID_Ciclo) + '_' + CONVERT(varchar(5), @MesAno) + '_Cliente''      
 ) c      
  ON      
   SUBSTRING(mc.tb_meta_consenso, 3, 5) = SUBSTRING(c.tb_cliente, 3, 5)      
    JOIN (        
     SELECT        
      OBJECT_NAME(object_id) [tb_hierarquia]        
     FROM sys.objects        
     WHERE name = ' + @table_hierarchy + '        
    ) cdh1         
  ON       
   SUBSTRING(cd.tb_critica_desdobramento, 3, 5) = SUBSTRING(cdh1.tb_hierarquia, 3, 5)        
    JOIN (        
     SELECT        
      OBJECT_NAME(object_id) [tb_hierarquia_2]        
     FROM sys.objects        
     WHERE name = ' + @table_hierarchy2 + '        
    ) cdh2         
     ON SUBSTRING(cd.tb_critica_desdobramento, 3, 5) = SUBSTRING(cdh2.tb_hierarquia_2, 3, 5)        
    JOIN (        
     SELECT        
      OBJECT_NAME(object_id) [tb_hierarquia_3]        
     FROM sys.objects        
     WHERE name = ' + @table_hierarchy3 + '        
    ) cdh3         
     ON SUBSTRING(cd.tb_critica_desdobramento, 3, 5) = SUBSTRING(cdh3.tb_hierarquia_3, 3, 5)        
   ';        

-- Variável para armazenar a SQL final
DECLARE @sql_final nvarchar(max)
  
-- Cria uma tabela temporária para armazenar o resultado das tabelas encontradas
IF OBJECT_ID('tempdb..#TabelasEncontradas') IS NOT NULL
	BEGIN
		DROP TABLE #TabelasEncontradas
	END

-- Gera os nomes das tabelas e insere na tabela temporária
SET @sql_final = '
	SELECT *
	INTO #TabelasEncontradas
	' + @sql_fetchtables_engine + '

	SELECT ''Tabela'' AS Tabela, ''Nome'' AS Nome -- placeholder apenas para structure
	WHERE 1 = 0
'

-- Executa a query que popula #TabelasEncontradas
EXEC sp_executesql @sql_final

-- Armazena os nomes das colunas (para unpivot dinâmico)
DECLARE @unpivot_cols nvarchar(max)

SELECT @unpivot_cols = STUFF((
        SELECT ',' + QUOTENAME(name)
        FROM tempdb.sys.columns
        WHERE object_id = OBJECT_ID('tempdb..#TabelasEncontradas')
        FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 1, '')
FROM tempdb.sys.columns
WHERE object_id = OBJECT_ID('tempdb..#TabelasEncontradas')

-- Executa a query de UNPIVOT dinâmico
SET @sql_final = '
	SELECT Tabela, Nome
	FROM (
		SELECT * FROM #TabelasEncontradas
	) AS src
	UNPIVOT (
		Nome FOR Tabela IN (' + @unpivot_cols + ')
	) AS unpvt
'

EXEC sp_executesql @sql_final
           
   SELECT @set_final_query = CONCAT(@tables_controller, @sql_fetchtables_engine)        
   EXEC sp_executesql @set_final_query        
  END

-----------------------------------------------------------------------------------------------------

--Desdobramento de features entre "dimensão", "fatos" e "scenario" --> (Implementação)

--CENÁRIO 1: **Tempo médio de execução da crítica**
--Descrição: Este cenário apresenta a média de tempo que cada usuário executa a critica, considerando o ciclo e o tipo de canal,
  --podendo ser agrupado a soma de tempo por perfil.

CREATE TABLE [dimensao].[critica_executor_historico](
	[sk_critica_historico] [int] IDENTITY(1,1) NOT NULL,
	[id_ciclo_processo] [int] NOT NULL,
	[id_hierarquia_comercial] [int] NOT NULL,
	[id_config_hierarquia] [int] NOT NULL,
	[cod_canal] [nvarchar](50) NOT NULL,
	[nome] [varchar](100) NULL,
	[cod_hierarquia] [varchar](50) NOT NULL,
	[nivel] [int] NOT NULL,
	[tipo_movimentacao] [varchar](50) NOT NULL,
	[valor_alocado] [float] NOT NULL,
	[dtInclusao] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[sk_critica_historico] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

--Query para popular tabela abaixo

INSERT INTO dimensao.critica_executor_historico
SELECT
	ch.cicloProcessoId,
	hc.Id [id_hierarquia_comercial],
	config.Id [id_config_hierarquia],
	c.codigo [cod_canal],
	hc.nome,
	hc.codigo [cod_hierarquia],
	hc.NivelAtual,
	ch.TipoMovimentacao,
	ch.ValorTotalMovimentado,
	ch.CriadoEm
FROM __43114_12024_CriticaHistorico ch
JOIN __43114_12024_HierarquiaComercial hc
ON
	hc.Id = ch.HierarquiaExecutorId
JOIN [DesdobramentoMetasConfig_BRF_V2]..ConfiguracaoHierarquia config
ON
	hc.ConfiguracaoHierarquiaId = config.Id
JOIN [DesdobramentoMetasConfig_BRF_V2]..Canal c
ON
	c.Id = config.CanalId

-------------------------------------------------------------------------------

CREATE TABLE [fatos].[tempo_critica] (
	[sk_critica_historico] [int] NOT NULL, --chave surrogada (artificial)
	[id_hierarquia_comercial] [int] NOT NULL,
	[id_config_hierarquia] [int] NOT NULL,
	[cod_canal] [nvarchar](50) NOT NULL,
	[TempoMedio] [time](7) NOT NULL,
	[total_alocacao] [int] NOT NULL,
	[total_remocao] [int] NOT NULL,
	[total_encerramento] [int] NOT NULL,
	[total_reabertura] [int] NOT NULL,
	[total_reinicializacao] [int] NOT NULL
) ON [PRIMARY]

--Query para popular tabela abaixo

INSERT INTO fatos.tempo_critica
	SELECT
		  r.CicloProcessoId [id_ciclo_processo],
		  r.HierarquiaComercialId,
		  r.ConfigHierarquiaId,
		  r.cod_canal,
		  CAST(
				  DATEADD(
						   ms, 
						   AVG(DATEDIFF(ms, '00:00:00', r.TempoMedio)), 
						   '00:00:00' 
					  ) 
				  AS time
		  ) [TempoMedioProcessamento],
		  MAX(r.qtd_Alocacao) qtd_Alocacao ,
		  MAX(r.qtd_Remocao) qtd_Remocao,
		  MAX(r.qtd_Encerramento) qtd_Encerramento,
		  MAX(r.qtd_Reabertura) qtd_Reabertura,
		  MAX(r.qtd_Reinicializacao) qtd_Reinicializacao
	FROM (
		SELECT
			  hist.CicloProcessoId,
			  hc.Id [HierarquiaComercialId],
			  ch.Id [ConfigHierarquiaId],
			  c.Codigo [cod_canal],
			  [TempoMedio] = CAST(MAX(hist.CriadoEm) - MIN(hist.CriadoEm) AS time),
			  [qtd_Alocacao] = IIF(TipoMovimentacao = 'Alocado Volume', COUNT(*), 0),
			  [qtd_Remocao] = IIF(TipoMovimentacao = 'Removido Volume', COUNT(*), 0),
			  [qtd_Encerramento] = IIF(TipoMovimentacao = 'Encerramento da Crítica', COUNT(*), 0),
			  [qtd_Reabertura] = IIF(TipoMovimentacao = 'Rebertura da Crítica', COUNT(*), 0),
			  [qtd_Reinicializacao] = IIF(TipoMovimentacao = 'Reinicialização da crítica por superior', COUNT(*), 0)
		FROM __43114_12024_CriticaHistorico hist
		JOIN __43114_12024_HierarquiaComercial hc
		ON
			hc.Id = hist.HierarquiaExecutorId
		JOIN [DesdobramentoMetasConfig_BRF_V2]..ConfiguracaoHierarquia ch
		ON
			hc.ConfiguracaoHierarquiaId = ch.Id
		JOIN [DesdobramentoMetasConfig_BRF_V2]..Canal c
		ON
			ch.CanalId = c.Id
		GROUP BY hc.Id, ch.Id, c.codigo, TipoMovimentacao, hist.CicloProcessoId
) r
	GROUP BY
		  r.CicloProcessoId,
		  r.HierarquiaComercialId,
		  r.ConfigHierarquiaId,
		  r.cod_canal

------------------------------------------------------------------------------------------------------------------------

CREATE TABLE [Scenario].[tempo_critica_perfil](
	[CicloProcessoId] [int] NOT NULL,
	[cod_hierarquia] [varchar](50) NOT NULL,
	[Canal] [nvarchar](50) NULL,
	[nome_hierarquia] [nvarchar](50) NOT NULL,
	[tempo_medio] [time](7) NOT NULL,
	[qtd_alocacao] [int] NOT NULL,
	[qtd_remocao] [int] NOT NULL,
	[qtd_encerramento] [int] NOT NULL,
	[qtd_reabertura] [int] NOT NULL,
	[qtd_reinicializacao] [int] NOT NULL
) ON [PRIMARY]

--Query que popula tabela abaixo

DECLARE @checa_canal_id int
DECLARE @ciclo_id int = ? --Variável dinâmica que armazena o valor do último ciclo criado

IF (SELECT 
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2].Auxiliar.CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id) = 'AS'

	BEGIN
		SET @checa_canal_id = 3
	END

IF(
	SELECT 
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2].Auxiliar.CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id) = 'AS (4 Níveis Execução)'

	BEGIN
		SET @checa_canal_id = 11
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2].Auxiliar.CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'Rota'

	BEGIN
		SET @checa_canal_id = 1
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2].Auxiliar.CicloProcesso cp
	ON
		c.Id =  cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'Food Estrategico'

	BEGIN
		SET @checa_canal_id = 4
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2]..CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'Kits'

	BEGIN
		SET @checa_canal_id = 5
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2]..CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'Kidelli'

	BEGIN
		SET @checa_canal_id = 6
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2]..CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'FVI'

	BEGIN
		SET @checa_canal_id = 7
	END

IF (
	SELECT
		C.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2]..CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'DISTRIBUIDOR'

	BEGIN
		SET @checa_canal_id = 8
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2]..CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
)  = 'AS (4 Níveis Execução) - PERD'

	BEGIN
		SET @checa_canal_id = 12
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2].Auxiliar.CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'AS (4 Níveis Execução) - SADI'

	BEGIN
		SET @checa_canal_id = 13
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2].Auxiliar.CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'Rota - PERD'

	BEGIN
		SET @checa_canal_id = 14
	END

IF (
	SELECT
		c.Nome
	FROM [DesdobramentoMetasConfig_BRF_V2]..Canal c
	JOIN [DesdobramentoMetasConfig_BRF_V2].Auxiliar.CicloProcesso cp
	ON
		c.Id = cp.CanalId
	WHERE cp.Id = @ciclo_id
) = 'Rota - SADI'

	BEGIN
		SET @checa_canal_id = 15
	END

SELECT
	  d.id_ciclo_processo [CicloProcessoId],
	  d.cod_hierarquia,
	  c.Nome [Canal],
	  config.NomeHierarquia [nome_hierarquia],
	  f.TempoMedio [tempo_medio],
	  total_alocacao [qtd_alocacao],
	  total_remocao [qtd_remocao],
	  total_encerramento [qtd_encerramento],
	  total_reabertura [qtd_reabertura],
	  total_reinicializacao [qtd_reinicializacao]
FROM fatos.tempo_critica f
JOIN [DesdobramentoMetasConfig_BRF_V2]..Canal c
ON
	f.cod_canal = c.Codigo
JOIN [DesdobramentoMetasConfig_BRF_V2]..ConfiguracaoHierarquia config
ON
	f.id_config_hierarquia = config.Id
JOIN dimensao.critica_executor_historico d
ON
	f.sk_critica_historico = d.sk_critica_historico
WHERE d.id_ciclo_processo = @ciclo_id
AND c.Id = @checa_canal_id

----------------------------------------------------------------------------------

--CENÁRIO 2: **Comparativo de meta (planejada, criticada e histórico)**
--Descrição: Este cenário apresenta o comparativo da meta que o sistema sugeriu (resultado da aplicação dos cálculos e de acordo
--com o usuário administrador), a meta criticada (meta onde hierarquia do cliente adicionou o ajuste na retirada e alocação dos valores)
--e valores do histórico relacionados à mesma abertura que o cálculo usou no desdobramento.

CREATE TABLE [dimensao].[desdobramento_hierarquia_meta](
	[sk_critica_desd] [int] IDENTITY(1,1) NOT NULL, --chave surrogada (artificial)
	[id_ciclo_processo] [int] NOT NULL, --código identificador do ciclo
	[id_meta] [int] NOT NULL, --código identificador da meta
	[cod_sku] [varchar](50) NOT NULL, --código do produto
	[cod_canal_dist] [varchar](50) NOT NULL, --código do canal de distribuição logística
	[cod_cliente] [varchar](50) NOT NULL, --código do cliente
	[cod_regional] [varchar](50) NOT NULL, --código regional
	[cod_filial] [varchar](50) NOT NULL, --código da filial
	[sku] [varchar](100) NULL, --nome do sku
	[canal_dist] [varchar](100) NULL, --nome do canal de distribuição
	[cliente] [varchar](100) NULL, --nome do cliente
	[regional] [varchar](100) NULL, --nome da regional
	[filial] [varchar](100) NULL, --nome da filial
	[vlr_meta] [float] NOT NULL, --valor calculada da meta
PRIMARY KEY CLUSTERED 
(
	[sk_critica_desd] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

--Query que popula tabela abaixo
	
SELECT
	  cd.CicloProcessoId,
	  mc.Id [id_meta],
	  m.Codigo [cod_sku],
	  canal.Codigo [cod_canal],
	  c.Codigo [cod_cliente],
	  r.Codigo [cod_regional],
	  f.Codigo [cod_filial],
	  m.Nome [nome_sku],
	  canal.Nome [nome_canal],
	  c.Nome [nome_cliente],
	  r.Nome [nome_regional],
	  f.Nome [nome_filial],
	  mc.VolumeMeta
FROM __43114_12024_CriticaDesdobramento cd
JOIN __43114_12024_CanalDistribuicao canal
ON
	cd.CanalDistribuicaoId = canal.Id
JOIN __43114_12024_Material m
ON
	cd.MaterialId = m.Id
JOIN __43114_12024_Diretoria r
ON
	cd.DiretoriaId = r.Id
JOIN __43114_12024_Filial f
ON
	cd.FilialId = f.Id
JOIN __43114_12024_MetaConsenso mc
ON
	cd.MetaConsensoId = mc.Id
JOIN __43114_12024_Cliente c
ON
	cd.ClienteId = c.Id

-------------------------------------------------------------------------------------------

CREATE TABLE [fatos].[meta_criticada](
	[sk_desd_meta] [int] NOT NULL,
	[id_meta] [int] NOT NULL,
	[id_ciclo_processo] [int] NOT NULL,
	[cod_sku] [varchar](50) NOT NULL,
	[cod_canal_dist] [varchar](50) NOT NULL,
	[cod_regional] [varchar](50) NOT NULL,
	[cod_filial] [varchar](50) NOT NULL,
	[desdobramento] [float] NOT NULL,
	[vlr_critica_1] [float] NULL,
	[vlr_critica_2] [float] NULL,
	[vlr_critica_3] [float] NULL,
	[vlr_critica_5] [float] NULL,
PRIMARY KEY CLUSTERED 
(
	[sk_desd_meta] ASC
)
--Query que popula tabela abaixo

SELECT
    t.CicloProcessoId [id_ciclo_processo],
	  t.id_meta,
    t.CodMaterial [cod_sku],
    t.CanalDistribuicao AS cod_canal_dist,
	  t.CodRegional [cod_regional],
    t.CodFilial [cod_filial],
    ROUND(SUM(t.VolumeDesdobramento), 2) AS desdobramento,
    SUM(t.ValorCriticaHier1) AS vlr_critica_1,
    SUM(t.ValorCriticaHier2) AS vlr_critica_2,
    SUM(t.ValorCriticaHier3) AS vlr_critica_3
FROM (
    SELECT
        cd.CicloProcessoId,
		mc.Id AS [id_meta],
        m.Codigo AS CodMaterial,
        canal.Codigo AS CanalDistribuicao,
        r.Codigo AS CodRegional,
		f.Codigo [CodFilial],
        cd.VolumeDesdobramento,
        cdh1.ValorCriticado AS ValorCriticaHier1,
        cdh2.ValorCriticado AS ValorCriticaHier2,
        cdh3.ValorCriticado AS ValorCriticaHier3
    FROM __43114_12024_CriticaDesdobramento cd
    LEFT JOIN __43114_12024_CanalDistribuicao canal ON cd.CanalDistribuicaoId = canal.Id
    JOIN __43114_12024_Material m ON cd.MaterialId = m.Id
    JOIN __43114_12024_Diretoria r ON cd.DiretoriaId = r.Id
    JOIN __43114_12024_Filial f ON cd.FilialId = f.Id
    JOIN __43114_12024_MetaConsenso mc ON cd.MetaConsensoId = mc.Id
    JOIN __43114_12024_Cliente c ON cd.ClienteId = c.Id
    LEFT JOIN __43114_12024_CriticaDesdobramentoHierarquia cdh1 ON cdh1.CriticaDesdobramentoId = cd.Id
    LEFT JOIN __43114_12024_CriticaDesdobramentoHierarquia2 cdh2 ON cdh2.CriticaDesdobramentoId = cd.Id
    LEFT JOIN __43114_12024_CriticaDesdobramentoHierarquia3 cdh3 ON cdh3.CriticaDesdobramentoId = cd.Id
) AS t
GROUP BY 
    t.CicloProcessoId,
	  t.id_meta,
    t.CodMaterial,
    t.CanalDistribuicao,
    t.CodRegional,
    t.CodFilial
------------------------------------------------------------------------------------------------------------------

CREATE TABLE [Scenario].[comparativo_meta_planejada](
	[CicloProcessoId] int NOT NULL, --Id do ciclo de vendas
	[cod_sku] varchar(50) NOT NULL, --Código do produto/material
	[canal_distribuicao] varchar(100) NULL, --Nome do canal de distribuição logística
	[regional] varchar(100) NULL, --Nome da regional
	[filial] varchar(100) NULL, --Nome da filial
	[meta] float NOT NULL, --Meta final calculada
	[desdobramento] float NOT NULL, --Desdobramento da meta entre cliente/sku
	[vlr_critica_1] float NULL, --Valor criticado pelo nivel 1 da hierarquia
	[vlr_critica_2] float NULL, --Valor criticado pelo nivel 2 da hierarquia
	[vlr_critica_3] float NULL, --Valor criticado pelo nivel 3 da hierarquia
	[vlr_critica_5] float NULL, --Valor criticado pelo nivel 4 da hierarquia
	[% meta_hierarquia] varchar(31) NULL,
	[% meta_hierarquia2] varchar(31) NULL,
	[% meta_hierarquia3] varchar(31) NULL,
	[% meta_hierarquia5] varchar(31) NULL
) ON [PRIMARY]

--Query que popula tabela abaixo

SELECT
	  d.id_ciclo_processo [CicloProcessoId],
	  d.cod_sku,
	  d.canal_dist [canal_distribuicao],
	  d.regional,
	  REPLACE(RIGHT(d.filial, LEN(d.filial) - CHARINDEX('-', d.filial)), '', '') [filial],
	  d.vlr_meta [meta],
	  f.desdobramento,
	  f.vlr_critica_1,
	  f.vlr_critica_2,
	  f.vlr_critica_3,
	  f.vlr_critica_5,
	  CONVERT(varchar, CAST(f.vlr_critica_1 / MIN(d.vlr_meta) * 100.00 AS decimal(5,2))) + '%' AS [% meta_hierarquia],
    CONVERT(varchar, CAST(f.vlr_critica_2 / MIN(d.vlr_meta) * 100.00 AS decimal(5,2))) + '%' AS [% meta_hierarquia2],
    CONVERT(varchar, CAST(f.vlr_critica_3 / MIN(d.vlr_meta) * 100.00 AS decimal(5,2))) + '%' AS [% meta_hierarquia3],
	  CONVERT(varchar, CAST(f.vlr_critica_5 / MIN(d.vlr_meta) * 100.00 AS decimal(5,2))) + '%' AS [% meta_hierarquia5]
FROM dimensao.desdobramento_hierarquia_meta d
WHERE d.id_ciclo_processo = ?
JOIN fatos.meta_criticada f
ON
	d.sk_critica_desd = f.sk_desd_meta
GROUP BY
	  d.id_ciclo_processo,
	  d.cod_sku,
	  d.canal_dist,
	  d.regional,
	  d.filial,
	  d.vlr_meta,
	  f.desdobramento,
	  f.vlr_critica_1,
	  f.vlr_critica_2,
	  f.vlr_critica_3,
	  f.vlr_critica_5

---------------------------------------------------------------------------------------

--CENÁRIO 3: **Filtros utilizados durante a critica por usuários**
--Descrição: Este cenário apresenta os nomes dos filtros que cada usuário por perfil utilizou durante a execução da critica.

CREATE TABLE [dimensao].[critica_filtro](
	[sk_filtro] [int] IDENTITY(1,1) NOT NULL,
	[id_config_filtro] [int] NOT NULL, --Id de configuração do ciclo
	[id_ciclo_processo] [int] NOT NULL, --Id do ciclo de vendas
	[nome_executor] [varchar](100) NULL, --Nome do executor de critica
	[cod_hierarquia] [varchar](50) NOT NULL, --Chave da posição hierárquica
	[nome_hierarquia] [varchar](100) NULL, --Nome da hierarquia (Gerente, Coordenador, Supervisor, etc.)
	[nome_filtro] [varchar](50) NOT NULL, --Nome da coluna usada como filtro
	[cod_filtro] [varchar](50) NOT NULL, --Chave do filtro
	[CanalId] [int] NOT NULL, --Id do canal de vendas
	[nivel] [int] NOT NULL, --Nivel da hierarquia
	[valor_filtro] [varchar](15) NOT NULL, --Valor da coluna filtrada
PRIMARY KEY CLUSTERED 
(
	[sk_filtro] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

--Query que popula tabela acima

SELECT
	ccf.Id [id_config_filtro],
	ch.CicloProcessoId,
	chf.Nome [nome_executor],
	ch.CodigoHierarquia,
 	config.NomeHierarquia,
	COALESCE(NomeChaveTabelaMaterial, NomeChaveTabelaCritica, TRIM(' ')) [nome_filtro],
	ccf.Codigo [cod_filtro],
	config.CanalId,
	ch.NivelHierarquia,
	chf.ValorFiltro
FROM __43114_12024_CriticaHierarquia ch
JOIN __43114_12024_CriticaHierarquiaFiltro chf
ON
	ch.CodigoHierarquia = chf.Codigo
JOIN [DesdobramentoMetasConfig_BRF_V2].Auxiliar.CicloProcesso cp 
ON
	ch.CicloProcessoId = cp.Id
JOIN [DesdobramentoMetasConfig_BRF_V2]..ConfiguracaoCriticaFiltro ccf
ON 
	chf.ConfiguracaoCriticaFiltroId = ccf.Id
JOIN [DesdobramentoMetasConfig_BRF_V2]..ConfiguracaoHierarquia config
ON
	(cp.CanalId = config.CanalId AND ch.NivelHierarquia = config.NivelHierarquia)

-----------------------------------------------------------------------------------------------

CREATE TABLE [fatos].[total_filtro_critica](
	[sk_filtro] [int] NOT NULL,
	[id_ciclo_processo] [int] NOT NULL, --Id co ciclo de vendas
	[id_config_filtro] [int] NOT NULL, --Id de configuração do filtro
	[total] [int] NOT NULL --Frequência de cada feature selecionada como filtro
) ON [PRIMARY]

--Query que popula tabela acima

SELECT
	ccf.Id [id_config_filtro],
	chf.CicloProcessoId [id_ciclo_processo],
	[total_filtro] = COUNT_BIG(*)
FROM [DesdobramentoMetasConfig_BRF_V2]..ConfiguracaoCriticaFiltro ccf
JOIN __43114_12024_CriticaHierarquiaFiltro chf
ON
	ccf.Id = chf.ConfiguracaoCriticaFiltroId
GROUP BY
	ccf.Id,
	chf.CicloProcessoId

----------------------------------------------------------------------------------------

CREATE TABLE [Scenario].[filtro_critica](
	[CicloProcessoid] [int] NOT NULL, --Id do ciclo de vendas
	[cod_hierarquia] [varchar](50) NOT NULL, --Chave dos perfis executores de critica
	[nome_hierarquia] [varchar](100) NULL, --Nome da posição da hierarquia
	[nome_filtro] [varchar](50) NOT NULL, --Nome do atributo filtrado
	[canal] [nvarchar](50) NULL, --Nome do canal de vendas
	[valor_filtro] [varchar](15) NOT NULL, --Valor da coluna filtrada
	[total] [int] NOT NULL --Frequência de cada feature selecionada como filtro
) ON [PRIMARY]

--Query que popula tabela acima

SELECT
	d.id_ciclo_processo [CicloProcessoid],
	d.cod_hierarquia,
	d.nome_hierarquia,
	d.nome_filtro,
	c.nome [canal],
	d.valor_filtro,
	f.total
FROM dimensao.critica_filtro d
JOIN fatos.total_filtro_critica f
ON
	d.sk_filtro = f.sk_filtro
JOIN [DesdobramentoMetasConfig_BRF_V2]..Canal c
ON
	d.CanalId = C.Id
WHERE d.id_ciclo_processo = ?

--------------------------------------------------------------------------------------
--CENÁRIO 4: **Explicação - Meta Vendedor**
--Descrição: Este cenário apresenta o fluxo de cálculo que o sistema executou, mostrando as médias, valores de consenso,
--histórico, pesos, nome do cálculo, benefícios e/ou recompensas concedidas como reconhecimento pelo alcance de metas,
--indicadores de performance, e obrigações atribuídas ao vendedor como metas desafiadoras e prazos apertados.

CREATE TABLE [dimensao].[Historico_Critica](
	[sk_hist_vend] [int] IDENTITY(1,1) NOT NULL,
	[id_ciclo_processo] [int] NOT NULL, --Id do ciclo de vendas
	[cod_vendedor] [varchar](50) NOT NULL, --Chave do vendedor
	[cod_material] [varchar](50) NOT NULL, --Código do produto/material
	[cod_cliente] [varchar](50) NOT NULL, --Código do cliente (restaurantes, mercados, atacadistas, padarias, etc.)
	[vlr_meta] [float] NOT NULL, --Valor final calculado da meta
	[FormulaMediaCalculo] [varchar](50) NULL, --Formula que dá origem às medidas
	[dtInclusao] [datetime] NULL, --Data de inserção dos dados
PRIMARY KEY CLUSTERED 
(
	[sk_hist_vend] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

--Query que popula tabela acima

SELECT
	cd.CicloProcessoId,
	SUBSTRING(hc.Codigo, 1, CHARINDEX('_', hc.Codigo) -1) [cod_vendedor],
	m.Codigo [cod_material],
	REPLACE(SUBSTRING(hc.Codigo, CHARINDEX('_', hc.Codigo), 15), '_', TRIM(' ')) [cod_cliente],
	mc.VolumeMeta,
	hcr.FormulaMediaCalculo,
	mc.CriadoEm
FROM __43114_12024_HierarquiaCliente hc
INNER JOIN __43114_12024_Cliente c
ON
	REPLACE(SUBSTRING(hc.Codigo, CHARINDEX('_', hc.Codigo), 15), '_', '') = c.Codigo
INNER JOIN __43114_12024_CriticaDesdobramento cd
ON
	c.Id = cd.ClienteId
JOIN __43114_12024_Material m
ON
	cd.MaterialId = m.Id
JOIN __43114_12024_MetaConsenso mc
ON
	cd.MetaConsensoId = mc.Id
JOIN __43114_12024_HistoricoCriticaRegional hcr
ON
	hcr.MaterialId = m.Id AND hcr.ClienteId = c.Id

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE [fatos].[meta_vendedor](
	[sk_hist_vend] [int] NOT NULL, --Chave estraingeira (não exclusiva)
	[id_ciclo_processo] [int] NOT NULL, --Id do ciclo de vendas
	[cod_sku] [varchar](50) NOT NULL, --Código do produto/material
	[cod_vendedor] [varchar](50) NOT NULL, --Chave do vendedor
	[meta_final] [float] NOT NULL, --Meta final calculada
	[qtde_ult_mes] [float] NOT NULL, --Total vendido mês anterior
	[qtde_ult_ano] [float] NOT NULL, --Total vendido no último ano
	[MediaUlt3Meses] [float] NOT NULL, --Média acumulada em vendas nos últimos 3 meses
	[ultima_critica] [float] NOT NULL, --Último reajuste efetuado de movimentações
	[media_hist_calc] [float] NOT NULL,
	[vlr_critica_1] [float] NULL, --Total de alocações efetivadas pelo nível 1 da hierarquia
	[vlr_critica_2] [float] NULL, --Total de alocações efetivadas pelo nível 2 da hierarquia
	[vlr_critica_3] [float] NULL, --Total de alocações efetivadas pelo nível 3 da hierarquia
	[vlr_critica_5] [float] NULL, --Total de alocações efetivadas pelo nível 4 da hierarquia
	[ValorRepresentatividade] [float] NULL,
	[ValorBalanceamento] [float] NULL
) ON [PRIMARY]

--Query que popula tabela acima

SELECT
	hcr.CicloProcessoId,
	temp.cod_sku,
	temp.cod_vendedor,
	temp.vlr_meta [meta_final],
	hcr.VolumeUltimoMes [QtdUltimoMes],
	hcr.VolumeUltimoAno [QtdUltimoAno],
	hcr.MediaUltimos3Meses [MediaUltimos3M],
	hcr.UltimoValorCritica [UltimaCritica],
	hcr.MediaHistoricoCalculo [MediaHistoricoCalculo],
	ISNULL(MAX(vlr_critica_1), 0) [vlr_critica_1],
	ISNULL(MAX(vlr_critica_2), 0) [vlr_critica_2],
	ISNULL(MAX(vlr_critica_3), 0) [vlr_critica_3],
	ISNULL(MAX(vlr_critica_5), 0) [vlr_critica_5],
	SUM(VolumeRepresentatividade) [ValorRepresentatividade],
	SUM(VolumeBalanceamento) [ValorBalanceamento]
FROM __43114_12024_HistoricoCriticaRegional hcr
INNER JOIN (
	SELECT	
		dhm.id_meta,
		SUBSTRING(hc.Codigo, 1, CHARINDEX('_', hc.Codigo) -1) [cod_vendedor],
		COALESCE(CONVERT(bigint, m.Id), 0, 'Error') [MaterialId],
		dhm.cod_sku,
		dhm.cod_cliente,
		dhm.vlr_meta
	FROM dimensao.Desdobramento_Hierarquia_Meta dhm
	JOIN __43114_12024_HierarquiaCliente hc
	ON
		dhm.cod_cliente = REPLACE(SUBSTRING(hc.Codigo, CHARINDEX('_', hc.Codigo), 8), '_', '')
	JOIN __43114_12024_Material m
	ON
		dhm.cod_sku = m.Codigo
) [temp]
	ON hcr.MaterialId = temp.MaterialId AND hcr.CodigoCliente = temp.cod_cliente
JOIN (
	SELECT	
		dm.MetaConsensoId [id_meta],
		f.vlr_critica_1,
		f.vlr_critica_2,
		f.vlr_critica_3,
		f.vlr_critica_5,
		dm.VolumeRepresentatividade,
		dm.VolumeBalanceamento
	FROM __43114_12024_DesdobramentoMeta dm
	JOIN fatos.meta_criticada f
	ON
		dm.MetaConsensoId = f.id_meta
) dm 
	ON
		temp.id_meta = dm.id_meta
GROUP BY
	hcr.CicloProcessoId,
	hcr.MediaHistoricoCalculo,
	temp.cod_sku,
	temp.cod_vendedor,
	temp.vlr_meta,
	hcr.VolumeUltimoMes,
	hcr.VolumeUltimoAno,
	hcr.MediaUltimos3Meses,
	hcr.UltimoValorCritica,
	hcr.MediaHistoricoCalculo
