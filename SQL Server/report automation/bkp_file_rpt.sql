--Objetivo: Automatizar a atualização referente aos últimos backups das bases principais da "Oi", servindo como relatório de acompanhamento para exercer ações rápidas,
--caso surjam irregularidades de extrema gravidade que traz um prejuízo extemporâneo, atrapalhando a rotina de atualização.

--Resultado: 
    --Entrega contínua de relatórios com informações recentes sobre os backups, 
    --Garantia de segurança,
    --Garantia de maior controle
    --Confiabilidade nos dados extraídos.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

DECLARE @Databases VARCHAR(50)
DECLARE @SQLCmd NVARCHAR(MAX)


IF EXISTS (
	SELECT * FROM sys.objects o 
	WHERE object_id = OBJECT_ID(N'[dbacorp_maintenance]..[Relatorio_Backup]') 
	AND o.type = N'U'
	)
	BEGIN
		TRUNCATE TABLE Relatorio_Backup
	END


DECLARE db_cursor CURSOR FOR
SELECT 
  DB_NAME(database_id) 
FROM sys.databases
WHERE database_id NOT IN (
  DB_ID(N'master'), DB_ID(N'tempdb'), DB_ID(N'model'), DB_ID(N'Dashboard_OI_Homolog'), 
  DB_ID(N'SSISDB'), DB_ID(N'Monitor')
) AND state_desc = 'ONLINE'
	ORDER BY name ASC

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @Databases
	WHILE @@FETCH_STATUS = 0
		BEGIN

			SET @SQLCmd = N'INSERT INTO [dbacorp_maintenance]..Relatorio_Backup
				SELECT  bs.database_name,
				        CONVERT(VARCHAR, CAST((bs.backup_size / 1048576) AS DECIMAL(10,2))) [backup_size_MB],
				        bs.backup_start_date,
				        [Tipo de Backup] = CASE WHEN type = ''I'' THEN ''DIFF'' ELSE ''FULL'' END,
				        bmf.physical_device_name
				FROM  msdb.dbo.backupmediafamily bmf
				JOIN msdb.dbo.backupmediaset bms ON bmf.media_set_id = bms.media_set_id
				JOIN msdb.dbo.backupset bs ON bms.media_set_id = bs.media_set_id
				WHERE --bs.[type] IN (''D'')
				 bs.is_copy_only = 0 AND bs.database_name = ''' + @Databases + '''
				AND backup_start_date IN (
					SELECT MAX(backup_start_date) FROM msdb.dbo.backupset WHERE type = ''I''
					AND database_name = ''' + @Databases + '''
				) OR backup_start_date IN (
					SELECT MAX(backup_start_date) FROM msdb.dbo.backupset WHERE type = ''D''
					AND database_name = ''' + @Databases + '''
				)
				ORDER BY bs.backup_start_date DESC'	

		EXEC (@SQLCmd)

FETCH NEXT FROM db_cursor INTO @Databases  --Passa o cursor para a próxima linha                              
		END                              
 CLOSE db_cursor  --Encerra o cursor                         
 DEALLOCATE db_cursor  --Desaloca cursor
