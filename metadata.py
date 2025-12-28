"""
Gestión de tabla de metadatos para control de sincronización
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from db import DatabaseConnection
from config import Config, TableSyncConfig

logger = logging.getLogger(__name__)


class SyncMetadataManager:
    """Gestiona la tabla de metadatos de sincronización en destino"""
    
    def __init__(self, dest_db: DatabaseConnection):
        self.db = dest_db
        self.table_name = f"[{Config.METADATA_SCHEMA}].[{Config.METADATA_TABLE_NAME}]"
        self._ensure_metadata_table()
    
    def _ensure_metadata_table(self):
        """Crea la tabla de metadatos si no existe"""
        create_script = f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{Config.METADATA_SCHEMA}' 
            AND t.name = '{Config.METADATA_TABLE_NAME}'
        )
        BEGIN
            CREATE TABLE {self.table_name} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                schema_name NVARCHAR(128) NOT NULL,
                table_name NVARCHAR(128) NOT NULL,
                
                -- Clave primaria detectada/configurada
                primary_key_columns NVARCHAR(MAX),
                pk_auto_detected BIT DEFAULT 1,
                
                -- Filtros aplicados
                where_clause NVARCHAR(MAX),
                
                -- Detección de cambios
                change_detection_strategy NVARCHAR(50),
                rowversion_column NVARCHAR(128),
                last_rowversion_synced BINARY(8),
                last_hash_synced NVARCHAR(64),
                
                -- Control de sincronización
                last_sync_date DATETIME2,
                last_sync_status NVARCHAR(50),
                records_inserted INT DEFAULT 0,
                records_updated INT DEFAULT 0,
                records_deleted INT DEFAULT 0,
                
                -- Errores
                last_error_message NVARCHAR(MAX),
                last_error_date DATETIME2,
                
                -- Auditoría
                created_date DATETIME2 DEFAULT GETDATE(),
                modified_date DATETIME2 DEFAULT GETDATE(),
                
                CONSTRAINT UQ_SyncMetadata_Table UNIQUE (schema_name, table_name)
            );
            
            CREATE INDEX IX_SyncMetadata_LastSync 
            ON {self.table_name}(last_sync_date DESC);
        END
        """
        
        try:
            self.db.execute_non_query(create_script)
            logger.info(f"Tabla de metadatos {self.table_name} verificada/creada")
        except Exception as e:
            logger.error(f"Error al crear tabla de metadatos: {e}")
            raise
    
    def get_metadata(self, schema: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Obtiene metadatos de sincronización para una tabla"""
        query = f"""
            SELECT 
                id,
                schema_name,
                table_name,
                primary_key_columns,
                pk_auto_detected,
                where_clause,
                change_detection_strategy,
                rowversion_column,
                last_rowversion_synced,
                last_hash_synced,
                last_sync_date,
                last_sync_status,
                records_inserted,
                records_updated,
                records_deleted,
                last_error_message,
                last_error_date,
                created_date,
                modified_date
            FROM {self.table_name}
            WHERE schema_name = ? AND table_name = ?
        """
        
        rows = self.db.execute_query(query, (schema, table_name))
        
        if not rows:
            return None
        
        row = rows[0]
        return {
            'id': row.id,
            'schema_name': row.schema_name,
            'table_name': row.table_name,
            'primary_key_columns': row.primary_key_columns,
            'pk_auto_detected': row.pk_auto_detected,
            'where_clause': row.where_clause,
            'change_detection_strategy': row.change_detection_strategy,
            'rowversion_column': row.rowversion_column,
            'last_rowversion_synced': row.last_rowversion_synced,
            'last_hash_synced': row.last_hash_synced,
            'last_sync_date': row.last_sync_date,
            'last_sync_status': row.last_sync_status,
            'records_inserted': row.records_inserted,
            'records_updated': row.records_updated,
            'records_deleted': row.records_deleted,
            'last_error_message': row.last_error_message,
            'last_error_date': row.last_error_date,
            'created_date': row.created_date,
            'modified_date': row.modified_date
        }
    
    def upsert_metadata(self, config: TableSyncConfig, 
                       change_detection_strategy: str = None,
                       rowversion_column: str = None):
        """Inserta o actualiza metadatos de configuración"""
        
        pk_columns_str = ','.join(config.primary_key_columns) if config.primary_key_columns else None
        
        query = f"""
            MERGE {self.table_name} AS target
            USING (SELECT ? AS schema_name, ? AS table_name) AS source
            ON target.schema_name = source.schema_name 
               AND target.table_name = source.table_name
            WHEN MATCHED THEN
                UPDATE SET
                    primary_key_columns = ?,
                    pk_auto_detected = ?,
                    where_clause = ?,
                    change_detection_strategy = ?,
                    rowversion_column = ?,
                    modified_date = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (schema_name, table_name, primary_key_columns, 
                       pk_auto_detected, where_clause, 
                       change_detection_strategy, rowversion_column)
                VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        
        params = (
            config.schema, config.table_name,
            pk_columns_str, config.pk_auto_detected, config.where_clause,
            change_detection_strategy, rowversion_column,
            config.schema, config.table_name, pk_columns_str,
            config.pk_auto_detected, config.where_clause,
            change_detection_strategy, rowversion_column
        )
        
        self.db.execute_non_query(query, params)
        logger.debug(f"Metadatos actualizados para {config.full_name}")
    
    def update_sync_status(self, schema: str, table_name: str,
                          status: str, 
                          records_inserted: int = 0,
                          records_updated: int = 0,
                          records_deleted: int = 0,
                          error_message: str = None):
        """Actualiza el estado de sincronización"""
        
        if error_message:
            query = f"""
                UPDATE {self.table_name}
                SET 
                    last_sync_date = GETDATE(),
                    last_sync_status = ?,
                    records_inserted = records_inserted + ?,
                    records_updated = records_updated + ?,
                    records_deleted = records_deleted + ?,
                    last_error_message = ?,
                    last_error_date = GETDATE(),
                    modified_date = GETDATE()
                WHERE schema_name = ? AND table_name = ?
            """
            params = (status, records_inserted, records_updated, records_deleted,
                     error_message, schema, table_name)
        else:
            query = f"""
                UPDATE {self.table_name}
                SET 
                    last_sync_date = GETDATE(),
                    last_sync_status = ?,
                    records_inserted = records_inserted + ?,
                    records_updated = records_updated + ?,
                    records_deleted = records_deleted + ?,
                    last_error_message = NULL,
                    modified_date = GETDATE()
                WHERE schema_name = ? AND table_name = ?
            """
            params = (status, records_inserted, records_updated, records_deleted,
                     schema, table_name)
        
        self.db.execute_non_query(query, params)
    
    def update_rowversion_synced(self, schema: str, table_name: str, 
                                rowversion: bytes):
        """Actualiza el último rowversion sincronizado"""
        query = f"""
            UPDATE {self.table_name}
            SET 
                last_rowversion_synced = ?,
                modified_date = GETDATE()
            WHERE schema_name = ? AND table_name = ?
        """
        
        self.db.execute_non_query(query, (rowversion, schema, table_name))
    
    def update_hash_synced(self, schema: str, table_name: str, 
                          hash_value: str):
        """Actualiza el último hash sincronizado"""
        query = f"""
            UPDATE {self.table_name}
            SET 
                last_hash_synced = ?,
                modified_date = GETDATE()
            WHERE schema_name = ? AND table_name = ?
        """
        
        self.db.execute_non_query(query, (hash_value, schema, table_name))
    
    def get_sync_summary(self) -> list:
        """Obtiene resumen de todas las sincronizaciones"""
        query = f"""
            SELECT 
                schema_name,
                table_name,
                change_detection_strategy,
                last_sync_date,
                last_sync_status,
                records_inserted,
                records_updated,
                records_deleted,
                last_error_message
            FROM {self.table_name}
            ORDER BY last_sync_date DESC
        """
        
        rows = self.db.execute_query(query)
        
        summary = []
        for row in rows:
            summary.append({
                'schema': row.schema_name,
                'table': row.table_name,
                'strategy': row.change_detection_strategy,
                'last_sync': row.last_sync_date,
                'status': row.last_sync_status,
                'inserted': row.records_inserted,
                'updated': row.records_updated,
                'deleted': row.records_deleted,
                'error': row.last_error_message
            })
        
        return summary
    
    def reset_table_metadata(self, schema: str, table_name: str):
        """Resetea los contadores de una tabla (útil para resincronización completa)"""
        query = f"""
            UPDATE {self.table_name}
            SET 
                last_rowversion_synced = NULL,
                last_hash_synced = NULL,
                records_inserted = 0,
                records_updated = 0,
                records_deleted = 0,
                last_error_message = NULL,
                last_error_date = NULL,
                modified_date = GETDATE()
            WHERE schema_name = ? AND table_name = ?
        """
        
        self.db.execute_non_query(query, (schema, table_name))
        logger.info(f"Metadatos reseteados para [{schema}].[{table_name}]")
