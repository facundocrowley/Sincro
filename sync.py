"""
Módulo de sincronización incremental de datos
Implementa solo INSERT de registros nuevos (no UPDATE ni DELETE)
"""

import logging
from typing import List, Dict, Any, Optional
from db import DatabaseConnection
from config import TableSyncConfig, Config
from metadata import SyncMetadataManager
from schema import SchemaExtractor, create_table_as_mirror

logger = logging.getLogger(__name__)


class TableSynchronizer:
    """Sincronizador de datos de tabla individual"""
    
    def __init__(self, source_db: DatabaseConnection, 
                 dest_db: DatabaseConnection,
                 config: TableSyncConfig,
                 metadata_manager: SyncMetadataManager,
                 progress_callback=None):
        self.source_db = source_db
        self.dest_db = dest_db
        self.config = config
        self.metadata = metadata_manager
        self.progress_callback = progress_callback
        
        # Detectar estrategia de cambios
        self._detect_change_strategy()
    
    def _detect_change_strategy(self):
        """Detecta la mejor estrategia para detectar cambios"""
        # Buscar columnas ROWVERSION/TIMESTAMP
        query = """
            SELECT c.name
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN sys.tables tbl ON c.object_id = tbl.object_id
            INNER JOIN sys.schemas s ON tbl.schema_id = s.schema_id
            WHERE s.name = ? AND tbl.name = ? 
            AND t.name IN ('timestamp', 'rowversion')
        """
        
        rows = self.source_db.execute_query(query, (self.config.schema, self.config.table_name))
        
        if rows:
            self.config.has_rowversion = True
            self.config.rowversion_column = rows[0][0]
            self.config.change_detection_strategy = Config.CHANGE_DETECTION_ROWVERSION
            logger.info(f"{self.config.full_name}: Usando ROWVERSION ({self.config.rowversion_column})")
        else:
            self.config.has_rowversion = False
            self.config.change_detection_strategy = Config.CHANGE_DETECTION_HASH
            logger.info(f"{self.config.full_name}: Usando HASH para detección de cambios")
    
    def _get_primary_key_columns(self) -> List[str]:
        """Detecta automáticamente las columnas de clave primaria"""
        if self.config.primary_key_columns:
            return self.config.primary_key_columns
        
        query = """
            SELECT c.name
            FROM sys.key_constraints kc
            INNER JOIN sys.indexes i ON kc.parent_object_id = i.object_id 
                AND kc.unique_index_id = i.index_id
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id 
                AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
            INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ? AND kc.type = 'PK'
            ORDER BY ic.key_ordinal
        """
        
        rows = self.source_db.execute_query(query, (self.config.schema, self.config.table_name))
        pk_columns = [row[0] for row in rows]
        
        if not pk_columns:
            raise ValueError(f"No se pudo detectar clave primaria para {self.config.full_name}. "
                           f"Configure manualmente las columnas PK.")
        
        self.config.primary_key_columns = pk_columns
        self.config.pk_auto_detected = True
        
        logger.info(f"{self.config.full_name}: PK detectada = {pk_columns}")
        return pk_columns
    
    def _get_all_columns(self) -> List[str]:
        """Obtiene lista de todas las columnas (excluyendo computed y rowversion para INSERT)"""
        query = """
            SELECT c.name
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ?
            AND c.is_computed = 0
            ORDER BY c.column_id
        """
        
        rows = self.source_db.execute_query(query, (self.config.schema, self.config.table_name))
        return [row[0] for row in rows]
    
    def _get_insertable_columns(self) -> List[str]:
        """Obtiene columnas que se pueden usar en INSERT (excluye IDENTITY, ROWVERSION, COMPUTED)"""
        query = """
            SELECT c.name
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            WHERE s.name = ? AND t.name = ?
            AND c.is_computed = 0
            AND c.is_identity = 0
            AND ty.name NOT IN ('timestamp', 'rowversion')
            ORDER BY c.column_id
        """
        
        rows = self.source_db.execute_query(query, (self.config.schema, self.config.table_name))
        return [row[0] for row in rows]
    
    def _build_where_clause(self) -> str:
        """Construye cláusula WHERE completa (si existe filtro configurado)"""
        if self.config.where_clause:
            return f"WHERE {self.config.where_clause}"
        return ""
    
    def synchronize(self) -> Dict[str, int]:
        """
        Ejecuta sincronización completa de la tabla
        Retorna diccionario con estadísticas
        """
        logger.info(f"=== Iniciando sincronización de {self.config.full_name} ===")
        
        stats = {
            'inserted': 0,
            'updated': 0,
            'deleted': 0,
            'errors': 0
        }
        
        try:
            # 1. Verificar que tabla existe en destino
            if not self.dest_db.table_exists(self.config.schema, self.config.table_name):
                logger.warning(f"Tabla {self.config.full_name} no existe en destino. Creando espejo perfecto...")
                create_table_as_mirror(self.source_db, self.dest_db, 
                                     self.config.schema, self.config.table_name)
                logger.info(f"✅ Tabla {self.config.full_name} creada exitosamente como réplica exacta")
            else:
                logger.info(f"Tabla {self.config.full_name} ya existe en destino")
            
            # 2. Detectar PK
            self._get_primary_key_columns()
            
            # 3. Guardar/actualizar configuración en metadatos
            self.metadata.upsert_metadata(
                self.config,
                self.config.change_detection_strategy,
                self.config.rowversion_column
            )
            
            # 4. Ejecutar sincronización según estrategia
            if self.config.change_detection_strategy == Config.CHANGE_DETECTION_ROWVERSION:
                stats = self._sync_with_rowversion()
            else:
                stats = self._sync_with_hash()
            
            # 5. Actualizar metadatos con resultado
            self.metadata.update_sync_status(
                self.config.schema,
                self.config.table_name,
                'SUCCESS',
                stats['inserted'],
                stats['updated'],
                stats['deleted']
            )
            
            logger.info(f"Sincronización completada: {stats}")
            return stats
            
        except Exception as e:
            stats['errors'] = 1
            error_msg = str(e)
            logger.error(f"Error en sincronización de {self.config.full_name}: {error_msg}")
            
            self.metadata.update_sync_status(
                self.config.schema,
                self.config.table_name,
                'ERROR',
                stats['inserted'],
                stats['updated'],
                stats['deleted'],
                error_msg
            )
            
            raise
    
    def _sync_with_rowversion(self) -> Dict[str, int]:
        """Sincronización completa con INSERT/UPDATE/DELETE usando ROWVERSION"""
        stats = {'inserted': 0, 'updated': 0, 'deleted': 0, 'errors': 0}
        
        pk_columns = self._get_primary_key_columns()
        where_clause = self._build_where_clause()
        
        # 1. INSERTS: registros que están en origen pero no en destino
        stats['inserted'] = self._perform_inserts(pk_columns, where_clause)
        
        # 2. UPDATES: registros que existen en ambos pero con diferencias
        stats['updated'] = self._perform_updates(pk_columns, where_clause)
        
        # 3. DELETES: registros que están en destino pero no en origen (respetando WHERE)
        stats['deleted'] = self._perform_deletes(pk_columns, where_clause)
        
        return stats
    
    def _sync_with_hash(self) -> Dict[str, int]:
        """Sincronización completa con INSERT/UPDATE/DELETE"""
        stats = {'inserted': 0, 'updated': 0, 'deleted': 0, 'errors': 0}
        
        pk_columns = self._get_primary_key_columns()
        where_clause = self._build_where_clause()
        
        # 1. INSERTS
        stats['inserted'] = self._perform_inserts(pk_columns, where_clause)
        
        # 2. UPDATES
        stats['updated'] = self._perform_updates(pk_columns, where_clause)
        
        # 3. DELETES
        stats['deleted'] = self._perform_deletes(pk_columns, where_clause)
        
        return stats
    
    def _perform_inserts(self, pk_columns: List[str], where_clause: str) -> int:
        """Ejecuta INSERTs de registros nuevos"""
        insertable_cols = self._get_insertable_columns()
        cols_list = ', '.join([f"[{col}]" for col in insertable_cols])
        pk_list = ', '.join([f"src.[{col}]" for col in pk_columns])
        pk_join = ' AND '.join([f"src.[{col}] = dst.[{col}]" for col in pk_columns])
        
        insert_query = f"""
            INSERT INTO [{self.config.schema}].[{self.config.table_name}] ({cols_list})
            SELECT {cols_list}
            FROM OPENQUERY([ORIGEN], '
                SELECT {cols_list}
                FROM [{self.config.schema}].[{self.config.table_name}]
                {where_clause}
            ') src
            WHERE NOT EXISTS (
                SELECT 1 
                FROM [{self.config.schema}].[{self.config.table_name}] dst
                WHERE {pk_join}
            )
        """
        
        # Como no podemos usar OPENQUERY sin linked server, 
        # haremos fetch desde origen e insert en destino por batches
        return self._insert_missing_records(insertable_cols, pk_columns, where_clause)
    
    def _insert_missing_records(self, insertable_cols: List[str], 
                               pk_columns: List[str], where_clause: str) -> int:
        """
        Inserta registros que faltan en destino - MÁXIMA VELOCIDAD
        Estrategia: 1 fetch completo de origen + filtrado Python + INSERT en batches grandes
        """
        
        cols_list = ', '.join([f"[{col}]" for col in insertable_cols])
        pk_list = ', '.join([f"[{col}]" for col in pk_columns])
        
        # DEBUG: Verificar columnas
        logger.debug(f"{self.config.full_name}: Columnas a insertar: {insertable_cols}")
        
        # 1. Obtener PKs existentes en destino (set para lookup O(1))
        dest_pk_query = f"""
            SELECT {pk_list}
            FROM [{self.config.schema}].[{self.config.table_name}]
        """
        dest_pks = self.dest_db.execute_query(dest_pk_query)
        
        dest_pk_set = set()
        for row in dest_pks:
            key = tuple([getattr(row, col) for col in pk_columns])
            dest_pk_set.add(key)
        
        logger.info(f"{self.config.full_name}: {len(dest_pk_set):,} registros ya existen en destino")
        
        # 2. Fetch TODOS los datos de origen de una vez (con WHERE si aplica)
        logger.info(f"{self.config.full_name}: Obteniendo datos completos desde origen...")
        
        fetch_query = f"""
            SELECT {cols_list}
            FROM [{self.config.schema}].[{self.config.table_name}]
            {where_clause}
        """
        
        # DEBUG: Mostrar query completa
        logger.debug(f"{self.config.full_name}: Query SELECT: {fetch_query}")
        
        all_source_rows = self.source_db.execute_query(fetch_query)
        
        if not all_source_rows:
            logger.info(f"{self.config.full_name}: No hay registros en origen con el filtro aplicado")
            return 0
        
        logger.info(f"{self.config.full_name}: {len(all_source_rows):,} registros obtenidos desde origen")
        
        # 3. Filtrar en Python solo los que faltan en destino
        rows_to_insert = []
        for row in all_source_rows:
            # Construir PK del registro
            pk_key = tuple([getattr(row, col) for col in pk_columns])
            
            # Solo agregar si NO existe en destino
            if pk_key not in dest_pk_set:
                rows_to_insert.append(tuple([getattr(row, col) for col in insertable_cols]))
        
        if not rows_to_insert:
            logger.info(f"{self.config.full_name}: No hay registros nuevos para insertar")
            return 0
        
        total_to_insert = len(rows_to_insert)
        logger.info(f"{self.config.full_name}: {total_to_insert:,} registros nuevos a insertar")
        
        # 4. Insertar en batches grandes (50K por batch)
        insert_batch_size = 50000
        total_inserted = 0
        
        placeholders = ', '.join(['?' for _ in insertable_cols])
        insert_stmt = f"""
            INSERT INTO [{self.config.schema}].[{self.config.table_name}] 
            ({cols_list}) VALUES ({placeholders})
        """
        
        # DEBUG: Mostrar statement INSERT
        logger.debug(f"{self.config.full_name}: Statement INSERT: {insert_stmt}")
        
        for batch_start in range(0, total_to_insert, insert_batch_size):
            batch_end = min(batch_start + insert_batch_size, total_to_insert)
            batch_data = rows_to_insert[batch_start:batch_end]
            
            self.dest_db.execute_batch(insert_stmt, batch_data, commit=True)
            inserted_count = len(batch_data)
            total_inserted += inserted_count
            
            # Reportar progreso
            if self.progress_callback:
                progress_pct = int((batch_end / total_to_insert) * 100)
                self.progress_callback('PROGRESS', batch_end, total_to_insert, progress_pct)
            
            logger.debug(f"Batch {(batch_start // insert_batch_size) + 1}: {inserted_count:,} registros insertados ({total_inserted:,}/{total_to_insert:,})")
        
        logger.info(f"{self.config.full_name}: {total_inserted:,} registros insertados")
        return total_inserted
    
    def _perform_updates(self, pk_columns: List[str], where_clause: str) -> int:
        """
        Actualiza registros que existen en ambos pero tienen diferencias
        Estrategia: Fetch completo de ambos lados, comparar en Python
        """
        insertable_cols = self._get_insertable_columns()
        cols_list = ', '.join([f"[{col}]" for col in insertable_cols])
        pk_list = ', '.join([f"[{col}]" for col in pk_columns])
        
        # Columnas a comparar (no-PK)
        compare_cols = [col for col in insertable_cols if col not in pk_columns]
        
        if not compare_cols:
            logger.info(f"{self.config.full_name}: No hay columnas actualizables")
            return 0
        
        logger.info(f"{self.config.full_name}: Comparando registros para UPDATE...")
        
        # 1. Obtener TODOS los datos de origen
        fetch_query = f"""
            SELECT {cols_list}
            FROM [{self.config.schema}].[{self.config.table_name}]
            {where_clause}
        """
        source_rows = self.source_db.execute_query(fetch_query)
        
        if not source_rows:
            return 0
        
        logger.info(f"{self.config.full_name}: {len(source_rows):,} registros en origen")
        
        # 2. Obtener TODOS los datos de destino
        dest_fetch_query = f"""
            SELECT {cols_list}
            FROM [{self.config.schema}].[{self.config.table_name}]
        """
        dest_rows = self.dest_db.execute_query(dest_fetch_query)
        
        logger.info(f"{self.config.full_name}: {len(dest_rows):,} registros en destino")
        
        # 3. Crear diccionario de destino por PK para lookup rápido
        dest_dict = {}
        for row in dest_rows:
            pk_key = tuple([getattr(row, col) for col in pk_columns])
            dest_dict[pk_key] = row
        
        # 4. Comparar todos los registros de origen con destino
        rows_to_update = []
        
        for source_row in source_rows:
            pk_key = tuple([getattr(source_row, col) for col in pk_columns])
            
            # Solo procesar si existe en destino
            if pk_key in dest_dict:
                dest_row = dest_dict[pk_key]
                
                # Comparar columnas no-PK
                has_differences = False
                for col in compare_cols:
                    source_val = getattr(source_row, col, None)
                    dest_val = getattr(dest_row, col, None)
                    
                    if source_val != dest_val:
                        has_differences = True
                        break
                
                if has_differences:
                    rows_to_update.append(source_row)
        
        if not rows_to_update:
            logger.info(f"{self.config.full_name}: No hay cambios para actualizar")
            return 0
        
        logger.info(f"{self.config.full_name}: {len(rows_to_update):,} registros con cambios detectados")
        
        # 5. Ejecutar UPDATE en batches grandes
        total_updated = 0
        batch_size = 10000  # Batches grandes como en INSERT
        
        set_clause = ', '.join([f"[{col}] = ?" for col in compare_cols])
        where_pk = ' AND '.join([f"[{col}] = ?" for col in pk_columns])
        
        update_stmt = f"""
            UPDATE [{self.config.schema}].[{self.config.table_name}]
            SET {set_clause}
            WHERE {where_pk}
        """
        
        for batch_start in range(0, len(rows_to_update), batch_size):
            batch_rows = rows_to_update[batch_start:batch_start + batch_size]
            
            params_list = []
            for row in batch_rows:
                params = []
                # Valores para SET (columnas no-PK)
                for col in compare_cols:
                    params.append(getattr(row, col))
                # Valores para WHERE (PKs)
                for col in pk_columns:
                    params.append(getattr(row, col))
                params_list.append(tuple(params))
            
            self.dest_db.execute_batch(update_stmt, params_list, commit=True)
            updated_count = len(params_list)
            total_updated += updated_count
            
            # Reportar progreso
            if self.progress_callback:
                progress_pct = int(((batch_start + updated_count) / len(rows_to_update)) * 100)
                self.progress_callback('PROGRESS', batch_start + updated_count, len(rows_to_update), progress_pct)
            
            logger.debug(f"Batch UPDATE: {updated_count:,} registros actualizados ({total_updated:,}/{len(rows_to_update):,})")
        
        logger.info(f"{self.config.full_name}: {total_updated:,} registros actualizados")
        return total_updated
    
    def _perform_deletes(self, pk_columns: List[str], where_clause: str) -> int:
        """
        Elimina registros que están en destino pero no en origen
        IMPORTANTE: Solo elimina dentro del alcance del WHERE clause
        """
        pk_list = ', '.join([f"[{col}]" for col in pk_columns])
        
        # 1. Obtener PKs de origen (con WHERE si aplica)
        logger.info(f"{self.config.full_name}: Verificando registros para DELETE...")
        
        source_pk_query = f"""
            SELECT {pk_list}
            FROM [{self.config.schema}].[{self.config.table_name}]
            {where_clause}
        """
        source_pks = self.source_db.execute_query(source_pk_query)
        
        source_pk_set = set()
        for row in source_pks:
            key = tuple([getattr(row, col) for col in pk_columns])
            source_pk_set.add(key)
        
        # 2. Obtener PKs de destino (con el MISMO WHERE para respetar alcance)
        dest_pks = self.dest_db.execute_query(source_pk_query)
        
        # 3. Identificar registros a eliminar (en destino pero no en origen)
        pks_to_delete = []
        for row in dest_pks:
            key = tuple([getattr(row, col) for col in pk_columns])
            if key not in source_pk_set:
                pks_to_delete.append(key)
        
        if not pks_to_delete:
            logger.info(f"{self.config.full_name}: No hay registros para eliminar")
            return 0
        
        logger.info(f"{self.config.full_name}: {len(pks_to_delete):,} registros a eliminar")
        
        # 4. Ejecutar DELETE en batches grandes
        total_deleted = 0
        batch_size = 10000  # Batches grandes
        
        where_pk = ' AND '.join([f"[{col}] = ?" for col in pk_columns])
        delete_stmt = f"""
            DELETE FROM [{self.config.schema}].[{self.config.table_name}]
            WHERE {where_pk}
        """
        
        for batch_start in range(0, len(pks_to_delete), batch_size):
            batch_pks = pks_to_delete[batch_start:batch_start + batch_size]
            
            self.dest_db.execute_batch(delete_stmt, batch_pks, commit=True)
            deleted_count = len(batch_pks)
            total_deleted += deleted_count
            
            # Reportar progreso
            if self.progress_callback:
                progress_pct = int(((batch_start + deleted_count) / len(pks_to_delete)) * 100)
                self.progress_callback('PROGRESS', batch_start + deleted_count, len(pks_to_delete), progress_pct)
            
            logger.debug(f"Batch DELETE: {deleted_count:,} registros eliminados ({total_deleted:,}/{len(pks_to_delete):,})")
        
        logger.info(f"{self.config.full_name}: {total_deleted:,} registros eliminados")
        return total_deleted


class SyncOrchestrator:
    """Orquesta la sincronización de múltiples tablas"""
    
    def __init__(self, source_db: DatabaseConnection, dest_db: DatabaseConnection):
        self.source_db = source_db
        self.dest_db = dest_db
        self.metadata_manager = SyncMetadataManager(dest_db)
    
    def synchronize_tables(self, table_configs: List[TableSyncConfig],
                          callback=None) -> Dict[str, Any]:
        """
        Sincroniza lista de tablas
        callback: función opcional para reportar progreso (table_name, progress, status)
        """
        total_stats = {
            'total_tables': len(table_configs),
            'successful': 0,
            'failed': 0,
            'total_inserted': 0,
            'total_updated': 0,
            'total_deleted': 0,
            'errors': []
        }
        
        for i, config in enumerate(table_configs):
            if not config.is_selected or not config.sync_enabled:
                continue
            
            try:
                if callback:
                    callback(config.full_name, i + 1, len(table_configs), 'SYNCING')
                
                synchronizer = TableSynchronizer(
                    self.source_db, self.dest_db, config, self.metadata_manager,
                    progress_callback=callback
                )
                
                stats = synchronizer.synchronize()
                
                total_stats['successful'] += 1
                total_stats['total_inserted'] += stats['inserted']
                total_stats['total_updated'] += stats['updated']
                total_stats['total_deleted'] += stats['deleted']
                
                if callback:
                    callback(config.full_name, i + 1, len(table_configs), 'SUCCESS')
                
            except Exception as e:
                total_stats['failed'] += 1
                error_msg = f"{config.full_name}: {str(e)}"
                total_stats['errors'].append(error_msg)
                logger.error(error_msg)
                
                if callback:
                    callback(config.full_name, i + 1, len(table_configs), 'ERROR')
        
        return total_stats
