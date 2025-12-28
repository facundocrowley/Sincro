"""
Gestión de conexiones y operaciones básicas de base de datos
"""

import pyodbc
import logging
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
from config import DBConfig, Config

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Gestor de conexión a base de datos SQL Server"""
    
    def __init__(self, db_config: DBConfig):
        self.config = db_config
        self.connection: Optional[pyodbc.Connection] = None
        self._test_connection()
    
    def _test_connection(self):
        """Prueba la conexión a la base de datos"""
        try:
            conn = pyodbc.connect(
                self.config.get_connection_string(),
                timeout=Config.CONNECTION_TIMEOUT
            )
            conn.close()
            logger.info(f"Conexión exitosa a {self.config}")
        except Exception as e:
            logger.error(f"Error al conectar a {self.config}: {e}")
            raise
    
    def connect(self) -> pyodbc.Connection:
        """Establece conexión a la base de datos"""
        if self.connection is None or self.connection.closed:
            self.connection = pyodbc.connect(
                self.config.get_connection_string(),
                timeout=Config.CONNECTION_TIMEOUT
            )
            self.connection.timeout = Config.COMMAND_TIMEOUT
        return self.connection
    
    def disconnect(self):
        """Cierra la conexión"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            self.connection = None
    
    @contextmanager
    def get_cursor(self, commit: bool = False):
        """Context manager para obtener cursor"""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en operación de BD: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_query(self, query: str, params: Tuple = None) -> List[pyodbc.Row]:
        """Ejecuta consulta SELECT y retorna resultados"""
        with self.get_cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def execute_scalar(self, query: str, params: Tuple = None) -> Any:
        """Ejecuta consulta y retorna un solo valor"""
        with self.get_cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            row = cursor.fetchone()
            return row[0] if row else None
    
    def execute_non_query(self, query: str, params: Tuple = None, commit: bool = True) -> int:
        """Ejecuta comando que no retorna resultados (INSERT, UPDATE, DELETE)"""
        with self.get_cursor(commit=commit) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.rowcount
    
    def execute_batch(self, query: str, params_list: List[Tuple], commit: bool = True) -> int:
        """Ejecuta múltiples comandos en batch"""
        with self.get_cursor(commit=commit) as cursor:
            cursor.fast_executemany = True
            cursor.executemany(query, params_list)
            return cursor.rowcount
    
    def get_tables(self) -> List[Dict[str, str]]:
        """Obtiene lista de tablas de la base de datos"""
        query = """
            SELECT 
                s.name AS schema_name,
                t.name AS table_name,
                t.object_id,
                CAST(SUM(p.rows) AS BIGINT) AS row_count
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.partitions p ON t.object_id = p.object_id 
                AND p.index_id IN (0, 1)
            WHERE t.is_ms_shipped = 0
            GROUP BY s.name, t.name, t.object_id
            ORDER BY s.name, t.name
        """
        
        rows = self.execute_query(query)
        tables = []
        for row in rows:
            tables.append({
                'schema': row.schema_name,
                'table': row.table_name,
                'object_id': row.object_id,
                'row_count': row.row_count or 0
            })
        
        return tables
    
    def table_exists(self, schema: str, table_name: str) -> bool:
        """Verifica si una tabla existe"""
        query = """
            SELECT COUNT(*)
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ?
        """
        count = self.execute_scalar(query, (schema, table_name))
        return count > 0
    
    def get_table_object_id(self, schema: str, table_name: str) -> Optional[int]:
        """Obtiene el object_id de una tabla"""
        query = """
            SELECT t.object_id
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ?
        """
        return self.execute_scalar(query, (schema, table_name))
    
    def begin_transaction(self):
        """Inicia una transacción explícita"""
        conn = self.connect()
        conn.autocommit = False
    
    def commit(self):
        """Confirma transacción"""
        if self.connection:
            self.connection.commit()
    
    def rollback(self):
        """Revierte transacción"""
        if self.connection:
            self.connection.rollback()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        self.disconnect()
        return False
