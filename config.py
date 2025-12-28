"""
Configuración y constantes de la aplicación de sincronización SQL Server
"""

class Config:
    """Configuración global de la aplicación"""
    
    # Configuración de conexión
    CONNECTION_TIMEOUT = 30
    COMMAND_TIMEOUT = 300
    
    # Configuración de sincronización
    BATCH_SIZE = 1000  # Registros por batch en INSERT/UPDATE
    MAX_PARALLEL_TABLES = 5  # Tablas a sincronizar en paralelo
    
    # Tabla de metadatos
    METADATA_TABLE_NAME = "SyncMetadata"
    METADATA_SCHEMA = "dbo"
    
    # Configuración de logging
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Estrategias de detección de cambios
    CHANGE_DETECTION_ROWVERSION = "rowversion"
    CHANGE_DETECTION_HASH = "hash"
    CHANGE_DETECTION_FULL = "full"  # Comparación completa (último recurso)


class DBConfig:
    """Configuración de conexión a base de datos"""
    
    def __init__(self, server: str = "", database: str = "", 
                 username: str = "", password: str = "", 
                 use_windows_auth: bool = False):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.use_windows_auth = use_windows_auth
    
    def get_connection_string(self) -> str:
        """Genera string de conexión para pyodbc"""
        if self.use_windows_auth:
            return (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
        else:
            return (
                f"DRIVER={{SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )
    
    def is_valid(self) -> bool:
        """Valida que la configuración esté completa"""
        if not self.server or not self.database:
            return False
        if not self.use_windows_auth and not self.username:
            return False
        return True
    
    def __repr__(self):
        return f"DBConfig(server='{self.server}', database='{self.database}')"


class TableSyncConfig:
    """Configuración de sincronización para una tabla específica"""
    
    def __init__(self, schema: str, table_name: str):
        self.schema = schema
        self.table_name = table_name
        self.full_name = f"[{schema}].[{table_name}]"
        
        # Configuración de PK
        self.primary_key_columns = []  # Lista de columnas que forman la PK
        self.pk_auto_detected = True   # Si fue detectada automáticamente
        
        # Filtros
        self.where_clause = ""  # WHERE adicional para filtrar datos
        
        # Detección de cambios
        self.change_detection_strategy = None  # Se determina automáticamente
        self.has_rowversion = False
        self.rowversion_column = None
        
        # Estado
        self.is_selected = False
        self.sync_enabled = True
    
    def get_pk_where_clause(self, source_alias: str = "src", dest_alias: str = "dst") -> str:
        """Genera cláusula WHERE para comparar PKs entre dos tablas"""
        if not self.primary_key_columns:
            return ""
        
        conditions = []
        for col in self.primary_key_columns:
            conditions.append(f"{source_alias}.[{col}] = {dest_alias}.[{col}]")
        
        return " AND ".join(conditions)
    
    def __repr__(self):
        return f"TableSyncConfig({self.full_name}, pk={self.primary_key_columns})"
