"""
Extracción y creación de esquema completo de tablas (ESPEJO PERFECTO)
Incluye: columnas, tipos, PKs, índices, constraints, triggers, computed columns, etc.
"""

import logging
from typing import List, Dict, Any, Optional
from db import DatabaseConnection
from config import TableSyncConfig

logger = logging.getLogger(__name__)


class SchemaExtractor:
    """Extrae esquema completo de tablas de SQL Server"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def get_table_definition(self, schema: str, table_name: str) -> Dict[str, Any]:
        """
        Obtiene la definición completa de una tabla como espejo perfecto
        """
        object_id = self.db.get_table_object_id(schema, table_name)
        if not object_id:
            raise ValueError(f"Tabla {schema}.{table_name} no encontrada")
        
        definition = {
            'schema': schema,
            'table_name': table_name,
            'object_id': object_id,
            'columns': self._get_columns(object_id),
            'primary_key': self._get_primary_key(object_id),
            'indexes': self._get_indexes(object_id),
            'foreign_keys': self._get_foreign_keys(object_id),
            'check_constraints': self._get_check_constraints(object_id),
            'default_constraints': self._get_default_constraints(object_id),
            'unique_constraints': self._get_unique_constraints(object_id),
            'triggers': [],  # No copiar triggers
            'computed_columns': self._get_computed_columns(object_id)
        }
        
        return definition
    
    def _get_columns(self, object_id: int) -> List[Dict[str, Any]]:
        """Extrae definición completa de columnas"""
        query = """
            SELECT 
                c.column_id,
                c.name AS column_name,
                t.name AS type_name,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                c.is_computed,
                c.is_rowguidcol,
                CAST(ISNULL(ic.seed_value, 0) AS BIGINT) AS identity_seed,
                CAST(ISNULL(ic.increment_value, 0) AS BIGINT) AS identity_increment,
                c.collation_name,
                cc.definition AS computed_definition,
                cc.is_persisted AS computed_is_persisted
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id 
                AND c.column_id = ic.column_id
            LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id 
                AND c.column_id = cc.column_id
            WHERE c.object_id = ?
            ORDER BY c.column_id
        """
        
        rows = self.db.execute_query(query, (object_id,))
        columns = []
        
        for row in rows:
            col = {
                'column_id': row.column_id,
                'name': row.column_name,
                'type_name': row.type_name,
                'max_length': row.max_length,
                'precision': row.precision,
                'scale': row.scale,
                'is_nullable': row.is_nullable,
                'is_identity': row.is_identity,
                'is_computed': row.is_computed,
                'is_rowguidcol': row.is_rowguidcol,
                'identity_seed': row.identity_seed,
                'identity_increment': row.identity_increment,
                'collation_name': row.collation_name,
                'computed_definition': row.computed_definition,
                'computed_is_persisted': row.computed_is_persisted
            }
            columns.append(col)
        
        return columns
    
    def _get_primary_key(self, object_id: int) -> Optional[Dict[str, Any]]:
        """Extrae definición de clave primaria"""
        query = """
            SELECT 
                kc.name AS constraint_name,
                i.type_desc AS index_type,
                ic.key_ordinal,
                c.name AS column_name,
                ic.is_descending_key
            FROM sys.key_constraints kc
            INNER JOIN sys.indexes i ON kc.parent_object_id = i.object_id 
                AND kc.unique_index_id = i.index_id
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id 
                AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
            WHERE kc.parent_object_id = ? AND kc.type = 'PK'
            ORDER BY ic.key_ordinal
        """
        
        rows = self.db.execute_query(query, (object_id,))
        if not rows:
            return None
        
        pk = {
            'constraint_name': rows[0].constraint_name,
            'index_type': rows[0].index_type,
            'columns': []
        }
        
        for row in rows:
            pk['columns'].append({
                'name': row.column_name,
                'ordinal': row.key_ordinal,
                'is_descending': row.is_descending_key
            })
        
        return pk
    
    def _get_indexes(self, object_id: int) -> List[Dict[str, Any]]:
        """Extrae definición de índices (excluyendo PK que ya se extrajo)"""
        query = """
            SELECT 
                i.index_id,
                i.name AS index_name,
                i.type_desc,
                i.is_unique,
                i.is_primary_key,
                i.fill_factor,
                i.has_filter,
                i.filter_definition,
                ic.key_ordinal,
                ic.is_descending_key,
                ic.is_included_column,
                c.name AS column_name
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id 
                AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
            WHERE i.object_id = ? 
                AND i.is_primary_key = 0
                AND i.type > 0  -- Excluir heap
            ORDER BY i.index_id, ic.key_ordinal, ic.is_included_column
        """
        
        rows = self.db.execute_query(query, (object_id,))
        indexes_dict = {}
        
        for row in rows:
            idx_id = row.index_id
            
            if idx_id not in indexes_dict:
                indexes_dict[idx_id] = {
                    'index_id': idx_id,
                    'name': row.index_name,
                    'type_desc': row.type_desc,
                    'is_unique': row.is_unique,
                    'fill_factor': row.fill_factor,
                    'has_filter': row.has_filter,
                    'filter_definition': row.filter_definition,
                    'key_columns': [],
                    'included_columns': []
                }
            
            col_info = {
                'name': row.column_name,
                'ordinal': row.key_ordinal,
                'is_descending': row.is_descending_key
            }
            
            if row.is_included_column:
                indexes_dict[idx_id]['included_columns'].append(col_info)
            else:
                indexes_dict[idx_id]['key_columns'].append(col_info)
        
        return list(indexes_dict.values())
    
    def _get_foreign_keys(self, object_id: int) -> List[Dict[str, Any]]:
        """Extrae definición de claves foráneas"""
        query = """
            SELECT 
                fk.name AS fk_name,
                fk.delete_referential_action_desc,
                fk.update_referential_action_desc,
                fk.is_disabled,
                SCHEMA_NAME(ref_t.schema_id) AS referenced_schema,
                ref_t.name AS referenced_table,
                parent_col.name AS parent_column,
                ref_col.name AS referenced_column,
                fkc.constraint_column_id
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.columns parent_col ON fkc.parent_object_id = parent_col.object_id 
                AND fkc.parent_column_id = parent_col.column_id
            INNER JOIN sys.columns ref_col ON fkc.referenced_object_id = ref_col.object_id 
                AND fkc.referenced_column_id = ref_col.column_id
            INNER JOIN sys.tables ref_t ON fkc.referenced_object_id = ref_t.object_id
            WHERE fk.parent_object_id = ?
            ORDER BY fk.name, fkc.constraint_column_id
        """
        
        rows = self.db.execute_query(query, (object_id,))
        fk_dict = {}
        
        for row in rows:
            fk_name = row.fk_name
            
            if fk_name not in fk_dict:
                fk_dict[fk_name] = {
                    'name': fk_name,
                    'referenced_schema': row.referenced_schema,
                    'referenced_table': row.referenced_table,
                    'delete_action': row.delete_referential_action_desc,
                    'update_action': row.update_referential_action_desc,
                    'is_disabled': row.is_disabled,
                    'columns': []
                }
            
            fk_dict[fk_name]['columns'].append({
                'parent_column': row.parent_column,
                'referenced_column': row.referenced_column
            })
        
        return list(fk_dict.values())
    
    def _get_check_constraints(self, object_id: int) -> List[Dict[str, Any]]:
        """Extrae CHECK constraints"""
        query = """
            SELECT 
                cc.name AS constraint_name,
                cc.definition,
                cc.is_disabled
            FROM sys.check_constraints cc
            WHERE cc.parent_object_id = ?
            ORDER BY cc.name
        """
        
        rows = self.db.execute_query(query, (object_id,))
        return [{
            'name': row.constraint_name,
            'definition': row.definition,
            'is_disabled': row.is_disabled
        } for row in rows]
    
    def _get_default_constraints(self, object_id: int) -> List[Dict[str, Any]]:
        """Extrae DEFAULT constraints"""
        query = """
            SELECT 
                dc.name AS constraint_name,
                c.name AS column_name,
                dc.definition
            FROM sys.default_constraints dc
            INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id 
                AND dc.parent_column_id = c.column_id
            WHERE dc.parent_object_id = ?
            ORDER BY c.column_id
        """
        
        rows = self.db.execute_query(query, (object_id,))
        return [{
            'name': row.constraint_name,
            'column_name': row.column_name,
            'definition': row.definition
        } for row in rows]
    
    def _get_unique_constraints(self, object_id: int) -> List[Dict[str, Any]]:
        """Extrae UNIQUE constraints (que no sean PK)"""
        query = """
            SELECT 
                kc.name AS constraint_name,
                c.name AS column_name,
                ic.key_ordinal
            FROM sys.key_constraints kc
            INNER JOIN sys.indexes i ON kc.parent_object_id = i.object_id 
                AND kc.unique_index_id = i.index_id
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id 
                AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
            WHERE kc.parent_object_id = ? AND kc.type = 'UQ'
            ORDER BY kc.name, ic.key_ordinal
        """
        
        rows = self.db.execute_query(query, (object_id,))
        uq_dict = {}
        
        for row in rows:
            uq_name = row.constraint_name
            if uq_name not in uq_dict:
                uq_dict[uq_name] = {
                    'name': uq_name,
                    'columns': []
                }
            uq_dict[uq_name]['columns'].append(row.column_name)
        
        return list(uq_dict.values())
    
    def _get_triggers(self, object_id: int) -> List[Dict[str, Any]]:
        """Extrae triggers"""
        query = """
            SELECT 
                tr.name AS trigger_name,
                tr.is_disabled,
                tr.is_instead_of_trigger,
                OBJECT_DEFINITION(tr.object_id) AS definition
            FROM sys.triggers tr
            WHERE tr.parent_id = ?
            ORDER BY tr.name
        """
        
        rows = self.db.execute_query(query, (object_id,))
        return [{
            'name': row.trigger_name,
            'is_disabled': row.is_disabled,
            'is_instead_of': row.is_instead_of_trigger,
            'definition': row.definition
        } for row in rows]
    
    def _get_computed_columns(self, object_id: int) -> List[Dict[str, Any]]:
        """Extrae información de columnas computadas"""
        query = """
            SELECT 
                c.name AS column_name,
                cc.definition,
                cc.is_persisted
            FROM sys.computed_columns cc
            INNER JOIN sys.columns c ON cc.object_id = c.object_id 
                AND cc.column_id = c.column_id
            WHERE cc.object_id = ?
            ORDER BY c.column_id
        """
        
        rows = self.db.execute_query(query, (object_id,))
        return [{
            'column_name': row.column_name,
            'definition': row.definition,
            'is_persisted': row.is_persisted
        } for row in rows]


class SchemaBuilder:
    """Genera scripts DDL para crear tabla como espejo perfecto"""
    
    @staticmethod
    def generate_create_table_script(table_def: Dict[str, Any]) -> str:
        """Genera script CREATE TABLE completo"""
        schema = table_def['schema']
        table_name = table_def['table_name']
        columns = table_def['columns']
        
        script = f"CREATE TABLE [{schema}].[{table_name}] (\n"
        
        column_defs = []
        for col in columns:
            if col['is_computed']:
                # Columna computada
                col_def = f"    [{col['name']}] AS {col['computed_definition']}"
                if col['computed_is_persisted']:
                    col_def += " PERSISTED"
            else:
                # Columna normal
                col_def = f"    [{col['name']}] {SchemaBuilder._get_column_type(col)}"
                
                # COLLATE
                if col['collation_name']:
                    col_def += f" COLLATE {col['collation_name']}"
                
                # IDENTITY
                if col['is_identity']:
                    col_def += f" IDENTITY({col['identity_seed']},{col['identity_increment']})"
                
                # ROWGUIDCOL
                if col['is_rowguidcol']:
                    col_def += " ROWGUIDCOL"
                
                # NULL/NOT NULL
                if col['is_nullable']:
                    col_def += " NULL"
                else:
                    col_def += " NOT NULL"
            
            column_defs.append(col_def)
        
        script += ",\n".join(column_defs)
        script += "\n);\n"
        
        return script
    
    @staticmethod
    def _get_column_type(col: Dict[str, Any]) -> str:
        """Genera definición de tipo de columna exacta"""
        type_name = col['type_name']
        
        # Tipos con longitud
        if type_name in ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary'):
            if col['max_length'] == -1:
                return f"[{type_name}](MAX)"
            else:
                # Para nchar/nvarchar dividir entre 2
                length = col['max_length'] // 2 if type_name.startswith('n') else col['max_length']
                return f"[{type_name}]({length})"
        
        # Tipos con precisión y escala
        elif type_name in ('decimal', 'numeric'):
            return f"[{type_name}]({col['precision']},{col['scale']})"
        
        # Tipos con solo precisión
        elif type_name in ('time', 'datetime2', 'datetimeoffset'):
            return f"[{type_name}]({col['scale']})"
        
        # Tipos sin parámetros
        else:
            return f"[{type_name}]"
    
    @staticmethod
    def generate_primary_key_script(schema: str, table_name: str, 
                                   pk_def: Optional[Dict[str, Any]]) -> Optional[str]:
        """Genera script para crear PRIMARY KEY"""
        if not pk_def:
            return None
        
        pk_name = pk_def['constraint_name']
        pk_type = "CLUSTERED" if "CLUSTERED" in pk_def['index_type'] else "NONCLUSTERED"
        
        columns = []
        for col in pk_def['columns']:
            col_spec = f"[{col['name']}]"
            if col['is_descending']:
                col_spec += " DESC"
            else:
                col_spec += " ASC"
            columns.append(col_spec)
        
        columns_str = ", ".join(columns)
        
        script = f"""
ALTER TABLE [{schema}].[{table_name}]
ADD CONSTRAINT [{pk_name}] PRIMARY KEY {pk_type} ({columns_str});
"""
        return script
    
    @staticmethod
    def generate_indexes_scripts(schema: str, table_name: str, 
                                indexes: List[Dict[str, Any]]) -> List[str]:
        """Genera scripts para crear índices"""
        scripts = []
        
        for idx in indexes:
            unique_str = "UNIQUE " if idx['is_unique'] else ""
            type_str = idx['type_desc'].replace('_', ' ')
            
            # Columnas clave
            key_cols = []
            for col in idx['key_columns']:
                col_spec = f"[{col['name']}]"
                if col['is_descending']:
                    col_spec += " DESC"
                else:
                    col_spec += " ASC"
                key_cols.append(col_spec)
            
            key_cols_str = ", ".join(key_cols)
            
            script = f"CREATE {unique_str}{type_str} [{idx['name']}]\n"
            script += f"ON [{schema}].[{table_name}] ({key_cols_str})"
            
            # Columnas incluidas
            if idx['included_columns']:
                inc_cols = [f"[{col['name']}]" for col in idx['included_columns']]
                script += f"\nINCLUDE ({', '.join(inc_cols)})"
            
            # Filtro
            if idx['has_filter'] and idx['filter_definition']:
                script += f"\nWHERE {idx['filter_definition']}"
            
            # Fill factor
            if idx['fill_factor'] and idx['fill_factor'] > 0:
                script += f"\nWITH (FILLFACTOR = {idx['fill_factor']})"
            
            script += ";\n"
            scripts.append(script)
        
        return scripts
    
    @staticmethod
    def generate_foreign_keys_scripts(schema: str, table_name: str, 
                                     fks: List[Dict[str, Any]]) -> List[str]:
        """Genera scripts para crear FOREIGN KEYS"""
        scripts = []
        
        for fk in fks:
            parent_cols = [col['parent_column'] for col in fk['columns']]
            ref_cols = [col['referenced_column'] for col in fk['columns']]
            
            parent_cols_str = ", ".join([f"[{col}]" for col in parent_cols])
            ref_cols_str = ", ".join([f"[{col}]" for col in ref_cols])
            
            script = f"""
ALTER TABLE [{schema}].[{table_name}]
ADD CONSTRAINT [{fk['name']}] FOREIGN KEY ({parent_cols_str})
REFERENCES [{fk['referenced_schema']}].[{fk['referenced_table']}] ({ref_cols_str})"""
            
            if fk['delete_action'] != 'NO_ACTION':
                script += f"\nON DELETE {fk['delete_action'].replace('_', ' ')}"
            
            if fk['update_action'] != 'NO_ACTION':
                script += f"\nON UPDATE {fk['update_action'].replace('_', ' ')}"
            
            script += ";\n"
            
            if fk['is_disabled']:
                script += f"ALTER TABLE [{schema}].[{table_name}] NOCHECK CONSTRAINT [{fk['name']}];\n"
            
            scripts.append(script)
        
        return scripts
    
    @staticmethod
    def generate_check_constraints_scripts(schema: str, table_name: str,
                                          checks: List[Dict[str, Any]]) -> List[str]:
        """Genera scripts para crear CHECK constraints"""
        scripts = []
        
        for chk in checks:
            script = f"""
ALTER TABLE [{schema}].[{table_name}]
ADD CONSTRAINT [{chk['name']}] CHECK {chk['definition']};
"""
            if chk['is_disabled']:
                script += f"ALTER TABLE [{schema}].[{table_name}] NOCHECK CONSTRAINT [{chk['name']}];\n"
            
            scripts.append(script)
        
        return scripts
    
    @staticmethod
    def generate_default_constraints_scripts(schema: str, table_name: str,
                                            defaults: List[Dict[str, Any]]) -> List[str]:
        """Genera scripts para crear DEFAULT constraints"""
        scripts = []
        
        for df in defaults:
            script = f"""
ALTER TABLE [{schema}].[{table_name}]
ADD CONSTRAINT [{df['name']}] DEFAULT {df['definition']} FOR [{df['column_name']}];
"""
            scripts.append(script)
        
        return scripts
    
    @staticmethod
    def generate_unique_constraints_scripts(schema: str, table_name: str,
                                           uniques: List[Dict[str, Any]]) -> List[str]:
        """Genera scripts para crear UNIQUE constraints"""
        scripts = []
        
        for uq in uniques:
            columns_str = ", ".join([f"[{col}]" for col in uq['columns']])
            script = f"""
ALTER TABLE [{schema}].[{table_name}]
ADD CONSTRAINT [{uq['name']}] UNIQUE ({columns_str});
"""
            scripts.append(script)
        
        return scripts
    
    @staticmethod
    def generate_triggers_scripts(schema: str, table_name: str,
                                 triggers: List[Dict[str, Any]]) -> List[str]:
        """Genera scripts para crear TRIGGERS"""
        scripts = []
        
        for trg in triggers:
            # El OBJECT_DEFINITION ya incluye CREATE TRIGGER completo
            script = trg['definition']
            
            if trg['is_disabled']:
                script += f"\nDISABLE TRIGGER [{schema}].[{trg['name']}] ON [{schema}].[{table_name}];\n"
            
            scripts.append(script)
        
        return scripts
    
    @staticmethod
    def generate_full_table_script(table_def: Dict[str, Any]) -> str:
        """
        Genera el script DDL COMPLETO para crear tabla como ESPEJO PERFECTO
        """
        schema = table_def['schema']
        table_name = table_def['table_name']
        
        full_script = "-- =============================================\n"
        full_script += f"-- Creación de tabla [{schema}].[{table_name}] (ESPEJO PERFECTO)\n"
        full_script += "-- =============================================\n\n"
        
        # 1. CREATE TABLE
        full_script += "-- 1. Estructura de tabla\n"
        full_script += SchemaBuilder.generate_create_table_script(table_def)
        full_script += "\n"
        
        # 2. PRIMARY KEY
        if table_def['primary_key']:
            full_script += "-- 2. Primary Key\n"
            pk_script = SchemaBuilder.generate_primary_key_script(
                schema, table_name, table_def['primary_key']
            )
            if pk_script:
                full_script += pk_script + "\n"
        
        # 3. UNIQUE CONSTRAINTS
        if table_def['unique_constraints']:
            full_script += "-- 3. Unique Constraints\n"
            for script in SchemaBuilder.generate_unique_constraints_scripts(
                schema, table_name, table_def['unique_constraints']
            ):
                full_script += script + "\n"
        
        # 4. DEFAULT CONSTRAINTS
        if table_def['default_constraints']:
            full_script += "-- 4. Default Constraints\n"
            for script in SchemaBuilder.generate_default_constraints_scripts(
                schema, table_name, table_def['default_constraints']
            ):
                full_script += script + "\n"
        
        # 5. CHECK CONSTRAINTS
        if table_def['check_constraints']:
            full_script += "-- 5. Check Constraints\n"
            for script in SchemaBuilder.generate_check_constraints_scripts(
                schema, table_name, table_def['check_constraints']
            ):
                full_script += script + "\n"
        
        # 6. INDEXES
        if table_def['indexes']:
            full_script += "-- 6. Indexes\n"
            for script in SchemaBuilder.generate_indexes_scripts(
                schema, table_name, table_def['indexes']
            ):
                full_script += script + "\n"
        
        # 7. FOREIGN KEYS (al final por dependencias)
        if table_def['foreign_keys']:
            full_script += "-- 7. Foreign Keys\n"
            for script in SchemaBuilder.generate_foreign_keys_scripts(
                schema, table_name, table_def['foreign_keys']
            ):
                full_script += script + "\n"
        
        # 8. TRIGGERS
        if table_def['triggers']:
            full_script += "-- 8. Triggers\n"
            for script in SchemaBuilder.generate_triggers_scripts(
                schema, table_name, table_def['triggers']
            ):
                full_script += script + "\n"
        
        return full_script


def create_table_as_mirror(source_db: DatabaseConnection, 
                          dest_db: DatabaseConnection,
                          schema: str, table_name: str) -> bool:
    """
    Crea una tabla en destino como ESPEJO PERFECTO de origen
    """
    try:
        logger.info(f"Creando espejo perfecto de [{schema}].[{table_name}]")
        
        # 1. Extraer definición completa de origen
        extractor = SchemaExtractor(source_db)
        table_def = extractor.get_table_definition(schema, table_name)
        
        # 2. Generar script DDL completo
        ddl_script = SchemaBuilder.generate_full_table_script(table_def)
        
        # 3. Ejecutar en destino dentro de transacción
        dest_db.begin_transaction()
        
        # Dividir por GO si existe y ejecutar cada batch
        batches = ddl_script.split('\nGO\n')
        
        for batch in batches:
            batch = batch.strip()
            if batch:
                dest_db.execute_non_query(batch, commit=False)
        
        dest_db.commit()
        
        logger.info(f"Tabla [{schema}].[{table_name}] creada exitosamente como espejo perfecto")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear espejo de tabla [{schema}].[{table_name}]: {e}")
        dest_db.rollback()
        raise
