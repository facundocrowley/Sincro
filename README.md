# 🔄 SQL Server Database Synchronizer

**Sincronizador bidireccional profesional de bases de datos SQL Server**

Sistema de sincronización incremental con creación de tablas como **ESPEJO PERFECTO**, optimizado para entornos empresariales críticos.

---

## 🎯 Características Principales

### ✅ Espejo Perfecto de Estructura
- Replica **EXACTAMENTE** la estructura de tablas origen → destino
- Incluye:
  - ✓ Columnas (tipos, longitudes, precision, NULL/NOT NULL)
  - ✓ IDENTITY (seed, increment)
  - ✓ ROWVERSION / TIMESTAMP
  - ✓ Columnas computadas (COMPUTED COLUMNS)
  - ✓ COLLATION por columna
  - ✓ Primary Keys (con orden exacto)
  - ✓ Índices (UNIQUE, CLUSTERED, NONCLUSTERED, INCLUDE columns)
  - ✓ Foreign Keys (con ON DELETE/UPDATE)
  - ✓ CHECK Constraints
  - ✓ DEFAULT Constraints
  - ✓ UNIQUE Constraints
  - ✓ Triggers

### ⚡ Sincronización Incremental Optimizada
- **NO** elimina y recarga datos
- Detección inteligente de cambios:
  - **ROWVERSION**: Delta real para máxima velocidad
  - **HASH**: Cuando no hay ROWVERSION
- Operaciones precisas:
  - **INSERT**: Solo registros nuevos
  - **UPDATE**: Solo registros modificados
  - **DELETE**: Solo registros eliminados en origen

### 🔑 Gestión Inteligente de Claves Primarias
- Detección automática de PKs desde metadatos SQL Server
- Soporte para claves primarias compuestas
- Override manual de PKs por tabla
- Respeta PKs en todas las operaciones

### 🎛️ Filtros Personalizables
- WHERE clause configurable por tabla
- Ejemplo: `Sucursal = 1 AND Activo = 1`
- Aplica en INSERT/UPDATE/DELETE

### 📊 Interfaz Profesional (PySide6/Qt)
- Configuración visual de conexiones (Origen/Destino)
- Selección múltiple de tablas
- Configuración individual de PK y WHERE por tabla
- Log en tiempo real
- Barra de progreso
- Sin bloqueo de UI (threading)

### 🧠 Metadatos y Auditoría
- Tabla `SyncMetadata` en destino
- Tracking de:
  - Último ROWVERSION sincronizado
  - Fecha de última sincronización
  - Contadores de INSERT/UPDATE/DELETE
  - Errores y warnings
  - Configuración de PKs y WHERE

---

## 📋 Requisitos

### Software
- **Python 3.8+**
- **SQL Server 2016+** (ambas instancias)
- **ODBC Driver 17 for SQL Server** (o superior)

### Instalación de Driver ODBC
Descargar desde: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

---

## 🚀 Instalación

### 1. Clonar/Descargar el proyecto
```bash
cd C:\Python\Sincro
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Ejecutar la aplicación
```bash
python main.py
```

---

## 📖 Uso

### 1️⃣ Configurar Conexiones

**Base de Datos ORIGEN:**
- Click en "⚙️ Configurar Conexión"
- Ingresar:
  - Servidor (ej: `localhost\SQLEXPRESS` o `192.168.1.100`)
  - Base de Datos
  - Autenticación (Windows o SQL Server)

**Base de Datos DESTINO:**
- Repetir proceso para BD destino

### 2️⃣ Cargar Tablas
- Click en "📋 Cargar Tablas"
- Se cargan todas las tablas de la BD origen

### 3️⃣ Seleccionar Tablas
- ✓ Marcar las tablas a sincronizar
- O usar "☑️ Seleccionar Todas"

### 4️⃣ Configurar Tablas (Opcional)
Para cada tabla, click en botón "⚙️":
- **Primary Key Columns**: Modificar si necesario (separar por comas)
- **WHERE Clause**: Agregar filtro opcional (sin la palabra WHERE)
  - Ejemplo: `Sucursal = 1`
  - Ejemplo: `FechaCreacion >= '2024-01-01' AND Estado = 'A'`

### 5️⃣ Sincronizar
- Click en "🔄 SINCRONIZAR"
- Confirmar operación
- Ver progreso en tiempo real
- Revisar log detallado en pestaña "📜 Log"

---

## 🏗️ Arquitectura

### Módulos

```
Sincro/
├── main.py           # Punto de entrada
├── ui.py             # Interfaz gráfica (PySide6)
├── config.py         # Configuraciones y constantes
├── db.py             # Gestión de conexiones y queries
├── schema.py         # Extracción y creación de esquema
├── sync.py           # Lógica de sincronización
├── metadata.py       # Tabla de control SyncMetadata
└── requirements.txt  # Dependencias
```

### Flujo de Sincronización

```
1. Usuario selecciona tablas y configura
2. Para cada tabla:
   a. Verificar si existe en destino
      - NO → Crear como ESPEJO PERFECTO
      - SÍ → Continuar
   b. Detectar estrategia de cambios (ROWVERSION vs HASH)
   c. Detectar/validar Primary Key
   d. Ejecutar sincronización incremental:
      - INSERT: Registros en origen pero no en destino
      - UPDATE: Registros modificados (según ROWVERSION/HASH)
      - DELETE: Registros en destino pero no en origen
   e. Actualizar metadatos (SyncMetadata)
3. Reportar estadísticas finales
```

### Detección de Cambios

**Modo ROWVERSION** (Óptimo):
```sql
-- Solo procesa registros modificados desde última sync
WHERE [RowVersion] > @LastRowVersionSynced
```

**Modo HASH** (Fallback):
```sql
-- Compara hash de todas las columnas
WHERE HASHBYTES('SHA2_256', CONCAT(col1, col2, ...)) != ...
```

---

## 🔒 Seguridad y Transacciones

- ✅ Todas las operaciones dentro de transacciones
- ✅ ROLLBACK automático ante errores
- ✅ Logging detallado de todas las operaciones
- ✅ Validación de conexiones antes de sincronizar
- ✅ Respeto por constraints y FKs (orden de creación)

---

## ⚙️ Configuración Avanzada

### Constantes en `config.py`

```python
BATCH_SIZE = 1000              # Registros por batch
MAX_PARALLEL_TABLES = 5        # Tablas en paralelo (futuro)
CONNECTION_TIMEOUT = 30        # Timeout conexión (segundos)
COMMAND_TIMEOUT = 300          # Timeout query (segundos)
```

---

## 📊 Tabla de Metadatos

La aplicación crea automáticamente `dbo.SyncMetadata` en destino:

```sql
CREATE TABLE dbo.SyncMetadata (
    id INT IDENTITY(1,1) PRIMARY KEY,
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
    
    -- Configuración
    primary_key_columns NVARCHAR(MAX),
    pk_auto_detected BIT,
    where_clause NVARCHAR(MAX),
    
    -- Tracking
    change_detection_strategy NVARCHAR(50),
    rowversion_column NVARCHAR(128),
    last_rowversion_synced BINARY(8),
    last_hash_synced NVARCHAR(64),
    
    -- Estadísticas
    last_sync_date DATETIME2,
    last_sync_status NVARCHAR(50),
    records_inserted INT,
    records_updated INT,
    records_deleted INT,
    
    -- Errores
    last_error_message NVARCHAR(MAX),
    last_error_date DATETIME2,
    
    -- Auditoría
    created_date DATETIME2 DEFAULT GETDATE(),
    modified_date DATETIME2 DEFAULT GETDATE()
)
```

---

## 🐛 Resolución de Problemas

### Error: "ODBC Driver not found"
**Solución**: Instalar ODBC Driver 17 for SQL Server

### Error: "Login failed for user"
**Solución**: Verificar credenciales y permisos en SQL Server

### Error: "Foreign Key constraint failed"
**Solución**: Las FKs se crean respetando dependencias. Sincronizar tablas referenciadas primero.

### Sincronización muy lenta
**Soluciones**:
- Verificar índices en tablas grandes
- Aumentar `BATCH_SIZE` en config.py
- Usar filtro WHERE para reducir datos
- Verificar red entre servidores

---

## 🎯 Casos de Uso

### 1. Sincronización de Sucursales
```
WHERE Sucursal = 1
```

### 2. Sincronización de Datos Activos
```
WHERE Estado = 'A' AND FechaBaja IS NULL
```

### 3. Sincronización por Rango de Fechas
```
WHERE FechaCreacion >= '2024-01-01'
```

### 4. Sincronización Completa (Sin Filtro)
Dejar WHERE vacío

---

## 📝 Notas Importantes

⚠️ **IMPORTANTE**: Esta herramienta realiza operaciones de **escritura** en la base de datos destino (INSERT/UPDATE/DELETE). 

✅ **Recomendaciones**:
- Hacer **BACKUP** de BD destino antes de primera sincronización
- Probar primero en ambiente de desarrollo
- Revisar log detalladamente
- Validar resultados post-sincronización

---

## 🔮 Mejoras Futuras Potenciales

- [ ] Sincronización en paralelo de múltiples tablas
- [ ] Scheduling automático (cron/tareas programadas)
- [ ] Exportar/Importar configuración de sincronización
- [ ] Comparación de esquemas (schema drift detection)
- [ ] Sincronización bidireccional (conflict resolution)
- [ ] Soporte para Linked Servers
- [ ] Compresión de datos en tránsito
- [ ] Dashboard de monitoreo

---

## 👨‍💻 Autor

Sincronizador SQL Server - 2024

**Arquitectura**: Modular, escalable, preparado para producción

**Stack**: Python 3, PySide6, pyodbc, SQL Server

---

## 📄 Licencia

Este proyecto es de uso interno. Todos los derechos reservados.

---

## 🆘 Soporte

Para reportar problemas o solicitar features, revisar logs en la pestaña "📜 Log" de la aplicación.

**Logging adicional**: Los logs también se muestran en la consola de Python si se ejecuta desde terminal.

---

**¡Sincronización profesional de bases de datos SQL Server!** 🚀
