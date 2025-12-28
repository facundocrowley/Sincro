"""
Interfaz de usuario con PySide6
Aplicación de escritorio profesional para sincronización SQL Server
"""

import sys
import logging
from datetime import datetime
from typing import List, Optional
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QTableWidget, QTableWidgetItem,
    QTextEdit, QGroupBox, QCheckBox, QProgressBar, QMessageBox,
    QHeaderView, QTabWidget, QComboBox, QDialog, QDialogButtonBox,
    QFormLayout, QSplitter
)
from PySide6.QtCore import Qt, QThread, Signal, Slot
from PySide6.QtGui import QColor, QFont

from config import DBConfig, TableSyncConfig
from db import DatabaseConnection
from sync import SyncOrchestrator

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QTextEditLogger(logging.Handler):
    """Handler personalizado para mostrar logs en QTextEdit"""
    
    def __init__(self, text_edit: QTextEdit):
        super().__init__()
        self.text_edit = text_edit
    
    def emit(self, record):
        msg = self.format(record)
        self.text_edit.append(msg)
        # Auto-scroll
        self.text_edit.verticalScrollBar().setValue(
            self.text_edit.verticalScrollBar().maximum()
        )


class ConnectionDialog(QDialog):
    """Diálogo para configurar conexión a base de datos"""
    
    def __init__(self, title: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumWidth(400)
        
        self.config = DBConfig()
        self._init_ui()
    
    def _init_ui(self):
        layout = QFormLayout()
        
        self.txt_server = QLineEdit()
        self.txt_server.setPlaceholderText("localhost\\SQLEXPRESS")
        layout.addRow("Servidor:", self.txt_server)
        
        self.txt_database = QLineEdit()
        self.txt_database.setPlaceholderText("NombreBaseDatos")
        layout.addRow("Base de Datos:", self.txt_database)
        
        self.chk_windows_auth = QCheckBox("Usar autenticación de Windows")
        self.chk_windows_auth.setChecked(True)
        self.chk_windows_auth.toggled.connect(self._on_auth_changed)
        layout.addRow("", self.chk_windows_auth)
        
        self.txt_username = QLineEdit()
        self.txt_username.setEnabled(False)
        layout.addRow("Usuario:", self.txt_username)
        
        self.txt_password = QLineEdit()
        self.txt_password.setEchoMode(QLineEdit.Password)
        self.txt_password.setEnabled(False)
        layout.addRow("Contraseña:", self.txt_password)
        
        # Botones
        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        buttons.accepted.connect(self._on_accept)
        buttons.rejected.connect(self.reject)
        
        main_layout = QVBoxLayout()
        main_layout.addLayout(layout)
        main_layout.addWidget(buttons)
        
        self.setLayout(main_layout)
    
    def _on_auth_changed(self, checked):
        self.txt_username.setEnabled(not checked)
        self.txt_password.setEnabled(not checked)
    
    def _on_accept(self):
        # Validar
        if not self.txt_server.text().strip():
            QMessageBox.warning(self, "Validación", "Debe ingresar el servidor")
            return
        
        if not self.txt_database.text().strip():
            QMessageBox.warning(self, "Validación", "Debe ingresar la base de datos")
            return
        
        if not self.chk_windows_auth.isChecked():
            if not self.txt_username.text().strip():
                QMessageBox.warning(self, "Validación", "Debe ingresar el usuario")
                return
        
        # Guardar configuración
        self.config.server = self.txt_server.text().strip()
        self.config.database = self.txt_database.text().strip()
        self.config.use_windows_auth = self.chk_windows_auth.isChecked()
        self.config.username = self.txt_username.text().strip()
        self.config.password = self.txt_password.text().strip()
        
        # Probar conexión
        try:
            with DatabaseConnection(self.config):
                pass
            self.accept()
        except Exception as e:
            QMessageBox.critical(
                self, "Error de Conexión",
                f"No se pudo conectar a la base de datos:\n{str(e)}"
            )


class TableConfigDialog(QDialog):
    """Diálogo para configurar PK y WHERE de una tabla"""
    
    def __init__(self, table_config: TableSyncConfig, parent=None):
        super().__init__(parent)
        self.table_config = table_config
        self.setWindowTitle(f"Configurar {table_config.full_name}")
        self.setMinimumWidth(500)
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout()
        
        # Primary Keys
        pk_group = QGroupBox("Columnas de Clave Primaria")
        pk_layout = QVBoxLayout()
        
        pk_info = QLabel("Columnas detectadas automáticamente:" if self.table_config.pk_auto_detected 
                        else "Columnas configuradas manualmente:")
        pk_info.setStyleSheet("font-style: italic;")
        pk_layout.addWidget(pk_info)
        
        self.txt_pk = QLineEdit()
        self.txt_pk.setText(", ".join(self.table_config.primary_key_columns))
        self.txt_pk.setPlaceholderText("col1, col2, col3")
        pk_layout.addWidget(self.txt_pk)
        
        pk_help = QLabel("Separar múltiples columnas por comas")
        pk_help.setStyleSheet("color: gray; font-size: 10px;")
        pk_layout.addWidget(pk_help)
        
        pk_group.setLayout(pk_layout)
        layout.addWidget(pk_group)
        
        # WHERE Clause
        where_group = QGroupBox("Filtro WHERE (opcional)")
        where_layout = QVBoxLayout()
        
        self.txt_where = QLineEdit()
        self.txt_where.setText(self.table_config.where_clause)
        self.txt_where.setPlaceholderText("Sucursal = 1 AND Activo = 1")
        where_layout.addWidget(self.txt_where)
        
        where_help = QLabel("No incluir la palabra WHERE, solo la condición")
        where_help.setStyleSheet("color: gray; font-size: 10px;")
        where_layout.addWidget(where_help)
        
        where_group.setLayout(where_layout)
        layout.addWidget(where_group)
        
        # Botones
        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        buttons.accepted.connect(self._on_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        
        self.setLayout(layout)
    
    def _on_accept(self):
        # Validar PK
        pk_text = self.txt_pk.text().strip()
        if not pk_text:
            QMessageBox.warning(self, "Validación", 
                              "Debe especificar al menos una columna PK")
            return
        
        # Actualizar configuración
        self.table_config.primary_key_columns = [
            col.strip() for col in pk_text.split(',') if col.strip()
        ]
        self.table_config.pk_auto_detected = False
        self.table_config.where_clause = self.txt_where.text().strip()
        
        self.accept()


class SyncWorker(QThread):
    """Worker thread para ejecutar sincronización sin bloquear UI"""
    
    progress = Signal(str, int, int, str)  # table_name, current, total, status
    record_progress = Signal(str, int, int, int)  # status, current_records, total_records, percentage
    finished = Signal(dict)  # stats
    error = Signal(str)
    
    def __init__(self, source_config: DBConfig, dest_config: DBConfig,
                 table_configs: List[TableSyncConfig]):
        super().__init__()
        self.source_config = source_config
        self.dest_config = dest_config
        self.table_configs = table_configs
    
    def run(self):
        try:
            with DatabaseConnection(self.source_config) as source_db, \
                 DatabaseConnection(self.dest_config) as dest_db:
                
                orchestrator = SyncOrchestrator(source_db, dest_db)
                
                stats = orchestrator.synchronize_tables(
                    self.table_configs,
                    callback=self._progress_callback
                )
                
                self.finished.emit(stats)
                
        except Exception as e:
            logger.error(f"Error en sincronización: {e}")
            self.error.emit(str(e))
    
    def _progress_callback(self, table_name_or_status: str, current: int, 
                          total: int, status_or_pct: str):
        # Si es progreso de registros
        if table_name_or_status == 'PROGRESS':
            self.record_progress.emit('PROGRESS', current, total, status_or_pct)
        else:
            # Progreso de tabla
            self.progress.emit(table_name_or_status, current, total, status_or_pct)


class MainWindow(QMainWindow):
    """Ventana principal de la aplicación"""
    
    def __init__(self):
        super().__init__()
        
        self.source_config: Optional[DBConfig] = None
        self.dest_config: Optional[DBConfig] = None
        self.source_db: Optional[DatabaseConnection] = None
        self.dest_db: Optional[DatabaseConnection] = None
        self.table_configs: List[TableSyncConfig] = []
        self.sync_worker: Optional[SyncWorker] = None
        
        self._init_ui()
        self._setup_logging()
    
    def _init_ui(self):
        self.setWindowTitle("SQL Server Database Synchronizer")
        self.setMinimumSize(1200, 800)
        
        # Widget central
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # === Sección de Conexiones ===
        conn_layout = QHBoxLayout()
        
        # Origen
        source_group = self._create_connection_group("Base de Datos ORIGEN", True)
        conn_layout.addWidget(source_group)
        
        # Destino
        dest_group = self._create_connection_group("Base de Datos DESTINO", False)
        conn_layout.addWidget(dest_group)
        
        main_layout.addLayout(conn_layout)
        
        # === Botón Cargar Tablas ===
        self.btn_load_tables = QPushButton("📋 Cargar Tablas")
        self.btn_load_tables.setEnabled(False)
        self.btn_load_tables.clicked.connect(self._load_tables)
        self.btn_load_tables.setMinimumHeight(40)
        main_layout.addWidget(self.btn_load_tables)
        
        # === Tabs ===
        tabs = QTabWidget()
        
        # Tab 1: Tablas
        tab_tables = self._create_tables_tab()
        tabs.addTab(tab_tables, "📊 Tablas")
        
        # Tab 2: Log
        tab_log = self._create_log_tab()
        tabs.addTab(tab_log, "📜 Log")
        
        main_layout.addWidget(tabs)
        
        # === Barras de Progreso ===
        # Progreso de tablas
        progress_layout = QVBoxLayout()
        
        self.lbl_progress_table = QLabel("")
        self.lbl_progress_table.setVisible(False)
        progress_layout.addWidget(self.lbl_progress_table)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        progress_layout.addWidget(self.progress_bar)
        
        # Progreso de registros dentro de tabla actual
        self.lbl_progress_records = QLabel("")
        self.lbl_progress_records.setVisible(False)
        progress_layout.addWidget(self.lbl_progress_records)
        
        self.progress_bar_records = QProgressBar()
        self.progress_bar_records.setVisible(False)
        progress_layout.addWidget(self.progress_bar_records)
        
        main_layout.addLayout(progress_layout)
        
        # === Botones de Acción ===
        action_layout = QHBoxLayout()
        
        self.btn_sync = QPushButton("🔄 SINCRONIZAR")
        self.btn_sync.setEnabled(False)
        self.btn_sync.clicked.connect(self._start_sync)
        self.btn_sync.setMinimumHeight(50)
        font = QFont()
        font.setPointSize(12)
        font.setBold(True)
        self.btn_sync.setFont(font)
        action_layout.addWidget(self.btn_sync)
        
        main_layout.addLayout(action_layout)
        
        # Status bar
        self.statusBar().showMessage("Listo")
    
    def _create_connection_group(self, title: str, is_source: bool) -> QGroupBox:
        """Crea grupo de configuración de conexión"""
        group = QGroupBox(title)
        layout = QVBoxLayout()
        
        # Label de estado
        label = QLabel("❌ No configurado")
        label.setStyleSheet("font-weight: bold; color: red;")
        layout.addWidget(label)
        
        # Botón configurar
        btn = QPushButton("⚙️ Configurar Conexión")
        if is_source:
            self.lbl_source_status = label
            btn.clicked.connect(lambda: self._configure_connection(True))
        else:
            self.lbl_dest_status = label
            btn.clicked.connect(lambda: self._configure_connection(False))
        
        layout.addWidget(btn)
        
        group.setLayout(layout)
        return group
    
    def _create_tables_tab(self) -> QWidget:
        """Crea tab de tablas"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Barra de herramientas
        toolbar = QHBoxLayout()
        
        btn_select_all = QPushButton("☑️ Seleccionar Todas")
        btn_select_all.clicked.connect(self._select_all_tables)
        toolbar.addWidget(btn_select_all)
        
        btn_deselect_all = QPushButton("☐ Deseleccionar Todas")
        btn_deselect_all.clicked.connect(self._deselect_all_tables)
        toolbar.addWidget(btn_deselect_all)
        
        toolbar.addStretch()
        
        self.lbl_table_count = QLabel("0 tablas cargadas")
        toolbar.addWidget(self.lbl_table_count)
        
        layout.addLayout(toolbar)
        
        # Tabla
        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(7)
        self.table_widget.setHorizontalHeaderLabels([
            "✓", "Esquema", "Tabla", "Registros", "PK", "WHERE", "Configurar"
        ])
        
        header = self.table_widget.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.Stretch)
        header.setSectionResizeMode(5, QHeaderView.Stretch)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        
        self.table_widget.setAlternatingRowColors(True)
        layout.addWidget(self.table_widget)
        
        widget.setLayout(layout)
        return widget
    
    def _create_log_tab(self) -> QWidget:
        """Crea tab de log"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Botón limpiar
        btn_clear = QPushButton("🗑️ Limpiar Log")
        btn_clear.clicked.connect(self._clear_log)
        layout.addWidget(btn_clear)
        
        # Text edit para log
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Courier New", 9))
        layout.addWidget(self.log_text)
        
        widget.setLayout(layout)
        return widget
    
    def _setup_logging(self):
        """Configura handler de logging para mostrar en UI"""
        handler = QTextEditLogger(self.log_text)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        ))
        logging.getLogger().addHandler(handler)
    
    @Slot()
    def _configure_connection(self, is_source: bool):
        """Configura conexión a BD"""
        title = "Configurar Base de Datos ORIGEN" if is_source else "Configurar Base de Datos DESTINO"
        
        dialog = ConnectionDialog(title, self)
        if dialog.exec():
            if is_source:
                self.source_config = dialog.config
                self.lbl_source_status.setText(
                    f"✅ {dialog.config.server}\\{dialog.config.database}"
                )
                self.lbl_source_status.setStyleSheet("font-weight: bold; color: green;")
            else:
                self.dest_config = dialog.config
                self.lbl_dest_status.setText(
                    f"✅ {dialog.config.server}\\{dialog.config.database}"
                )
                self.lbl_dest_status.setStyleSheet("font-weight: bold; color: green;")
            
            # Habilitar botón de cargar tablas si ambas están configuradas
            if self.source_config and self.dest_config:
                self.btn_load_tables.setEnabled(True)
    
    @Slot()
    def _load_tables(self):
        """Carga lista de tablas de origen"""
        try:
            self.statusBar().showMessage("Cargando tablas...")
            QApplication.setOverrideCursor(Qt.WaitCursor)
            
            with DatabaseConnection(self.source_config) as db:
                tables = db.get_tables()
                
                # Detectar PKs para cada tabla
                self.table_configs.clear()
                self.table_widget.setRowCount(0)
                
                for table in tables:
                    config = TableSyncConfig(table['schema'], table['table'])
                    config.is_selected = False
                    
                    # Detectar PK automáticamente
                    pk_query = """
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
                    pk_rows = db.execute_query(pk_query, (config.schema, config.table_name))
                    if pk_rows:
                        config.primary_key_columns = [row[0] for row in pk_rows]
                        config.pk_auto_detected = True
                    
                    self.table_configs.append(config)
                    self._add_table_row(config, table['row_count'])
            
            self.lbl_table_count.setText(f"{len(tables)} tablas cargadas")
            self.btn_sync.setEnabled(True)
            self.statusBar().showMessage("Tablas cargadas exitosamente", 3000)
            logger.info(f"Cargadas {len(tables)} tablas desde {self.source_config.database}")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar tablas:\n{str(e)}")
            logger.error(f"Error al cargar tablas: {e}")
        finally:
            QApplication.restoreOverrideCursor()
    
    def _add_table_row(self, config: TableSyncConfig, row_count: int):
        """Agrega fila a la tabla de tablas"""
        row = self.table_widget.rowCount()
        self.table_widget.insertRow(row)
        
        # Checkbox
        chk = QCheckBox()
        chk.setChecked(config.is_selected)
        chk.stateChanged.connect(
            lambda state, cfg=config: self._on_table_selection_changed(cfg, state)
        )
        cell_widget = QWidget()
        cell_layout = QHBoxLayout(cell_widget)
        cell_layout.addWidget(chk)
        cell_layout.setAlignment(Qt.AlignCenter)
        cell_layout.setContentsMargins(0, 0, 0, 0)
        self.table_widget.setCellWidget(row, 0, cell_widget)
        
        # Esquema
        self.table_widget.setItem(row, 1, QTableWidgetItem(config.schema))
        
        # Tabla
        self.table_widget.setItem(row, 2, QTableWidgetItem(config.table_name))
        
        # Registros
        item = QTableWidgetItem(f"{row_count:,}")
        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.table_widget.setItem(row, 3, item)
        
        # PK (mostrar si se detectó)
        pk_text = ", ".join(config.primary_key_columns) if config.primary_key_columns else ""
        self.table_widget.setItem(row, 4, QTableWidgetItem(pk_text))
        
        # WHERE (vacío por ahora)
        self.table_widget.setItem(row, 5, QTableWidgetItem(""))
        
        # Botón configurar
        btn = QPushButton("⚙️")
        btn.clicked.connect(lambda checked, cfg=config, r=row: self._configure_table(cfg, r))
        self.table_widget.setCellWidget(row, 6, btn)
    
    def _on_table_selection_changed(self, config: TableSyncConfig, state):
        """Callback cuando cambia selección de tabla"""
        config.is_selected = (state == Qt.CheckState.Checked.value)
    
    def _configure_table(self, config: TableSyncConfig, row: int):
        """Abre diálogo de configuración de tabla"""
        dialog = TableConfigDialog(config, self)
        if dialog.exec():
            # Actualizar visualización
            pk_text = ", ".join(config.primary_key_columns)
            self.table_widget.setItem(row, 4, QTableWidgetItem(pk_text))
            self.table_widget.setItem(row, 5, QTableWidgetItem(config.where_clause))
    
    @Slot()
    def _select_all_tables(self):
        """Selecciona todas las tablas"""
        for i in range(self.table_widget.rowCount()):
            widget = self.table_widget.cellWidget(i, 0)
            chk = widget.findChild(QCheckBox)
            if chk:
                chk.setChecked(True)
    
    @Slot()
    def _deselect_all_tables(self):
        """Deselecciona todas las tablas"""
        for i in range(self.table_widget.rowCount()):
            widget = self.table_widget.cellWidget(i, 0)
            chk = widget.findChild(QCheckBox)
            if chk:
                chk.setChecked(False)
    
    @Slot()
    def _start_sync(self):
        """Inicia proceso de sincronización"""
        # Validar que hay tablas seleccionadas
        selected = [cfg for cfg in self.table_configs if cfg.is_selected]
        
        if not selected:
            QMessageBox.warning(self, "Validación", 
                              "Debe seleccionar al menos una tabla para sincronizar")
            return
        
        # Confirmar
        reply = QMessageBox.question(
            self, "Confirmar Sincronización",
            f"¿Desea sincronizar {len(selected)} tabla(s)?\n\n"
            f"Esto realizará operaciones INSERT/UPDATE/DELETE en la base de datos destino.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Deshabilitar controles
        self.btn_sync.setEnabled(False)
        self.btn_load_tables.setEnabled(False)
        
        # Mostrar barras de progreso
        self.lbl_progress_table.setVisible(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(len(selected))
        self.progress_bar.setValue(0)
        
        self.lbl_progress_records.setVisible(True)
        self.progress_bar_records.setVisible(True)
        self.progress_bar_records.setValue(0)
        
        # Iniciar worker
        self.sync_worker = SyncWorker(self.source_config, self.dest_config, selected)
        self.sync_worker.progress.connect(self._on_sync_progress)
        self.sync_worker.record_progress.connect(self._on_record_progress)
        self.sync_worker.finished.connect(self._on_sync_finished)
        self.sync_worker.error.connect(self._on_sync_error)
        self.sync_worker.start()
        
        self.statusBar().showMessage("Sincronizando...")
        logger.info(f"=== Iniciando sincronización de {len(selected)} tablas ===")
    
    @Slot(str, int, int, str)
    def _on_sync_progress(self, table_name: str, current: int, total: int, status: str):
        """Actualiza progreso de sincronización"""
        self.progress_bar.setValue(current)
        self.lbl_progress_table.setText(f"Tabla {current} de {total}: {table_name}")
        self.statusBar().showMessage(f"Sincronizando {table_name} ({current}/{total})")
        
        # Reset progreso de registros para nueva tabla
        if status == 'SYNCING':
            self.progress_bar_records.setValue(0)
            self.lbl_progress_records.setText("Procesando...")
        
        if status == 'SUCCESS':
            logger.info(f"✅ {table_name} sincronizada exitosamente")
        elif status == 'ERROR':
            logger.error(f"❌ Error en {table_name}")
    
    @Slot(str, int, int, int)
    def _on_record_progress(self, status: str, current: int, total: int, percentage: int):
        """Actualiza progreso de inserción de registros"""
        self.progress_bar_records.setMaximum(100)
        self.progress_bar_records.setValue(percentage)
        self.lbl_progress_records.setText(
            f"Insertando registros: {current:,} de {total:,} ({percentage}%)"
        )
    
    @Slot(dict)
    def _on_sync_finished(self, stats: dict):
        """Callback cuando finaliza sincronización"""
        # Ocultar barras de progreso
        self.progress_bar.setVisible(False)
        self.lbl_progress_table.setVisible(False)
        self.progress_bar_records.setVisible(False)
        self.lbl_progress_records.setVisible(False)
        
        self.btn_sync.setEnabled(True)
        self.btn_load_tables.setEnabled(True)
        
        # Mostrar resumen
        msg = f"""
Sincronización Completada

Tablas exitosas: {stats['successful']}
Tablas con error: {stats['failed']}

Total insertados: {stats['total_inserted']:,}
Total actualizados: {stats['total_updated']:,}
Total eliminados: {stats['total_deleted']:,}
        """
        
        if stats['errors']:
            msg += f"\n\nErrores:\n" + "\n".join(stats['errors'][:5])
            if len(stats['errors']) > 5:
                msg += f"\n... y {len(stats['errors']) - 5} más"
        
        QMessageBox.information(self, "Sincronización Completada", msg)
        
        logger.info(f"=== Sincronización completada: {stats['successful']} exitosas, {stats['failed']} fallidas ===")
        self.statusBar().showMessage("Sincronización completada", 5000)
    
    @Slot(str)
    def _on_sync_error(self, error_msg: str):
        """Callback cuando hay error en sincronización"""
        # Ocultar barras de progreso
        self.progress_bar.setVisible(False)
        self.lbl_progress_table.setVisible(False)
        self.progress_bar_records.setVisible(False)
        self.lbl_progress_records.setVisible(False)
        
        self.btn_sync.setEnabled(True)
        self.btn_load_tables.setEnabled(True)
        
        QMessageBox.critical(self, "Error de Sincronización", 
                           f"Error durante la sincronización:\n\n{error_msg}")
        
        self.statusBar().showMessage("Error en sincronización", 5000)
    
    @Slot()
    def _clear_log(self):
        """Limpia el log"""
        self.log_text.clear()


def main():
    """Punto de entrada de la aplicación"""
    app = QApplication(sys.argv)
    
    # Estilo
    app.setStyle("Fusion")
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
