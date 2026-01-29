#!/usr/bin/env python-sirius

import sys
import time
import datetime
from PyQt5.QtWidgets import (QApplication, QWidget, QPushButton, QVBoxLayout, QLineEdit, QTextEdit, QLabel, QSpinBox, QProgressBar, QHBoxLayout)
from PyQt5.QtCore import QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtCore import QUrl
from siriuspy.clientarch import ClientArchiver 
from PyQt5.QtCore import Qt
import epics
import urllib.parse
import random


class MonitorThread(QThread):
    updt_pv_log = pyqtSignal(list)
    update_status = pyqtSignal(str)
    frozen_pvs_signal = pyqtSignal(list)
    frozen_check_duration_signal = pyqtSignal(int)
    progress_update = pyqtSignal(int)
    not_stored_pvs_signal = pyqtSignal(list)

    ignored_pvs = [
        'RAD:Thermo3:TotalDoseRate:Dose',
        'RAD:Thermo6:TotalDoseRate:Dose',
        'RAD:Thermo12:TotalDoseRate:Dose',
    ]

    set_point_suffixes = ['-SP', '-Setpoint', '-SP:RBV']

    def __init__(self, parent=None, filters=None, check_interval=10, frozen_check_duration=2*60, initial_check_delay=10*60):
        super(MonitorThread, self).__init__(parent)
        self.conn = None
        self.filters = filters or []
        self.pv_values = {}
        self.pv_monitors = {}
        self.check_interval = check_interval
        self.frozen_check_duration = frozen_check_duration
        self.initial_check_delay = initial_check_delay
        self.start_time = None
        self.timer = None
        self.running = False
        self.is_first_check = True

    def stop(self):
        self.running = False
        if self.timer:
            self.timer.stop()
        self.update_status.emit("Monitoramento interrompido.")

    def run(self):
        self.running = True
        self.is_first_check = True
        self.progress_update.emit(0)

        self.update_status.emit("Conectando ao banco de dados...")
        self.conn = self.create_connector()
        if not self.running: return
        if not self.conn:
            self.update_status.emit("Não foi possível conectar ao banco de dados.")
            self.progress_update.emit(100)
            return
        self.progress_update.emit(5)

        self.update_status.emit("Obtendo todas as PVs do Archiver...")
        all_pvnames = self.get_all_pvnames()
        if not self.running: return
        if not all_pvnames:
            self.update_status.emit("Nenhuma PV encontrada no servidor Archiver.")
            self.progress_update.emit(100)
            return
        self.progress_update.emit(15)

        self.update_status.emit("Filtrando PVs...")
        filtered_pvnames = self.filter_pvnames(all_pvnames, base_progress=15, weight=25)
        if not self.running: return

        if not filtered_pvnames:
            self.update_status.emit("Nenhuma PV encontrada após a filtragem com os critérios fornecidos.")
            self.progress_update.emit(100)
            self.update_status.emit("Processamento Concluído (nenhuma PV para monitorar).")
            return

        self.updt_pv_log.emit(filtered_pvnames)
        self.update_status.emit(f"Listagem e filtragem concluídas. {len(filtered_pvnames)} PVs para monitorar.")

        self.update_status.emit("Iniciando monitores para PVs filtradas...")
        base_monitor_progress = 40
        weight_monitor_progress = 30
        num_filtered_pvs = len(filtered_pvnames)

        if num_filtered_pvs > 0:
            for i, pvname in enumerate(filtered_pvnames):
                if not self.running:
                    self.update_status.emit("Monitoramento interrompido durante o início dos monitores.")
                    return
                self.start_pv_monitor(pvname)
                progress = base_monitor_progress + int(((i + 1) / num_filtered_pvs) * weight_monitor_progress)
                self.progress_update.emit(progress)
        else:
            self.progress_update.emit(base_monitor_progress + weight_monitor_progress)

        if not self.running: return

        self.update_status.emit("Aguardando estabilização inicial dos monitores...")
        time.sleep(2)
        self.progress_update.emit(random.randint(71, 90))


        self.update_status.emit("Realizando verificação inicial de congelamento...")
        self.check_pvs(is_initial_check=True, max_pvs=200)
        self.progress_update.emit(100)
        self.update_status.emit("Processamento Concluído.")

        if self.running:
            self.start_time = time.time()
            self.start_frozen_check()

    def create_connector(self, timeout=1):
        conn = ClientArchiver()
        t0 = time.time()
        while time.time() - t0 < timeout:
            if not self.running: return None
            if conn.connected:
                return conn
            time.sleep(0.1)
        return None

    def get_all_pvnames(self):
        if self.conn and self.running:
            try:
                return self.conn.getAllPVs('*')
            except Exception as e:
                self.update_status.emit(f"Erro ao obter PVs: {e}")
                return None
        return None

    def filter_pvnames(self, all_pvnames, base_progress=0, weight=100):
        filtered_pvnames = []
        if not self.filters or len(self.filters) < 2:
            if self.running: self.progress_update.emit(base_progress + weight)
            return all_pvnames

        filter1 = self.filters[0]
        filter2 = self.filters[1]

        total_pvs = len(all_pvnames)
        for i, pvname in enumerate(all_pvnames):
            if not self.running: return filtered_pvnames

            if pvname in self.ignored_pvs:
                continue

            if filter1 and not pvname.startswith(filter1): continue
            if filter2 and not pvname.endswith(filter2): continue

            filtered_pvnames.append(pvname)

            if self.running:
                progress = base_progress + int(((i + 1) / total_pvs) * weight)
                self.progress_update.emit(progress)

        self.progress_update.emit(base_progress + weight)
        return filtered_pvnames

    def start_pv_monitor(self, pvname):
        def callback(pvname=None, value=None, **kwargs):
            if pvname not in self.pv_values:
                self.pv_values[pvname] = {"values": [], "timestamps": []}
            self.pv_values[pvname]["values"].append(value)
            self.pv_values[pvname]["timestamps"].append(time.time())

        initial_value = self.get_pv_value(pvname)
        if pvname not in self.pv_values:
            self.pv_values[pvname] = {"values": [], "timestamps": []}
        self.pv_values[pvname]["values"].append(initial_value)
        self.pv_values[pvname]["timestamps"].append(time.time())

        self.pv_monitors[pvname] = epics.camonitor(pvname, callback=callback)

    def stop_pv_monitor(self, pvname):
        if pvname in self.pv_monitors:
            epics.camonitor_clear(self.pv_monitors[pvname])
            del self.pv_monitors[pvname]

    def get_pv_value(self, pvname):
        try:
            return epics.caget(pvname)
        except:
            return None

    def is_set_point_pv(self, pvname):
        return any(pvname.endswith(suffix) for suffix in self.set_point_suffixes)

    def start_frozen_check(self):
        self.update_status.emit("Iniciando monitoramento...")
        self.timer = QTimer()
        self.timer.timeout.connect(self.check_pvs)
        self.timer.setInterval(self.check_interval * 1000)
        self.timer.start()
        self.check_pvs()

    def check_pv_frozen(self, pvname):
        if pvname not in self.pv_values:
            return False

        values = self.pv_values[pvname].get("values", [])
        timestamps = self.pv_values[pvname].get("timestamps", [])
        time.sleep(4)

        if not values or not timestamps:
            return False

        if self.is_set_point_pv(pvname):
            return True

        if values[-1] is None:
            return False

        num_samples = min(60, len(values))
        first_values = values[:num_samples]
        last_values = values[-num_samples:] if len(values) >= num_samples else values

        if len(first_values) < 1 or len(last_values) < 1:
            return False

        all_first_equal = all(v == first_values[0] for v in first_values)
        all_last_equal = all(v == last_values[0] for v in last_values)
        first_last_equal = first_values[0] == last_values[0]

        return all_first_equal and all_last_equal and first_last_equal

    def check_pv_connected(self, pvname):
        if pvname not in self.pv_values:
            return False

        values = self.pv_values[pvname].get("values", [])
        return len(values) > 0 and values[-1] is not None

    def check_pvs(self, is_initial_check=False, max_pvs=None):
        if not self.running: return

        frozen_pvs = []
        disconnected_pvs = []
        ctrl_frozen = 0
        ctrl_disconnected = 0

        pv_names_to_check = list(self.pv_values.keys())
        if is_initial_check and max_pvs is not None:
            pv_names_to_check = pv_names_to_check[:max_pvs]

        for i, pvname in enumerate(pv_names_to_check):
            if not self.running: return

            if not self.check_pv_connected(pvname):
                disconnected_pvs.append(pvname)
                ctrl_disconnected += 1
                continue

            if self.check_pv_frozen(pvname):
                frozen_pvs.append(pvname)
                ctrl_frozen += 1

        self.frozen_pvs_signal.emit(frozen_pvs if frozen_pvs else [])
        self.not_stored_pvs_signal.emit(disconnected_pvs if disconnected_pvs else [])
        self.list_pvs(len(self.pv_values), ctrl_frozen, ctrl_disconnected)

    def list_pvs(self, total_pvs, ctrl_frozen, ctrl_disconnected):
        self.update_status.emit(f"Quantidade de PVs analisadas = {total_pvs}")
        self.update_status.emit(f"Quantidade de PVs congeladas = {ctrl_frozen}")
        self.update_status.emit(f"Quantidade de PVs desconectadas = {ctrl_disconnected}")
        time.sleep(1)
        self.update_status.emit("Análise finalizada.")



class MainWindow(QWidget):
    """GUI com conexão de threads..."""
    def __init__(self):
        super(MainWindow, self).__init__()
        self.monitor_thread = None
        self.generated_url = None
        self.frozen_check_duration = 0 
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle('Análise de PVs')
        self.setGeometry(100, 100, 800, 900)

        layout = QVBoxLayout(self)
        self.frozen_setor_label = QLabel("Digite o setor e sub-setor (ex: SI-01C1):", self)
        layout.addWidget(self.frozen_setor_label)

        self.filter1_input = QLineEdit(self)
        self.filter1_input.setPlaceholderText("Digite o setor e sub-setor (ex: SI-01C1)")
        layout.addWidget(self.filter1_input)

        self.frozen_digPV_label = QLabel("Digite o tipo de PV (ex: Temp-Mon):", self)
        layout.addWidget(self.frozen_digPV_label)

        self.filter2_input = QLineEdit(self)
        self.filter2_input.setPlaceholderText("Digite o tipo de PV (ex: Temp-Mon)")
        layout.addWidget(self.filter2_input)

        self.interval_label = QLabel("G O P  |  L N L S", self)
        self.interval_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.interval_label)
        self.interval_input = QSpinBox(self)
        self.interval_input.setRange(1, 3600) 
        self.interval_input.setValue(10)
        self.interval_input.setEnabled(False)
        self.interval_input.hide()
        layout.addWidget(self.interval_input)

        self.frozen_duration_label = QLabel("O App faz uma análise de pontos das PVs e determina se está congelada ou desconectada!", self)
        
        self.frozen_duration_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.frozen_duration_label)
        self.frozen_duration_input = QSpinBox(self)
        self.frozen_duration_input.setRange(1, 24*60)
        self.frozen_duration_input.setValue(30)
        self.frozen_duration_input.setEnabled(False)
        self.frozen_duration_input.hide()
        layout.addWidget(self.frozen_duration_input)
        self.frozen_durationb_label = QLabel("A quantidade de PVs analisadas é limitada, 200 PVs por rodada.", self)
        self.frozen_durationb_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.frozen_durationb_label)

        h_layout_buttons = QHBoxLayout()
        self.stop_button = QPushButton('Parar Monitoramento', self)
        self.stop_button.clicked.connect(self.stop_monitor)
        h_layout_buttons.addWidget(self.stop_button)

        self.clear_button = QPushButton('Limpar Logs', self)
        self.clear_button.clicked.connect(self.clear_monitor)
        h_layout_buttons.addWidget(self.clear_button)
        layout.addLayout(h_layout_buttons)

        self.list_pv_button = QPushButton('Iniciar Análise (Listar PVs)', self)
        self.list_pv_button.clicked.connect(self.start_monitor_thread)
        layout.addWidget(self.list_pv_button)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.status_label = QLabel("Status: Ocioso", self)
        layout.addWidget(self.status_label)

        h_layout_outputs = QHBoxLayout()
        v_layout_pv_list = QVBoxLayout()
        v_layout_pv_list.addWidget(QLabel("PVs Filtradas:"))
        self.pv_output = QTextEdit(self)
        self.pv_output.setReadOnly(True)
        v_layout_pv_list.addWidget(self.pv_output)
        h_layout_outputs.addLayout(v_layout_pv_list)

        v_layout_frozen_disconnected = QVBoxLayout()
        v_layout_frozen_disconnected.addWidget(QLabel("PVs Congeladas:"))
        self.frozen_pv_output = QTextEdit(self)
        self.frozen_pv_output.setReadOnly(True)
        v_layout_frozen_disconnected.addWidget(self.frozen_pv_output)

        v_layout_frozen_disconnected.addWidget(QLabel("PVs Desconectadas:"))
        self.disconnected_pv_output = QTextEdit(self)
        self.disconnected_pv_output.setReadOnly(True)
        self.disconnected_pv_output.setPlaceholderText("PVs desconectadas aparecerão aqui.")
        v_layout_frozen_disconnected.addWidget(self.disconnected_pv_output)
        h_layout_outputs.addLayout(v_layout_frozen_disconnected)
        
        layout.addLayout(h_layout_outputs)

        self.status_output_label = QLabel("Log de Status:", self)
        layout.addWidget(self.status_output_label)
        self.status_output = QTextEdit(self)
        self.status_output.setReadOnly(True)
        self.status_output.setFixedHeight(100)
        layout.addWidget(self.status_output)

        self.open_link_button = QPushButton('Abrir link para PVs Congeladas no Archiver', self)
        self.open_link_button.setVisible(False)
        self.open_link_button.clicked.connect(self.open_generated_url)
        layout.addWidget(self.open_link_button)

        self.setLayout(layout)

    def start_monitor_thread(self):
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.update_status_output("Tentando parar monitoramento anterior...")
            self.monitor_thread.stop()
            if not self.monitor_thread.wait(3000):
                 self.update_status_output("Monitoramento anterior não parou a tempo. Forçando.")
            self.monitor_thread = None
        
        self.progress_bar.setValue(0)
        self.status_label.setText("Status: Iniciando...")
        self.pv_output.clear()
        self.frozen_pv_output.clear()
        self.disconnected_pv_output.clear()
        self.open_link_button.setVisible(False)

        filters = [
            self.filter1_input.text().strip(),
            self.filter2_input.text().strip()
        ]
        
        if not filters[0]:
            self.update_status_output("Erro: O filtro de setor e sub-setor não pode estar vazio.")
            self.status_label.setText("Status: Erro de Filtro")
            self.progress_bar.setValue(100)
            return

        check_interval = self.interval_input.value()
        self.frozen_check_duration = self.frozen_duration_input.value() * 60

        self.monitor_thread = MonitorThread(
            filters=filters, 
            check_interval=check_interval, 
            frozen_check_duration=self.frozen_check_duration
        )
        self.monitor_thread.updt_pv_log.connect(self.update_pv_output)
        self.monitor_thread.update_status.connect(self.update_status_output)
        self.monitor_thread.frozen_pvs_signal.connect(self.update_frozen_pv_output)
        self.monitor_thread.not_stored_pvs_signal.connect(self.update_disconnected_pv_output)
        self.monitor_thread.progress_update.connect(self.update_progress)
        
        self.monitor_thread.finished.connect(self.on_monitor_finished)
        self.monitor_thread.start()
        self.update_status_output("Monitoramento iniciado. Aguardando processamento...")
        self.status_label.setText("Status: Processando...")

    def on_monitor_finished(self):
        if self.monitor_thread and not self.monitor_thread.running: # Se foi parada via stop
            self.status_label.setText("Status: Interrompido")
        else: # Se terminou normalmente
            if self.progress_bar.value() == 100: # E a barra chegou a 100%
                 self.status_label.setText("Status: Concluído")
            else: # Caso termine sem chegar a 100 e não foi 'stop'
                 self.status_label.setText("Status: Finalizado com pendências")
        
    def update_disconnected_pv_output(self, disconnected_pvs):
        self.disconnected_pv_output.clear()
        if not disconnected_pvs:
            self.disconnected_pv_output.append("Nenhuma PV desconectada detectada.")
            return
        self.disconnected_pv_output.append(f"PVs desconectadas ({len(disconnected_pvs)}):")
        for pv in disconnected_pvs:
            self.disconnected_pv_output.append(pv)

    def stop_monitor(self):
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.update_status_output("Interrompendo monitoramento...")
            self.monitor_thread.stop()
        else:
            self.update_status_output("Nenhum monitoramento ativo para interromper.")
        self.status_label.setText("Status: Interrompido pelo usuário")
        self.progress_bar.setValue(0)

    def clear_monitor(self):
        self.status_output.clear()
        self.pv_output.clear()
        self.frozen_pv_output.clear()
        self.disconnected_pv_output.clear()
        self.open_link_button.setVisible(False)
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.status_label.setText("Status: Processando (logs limpos)")
        else:
            self.status_label.setText("Status: Ocioso (logs limpos)")
        self.progress_bar.setValue(0)
        self.update_status_output("Logs limpos.")

    def update_progress(self, value):
        self.progress_bar.setValue(value)
        if value == 100 and self.status_label.text() == "Status: Processando...":
            self.status_label.setText("Status: Quase lá...") 

    def update_pv_output(self, pv_list):
        self.pv_output.clear()
        if not pv_list:
            self.pv_output.append("Nenhuma PV foi listada com os filtros aplicados.")
        else:
            self.pv_output.append(f"PVs filtradas ({len(pv_list)}):")
            for pv in pv_list:
                self.pv_output.append(pv)

    def update_status_output(self, status):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.status_output.append(f"[{timestamp}] {status}")
        self.status_output.ensureCursorVisible()

    def update_frozen_pv_output(self, frozen_pvs):
        self.frozen_pv_output.clear()
        if not frozen_pvs:
            self.frozen_pv_output.append("Nenhuma PV congelada detectada.")
            self.open_link_button.setVisible(False)
            return

        self.frozen_pv_output.append(f"PVs congeladas ({len(frozen_pvs)}):")
        for pv in frozen_pvs:
            self.frozen_pv_output.append(pv)

        base_url = 'http://archiver-viewer.lnls.br/'
        minutes_offset_for_link = self.frozen_duration_input.value()

        params = {
            'pv': frozen_pvs,
            'from': self.get_time_string(minutes_offset=minutes_offset_for_link),
            'to': self.get_time_string()
        }
        query_string = urllib.parse.urlencode(params, doseq=True, safe=':/')
        self.generated_url = f"{base_url}?{query_string}"
        self.frozen_pv_output.append(f"\nLink gerado para Archiver:\n{self.generated_url}")
        self.open_link_button.setVisible(True)

    def get_time_string(self, minutes_offset=0):
        time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        target_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes_offset)
        return target_time.strftime(time_format)

    def open_generated_url(self):
        if self.generated_url:
            QDesktopServices.openUrl(QUrl(self.generated_url))
            
    def closeEvent(self, event):
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.monitor_thread.stop()
            self.monitor_thread.wait()
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec_())

























