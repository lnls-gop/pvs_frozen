#!/usr/bin/env python-sirius

import sys
import time
import datetime
from PyQt5.QtWidgets import (QApplication, QWidget, QPushButton, QVBoxLayout, QLineEdit, QTextEdit, QLabel, QSpinBox, QProgressBar, QHBoxLayout)
from PyQt5.QtCore import QObject, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtCore import QUrl
from siriuspy.clientarch import ClientArchiver
import epics
import urllib.parse


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

    # Sufixos que indicam que a PV é de set point
    set_point_suffixes = ['-SP', '-Setpoint', '-SP:RBV']

    def __init__(self, parent=None, filters=None, check_interval=10, frozen_check_duration=2*60, initial_check_delay=10*60):
        super(MonitorThread, self).__init__(parent)
        self.conn = None
        self.filters = filters or []
        self.pv_values = {}  # Armazena o valor e o tempo da última atualização de cada PV
        self.pv_monitors = {}  # Para armazenar os monitores de PV
        self.check_interval = check_interval
        self.frozen_check_duration = frozen_check_duration
        self.initial_check_delay = initial_check_delay
        self.start_time = None
        self.timer = None
        self.running = True

    def stop(self):
        self.running = False
        if self.timer:
            self.timer.stop()
        self.update_status.emit("Monitoramento interrompido.")

    def run(self):
        self.conn = self.create_connector()
        if not self.conn:
            self.update_status.emit("Não foi possível conectar ao banco de dados.")
            return

        all_pvnames = self.get_all_pvnames()
        if not all_pvnames:
            self.update_status.emit("Nenhuma PV encontrada.")
            return

        filtered_pvnames = self.filter_pvnames(all_pvnames)
        self.updt_pv_log.emit(filtered_pvnames)
        self.update_status.emit("Listagem e filtragem concluídas.")

        # Iniciar monitores para as PVs filtradas
        for pvname in filtered_pvnames:
            self.start_pv_monitor(pvname)

        # Aguardar um pouco para garantir que as PVs sejam atualizadas
        time.sleep(2)

        self.start_time = time.time()
        self.start_frozen_check()

        self.progress_update.emit(100)
        self.update_status.emit("Processamento Concluído.")

    def create_connector(self, timeout=1):
        conn = ClientArchiver()
        t0 = time.time()
        while time.time() - t0 < timeout:
            if conn.connected:
                return conn
            time.sleep(0.1)
        return None

    def get_all_pvnames(self):
        if self.conn:
            return self.conn.getAllPVs('*')
        return None

    def filter_pvnames(self, all_pvnames):
        filtered_pvnames = []
        if not all_pvnames:
            return filtered_pvnames

        filter1 = self.filters[0]
        filter2 = self.filters[1]

        total_pvs = len(all_pvnames)
        for i, pvname in enumerate(all_pvnames):
            if pvname in self.ignored_pvs:
                continue

            if filter1 and not pvname.startswith(filter1):
                continue

            if filter2 and not pvname.endswith(filter2):
                continue

            filtered_pvnames.append(pvname)
            progress = int((i + 1) / total_pvs * 100)
            self.progress_update.emit(progress)

        return filtered_pvnames

    def start_pv_monitor(self, pvname):
        """Inicia um monitor para a PV."""
        def callback(pvname=None, value=None, **kwargs):
            self.pv_values[pvname] = (value, time.time())

        # Inicializa o valor da PV antes de iniciar o monitor
        initial_value = self.get_pv_value(pvname)
        self.pv_values[pvname] = (initial_value, time.time())

        # Inicia o monitor
        self.pv_monitors[pvname] = epics.camonitor(pvname, callback=callback)

    def stop_pv_monitor(self, pvname):
        """Para o monitor de uma PV."""
        if pvname in self.pv_monitors:
            epics.camonitor_clear(self.pv_monitors[pvname])
            del self.pv_monitors[pvname]

    def get_pv_value(self, pvname):
        """Obtém o valor atual de uma PV."""
        try:
            return epics.caget(pvname)
        except:
            return None

    def is_set_point_pv(self, pvname):
        """Verifica se a PV é de set point com base no sufixo."""
        return any(pvname.endswith(suffix) for suffix in self.set_point_suffixes)

    def start_frozen_check(self):
        self.update_status.emit("Iniciando monitoramento...")
        self.timer = QTimer()
        self.timer.timeout.connect(self.check_pvs)
        self.timer.setInterval(self.check_interval * 1000)
        self.timer.start()
        self.check_pvs()

    def start_pv_monitor(self, pvname):
    #"""Inicia um monitor para a PV e armazena o histórico de valores."""
        def callback(pvname=None, value=None, **kwargs):
            if pvname not in self.pv_values:
                self.pv_values[pvname] = {"values": [], "timestamps": []}
            self.pv_values[pvname]["values"].append(value)
            self.pv_values[pvname]["timestamps"].append(time.time())

        # Inicializa o valor da PV antes de iniciar o monitor
        initial_value = self.get_pv_value(pvname)
        if pvname not in self.pv_values:
            self.pv_values[pvname] = {"values": [], "timestamps": []}
        self.pv_values[pvname]["values"].append(initial_value)
        self.pv_values[pvname]["timestamps"].append(time.time())

        # Inicia o monitor
        self.pv_monitors[pvname] = epics.camonitor(pvname, callback=callback)

    def check_pv_frozen(self, pvname):
        """Verifica se a PV está congelada comparando diretamente 15 primeiros e 15 últimos valores."""
        if pvname not in self.pv_values:
            return False  # PV não monitorada ou sem histórico

        values = self.pv_values[pvname].get("values", [])
        timestamps = self.pv_values[pvname].get("timestamps", [])

        if not values or not timestamps:
            return False  # Sem dados para verificar

        # Verifica se a PV é um set point
        if self.is_set_point_pv(pvname):
            return True

        # Verifica se a PV está desconectada
        if values[-1] is None:
            return False

        # Obtém os primeiros e últimos 15 valores (ou menos se não houver suficientes)
        num_samples = min(15, len(values))  # Garante que não ultrapasse o número de valores disponíveis
        first_values = values[:num_samples]
        last_values = values[-num_samples:] if len(values) >= num_samples else values

        # Verifica se temos dados suficientes
        if len(first_values) < 1 or len(last_values) < 1:
            return False

        # Verifica se todos os valores são idênticos nos dois conjuntos
        # Primeiro verifica se todos os primeiros valores são iguais ao primeiro
        all_first_equal = all(v == first_values[0] for v in first_values)

        # Depois verifica se todos os últimos valores são iguais ao último
        all_last_equal = all(v == last_values[0] for v in last_values)

        # Finalmente verifica se o primeiro e último valor são iguais
        first_last_equal = first_values[0] == last_values[0]

        # Considera congelada apenas se:
        # 1. Todos os primeiros valores são iguais entre si E
        # 2. Todos os últimos valores são iguais entre si E
        # 3. O primeiro valor é igual ao último valor
        if all_first_equal and all_last_equal and first_last_equal:
            return True  # PV congelada

        return False  # PV não congelada
    
    def check_pv_connected(self, pvname):
        """Verifica se a PV está conectada."""
        if pvname not in self.pv_values:
            return False  # PV não monitorada

        # Verifica se há valores registrados e se o último valor não é None
        values = self.pv_values[pvname].get("values", [])
        return len(values) > 0 and values[-1] is not None

    def check_pvs(self):
        """Verifica se as PVs estão conectadas e congeladas."""
        current_time = time.time()
        frozen_pvs = []
        disconnected_pvs = []
        ctrl_frozen = 0
        ctrl_disconnected = 0

        for pvname in self.pv_values.keys():
            # Verificar se a PV está desconectada
            if not self.check_pv_connected(pvname):
                disconnected_pvs.append(pvname)
                ctrl_disconnected += 1
                continue

            # Verificar se a PV está congelada
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
    """."""
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

        self.filter1_input = QLineEdit(self)
        self.filter1_input.setPlaceholderText("Digite o setor e sub-setor (ex: SI-01C1)")
        layout.addWidget(self.filter1_input)

        self.filter2_input = QLineEdit(self)
        self.filter2_input.setPlaceholderText("Digite o tipo de PV (ex: Temp-Mon)")
        layout.addWidget(self.filter2_input)

        self.interval_label = QLabel("Intervalo de verificação (segundos):", self)
        layout.addWidget(self.interval_label)

        self.interval_input = QSpinBox(self)
        self.interval_input.setRange(1, 3600)
        self.interval_input.setValue(1)
        self.interval_input.setEnabled(False)
        layout.addWidget(self.interval_input)

        self.frozen_duration_label = QLabel("Duração para PV congelada (minutos):", self)
        layout.addWidget(self.frozen_duration_label)

        self.frozen_duration_input = QSpinBox(self)
        self.frozen_duration_input.setRange(1, 60)
        self.frozen_duration_input.setValue(5)
        self.frozen_duration_input.setEnabled(False)
        layout.addWidget(self.frozen_duration_input)

        self.stop_button = QPushButton('1 - Stop', self)
        self.stop_button.clicked.connect(self.stop_monitor)
        layout.addWidget(self.stop_button)

        self.clear_button = QPushButton('2 - Clear Log', self)
        self.clear_button.clicked.connect(self.clear_monitor)
        layout.addWidget(self.clear_button)

        self.list_pv_button = QPushButton('3 - Listar PVs', self)
        self.list_pv_button.clicked.connect(self.start_monitor_thread)
        layout.addWidget(self.list_pv_button)

        self.progress_bar = QProgressBar(self)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.status_label = QLabel("Status: Ocioso", self)
        layout.addWidget(self.status_label)

        self.pv_output = QTextEdit(self)
        self.pv_output.setReadOnly(True)
        layout.addWidget(self.pv_output)

        self.status_output = QTextEdit(self)
        self.status_output.setReadOnly(True)
        layout.addWidget(self.status_output)

        self.frozen_pv_output = QTextEdit(self)
        self.frozen_pv_output.setReadOnly(True)
        layout.addWidget(self.frozen_pv_output)

        # Atualização 20/03
        self.disconnected_pv_output = QTextEdit(self)
        self.disconnected_pv_output.setReadOnly(True)
        self.disconnected_pv_output.setPlaceholderText("PVs desconectadas aparecerão aqui.")
        layout.addWidget(QLabel("PVs Desconectadas:"))
        layout.addWidget(self.disconnected_pv_output)

        self.open_link_button = QPushButton('Abrir link', self)
        self.open_link_button.setVisible(False)
        self.open_link_button.clicked.connect(self.open_generated_url)
        layout.addWidget(self.open_link_button)

        self.setLayout(layout)

    def start_monitor_thread(self):
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.stop_monitor()
        
        filters = [
            self.filter1_input.text(),
            self.filter2_input.text() if self.filter2_input.text() else None
        ]
        check_interval = self.interval_input.value()
        frozen_check_duration = self.frozen_duration_input.value() * 60
        self.monitor_thread = MonitorThread(filters=filters, check_interval=check_interval, frozen_check_duration=frozen_check_duration)
        self.monitor_thread.updt_pv_log.connect(self.update_pv_output)
        self.monitor_thread.update_status.connect(self.update_status_output)
        self.monitor_thread.frozen_pvs_signal.connect(self.update_frozen_pv_output)
        self.monitor_thread.frozen_check_duration_signal.connect(self.set_frozen_check_duration)
        self.monitor_thread.not_stored_pvs_signal.connect(self.update_disconnected_pv_output) #Atualização 20/03
        self.monitor_thread.progress_update.connect(self.update_progress)
        self.monitor_thread.start()
        self.update_status_output("Monitoramento iniciado.")
        self.status_label.setText("Status: OK")

    #Atualização 20/03
    def update_disconnected_pv_output(self, disconnected_pvs):
        """Atualiza a exibição das PVs desconectadas."""
        self.disconnected_pv_output.clear()
        if not disconnected_pvs:
            self.disconnected_pv_output.append("Nenhuma PV desconectada detectada.")
            return

        self.disconnected_pv_output.append("PVs desconectadas:")
        for pv in disconnected_pvs:
            self.disconnected_pv_output.append(pv)

    def stop_monitor(self):
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.monitor_thread.stop()
        self.status_label.setText("Status: Interrompido")
        self.progress_bar.setValue(0)

    def clear_monitor(self):
        self.status_output.clear()
        self.pv_output.clear()
        self.frozen_pv_output.clear()
        self.disconnected_pv_output.clear()
        self.status_label.setText("Status: Ocioso")
        self.progress_bar.setValue(0)

    def set_frozen_check_duration(self, duration):
        self.frozen_check_duration = duration

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def update_pv_output(self, pv_list):
        self.pv_output.clear()
        if not pv_list:
            self.pv_output.append("Nenhuma PV foi listada.")
        else:
            for pv in pv_list:
                self.pv_output.append(pv)

    def update_status_output(self, status):
        self.status_output.append(status)

    def update_frozen_pv_output(self, frozen_pvs):
        self.frozen_pv_output.clear()
        if not frozen_pvs:
            self.frozen_pv_output.append("Nenhuma PV congelada detectada.")
            return

        self.frozen_pv_output.append("PVs congeladas:")
        for pv in frozen_pvs:
            self.frozen_pv_output.append(pv)

        base_url = 'http://archiver-viewer.lnls.br/'
        params = {
            'pv': frozen_pvs,
            'from': self.get_time_string(minutes_offset=self.frozen_check_duration // 60),
            'to': self.get_time_string()
        }
        query_string = urllib.parse.urlencode(params, doseq=True, safe=':/')
        self.generated_url = f"{base_url}?{query_string}"
        self.frozen_pv_output.append(f"\nLink gerado:\n{self.generated_url}")
        self.open_link_button.setVisible(True)

    def get_time_string(self, minutes_offset=0):
        time_format = '%Y-%m-%dT%H:%M:%S'
        now = datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes_offset)
        return now.strftime(time_format)

    def open_generated_url(self):
        if self.generated_url:
            QDesktopServices.openUrl(QUrl(self.generated_url))


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec_())

























