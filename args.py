import os

class Args:
    def __init__(self):
        # ── Mode — default is dev, prod must be explicit ───────
        mode = os.getenv('MODE', 'dev').lower()
        if mode == 'local':
            self.dev = True
            self.local_run = True
        elif mode == 'prod':
            self.dev = False
            self.local_run = False
        else:
            self.dev = True
            self.local_run = False

        # ── DB host for dev mode ───────────────────────────────
        if self.dev and not self.local_run:
            os.environ.setdefault('METAMOTH_DB_HOST', 'localhost')

        # ── Logging ────────────────────────────────────────────
        self.log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

        # ── POE connection (defaults from config) ──────────────
        from config import POE_ADDRESS, POE_PORT, POE_CHUNK_SIZE, METAMGR_ADDRESS, METAMGR_PORT
        self.poe_socket_address = os.getenv('POE_SOCKET_ADDRESS', POE_ADDRESS)
        self.poe_socket_port = int(os.getenv('POE_SOCKET_PORT', POE_PORT))
        self.poe_chunk_size = int(os.getenv('POE_CHUNK_SIZE', POE_CHUNK_SIZE))
        self.force_io_dump = os.getenv('FORCE_IO_DUMP', 'false').lower() in ['true', '1', 't']

        # ── Metamanager connection ─────────────────────────────
        self.metamgr_socket_address = os.getenv('METAMGR_SOCKET_ADDRESS', METAMGR_ADDRESS)
        self.metamgr_socket_port = int(os.getenv('METAMGR_SOCKET_PORT', METAMGR_PORT))

        # ── Station lookup control ─────────────────────────────
        _run_sl_env = os.getenv('RUN_STATION_LOOKUP')
        if _run_sl_env is not None:
            self.run_station_lookup = _run_sl_env.lower() in ['true', '1', 't']
        elif self.local_run:
            self.run_station_lookup = False
        else:
            self.run_station_lookup = True

        # ── Fargate safety ─────────────────────────────────────
        self.shutdown_timeout_minutes = int(os.getenv('SHUTDOWN_TIMEOUT_MINUTES', 10))

        # ── POE submission control ─────────────────────────────
        _send_to_poe_env = os.getenv('SEND_TO_POE')
        if _send_to_poe_env is not None:
            self.send_to_poe = _send_to_poe_env.lower() in ['true', '1', 't']
        elif self.local_run:
            self.send_to_poe = False
        elif self.dev:
            self.send_to_poe = False
        else:
            self.send_to_poe = True

args = Args()
