# `orquestrador_functions`

Support code for **`orquestrador.py`** (WFM queue loop) and shared WFM messaging used by the GD service and data models.

## Who calls what

| Area | Used by |
|------|---------|
| `Logs/` | `orquestrador.py`, `algoritmo_gd.py`, data models (`message_loader`, labels, batch log writers, `process_wfm_errors`) |
| `WFM_Process/` | Orchestrator: process queue getters and status setters |
| `Data_Handlers/GetGlobalData.py` | Orchestrator: concurrency / retry parameters |
| `Classes/Connection/connect.py` | Low-level Oracle connect / ping / reconnect (config manager credentials) |
| `Classes/AlgorithmPrepClasses/ConnectionHandler.py` | Orchestrator only: holds one long-lived raw connection for the polling loop |

## Database connections (two roles, not two databases)

1. **Orchestrator connection** — `ConnectionHandler` opens a **raw** Oracle session used for the whole poll loop: pick processes, update WFM status, orchestrator-level `set_process_errors`, and pass the same handle into the child run as `external_raw_connection` (avoids extra sessions; see comment in `orquestrador.py`).

2. **`data_manager` connection** — `create_components(..., use_db=True)` builds the **framework** SQLAlchemy engine for loads, inserts, and model I/O inside `with data_manager:` during `run_batch_process`.

The service uses **`data_manager` for data** and **`external_raw_connection` (when provided) for WFM `esc_processo_erros` rows** via `process_wfm_errors.set_process_errors`. Unifying everything into a single API would be a separate refactor (out of scope for STRSOL-1848).

## Legacy removed (STRSOL-1848)

- `Algorithm_Functions/` — old ESC/MPD runner dispatch; never wired to current GD batch.
- `Insert_read_schedules/` — unused batch insert helper.
