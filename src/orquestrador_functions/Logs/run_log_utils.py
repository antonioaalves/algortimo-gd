# -*- coding: utf-8 -*-
"""Utilities for orchestrator run log files (file handlers created by setup_logger)."""

import logging
import os


def discard_run_log_file(run_logger: logging.Logger) -> bool:
    """
    Close the logger's file handler and delete its log file.

    Used when a scheduled orchestrator run did no meaningful work and the
    log file would only contain startup noise. Returns True if a file was removed.
    """
    for handler in list(run_logger.handlers):
        if isinstance(handler, logging.FileHandler):
            log_path = handler.baseFilename
            handler.close()
            run_logger.removeHandler(handler)
            try:
                if os.path.isfile(log_path):
                    os.remove(log_path)
                    return True
            except OSError:
                pass
            return False
    return False
