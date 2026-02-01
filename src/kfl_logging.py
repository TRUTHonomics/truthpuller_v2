"""
KFL/QBN logging volgens SKILL kfl-logging.

Regels: _log/ in project root, bestandsnaam {YYMMDD-HH-MM-ss}_{scriptname}.log,
archivering *_{scriptname}.log naar _log/archive/, format timestamp, level, message,
console ANSI voor WARNING/ERROR bij TTY.
"""

import inspect
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

FILE_FMT = "%(asctime)s, %(levelname)s, %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"

_BOLD_ORANGE = "\033[1;38;5;208m"
_BOLD_RED = "\033[1;31m"
_RESET = "\033[0m"


def _find_project_root(start: Path) -> Path:
    """Walk up until we find .cursorrules or .docs/architecture.yaml or truth_config.yaml or hit drive root."""
    for parent in [start, *start.parents]:
        if (parent / ".cursorrules").exists():
            return parent
        if (parent / ".docs" / "architecture.yaml").exists():
            return parent
        if (parent / "truth_config.yaml").exists():
            return parent
        if parent == parent.parent:
            break
    return start


class _ConsoleFormatter(logging.Formatter):
    """Formatter that adds ANSI colors for WARNING/ERROR when used on console."""

    def format(self, record):
        record.levelname = record.levelname.upper()
        base = super().format(record)
        level_colors = {"WARNING": _BOLD_ORANGE, "ERROR": _BOLD_RED}
        color = level_colors.get(record.levelname, "")
        if color:
            base = base.replace(record.levelname, f"{color}{record.levelname}{_RESET}")
        return base


def setup_kfl_logging(
    script_name: Optional[str] = None,
    project_root: Optional[Path] = None,
    log_level: int = logging.INFO,
) -> logging.Logger:
    """
    Setup logging volgens KFL/QBN logregels.

    Args:
        script_name: Naam voor het log bestand (zonder extensie). Als None, wordt de aanroepende scriptnaam gebruikt.
        project_root: Optioneel; anders wordt project root gezocht vanaf aanroepende bestand.
        log_level: Console/log level (default: INFO). File krijgt altijd DEBUG.

    Returns:
        Geconfigureerde logger instance.
    """
    if script_name is None:
        try:
            frame = inspect.stack()[1]
            script_name = Path(frame.filename).stem
        except Exception:
            script_name = "script"

    if project_root is None:
        try:
            frame = inspect.stack()[1]
            start = Path(frame.filename).resolve().parent
            project_root = _find_project_root(start)
        except Exception:
            project_root = Path(__file__).resolve().parent.parent

    log_dir = project_root / "_log"
    archive_dir = log_dir / "archive"
    log_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%y%m%d-%H-%M-%S")
    log_file = log_dir / f"{ts}_{script_name}.log"

    for f in log_dir.glob(f"*_{script_name}.log"):
        if f.is_file():
            try:
                f.rename(archive_dir / f.name)
            except Exception:
                pass

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(FILE_FMT, datefmt=DATE_FMT))
    logger.addHandler(file_handler)

    if sys.stderr.isatty():
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(_ConsoleFormatter(FILE_FMT, datefmt=DATE_FMT))
        logger.addHandler(console_handler)
    else:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(logging.Formatter(FILE_FMT, datefmt=DATE_FMT))
        logger.addHandler(console_handler)

    logger.info("Started. Logging to: %s", log_file)
    return logger
