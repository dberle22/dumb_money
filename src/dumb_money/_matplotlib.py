"""Shared Matplotlib configuration for headless report rendering."""

from __future__ import annotations

import os

import matplotlib

# Report builds run headlessly under Codex/CLI, so prefer a non-interactive
# backend unless the caller explicitly selected one first.
if not os.environ.get("MPLBACKEND"):
    matplotlib.use("Agg")

