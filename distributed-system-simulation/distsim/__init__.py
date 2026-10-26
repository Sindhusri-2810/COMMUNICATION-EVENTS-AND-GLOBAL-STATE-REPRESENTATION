"""Distributed system simulation: processes, message passing, events, global state."""
from .simulation import run_simulation, SimulationResult, CHANNELS
from .snapshot import GlobalSnapshot

__all__ = ["run_simulation", "SimulationResult", "GlobalSnapshot", "CHANNELS"]
