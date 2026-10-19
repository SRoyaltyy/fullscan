"""Run the evaluator with Python site packages and network access disabled.

Usage: python -I -S scripts/offline_evidence_checks.py --engine evidence ...
Resource bootstrap is a separate one-time operation, never part of scanning.
"""
import runpy
import socket
import sys
from pathlib import Path

if not sys.flags.isolated or not sys.flags.no_site:
    raise SystemExit('Run this check with python -I -S')

def denied(*args, **kwargs):
    raise RuntimeError('Network access is disabled during evidence evaluation')

class OfflineSocket(socket.socket):
    connect = denied
    connect_ex = denied

socket.socket = OfflineSocket
socket.create_connection = denied
socket.getaddrinfo = denied
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.argv[0]='src.eval_correlation'
runpy.run_module('src.eval_correlation',run_name='__main__')
