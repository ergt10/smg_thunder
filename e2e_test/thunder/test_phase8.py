#!/usr/bin/env python3
"""Phase 8 e2e: TR pause/resume scheduler.

Run from repo root:
    python3 e2e_test/thunder/test_phase8.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MOCK_PORT = int(os.environ.get("MOCK_PORT", "8001"))
SMG_PORT = int(os.environ.get("SMG_PORT", "30000"))
PROM_PORT = int(os.environ.get("SMG_PROM_PORT", "29000"))


def request_json(method: str, url: str, payload: dict | None = None, timeout: float = 5.0):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def wait_http(url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=1).read()
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {url}")


def main() -> int:
    mock_log = tempfile.NamedTemporaryFile(prefix="thunder_phase8_mock_", suffix=".log", delete=False)
    smg_log = tempfile.NamedTemporaryFile(prefix="thunder_phase8_smg_", suffix=".log", delete=False)
    processes: list[subprocess.Popen] = []

    try:
        print(f"== boot mock_vllm on :{MOCK_PORT} ==")
        mock = subprocess.Popen(
            [
                sys.executable,
                "e2e_test/thunder/mock_vllm.py",
                "--port",
                str(MOCK_PORT),
                "--kv-cache-block-tokens",
                "16",
                "--num-kv-cache-blocks",
                "8",
            ],
            cwd=REPO_ROOT,
            stdout=mock_log,
            stderr=subprocess.STDOUT,
        )
        processes.append(mock)
        wait_http(f"http://localhost:{MOCK_PORT}/health")

        print(f"== boot smg TR mode on :{SMG_PORT} ==")
        smg_bin = REPO_ROOT / "target" / "debug" / "smg"
        smg_cmd = (
            [str(smg_bin)]
            if smg_bin.exists()
            else ["cargo", "run", "--quiet", "--bin", "smg", "--"]
        )
        smg = subprocess.Popen(
            [
                *smg_cmd,
                "--backend",
                "thunder",
                "--worker-urls",
                f"http://localhost:{MOCK_PORT}",
                "--port",
                str(SMG_PORT),
                "--prometheus-port",
                str(PROM_PORT),
                "--thunder-sub-mode",
                "tr",
            ],
            cwd=REPO_ROOT,
            stdout=smg_log,
            stderr=subprocess.STDOUT,
        )
        processes.append(smg)
        wait_http(f"http://localhost:{SMG_PORT}/health")
        time.sleep(1.2)

        print("== fill capacity with program-alpha ==")
        status, body = request_json(
            "POST",
            f"http://localhost:{SMG_PORT}/v1/chat/completions",
            {
                "model": "mock",
                "program_id": "program-alpha",
                "messages": [{"role": "user", "content": "hi"}],
            },
            timeout=10,
        )
        assert status == 200, body

        print("== start program-beta while capacity is full ==")
        result: dict[str, object] = {}

        def send_beta() -> None:
            try:
                result["status"], result["body"] = request_json(
                    "POST",
                    f"http://localhost:{SMG_PORT}/v1/chat/completions",
                    {
                        "model": "mock",
                        "program_id": "program-beta",
                        "messages": [{"role": "user", "content": "wait then resume"}],
                    },
                    timeout=20,
                )
            except urllib.error.HTTPError as exc:
                result["status"] = exc.code
                result["body"] = exc.read().decode("utf-8")
            except Exception as exc:
                result["error"] = repr(exc)

        thread = threading.Thread(target=send_beta, daemon=True)
        thread.start()

        deadline = time.time() + 10
        while time.time() < deadline:
            _, programs = request_json("GET", f"http://localhost:{SMG_PORT}/programs")
            beta = programs.get("program-beta")
            if beta and beta["state"] == "paused":
                print("program-beta paused:", json.dumps(beta, sort_keys=True))
                break
            time.sleep(0.25)
        else:
            raise AssertionError(f"program-beta never paused: {programs}")

        if result:
            raise AssertionError(f"program-beta returned before resume: {result}")

        print("== expand capacity and wait for scheduler resume ==")
        request_json(
            "POST",
            f"http://localhost:{MOCK_PORT}/control/capacity",
            {"num_kv_cache_blocks": 64},
        )
        thread.join(timeout=10)
        if thread.is_alive():
            raise AssertionError("program-beta did not resume within 10s")
        assert result.get("status") == 200, result

        _, programs = request_json("GET", f"http://localhost:{SMG_PORT}/programs")
        assert programs["program-alpha"]["state"] == "active", programs
        assert programs["program-beta"]["state"] == "active", programs
        assert programs["program-beta"]["status"] == "acting", programs

        _, mock_state = request_json("GET", f"http://localhost:{MOCK_PORT}/control/state")
        assert mock_state["request_count"] == 2, mock_state

        print("PASS: Phase 8 e2e complete")
        return 0
    finally:
        for process in reversed(processes):
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        mock_log.close()
        smg_log.close()


if __name__ == "__main__":
    raise SystemExit(main())
