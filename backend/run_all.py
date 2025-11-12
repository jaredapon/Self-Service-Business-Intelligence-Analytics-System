import sys
import subprocess
import threading
from pathlib import Path
import time

SCRIPT_DIR = Path(__file__).resolve().parent

def stream_output(prefix, stream):
    for line in iter(stream.readline, ""):
        print(f"[{prefix}] {line.rstrip()}")
    stream.close()

def start_process(name, cmd):
    return subprocess.Popen(
        cmd,
        cwd=str(SCRIPT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

def main():
    py = sys.executable
    p_api = start_process("api", [py, str(SCRIPT_DIR / "run_api.py")])
    p_obs = start_process("observer", [py, str(SCRIPT_DIR / "run_observer.py")])

    t_api = threading.Thread(target=stream_output, args=("API", p_api.stdout), daemon=True)
    t_obs = threading.Thread(target=stream_output, args=("OBSV", p_obs.stdout), daemon=True)
    t_api.start()
    t_obs.start()

    try:
        # Wait for any process to exit; if one exits, shut both down.
        while True:
            if p_api.poll() is not None or p_obs.poll() is not None:
                break
            time.sleep(0.5)  # portable alternative to signal.pause() on Windows
    except KeyboardInterrupt:
        pass
    finally:
        for proc in (p_api, p_obs):
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    proc.kill()
        # Allow threads to finish
        t_api.join(timeout=1)
        t_obs.join(timeout=1)

if __name__ == "__main__":
    main()