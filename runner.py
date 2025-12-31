import argparse
import subprocess
import os
import sys
import platform

def run_command(command, cwd=None):
    """Runs a shell command and checks for errors."""
    print(f"Running: {command}")
    try:
        subprocess.check_call(command, shell=True, cwd=cwd)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        sys.exit(1)

def install():
    """Installs dependencies from requirements.txt."""
    print("Installing dependencies...")
    run_command(f"{sys.executable} -m pip install -r requirements.txt")
    print("Dependencies installed.")

def clean():
    """Cleans up the SQLite database files."""
    files = ["storage/iot.db", "storage/iot.db-wal", "storage/iot.db-shm"]
    cleaned = False
    for f in files:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"Removed {f}")
                cleaned = True
            except OSError as e:
                print(f"Error removing {f}: {e}")
    if not cleaned:
        print("No database files found to clean.")
    else:
        print("Database cleanup complete.")

def start_edge():
    """Starts the Edge Processor."""
    print("Starting Edge Processor...")
    # Using sys.executable to ensure we use the same python interpreter
    cmd = f"{sys.executable} -m edge.edge_processor --db storage/iot.db --achp-threshold 50.0"
    run_command(cmd)

def start_sim():
    """Starts the Simulator."""
    print("Starting Simulator...")
    cmd = f"{sys.executable} simulator/simulator.py --interval 1"
    run_command(cmd)

def start_dash():
    """Starts the Live Dashboard."""
    print("Starting Live Dashboard...")
    cmd = f"{sys.executable} visualize/live_dashboard.py --db storage/iot.db --device-id gh_01 --refresh 1"
    run_command(cmd)

def start_plot():
    """Starts the Plot visualization."""
    print("Starting Plot...")
    cmd = f"{sys.executable} visualize/plot.py --db storage/iot.db --limit 300"
    run_command(cmd)

def start_web():
    """Starts the Streamlit Web Dashboard."""
    print("Starting Web Dashboard...")
    # Streamlit runs as a separate executable, but usually installed in the same venv bin
    # We can invoke it via `python -m streamlit run ...` to be safe with the current python env
    cmd = f"{sys.executable} -m streamlit run visualize/web_dashboard.py"
    run_command(cmd)

def main():
    parser = argparse.ArgumentParser(description="IoT Greenhouse Project Runner")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Install
    subparsers.add_parser("install", help="Install dependencies from requirements.txt")

    # Clean
    subparsers.add_parser("clean", help="Remove database files (iot.db and related)")

    # Components
    subparsers.add_parser("edge", help="Run the Edge Processor (Subscribes to MQTT -> Writes to DB)")
    subparsers.add_parser("sim", help="Run the Simulator (Publishes to MQTT)")
    subparsers.add_parser("dash", help="Run the Live Dashboard (Reads from DB)")
    subparsers.add_parser("plot", help="Run the Matplotlib Plot (Reads from DB)")
    subparsers.add_parser("web", help="Run the Streamlit Web Dashboard")

    args = parser.parse_args()

    if args.command == "install":
        install()
    elif args.command == "clean":
        clean()
    elif args.command == "edge":
        start_edge()
    elif args.command == "sim":
        start_sim()
    elif args.command == "dash":
        start_dash()
    elif args.command == "plot":
        start_plot()
    elif args.command == "web":
        start_web()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
