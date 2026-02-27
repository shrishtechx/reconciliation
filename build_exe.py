"""
Build script to create a standalone .exe for the Ledger Reconciliation app.
Usage: python build_exe.py
"""

import subprocess
import sys
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

def main():
    # 1. Build frontend if not already built
    dist_dir = os.path.join(ROOT, "frontend", "dist")
    if not os.path.isdir(dist_dir):
        print("Building frontend...")
        subprocess.check_call(["npm", "run", "build"], cwd=os.path.join(ROOT, "frontend"), shell=True)
    else:
        print(f"Frontend dist found at {dist_dir}")

    # 2. Install PyInstaller if not available
    try:
        import PyInstaller
        print(f"PyInstaller {PyInstaller.__version__} found")
    except ImportError:
        print("Installing PyInstaller...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])

    # 3. Run PyInstaller
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name", "LedgerReconciliation",
        "--onedir",
        "--console",
        "--icon", "NONE",
        # Add frontend dist
        "--add-data", f"frontend/dist;frontend/dist",
        # Add reconciliation package
        "--add-data", f"reconciliation;reconciliation",
        # Hidden imports that PyInstaller may miss
        "--hidden-import", "uvicorn.logging",
        "--hidden-import", "uvicorn.loops",
        "--hidden-import", "uvicorn.loops.auto",
        "--hidden-import", "uvicorn.protocols",
        "--hidden-import", "uvicorn.protocols.http",
        "--hidden-import", "uvicorn.protocols.http.auto",
        "--hidden-import", "uvicorn.protocols.websockets",
        "--hidden-import", "uvicorn.protocols.websockets.auto",
        "--hidden-import", "uvicorn.lifespan",
        "--hidden-import", "uvicorn.lifespan.on",
        "--hidden-import", "uvicorn.lifespan.off",
        "--hidden-import", "uvicorn.config",
        "--hidden-import", "uvicorn.main",
        "--hidden-import", "encodings",
        "--hidden-import", "multipart",
        "--hidden-import", "openpyxl",
        "--hidden-import", "xlrd",
        "--hidden-import", "xlsxwriter",
        "--hidden-import", "rapidfuzz",
        "--hidden-import", "rapidfuzz.fuzz",
        "--hidden-import", "rapidfuzz.process",
        "--hidden-import", "numpy",
        "--hidden-import", "pandas",
        # Collect all submodules for packages that PyInstaller misses
        "--collect-submodules", "uvicorn",
        "--collect-submodules", "fastapi",
        "--collect-submodules", "starlette",
        "--collect-submodules", "rapidfuzz",
        "--collect-submodules", "multipart",
        # Exclude unnecessary packages that cause build errors
        "--exclude-module", "sqlalchemy",
        "--exclude-module", "IPython",
        "--exclude-module", "matplotlib",
        "--exclude-module", "PIL",
        "--exclude-module", "scipy",
        "--exclude-module", "tkinter",
        # Don't confirm overwrite
        "--noconfirm",
        # Entry point
        "server.py",
    ]

    print("\nRunning PyInstaller...")
    print(" ".join(cmd))
    subprocess.check_call(cmd, cwd=ROOT)

    exe_path = os.path.join(ROOT, "dist", "LedgerReconciliation", "LedgerReconciliation.exe")
    if os.path.isfile(exe_path):
        print(f"\n{'='*60}")
        print(f"  BUILD SUCCESSFUL!")
        print(f"  EXE: {exe_path}")
        print(f"  Folder: {os.path.dirname(exe_path)}")
        print(f"{'='*60}")
        print(f"\nTo run: double-click LedgerReconciliation.exe")
        print(f"It will start the server and open the browser automatically.")
    else:
        print("\nBuild may have failed — exe not found at expected path.")


if __name__ == "__main__":
    main()
