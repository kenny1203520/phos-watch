import os
import sys
import subprocess
import platform

def main():
    print("Building phos-watch executable...")
    # 1. Ensure pyinstaller is installed
    try:
        import PyInstaller
    except ImportError:
        print("PyInstaller not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        
    # 2. Determine OS-specific separator for --add-data
    # PyInstaller uses ';' on Windows and ':' on Linux/macOS
    sep = ';' if platform.system() == 'Windows' else ':'
    
    # We want to add the 'static' folder
    # Source path: static, Destination inside executable: static
    add_data_arg = f"static{sep}static"
    
    # 3. Hidden imports for Uvicorn and Watchdog
    hidden_imports = [
        "phos_watch.api",
        "uvicorn.loops",
        "uvicorn.loops.auto",
        "uvicorn.loops.asyncio",
        "uvicorn.protocols",
        "uvicorn.protocols.http",
        "uvicorn.protocols.http.auto",
        "uvicorn.protocols.http.h11_impl",
        "uvicorn.protocols.websockets",
        "uvicorn.protocols.websockets.auto",
        "uvicorn.protocols.websockets.wsproto_impl",
        "uvicorn.protocols.websockets.websockets_impl",
        "uvicorn.lifespan",
        "uvicorn.lifespan.on",
        "uvicorn.lifespan.off",
    ]
    
    # OS-specific watchdog observers
    if platform.system() == 'Windows':
        hidden_imports.append("watchdog.observers.read_directory_changes")
    elif platform.system() == 'Linux':
        hidden_imports.append("watchdog.observers.inotify")
    elif platform.system() == 'Darwin':  # macOS
        hidden_imports.append("watchdog.observers.kqueue")
        
    # Build command
    cmd = [
        sys.executable,
        "-m", "PyInstaller",
        "--onefile",
        "--name", "phos-watch",
        "--add-data", add_data_arg,
    ]
    
    for imp in hidden_imports:
        cmd.extend(["--hidden-import", imp])
        
    cmd.append("app.py")
    
    print(f"Running command: {' '.join(cmd)}")
    subprocess.check_call(cmd)
    print("\n==================================================")
    print("Build complete! Executable is located in the 'dist' directory.")
    print("==================================================")

if __name__ == '__main__':
    main()
