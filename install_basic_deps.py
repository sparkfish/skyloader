#!/usr/bin/env python3
"""
Install basic dependencies for testing skyloader without Google Drive features.
"""

import subprocess
import sys

def install_package(package):
    """Install a package using pip"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"✓ Successfully installed {package}")
        return True
    except subprocess.CalledProcessError:
        print(f"✗ Failed to install {package}")
        return False

def main():
    print("Installing basic dependencies for skyloader testing...")
    
    basic_deps = [
        "pandas",
        "numpy", 
        "requests"
    ]
    
    success_count = 0
    for dep in basic_deps:
        if install_package(dep):
            success_count += 1
    
    print(f"\nInstalled {success_count}/{len(basic_deps)} basic dependencies")
    
    if success_count == len(basic_deps):
        print("\n✓ All basic dependencies installed successfully!")
        print("You can now run: python test_skyloader.py")
    else:
        print("\n⚠ Some dependencies failed to install")
        print("You may need to install them manually:")
        for dep in basic_deps:
            print(f"  pip install {dep}")

if __name__ == "__main__":
    main()
