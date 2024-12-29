import sys
import subprocess

print("Python Path:", sys.executable)

# Listar pacotes instalados
print("Installed Packages:")
subprocess.run([sys.executable, "-m", "pip", "list"])
