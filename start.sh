#!/bin/bash
# Ambassadors Back Office — Script de arranque
# Uso: bash start.sh

echo "🚀 Arrancando Ambassadors Back Office..."

# Verificar Python3
if ! command -v python3 &> /dev/null; then
  echo "❌ Python3 no encontrado. Por favor instala Python 3.9+"
  exit 1
fi

# Matar proceso anterior si existe
pkill -f "server.py" 2>/dev/null
sleep 1

# Arrancar servidor en background
python3 "$(dirname "$0")/server.py" &
SERVER_PID=$!

sleep 2

# Verificar que arrancó
if curl -s http://localhost:8787/api/dashboard > /dev/null 2>&1; then
  echo "✅ API corriendo en http://localhost:8787"
  echo "📂 Base de datos: backend/ambassadors.db"
  echo "🌐 Abre index.html en tu navegador"
  echo "⏹  Para parar el servidor: kill $SERVER_PID"
else
  echo "❌ Error arrancando el servidor. Revisa server.py"
  exit 1
fi

# Abrir el navegador (macOS)
if command -v open &> /dev/null; then
  open "$(dirname "$0")/index.html"
fi

echo ""
echo "Logs del servidor:"
wait $SERVER_PID
