# Ejecutar en PowerShell COMO ADMINISTRADOR, en cada equipo Windows (sobre todo en el que falla):
#   Set-ExecutionPolicy -Scope Process Bypass; .\preparar_windows.ps1
# 1) Marca el adaptador de Hamachi como red PRIVADA (si es Publica, Windows bloquea lo entrante)
# 2) Abre los puertos 9092 y 9093 (TCP) en todos los perfiles

Write-Host "== Adaptadores de red y su perfil =="
Get-NetConnectionProfile | Format-Table Name, InterfaceAlias, NetworkCategory -AutoSize

$hama = Get-NetConnectionProfile | Where-Object { $_.InterfaceAlias -like "*Hamachi*" }
if ($hama) {
    foreach ($h in $hama) {
        Set-NetConnectionProfile -InterfaceIndex $h.InterfaceIndex -NetworkCategory Private
        Write-Host "Hamachi ($($h.InterfaceAlias)) marcado como Privado."
    }
} else {
    Write-Host "No se encontro el adaptador de Hamachi (esta Hamachi encendido y conectado a la red?)."
}

Remove-NetFirewallRule -DisplayName "Kafka 9092-9093" -ErrorAction SilentlyContinue
New-NetFirewallRule -DisplayName "Kafka 9092-9093" -Direction Inbound -Protocol TCP `
    -LocalPort 9092,9093 -Action Allow -Profile Any | Out-Null
Write-Host "Regla de firewall creada: entrada TCP 9092 y 9093."

Write-Host ""
Write-Host "Prueba desde ESTE equipo hacia los otros (cambia las IP por las de Hamachi):"
Write-Host "  Test-NetConnection 25.x.x.x -Port 9093"
