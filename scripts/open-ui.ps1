param(
    [ValidatePattern('^[a-zA-Z0-9][a-zA-Z0-9.-]*$')]
    [string]$VmAddress = '',
    [ValidatePattern('^[a-z_][a-zA-Z0-9_-]*$')]
    [string]$RemoteUser = '',
    [ValidatePattern('^/[a-zA-Z0-9_./-]+$')]
    [string]$ProjectPath = '',
    [ValidatePattern('^[a-z0-9-]+$')]
    [string]$Namespace = 'data-stack',
    [string]$ConfigPath = (Join-Path $HOME '.config/data-stack/ui.json')
)

$ErrorActionPreference = 'Stop'
if (Test-Path $ConfigPath) {
    $config = Get-Content -Raw $ConfigPath | ConvertFrom-Json
    if (-not $VmAddress) { $VmAddress = $config.vmAddress }
    if (-not $RemoteUser) { $RemoteUser = $config.remoteUser }
    if (-not $ProjectPath) { $ProjectPath = $config.projectPath }
    if ($config.namespace -and -not $PSBoundParameters.ContainsKey('Namespace')) { $Namespace = $config.namespace }
}
if (-not $VmAddress) { $VmAddress = Read-Host 'VM IP or hostname' }
if (-not $RemoteUser) { $RemoteUser = Read-Host 'SSH username' }
if (-not $ProjectPath) { $ProjectPath = '/home/' + $RemoteUser + '/projects/data-stack' }
if ($VmAddress -notmatch '^[a-zA-Z0-9][a-zA-Z0-9.-]*$' -or
    $RemoteUser -notmatch '^[a-z_][a-zA-Z0-9_-]*$' -or
    $ProjectPath -notmatch '^/[a-zA-Z0-9_./-]+$' -or $Namespace -notmatch '^[a-z0-9-]+$') {
    throw 'Invalid host, user or project path in parameters/configuration.'
}
$ssh = (Get-Command ssh.exe -ErrorAction Stop).Source
$remoteCommand = 'env KUBECONFIG=$HOME/.kube/config NAMESPACE=' + $Namespace + ' AIRFLOW_LOCAL_PORT=18080 METABASE_LOCAL_PORT=13001 PROMETHEUS_LOCAL_PORT=19090 bash ' + $ProjectPath + '/scripts/ui.sh'

Write-Host 'Keep this window open. Press Ctrl+C to disconnect.'
Write-Host 'After the three Forwarding messages, open:'
Write-Host 'Airflow:    http://localhost:28080'
Write-Host 'Metabase:   http://localhost:23001'
Write-Host 'Prometheus: http://localhost:29090'

# A remote PTY propagates disconnects to ui.sh, which cleans up its child processes.
# Ignore existing SSH forwards (for example a configured remote port 1080).
$sshArguments = @(
    '-F', 'NUL', '-tt',
    '-o', 'ExitOnForwardFailure=yes',
    '-o', 'ConnectTimeout=10',
    '-o', 'ServerAliveInterval=15',
    '-o', 'ServerAliveCountMax=3',
    '-L', '28080:127.0.0.1:18080',
    '-L', '23001:127.0.0.1:13001',
    '-L', '29090:127.0.0.1:19090',
    "$RemoteUser@$VmAddress", $remoteCommand
)
& $ssh @sshArguments
if ($LASTEXITCODE -ne 0) {
    Write-Host 'Connection closed with an error. Check VM availability and existing tunnels.'
    exit $LASTEXITCODE
}
