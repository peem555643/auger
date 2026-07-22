# Run a cargo command inside the dev container.
#   .\x.ps1 check
#   .\x.ps1 test
#   .\x.ps1 run -- --mongo-uri mongodb://mongo:27017
param([Parameter(ValueFromRemainingArguments = $true)] $Args)

$ErrorActionPreference = "Stop"

$running = docker compose ps --status running --services 2>$null
if ($running -notcontains "dev") {
    Write-Host "starting dev environment..." -ForegroundColor Cyan
    docker compose up -d
}

docker compose exec -T dev cargo @Args
exit $LASTEXITCODE
