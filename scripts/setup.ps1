# GTFS Database Setup Script (PowerShell)
# Quick setup for Windows users

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  GTFS Transit Database Setup (PostgreSQL + PostGIS)" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is running
Write-Host "Checking Docker..." -ForegroundColor Yellow
try {
    $dockerVersion = docker --version
    Write-Host "✓ Docker found: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Docker not found or not running" -ForegroundColor Red
    Write-Host "  Please install Docker Desktop: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "Creating .env file from template..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    Write-Host "✓ Created .env file" -ForegroundColor Green
    Write-Host "⚠ Please edit .env to set secure passwords!" -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press Enter to continue after editing .env"
}

# Start Docker Compose
Write-Host ""
Write-Host "Starting PostgreSQL + PostGIS..." -ForegroundColor Yellow
docker-compose up -d

# Wait for database to be ready
Write-Host ""
Write-Host "Waiting for database to initialize..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0
$ready = $false

while ($attempt -lt $maxAttempts -and -not $ready) {
    $attempt++
    Start-Sleep -Seconds 2
    
    try {
        $healthStatus = docker inspect --format='{{.State.Health.Status}}' gtfs_db 2>$null
        if ($healthStatus -eq "healthy") {
            $ready = $true
        } else {
            Write-Host "  Attempt $attempt/$maxAttempts - Status: $healthStatus" -ForegroundColor Gray
        }
    } catch {
        Write-Host "  Attempt $attempt/$maxAttempts - Waiting..." -ForegroundColor Gray
    }
}

if (-not $ready) {
    Write-Host "✗ Database failed to start within timeout" -ForegroundColor Red
    Write-Host "  Check logs: docker-compose logs gtfs-postgres" -ForegroundColor Yellow
    exit 1
}

Write-Host "✓ Database is ready!" -ForegroundColor Green

# Install Python dependencies
Write-Host ""
Write-Host "Installing Python dependencies..." -ForegroundColor Yellow

if (Get-Command python -ErrorAction SilentlyContinue) {
    python -m pip install -r ..\requirements.txt --quiet
    Write-Host "✓ Python dependencies installed" -ForegroundColor Green
} else {
    Write-Host "⚠ Python not found - skipping Python setup" -ForegroundColor Yellow
    Write-Host "  Install Python 3.8+: https://www.python.org/downloads/" -ForegroundColor Yellow
}

# Test connection
Write-Host ""
Write-Host "Testing database connection..." -ForegroundColor Yellow

if (Get-Command python -ErrorAction SilentlyContinue) {
    cd ..
    python -m database test
    cd scripts
} else {
    Write-Host "⚠ Skipping connection test (Python not available)" -ForegroundColor Yellow
}

# Show next steps
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Setup Complete! " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Load GTFS data:" -ForegroundColor White
Write-Host "   python -m database load" -ForegroundColor Cyan
Write-Host ""
Write-Host "2. Access database:" -ForegroundColor White
Write-Host "   - psql: psql -h localhost -U gtfs_admin -d gtfs_transit" -ForegroundColor Cyan
Write-Host "   - pgAdmin: http://localhost:5050" -ForegroundColor Cyan
Write-Host ""
Write-Host "3. Stop database:" -ForegroundColor White
Write-Host "   docker-compose down" -ForegroundColor Cyan
Write-Host ""
Write-Host "4. View logs:" -ForegroundColor White
Write-Host "   docker-compose logs -f" -ForegroundColor Cyan
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
