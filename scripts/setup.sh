#!/bin/bash
# GTFS Database Setup Script (Bash)
# Quick setup for Linux/Mac users

set -e  # Exit on error

echo "============================================================"
echo "  GTFS Transit Database Setup (PostgreSQL + PostGIS)"
echo "============================================================"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Check if Docker is running
echo -e "${YELLOW}Checking Docker...${NC}"
if command -v docker &> /dev/null; then
    docker_version=$(docker --version)
    echo -e "${GREEN}✓ Docker found: $docker_version${NC}"
else
    echo -e "${RED}✗ Docker not found or not running${NC}"
    echo -e "${YELLOW}  Please install Docker: https://docs.docker.com/get-docker/${NC}"
    exit 1
fi

# Check if docker-compose is available
if ! docker compose version &> /dev/null && ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ docker-compose not found${NC}"
    echo -e "${YELLOW}  Please install Docker Compose: https://docs.docker.com/compose/install/${NC}"
    exit 1
fi

# Use docker compose or docker-compose
DOCKER_COMPOSE="docker compose"
if ! docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Creating .env file from template...${NC}"
    cp .env.example .env
    echo -e "${GREEN}✓ Created .env file${NC}"
    echo -e "${YELLOW}⚠ Please edit .env to set secure passwords!${NC}"
    echo ""
    read -p "Press Enter to continue after editing .env..."
fi

# Start Docker Compose
echo ""
echo -e "${YELLOW}Starting PostgreSQL + PostGIS...${NC}"
$DOCKER_COMPOSE up -d

# Wait for database to be ready
echo ""
echo -e "${YELLOW}Waiting for database to initialize...${NC}"
max_attempts=30
attempt=0
ready=false

while [ $attempt -lt $max_attempts ] && [ "$ready" = false ]; do
    attempt=$((attempt + 1))
    sleep 2
    
    health_status=$(docker inspect --format='{{.State.Health.Status}}' gtfs_db 2>/dev/null || echo "starting")
    
    if [ "$health_status" = "healthy" ]; then
        ready=true
    else
        echo "  Attempt $attempt/$max_attempts - Status: $health_status"
    fi
done

if [ "$ready" = false ]; then
    echo -e "${RED}✗ Database failed to start within timeout${NC}"
    echo -e "${YELLOW}  Check logs: $DOCKER_COMPOSE logs gtfs-postgres${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Database is ready!${NC}"

# Install Python dependencies
echo ""
echo -e "${YELLOW}Installing Python dependencies...${NC}"

if command -v python3 &> /dev/null; then
    python3 -m pip install -r ../requirements.txt --quiet
    echo -e "${GREEN}✓ Python dependencies installed${NC}"
elif command -v python &> /dev/null; then
    python -m pip install -r ../requirements.txt --quiet
    echo -e "${GREEN}✓ Python dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠ Python not found - skipping Python setup${NC}"
    echo -e "${YELLOW}  Install Python 3.8+: https://www.python.org/downloads/${NC}"
fi

# Test connection
echo ""
echo -e "${YELLOW}Testing database connection...${NC}"

if command -v python3 &> /dev/null; then
    cd ..
    python3 -m database test
    cd scripts
elif command -v python &> /dev/null; then
    cd ..
    python -m database test
    cd scripts
else
    echo -e "${YELLOW}⚠ Skipping connection test (Python not available)${NC}"
fi

# Show next steps
echo ""
echo -e "${CYAN}============================================================${NC}"
echo -e "${GREEN}  Setup Complete! ${NC}"
echo -e "${CYAN}============================================================${NC}"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo ""
echo -e "${NC}1. Load GTFS data:${NC}"
echo -e "${CYAN}   python3 -m database load${NC}"
echo ""
echo -e "${NC}2. Access database:${NC}"
echo -e "${CYAN}   - psql: psql -h localhost -U gtfs_admin -d gtfs_transit${NC}"
echo -e "${CYAN}   - pgAdmin: http://localhost:5050${NC}"
echo ""
echo -e "${NC}3. Stop database:${NC}"
echo -e "${CYAN}   $DOCKER_COMPOSE down${NC}"
echo ""
echo -e "${NC}4. View logs:${NC}"
echo -e "${CYAN}   $DOCKER_COMPOSE logs -f${NC}"
echo ""
echo -e "${CYAN}============================================================${NC}"
