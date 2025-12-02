#!/bin/bash

#########################################################################
# eFICHE Assessment - Docker Cleanup and Setup Script
# Project: efiche_assessment_eric_maniraguha
# Purpose: Clean existing containers/images and set up fresh environment
#########################################################################

set -e  # Exit on error

echo "======================================================================"
echo "eFICHE Assessment - Docker Cleanup & Initialization"
echo "======================================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color



echo -e "${BLUE}Step 1: Stopping all running containers...${NC}"
docker-compose down 2>/dev/null || true
docker stop $(docker ps -q) 2>/dev/null || true

echo -e "${GREEN}✓ Containers stopped${NC}"
echo ""

echo -e "${BLUE}Step 2: Removing existing containers...${NC}"
docker rm $(docker ps -a -q) 2>/dev/null || true

echo -e "${GREEN}✓ Containers removed${NC}"
echo ""

echo -e "${BLUE}Step 3: Cleaning up unused images...${NC}"
echo "   Removing dangling images..."
docker image prune -f 2>/dev/null || true

echo "   Removing unused images..."
docker image prune -a -f 2>/dev/null || true

echo -e "${GREEN}✓ Images cleaned${NC}"
echo ""

echo -e "${BLUE}Step 4: Cleaning up volumes (optional)...${NC}"
echo "   Removing unused volumes..."
docker volume prune -f 2>/dev/null || true

echo -e "${GREEN}✓ Volumes cleaned${NC}"
echo ""

echo -e "${BLUE}Step 5: Creating Docker network...${NC}"
docker network create ${NETWORK_NAME} 2>/dev/null || true

echo -e "${GREEN}✓ Network created: ${NETWORK_NAME}${NC}"
echo ""

echo -e "${YELLOW}=====================================================================${NC}"
echo -e "${YELLOW}CLEANUP COMPLETE!${NC}"
echo -e "${YELLOW}=====================================================================${NC}"
echo ""

# Check if user wants to start containers
echo -e "${BLUE}Step 6: Starting Docker containers...${NC}"
echo "   This will pull images and start all services."
echo ""

# Navigate to project root (assuming script is in config/ directory)
cd "$(dirname "$0")/.." || exit

# Start containers
docker-compose up -d

echo ""
echo -e "${GREEN}✓ Containers started successfully!${NC}"
echo ""
echo -e "${BLUE}Waiting for services to be healthy...${NC}"
sleep 10

# Check container status
echo ""
docker-compose ps
echo ""
echo -e "${BLUE}Project Configuration:${NC}"
echo "  Project Name: ${PROJECT_NAME}"
echo "  Network: ${NETWORK_NAME}"
echo ""
echo -e "${BLUE}Service Details:${NC}"
echo ""
echo "   PostgreSQL (Main)"
echo "    - User: ${PG_USER}"
echo "    - Password: ${PG_PASSWORD}"
echo "    - Database: ${PG_DATABASE}"
echo "    - Port: ${PG_PORT}"
echo ""
echo "   PostgreSQL (Airflow DB)"
echo "    - User: ${AIRFLOW_DB_USER}"
echo "    - Password: ${AIRFLOW_DB_PASSWORD}"
echo "    - Port: ${AIRFLOW_DB_PORT}"
echo ""
echo "   Apache Airflow"
echo "    - User: ${AIRFLOW_USER}"
echo "    - Password: ${AIRFLOW_PASSWORD}"
echo "    - Port: ${AIRFLOW_PORT}"
echo "    - URL: http://localhost:${AIRFLOW_PORT}"
echo ""
echo "  🔧 pgAdmin (Database Management UI)"
echo "    - Email: ${PGADMIN_EMAIL}"
echo "    - Password: ${PGADMIN_PASSWORD}"
echo "    - Port: ${PGADMIN_PORT}"
echo "    - URL: http://localhost:${PGADMIN_PORT}"
echo ""
echo "   Apache Superset (Data Visualization)"
echo "    - Port: ${SUPERSET_PORT}"
echo "    - URL: http://localhost:${SUPERSET_PORT}"
echo "    - Note: Requires initial setup on first access"
echo ""
echo -e "${YELLOW}=====================================================================${NC}"
echo -e "${YELLOW}QUICK ACCESS - WEB INTERFACES${NC}"
echo -e "${YELLOW}=====================================================================${NC}"
echo ""
echo -e "${GREEN} Browser Access URLs:${NC}"
echo "   • Airflow UI:  http://localhost:${AIRFLOW_PORT}"
echo "   • pgAdmin:     http://localhost:${PGADMIN_PORT}"
echo "   • Superset:    http://localhost:${SUPERSET_PORT}"
echo ""
echo -e "${BLUE} Quick Login Credentials:${NC}"
echo "   Airflow  → Username: ${AIRFLOW_USER} | Password: ${AIRFLOW_PASSWORD}"
echo "   pgAdmin  → Email: ${PGADMIN_EMAIL} | Password: ${PGADMIN_PASSWORD}"
echo ""
echo -e "${GREEN} All services are now running!${NC}"
echo -e "${BLUE} Tip: Use 'docker-compose logs -f [service]' to view logs${NC}"
echo ""