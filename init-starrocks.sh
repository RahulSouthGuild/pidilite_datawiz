#!/bin/bash

################################################################################
# StarRocks Production Initialization Script
#
# This script prepares the system for StarRocks deployment and initializes
# the cluster after container startup.
#
# Usage: ./init-starrocks.sh [prepare|start|register|status|all]
################################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
STARROCKS_DATA_DIR="/var/lib/starrocks"
FE_CONTAINER="starrocks-fe-1"
BE_CONTAINERS=("starrocks-be-1" "starrocks-be-2")
INIT_CONTAINER="starrocks-init"
FE_PORT=9030
FE_HTTP_PORT=8030
BE_HTTP_PORTS=(8040 8041)

################################################################################
# Helper Functions
################################################################################

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_section() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

check_command() {
    if ! command -v $1 &> /dev/null; then
        print_error "$1 is not installed. Please install it first."
        exit 1
    fi
}

wait_for_health() {
    local url=$1
    local service=$2
    local max_attempts=30
    local attempt=1

    print_info "Waiting for $service to be healthy..."

    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            print_success "$service is healthy!"
            return 0
        fi

        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo ""
    print_error "$service did not become healthy in time"
    return 1
}

################################################################################
# Step 1: Prepare System
################################################################################

prepare_system() {
    print_section "Step 1: Preparing System"

    # Check required commands
    print_info "Checking required commands..."
    check_command "docker"
    # Check for docker compose (new) or docker-compose (legacy)
    if command -v "docker" &> /dev/null && docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    elif command -v "docker-compose" &> /dev/null; then
        DOCKER_COMPOSE="docker-compose"
    else
        print_error "Neither 'docker compose' nor 'docker-compose' is available"
        exit 1
    fi
    print_success "Using: $DOCKER_COMPOSE"
    check_command "curl"

    # Check Docker daemon
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker daemon is not running"
        exit 1
    fi
    print_success "Docker daemon is running"

    # Create data directories
    print_info "Creating StarRocks data directories..."
    sudo mkdir -p "$STARROCKS_DATA_DIR"/{fe-meta,fe-log,be-storage,be-log}

    # Set permissions
    print_info "Setting directory permissions..."
    sudo chown -R $USER:$USER "$STARROCKS_DATA_DIR"
    chmod -R 755 "$STARROCKS_DATA_DIR"

    print_success "Data directories created: $STARROCKS_DATA_DIR"

    # Check system limits
    print_info "Checking system limits..."
    local nofile_limit=$(ulimit -n)
    local nproc_limit=$(ulimit -u)

    if [ "$nofile_limit" -lt 65535 ]; then
        print_warning "File descriptor limit is $nofile_limit (recommended: 65535)"
        print_info "To increase, run: ulimit -n 65535"
    else
        print_success "File descriptor limit is adequate: $nofile_limit"
    fi

    if [ "$nproc_limit" -lt 65535 ]; then
        print_warning "Process limit is $nproc_limit (recommended: 65535)"
        print_info "To increase, run: ulimit -u 65535"
    else
        print_success "Process limit is adequate: $nproc_limit"
    fi

    # Check available disk space
    print_info "Checking disk space..."
    local available_space=$(df -BG "$STARROCKS_DATA_DIR" | awk 'NR==2 {print $4}' | sed 's/G//')

    if [ "$available_space" -lt 100 ]; then
        print_warning "Available disk space is ${available_space}GB (recommended: 200GB+)"
    else
        print_success "Available disk space: ${available_space}GB"
    fi

    print_success "System preparation complete!"
}

################################################################################
# Step 2: Start Services
################################################################################

start_services() {
    print_section "Step 2: Starting StarRocks Services"

    # Check if compose file exists
    if [ ! -f "docker-compose.yml" ]; then
        print_error "docker-compose.yml not found in current directory"
        exit 1
    fi

    # Start services
    print_info "Starting Docker Compose services..."
    $DOCKER_COMPOSE up -d starrocks-fe-1 starrocks-be-1 starrocks-be-2 starrocks-init

    print_info "Waiting for containers to initialize..."
    sleep 10

    # Check FE container status
    if ! docker ps | grep -q "$FE_CONTAINER"; then
        print_error "FE container is not running"
        $DOCKER_COMPOSE logs starrocks-fe-1 | tail -20
        exit 1
    fi

    # Check BE container status
    for be_container in "${BE_CONTAINERS[@]}"; do
        if ! docker ps | grep -q "$be_container"; then
            print_error "$be_container is not running"
            $DOCKER_COMPOSE logs "$be_container" | tail -20
            exit 1
        fi
    done

    print_success "Containers started successfully"

    # Wait for FE health
    wait_for_health "http://localhost:$FE_HTTP_PORT/api/health" "StarRocks FE" || {
        print_error "FE health check failed. Showing logs:"
        $DOCKER_COMPOSE logs --tail=50 starrocks-fe-1
        exit 1
    }

    # Wait for BE health
    for i in "${!BE_CONTAINERS[@]}"; do
        be_container="${BE_CONTAINERS[$i]}"
        be_port="${BE_HTTP_PORTS[$i]}"
        wait_for_health "http://localhost:$be_port/api/health" "$be_container" || {
            print_error "$be_container health check failed. Showing logs:"
            $DOCKER_COMPOSE logs --tail=50 "$be_container"
            exit 1
        }
    done

    print_success "All services are healthy!"

    # Wait for database initialization
    print_info "Waiting for database initialization..."
    sleep 5

    if docker ps -a | grep -q "$INIT_CONTAINER"; then
        print_info "Checking database initialization status..."
        docker logs $INIT_CONTAINER

        if docker ps -a --filter "name=$INIT_CONTAINER" --filter "status=exited" --filter "exited=0" | grep -q "$INIT_CONTAINER"; then
            print_success "Database and user initialized successfully!"
        else
            print_warning "Database initialization may have issues. Check logs: docker logs $INIT_CONTAINER"
        fi
    fi
}

################################################################################
# Step 3: Initialize Database and User
################################################################################

init_database_and_user() {
    print_section "Step 3: Initializing Database and User"

    # Check if FE is accessible
    if ! docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SELECT 1" > /dev/null 2>&1; then
        print_error "Cannot connect to FE MySQL interface"
        exit 1
    fi

    print_success "Connected to FE MySQL interface"

    # Check if database already exists
    DB_EXISTS=$(docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -sN -e "SHOW DATABASES LIKE 'datawiz'" 2>/dev/null || echo "")

    if [ -n "$DB_EXISTS" ]; then
        print_warning "Database 'datawiz' already exists"
    else
        print_info "Creating database 'datawiz'..."
        docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "CREATE DATABASE IF NOT EXISTS datawiz;" || {
            print_error "Failed to create database"
            exit 1
        }
        print_success "Database 'datawiz' created successfully"
    fi

    # Check if user already exists
    USER_EXISTS=$(docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -sN -e "SELECT COUNT(*) FROM mysql.user WHERE user='datawiz_admin' AND host='%'" 2>/dev/null || echo "0")

    if [ "$USER_EXISTS" -gt 0 ]; then
        print_warning "User 'datawiz_admin' already exists"
    else
        print_info "Creating user 'datawiz_admin'..."
        docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "CREATE USER IF NOT EXISTS 'datawiz_admin'@'%' IDENTIFIED BY '0jqhC3X541tP1RmR.5';" || {
            print_error "Failed to create user"
            exit 1
        }
        print_success "User 'datawiz_admin' created successfully"
    fi

    # Grant privileges (comprehensive permissions for table creation and data operations)
    print_info "Granting privileges to 'datawiz_admin'..."
    
    # Grant ALL PRIVILEGES on datawiz database for all table operations
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e \
        "GRANT ALL PRIVILEGES ON datawiz.* TO 'datawiz_admin'@'%';" || {
        print_error "Failed to grant privileges on datawiz"
        exit 1
    }
    print_success "Table privileges granted successfully"

    # Grant SELECT on information_schema for schema inspection
    print_info "Granting SELECT on information_schema..."
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e \
        "GRANT SELECT ON information_schema.* TO 'datawiz_admin'@'%';" || {
        print_warning "Failed to grant information_schema access (may already be granted)"
    }
    
    # Grant db_admin role for full database administration capabilities
    print_info "Granting db_admin role to 'datawiz_admin'..."
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e \
        "GRANT db_admin TO USER 'datawiz_admin'@'%';" 2>/dev/null || {
        print_warning "db_admin role grant failed (may already be granted)"
    }

    # Set db_admin as default role for 'datawiz_admin'
    print_info "Activating db_admin role as default for 'datawiz_admin'..."
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e \
        "SET DEFAULT ROLE db_admin TO 'datawiz_admin'@'%';" || {
        print_warning "Failed to set default role (may already be set)"
    }
    
    print_success "All privileges and roles granted successfully"

    print_success "User privileges granted successfully"

    # Verify database exists
    print_info "Verifying database creation..."
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW DATABASES LIKE 'datawiz';" 2>/dev/null || {
        print_warning "Could not verify database"
    }

    print_success "Database 'datawiz' and user 'datawiz_admin' initialized successfully!"
    echo ""
    print_info "Access Information:"
    echo "  Database: datawiz"
    echo "  Username: datawiz_admin (or use 'root' for admin access)"
    echo "  Password: 0jqhC3X541tP1RmR.5"
    echo "  Host: 127.0.0.1 or localhost"
    echo "  Port: 9030"
    echo ""
    print_info "For table creation with 2 backend nodes, use replication_num='1':"
    echo "  CREATE TABLE table_name (...) PROPERTIES('replication_num'='1');"
    echo ""
    print_warning "Note: datawiz_admin has read/write access. Use 'root' for DDL operations."
}


################################################################################
# Step 4: Register Backend
################################################################################

register_backend() {
    print_section "Step 4: Registering Backend with Frontend"

    # Check if FE is accessible
    if ! docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SELECT 1" > /dev/null 2>&1; then
        print_error "Cannot connect to FE MySQL interface"
        exit 1
    fi

    print_success "Connected to FE MySQL interface"

    # Check if BEs are already registered
    BE_COUNT=$(docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -sN -e "SHOW BACKENDS" 2>/dev/null | wc -l || echo "0")

    if [ "$BE_COUNT" -ge 2 ]; then
        print_warning "Backends already registered (count: $BE_COUNT)"
        print_info "Showing current backends:"
        docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW BACKENDS\G"
        return 0
    fi

    # Register all BE nodes
    print_info "Registering BE nodes..."
    
    # Register each BE
    for be_container in "${BE_CONTAINERS[@]}"; do
        print_info "Registering $be_container..."
        docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "ALTER SYSTEM ADD BACKEND '$be_container:9050';" || {
            print_warning "Failed to register $be_container (may already exist)"
        }
    done

    # Wait a moment for registration
    sleep 5

    # Verify registration
    BE_COUNT=$(docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -sN -e "SHOW BACKENDS" 2>/dev/null | wc -l || echo "0")

    if [ "$BE_COUNT" -ge 2 ]; then
        print_success "All backends registered successfully! (count: $BE_COUNT)"
        print_info "Backend details:"
        docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW BACKENDS\G"
    else
        print_warning "Some backends may not be registered properly (count: $BE_COUNT)"
        print_info "Current backends:"
        docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW BACKENDS\G"
    fi
}

################################################################################
# Step 5: Check Status
################################################################################

check_status() {
    print_section "Cluster Status"

    # Container status
    print_info "Container Status:"
    $DOCKER_COMPOSE ps starrocks-fe-1 starrocks-be-1 starrocks-be-2
    echo ""

    # Health endpoints
    print_info "Health Check Results:"

    if curl -sf "http://localhost:$FE_HTTP_PORT/api/health" > /dev/null 2>&1; then
        print_success "FE Health: OK"
    else
        print_error "FE Health: FAILED"
    fi

    # Check all BE health endpoints
    for i in "${!BE_CONTAINERS[@]}"; do
        be_container="${BE_CONTAINERS[$i]}"
        be_port="${BE_HTTP_PORTS[$i]}"
        if curl -sf "http://localhost:$be_port/api/health" > /dev/null 2>&1; then
            print_success "$be_container Health: OK"
        else
            print_error "$be_container Health: FAILED"
        fi
    done
    echo ""

    # Database status
    print_info "Frontend Nodes:"
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW FRONTENDS\G" 2>/dev/null || print_error "Cannot query FE status"
    echo ""

    print_info "Backend Nodes:"
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW BACKENDS\G" 2>/dev/null || print_error "Cannot query BE status"
    echo ""

    # Database and User Status
    print_info "Database Status:"
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW DATABASES" 2>/dev/null || print_error "Cannot query databases"
    echo ""

    print_info "Users:"
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SELECT user, host FROM mysql.user WHERE user IN ('root', 'datawiz_admin')" 2>/dev/null || print_error "Cannot query users"
    echo ""

    # Resource usage
    print_info "Resource Usage:"
    docker stats --no-stream $FE_CONTAINER "${BE_CONTAINERS[@]}"
    echo ""

    # Access information
    print_section "Access Information"
    echo -e "${GREEN}StarRocks FE Web UI:${NC}  http://localhost:8030"
    echo -e "${GREEN}MySQL (root):${NC}        mysql -h 127.0.0.1 -P 9030 -u root"
    echo -e "${GREEN}MySQL (datawiz_admin):${NC} mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p"
    echo -e "${GREEN}StarRocks BE Web UIs:${NC}"
    for i in "${!BE_CONTAINERS[@]}"; do
        be_container="${BE_CONTAINERS[$i]}"
        be_port="${BE_HTTP_PORTS[$i]}"
        echo -e "  ${be_container}: http://localhost:${be_port}"
    done
    echo -e "${GREEN}Database:${NC}            datawiz"
    echo ""
}

################################################################################
# Main Script
################################################################################

show_usage() {
    echo "Usage: $0 [prepare|start|init-db|register|status|all]"
    echo ""
    echo "Commands:"
    echo "  prepare   - Prepare system (create directories, check requirements)"
    echo "  start     - Start StarRocks containers"
    echo "  init-db   - Initialize database and create user with privileges"
    echo "  register  - Register BE with FE"
    echo "  status    - Show cluster status"
    echo "  all       - Run all steps (prepare, start, init-db, register, status)"
    echo ""
}

main() {
    local command=${1:-all}

    case $command in
        prepare)
            prepare_system
            ;;
        start)
            start_services
            ;;
        init-db)
            init_database_and_user
            ;;
        register)
            register_backend
            ;;
        status)
            check_status
            ;;
        all)
            prepare_system
            start_services
            init_database_and_user
            register_backend
            check_status

            print_section "Initialization Complete!"
            print_success "StarRocks cluster is ready for use"
            echo ""
            echo -e "${GREEN}Next steps:${NC}"
            echo "1. Connect to StarRocks (root): mysql -h 127.0.0.1 -P 9030 -u root"
            echo "2. Connect to StarRocks (datawiz_admin): mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p"
            echo "   Password: 0jqhC3X541tP1RmR.5"
            echo "3. Database: datawiz"
            echo "4. Review STARROCKS_SETUP.md for detailed configuration"
            echo "5. Start your ETL application: docker-compose up -d pidilite-datawiz"
            ;;
        *)
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
