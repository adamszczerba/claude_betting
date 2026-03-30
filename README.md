# Build and start both scrapers with their VPNs
docker compose up -d --build

# View logs
docker compose logs -f

# Stop
docker compose down