Write-Host "=== Building microservices ==="

cd seller-service
./mvnw clean package -DskipTests
docker build -t seller-service .
cd ..

cd bidder-service
./mvnw clean package -DskipTests
docker build -t bidder-service .
cd ..

cd auction-manager-service
./mvnw clean package -DskipTests
docker build -t auction-manager-service .
cd ..

Write-Host "=== Starting Docker Compose ==="
docker compose up -d

Write-Host "=== System is running ==="
