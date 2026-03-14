
docker run -it -d --network=warehouse_network --hostname=warehouse.local --name=warehouse -p 443:443 -p 50051:50051 -v c:\code\kionas:/workspace -e KIONAS_HOME=/workspace docker-devcontainer 