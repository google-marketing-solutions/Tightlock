# Create env
NON_INTERACTIVE_FLAG=$1
./create_env.sh $NON_INTERACTIVE_FLAG

# Run containers
docker-compose up --build -d
