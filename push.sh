DOCKER_BUILDKIT=1 docker buildx build --push --platform=linux/arm64,linux/amd64 -t izihawa/trident:latest .
