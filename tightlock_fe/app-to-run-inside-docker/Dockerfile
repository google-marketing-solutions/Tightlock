# Step 1: Copy local files and install dependencies.

FROM node:alpine AS my-app-build
WORKDIR /app
COPY . .
RUN npm ci && npm run build

# Step 2: Copy what is built from docker container in Step 1
# above and move to nginx for serving.

FROM nginx:alpine
COPY --from=my-app-build /app/dist/app-to-run-inside-docker /usr/share/nginx/html
EXPOSE 80
