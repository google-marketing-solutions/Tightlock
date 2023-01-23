# Stage 1: Copy all contents of file to docker as app (excluding what is in .dockerignore)

FROM node:alpine AS my-app-build
WORKDIR /app
COPY . .
RUN npm ci && npm run build

# Stage 2: Copy the output from Stage 1, specifically what is in the dist folder to nginx.
# Exposing port 80.

FROM nginx:alpine
COPY --from=my-app-build /app/dist/app /usr/share/nginx/html
EXPOSE 80