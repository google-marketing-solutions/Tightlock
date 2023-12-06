# Stage 1: Copy all contents of file to docker as app (excluding what is in .dockerignore)

FROM node:21.4.0-alpine3.17 AS tightlock-frontend
WORKDIR /app
COPY ./tightlock_ui .
RUN npm ci && npm run build

# Stage 2: Copy the output from Stage 1, specifically what is in the dist folder to nginx.
# Exposing port 8080.

FROM nginx:alpine
COPY --from=tightlock-frontend /app/dist/tightlock_ui /usr/share/nginx/html
VOLUME ["/etc/nginx/conf.d/"]
COPY ./tightlock_ui/site.template /etc/nginx/conf.d/
CMD ["/bin/sh", "-c", "envsubst '' < /etc/nginx/conf.d/site.template > /etc/nginx/conf.d/default.conf && exec nginx -g 'daemon off;'"]
EXPOSE 8080
