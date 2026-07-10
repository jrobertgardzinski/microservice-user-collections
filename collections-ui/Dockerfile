# The favourites UI — React Native's component model in the browser (react-native-web via Vite),
# built here and served as static files by nginx. Its own origin (:8093 on the host) is the point:
# the browser then calls security (:8080) and user-collections (:8092) CROSS-origin, exercising
# both services' CORS edges. API URLs are baked at build time (VITE_*_URL); the defaults target
# the compose stack's host-published ports.
FROM node:22-alpine AS build
WORKDIR /ui
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /ui/dist /usr/share/nginx/html
EXPOSE 80
