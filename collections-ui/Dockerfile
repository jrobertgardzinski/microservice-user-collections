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
# the one server block this image needs: static bundle + a same-origin proxy to the gallery, so
# the favourites view can tell "this meme is gone" apart from "the gallery did not answer" (see
# nginx.conf.template and src/refs.ts). Templated, so a deployment can repoint it without a rebuild
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
# where the gallery lives, and which DNS answers for it. The defaults are the compose stack's;
# a cluster deployment overrides MEMES_RESOLVER with its own DNS service (in k8s:
# kube-dns.kube-system.svc.cluster.local). Getting it wrong costs a 502 on the check, which the
# page renders as "couldn't check" — never as a deletion offer
ENV MEMES_UPSTREAM=http://memes:8083 \
    MEMES_RESOLVER=127.0.0.11 \
    NGINX_ENVSUBST_FILTER=^MEMES_
EXPOSE 80
