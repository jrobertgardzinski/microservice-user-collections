# The favourites UI — React Native's component model in the browser (react-native-web via Vite),
# built here and served as static files by nginx. Its own origin (:8093 on the host) is the point:
# the browser then calls security (:8080) and user-collections (:8092) CROSS-origin, exercising
# both services' CORS edges. API URLs come from the DEPLOYMENT (ui-config.sh writes them at
# container start), falling back to the VITE_*_URL values baked at build time and then to the
# compose stack's host-published ports.
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
# Runs before nginx starts (the image executes every /docker-entrypoint.d/*.sh) and writes the API
# addresses this page's BROWSER should call into the document root. Numbered 25 so it lands after
# the image's own 20-envsubst step; that step targets nginx CONFIG, not files under the doc root,
# which is why this is a script rather than another template. Unset variables fall back to compose's
# addresses, so an unconfigured deployment behaves exactly as this image always did.
COPY ui-config.sh /docker-entrypoint.d/25-ui-config.sh
RUN chmod +x /docker-entrypoint.d/25-ui-config.sh
# where the gallery lives, and which DNS answers for it. The defaults are the compose stack's;
# a cluster deployment overrides MEMES_RESOLVER with its own DNS service (in k8s:
# kube-dns.kube-system.svc.cluster.local). Getting it wrong costs a 502 on the check, which the
# page renders as "couldn't check" — never as a deletion offer
ENV MEMES_UPSTREAM=http://memes:8083 \
    MEMES_RESOLVER=127.0.0.11 \
    NGINX_ENVSUBST_FILTER=^MEMES_
EXPOSE 80
