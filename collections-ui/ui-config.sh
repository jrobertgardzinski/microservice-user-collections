#!/bin/sh
# Writes the addresses this page's BROWSER should call, at container start.
#
# Vite substitutes import.meta.env.VITE_* at BUILD time, so the bundle baked into this image had
# exactly one set of API addresses in it: compose's localhost ports. Served from a cluster, the very
# same page told the browser to call the developer's own machine — the UI rendered, nginx was
# healthy, and nothing worked. An image whose behaviour is fixed at build time cannot be deployed
# twice.
#
# The nginx image runs every executable /docker-entrypoint.d/*.sh before starting the server, which
# is why this is a script and not another envsubst template: the envsubst step targets nginx CONFIG
# (/etc/nginx/conf.d), not files under the document root.
#
# index.html loads the result as a CLASSIC script, so it runs before the deferred module bundle and
# the values are simply present when the first module reads them. Unset variables fall back to
# compose's addresses, so this image behaves exactly as it did before when nothing is configured.
set -eu

: "${UI_SECURITY_URL:=http://localhost:8080}"
: "${UI_COLLECTIONS_URL:=http://localhost:8092}"
# empty on purpose: the gallery is reached same-origin through the nginx proxy in front of this
# bundle (see nginx.conf.template) - an absolute URL here would make it cross-origin and blocked
: "${UI_MEMES_URL:=}"

cat > /usr/share/nginx/html/ui-config.js <<EOF
window.__PORTAL_CONFIG__ = {
  securityUrl: "${UI_SECURITY_URL}",
  collectionsUrl: "${UI_COLLECTIONS_URL}",
  memesUrl: "${UI_MEMES_URL}"
};
EOF
