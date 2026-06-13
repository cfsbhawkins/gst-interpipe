# Debian Sid currently ships GStreamer 1.28.x, which is the version this
# CI image targets for building and testing the interpipe plugin.
FROM debian:sid-slim

ENV DEBIAN_FRONTEND=noninteractive

# Build/test dependencies for the Meson path:
#   - libgstreamer1.0-dev             -> gstreamer-1.0, gstreamer-base, gstreamer-check-1.0
#   - libgstreamer-plugins-base1.0-dev-> gstreamer-app-1.0
#   - gstreamer1.0-plugins-base       -> videotestsrc/audiotestsrc/appsrc/appsink used by the tests
#   - gstreamer1.0-tools              -> gst-inspect-1.0 for the manual inspection gate
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    ninja-build \
    meson \
    libglib2.0-dev \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    gstreamer1.0-plugins-base \
    gstreamer1.0-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /gst-interpipe

# Copy source code
COPY . .

# Let gst-inspect-1.0 find the freshly built plugin inside the container.
ENV GST_PLUGIN_PATH=/gst-interpipe/build/gst/interpipe

# Default: configure with Meson, build, and run the full check suite.
# gtk-doc is disabled — this image builds and tests the plugin, not its docs.
CMD ["sh", "-c", "meson setup build -Denable-gtk-doc=false --wipe 2>/dev/null || meson setup build -Denable-gtk-doc=false && ninja -C build && ninja -C build test"]
