# GstInterpipe 1.0
GstInterpipe is a Gstreamer plug-in that allows communication between
two independent pipelines. The plug-in consists of two elements:
- interpipesink
- interpipesrc

The concept behind the Interpipes project is to simplify the
construction of GStreamer applications, which often has the complexity
of requiring dynamic pipelines. It transforms the construction process
from low level pad probe manipulation to the higher level setting an
element's parameter value. Application developers don't get mired down
in stalled pipelines due to one branch of a complex pipeline changed
state.

The official user documentation is held at [RidgeRun's Developers
Wiki](http://developer.ridgerun.com/wiki/index.php?title=GstInterpipe)

The API reference is uploaded to [GitHub's project
page](http://ridgerun.github.io/gst-interpipe/).

## Building and testing with Docker

The repository ships three Docker images so the plugin can be built and
exercised against a known GStreamer (Debian Sid, currently 1.28.x) without
touching the host. Each `Dockerfile*` is self-contained; run the commands from
the repository root.

### `Dockerfile` — build and run the check suite

The default image builds the plugin with Meson and runs the full `gstcheck`
suite (the same `ninja -C build test` as a local build).

```sh
docker build -t interpipe-ci -f Dockerfile .
docker run --rm interpipe-ci
```

To test working-tree changes without rebuilding the image, bind-mount the
source and build into a container-local directory (so the host `build/` is not
clobbered):

```sh
docker run --rm -v "$PWD":/src -w /src interpipe-ci sh -c \
  'meson setup /tmp/b -Denable-gtk-doc=false && ninja -C /tmp/b && meson test -C /tmp/b --print-errorlogs'
```

### `Dockerfile.valgrind` — leak checks

Builds a debug binary and provides combined GLib + GStreamer suppressions at
`/opt/interpipe.supp`, so only real leaks surface. Note the Meson test
executables are named `tests/check/gst_test_<name>`.

```sh
docker build -t interpipe-valgrind -f Dockerfile.valgrind .
docker run --rm -v "$PWD":/src -w /src interpipe-valgrind sh -c '
  meson setup /tmp/b -Dbuildtype=debug -Denable-gtk-doc=false && ninja -C /tmp/b
  CK_FORK=no GST_PLUGIN_PATH=/tmp/b/gst/interpipe \
    valgrind --leak-check=full --suppressions=/opt/interpipe.supp \
    /tmp/b/tests/check/gst_test_cold_attach'
```

`CK_FORK=no` keeps the check framework from forking so valgrind follows the
whole run.

### `Dockerfile.gstd` — cross-process cold-attach reproduction

Some behaviours (notably the cold-attach negotiation race) only appear under a
real daemon driving independent pipeline lifecycles, because interpipe's node
registry is per-process — two `gst-launch` processes never see each other. This
image builds [gstd](https://github.com/cfsbhawkins/gstd-1.x) on top of the same
GStreamer and the `tests/gstd/` scripts drive a three-leg split over gstd's HTTP
API, counting `not-negotiated` errors and buffers per node.

```sh
docker build -t interpipe-gstd -f Dockerfile.gstd .
# Build the plugin from the bind-mounted tree, run the repro, report the verdict:
docker run --rm -v "$PWD":/src -w /src interpipe-gstd bash tests/gstd/run.sh ordered
docker run --rm -v "$PWD":/src -w /src interpipe-gstd bash tests/gstd/run.sh rapid
```

A clean tree reports zero `not-negotiated` errors with buffers flowing through
every leg (exit status 0); a regressed tree is non-zero.

GstInterpipe copyright (C) 2016-2022 RidgeRun LLC

This GStreamer plug-in is free software; you can redistribute it
and/or modify it under the terms of the GNU Lesser General Public
License version 2.1 as published by the Free Software Foundation.

This GStreamer plug-in is distributed in the hope that it will be
useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Lesser General Public License below for more details.
