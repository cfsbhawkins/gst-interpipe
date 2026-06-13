# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

GstInterpipe is a GStreamer 1.0 plug-in that lets two independent pipelines exchange buffers and events without low-level pad-probe manipulation. It registers two elements:

- **interpipesink** — extends `GstAppSink`; acts as a *node* that buffers/events flow into.
- **interpipesrc** — extends `GstAppSrc`; acts as a *listener* that attaches to a named sink and pulls its data into another pipeline.

The point is dynamic pipeline switching done by setting an element property (`interpipesrc listen-to=<sink-name>`) rather than rewiring pads, so one stalled branch can't block another.

## Build & test

Build with Meson (preferred):

```sh
meson build
ninja -C build
ninja -C build test          # run the full check suite
meson test -C build <name>   # run a single test, e.g. test_set_caps
```

Autotools is also supported via `./autogen.sh` (runs `git submodule update` for `common/`, then `autoreconf` + `./configure`), followed by `make` / `make check`. The `common/` submodule must be present for the autotools path.

Try the plugin manually after building:

```sh
GST_PLUGIN_PATH=build/gst/interpipe gst-inspect-1.0 interpipesrc
```

External deps: `gstreamer-app-1.0` and `gstreamer-check-1.0` (>= 1.0.5).

## Architecture

The core decouples sinks from sources through a global registry plus two GInterfaces. Read these together — the indirection only makes sense as a set:

- **[gstinterpipe.c](gst/interpipe/gstinterpipe.c)** — the registry. Two global hash-table singletons (guarded by `nodes_mutex` and the recursive `listeners_mutex`) map names → nodes and names → listeners. `gst_inter_pipe_add_node` / `remove_node` and `listen_node` / `leave_node` are the only entry points. When a node appears or disappears, every registered listener is notified via `g_hash_table_foreach`, so a listener can `listen-to` a sink that does not exist yet and connect later.
- **[gstinterpipeinode.h](gst/interpipe/gstinterpipeinode.h)** (`GstInterPipeINode`) — interface implemented by **interpipesink**. Holds a list of attached listeners and forwards upstream events to them.
- **[gstinterpipeilistener.h](gst/interpipe/gstinterpipeilistener.h)** (`GstInterPipeIListener`) — interface implemented by **interpipesrc**. Receives `push_buffer` / `push_event` / `send_eos` callbacks and handles caps negotiation (`get_caps` / `set_caps`) plus `node_added` / `node_removed` notifications.

Data flow: a buffer entering interpipesink is fanned out to each attached interpipesrc via the listener interface, with `basetime` passed through so each src can re-timestamp into its own pipeline's clock.

Key element properties (the behavioral surface most tests exercise):
- interpipesink: `forward-eos`, `forward-events`, `num-listeners` (read-only).
- interpipesrc: `listen-to`, `block-switch`, `allow-renegotiation`, `stream-sync`, `accept-events`, `accept-eos-event`.

## Tests

Tests live in [tests/check/gst/](tests/check/gst/) using the GStreamer `gstcheck` framework. Each `test_*.c` builds a standalone executable and constructs real pipelines (one sink pipeline + one or more src pipelines) to verify a single behavior — caps negotiation/renegotiation, hot-plug switching, stream-sync, event bounds, EOS forwarding, etc. When adding a behavior, add a matching `test_*.c` and register it in the `core_tests` list in [tests/check/meson.build](tests/check/meson.build) (and the autotools `Makefile.am`).

## Conventions

- C code follows the GStreamer coding style (gst-indent / GNU-style, 2-space indent, declarations before statements — `-Wdeclaration-after-statement` is enabled). The `common/` submodule installs a pre-commit hook enforcing this.
- All public symbols use the `gst_inter_pipe_` prefix; types use `GstInterPipe*`.
- Licensed LGPL 2.1; keep the existing license header on new files.
