# Webserver

A minimal static HTTP server written in **C**.

`webserver` serves static files from any directory with live browser reload on file changes.  
It uses **POSIX sockets**, **pthreads**, **sendfile()**, and **inotify** — no external dependencies.

---

## Features

- Serve static files from any directory
- **Live reload** — browser auto-refreshes when any file is saved
- Thread pool (16 workers, 256-slot queue) with backpressure
- HTTP/1.1 Keep-Alive for connection reuse
- Zero-copy file transfer via `sendfile()`
- `Cache-Control` and `Last-Modified` headers for static assets
- Directory listing with single-syscall `writev()` response
- Directory fallback to `index.html`
- Extended MIME type support (`.mjs`, `.woff2`, `.webp`, `.wasm`, …)
- Path traversal protection

---

## Requirements

- Linux (uses `sendfile`, `inotify`)
- GCC or Clang
- pthreads

---

## Build

```bash
gcc webserver.c -o webserver -pthread -O2 -march=native -flto
```

---

## Usage

```bash
./webserver [directory] [port]
```

| Argument    | Default     | Description                  |
|-------------|-------------|------------------------------|
| `directory` | `.`         | Directory to serve           |
| `port`      | `9090`      | Port to listen on            |

**Examples:**

```bash
./webserver                      # serve current directory on :9090
./webserver ~/my-site            # serve ~/my-site on :9090
./webserver ~/my-site 8080       # serve ~/my-site on :8080
```

---

## Live reload

The server injects a small script into every HTML response:

```html
<script>new EventSource('/_live-reload').onmessage=()=>location.reload()</script>
```

It watches the served directory recursively with `inotify`. When any file is saved, all connected browsers reload instantly. No browser extension or build tool required.
