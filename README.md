# Webserver

A minimal multithreaded static HTTP server written in **C**.

`webserver` is a lightweight web server capable of serving static files from any directory.  
It uses **POSIX sockets**, **pthread**, and **sendfile()** for efficient file transfer.

The goal of this project is to demonstrate how a basic web server works internally while keeping the code simple and readable.

---

## Features

- Serve static files from any directory
- Custom listening port
- Multithreaded request handling
- Zero-copy file transfer using `sendfile()`
- Basic MIME type detection
- Directory fallback to `index.html`
- Simple path traversal protection (`..`)
- Lightweight and dependency-free

---

## Requirements

- Linux / Unix system
- GCC or Clang
- POSIX threads

---

## Build

Compile the server using:

```bash
gcc server.c -o webserver -pthread
