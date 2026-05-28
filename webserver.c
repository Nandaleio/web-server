#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/inotify.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#define BUFFER_SIZE           4096
#define DEFAULT_PORT          9090
#define KEEPALIVE_TIMEOUT_SEC 5
#define POOL_SIZE             16
#define QUEUE_CAP             256
#define LIVE_RELOAD_PATH      "/_live-reload"

char root[1024] = ".";
int port = DEFAULT_PORT;

/* ---------- live reload ---------- */

static pthread_mutex_t reload_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  reload_cond = PTHREAD_COND_INITIALIZER;
static uint64_t        reload_gen  = 0;

static const char live_script[] =
    "<script>new EventSource('/_live-reload').onmessage=()=>location.reload()</script>";

static void add_watches(int ifd, const char *path) {
    inotify_add_watch(ifd, path,
        IN_CLOSE_WRITE | IN_CREATE | IN_DELETE | IN_MOVED_TO);
    DIR *d = opendir(path);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (e->d_type != DT_DIR) continue;
        char sub[1024];
        snprintf(sub, sizeof(sub), "%s/%s", path, e->d_name);
        add_watches(ifd, sub);
    }
    closedir(d);
}

static void *watch_thread(void *arg) {
    (void)arg;
    int ifd = inotify_init();
    if (ifd < 0) return NULL;
    add_watches(ifd, root);

    char buf[4096] __attribute__((aligned(8)));
    while (1) {
        if (read(ifd, buf, sizeof(buf)) > 0) {
            pthread_mutex_lock(&reload_lock);
            reload_gen++;
            pthread_cond_broadcast(&reload_cond);
            pthread_mutex_unlock(&reload_lock);
        }
    }
    close(ifd);
    return NULL;
}

/* SSE endpoint: occupies a worker thread until the browser disconnects */
static void handleSSE(int sock) {
    const char *hdr =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Connection: keep-alive\r\n\r\n";
    if (send(sock, hdr, strlen(hdr), 0) < 0) return;

    uint64_t seen = reload_gen;
    for (;;) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 2;

        pthread_mutex_lock(&reload_lock);
        while (reload_gen == seen) {
            if (pthread_cond_timedwait(&reload_cond, &reload_lock, &ts) == ETIMEDOUT)
                break;
        }
        int changed = (reload_gen != seen);
        seen = reload_gen;
        pthread_mutex_unlock(&reload_lock);

        if (changed) {
            if (send(sock, "data: reload\n\n", 14, 0) < 0) break;
        } else {
            /* heartbeat comment — detects dead connections every 2 s */
            if (send(sock, ":\n\n", 3, 0) < 0) break;
        }
    }
}

/* ---------- thread pool ---------- */

typedef struct {
    int fds[QUEUE_CAP];
    int head, tail, count;
    pthread_mutex_t lock;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} WorkQueue;

static WorkQueue gQueue;
static pthread_t gPool[POOL_SIZE];

static void queue_push(int fd) {
    pthread_mutex_lock(&gQueue.lock);
    while (gQueue.count == QUEUE_CAP)
        pthread_cond_wait(&gQueue.not_full, &gQueue.lock);
    gQueue.fds[gQueue.tail % QUEUE_CAP] = fd;
    gQueue.tail++;
    gQueue.count++;
    pthread_cond_signal(&gQueue.not_empty);
    pthread_mutex_unlock(&gQueue.lock);
}

static int queue_pop(void) {
    pthread_mutex_lock(&gQueue.lock);
    while (gQueue.count == 0)
        pthread_cond_wait(&gQueue.not_empty, &gQueue.lock);
    int fd = gQueue.fds[gQueue.head % QUEUE_CAP];
    gQueue.head++;
    gQueue.count--;
    pthread_cond_signal(&gQueue.not_full);
    pthread_mutex_unlock(&gQueue.lock);
    return fd;
}

/* ---------- MIME ---------- */

const char *mime(const char *path) {
    const char *ext = strrchr(path, '.');

    if (!ext) return "text/plain";
    if (!strcmp(ext, ".html") || !strcmp(ext, ".htm")) return "text/html";
    if (!strcmp(ext, ".css")) return "text/css";
    if (!strcmp(ext, ".js") || !strcmp(ext, ".mjs") || !strcmp(ext, ".cjs")) return "application/javascript";
    if (!strcmp(ext, ".ts") || !strcmp(ext, ".mts") || !strcmp(ext, ".cts")) return "application/typescript";
    if (!strcmp(ext, ".png")) return "image/png";
    if (!strcmp(ext, ".jpg") || !strcmp(ext, ".jpeg")) return "image/jpeg";
    if (!strcmp(ext, ".gif")) return "image/gif";
    if (!strcmp(ext, ".svg")) return "image/svg+xml";
    if (!strcmp(ext, ".webp")) return "image/webp";
    if (!strcmp(ext, ".ico")) return "image/x-icon";
    if (!strcmp(ext, ".json")) return "application/json";
    if (!strcmp(ext, ".xml")) return "application/xml";
    if (!strcmp(ext, ".pdf")) return "application/pdf";
    if (!strcmp(ext, ".wasm")) return "application/wasm";
    if (!strcmp(ext, ".txt")) return "text/plain";
    if (!strcmp(ext, ".csv")) return "text/csv";
    if (!strcmp(ext, ".md")) return "text/markdown";
    if (!strcmp(ext, ".woff")) return "font/woff";
    if (!strcmp(ext, ".woff2")) return "font/woff2";
    if (!strcmp(ext, ".ttf")) return "font/ttf";
    if (!strcmp(ext, ".otf")) return "font/otf";
    if (!strcmp(ext, ".mp4")) return "video/mp4";
    if (!strcmp(ext, ".webm")) return "video/webm";
    if (!strcmp(ext, ".mp3")) return "audio/mpeg";
    if (!strcmp(ext, ".ogg")) return "audio/ogg";
    if (!strcmp(ext, ".wav")) return "audio/wav";
    if (!strcmp(ext, ".zip")) return "application/zip";
    if (!strcmp(ext, ".gz")) return "application/gzip";

    return "application/octet-stream";
}

/* ---------- response helpers ---------- */

static const char *conn_header(int keepalive) {
    return keepalive
        ? "Connection: keep-alive\r\n"
        : "Connection: close\r\n";
}

void send404(int sock, int keepalive) {
    static const char body[] = "404 Not Found";
    char msg[128];
    snprintf(msg, sizeof(msg),
        "HTTP/1.1 404 Not Found\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 13\r\n"
        "%s\r\n"
        "%s",
        conn_header(keepalive), body);
    send(sock, msg, strlen(msg), 0);
}

void sendFile(int sock, const char *path, int keepalive) {

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        send404(sock, keepalive);
        return;
    }

    struct stat st;
    fstat(fd, &st);

    char timebuf[64];
    struct tm tm;
    gmtime_r(&st.st_mtime, &tm);
    strftime(timebuf, sizeof(timebuf), "%a, %d %b %Y %H:%M:%S GMT", &tm);

    const char *ext = strrchr(path, '.');
    int is_html = ext && (!strcmp(ext, ".html") || !strcmp(ext, ".htm"));

    if (is_html) {
        /* Read into memory so we can inject the live-reload script */
        size_t slen = sizeof(live_script) - 1;
        char *content = malloc(st.st_size + 1);
        if (!content) { close(fd); send404(sock, keepalive); return; }

        ssize_t r = read(fd, content, st.st_size);
        close(fd);
        if (r < 0) { free(content); send404(sock, keepalive); return; }
        content[r] = '\0';

        char *inject = strstr(content, "</body>");
        if (!inject) inject = strstr(content, "</html>");

        size_t total = (size_t)r + slen;
        char header[512];
        int hlen = snprintf(header, sizeof(header),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html\r\n"
            "Content-Length: %zu\r\n"
            "Cache-Control: no-cache\r\n"
            "Last-Modified: %s\r\n"
            "%s\r\n",
            total, timebuf, conn_header(keepalive));

        if (inject) {
            size_t prefix = (size_t)(inject - content);
            struct iovec iov[4];
            iov[0].iov_base = header;             iov[0].iov_len = hlen;
            iov[1].iov_base = content;            iov[1].iov_len = prefix;
            iov[2].iov_base = (char *)live_script; iov[2].iov_len = slen;
            iov[3].iov_base = inject;             iov[3].iov_len = (size_t)r - prefix;
            writev(sock, iov, 4);
        } else {
            struct iovec iov[3];
            iov[0].iov_base = header;             iov[0].iov_len = hlen;
            iov[1].iov_base = content;            iov[1].iov_len = r;
            iov[2].iov_base = (char *)live_script; iov[2].iov_len = slen;
            writev(sock, iov, 3);
        }
        free(content);
        return;
    }

    char header[512];
    snprintf(header, sizeof(header),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %ld\r\n"
        "Cache-Control: max-age=3600\r\n"
        "Last-Modified: %s\r\n"
        "%s\r\n",
        mime(path), st.st_size, timebuf, conn_header(keepalive));

    send(sock, header, strlen(header), 0);

    off_t offset = 0;
    while (offset < st.st_size) {
        ssize_t sent = sendfile(sock, fd, &offset, st.st_size - offset);
        if (sent <= 0) break;
    }

    close(fd);
}

void sendDir(int sock, const char *path, int keepalive) {

    DIR *d = opendir(path);
    if (!d) {
        send404(sock, keepalive);
        return;
    }

    size_t cap = 16384, used = 0;
    char *body = malloc(cap);
    if (!body) { closedir(d); send404(sock, keepalive); return; }

    used += snprintf(body + used, cap - used,
        "<html><body><h1>Index of %s</h1><ul>", path);

    struct dirent *ent;
    while ((ent = readdir(d))) {
        char entry[600];
        int n = snprintf(entry, sizeof(entry),
            "<li><a href=\"%s\">%s</a></li>", ent->d_name, ent->d_name);
        if (used + (size_t)n + 1 > cap) {
            cap *= 2;
            char *tmp = realloc(body, cap);
            if (!tmp) break;
            body = tmp;
        }
        memcpy(body + used, entry, n);
        used += n;
    }
    closedir(d);

    const char *end = "</ul></body></html>";
    size_t endlen = strlen(end);
    if (used + endlen + 1 > cap) {
        cap = used + endlen + 1;
        char *tmp = realloc(body, cap);
        if (tmp) body = tmp;
    }
    memcpy(body + used, end, endlen);
    used += endlen;

    char header[256];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "Content-Length: %zu\r\n"
        "%s\r\n",
        used, conn_header(keepalive));

    struct iovec iov[2];
    iov[0].iov_base = header; iov[0].iov_len = hlen;
    iov[1].iov_base = body;   iov[1].iov_len = used;
    writev(sock, iov, 2);

    free(body);
}

/* ---------- path helpers ---------- */

void buildPath(char *dest, const char *route) {

    if (strstr(route, "..")) {
        dest[0] = '\0';
        return;
    }

    snprintf(dest, 1024, "%s%s", root, route);
}

/* ---------- client handler ---------- */

void *handleClient(void *arg) {

    int client = (int)(intptr_t)arg;

    struct timeval tv = { .tv_sec = KEEPALIVE_TIMEOUT_SEC };
    setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    for (;;) {
        char request[BUFFER_SIZE] = {0};
        int r = recv(client, request, BUFFER_SIZE - 1, 0);
        if (r <= 0) break;

        char method[16], route[512];
        sscanf(request, "%15s %511s", method, route);

        if (strcmp(method, "GET") != 0) break;

        if (strcmp(route, LIVE_RELOAD_PATH) == 0) {
            handleSSE(client);
            break;
        }

        int keepalive = !strstr(request, "Connection: close");

        char path[1024];
        buildPath(path, route);

        if (!strlen(path)) {
            send404(client, keepalive);
            if (!keepalive) break;
            continue;
        }

        struct stat st;

        if (stat(path, &st) < 0) {
            send404(client, keepalive);
        } else if (S_ISDIR(st.st_mode)) {
            char index[1024];
            snprintf(index, sizeof(index), "%s/index.html", path);
            if (stat(index, &st) == 0)
                sendFile(client, index, keepalive);
            else
                sendDir(client, path, keepalive);
        } else {
            sendFile(client, path, keepalive);
        }

        if (!keepalive) break;
    }

    close(client);
    return NULL;
}

/* ---------- worker thread ---------- */

static void *worker_thread(void *unused) {
    (void)unused;
    for (;;) {
        int fd = queue_pop();
        handleClient((void *)(intptr_t)fd);
    }
    return NULL;
}

static void pool_init(void) {
    pthread_mutex_init(&gQueue.lock, NULL);
    pthread_cond_init(&gQueue.not_empty, NULL);
    pthread_cond_init(&gQueue.not_full, NULL);
    for (int i = 0; i < POOL_SIZE; i++)
        pthread_create(&gPool[i], NULL, worker_thread, NULL);
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {

    if (argc >= 2)
        realpath(argv[1], root);

    if (argc >= 3)
        port = atoi(argv[2]);

    int server = socket(AF_INET, SOCK_STREAM, 0);

    int opt = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = INADDR_ANY
    };

    bind(server, (struct sockaddr *)&addr, sizeof(addr));
    listen(server, QUEUE_CAP);

    printf("Serving %s\n", root);
    printf("Listening on http://localhost:%d\n", port);

    pthread_t wt;
    pthread_create(&wt, NULL, watch_thread, NULL);
    pthread_detach(wt);

    pool_init();

    while (1) {
        int fd = accept(server, NULL, NULL);
        if (fd >= 0) queue_push(fd);
    }

    close(server);
}
