#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define BUFFER_SIZE 4096
#define DEFAULT_PORT 9090

char root[1024] = ".";
int port = DEFAULT_PORT;

const char *mime(const char *path) {
    const char *ext = strrchr(path, '.');

    if (!ext) return "text/plain";
    if (!strcmp(ext, ".html")) return "text/html";
    if (!strcmp(ext, ".css")) return "text/css";
    if (!strcmp(ext, ".js")) return "application/javascript";
    if (!strcmp(ext, ".png")) return "image/png";
    if (!strcmp(ext, ".jpg") || !strcmp(ext, ".jpeg")) return "image/jpeg";
    if (!strcmp(ext, ".gif")) return "image/gif";
    if (!strcmp(ext, ".svg")) return "image/svg+xml";
    if (!strcmp(ext, ".json")) return "application/json";

    return "application/octet-stream";
}

void send404(int sock) {
    char *msg =
        "HTTP/1.1 404 Not Found\r\n"
        "Content-Type: text/plain\r\n\r\n404 Not Found";

    send(sock, msg, strlen(msg), 0);
}

void sendFile(int sock, const char *path) {

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        send404(sock);
        return;
    }

    struct stat st;
    fstat(fd, &st);

    char header[256];

    snprintf(header,sizeof(header),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %ld\r\n\r\n",
        mime(path), st.st_size);

    send(sock, header, strlen(header), 0);

    off_t offset = 0;

    while (offset < st.st_size) {
        ssize_t sent = sendfile(sock, fd, &offset, st.st_size - offset);
        if (sent <= 0) break;
    }

    close(fd);
}

void sendDir(int sock, const char *path) {

    DIR *d = opendir(path);
    if (!d) {
        send404(sock);
        return;
    }

    char header[] =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n\r\n";

    send(sock, header, strlen(header), 0);

    char html[BUFFER_SIZE];

    snprintf(html,sizeof(html),
        "<html><body><h1>Index of %s</h1><ul>", path);

    send(sock, html, strlen(html), 0);

    struct dirent *ent;

    while ((ent = readdir(d))) {

        snprintf(html,sizeof(html),
            "<li><a href=\"%s\">%s</a></li>",
            ent->d_name, ent->d_name);

        send(sock, html, strlen(html), 0);
    }

    char end[]="</ul></body></html>";
    send(sock,end,strlen(end),0);

    closedir(d);
}

void buildPath(char *dest, const char *route) {

    if (strstr(route,"..")) {
        dest[0]='\0';
        return;
    }

    snprintf(dest,1024,"%s%s",root,route);
}

void *handleClient(void *arg) {

    int client = *(int*)arg;
    free(arg);

    char request[BUFFER_SIZE] = {0};

    int r = recv(client, request, BUFFER_SIZE - 1, 0);

    if (r <= 0) {
        close(client);
        return NULL;
    }

    char method[16];
    char route[512];

    sscanf(request,"%15s %511s",method,route);

    if (strcmp(method,"GET") != 0) {
        close(client);
        return NULL;
    }

    if (strcmp(route,"/")==0)
        strcpy(route,"/");

    char path[1024];
    buildPath(path,route);

    if (!strlen(path)) {
        send404(client);
        close(client);
        return NULL;
    }

    struct stat st;

    if (stat(path,&st) < 0) {
        send404(client);
    }
    else if (S_ISDIR(st.st_mode)) {

        char index[1024];
        snprintf(index,sizeof(index),"%s/index.html",path);

        if (stat(index,&st)==0)
            sendFile(client,index);
        else
            sendDir(client,path);
    }
    else {
        sendFile(client,path);
    }

    close(client);

    return NULL;
}

int main(int argc,char *argv[]) {

    if (argc >= 2)
        realpath(argv[1], root);

    if (argc >= 3)
        port = atoi(argv[2]);

    int server = socket(AF_INET, SOCK_STREAM, 0);

    int opt = 1;
    setsockopt(server,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = INADDR_ANY
    };

    bind(server,(struct sockaddr*)&addr,sizeof(addr));
    listen(server,20);

    printf("Serving %s\n",root);
    printf("Listening on http://localhost:%d\n",port);

    while (1) {

        int *client = malloc(sizeof(int));

        *client = accept(server,NULL,NULL);

        pthread_t t;

        pthread_create(&t,NULL,handleClient,client);

        pthread_detach(t);
    }

    close(server);
}