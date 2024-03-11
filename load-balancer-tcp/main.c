#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <unistd.h>

#define PORT "9999"
#define BACKLOG 10
#define NUM_UPSTREAMS 2
#define BUFFSIZE 4096

char *upstream_ports[NUM_UPSTREAMS] = {"3000", "3001"};
struct addrinfo *upstream_addrinfo[NUM_UPSTREAMS] = {0};

typedef enum {
  HANDLER_LISTENER,
  HANDLER_CLIENT,
  HANDLER_UPSTREAM
} handler_type_t;

typedef struct epoll_handler_t {
  int fd;
  handler_type_t type;
  struct epoll_handler_t *opposing;
} epoll_handler_t;

int epollfd;

void epoll_add_handler(int fd, uint32_t events) {
  struct epoll_event ev;
  ev.data.fd = fd;
  ev.events = events;

  if (epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
    perror("epoll_ctl: listen sock");
    exit(EXIT_FAILURE);
  }
}

void epoll_remove_handler(epoll_handler_t *handler) {}

int get_listener_socket(void) {
  struct addrinfo hints, *res;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  if (getaddrinfo(NULL, PORT, &hints, &res) < 0) {
    perror("Could not get address information");
    return -1;
  }

  int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (sockfd == 0) {
    perror("Could not open socket");
    return -1;
  }

  int optval = 1;
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

  if (bind(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
    perror("Could not bind to address");
    return -1;
  }

  if (listen(sockfd, BACKLOG) < 0) {
    perror("Could not listen on port");
    return -1;
  }

  return sockfd;
}

struct addrinfo *get_upstream_addrinfo(const char *hostname, const char *port) {
  struct addrinfo *server_info, *p;
  struct addrinfo hints = {0};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  int rv, sockfd;
  if ((rv = getaddrinfo(hostname, port, &hints, &server_info)) != 0) {
    fprintf(stderr, "Could not resolve address %s:%s :%s\n", hostname, port,
            rv != EAI_SYSTEM ? gai_strerror(rv) : strerror(errno));

    exit(EXIT_FAILURE);
  }

  for (p = server_info; p != NULL; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      perror("socket");
      continue;
    }

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      perror("connect");
      close(sockfd);
      continue;
    }

    break;
  }

  if (p == NULL) {
    fprintf(stderr, "Failed to connect to upstream\n");
    exit(EXIT_FAILURE);
  }

  close(sockfd);

  return p;
}

int get_upstream_socket(int i) {
  int sockfd;

  struct addrinfo *p = upstream_addrinfo[i];

  if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
    perror("socket");
    exit(EXIT_FAILURE);
  }

  if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
    perror("connect");
    close(sockfd);
    exit(EXIT_FAILURE);
  }

  return sockfd;
}

void connection_on_close_event(int fd) {
  epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, NULL);
  close(fd);
}

void handle_single_request(int fd, int upstream_fd) {
  char buf[BUFFSIZE];
  int bytes_read = read(fd, buf, BUFFSIZE);
  int bytes_sent = write(upstream_fd, buf, bytes_read);

  bytes_read = read(upstream_fd, buf, BUFFSIZE);
  bytes_sent = write(fd, buf, bytes_read);
}

void accept_connection(int listener) {
  struct sockaddr_storage remoteaddr;
  socklen_t addrlen = sizeof(remoteaddr);

  int client_fd = accept(listener, (struct sockaddr *)&remoteaddr, &addrlen);
  if (client_fd < 0) {
    perror("Could not accept incoming connection");
  }

  epoll_add_handler(client_fd, EPOLLIN);
}

int create_server_listener(char *server_port) {
  char *error_msg;
  int listener = get_listener_socket();
  if (listener < 0) {
    exit(EXIT_FAILURE);
  }

  struct epoll_event ev;
  ev.data.fd = listener;
  ev.events = EPOLLIN;
  if (epoll_ctl(epollfd, EPOLL_CTL_ADD, listener, &ev) == -1) {
    perror("epoll_ctl: listen sock");
    exit(EXIT_FAILURE);
  }

  return listener;
}

int main() {
  for (int i = 0; i < NUM_UPSTREAMS; i++) {
    upstream_addrinfo[i] =
        get_upstream_addrinfo("localhost", upstream_ports[i]);
  }

  int backend_fds[NUM_UPSTREAMS];
  backend_fds[0] = get_upstream_socket(0);
  backend_fds[1] = get_upstream_socket(1);

  epollfd = epoll_create1(0);
  if (epollfd < 0) {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }

  int listenerfd = create_server_listener(PORT);

  printf("Load balancer started, listening on port %s\n", PORT);

  int nrequests = 0;

  for (;;) {
    struct epoll_event current_event;
    epoll_wait(epollfd, &current_event, 1, -1);

    int sockfd = current_event.data.fd;
    if (sockfd == listenerfd) {
      struct sockaddr_storage remoteaddr;
      socklen_t addrlen = sizeof(remoteaddr);

      int client_fd = accept(listenerfd, (struct sockaddr *)&remoteaddr, &addrlen);
      if (client_fd < 0) {
        perror("Could not accept incoming connection");
      }

      epoll_add_handler(client_fd, EPOLLIN);
      continue;
    }

    if (current_event.events & EPOLLIN) {
      int backend_fd = backend_fds[++nrequests % NUM_UPSTREAMS];
      handle_single_request(current_event.data.fd, backend_fd);

      epoll_ctl(epollfd, EPOLL_CTL_DEL, current_event.data.fd, NULL);
      close(current_event.data.fd);
    }
  }
  return 0;
}
