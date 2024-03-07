#include <arpa/inet.h>
#include <asm-generic/errno.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "epoll_layer.h"

#define PORT "9999"
#define BACKLOG 10
#define NUM_UPSTREAMS 2
#define BUFFSIZE 4096

char *upstream_ports[] = {"3000", "3001"};

typedef struct client_data_t {
  char buf[BUFFSIZE];
  epoll_handler_t *opposing;
} client_data_t;

void set_non_blocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    perror("fcntl get");
    exit(EXIT_FAILURE);
  }

  flags |= O_NONBLOCK;

  int result = fcntl(fd, F_SETFL, flags);
  if (result < 0) {
    perror("fcntl set");
    exit(EXIT_FAILURE);
  }
}

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

  set_non_blocking(sockfd);

  if (listen(sockfd, BACKLOG) < 0) {
    perror("Could not listen on port");
    return -1;
  }

  return sockfd;
}

void remove_pfd(int epollfd, int fd, struct epoll_event *ev) {
  if (epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, ev) == -1) {
    perror("epoll del");
  }
}

void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET) {
    return &(((struct sockaddr_in *)sa)->sin_addr);
  }

  return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}

int get_upstream_socket(const char *hostname, const char *port) {
  struct addrinfo hints = {0};
  struct addrinfo *server_info, *p;

  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  int rv, sockfd;

  if ((rv = getaddrinfo(hostname, port, &hints, &server_info)) != 0) {
    fprintf(stderr, "Could not resolve address %s:%s :%s\n", hostname, port,
            gai_strerror(rv));
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

  freeaddrinfo(p);

  fprintf(stderr, "Successfully connected to %s:%s\n", hostname, port);

  return sockfd;
}

void connection_on_close_event(epoll_handler_t *handler) {
  epoll_remove_handler(handler);

  close(handler->fd);
  if (handler->data) {
    free(handler->data);
    handler->data = NULL;
    free(handler);
  }
}

void write_to_fd(int sockfd, char *buffer, size_t buflen) {}

void connection_on_in_event(epoll_handler_t *handler) {
  client_data_t *data = (client_data_t *)handler->data;
  size_t bytes_recvd = 0;
  ssize_t nb = 0;

  while (true) {
    nb = recv(handler->fd, data->buf + bytes_recvd, BUFFSIZE - bytes_recvd, 0);
    if (nb <= 0) {
      if (nb == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
      }

      break;
    }

    bytes_recvd += nb;

    if (send(data->opposing->fd, data->buf, bytes_recvd, 0) < 0) {
      if (errno == ECONNRESET || errno == EPIPE) {
        connection_on_close_event(data->opposing);
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      perror("Could not send data to opposing socket");
    }
  }
}

void connection_on_out_event(epoll_handler_t *handler) {}

void connection_handle_event(epoll_handler_t *handler, uint32_t events) {
  if (events & EPOLLIN) {
    connection_on_in_event(handler);
  }

  if (events & EPOLLOUT) {
    connection_on_out_event(handler);
  }

  if ((events & EPOLLERR) | (events & EPOLLHUP) | (events & EPOLLRDHUP)) {
    connection_on_close_event(handler);
  }
}

epoll_handler_t *create_connection(int client_fd) {
  set_non_blocking(client_fd);

  client_data_t *data = malloc(sizeof(client_data_t));
  assert(data);
  memset(data, 0, sizeof(*data));

  epoll_handler_t *result = malloc(sizeof(epoll_handler_t));
  result->fd = client_fd;
  result->data = data;
  result->handle = connection_handle_event;

  epoll_add_handler(result, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);

  return result;
}

void handle_client_upstream_connection(int client_fd, char *upstream_port) {
  epoll_handler_t *client_connection = create_connection(client_fd);
  int upstream_fd = get_upstream_socket("localhost", upstream_port);

  epoll_handler_t *upstream_connection = create_connection(upstream_fd);

  ((client_data_t *)client_connection->data)->opposing = upstream_connection;
  ((client_data_t *)upstream_connection->data)->opposing = client_connection;
}

void accept_connection(epoll_handler_t *self, uint32_t events) {
  struct sockaddr_storage remoteaddr;
  socklen_t addrlen = sizeof(remoteaddr);
  int *connection_count = (int *)self->data;

  int listener = self->fd;
  int client_fd;

  while (true) {
    int client_fd = accept(listener, (struct sockaddr *)&remoteaddr, &addrlen);
    if (client_fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        break;
      }
      perror("Could not accept incoming connection");
    }

    printf("connection count: %d\n", *connection_count);
    handle_client_upstream_connection(
        client_fd, upstream_ports[++*connection_count % NUM_UPSTREAMS]);
  }
}

void create_server_listener(char *server_port) {
  char *error_msg;
  int listener = get_listener_socket();
  if (listener < 0) {
    exit(EXIT_FAILURE);
  }

  epoll_handler_t *handler = malloc(sizeof(epoll_handler_t));
  handler->fd = listener;
  handler->handle = accept_connection;
  handler->data = malloc(sizeof(int));
  *(int *)handler->data = 0;

  epoll_add_handler(handler, EPOLLIN | EPOLLET);
}

int main() {
  signal(SIGPIPE, SIG_IGN);

  epoll_init();
  create_server_listener(PORT);

  printf("Load balancer started, listening on port %s\n", PORT);

  do_epoll_loop();
  return 0;
}
