#include <arpa/inet.h>
#include <asm-generic/errno.h>
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

#define PORT "9999"
#define BACKLOG 10
#define NUM_UPSTREAMS 2
#define BUFFSIZE 4096

typedef struct epoll_handler_t {
  int fd;
  struct epoll_handler_t *opposing;
  union {
    uint8_t request_count;
    char buf[BUFFSIZE];
  } data;
} epoll_handler_t;

char *upstream_ports[] = {"3000", "3001"};
int epollfd;

void epoll_add_handler(epoll_handler_t *handler, uint32_t events) {
  struct epoll_event ev;
  ev.data.ptr = handler;
  ev.events = events;

  if (epoll_ctl(epollfd, EPOLL_CTL_ADD, handler->fd, &ev) == -1) {
    perror("epoll_ctl: listen sock");
    exit(EXIT_FAILURE);
  }
}

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

  return sockfd;
}

void connection_on_close_event(epoll_handler_t *handler) {
  epoll_ctl(epollfd, EPOLL_CTL_DEL, handler->fd, NULL);
  close(handler->fd);
  free(handler);
}

void connection_on_in_event(epoll_handler_t *handler) {
  size_t bytes_recvd = 0;
  ssize_t nb = 0;

  while (true) {
    nb = recv(handler->fd, handler->data.buf + bytes_recvd, BUFFSIZE - bytes_recvd, 0);
    if (nb <= 0) {
      // Já que o epoll está com a flat EPOLLET(edge triggered), só recebemos um
      // evento quando o fd estiver disponível para leitura Nesse caso, temos
      // que receber dados em um loop até receber EAGAIN ou EWOULDBLOCK
      if (nb == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
      }

      break;
    }

    bytes_recvd += nb;

    if (send(handler->opposing->fd, handler->data.buf, bytes_recvd, 0) < 0) {
      if (errno == ECONNRESET || errno == EPIPE) {
        connection_on_close_event(handler->opposing);
      }
      // Nesse caso, o buffer do socket está cheio, e devemos esperar a kernel fazer o flush
      // Não estou fazendo isso especificamente pq na rinha os requests nunca vão ser tão grandes a ponto de lotar o buffer
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      perror("Could not send data to opposing socket");
    }
  }
}

void connection_handle_event(epoll_handler_t *handler, uint32_t events) {
  if (events & EPOLLIN) {
    connection_on_in_event(handler);
  }

  if ((events & EPOLLERR) | (events & EPOLLHUP) | (events & EPOLLRDHUP)) {
    connection_on_close_event(handler);
    if (handler->opposing) {
      connection_on_close_event(handler->opposing);
    }
  }
}

epoll_handler_t *create_connection(int client_fd) {
  set_non_blocking(client_fd);

  epoll_handler_t *result = malloc(sizeof(epoll_handler_t));
  result->fd = client_fd;
  memset(result->data.buf, 0, BUFFSIZE);

  epoll_add_handler(result, EPOLLIN | EPOLLRDHUP | EPOLLET);

  return result;
}

void handle_client_upstream_connection(int client_fd, char *upstream_port) {
  epoll_handler_t *client_connection = create_connection(client_fd);
  int upstream_fd = get_upstream_socket("localhost", upstream_port);

  epoll_handler_t *upstream_connection = create_connection(upstream_fd);

  client_connection->opposing = upstream_connection;
  upstream_connection->opposing = client_connection;
}

void accept_connection(epoll_handler_t *self, uint32_t events) {
  struct sockaddr_storage remoteaddr;
  socklen_t addrlen = sizeof(remoteaddr);

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

    handle_client_upstream_connection(
        client_fd, upstream_ports[++self->data.request_count % NUM_UPSTREAMS]);
  }
}

int create_server_listener(char *server_port) {
  char *error_msg;
  int listener = get_listener_socket();
  if (listener < 0) {
    exit(EXIT_FAILURE);
  }

  epoll_handler_t *handler = malloc(sizeof(epoll_handler_t));
  handler->fd = listener;
  handler->data.request_count = 0;

  epoll_add_handler(handler, EPOLLIN | EPOLLET);

  return listener;
}

int main() {
  signal(SIGPIPE, SIG_IGN);

  epollfd = epoll_create1(0);
  if (epollfd < 0) {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }

  int listener_fd = create_server_listener(PORT);

  printf("Load balancer started, listening on port %s\n", PORT);
  for (;;) {
    struct epoll_event current_event;

    int nfds = epoll_wait(epollfd, &current_event, 1, -1);
    if (nfds == -1) {
      perror("Epoll_wait");
      exit(EXIT_FAILURE);
    }
    epoll_handler_t *handler = current_event.data.ptr;
    if (handler->fd == listener_fd) {
      accept_connection(handler, current_event.events);
    } else {
      connection_handle_event(handler, current_event.events);
    }
  }

  return 0;
}
