#include <arpa/inet.h>
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

char *upstream_ports[] = {"3000", "3001"};
struct addrinfo *upstream_addrinfo[NUM_UPSTREAMS] = {0};

typedef struct epoll_handler_t {
  int fd;
  void (*handle)(struct epoll_handler_t *, uint32_t);
  bool is_upstream;
  struct epoll_handler_t *opposing;
  union {
    uint8_t request_count;
    char buf[BUFFSIZE];
  } data;
} epoll_handler_t;

int epollfd;
void epoll_init() {
  epollfd = epoll_create1(0);
  if (epollfd < 0) {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }
}

void epoll_add_handler(epoll_handler_t *handler, uint32_t events) {
  struct epoll_event ev;
  ev.data.ptr = handler;
  ev.events = events;

  if (epoll_ctl(epollfd, EPOLL_CTL_ADD, handler->fd, &ev) == -1) {
    perror("epoll_ctl: listen sock");
    exit(EXIT_FAILURE);
  }
}

void epoll_remove_handler(epoll_handler_t *handler) {
  epoll_ctl(epollfd, EPOLL_CTL_DEL, handler->fd, NULL);
}

void do_epoll_loop() {
  char buf[4096];

  for (;;) {
    struct epoll_event current_event;

    int nfds = epoll_wait(epollfd, &current_event, 1, -1);
    if (nfds == -1) {
      perror("Epoll_wait");
      exit(EXIT_FAILURE);
    }
    epoll_handler_t *handler = current_event.data.ptr;
    handler->handle(handler, current_event.events);
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

void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET) {
    return &(((struct sockaddr_in *)sa)->sin_addr);
  }

  return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}

void get_upstream_addrinfo(const char *hostname, const char *port, int i) {
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
  upstream_addrinfo[i] = p;
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

void connection_on_close_event(epoll_handler_t *handler) {
  epoll_remove_handler(handler);
  close(handler->fd);
  free(handler);
}

void connection_on_in_event(epoll_handler_t *handler) {
  size_t bytes_recvd = 0;
  ssize_t nb = 0;

  while (true) {
    nb = recv(handler->fd, handler->data.buf + bytes_recvd,
              BUFFSIZE - bytes_recvd, 0);
    if (nb <= 0) {
      // Já que o epoll está com a flat EPOLLET(edge triggered), só recebemos um
      // evento quando o fd estiver disponível para leitura Nesse caso, temos
      // que receber dados em um loop até receber EAGAIN ou EWOULDBLOCK
      break;
    }

    bytes_recvd += nb;

    if (send(handler->opposing->fd, handler->data.buf, bytes_recvd, 0) < 0) {
      if (errno == ECONNRESET || errno == EPIPE) {
        connection_on_close_event(handler->opposing);
      }
      // Nesse caso, o buffer do socket está cheio, e devemos esperar a kernel
      // fazer o flush Não estou fazendo isso especificamente pq na rinha os
      // requests nunca vão ser tão grandes a ponto de lotar o buffer
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      perror("Could not send data to opposing socket");
    }
  }

  // Se recebemos do upstream, fechamos a conexão
  // TODO: Pode impactar a performance. Manter a conexão com o upstream aberta
  // e reutilizar em uma pool
  if (handler->is_upstream) {
    connection_on_close_event(handler->opposing);
    connection_on_close_event(handler);
  }
}

void connection_handle_event(epoll_handler_t *handler, uint32_t events) {
  if (events & EPOLLIN) {
    connection_on_in_event(handler);
  }
}

epoll_handler_t *create_connection(int client_fd, bool is_upstream) {
  set_non_blocking(client_fd);

  epoll_handler_t *result = malloc(sizeof(epoll_handler_t));
  result->fd = client_fd;
  memset(result->data.buf, 0, BUFFSIZE);
  result->handle = connection_handle_event;
  result->is_upstream = is_upstream;

  epoll_add_handler(result, EPOLLIN | EPOLLET);

  return result;
}

void handle_client_upstream_connection(int client_fd, int upstream_num) {
  epoll_handler_t *client_connection = create_connection(client_fd, false);
  int upstream_fd = get_upstream_socket(upstream_num);

  epoll_handler_t *upstream_connection = create_connection(upstream_fd, true);

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

    handle_client_upstream_connection(client_fd,
                                      ++self->data.request_count % NUM_UPSTREAMS);
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
  handler->data.request_count = 0;

  epoll_add_handler(handler, EPOLLIN | EPOLLET);
}

int main() {
  signal(SIGPIPE, SIG_IGN);

  for (int i = 0; i < NUM_UPSTREAMS; i++) {
    get_upstream_addrinfo("localhost", upstream_ports[i], i);
  }

  epoll_init();
  create_server_listener(PORT);

  printf("Load balancer started, listening on port %s\n", PORT);

  do_epoll_loop();
  return 0;
}
