#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define PORT "9999"
#define PORT_NUM 9999
#define BACKLOG 10

#define da_init(da, size)                                                      \
  do {                                                                         \
    da->cap = 16;                                                              \
    da->values = calloc(da->cap, size);                                        \
    da->len = 0;                                                               \
  } while (0)

#define da_append(da, value)                                                   \
  do {                                                                         \
    if (da->len == da->cap) {                                                  \
      da->cap *= 2;                                                            \
      da->values = realloc(da->values, da->cap * sizeof(da->values[0]));       \
    }                                                                          \
    da->values[da->len++] = value;                                             \
  } while (0)

typedef struct da_fds {
  size_t cap;
  size_t len;
  struct pollfd *values;
} da_fds;

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

void add_pfd(da_fds *fds, int newfd) {
  struct pollfd newfd_data = (struct pollfd){
      .events = POLLIN,
      .fd = newfd,
      .revents = 0,
  };
  da_append(fds, newfd_data);
}

void remove_pfd(da_fds *fds, int i) {
  fds->values[i] = fds->values[fds->len - 1];
  fds->len--;
}

void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET) {
    return &(((struct sockaddr_in *)sa)->sin_addr);
  }

  return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}

void broadcast(da_fds *fds, int listener, int sender_fd, int num_bytes,
               char *buf) {
  for (int j = 0; j < fds->len; j++) {
    int dest_fd = fds->values[j].fd;

    // Except the listener and ourselves
    if (dest_fd != listener && dest_fd != sender_fd) {
      if (send(dest_fd, buf, num_bytes, 0) == -1) {
        perror("send");
      }
    }
  }
}

void accept_connection(int listener, da_fds *fds) {
  struct sockaddr_storage remoteaddr;
  socklen_t addrlen = sizeof(remoteaddr);

  int newfd = accept(listener, (struct sockaddr *)&remoteaddr, &addrlen);
  if (newfd < 0) {
    perror("Could not accept incoming connection");
  }

  add_pfd(fds, newfd);

  char remote_ip[INET6_ADDRSTRLEN];
  printf("Load balancer: new connection from [%s] on socket %d\n",
         inet_ntop(remoteaddr.ss_family,
                   get_in_addr((struct sockaddr *)&remoteaddr), remote_ip,
                   INET6_ADDRSTRLEN),
         newfd);
}

void recv_and_redirect(da_fds *fds, int listener) {
  char buf[4096];

  for (;;) {
    int poll_count = poll(fds->values, fds->len, -1);
    if (poll_count < 0) {
      perror("poll");
      exit(EXIT_FAILURE);
    }

    for (int i = 0; i < fds->len; i++) {
      if (fds->values[i].revents & POLLIN) {
        if (fds->values[i].fd == listener) {
          accept_connection(listener, fds);

          continue;
        }

        int sender_fd = fds->values[i].fd;
        int num_bytes = recv(sender_fd, buf, sizeof(buf), 0);

        if (num_bytes <= 0) {
          if (num_bytes == 0) {
            printf("Load balancer: socket %d hung up\n", sender_fd);
          } else {
            perror("Could not receive data from client");
          }

          close(sender_fd);
          remove_pfd(fds, i);

          continue;
        }

        broadcast(fds, listener, sender_fd, num_bytes, buf);
      }
    }
  }
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

    break; // Successfully connected
  }

  if (p == NULL) {
    fprintf(stderr, "Failed to connect\n");
    exit(EXIT_FAILURE);
  }

  freeaddrinfo(p);

  fprintf(stderr, "Successfully connected to %s:%s\n", hostname, port);

  return sockfd;
}

int main() {
  char *error_msg;

  int listener = get_listener_socket();
  if (listener < 0) {
    return 1;
  }

  char *upstream_ports[] = {"3000", "3001"};
  da_fds fds = {0};
  da_fds *pfds = &fds;
  da_init(pfds, sizeof(struct pollfd));
  add_pfd(pfds, listener);

  for (int i = 0; i < 2; i++) {
    add_pfd(pfds, get_upstream_socket("localhost", upstream_ports[i]));
  }

  recv_and_redirect(pfds, listener);

  return 0;
}
