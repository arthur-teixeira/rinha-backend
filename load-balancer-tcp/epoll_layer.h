#ifndef EPOLL_LAYER_H
#define EPOLL_LAYER_H

#include <stdint.h>
#include <sys/epoll.h>
#define BUFFSIZE 4096

typedef struct epoll_handler_t {
  int fd;
  void (*handle)(struct epoll_handler_t *, uint32_t);
  struct epoll_handler_t *opposing;
  union {
    uint8_t request_count;
    char buf[BUFFSIZE];
  } data;
} epoll_handler_t;

void epoll_init();
void epoll_add_handler(epoll_handler_t *handler, uint32_t events);
void epoll_remove_handler(epoll_handler_t *handler);
void do_epoll_loop();

#endif // EPOLL_LAYER_H
