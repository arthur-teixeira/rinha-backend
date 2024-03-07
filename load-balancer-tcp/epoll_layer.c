#include "epoll_layer.h"
#include <stdio.h>
#include <stdlib.h>

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
