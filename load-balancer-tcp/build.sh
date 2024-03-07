#!/bin/bash

set -xe;

clang -O3 -ggdb -o load-balancer main.c epoll_layer.c
