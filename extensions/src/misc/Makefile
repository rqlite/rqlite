# Define the source files by searching the current directory
SRCS := $(wildcard *.c)

# Define the output files by replacing .c with .so
OBJS := $(SRCS:.c=.so)

# Subset of source files that need to be linked against zlib
ZLIB_SRCS := compress.c
ZLIB_OBJS := $(ZLIB_SRCS:.c=.so)

all: $(OBJS)

# Rule to compile and link .c files that require zlib
$(ZLIB_OBJS): %.so: %.c
	gcc -g -fPIC -I../. -shared $< -o $@ -lz

# Rule to compile and link all other .c files
$(filter-out $(ZLIB_OBJS),$(OBJS)): %.so: %.c
	gcc -g -fPIC -I../. -shared $< -o $@

clean:
	rm -f $(OBJS)

.PHONY: all clean
