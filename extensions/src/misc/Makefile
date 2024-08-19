# Define the source files by searching the current directory
SRCS := $(wildcard *.c)

# Define the output files by replacing .c with .so
OBJS := $(SRCS:.c=.so)

# Default target that builds all .so files
all: $(OBJS)

# Rule to compile each .c file into a .so shared library
%.so: %.c
	gcc -g -fPIC -I../. -shared $< -o $@

# Clean up the directory by removing all .so files
clean:
	rm -f $(OBJS)

.PHONY: all clean

