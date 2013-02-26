#ifndef DEBUG_H
#define DEBUG_H

#define DEBUG 1

#if DEBUG

// Will print the give given printf-style args preceded by the file name and
// line number and followed by a newline.
#define TRACE(...) do { \
        printf("%s(%d): ", __FILE__, __LINE__); \
        printf(__VA_ARGS__); \
        printf("\n"); \
    } while (0)
#else
#define TRACE(...)
#endif

#endif /* end of include guard: DEBUG_H */

