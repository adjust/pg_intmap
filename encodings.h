#include <stdint.h>

uint8_t *varint_encode(uint8_t *buf, uint64_t val);
uint8_t *varint_decode(uint8_t *buf, uint64_t *out);
