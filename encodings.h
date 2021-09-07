#include <stdint.h>

uint8_t *varint_encode(uint8_t *buf, uint64_t val);
uint8_t *varint_decode(uint8_t *buf, uint64_t *out);
uint8_t *bitpack_encode(uint8_t *buf, const uint64_t *vals, uint32_t nvals, uint8_t num_bits);
const uint8_t *bitpack_decode(const uint8_t *buf, uint64_t *out, uint32_t nvals, uint8_t num_bits);
uint64_t zigzag_encode(int64_t value);
int64_t zigzag_decode(uint64_t value);
