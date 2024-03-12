#ifndef _STRUCTURES_H_
#define _STRUCTURES_H_

#include "cstdint"

struct __attribute__((__packed__)) DataStructure {
    uint64_t session_id;
    uint64_t first_byte_num;
    uint8_t audio_data[65536];
};

#endif // _STRUCTURES_H_
