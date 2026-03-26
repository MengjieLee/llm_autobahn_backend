#ifndef HASH_UTILS_H
#define HASH_UTILS_H

#include <cstdint>
#include <cstddef>

namespace hash_utils {

constexpr uint64_t FNV_PRIME = 0x100000001b3;
constexpr uint64_t FNV_OFFSET_BASIS = 0xcbf29ce484222325;

// FNV-1a 变体（保持原有计算顺序以兼容已有数据）
inline uint64_t fnv1a_64(const uint8_t* data, size_t length) {
    uint64_t hash = FNV_OFFSET_BASIS;
    for (size_t i = 0; i < length; ++i) {
        hash *= FNV_PRIME;
        hash ^= static_cast<uint64_t>(data[i]);
    }
    return hash;
}

// 流式哈希：在已有 hash 状态上继续 hash 新数据
inline uint64_t fnv1a_64_continue(uint64_t hash, const uint8_t* data, size_t length) {
    for (size_t i = 0; i < length; ++i) {
        hash *= FNV_PRIME;
        hash ^= static_cast<uint64_t>(data[i]);
    }
    return hash;
}

// 计算单个 block 的哈希
inline uint64_t hash_block_tokens(bool is_first_block, uint64_t prev_block_hash,
                                   const int64_t* tokens, size_t token_count,
                                   bool prefix_hash = true) {
    if (is_first_block || !prefix_hash) {
        return fnv1a_64(reinterpret_cast<const uint8_t*>(tokens),
                        token_count * sizeof(int64_t));
    }
    // 前缀哈希：先将 prev_hash 作为前缀数据流式 hash，再 hash 当前 block 的 token
    // 等价于原来拼接 [prev_hash_as_int64, tokens...] 再整体 hash，但无需分配临时数组
    int64_t prev_hash_int64 = static_cast<int64_t>(prev_block_hash);
    uint64_t hash = fnv1a_64(reinterpret_cast<const uint8_t*>(&prev_hash_int64),
                              sizeof(int64_t));
    return fnv1a_64_continue(hash,
                              reinterpret_cast<const uint8_t*>(tokens),
                              token_count * sizeof(int64_t));
}

// 计算 token 序列的所有 block 哈希 ID
inline void get_block_hash_ids(const int64_t* token_list, size_t token_count,
                                size_t block_size, uint64_t* output,
                                bool prefix_hash = true) {
    uint64_t prev_hash = 0;
    bool first = true;
    size_t idx = 0;
    for (size_t i = 0; i < token_count; i += block_size) {
        size_t count = (i + block_size <= token_count) ? block_size : (token_count - i);
        prev_hash = hash_block_tokens(first, prev_hash, token_list + i, count, prefix_hash);
        output[idx++] = prev_hash;
        first = false;
    }
}

} // namespace hash_utils

#endif
