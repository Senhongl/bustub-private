//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>
#include <list>
#include <map>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

/**
 * Linkedlist node with previous pointer point to the preivous node and a next pointer
 * point to the next node
 */
class Node {
 public:
  frame_id_t frame_id_;
  Node *next_;
  Node *prev_;
};

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  void VictimWithoutLock(frame_id_t *frame_id);
  std::map<frame_id_t, Node *> map_;
  size_t max_num_pages_;
  Node *head_;
  Node *tail_;
  std::mutex latch_;
};

}  // namespace bustub
