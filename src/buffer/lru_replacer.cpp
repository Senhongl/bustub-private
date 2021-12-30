//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  max_num_pages_ = num_pages;
  head_ = nullptr;
  tail_ = nullptr;
}

LRUReplacer::~LRUReplacer() {
  Node *cur = head_;
  Node *next;
  while (cur != nullptr) {
    next = cur->next_;
    delete cur;
    cur = next;
  }
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (map_.empty()) {
    return false;
  }

  std::memcpy(frame_id, &(tail_->frame_id_), sizeof(frame_id_t));
  map_.erase(*frame_id);
  auto tmp = tail_;
  tail_ = tail_->prev_;
  delete tmp;
  if (tail_ == nullptr) {
    head_ = nullptr;
    return true;
  }
  tail_->next_ = nullptr;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (map_.find(frame_id) == map_.end()) {
    return;
  }
  Node *node = map_[frame_id];
  map_.erase(frame_id);

  Node *prev = node->prev_;
  Node *next = node->next_;

  if (prev != nullptr) {
    prev->next_ = next;
  }

  if (next != nullptr) {
    next->prev_ = prev;
  }

  if (head_ == node) {
    head_ = next;
  }

  if (tail_ == node) {
    tail_ = prev;
  }
  delete node;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (map_.find(frame_id) != map_.end()) {
    return;
  }
  Node *node = new Node();
  node->frame_id_ = frame_id;
  map_[frame_id] = node;
  if (map_.size() > max_num_pages_) {
    Victim(new frame_id_t);
  }

  if (head_ == nullptr) {
    head_ = node;
    tail_ = node;
  } else {
    node->next_ = head_;
    head_->prev_ = node;
    head_ = node;
  }
}

size_t LRUReplacer::Size() { return map_.size(); }

}  // namespace bustub
