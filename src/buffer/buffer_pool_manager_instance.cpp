//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // flush a page regardless of its pin status
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t flush_frame_id = page_table_[page_id];
    Page *page = &pages_[flush_frame_id];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page_id, page->data_);
    }
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgImp(pages_[i].page_id_);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool ret_flag = true;
  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      ret_flag = false;
      break;
    }
    // LOG_DEBUG("page %d is pinned and its pin count is %d", pages_[i].GetPageId(), pages_[i].GetPinCount());
  }

  if (ret_flag) {
    latch_.unlock();
    return nullptr;
  }

  *page_id = AllocatePage();
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  Page *page = nullptr;
  frame_id_t victim_frame_id = -1;
  if (!free_list_.empty()) {
    victim_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    assert(replacer_->Victim(&victim_frame_id) == true);
  }
  assert(victim_frame_id != -1);
  page = &pages_[victim_frame_id];
  page_table_.erase(page->page_id_);

  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_[*page_id] = victim_frame_id;
  disk_manager_->ReadPage(*page_id, page->data_);
  latch_.unlock();
  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  Page *page;
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    // 1.1    If P exists, pin it and return it immediately.
    frame_id_t fetched_frame_id = page_table_[page_id];
    page = &pages_[fetched_frame_id];
    page->pin_count_ += 1;
    replacer_->Pin(fetched_frame_id);
    latch_.unlock();
    return page;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t victim_frame_id;
  if (!free_list_.empty()) {
    victim_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // if no page is available in the free list and all other pages are currently pinned
    if (!replacer_->Victim(&victim_frame_id)) {
      latch_.unlock();
      return nullptr;
    }
  }
  page = &pages_[victim_frame_id];

  // 2.     If R is dirty, write it back to the disk.
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->page_id_);
  page_table_[page_id] = victim_frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page->ResetMemory();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->data_);
  latch_.unlock();
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  latch_.lock();
  DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    latch_.unlock();
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  free_list_.push_back(frame_id);

  // it's not saying that the frame is pinned
  // but just a tricky way to remove the frame from the replacer, because now the frame is already in the free list
  replacer_->Pin(frame_id);
  latch_.unlock();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t unpin_frame_id = page_table_[page_id];
  Page *page = &pages_[unpin_frame_id];
  if (page->pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  page->pin_count_ -= 1;
  if (!page->is_dirty_) {
    page->is_dirty_ = is_dirty;
  }
  if (page->pin_count_ == 0) {
    replacer_->Unpin(unpin_frame_id);
  }
  latch_.unlock();
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
