//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // First of all, create a new directory page
  auto dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_, nullptr));
  // TODO(senhongl): what is log sequence number?
  dir_page->InitDirectory(directory_page_id_, -1);
  // Now we need to setup two bucket_pages for bucket 0 and 1
  page_id_t bucket_page_idx_0;
  buffer_pool_manager_->NewPage(&bucket_page_idx_0, nullptr);

  // set up their metadata and let bucket 0 and 1 points to them, respectively
  dir_page->SetBucketPageId(0, bucket_page_idx_0);
  dir_page->SetBucketPageId(1, bucket_page_idx_0);

  // set the global depth and local depth as 1
  dir_page->IncrGlobalDepth();
  dir_page->SetLocalDepth(0, 0);
  dir_page->SetLocalDepth(1, 0);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_idx_0, false, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash_val = Hash(key);
  uint32_t global_mask = dir_page->GetGlobalDepthMask();
  return hash_val & global_mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  return reinterpret_cast<HashTableDirectoryPage *>(page);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(page);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::NewBucketPage(page_id_t *page_id) {
  Page *page = buffer_pool_manager_->NewPage(page_id, nullptr);
  return reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(page);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page = FetchBucketPage(page_id);
  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool ret = bucket_page->GetValue(key, comparator_, result);
  // just reading stuff, nothing changed.
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(page_id);
  reinterpret_cast<Page *>(bucket_page)->WLatch();

  if (bucket_page->Insert(key, value, comparator_)) {
    // we sucessfully insert the k/v pair, so the bucket is dirty
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    table_latch_.WUnlock();
    return true;
  }
  if (bucket_page->IsFull()) {
    bool ret = SplitInsert(transaction, key, value);
    // The dir_page is dirty because we split the buckets, so does the old_bucket_page
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    table_latch_.WUnlock();
    return ret;
  }
  // If and only if there is a k/v pair exists in the bucket that's exactly the same as the pair
  // that wants to be inserted, we will reach here. So nothing changed.
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();
  table_latch_.WUnlock();
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t old_page_id = KeyToPageId(key, dir_page);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *old_bucket_page = FetchBucketPage(old_page_id);

  // if the bucket_idx is out_of_range, then return false
  uint32_t old_bucket_idx = KeyToDirectoryIndex(key, dir_page);
  if (old_bucket_idx >= DIRECTORY_ARRAY_SIZE) {
    // since we fetch these two pages again, need to unpin twice
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(old_page_id, false, nullptr);
    return false;
  }

  // TODO(senhongl): we might not be able to split the bucket

  // if the bucket page is full, then we need to split it into two different pages
  // there are three possible scenarios:
  //    a. The local depth of old_bucket_page is smaller than the global depth.
  //       In this case, there must be multiple buckets point to this page. Half of these buckets will
  //       point to the old_bucket_page and half of them will point to the new page.
  //    b. The local depth of old_bucket_page is equal to the global depth.
  //       In this case, there is only one bucket pointing to the old_bucket_page.
  //    c. We can't split the bucket
  uint32_t local_bucket_idx;
  if (dir_page->GetLocalDepth(old_bucket_idx) < dir_page->GetGlobalDepth()) {
    uint32_t local_depth_mask = dir_page->GetLocalHighBit(old_bucket_idx);
    local_bucket_idx = old_bucket_idx & local_depth_mask;
  } else {
    local_bucket_idx = old_bucket_idx;
  }

  // 1. increase the local depth of directory page and possibly the global depth
  dir_page->IncrLocalDepth(old_bucket_idx);
  // 2. split the bucket by:
  //  2.1 create a new page
  page_id_t new_page_id;
  auto new_bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_page_id, nullptr));
  assert(new_bucket_page);
  // reinterpret_cast<Page *>(new_bucket_page)->WLatch();
  //  2.2 get everything out from the bucket
  std::vector<KeyType> keys;
  std::vector<ValueType> values;
  old_bucket_page->EmptyAll(&keys, &values);
  uint32_t new_bucket_idx = 0;
  keys.push_back(key);
  values.push_back(value);
  //  2.3 re-hash everything and then re-assign them
  for (size_t i = 0; i < keys.size(); i++) {
    uint32_t bucket_idx = KeyToDirectoryIndex(keys[i], dir_page);
    uint32_t updated_local_bucket_idx = bucket_idx & dir_page->GetLocalHighBit(bucket_idx);
    if (updated_local_bucket_idx != local_bucket_idx) {
      // update metadata for new page
      new_bucket_idx = bucket_idx;
      dir_page->SetBucketPageId(bucket_idx, new_page_id);
      new_bucket_page->Insert(keys[i], values[i], comparator_);
    } else {
      old_bucket_page->Insert(keys[i], values[i], comparator_);
    }
  }
  dir_page->CheckAndUpdateDirectory(new_bucket_idx);
  // before everything, remember to unpin the new bucket page
  // since we fetch these two pages again, need to unpin twice;
  buffer_pool_manager_->UnpinPage(new_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(old_page_id, true, nullptr);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(page_id);
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  if (!bucket_page->Remove(key, value, comparator_)) {
    // We didn't remove the k/v pair, so nothing changed
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    table_latch_.WUnlock();
    return false;
  }

  if (bucket_page->IsEmpty()) {
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    Merge(transaction, key, value);
    // We successfully remove the k/v pair and merge it with split image, everthing changed
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
  } else {
    // We just remove the k/v pair, so only the bucket is dirty
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
  }
  table_latch_.WUnlock();
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(page_id);
  reinterpret_cast<Page *>(bucket_page)->RLatch();
  if (!bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
    reinterpret_cast<Page *>(bucket_page)->RUnlatch();
    return;
  }

  uint32_t split_image_idx = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t split_image_page_id = dir_page->GetBucketPageId(split_image_idx);

  if (dir_page->GetLocalDepth(split_image_idx) != dir_page->GetLocalDepth(bucket_idx) ||
      dir_page->GetLocalDepth(bucket_idx) == 0) {
    // Buckets can only be merged with their split image if their split image has the same local depth.
    // Buckets can only be merged if their local depth is greater than 0.
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
    reinterpret_cast<Page *>(bucket_page)->RUnlatch();
    return;
  }

  for (size_t i = 0; i < DIRECTORY_ARRAY_SIZE; i++) {
    if (dir_page->GetBucketPageId(i) == page_id) {
      dir_page->SetBucketPageId(i, split_image_page_id);
      dir_page->DecrLocalDepth(i);
    } else if (dir_page->GetBucketPageId(i) == split_image_page_id) {
      dir_page->DecrLocalDepth(i);
    }
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();
  auto new_page_id = KeyToPageId(key, dir_page);
  auto split_image_page = FetchBucketPage(new_page_id);
  reinterpret_cast<Page *>(split_image_page)->RLatch();
  if (split_image_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(new_page_id, false, nullptr);
    reinterpret_cast<Page *>(split_image_page)->RUnlatch();
    Merge(transaction, key, value);
  } else {
    buffer_pool_manager_->UnpinPage(new_page_id, false, nullptr);
    reinterpret_cast<Page *>(split_image_page)->RUnlatch();
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyEmpty() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  for (size_t i = 0; i < DIRECTORY_ARRAY_SIZE; i++) {
    page_id_t page_id = dir_page->GetBucketPageId(i);
    if (page_id != INVALID_PAGE_ID) {
      auto page = FetchBucketPage(page_id);
      if (!page->IsEmpty()) {
        LOG_DEBUG("page %d is not empty", page_id);
      }
      assert(buffer_pool_manager_->UnpinPage(page_id, false, nullptr));
    }
  }
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
