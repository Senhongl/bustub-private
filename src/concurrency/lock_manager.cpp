//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockHolded(txn_id_t txn_id, const RID &rid) {
  LockRequestQueue &queue = lock_table_[rid];
  for (LockRequest request : queue.request_queue_) {
    if (request.txn_id_ == txn_id) {
      return true;
    }
  }
  return false;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  latch_.lock();
  // check if lock on read uncommitted
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (LockHolded(txn->GetTransactionId(), rid)) {
    latch_.unlock();
    return true;
  }

  LockRequest request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  txn_map_[txn->GetTransactionId()] = txn;
  /**
   * 1. Do the wound-wait prevention check.
   * 2. If there exists a exclusive request before *this* request, add the info to the wait list and sleep
   * 2. Otherwise, grant the lock to *this* request
   */

  std::mutex tmp;
  std::unique_lock<std::mutex> lck(tmp);
  LockRequestQueue &queue = lock_table_[rid];
  queue.request_queue_.push_back(request);

  while (true) {
    // first of all, do the wound-wait prevention check
    if (txn->GetState() == TransactionState::ABORTED) {
      txn_map_.erase(txn->GetTransactionId());
      queue.request_queue_.remove_if([txn](LockRequest request) { return request.txn_id_ == txn->GetTransactionId(); });
      latch_.unlock();
      // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      return false;
    }

    bool exclusive_request_before = false;
    for (LockRequest request : queue.request_queue_) {
      if (request.txn_id_ == txn->GetTransactionId()) {
        break;
      }

      if (request.lock_mode_ == LockMode::EXCLUSIVE) {
        if (request.txn_id_ > txn->GetTransactionId()) {
          Transaction *aborted_txn = txn_map_[request.txn_id_];
          aborted_txn->SetState(TransactionState::ABORTED);
          if (sleeping_map_.find(request.txn_id_) != sleeping_map_.end()) {
            RID &sleeping_rid = sleeping_map_[request.txn_id_];
            lock_table_[sleeping_rid].cv_.notify_all();
          }
        }
        exclusive_request_before = true;
        // waiting_list_[request.txn_id_].insert(txn->GetTransactionId());
      }
    }

    if (!exclusive_request_before) {
      sleeping_map_.erase(txn->GetTransactionId());
      break;
    }

    sleeping_map_[txn->GetTransactionId()] = rid;
    latch_.unlock();
    queue.cv_.wait(lck);
    latch_.lock();
  }

  request.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  latch_.unlock();
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  latch_.lock();

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (LockHolded(txn->GetTransactionId(), rid)) {
    latch_.unlock();
    return true;
  }

  LockRequest request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  txn_map_[txn->GetTransactionId()] = txn;

  std::mutex tmp;
  std::unique_lock<std::mutex> lck(tmp);
  LockRequestQueue &queue = lock_table_[rid];
  queue.request_queue_.push_back(request);

  /**
   * 1. Do the wound-wait prevention check.
   * 2. If there is any request before *this* request, add the info to the wait list and sleep
   * 2. Otherwise, grant the lock to *this* request
   */

  while (true) {
    LockRequestQueue &queue = lock_table_[rid];
    if (txn->GetState() == TransactionState::ABORTED) {
      txn_map_.erase(txn->GetTransactionId());
      queue.request_queue_.remove_if([txn](LockRequest request) { return request.txn_id_ == txn->GetTransactionId(); });
      latch_.unlock();
      // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      return false;
    }

    bool request_before = false;

    for (LockRequest request : queue.request_queue_) {
      if (request.txn_id_ == txn->GetTransactionId()) {
        break;
      }
      if (request.txn_id_ > txn->GetTransactionId()) {
        Transaction *aborted_txn = txn_map_[request.txn_id_];
        aborted_txn->SetState(TransactionState::ABORTED);
        if (sleeping_map_.find(request.txn_id_) != sleeping_map_.end()) {
          RID &sleeping_rid = sleeping_map_[request.txn_id_];
          lock_table_[sleeping_rid].cv_.notify_all();
        }
      }
      request_before = true;
    }

    if (!request_before) {
      sleeping_map_.erase(txn->GetTransactionId());
      break;
    }

    sleeping_map_[txn->GetTransactionId()] = rid;
    latch_.unlock();
    queue.cv_.wait(lck);
    latch_.lock();
  }

  request.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  latch_.unlock();
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  latch_.lock();

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  LockRequestQueue &queue = lock_table_[rid];
  if (queue.upgrading_ != INVALID_TXN_ID) {
    // another transaction is already waiting to upgrade the lock
    txn->SetState(TransactionState::ABORTED);
    latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // check if the txn is holding the share lock for this rid
  bool holding_flag = false;
  LockRequest new_request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  for (LockRequest request : queue.request_queue_) {
    if (request.txn_id_ == txn->GetTransactionId()) {
      if (request.lock_mode_ == LockMode::SHARED) {
        holding_flag = true;
        break;
      }
      latch_.unlock();
      return true;
    }
  }

  if (!holding_flag) {
    // todo: correct?
    latch_.unlock();
    return false;
  }

  txn_map_[txn->GetTransactionId()] = txn;
  std::mutex tmp;
  std::unique_lock<std::mutex> lck(tmp);
  queue.request_queue_.push_back(new_request);
  queue.upgrading_ = txn->GetTransactionId();

  /**
   * 1. Do the wound-wait prevention check.
   * 2. If there is any other request except for the transaction itself before *this* request,
   *    add the info to the wait list and sleep
   * 2. Otherwise, remove this txn's shared request and grant the exclusive lock
   */

  while (true) {
    LockRequestQueue &queue = lock_table_[rid];

    if (txn->GetState() == TransactionState::ABORTED) {
      queue.upgrading_ = INVALID_TXN_ID;
      queue.request_queue_.remove_if(
          [txn](LockRequest request) { return request.txn_id_ == txn->GetTransactionId() && !request.granted_; });
      latch_.unlock();
      // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      return false;
    }

    bool request_before = false;

    for (LockRequest request : queue.request_queue_) {
      if (request.txn_id_ == txn->GetTransactionId() && request.lock_mode_ == LockMode::EXCLUSIVE) {
        break;
      }
      if (request.txn_id_ == txn->GetTransactionId()) {
        continue;
      }
      if (request.txn_id_ > txn->GetTransactionId()) {
        Transaction *aborted_txn = txn_map_[request.txn_id_];
        aborted_txn->SetState(TransactionState::ABORTED);
        if (sleeping_map_.find(request.txn_id_) != sleeping_map_.end()) {
          RID &sleeping_rid = sleeping_map_[request.txn_id_];
          lock_table_[sleeping_rid].cv_.notify_all();
        }
      }
      request_before = true;
    }

    if (!request_before) {
      sleeping_map_.erase(txn->GetTransactionId());
      break;
    }

    sleeping_map_[txn->GetTransactionId()] = rid;
    latch_.unlock();
    queue.cv_.wait(lck);
    latch_.lock();
  }

  assert(queue.request_queue_.front().txn_id_ == txn->GetTransactionId() &&
         queue.request_queue_.front().lock_mode_ == LockMode::SHARED);
  queue.request_queue_.pop_front();

  assert(queue.request_queue_.front().txn_id_ == txn->GetTransactionId() &&
         queue.request_queue_.front().lock_mode_ == LockMode::EXCLUSIVE);
  queue.upgrading_ = INVALID_TXN_ID;
  new_request.granted_ = true;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  latch_.unlock();
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  latch_.lock();
  LockRequestQueue &queue = lock_table_[rid];

  std::list<LockRequest>::iterator request = queue.request_queue_.begin();
  bool exclusive_lock = false;
  while (request != queue.request_queue_.end()) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      exclusive_lock = (request->lock_mode_ == LockMode::EXCLUSIVE);
      queue.request_queue_.erase(request);
      break;
    }
    request++;
  }

  // didn't found the request
  if (request == queue.request_queue_.end()) {
    latch_.unlock();
    return false;
  }

  txn_map_.erase(txn->GetTransactionId());
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ || exclusive_lock) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (queue.request_queue_.empty()) {
    lock_table_.erase(rid);
  } else {
    queue.cv_.notify_all();
  }

  latch_.unlock();
  return true;
}

}  // namespace bustub
