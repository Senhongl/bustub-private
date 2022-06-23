//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  txn_ = exec_ctx_->GetTransaction();
  lock_mgr_ = exec_ctx_->GetLockManager();
  if (plan_->IsRawInsert()) {
    is_raw_insert_ = true;
    raw_values_ = plan_->RawValues();
    iterator_ = raw_values_.begin();
  } else {
    is_raw_insert_ = false;
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableHeap *table_heap = table_info_->table_.get();
  RID inserted_rid;
  std::vector<IndexInfo *> indexes = catalog_->GetTableIndexes(table_info_->name_);
  if (is_raw_insert_) {
    if (iterator_ == raw_values_.end()) {
      return false;
    }
    std::vector<Value> raw_value = *(iterator_);
    *tuple = Tuple(raw_value, &table_info_->schema_);
    if (!table_heap->InsertTuple(*tuple, &inserted_rid, txn_)) {
      return false;
    }
    // lock_mgr_->LockExclusive(txn_, inserted_rid);
    iterator_++;
  } else {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
    if (!table_heap->InsertTuple(*tuple, &inserted_rid, txn_)) {
      return false;
    }
    // lock_mgr_->LockExclusive(txn_, inserted_rid);
  }

  for (auto index_info : indexes) {
    const auto column_idx = index_info->index_->GetKeyAttrs();
    const auto index_key = tuple->KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), column_idx);
    index_info->index_->InsertEntry(index_key, inserted_rid, txn_);
    // const IndexWriteRecord record(inserted_rid, plan_->TableOid(), WType::INSERT, *tuple, index_info->index_oid_,
    //                               catalog_);
    // txn_->AppendTableWriteRecord(record);
  }
  return true;
}

}  // namespace bustub
