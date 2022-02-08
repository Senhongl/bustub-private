//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  LOG_DEBUG("delete init");
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  txn_ = exec_ctx_->GetTransaction();
  LOG_DEBUG("child init");
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableHeap *table_heap = table_info_->table_.get();
  std::vector<IndexInfo *> indexes = catalog_->GetTableIndexes(table_info_->name_);
  // RID delete_rid;
  // Tuple delete_tuple;

  while (child_executor_->Next(tuple, rid)) {
    if (!table_heap->MarkDelete(*rid, txn_)) {
      continue;
    }
    for (auto index_info : indexes) {
      const auto column_idx = index_info->index_->GetKeyAttrs();
      const auto index_key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, column_idx);
      index_info->index_->DeleteEntry(index_key, *rid, txn_);
    }
    return true;
  }

  LOG_DEBUG("delete finish");
  return false;
}

}  // namespace bustub
