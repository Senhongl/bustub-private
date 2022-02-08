//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  exec_ctx_ = exec_ctx;
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  LOG_DEBUG("seq_scan init for %u", plan_->GetTableOid());
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->GetTableOid());
  txn_ = exec_ctx_->GetTransaction();
  iterator_ = TableIterator(table_info_->table_->Begin(txn_));
  predicate_ = plan_->GetPredicate();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iterator_ != table_info_->table_->End()) {
    Tuple tmp_tuple = *iterator_;
    auto cols = GetOutputSchema()->GetColumns();
    std::vector<Value> extracted_vals;
    extracted_vals.reserve(cols.size());
    try {
      for (auto &col : cols) {
        // column might not exists in the table
        uint32_t col_idx = table_info_->schema_.GetColIdx(col.GetName());
        extracted_vals.push_back(tmp_tuple.GetValue(&table_info_->schema_, col_idx));
      }
    } catch (std::logic_error &error) {
      for (uint32_t col_idx = 0; col_idx < plan_->OutputSchema()->GetColumnCount(); col_idx++) {
        extracted_vals.push_back(tmp_tuple.GetValue(&table_info_->schema_, col_idx));
      }
    }

    *tuple = Tuple(extracted_vals, GetOutputSchema());
    *rid = tmp_tuple.GetRid();
    iterator_++;
    if (predicate_ == nullptr || predicate_->Evaluate(&tmp_tuple, &table_info_->schema_).GetAs<bool>()) {
      return true;
    }
  }

  LOG_DEBUG("scan finish");
  return false;
}

}  // namespace bustub
