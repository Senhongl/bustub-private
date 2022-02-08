//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  LOG_DEBUG("hash join init");
  catalog_ = exec_ctx_->GetCatalog();
  txn_ = exec_ctx_->GetTransaction();
  left_key_expression_ = plan_->LeftJoinKeyExpression();
  right_key_expression_ = plan_->RightJoinKeyExpression();
  left_executor_->Init();
  right_executor_->Init();
  multiple_values_ = false;
  idx_ = 0;
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    HashJoinKey key;
    key.group_bys_.push_back(left_key_expression_->Evaluate(&tuple, left_executor_->GetOutputSchema()));
    if (ht_.count(key) == 0) {
      std::vector<Tuple> values;
      values.push_back(tuple);
      ht_[key] = values;
    } else {
      ht_[key].push_back(tuple);
    }
  }
}

std::vector<Value> HashJoinExecutor::CombinedTuples(const Tuple left_tuple, const Tuple right_tuple) {
  std::vector<Value> combined_value;
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    combined_value.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                         right_executor_->GetOutputSchema()));
  }
  return combined_value;
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;
  if (multiple_values_) {
    std::vector<Value> combined_value = CombinedTuples(left_tuples_[idx_], right_tuple_);
    *tuple = Tuple(combined_value, GetOutputSchema());
    idx_++;
    if (idx_ == left_tuples_.size()) {
      multiple_values_ = false;
      idx_ = 0;
    }
    return true;
  }
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    HashJoinKey key;
    right_tuple_ = Tuple(right_tuple);
    key.group_bys_.push_back(right_key_expression_->Evaluate(&right_tuple, right_executor_->GetOutputSchema()));
    if (ht_.count(key) == 0) {
      continue;
    }
    std::vector<Tuple> values = ht_[key];
    if (values.size() > 1) {
      left_tuples_ = values;
      idx_ = 0;
      multiple_values_ = true;
      std::vector<Value> combined_value = CombinedTuples(left_tuples_[idx_], right_tuple);
      *tuple = Tuple(combined_value, GetOutputSchema());
      idx_++;
    } else {
      std::vector<Value> combined_value = CombinedTuples(values[0], right_tuple);
      *tuple = Tuple(combined_value, GetOutputSchema());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
