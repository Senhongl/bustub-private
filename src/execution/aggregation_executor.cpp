//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  LOG_DEBUG("aggregate init");
  Tuple tuple;
  RID rid;
  const std::vector<const AbstractExpression *> group_by_exprs = plan_->GetGroupBys();
  const std::vector<const AbstractExpression *> aggregate_exprs = plan_->GetAggregates();
  child_->Init();
  having_expr_ = plan_->GetHaving();
  while (child_->Next(&tuple, &rid)) {
    AggregateKey keys;
    AggregateValue vals;
    for (auto group_by_expr : group_by_exprs) {
      keys.group_bys_.push_back(group_by_expr->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    for (auto aggregate_expr : aggregate_exprs) {
      vals.aggregates_.push_back(aggregate_expr->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    aht_.InsertCombine(keys, vals);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    const std::vector<Value> group_bys = aht_iterator_.Key().group_bys_;
    const std::vector<Value> aggregates = aht_iterator_.Val().aggregates_;
    ++aht_iterator_;
    if (having_expr_ == nullptr || having_expr_->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()) {
      auto cols = GetOutputSchema()->GetColumns();
      std::vector<Value> vals;
      vals.reserve(cols.size());
      for (auto &col : cols) {
        vals.push_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
      }
      *tuple = Tuple(vals, GetOutputSchema());
      return true;
    }
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
