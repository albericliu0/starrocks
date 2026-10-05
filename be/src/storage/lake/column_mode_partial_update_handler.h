// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/rowset_column_update_state.h"

namespace starrocks::lake {

class LakeDeltaColumnGroupLoader : public DeltaColumnGroupLoader {
public:
    LakeDeltaColumnGroupLoader(TabletMetadataPtr tablet_metadata);
    Status load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs) override;
    Status load(int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version,
                DeltaColumnGroupList* pdcgs) override;

private:
    TabletMetadataPtr _tablet_metadata;
};

// Used in column mode partial update
class ColumnModePartialUpdateHandler {
public:
    ColumnModePartialUpdateHandler(int64_t base_version, int64_t txn_id, MemTracker* tracker);
    ~ColumnModePartialUpdateHandler();

    Status execute(const RowsetUpdateStateParams& params, MetaFileBuilder* builder);

    // Apply several consecutive column-mode writes of one tablet in one pass, one handler per write,
    // in commit order. Every source segment is read once per column batch and gets one .cols file
    // with the final value of every column any of the writes touched, instead of one read and one
    // .cols per write. A batch publish only materializes its last version, so only the final state
    // has to exist.
    static Status execute_batch(const std::vector<ColumnModePartialUpdateHandler*>& handlers,
                                const std::vector<const RowsetUpdateStateParams*>& params_list,
                                MetaFileBuilder* builder);

private:
    // What one write updates, resolved against the current tablet schema.
    struct Prepared {
        std::vector<ColumnId> update_column_ids;
        std::vector<ColumnUID> unique_update_column_ids;
        // rssid -> update file id -> <source rowid, update rowid>
        std::map<uint32_t, UptidToRowidPairs> rss_upt_id_to_rowid_pairs;
        size_t partial_update_states_size = 0;
    };
    Status _prepare(const RowsetUpdateStateParams& params, Prepared* prepared);
    Status _load_update_state(const RowsetUpdateStateParams& params);
    void _release_upserts(uint32_t start_idx, uint32_t end_idx);
    Status _load_upserts(const RowsetUpdateStateParams& params, const Schema& pkey_schema,
                         const std::vector<ChunkIteratorPtr>& segment_iters, uint32_t start_idx, uint32_t* end_idx);
    Status _prepare_partial_update_states(const RowsetUpdateStateParams& params, uint32_t start_idx, uint32_t end_idx,
                                          bool need_lock);
    StatusOr<std::unique_ptr<SegmentWriter>> _prepare_delta_column_group_writer(
            const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& tschema);
    Status _update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& partial_schema,
                                       ChunkPtr* source_chunk);

    // The update files' columns for the column batch being processed, read once and kept until the
    // batch is done. One update file usually carries rows of nearly every source segment (keys are
    // hash distributed), and the source segments are processed one at a time, so without this every
    // source segment re-read every update file it touched: O(source segments x update files) reads of
    // the same data. That was most of a column-mode publish's time on wide tables.
    Status _prepare_upt_chunk_cache(const Schema& partial_schema);
    void _release_upt_chunk_cache();
    // The chunk of update file `upt_id`. `*cached` tells whether it is kept in the cache (the caller
    // must not release its memory) or was read just for this call (the caller releases it).
    StatusOr<ChunkPtr> _get_upt_chunk(uint32_t upt_id, const Schema& partial_schema, bool* cached);
    StatusOr<ChunkPtr> _read_from_source_segment(const RowsetUpdateStateParams& params, const Schema& schema,
                                                 uint32_t rssid);

private:
    // params
    int64_t _base_version = 0;
    int64_t _txn_id = 0;
    MemTracker* _tracker = nullptr;
    // Used for release memory to tracker when meet failure.
    int64_t _memory_usage = 0;

    std::vector<BatchPKsPtr> _upserts;

    // maintain the reference from rowids in segment files been updated to rowids in update files.
    std::vector<ColumnPartialUpdateState> _partial_update_states;

    // `_rowset_meta_ptr` contains full life cycle rowset meta in `_rowset_ptr`.
    RowsetMetadataUniquePtr _rowset_meta_ptr;
    std::unique_ptr<Rowset> _rowset_ptr;

    struct UptChunkCache {
        // One iterator per update file, consumed by the first read of that file.
        std::vector<ChunkIteratorPtr> iters;
        // Indexed by update file id; nullptr until read, or when memory did not allow keeping it.
        std::vector<ChunkPtr> chunks;
        int64_t bytes = 0;
    };
    UptChunkCache _upt_cache;
    // Must outlive the iterators in `_upt_cache`.
    OlapReaderStatistics _upt_stats;
};

class CompactionUpdateConflictChecker {
public:
    static bool conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                               const TabletMetadata& metadata, MetaFileBuilder* builder);
};

} // namespace starrocks::lake