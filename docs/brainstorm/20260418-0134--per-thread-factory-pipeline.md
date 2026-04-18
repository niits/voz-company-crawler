# Per-Thread Factory Pipeline Architecture

## Vấn đề

Pipeline hiện tại crawl nhiều thread URL nhưng tất cả thread chia sẻ một `DynamicPartitionsDefinition("voz_pages")` và một `voz_page_posts_assets` ingestion asset. Điều này có nghĩa:

- Failure của một thread ảnh hưởng đến monitoring/visibility của thread khác
- Dagster's `AutomationCondition.eager()` resolve trên toàn bộ shared partition set — không có isolation theo thread
- Không có cách group job/sensor theo thread URL trong Dagit UI
- Mở rộng ra nhiều thread URLs cần cơ chế routing phức tạp trong asset bodies

## Yêu cầu thiết kế

1. Mỗi thread URL có asset chain riêng từ crawl → detect_implicit_replies
2. Failure/backfill một thread không ảnh hưởng thread khác
3. Concurrency vẫn coordinate cross-thread (shared `dagster/concurrency_key`)
4. Có thể thêm `merge_entities` step sau này — step này tổng hợp data từ ALL threads để dedup công ty, build company profiles, find related jobs

## Các kiến trúc được cân nhắc

### Option A: Shared partition set + per-thread dlt_assets

Giữ `DynamicPartitionsDefinition("voz_pages")` chung, nhưng mỗi thread có dlt_assets riêng (`key_prefix=[thread_id]`).

**Vấn đề**: Nếu `crawl_page_job` select cả hai thread assets, chạy với `partition_key="677450:42"` sẽ trigger cả `voz_page_posts_678000` — asset này sẽ call sai URL vì `url_for_partition("677450:42")` trả về URL của thread 677450. Cần per-thread crawl jobs để tránh cross-thread partition conflict. Với `run_status_sensor`, `RunRequest.job_name` override được `request_job` nhưng phức tạp.

**Lý do loại bỏ**: Shared partition set và per-thread assets là semantic contradiction — Dagster không có "prefix filter" cho partition keys. Mỗi asset sẽ nhận tất cả partition keys kể cả keys không thuộc thread của nó.

### Option B: Per-thread partition set + per-thread assets (CHỌN)

Mỗi thread có `DynamicPartitionsDefinition(f"voz_pages_{thread_id}")` riêng. Partition key format giữ nguyên `{thread_id}:{page}` để:
- Zero ArangoDB data migration (documents đang lưu `partition_key = "677450:42"`)
- URL routing logic trong asset bodies không thay đổi

`merge_entities` step (future) dùng partition definition KHÁC — ví dụ unpartitioned hoặc time-partitioned — không phụ thuộc vào partition set của crawl pipeline.

**Ưu điểm**:
- Full isolation: mỗi thread có partition set, assets, jobs, sensors riêng
- Cross-thread merge không bị ảnh hưởng vì nó dùng partition scheme khác
- Concurrency vẫn shared qua `dagster/concurrency_key` tags
- Dagit UI: mỗi thread là một group riêng biệt, dễ debug

## Quyết định: Option B

### Partition key format giữ nguyên

`{thread_id}:{page}` thay vì chuyển sang chỉ `{page}`:
- Không phá vỡ ArangoDB data (`partition_key = "677450:42"`)
- `url_for_partition`, `_page_url` logic không cần thay đổi
- Backward compatible cho tests (`TEST_PARTITION_KEY = "test:fixtures"`)

### Ingestion asset: per-thread dlt_assets

```python
def build_ingestion_assets(thread_id, partitions_def):
    @dlt_assets(
        name=f"voz_page_posts_{thread_id}",
        key_prefix=[thread_id],               # AssetKey([thread_id, "voz__posts"])
        group_name=f"thread_{thread_id}",     # cùng group với graph assets
        partitions_def=partitions_def,        # per-thread partition set
    )
    def ingestion_assets(...):
        # body không đổi
        ...
    return ingestion_assets
```

`pipeline_name=f"voz_crawler_{thread_id}"` để tránh dlt internal state conflict giữa các threads.

### Concurrency management

Dagster's `dagster/concurrency_key` là global — shared concurrency key `voz_llm` throttle LLM calls từ mọi thread. Với per-thread definitions, mỗi thread có job/sensor riêng nhưng chúng vẫn compete trên cùng một concurrency pool. Đây là hành vi mong muốn: tổng LLM concurrency = global budget, không phải per-thread budget.

### Merge entities (future)

```python
@asset(
    deps=[
        AssetDep(AssetKey([thread1_id, "extract_company_mentions"]),
                 partition_mapping=AllPartitionMapping()),
        AssetDep(AssetKey([thread2_id, "extract_company_mentions"]),
                 partition_mapping=AllPartitionMapping()),
    ],
    # unpartitioned hoặc time-partitioned (e.g. daily)
)
def merge_entities(arango: ArangoDBResource):
    """Dedup companies across all threads, build profiles, find related jobs."""
    ...
```

`AllPartitionMapping()` cho phép unpartitioned asset depend on partitioned assets — Dagster sẽ eager-trigger khi bất kỳ partition nào của upstream materialize.

## Kết quả sau refactor

Mỗi thread URL `{url}` với `thread_id` tương ứng tạo ra:

**Assets** (trong group `thread_{thread_id}`):
- `{thread_id}/voz__posts` — dlt ingestion
- `{thread_id}/sync_posts_to_arango`
- `{thread_id}/extract_explicit_edges`
- `{thread_id}/normalize_posts`
- `{thread_id}/compute_embeddings`
- `{thread_id}/classify_posts`
- `{thread_id}/extract_company_mentions`
- `{thread_id}/detect_implicit_replies`

**Jobs**:
- `discover_pages_job_{thread_id}`
- `crawl_page_job_{thread_id}`
- `reply_graph_job_{thread_id}`
- `implicit_reply_job_{thread_id}`

**Sensors**:
- `voz_discover_sensor_{thread_id}` (6-hour interval)
- `voz_crawl_sensor_{thread_id}` (run_status_sensor after discover_job)
- `implicit_reply_sensor_{thread_id}` (60-second poll)

`definitions.py` gọi `_build_thread_pipeline(thread_id)` trong loop qua `VOZ_THREAD_URLS`.
