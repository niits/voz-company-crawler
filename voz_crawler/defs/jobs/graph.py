from dagster import AssetSelection, define_asset_job

reply_graph_job = define_asset_job(
    name="reply_graph_job",
    selection=AssetSelection.groups("reply_graph"),
    description="Run the full reply graph pipeline: sync, explicit edges, embeddings, implicit edges.",
)
