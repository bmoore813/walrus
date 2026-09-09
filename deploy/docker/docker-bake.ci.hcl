// CI builds both deployable images as one Bake group. BuildKit schedules the independent targets
// concurrently while preserving the per-image GHA cache scopes used by the previous serial steps.
group "default" {
  targets = ["extractor", "transformer"]
}

target "extractor" {
  context    = "."
  dockerfile = "deploy/docker/Dockerfile.extractor"
  tags       = ["walrus-extractor:ci"]
  cache-from = ["type=gha,scope=extractor"]
  cache-to   = ["type=gha,scope=extractor,mode=max"]
}

target "transformer" {
  context    = "."
  dockerfile = "deploy/docker/Dockerfile.transformer"
  tags       = ["walrus-transformer:ci"]
  cache-from = ["type=gha,scope=transformer"]
  cache-to   = ["type=gha,scope=transformer,mode=max"]
}
