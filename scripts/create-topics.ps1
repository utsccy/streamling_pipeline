$BootstrapServers = if ($env:BOOTSTRAP_SERVERS) { $env:BOOTSTRAP_SERVERS } else { "host.docker.internal:9092" }
$ReplicationFactor = if ($env:REPLICATION_FACTOR) { $env:REPLICATION_FACTOR } else { "1" }
$KafkaImage = if ($env:KAFKA_IMAGE) { $env:KAFKA_IMAGE } else { "confluentinc/cp-kafka:7.7.0" }
$DockerNetwork = $env:DOCKER_NETWORK

function Test-DockerAvailable {
  $null = Get-Command docker -ErrorAction SilentlyContinue
  if (-not $?) {
    throw "Docker CLI not found. Install Docker Desktop or add docker to PATH before running this script."
  }
}

function Invoke-KafkaTopics {
  param(
    [string] $Topic,
    [int] $Partitions,
    [string] $CleanupPolicy,
    [string[]] $ExtraArgs = @()
  )

  $dockerArgs = @("run", "--rm")
  if ($DockerNetwork) {
    $dockerArgs += @("--network", $DockerNetwork)
  }

  $dockerArgs += @(
    $KafkaImage,
    "kafka-topics",
    "--bootstrap-server", $BootstrapServers,
    "--create", "--if-not-exists",
    "--topic", $Topic,
    "--partitions", $Partitions,
    "--replication-factor", $ReplicationFactor,
    "--config", "cleanup.policy=$CleanupPolicy",
    "--config", "retention.ms=604800000"
  )

  if ($ExtraArgs.Count -gt 0) {
    $dockerArgs += $ExtraArgs
  }

  & docker @dockerArgs
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to create or validate topic '$Topic'. Check BOOTSTRAP_SERVERS and DOCKER_NETWORK."
  }
}

Test-DockerAvailable

Invoke-KafkaTopics -Topic "input-events" -Partitions 3 -CleanupPolicy "delete"
Invoke-KafkaTopics -Topic "processed-events" -Partitions 3 -CleanupPolicy "delete"
Invoke-KafkaTopics -Topic "event-counts" -Partitions 1 -CleanupPolicy "compact,delete" -ExtraArgs @(
  "--config", "min.cleanable.dirty.ratio=0.1"
)
