Param(
  [string]$SpaceBase = "https://hamza-naimat-ai-chatbot.hf.space",
  [string]$DbName = "agentfactory",
  [string]$CrawlUrl = "https://agentfactory.panaversity.org",
  [int]$MaxPages = 5000,
  [switch]$ClearBeforeCrawl = $true
)

$ErrorActionPreference = "Stop"

function Write-Json($obj) {
  $obj | ConvertTo-Json -Depth 20
}

Write-Host ""
Write-Host "HF Space: $SpaceBase"
Write-Host "DB:       $DbName"
Write-Host "URL:      $CrawlUrl"
Write-Host "MaxPages: $MaxPages"
Write-Host "Clear:    $($ClearBeforeCrawl.IsPresent)"
Write-Host ""

$pw = Read-Host -Prompt "Enter ADMIN password (input hidden)" -AsSecureString
$bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($pw)
try {
  $plain = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
} finally {
  [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
}

if (-not $plain -or -not $plain.Trim()) {
  throw "Password required."
}

Write-Host ""
Write-Host "1) Setting active DB..."
$setResp = Invoke-RestMethod -Method Post -Uri "$SpaceBase/admin/databases/set-active" -Body @{
  password = $plain
  name = $DbName
} -TimeoutSec 60
Write-Host (Write-Json $setResp)

Write-Host ""
Write-Host "2) Starting crawl..."
$body = @{
  password = $plain
  db_name = $DbName
  url = $CrawlUrl
  max_pages = $MaxPages
  clear_before_crawl = [bool]$ClearBeforeCrawl
  url_patterns = @()
  embedding_model = "bge"
} | ConvertTo-Json -Depth 10

try {
  # /admin/crawl streams SSE; Invoke-WebRequest often errors locally even when the server is running fine.
  Invoke-WebRequest -Method Post -Uri "$SpaceBase/admin/crawl" -ContentType "application/json" -Body $body -TimeoutSec 60 | Out-Null
} catch {
  Write-Host "NOTE: /admin/crawl is a streaming endpoint; client may show an error while crawl continues."
  Write-Host ("Client error (safe to ignore if crawl-status shows running): " + $_.Exception.Message)
}

Write-Host ""
Write-Host "3) Monitoring crawl-status (polling every 10s)..."
$encoded = [uri]::EscapeDataString($plain)
$statusUrl = "$SpaceBase/admin/crawl-status?password=$encoded&db_name=$DbName"

while ($true) {
  Start-Sleep -Seconds 10
  try {
    $st = Invoke-RestMethod -Uri $statusUrl -TimeoutSec 60
  } catch {
    Write-Host ("crawl-status error: " + $_.Exception.Message)
    continue
  }

  $running = [bool]$st.running
  $done = [bool]$st.done
  $error = [bool]$st.error
  $total = $st.total

  Write-Host ""
  Write-Host ("running={0} done={1} error={2} total={3}" -f $running, $done, $error, $total)

  $logs = @()
  try { $logs = @($st.logs) } catch {}
  if ($logs.Count -gt 0) {
    $tail = $logs | Select-Object -Last 20
    Write-Host "---- last 20 logs ----"
    $tail | ForEach-Object { Write-Host $_ }
    Write-Host "----------------------"
  }

  if ($done -or (-not $running)) {
    if ($error) {
      Write-Host ""
      Write-Host "Crawl ended with error=true. See logs above."
      exit 2
    }
    Write-Host ""
    Write-Host "Crawl finished."
    exit 0
  }
}

