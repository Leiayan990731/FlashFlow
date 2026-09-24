param(
    [string]$BaseUrl = "http://localhost:8080"
)

$product = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/products/sku-phone"
Write-Host "Product stock: $($product.stock)"

$headers = @{
    "X-User-ID" = "smoke-user"
    "Idempotency-Key" = "smoke-$([Guid]::NewGuid())"
}
$body = @{ productId = "sku-phone"; quantity = 1 } | ConvertTo-Json
$order = Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/orders" -Headers $headers -ContentType "application/json" -Body $body
Write-Host "Accepted order: $($order.orderId)"

for ($attempt = 0; $attempt -lt 30; $attempt++) {
    $current = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/orders/$($order.orderId)" -Headers @{ "X-User-ID" = "smoke-user" }
    if ($current.status -ne "PENDING") {
        Write-Host "Final status: $($current.status)"
        exit 0
    }
    Start-Sleep -Milliseconds 500
}
throw "Order remained pending for more than 15 seconds"
