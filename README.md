# BigQuery MCP サーバー

### example setting

```json
{
  "mcpServers": {
    "bigquery": {
      "command": "path/to/sisisin-sandbox/try-mcp-server/bin/mcp_bigquery",
      "args": [],
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "path/to/.config/gcloud/application_default_credentials.json"
      },
      "disabled": false,
      "alwaysAllow": []
    }
  }
}
```
