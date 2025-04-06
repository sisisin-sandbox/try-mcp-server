import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { applyBigQueryServer } from './bigqueryServer.ts';

// McpServerの初期化
const server = new McpServer({
  name: 'bigquery-mcp-server',
  version: '0.1.0',
});

applyBigQueryServer(server);

const transport = new StdioServerTransport();
server.connect(transport).catch((error) => {
  console.error('サーバー実行エラー:', error);
  Deno.exit(1);
});

// SIGINTハンドリング
Deno.addSignalListener('SIGINT', async () => {
  await server.close();
  Deno.exit(0);
});
