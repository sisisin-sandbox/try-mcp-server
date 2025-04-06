#!/usr/bin/env -S deno run --allow-net --allow-env --allow-read
import { McpServer, ResourceTemplate } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { listTables } from './bigquery.ts';
import { ReadResourceResult } from '@modelcontextprotocol/sdk/types.js';

// McpServerの初期化
const server = new McpServer({
  name: 'bigquery-mcp-server',
  version: '0.1.0',
});

server.resource(
  'BigQueryのテーブル一覧',
  new ResourceTemplate('bigquery://{datasetId}/tables', { list: undefined }),
  async (_uri: URL, variables): Promise<ReadResourceResult> => {
    const datasetId = variables.datasetId;
    if (typeof datasetId !== 'string') {
      return {
        contents: [
          {
            uri: 'error://invalid-dataset-id',
            text: 'datasetIdが指定されていません。',
          },
        ],
        isError: true,
      };
    }

    try {
      const tables = await listTables(datasetId);
      return {
        contents: [
          {
            uri: 'bigquery://{datasetId}/tables',
            text: JSON.stringify(tables, null, 2),
          },
        ],
      };
    } catch (error) {
      return {
        contents: [
          {
            uri: 'error://query-error',
            text: error instanceof Error ? error.message : String(error),
          },
        ],
        isError: true,
      };
    }
  },
);

// サーバーの起動
async function run() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('BigQuery MCP サーバーが起動しました (stdio)');
}

// エラーハンドリング
run().catch((error) => {
  console.error('サーバー実行エラー:', error);
  Deno.exit(1);
});

// SIGINTハンドリング
Deno.addSignalListener('SIGINT', async () => {
  await server.close();
  Deno.exit(0);
});
