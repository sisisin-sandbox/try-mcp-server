import { McpServer, ResourceTemplate } from '@modelcontextprotocol/sdk/server/mcp.js';
import { ReadResourceResult } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { listTables } from './bigquery.ts';

const listTablesSchema = z.object({
  datasetId: z.string(),
  projectId: z.string().optional(),
});

export function applyBigQueryServer(server: McpServer) {
  server.resource(
    'BigQueryのテーブル一覧',
    new ResourceTemplate('bigquery://datasets/{datasetId}/tables', { list: undefined }),
    {
      description:
        '指定したdataset配下のBigQueryのテーブル一覧を取得します。query parameterにprojectIdを指定できます。',
      mimeType: 'application/json',
    },
    async (_uri: URL, variables): Promise<ReadResourceResult> => {
      const result = listTablesSchema.safeParse(variables);
      if (!result.success) {
        return {
          contents: [
            {
              uri: 'error://invalid-parameters',
              text: `datasetIdが指定されていません。`,
            },
          ],
          isError: true,
        };
      }
      const datasetId = result.data.datasetId;
      const projectId = result.data.projectId;

      try {
        const tables = await listTables(datasetId, projectId);
        return {
          contents: [
            {
              uri: 'bigquery://datasets/{datasetId}/tables',
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
}
