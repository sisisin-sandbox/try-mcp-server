import { McpServer, ResourceTemplate } from '@modelcontextprotocol/sdk/server/mcp.js';
import { ReadResourceResult } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { executeQuery, listTables } from './bigquery.ts';

const listTablesSchema = z.object({
  datasetId: z.string(),
  projectId: z.string().optional(),
});

export function applyBigQueryServer(server: McpServer) {
  server.resource(
    'BigQueryのテーブル一覧',
    new ResourceTemplate('bigquery://datasets/{datasetId}/tables{?projectId}', { list: undefined }),
    {
      description:
        '指定したdataset配下のBigQueryのテーブル一覧を取得します。URLのクエリパラメータまたは変数としてprojectIdを指定できます（例: bigquery://datasets/{datasetId}/tables?projectId=your-project-id）。projectIdが指定されない場合はデフォルトのプロジェクトが使用されます。',
      mimeType: 'application/json',
    },
    async (_uri: URL, variables): Promise<ReadResourceResult> => {
      const result = listTablesSchema.safeParse(variables);
      if (!result.success) {
        return {
          contents: [
            {
              uri: 'error://invalid-parameters',
              text: `datasetIdが指定されていません。projectIdはオプショナルです。`,
            },
          ],
          isError: true,
        };
      }
      const datasetId = result.data.datasetId;

      // URLのクエリパラメータからprojectIdを取得
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
  // クエリ実行ツールの追加
  server.tool(
    'BigQueryクエリ実行',
    'BigQueryに対してSQLクエリを実行します。',
    {
      query: z.string(),
      projectId: z.string().optional(),
      location: z.string().optional(),
      maxResults: z.number().optional(),
      params: z.record(z.unknown()).optional(),
      dryRun: z.boolean().optional(),
    },
    async (args, _extra) => {
      try {
        const result = await executeQuery(args.query, {
          projectId: args.projectId,
          location: args.location,
          maxResults: args.maxResults,
          params: args.params,
          dryRun: args.dryRun,
        });

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(
                {
                  rows: result.rows,
                  metadata: result.metadata,
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch (error) {
        return {
          content: [
            {
              type: 'text',
              text: error instanceof Error ? error.message : String(error),
            },
          ],
          isError: true,
        };
      }
    },
  );
}
