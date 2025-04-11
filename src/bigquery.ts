import { BigQuery, Query, QueryRowsResponse } from '@google-cloud/bigquery';

// 環境変数からGCPプロジェクトIDを取得（オプション）
// const PROJECT_ID = Deno.env.get('GCP_PROJECT_ID');

// BigQueryクライアントの初期化
// credentialsを指定せず、デフォルトの認証情報を使用
const credPath = Deno.env.get('GOOGLE_APPLICATION_CREDENTIALS');
if (!credPath) {
  throw new Error('GOOGLE_APPLICATION_CREDENTIALS環境変数が設定されていません。');
}
const bigqueryClient = new BigQuery();

/**
 * 指定したデータセット内のテーブル一覧を取得する
 * @param datasetId データセットID
 * @param projectId プロジェクトID（オプション）
 * @returns テーブル情報の配列
 */
export async function listTables(datasetId: string, projectId: string | undefined) {
  const [tables] = await bigqueryClient.dataset(datasetId, { projectId }).getTables();

  return tables.map((table) => {
    // 基本情報を取得
    const tableInfo: Record<string, unknown> = {
      id: table.id,
    };

    // メタデータが存在する場合のみ追加
    if (table.metadata) {
      if (table.metadata.kind) tableInfo.kind = table.metadata.kind;
      if (table.metadata.type) tableInfo.type = table.metadata.type;

      // 時間値の処理（存在する場合のみ変換）
      try {
        if (table.metadata.creationTime) {
          tableInfo.creationTime = new Date(parseInt(table.metadata.creationTime)).toISOString();
        }
        if (table.metadata.lastModifiedTime) {
          tableInfo.lastModifiedTime = new Date(parseInt(table.metadata.lastModifiedTime)).toISOString();
        }
      } catch (timeError) {
        console.error('時間値の変換エラー:', timeError);
        // エラーが発生しても処理を続行
      }
    }

    return tableInfo;
  });
}

/**
 * BigQueryに対してSQLクエリを実行する
 * @param query 実行するSQLクエリ
 * @param options クエリオプション
 * @returns クエリ結果
 */
export async function executeQuery(
  query: string,
  options?: {
    projectId?: string;
    location?: string;
    maxResults?: number;
    params?: Record<string, unknown>;
    dryRun?: boolean;
  },
): Promise<{
  rows: Record<string, unknown>[];
  metadata: {
    totalRows: string;
    schema?: unknown;
    jobId?: string;
    jobReference?: unknown;
    totalBytesProcessed?: string;
    cacheHit?: boolean;
  };
}> {
  const queryOptions: Query = {};

  if (options?.projectId) {
    queryOptions.projectId = options.projectId;
  }

  if (options?.location) {
    queryOptions.location = options.location;
  }

  if (options?.maxResults) {
    queryOptions.maxResults = options.maxResults;
  }

  if (options?.params) {
    queryOptions.params = options.params;
  }

  if (options?.dryRun) {
    queryOptions.dryRun = options.dryRun;
  }

  try {
    const [rows, job] = await bigqueryClient.query({
      query,
      ...queryOptions,
    });

    // 結果とジョブ情報を整形
    const result = {
      rows: rows as Record<string, unknown>[],
      metadata: {
        totalRows: rows.length.toString(),
        jobId: job?.id || '',
      },
    };

    return result;
  } catch (error) {
    console.error('クエリ実行エラー:', error);
    throw error;
  }
}
