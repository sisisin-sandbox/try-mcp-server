import { BigQuery } from '@google-cloud/bigquery';

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
 * @returns テーブル情報の配列
 */
export async function listTables(datasetId: string) {
  const adc = await bigqueryClient.authClient.getApplicationDefault();
  try {
    const [tables] = await bigqueryClient.dataset(datasetId).getTables();

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
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`BigQuery API エラー: ${errorMessage}, ADC: ${JSON.stringify(adc)}`);
  }
}
