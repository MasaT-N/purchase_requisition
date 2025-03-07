import requests
import yaml
import dotenv
import os
import json
import base64
from datetime import date, datetime
import pytz
# import sqlite3    # SQLiteへの接続は廃止
import logging
import pyodbc   # SQL Serverへの接続に変更

dotenv.load_dotenv()

USER_ID = os.getenv('USER_ID')
API_KEY = os.getenv('API_KEY')
API_URL = os.getenv('API_URL')

DATABASE_SERVER = os.getenv('DATABASE_SERVER')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')

DB_CONNECTION_STRING = f"DRIVER={{SQL Server}};SERVER={DATABASE_SERVER};DATABASE={DATABASE_NAME};UID={DATABASE_USER};PWD={DATABASE_PASSWORD}"
config = {}

logging.basicConfig(
    filename='check_approved_requests.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

with open("config.yml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# DATABASE_NAME = "purchase_requisitions.db"  # データベースファイル名
TABLE_NAME = config["table_name"]

def main():
    """
    メイン処理関数。
    """
    search_results = get_data()

    if search_results:
        parsed_data = parse_documents_list(search_results)  # データを解析
        newly_added_count = save_documents_to_db(parsed_data)   # データベースに保存
        print(f"申請文書データをデータベース({DATABASE_NAME})の{TABLE_NAME}テーブルに保存しました。") #テーブル名出力
        print(f"新規登録件数：{newly_added_count}件")
        if newly_added_count > 0:
            msg = f"申請文書データをデータベース({DATABASE_NAME})の{TABLE_NAME}テーブルに保存しました。新規登録件数：{newly_added_count}件"
            logging.info(msg=msg)
    else:
        print("検索結果が取得できませんでした。")
        logging.ERROR("申請文書データの検索結果が取得できませんでした。")


def get_data(form_id=40):
    """
    コラボフローAPIを使用して申請書を検索する関数。
    """
    url = f"{API_URL}/v1/documents/search"  # APIのエンドポイントURL

    AUTH_KEY = generate_auth_key(USER_ID, API_KEY)  # 認証キーを生成

    headers = {
        "X-Collaboflow-Authorization": "Basic " + AUTH_KEY, # 認証キーをヘッダーに追加
        "Content-Type": "application/json"
    }
    # 本日の日付を取得し、ISO 8601形式の文字列に変換
    today = date.today().strftime("%Y-%m-%d")   # 本日の日付を取得

    # 検索条件の設定
    payload = {
        "app_cd": 1,
        "query": f"form_id = {form_id} AND end_date >= '{today}'" + " ORDER BY end_date DESC",  # フォームIDと決裁日時の条件を設定
        "offset": 0,
        "limit": 100,
   }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # HTTPエラーが発生した場合に例外を発生させる
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"APIへのリクエスト中にエラーが発生しました: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSONデータのデコード中にエラーが発生しました: {e}")
        print(f"レスポンス内容: {response.text}")  # Debugging purposes
        return None
    except Exception as e:
        print(f"予期しないエラーが発生しました: {e}")
        return None


def generate_auth_key(user_id, api_key):
    """
    Collaboflow APIの認証キーを生成する関数。
    """
    auth_string = f"{user_id}/apikey:{api_key}" # ユーザーIDとAPIキーを結合
    auth_bytes = auth_string.encode('utf-8')    # バイト列に変換
    auth_key_bytes = base64.b64encode(auth_bytes)   # Base64エンコード
    auth_key = auth_key_bytes.decode('utf-8')       # 文字列に変換

    return auth_key # 認証キーを返す


def parse_documents_list(data: dict, form_id=40) -> list:
    """
    APIから取得したデータを解析する関数。

    Parameters
    ----------
    data : dict
        APIから取得したJSONデータ
    form_id : int 
        フォームID  (デフォルト値: 40)

    Returns
    -------
    list
        解析された申請文書リスト
    """
    documents_list = []
    if "records" in data:
        for record in data["records"]:
            # UTCのdatetimeオブジェクトに変換
            utc_dt = datetime.fromisoformat(record["end_date"].replace("Z", "+00:00"))

            # 日本時間に変換
            jst_tz = pytz.timezone('Asia/Tokyo')
            jst_dt = utc_dt.astimezone(jst_tz)

            # フォーマットを 'yyyy-mm-dd hh:mm:ss' に変換
            formatted_end_date = jst_dt.strftime('%Y-%m-%d %H:%M:%S')

            document = {
                "document_id": record["document_id"],  # 文書ID
                "document_number": record["document_number"],  # 文書番号
                "title": record["title"],  # 文書タイトル
                "request_user": record["request_user"]["name"],  # 申請者
                "request_group": record["request_group"]["name"],  # 申請部署
                "end_date": formatted_end_date,  # 決裁日時
                "form_id": form_id,  # フォームID
            }
            if form_id == 40:  # 長津工業の購買申請書フォームの場合
                factory_key = record["document_number"][3:4]
                dict_factory = config["request_factory_list"]
                if factory_key in dict_factory:
                    document.setdefault("request_factory", dict_factory[factory_key])  # 申請工場
                else:
                    document.setdefault("request_factory", "不明")
            documents_list.append(document) # リストに追加

    return documents_list   # リストを返す


def create_table(cursor):
    """
    データベースにテーブルを作成する関数。
    """
    try:
        # テーブルが既に存在するかどうかを確認しながらテーブル作成
        cursor.execute(f"""
            IF OBJECT_ID('{TABLE_NAME}', 'U') IS NULL
            CREATE TABLE {TABLE_NAME} (
                document_id INT not null,
                document_number nvarchar(30),
                title nvarchar(100),
                request_user nvarchar(40),
                request_group nvarchar(20),
                request_factory nvarchar(20) default '',                
                form_id INT default 0,
                end_date datetime,
                primary key (document_id)
            )
        """)
        return True
    except Exception as e:
        print(f"テーブルの作成に失敗しました: {e}")
        logging.error(f"{TABLE_NAME}テーブルの作成に失敗しました: {e}")
        return False

    


def save_documents_to_db(documents):
    """
    申請文書リストをデータベースに保存する関数。
     document_id が重複しないように、INSERT OR IGNORE を使用
     新規に登録された件数を返すように変更
    """
    conn = None
    newly_added_count = 0

    try:
        # Microsoft SQL Server接続文字列
        conn_str = DB_CONNECTION_STRING
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        # データベース接続
        if create_table(cursor):
                    # INSERT文を変更
        # 現在の行数を取得
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            current_count = cursor.fetchone()[0]

            for document in documents:
                cursor.execute(f"""
                    IF NOT EXISTS (
                        SELECT 1
                        FROM {TABLE_NAME}
                        WHERE document_id = ?
                    )
                    INSERT INTO {TABLE_NAME} (
                        document_id, document_number, title, request_user,
                        request_group, end_date, form_id, request_factory
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    document["document_id"],
                    document["document_id"],
                    document["document_number"],
                    document["title"],
                    document["request_user"],
                    document["request_group"],
                    document["end_date"],
                    document["form_id"],
                    document.get("request_factory", ""),
                ))
            
            # INSERT後の行数を取得
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            new_count = cursor.fetchone()[0]

            # 新規登録件数を計算
            newly_added_count = new_count - current_count

            conn.commit()

    except pyodbc.Error as e:
        print(f"データベースエラー: {e}")
        logging.error(f"データベースエラー: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return newly_added_count # 新規登録件数を返す


if __name__ == "__main__":
    main()
