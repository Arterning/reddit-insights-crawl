import sqlite3

DB_PATH = "reddit_data/reddit_data.db"

def initialize_database():
    """初始化数据库，创建所需的表"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 创建 favorites 表
        # post_id 作为主键，并添加一个时间戳记录收藏时间
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS favorites (
                post_id TEXT PRIMARY KEY,
                favorited_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (post_id) REFERENCES posts (id)
            )
                       
            CREATE TABLE IF NOT EXISTS posts (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    content TEXT,
                    score INTEGER,
                    num_comments INTEGER,
                    created_utc TIMESTAMP,
                    author TEXT,
                    subreddit TEXT,
                    url TEXT,
                    search_pattern TEXT,
                    upvote_ratio REAL,
                    is_self BOOLEAN,
                    domain TEXT,
                    extracted_at TIMESTAMP
                )
                       

            CREATE TABLE IF NOT EXISTS comments (
                    comment_id TEXT PRIMARY KEY,
                    post_id TEXT,
                    body TEXT,
                    score INTEGER,
                    created_utc TIMESTAMP,
                    author TEXT,
                    FOREIGN KEY (post_id) REFERENCES posts (id)
                )
        ''')
        
        print("数据库初始化成功，'favorites' 表已创建或已存在。")
        
        conn.commit()
    except Exception as e:
        print(f"数据库初始化失败: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    initialize_database()
