import glob
import sqlite3
import logging
import json
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_all_files_to_sqlite(output_dir="reddit_data"):
    """将目录下所有JSON文件导入SQLite"""
    db_file = f"{output_dir}/reddit_data.db"
    
    # 获取所有JSON文件
    post_files = glob.glob(f"{output_dir}/reddit_posts_*.json")
    comment_files = glob.glob(f"{output_dir}/reddit_comments_*.json")
    
    logger.info(f"找到 {len(post_files)} 个帖子文件和 {len(comment_files)} 个评论文件")
    
    with sqlite3.connect(db_file) as conn:
        # 创建表结构（如果不存在）
        conn.execute('''
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
        ''')
        
        conn.execute('''
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
        
        # 导入帖子数据
        posts_imported = 0
        for file_path in post_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    posts_data = json.load(f)
                
                for post in posts_data:
                    # 处理日期字段
                    if isinstance(post.get('created_utc'), str):
                        post['created_utc'] = datetime.fromisoformat(post['created_utc'].replace('Z', '+00:00'))
                    if isinstance(post.get('extracted_at'), str):
                        post['extracted_at'] = datetime.fromisoformat(post['extracted_at'].replace('Z', '+00:00'))
                    
                    conn.execute('''
                        INSERT OR REPLACE INTO posts 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        post['id'], post['title'], post['content'], post['score'],
                        post['num_comments'], post['created_utc'], post['author'],
                        post['subreddit'], post['url'], post['search_pattern'],
                        post['upvote_ratio'], post['is_self'], post['domain'],
                        post['extracted_at']
                    ))
                    posts_imported += 1
                
                logger.info(f"已导入帖子文件: {file_path}")
            except Exception as e:
                logger.error(f"导入帖子文件 {file_path} 时出错: {e}")
        
        # 导入评论数据
        comments_imported = 0
        for file_path in comment_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    comments_data = json.load(f)
                
                for comment in comments_data:
                    # 处理日期字段
                    if isinstance(comment.get('created_utc'), str):
                        comment['created_utc'] = datetime.fromisoformat(comment['created_utc'].replace('Z', '+00:00'))
                    
                    conn.execute('''
                        INSERT OR REPLACE INTO comments 
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        comment['comment_id'], comment['post_id'], comment['body'],
                        comment['score'], comment['created_utc'], comment['author']
                    ))
                    comments_imported += 1
                
                logger.info(f"已导入评论文件: {file_path}")
            except Exception as e:
                logger.error(f"导入评论文件 {file_path} 时出错: {e}")
        
        conn.commit()
    
    logger.info(f"导入完成: {posts_imported} 个帖子, {comments_imported} 条评论")
    return posts_imported, comments_imported

