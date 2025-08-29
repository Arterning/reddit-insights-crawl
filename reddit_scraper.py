import praw
import pandas as pd
import re
import json
import time
from datetime import datetime
import logging
import sqlite3

import os
from dotenv import load_dotenv

# 加载 .env 文件（默认查找项目根目录的 .env）
load_dotenv()  # 等价于 load_dotenv(".env")



# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedditScraper:
    def __init__(self, client_id, client_secret, user_agent, proxy_url=None):
        """
        初始化Reddit API客户端
        
        Args:
            client_id: Reddit应用的客户端ID
            client_secret: Reddit应用的客户端密钥
            user_agent: 用户代理字符串，格式如 "YourAppName/1.0"
            proxy_url: HTTP代理URL，格式如 "http://proxy_host:proxy_port" 或 "socks5://proxy_host:proxy_port"
        """
        # 配置代理
        requestor_kwargs = {}
        if proxy_url:
            import requests
            session = requests.Session()
            session.proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            requestor_kwargs['session'] = session
        
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            requestor_kwargs=requestor_kwargs
        )
        
        # 定义搜索模式 - 这些都是发现SaaS机会的关键词
        self.search_patterns = [
            "is there a tool",
            "i wish there was an app", 
            "i wish there an app",
            "how do you guys manage",
            "is there a better way to",
            "looking for a tool",
            "need an app for",
            "wish someone would build",
            "there should be an app",
            "anyone know of a tool",
            "how do you handle",
            "what tools do you use",
            "struggling with",
            "pain point",
            "frustrating that there's no"
        ]
    
    def search_posts(self, subreddit_names, limit=100, time_filter='month'):
        """
        搜索相关帖子
        
        Args:
            subreddit_names: 子版块名称列表
            limit: 每个搜索模式的结果限制
            time_filter: 时间筛选 ('hour', 'day', 'week', 'month', 'year', 'all')
        """
        all_posts = []
        
        for subreddit_name in subreddit_names:
            logger.info(f"正在搜索子版块: {subreddit_name}")
            subreddit = self.reddit.subreddit(subreddit_name)
            
            for pattern in self.search_patterns:
                try:
                    logger.info(f"搜索模式: '{pattern}'")
                    
                    # 搜索帖子
                    search_results = subreddit.search(
                        pattern, 
                        limit=limit, 
                        time_filter=time_filter,
                        sort='relevance'
                    )
                    
                    for post in search_results:
                        # 检查标题或内容是否真正匹配我们的模式
                        if self._is_relevant_post(post, pattern):
                            post_data = self._extract_post_data(post, pattern, subreddit_name)
                            all_posts.append(post_data)
                    
                    # 避免API限制
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"搜索 '{pattern}' 时出错: {e}")
                    continue
        
        return all_posts
    
    def _is_relevant_post(self, post, pattern):
        """检查帖子是否真正相关"""
        text_to_check = f"{post.title} {post.selftext}".lower()
        
        # 更精确的匹配逻辑
        pattern_variations = [
            pattern.lower(),
            pattern.lower().replace("is there", "is there any"),
            pattern.lower().replace("i wish", "i really wish"),
        ]
        
        return any(variation in text_to_check for variation in pattern_variations)
    
    def _extract_post_data(self, post, search_pattern, subreddit_name):
        """提取帖子数据"""
        return {
            'id': post.id,
            'title': post.title,
            'content': post.selftext,
            'score': post.score,
            'num_comments': post.num_comments,
            'created_utc': datetime.fromtimestamp(post.created_utc),
            'author': str(post.author) if post.author else '[deleted]',
            'subreddit': subreddit_name,
            'url': f"https://reddit.com{post.permalink}",
            'search_pattern': search_pattern,
            'upvote_ratio': post.upvote_ratio,
            'is_self': post.is_self,
            'domain': post.domain,
            'extracted_at': datetime.now()
        }
    
    def get_comments(self, post_ids, max_comments=50):
        """获取帖子评论"""
        comments_data = []
        
        for post_id in post_ids:
            try:
                post = self.reddit.submission(id=post_id)
                post.comments.replace_more(limit=0)  # 移除 "更多评论" 的占位符
                
                for comment in post.comments.list()[:max_comments]:
                    if hasattr(comment, 'body'):
                        comments_data.append({
                            'post_id': post_id,
                            'comment_id': comment.id,
                            'body': comment.body,
                            'score': comment.score,
                            'created_utc': datetime.fromtimestamp(comment.created_utc),
                            'author': str(comment.author) if comment.author else '[deleted]'
                        })
                
                time.sleep(0.5)  # 避免API限制
                
            except Exception as e:
                logger.error(f"获取帖子 {post_id} 的评论时出错: {e}")
                continue
        
        return comments_data
    
    def save_to_files(self, posts_data, comments_data=None, output_dir="reddit_data", save_to_sqlite=True):
        """保存数据到文件"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存帖子数据
        posts_df = pd.DataFrame(posts_data)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        posts_file = f"{output_dir}/reddit_posts_{timestamp}.csv"
        posts_df.to_csv(posts_file, index=False, encoding='utf-8')
        logger.info(f"帖子数据已保存到: {posts_file}")
        
        # 保存为JSON格式（便于后续AI分析）
        json_file = f"{output_dir}/reddit_posts_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(posts_data, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"帖子数据已保存到: {json_file}")
        
        # 保存评论数据
        if comments_data:
            comments_df = pd.DataFrame(comments_data)
            comments_file = f"{output_dir}/reddit_comments_{timestamp}.csv"
            comments_df.to_csv(comments_file, index=False, encoding='utf-8')
            logger.info(f"评论数据已保存到: {comments_file}")

        # 保存到SQLite
        if save_to_sqlite:
            self.save_to_sqlite(posts_data, comments_data, output_dir)
        
        return posts_file, json_file

    def save_to_sqlite(self, posts_data, comments_data=None, db_path="reddit_data"):
        """保存数据到SQLite数据库"""
        db_file = f"{db_path}/reddit_data.db"
        
        with sqlite3.connect(db_file) as conn:
            # 创建帖子表
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
            
            # 创建评论表
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
            
            # 插入帖子数据
            for post in posts_data:
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
            
            # 插入评论数据
            if comments_data:
                for comment in comments_data:
                    conn.execute('''
                        INSERT OR REPLACE INTO comments 
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        comment['comment_id'], comment['post_id'], comment['body'],
                        comment['score'], comment['created_utc'], comment['author']
                    ))
            
            conn.commit()
        
        logger.info(f"数据已保存到SQLite数据库: {db_file}")
    
    def analyze_patterns(self, posts_data):
        """分析搜索模式的效果"""
        df = pd.DataFrame(posts_data)
        
        pattern_analysis = df.groupby('search_pattern').agg({
            'score': ['count', 'mean', 'sum'],
            'num_comments': ['mean', 'sum'],
            'upvote_ratio': 'mean'
        }).round(2)
        
        print("\n=== 搜索模式分析 ===")
        print(pattern_analysis)
        
        # 高质量帖子分析（高分且评论多）
        high_quality = df[(df['score'] >= 10) & (df['num_comments'] >= 5)]
        print(f"\n高质量帖子数量: {len(high_quality)}")
        
        return pattern_analysis

# 使用示例
def main():
    # 配置你的Reddit API credentials
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    USER_AGENT = "SaaSOpportunityFinder/1.0"
    
    # HTTP代理配置（可选）
    # 支持格式：
    # HTTP代理: "http://proxy_host:proxy_port"
    # HTTPS代理: "https://proxy_host:proxy_port" 
    # SOCKS5代理: "socks5://proxy_host:proxy_port"
    # 带认证: "http://username:password@proxy_host:proxy_port"
    # 设置为你的代理URL，或保持None不使用代理
    PROXY_URL = os.getenv("PROXY_URL")
    
    # 初始化爬虫
    scraper = RedditScraper(CLIENT_ID, CLIENT_SECRET, USER_AGENT, PROXY_URL)
    
    # 定义要搜索的子版块（选择与SaaS相关的社区）
    target_subreddits = [
        'entrepreneur',
        'startups', 
        'SaaS',
        'productivity',
        'smallbusiness',
        'webdev',
        'digitalnomad',
        'freelance',
        'marketing',
        'analytics'
    ]
    
    # 搜索帖子
    print("开始搜索相关帖子...")
    posts = scraper.search_posts(
        subreddit_names=target_subreddits,
        limit=50,  # 每个模式限制50个结果
        time_filter='month'  # 搜索最近一个月的内容
    )
    
    print(f"找到 {len(posts)} 个相关帖子")
    
    # 获取评论（可选，会增加API调用次数）
    print("获取评论数据...")
    post_ids = [post['id'] for post in posts[:20]]  # 只获取前20个帖子的评论
    comments = scraper.get_comments(post_ids, max_comments=30)
    
    print(f"获取了 {len(comments)} 条评论")
    
    # 保存数据
    posts_file, json_file = scraper.save_to_files(posts, comments)
    
    # 分析模式效果
    scraper.analyze_patterns(posts)
    
    print(f"\n数据已保存，可以用于后续AI分析")
    print(f"帖子文件: {posts_file}")
    print(f"JSON文件: {json_file}")

if __name__ == "__main__":
    main()