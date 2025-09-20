from flask import Flask, render_template, request, jsonify
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json
import os
from reddit_scraper import RedditScraper
from favorites_api import favorites_api

app = Flask(__name__)
app.register_blueprint(favorites_api)

# 数据库配置
DB_PATH = "reddit_data/reddit_data.db"

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    """首页 - 显示数据统计概览"""
    try:
        conn = get_db_connection()
        
        # 获取基本统计数据
        stats = {}
        
        # 帖子总数
        stats['total_posts'] = conn.execute('SELECT COUNT(*) FROM posts').fetchone()[0]
        
        # 评论总数
        stats['total_comments'] = conn.execute('SELECT COUNT(*) FROM comments').fetchone()[0]
        
        # 子版块数量
        stats['total_subreddits'] = conn.execute('SELECT COUNT(DISTINCT subreddit) FROM posts').fetchone()[0]
        
        # 搜索模式数量
        stats['total_patterns'] = conn.execute('SELECT COUNT(DISTINCT search_pattern) FROM posts').fetchone()[0]
        
        # 平均分数
        avg_score = conn.execute('SELECT AVG(score) FROM posts').fetchone()[0]
        stats['avg_score'] = round(avg_score, 2) if avg_score else 0
        
        # 最近7天的帖子数量
        week_ago = datetime.now() - timedelta(days=7)
        stats['posts_last_week'] = conn.execute(
            'SELECT COUNT(*) FROM posts WHERE created_utc >= ?', 
            (week_ago,)
        ).fetchone()[0]
        
        conn.close()
        
        return render_template('index.html', stats=stats)
    
    except Exception as e:
        return f"数据库错误: {e}"

@app.route('/posts')
def posts():
    """帖子列表页面"""
    try:
        conn = get_db_connection()
        
        # 分页参数
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        
        # 筛选参数
        subreddit = request.args.get('subreddit', '')
        search_pattern = request.args.get('search_pattern', '')
        min_score = request.args.get('min_score', 0, type=int)
        sort = request.args.get('sort', 'extracted_at_desc')
        
        # 构建查询
        query = 'SELECT * FROM posts WHERE 1=1'
        params = []
        
        if subreddit:
            query += ' AND subreddit = ?'
            params.append(subreddit)
        
        if search_pattern:
            query += ' AND search_pattern = ?'
            params.append(search_pattern)
        
        if min_score > 0:
            query += ' AND score >= ?'
            params.append(min_score)
        
        # 排序映射
        sort_mapping = {
            'extracted_at_desc': 'extracted_at DESC',
            'extracted_at_asc': 'extracted_at ASC',
            'created_utc_desc': 'created_utc DESC',
            'created_utc_asc': 'created_utc ASC',
            'score_desc': 'score DESC',
            'score_asc': 'score ASC'
        }

        # 排序和分页
        sort_clause = sort_mapping.get(sort, 'extracted_at DESC')
        query += f' ORDER BY {sort_clause} LIMIT ? OFFSET ?'
        params.extend([per_page, (page - 1) * per_page])
        
        posts = conn.execute(query, params).fetchall()
        
        # 获取筛选选项
        subreddits = conn.execute('SELECT DISTINCT subreddit FROM posts ORDER BY subreddit').fetchall()
        patterns = conn.execute('SELECT DISTINCT search_pattern FROM posts ORDER BY search_pattern').fetchall()
        
        # 获取总数用于分页
        count_query = query.replace('SELECT *', 'SELECT COUNT(*)').split(' ORDER BY')[0]
        total_count = conn.execute(count_query, params[:-2]).fetchone()[0]
        
        conn.close()

        # print("debug", posts)
        
        return render_template('posts.html', 
                             posts=posts, 
                             subreddits=subreddits,
                             patterns=patterns,
                             page=page,
                             per_page=per_page,
                             total_count=total_count,
                             current_filters={
                                 'subreddit': subreddit,
                                 'search_pattern': search_pattern,
                                 'min_score': min_score,
                                 'sort': sort
                             })
    
    except Exception as e:
        import traceback
        traceback.print_exc()  # 打印完整的堆栈跟踪信息
        return f"数据库错误: {str(e)}"

@app.route('/post/<post_id>')
def post_detail(post_id):
    """帖子详情页面"""
    try:
        conn = get_db_connection()
        
        # 获取帖子详情
        post = conn.execute('SELECT * FROM posts WHERE id = ?', (post_id,)).fetchone()
        
        if not post:
            return "帖子未找到", 404
        
        # 获取评论
        comments = conn.execute(
            'SELECT * FROM comments WHERE post_id = ? ORDER BY score DESC',
            (post_id,)
        ).fetchall()
        
        # 检查帖子是否已收藏
        is_favorited = conn.execute('SELECT post_id FROM favorites WHERE post_id = ?', (post_id,)).fetchone() is not None
        
        conn.close()
        
        return render_template('post_detail.html', post=post, comments=comments, is_favorited=is_favorited)
    
    except Exception as e:
        return f"数据库错误: {e}"

@app.route('/favorites')
def favorites():
    """收藏帖子列表页面"""
    try:
        conn = get_db_connection()
        
        # 分页参数
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        
        # 构建查询，通过JOIN获取收藏帖子的完整信息
        query = '''
            SELECT p.* FROM posts p
            JOIN favorites f ON p.id = f.post_id
            ORDER BY f.favorited_at DESC
            LIMIT ? OFFSET ?
        '''
        params = [per_page, (page - 1) * per_page]
        
        posts = conn.execute(query, params).fetchall()
        
        # 获取总数用于分页
        total_count = conn.execute('SELECT COUNT(*) FROM favorites').fetchone()[0]
        
        conn.close()
        
        return render_template('favorites.html', 
                             posts=posts, 
                             page=page,
                             per_page=per_page,
                             total_count=total_count)
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"数据库错误: {str(e)}"

@app.route('/analytics')
def analytics():
    """数据分析页面"""
    try:
        conn = get_db_connection()
        
        # 按子版块统计
        subreddit_stats = conn.execute('''
            SELECT subreddit, 
                   COUNT(*) as post_count,
                   AVG(score) as avg_score,
                   SUM(num_comments) as total_comments
            FROM posts 
            GROUP BY subreddit 
            ORDER BY post_count DESC
        ''').fetchall()
        
        # 按搜索模式统计
        pattern_stats = conn.execute('''
            SELECT search_pattern,
                   COUNT(*) as post_count,
                   AVG(score) as avg_score,
                   AVG(num_comments) as avg_comments
            FROM posts 
            GROUP BY search_pattern 
            ORDER BY post_count DESC
        ''').fetchall()
        
        # 时间趋势分析
        time_stats = conn.execute('''
            SELECT DATE(created_utc) as date,
                   COUNT(*) as post_count,
                   AVG(score) as avg_score
            FROM posts 
            WHERE created_utc >= datetime('now', '-30 days')
            GROUP BY DATE(created_utc)
            ORDER BY date
        ''').fetchall()
        
        # 高质量帖子（分数>=10且评论>=5）
        high_quality_posts = conn.execute('''
            SELECT * FROM posts 
            WHERE score >= 10 AND num_comments >= 5 
            ORDER BY score DESC 
            LIMIT 10
        ''').fetchall()
        
        conn.close()
        
        return render_template('analytics.html',
                             subreddit_stats=subreddit_stats,
                             pattern_stats=pattern_stats,
                             time_stats=time_stats,
                             high_quality_posts=high_quality_posts)
    
    except Exception as e:
        return f"数据库错误: {e}"

@app.route('/api/search')
def api_search():
    """搜索API"""
    try:
        conn = get_db_connection()
        
        keyword = request.args.get('q', '')
        limit = request.args.get('limit', 50, type=int)
        
        if not keyword:
            return jsonify([])
        
        posts = conn.execute('''
            SELECT * FROM posts 
            WHERE title LIKE ? OR content LIKE ?
            ORDER BY score DESC 
            LIMIT ?
        ''', (f'%{keyword}%', f'%{keyword}%', limit)).fetchall()
        
        conn.close()
        
        # 转换为JSON格式
        result = []
        for post in posts:
            result.append({
                'id': post['id'],
                'title': post['title'],
                'content': post['content'][:200] + '...' if len(post['content']) > 200 else post['content'],
                'score': post['score'],
                'num_comments': post['num_comments'],
                'subreddit': post['subreddit'],
                'url': post['url']
            })
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/scrape', methods=['GET', 'POST'])
def scrape():
    """爬取数据页面"""
    if request.method == 'GET':
        return render_template('scrape.html')

    if request.method == 'POST':
        try:
            # 获取表单数据
            subreddits = request.form.getlist('subreddits')
            patterns = request.form.getlist('patterns')
            limit = int(request.form.get('limit', 50))
            time_filter = request.form.get('time_filter', 'month')
            get_comments = request.form.get('get_comments', 'false') == 'true'

            # 验证参数
            if not subreddits:
                return jsonify({'success': False, 'error': '请至少选择一个子版块'})

            if not patterns:
                return jsonify({'success': False, 'error': '请至少选择一个搜索关键词'})

            # 获取Reddit API配置
            client_id = os.getenv("CLIENT_ID")
            client_secret = os.getenv("CLIENT_SECRET")
            proxy_url = os.getenv("PROXY_URL")

            if not client_id or not client_secret:
                return jsonify({
                    'success': False,
                    'error': '缺少Reddit API配置，请检查环境变量CLIENT_ID和CLIENT_SECRET'
                })

            # 初始化爬虫
            scraper = RedditScraper(
                client_id=client_id,
                client_secret=client_secret,
                user_agent="SaaSOpportunityFinder/1.0",
                proxy_url=proxy_url
            )

            # 临时修改搜索模式，只使用用户选择的
            original_patterns = scraper.search_patterns
            scraper.search_patterns = patterns

            # 开始爬取
            posts = scraper.search_posts(
                subreddit_names=subreddits,
                limit=limit,
                time_filter=time_filter
            )

            comments = []
            if get_comments and posts:
                # 获取前20个帖子的评论
                post_ids = [post['id'] for post in posts[:20]]
                comments = scraper.get_comments(post_ids, max_comments=30)

            # 恢复原始搜索模式
            scraper.search_patterns = original_patterns

            # 保存数据
            if posts:
                posts_file, json_file = scraper.save_to_files(posts, comments)

                return jsonify({
                    'success': True,
                    'posts_count': len(posts),
                    'comments_count': len(comments),
                    'files': [posts_file, json_file] if json_file else [posts_file]
                })
            else:
                return jsonify({
                    'success': False,
                    'error': '未找到匹配的帖子，请尝试调整搜索条件'
                })

        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': f'爬取过程中发生错误: {str(e)}'
            })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)