from flask import Blueprint, request, jsonify
import sqlite3
from datetime import datetime

# 创建一个 Blueprint
favorites_api = Blueprint('favorites_api', __name__)

DB_PATH = "reddit_data/reddit_data.db"

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@favorites_api.route('/api/favorite', methods=['POST'])
def toggle_favorite():
    """切换帖子的收藏状态"""
    data = request.get_json()
    post_id = data.get('post_id')

    if not post_id:
        return jsonify({'success': False, 'error': '缺少 post_id'}), 400

    try:
        conn = get_db_connection()
        
        # 检查帖子是否已收藏
        existing = conn.execute('SELECT post_id FROM favorites WHERE post_id = ?', (post_id,)).fetchone()
        
        if existing:
            # 如果已收藏，则取消收藏
            conn.execute('DELETE FROM favorites WHERE post_id = ?', (post_id,))
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'status': 'unfavorited'})
        else:
            # 如果未收藏，则添加收藏
            conn.execute('INSERT INTO favorites (post_id) VALUES (?)', (post_id,))
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'status': 'favorited'})
            
    except sqlite3.IntegrityError:
        # 可能是帖子ID不存在于posts表中
        conn.close()
        return jsonify({'success': False, 'error': '帖子不存在或数据库约束失败'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
