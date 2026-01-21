# test_api.py
import pytest
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from httpx import AsyncClient, ASGITransport
from app.main import app

HEADERS = {
    "Authorization": f"Bearer xx" # 这里替换为你的 token
}

# 测试异步接口是否阻塞事件循环（核心检测逻辑）
@pytest.mark.asyncio
async def test_async_endpoint_non_blocking():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", headers=HEADERS) as ac:
        # 并发请求本地接口
        task1 = ac.post("/api/v1/sql/sql_query", json={"sql": "select * from qianfan_bos_catalog.all_data.infovqa_v0 limit 10;"})
        task2 = ac.post("/api/v1/sql/sql_query", json={"sql": "select * from qianfan_bos_catalog.all_data.infovqa_v0 limit 10;"})
        
        # 等待两个并发请求完成，统计总耗时
        start_time = asyncio.get_event_loop().time()
        response1, response2 = await asyncio.gather(task1, task2)
        total_time = asyncio.get_event_loop().time() - start_time
        
        # 验证响应状态码
        assert response1.status_code == 200
        assert response2.status_code == 200
        
        # 可选：打印耗时，直观判断是否阻塞（辅助分析粒度）
        print(f"\n两个并发请求总耗时：{total_time:.2f}秒")
