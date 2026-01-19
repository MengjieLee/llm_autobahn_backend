from context.constant import CREDENTIAL_FILE_PATH, TIME_FORMAT
from datetime import datetime, timedelta
import logging
import os
import shutil
import asyncio
from concurrent.futures import ThreadPoolExecutor

# ===================== 模块级配置与常量（消除魔法值，提升可维护性）=====================
logger = logging.getLogger(__name__)

# 列相关常量
ALLOW_COLUMNS = ["username", "token", "groups", "name", "created_at", "last_login", "is_active"]
COLUMN_NAMES = ALLOW_COLUMNS  # 与ALLOW_COLUMNS保持一致，简化后续映射
STANDARD_COLUMN_COUNT = 7
TOKEN_COLUMN_INDEX = 1
IS_ACTIVE_COLUMN_INDEX = 6
LAST_LOGIN_COLUMN_INDEX = 5

# 业务常量
USER_LOGIN_VALID_DAYS = 7
IS_ACTIVE_VALID_VALUES = (0, 1)

# 格式对齐常量（保留原有格式，统一维护）
COLUMN_WIDTHS = {
    "username": 20,
    "token": 50,
    "groups": 20,
    "name": 20,
    "created_at": 30,
    "last_login": 30,
    "is_active": 5
}

# 全局线程池（复用线程，避免频繁创建销毁，提升异步性能）
THREAD_POOL_EXECUTOR = ThreadPoolExecutor(max_workers=4)

# ===================== 同步文件I/O工具函数（仅封装阻塞I/O，无业务逻辑）=====================
def _sync_file_exists(file_path) -> bool:
    """同步判断文件是否存在"""
    return file_path.exists()

def _sync_mkdir_parent(file_path) -> None:
    """同步创建文件父目录"""
    file_path.parent.mkdir(parents=True, exist_ok=True)

def _sync_read_file_lines(file_path) -> list[str]:
    """同步读取文件所有行"""
    with open(file_path, "r", encoding="utf-8") as f:
        return f.readlines()

def _sync_append_file_line(file_path, line_content) -> None:
    """同步追加一行内容到文件"""
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(line_content)

def _sync_write_file_lines(file_path, lines) -> None:
    """同步写入多行内容到文件"""
    with open(file_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

def _sync_move_file(src, dst) -> None:
    """同步移动文件（原子操作，用于替换原文件）"""
    shutil.move(src, dst)

def _sync_remove_file(file_path) -> None:
    """同步删除文件（若存在）"""
    if file_path.exists():
        os.remove(file_path)

# ===================== 公共辅助函数（无阻塞I/O，保持同步）=====================
def _parse_user_file_line(line: str) -> list | None:
    """
    解析用户文件单行内容，返回标准化列数据（None表示无效行）
    """
    clean_line = line.strip()
    if not clean_line or "|" not in clean_line:
        return None
    columns = [col.strip() for col in clean_line.split("|")]
    if len(columns) != STANDARD_COLUMN_COUNT:
        logger.warning(f"⚠️ 警告：文件行格式异常（非{STANDARD_COLUMN_COUNT}列），已跳过：{clean_line[:50]}...")
        return None
    return columns

async def _find_user_by_token(target_token: str) -> tuple[list | None, list | None]:
    """
    异步：根据Token查找用户，返回用户列数据和所有文件行
    核心：将文件I/O移至线程池，避免阻塞事件循环
    """
    # 异步判断文件是否存在
    try:
        file_exists = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL_EXECUTOR,
            _sync_file_exists,
            CREDENTIAL_FILE_PATH
        )
    except Exception as e:
        logger.error(f"❌ 异步判断文件存在失败：{str(e)}")
        return None, None
    
    if not file_exists:
        logger.error(f"❌ 用户文件不存在：{CREDENTIAL_FILE_PATH}")
        return None, None
    
    # 异步读取文件所有行
    try:
        file_lines = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL_EXECUTOR,
            _sync_read_file_lines,
            CREDENTIAL_FILE_PATH
        )
    except Exception as e:
        logger.error(f"❌ 异步读取用户文件失败：{str(e)}")
        return None, None
    
    # 解析行数据（无I/O，同步执行）
    target_columns = None
    for line in file_lines:
        columns = _parse_user_file_line(line)
        if not columns:
            continue
        if columns[TOKEN_COLUMN_INDEX] == target_token:
            target_columns = columns
            break
    
    return target_columns, file_lines

# ===================== 核心业务函数（改造为async协程，保持业务逻辑不变）=====================
async def add_or_update_user(token: str, username: str, groups_list: list, name: str, is_active: int = 1) -> dict | None:
    """
    生产级 - 按指定格式追加用户记录（防重复+格式对齐+容错处理）
    异步改造：文件I/O通过线程池执行，支持await调用
    :return: 用户字典 | None
    """
    now = datetime.now()
    current_time = now.strftime(TIME_FORMAT)
    
    # 异步判断用户是否存在
    if await is_user_existed(token):
        # 异步更新用户信息（调用异步update_user）
        await update_user(token, "name", name)
        await update_user(token, "last_login", current_time)
        logger.info(f"👤 用户【{username}】已存在！更新最后一次登录时间")
    else:
        if not all([username, token, name]):
            logger.error(f"❌ 追加失败：用户名/Token/姓名不能为空！")
            return None
        
        # 处理权限组格式（原有逻辑不变）
        groups = ",".join(groups_list) if groups_list else ""
        
        # 构造行内容（保留原有格式对齐）
        line_content = (
            f"{username:<{COLUMN_WIDTHS['username']}} | "
            f"{token:<{COLUMN_WIDTHS['token']}} | "
            f"{groups:<{COLUMN_WIDTHS['groups']}} | "
            f"{name:<{COLUMN_WIDTHS['name']}} | "
            f"{current_time:<{COLUMN_WIDTHS['created_at']}} | "
            f"{current_time:<{COLUMN_WIDTHS['last_login']}} | "
            f"{is_active:<{COLUMN_WIDTHS['is_active']}}\n"
        )
        
        # 异步创建目录 + 异步追加写入文件
        try:
            # 异步创建父目录
            await asyncio.get_event_loop().run_in_executor(
                THREAD_POOL_EXECUTOR,
                _sync_mkdir_parent,
                CREDENTIAL_FILE_PATH
            )
            
            # 异步追加写入文件
            await asyncio.get_event_loop().run_in_executor(
                THREAD_POOL_EXECUTOR,
                _sync_append_file_line,
                CREDENTIAL_FILE_PATH,
                line_content
            )
        except Exception as e:
            logger.error(f"❌ 异步追加用户文件失败：{str(e)}")
            return None
        
        logger.info(f"👤 成功追加用户：{username} | 姓名：{name} | 状态：{'启用' if is_active==1 else '禁用'}")
    
    # 异步返回用户完整数据
    return await get_user(token)

async def is_user_existed(target_token: str) -> bool:
    """
    异步：校验用户是否存在（按token唯一判定）
    :return: bool - 存在返回True，不存在返回False
    """
    target_columns, _ = await _find_user_by_token(target_token)
    return target_columns is not None

async def is_user_valid(target_token: str) -> bool:
    """
    异步：严格校验用户是否有效（按 token 唯一判定）
    1. 是否在职
    2. 是否登录超过 7 天
    :return: bool - 有效返回True，无效返回False
    """
    target_columns, _ = await _find_user_by_token(target_token)
    if not target_columns:
        logger.warning(f"未找到匹配的 target_token:{target_token}")
        return False
    
    # 原有业务逻辑完全保留（无I/O，同步执行）
    username = target_columns[0]
    is_active = int(target_columns[IS_ACTIVE_COLUMN_INDEX])
    last_login_str = target_columns[LAST_LOGIN_COLUMN_INDEX]
    
    # 校验账号是否激活
    if is_active == 0:
        logger.warning(f"❌ 用户【{username}】已存在且账号未激活！")
        return False
    
    # 校验登录是否过期
    try:
        last_login_time = datetime.strptime(last_login_str, TIME_FORMAT)
        if datetime.now() > last_login_time + timedelta(days=USER_LOGIN_VALID_DAYS):
            logger.warning(f"❌ 用户【{username}】已存在且未登录超过{USER_LOGIN_VALID_DAYS}天！请重新登录！")
            return False
    except ValueError as e:
        logger.error(f"❌ 用户【{username}】最后登录时间格式错误：{str(e)}")
        return False
    
    return True

async def update_user(target_token: str, column_name: str, new_value) -> bool:
    """
    异步：通用更新函数（根据token修改指定列的数值）
    保持原有业务逻辑：前置校验、数据转换、原子更新
    :return: bool → True=更新成功，False=更新失败
    """
    # ===================== 1. 前置校验（原有逻辑不变）=====================
    if column_name not in ALLOW_COLUMNS:
        logger.error(f"❌ 更新失败：列名错误！仅支持：{ALLOW_COLUMNS}")
        return False
    
    # 异步判断文件是否存在
    try:
        file_exists = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL_EXECUTOR,
            _sync_file_exists,
            CREDENTIAL_FILE_PATH
        )
    except Exception as e:
        logger.error(f"❌ 异步判断文件存在失败：{str(e)}")
        return False
    
    if not file_exists:
        logger.error(f"❌ 更新失败：用户文件 {CREDENTIAL_FILE_PATH} 不存在！")
        return False
    
    # ===================== 2. 列专属数据校验+格式转换（原有逻辑不变）=====================
    processed_value = new_value
    if column_name == "groups":
        if not isinstance(new_value, list):
            logger.error(f"❌ 更新失败：groups列必须传入【列表】，如 ['admin', 'viewer']")
            return False
        processed_value = ",".join(new_value) if new_value else ""
    elif column_name == "is_active":
        if new_value not in IS_ACTIVE_VALID_VALUES:
            logger.error(f"❌ 更新失败：is_active列仅支持传入 0(禁用) / 1(启用)")
            return False
        processed_value = int(new_value)
    elif column_name in ["created_at", "last_login"]:
        try:
            datetime.strptime(new_value, TIME_FORMAT)
        except ValueError:
            logger.error(f"❌ 更新失败：{column_name}格式错误！必须符合 {TIME_FORMAT}")
            return False
        processed_value = new_value
    
    # ===================== 3. 异步读取+更新+写入（原子操作，避免数据丢失）=====================
    column_index = ALLOW_COLUMNS.index(column_name)
    temp_file_path = CREDENTIAL_FILE_PATH.with_suffix(".tmp")
    user_found = False
    
    try:
        # 3.1 异步读取原文件所有行
        file_lines = await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL_EXECUTOR,
            _sync_read_file_lines,
            CREDENTIAL_FILE_PATH
        )
        
        # 3.2 遍历更新行数据（原有逻辑不变，无I/O）
        updated_lines = []
        for line in file_lines:
            columns = _parse_user_file_line(line)
            if not columns:
                updated_lines.append(line)
                continue
            
            if columns[TOKEN_COLUMN_INDEX] == target_token:
                user_found = True
                # 更新指定列的值
                columns[column_index] = str(processed_value)
                # 还原格式对齐（保留原有格式）
                new_line = (
                    f"{columns[0]:<{COLUMN_WIDTHS['username']}} | "
                    f"{columns[1]:<{COLUMN_WIDTHS['token']}} | "
                    f"{columns[2]:<{COLUMN_WIDTHS['groups']}} | "
                    f"{columns[3]:<{COLUMN_WIDTHS['name']}} | "
                    f"{columns[4]:<{COLUMN_WIDTHS['created_at']}} | "
                    f"{columns[5]:<{COLUMN_WIDTHS['last_login']}} | "
                    f"{columns[6]:<{COLUMN_WIDTHS['is_active']}}\n"
                )
                updated_lines.append(new_line)
                logger.info(f"👤 成功更新用户【{target_token}】的【{column_name}】列 → 新值：{processed_value}")
            else:
                updated_lines.append(line)
        
        # 3.3 异步写入临时文件
        await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL_EXECUTOR,
            _sync_write_file_lines,
            temp_file_path,
            updated_lines
        )
        
        # 3.4 异步替换原文件（原子操作）或清理临时文件
        if user_found:
            await asyncio.get_event_loop().run_in_executor(
                THREAD_POOL_EXECUTOR,
                _sync_move_file,
                temp_file_path,
                CREDENTIAL_FILE_PATH
            )
        else:
            await asyncio.get_event_loop().run_in_executor(
                THREAD_POOL_EXECUTOR,
                _sync_remove_file,
                temp_file_path
            )
            logger.error(f"❌ 更新失败：未找到用户【{target_token}】")
            return False
    
    except Exception as e:
        logger.error(f"❌ 异步更新过程中发生异常：{str(e)}，已终止更新")
        # 异常清理临时文件
        await asyncio.get_event_loop().run_in_executor(
            THREAD_POOL_EXECUTOR,
            _sync_remove_file,
            temp_file_path
        )
        return False
    
    return True

async def get_user(target_token: str) -> dict | None:
    """
    异步：根据Token获取指定行的完整用户数据，以字典格式返回
    :return: dict | None → 匹配成功返回字典，失败返回None
    """
    target_columns, _ = await _find_user_by_token(target_token)
    if not target_columns:
        logger.error(f"❌ 未找到Token为【{target_token}】的用户数据")
        return None
    
    # 数据格式还原 + 封装为字典（原有逻辑不变）
    user_dict = dict(zip(COLUMN_NAMES, target_columns))
    user_dict["groups"] = user_dict["groups"].split(",") if user_dict["groups"] else []
    user_dict["is_active"] = int(user_dict["is_active"])
    
    logger.debug(f"👤 成功匹配Token，已返回用户完整数据")
    return user_dict
