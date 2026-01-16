from constant import CREDENTIAL_FILE_PATH, TIME_FORMAT
from datetime import datetime, timedelta
import logging


logger = logging.getLogger(__name__)


def add_or_update_user(token, username, groups_list, name, is_active=1):
    """
    生产级 - 按指定格式追加用户记录（防重复+格式对齐+容错处理）
    :return: bool - 追加成功返回True，重复返回False
    """
    current_time = datetime.now().strftime(TIME_FORMAT)
    if is_user_existed(token):
        # 特别说明，离职账户不开放接口更新，仅由管理员手工更新状态
        update_user(token, "name", name)
        update_user(token, "last_login", current_time)
        logger.info(f"✅ 用户【{username}】已存在！更新最后一次登录时间")
    else:
        if not all([username, token, name]):
            logger.error(f"❌ 追加失败：用户名/Token/分组/姓名不能为空！")
            return False
    
        groups = ",".join(groups_list) if groups_list else ""
        line_content = (
            f"{username:<20} | "       # 用户名 宽度20
            f"{token:<50} | "          # Token 宽度50（适配超长Token）
            f"{groups:<20} | "     # 权限组 宽度20
            f"{name:<20} | "           # 姓名 宽度10
            f"{current_time:<30} | "   # 创建时间 宽度25
            f"{current_time:<30} | "   # 最后登录 宽度25
            f"{is_active:<5}\n"           # ✅ 核心修正2：末尾强制加\n，保证每行独立！
        )
        CREDENTIAL_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CREDENTIAL_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(line_content)
        logger.info(f"✅ 成功追加用户：{username} | 姓名：{name} | 状态：{'启用' if is_active==1 else '禁用'}")
    
    return get_user(token)


def is_user_existed(target_token):
    """ 校验用户是否有效（按 token 唯一判定）"""
    if not CREDENTIAL_FILE_PATH.exists():
        logger.error(f"❌ 用户文件不存在：{CREDENTIAL_FILE_PATH}")
        return False
    with open(CREDENTIAL_FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            clean_line = line.strip()
            if clean_line and "|" in clean_line:
                exist_token = clean_line.split("|")[1].strip()
                if exist_token == target_token:
                    return True
    return False


def is_user_valid(target_token):
    """ 严格校验用户是否有效（按 token 唯一判定）
    1. 是否在职
    2. 是否登录超过 7 天
    """
    if not CREDENTIAL_FILE_PATH.exists():
        logger.error(f"❌ 用户文件不存在：{CREDENTIAL_FILE_PATH}")
        return False
    with open(CREDENTIAL_FILE_PATH, "r", encoding="utf-8") as f:
        file_lines = f.readlines()
        for line in file_lines:
            clean_line = line.strip()
            if clean_line and "|" in clean_line:
                exist_token = clean_line.split("|")[1].strip()
                if exist_token == target_token:
                    name = clean_line.split("|")[0].strip()
                    if int(clean_line.split("|")[6]) == 0:
                        logger.warning(f"❌ 用户【{name}】已存在且账号未激活！")
                        return False
                    # 如果当前时间大于 last_login + 7 天, 返回 False
                    if datetime.now() > datetime.strptime(clean_line.split("|")[5].strip(), TIME_FORMAT) + timedelta(days=7):
                        logger.warning(f"❌ 用户【{name}】已存在且未登录超过7天！请重新登录！")
                        return False
                    return True
    logger.warning(f"未找到匹配的 target_token:{target_token}")
    return False


def update_user(target_token, column_name, new_value):
    """
    通用更新函数：根据username修改指定列的数值
    :param target_token: 要更新的用户 token（唯一标识）
    :param column_name: 要更新的列名，可选值 ↓
           ["username", "token", "groups", "name", "created_at", "last_login", "is_active"]
    :param new_value: 新值（groups传列表，is_active传0/1，时间传指定格式字符串，其余传普通字符串）
    :return: bool → True=更新成功，False=更新失败
    """
    # ===================== 1. 前置校验（必做，防脏数据）=====================
    # 列名白名单校验（限定仅支持7列）
    ALLOW_COLUMNS = ["username", "token", "groups", "name", "created_at", "last_login", "is_active"]
    if column_name not in ALLOW_COLUMNS:
        print(f"❌ 更新失败：列名错误！仅支持：{ALLOW_COLUMNS}")
        return False
    
    # 文件不存在校验
    if not CREDENTIAL_FILE_PATH.exists():
        print(f"❌ 更新失败：用户文件 {CREDENTIAL_FILE_PATH} 不存在！")
        return False
    
    # ===================== 2. 列专属数据校验+格式转换 =====================
    processed_value = new_value  # 初始化处理后的值
    # ✅ groups列：列表 → 逗号分隔字符串（适配你的文件格式）
    if column_name == "groups":
        if not isinstance(new_value, list):
            logger.error(f"❌ 更新失败：groups列必须传入【列表】，如 ['admin', 'viewer']")
            return False
        processed_value = ",".join(new_value) if new_value else ""
    
    # ✅ is_active列：强制校验0/1
    elif column_name == "is_active":
        if new_value not in [0, 1]:
            logger.error(f"❌ 更新失败：is_active列仅支持传入 0(禁用) / 1(启用)")
            return False
        processed_value = int(new_value)  # 强制转数字
    
    # ✅ 时间列：校验格式是否符合 YYYY-mm-dd: HH:mm:ss
    elif column_name in ["created_at", "last_login"]:
        try:
            datetime.strptime(new_value, TIME_FORMAT)
        except ValueError:
            logger.error(f"❌ 更新失败：{column_name}格式错误！必须是 YYYY-mm-dd: HH:mm:ss")
            return False
        processed_value = new_value
    
    # ===================== 3. 读取文件+定位用户+更新数据 =====================
    file_lines = []          # 存储文件所有行
    user_found = False       # 标记是否找到目标用户
    column_index = ALLOW_COLUMNS.index(column_name)  # 列名 → 索引位置（0-6）

    # 读取所有行（先读后写，原子性操作）
    with open(CREDENTIAL_FILE_PATH, "r", encoding="utf-8") as f:
        file_lines = f.readlines()

    # 遍历行，更新指定用户的指定列
    with open(CREDENTIAL_FILE_PATH, "w", encoding="utf-8") as f:
        for line in file_lines:
            clean_line = line.strip()
            # 过滤空行/非规范行（保持原内容不变）
            if not clean_line or "|" not in clean_line:
                f.write(line)
                continue
            
            # 分割列数据 + 去除首尾空格（适配格式：内容前后有空格的情况）
            columns = [col.strip() for col in clean_line.split("|")]
            # 校验是否为7列规范行
            if len(columns) != 7:
                f.write(line)
                continue
            
            # 匹配目标用户
            if columns[1] == target_token:
                user_found = True
                # 更新指定列的值
                columns[column_index] = str(processed_value)
                # 重新拼接行内容（还原 | 分隔格式）
                # new_line = " | ".join(columns) + "\n"
                new_line = (
                    f"{columns[0]:<20} | "
                    f"{columns[1]:<50} | "
                    f"{columns[2]:<20} | "
                    f"{columns[3]:<20} | "
                    f"{columns[4]:<30} | "
                    f"{columns[5]:<30} | "
                    f"{columns[6]:<5}\n"
                )
                f.write(new_line)
                logger.info(f"✅ 成功更新用户【{target_token}】的【{column_name}】列 → 新值：{processed_value}")
            else:
                # 非目标用户，写入原内容
                f.write(line)
    
    # ===================== 4. 结果反馈 =====================
    if not user_found:
        logger.error(f"❌ 更新失败：未找到用户【{target_token}】")
        return False
    return True


def get_user(target_token):
    """
    根据Token获取指定行的完整用户数据，以字典格式返回
    :param target_token: 待匹配的用户token值
    :return: dict | None → 匹配成功返回字典，失败返回None
    """
    # 定义列名（与文件7列顺序严格对应，固定不变）
    COLUMN_NAMES = [
        "username", "token", "groups", 
        "name", "created_at", "last_login", "is_active"
    ]
    
    # 1. 前置校验：文件是否存在
    if not CREDENTIAL_FILE_PATH.exists():
        logger.error(f"❌ 错误：用户文件 {CREDENTIAL_FILE_PATH} 不存在！")
        return None
    
    # 2. 读取文件，逐行匹配Token
    with open(CREDENTIAL_FILE_PATH, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            # 过滤空行、无效行
            clean_line = line.strip()
            if not clean_line or "|" not in clean_line:
                continue
            
            # 分割列数据并去除首尾空格（兼容列内容前后有空格的情况）
            columns = [col.strip() for col in clean_line.split("|")]
            
            # 校验是否为标准7列格式，避免数据错乱导致报错
            if len(columns) != 7:
                logger.error(f"⚠️ 警告：第{line_num}行格式异常（非7列），已跳过")
                continue
            
            # 3. 匹配目标Token（第2列是token，索引为1）
            current_token = columns[1]
            if current_token == target_token:
                # 4. 数据格式还原 + 封装为字典
                user_dict = dict(zip(COLUMN_NAMES, columns))
                # ✅ groups字符串 → 列表（还原原始格式）
                user_dict["groups"] = user_dict["groups"].split(",") if user_dict["groups"] else []
                # ✅ is_active字符串 → 整型（0/1）
                user_dict["is_active"] = int(user_dict["is_active"])
                
                logger.debug(f"✅ 成功匹配Token，已返回用户完整数据")
                return user_dict
    
    # 5. 未匹配到Token的情况
    logger.error(f"❌ 未找到Token为【{target_token}】的用户数据")
    return None
