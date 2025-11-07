import json

import requests
import csv
from datetime import datetime


def export_vaults_to_csv():
    # 接口URL
    url = "https://stats-data.hyperliquid.xyz/Mainnet/vaults"

    try:
        # 发送请求并获取数据
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # 检查请求是否成功
        vaults_data = response.json()
        print(f"成功获取 {len(vaults_data)} 个金库数据，开始导出到CSV...")

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return

    # CSV文件路径和文件名
    csv_file = "hyperliquid_vaults.csv"

    # 定义CSV表头
    fieldnames = [
        "金库序号", "项目名称", "金库地址", "负责人地址",
        "年化收益率(APR)", "总锁仓价值(TVL)", "项目状态",
        "关联类型", "创建时间",
        "日收益总和", "周收益总和", "月收益总和", "所有时间收益总和"
    ]

    with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()  # 写入表头

        for idx, vault in enumerate(vaults_data, 1):
            # 解析时间戳
            create_time = datetime.fromtimestamp(
                vault['summary']['createTimeMillis'] / 1000
            ).strftime("%Y-%m-%d %H:%M:%S")

            # 解析收益数据
            pnl_dict = {}
            for time_type, pnl_list in vault['pnls']:
                # 将字符串收益转为浮点数并求和
                total = round(sum(float(p) for p in pnl_list), 6)
                pnl_dict[time_type] = total

            # 构建一行数据
            row = {
                "金库序号": idx,
                "项目名称": vault['summary']['name'],
                "金库地址": vault['summary']['vaultAddress'],
                "负责人地址": vault['summary']['leader'],
                "年化收益率(APR)": vault['apr'],
                "总锁仓价值(TVL)": vault['summary']['tvl'],
                "项目状态": "已关闭" if vault['summary']['isClosed'] else "运行中",
                "关联类型": vault['summary']['relationship']['type'],
                "创建时间": create_time,
                "日收益总和": pnl_dict.get('day', 0),
                "周收益总和": pnl_dict.get('week', 0),
                "月收益总和": pnl_dict.get('month', 0),
                "所有时间收益总和": pnl_dict.get('allTime', 0)
            }

            writer.writerow(row)  # 写入一行数据

    print(f"数据导出完成！CSV文件已保存为: {csv_file}")
    print(f"文件路径: 与当前脚本在同一目录下")


def get_vault_details():
    csv_file = "hyperliquid_vaults.csv"
    vault_addresses = []
    result_rows = []  # 用于存储所有要写入CSV的结果

    # ---------- 【读取CSV文件】 ----------
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                vault_addresses.append(row["金库地址"])

        print(f"成功读取前 {len(vault_addresses)} 个金库地址，开始批量请求详情...\n")
    except FileNotFoundError:
        print(f"错误：未找到CSV文件 '{csv_file}'，请先运行导出金库列表的脚本")
        return
    except Exception as e:
        print(f"读取CSV文件失败：{e}")
        return

    # ---------- 【API配置】 ----------
    url = "https://api-ui.hyperliquid.xyz/info"
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
        "Content-Type": "application/json"
    }

    # ---------- 【字段中英文映射关系】 ----------
    field_mapping = {
        # 原有字段（保留）
        "vaultAddress": "金库地址",
        "name": "金库名称",
        "manager": "经理地址",
        "creationTime": "创建时间",
        "assets": "资产总额",
        "pnl": "累计收益",
        "roi": "投资回报率",
        "aum": "管理资产规模",
        "fees": "费用",
        "isPublic": "是否公开",
        "description": "描述",
        "performance": "业绩表现",
        "depositToken": "存款代币",
        "minDeposit": "最低存款额",
        "maxDeposit": "最高存款额",
        "leader": "领导者地址",
        "apr": "年化收益率",
        "followerState": "跟随者状态",
        "leaderFraction": "领导者分成比例",
        "leaderCommission": "领导者佣金",
        "maxDistributable": "最大可分配金额",
        "maxWithdrawable": "最大可提取金额",
        "isClosed": "是否已关闭",
        "relationship": "关联关系",
        "allowDeposits": "是否允许存款",
        "alwaysCloseOnWithdraw": "提取时是否自动平仓"
    }

    # ---------- 【遍历请求并提取字段】 ----------
    for idx, address in enumerate(vault_addresses, 1):
        print(f"===== 处理第 {idx}/{len(vault_addresses)} 个金库 =====")
        print(f"金库地址：{address}")

        data = {
            "type": "vaultDetails",
            "vaultAddress": address
        }

        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            response.raise_for_status()
            vault_detail = response.json()

            # 提取除portfolio和followers之外的字段并转换为中文键
            filtered_data = {}
            for key, value in vault_detail.items():
                if key not in ["portfolio", "followers"]:
                    # 使用映射的中文键，若没有映射则保留原键
                    chinese_key = field_mapping.get(key, key)
                    filtered_data[chinese_key] = value

            result_rows.append(filtered_data)
            print("请求成功，已提取字段并加入CSV队列\n")
            print("=" * 50 + "\n")

        except requests.exceptions.RequestException as e:
            print(f"请求失败：{e}")
            print("\n" + "=" * 50 + "\n")

    # ---------- 【将结果写入CSV文件】 ----------
    if result_rows:
        output_csv = "vault_details_filtered.csv"
        with open(output_csv, mode='w', encoding='utf-8-sig', newline='') as f:
            # 获取第一个结果的中文键作为表头
            fieldnames = result_rows[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(result_rows)
        print(f"已成功将结果写入 {output_csv}")
    else:
        print("未获取到有效数据，无法生成CSV")


import csv
import requests
import os
from datetime import datetime


def convert_timestamp(ms_timestamp):
    """将毫秒级时间戳转换为可读日期时间"""
    try:
        timestamp = int(ms_timestamp) / 1000  # 转换为秒级时间戳
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return ""  # 异常时返回空字符串，避免程序中断


def convert_timestamp(ms_timestamp):
    """将毫秒级时间戳转换为可读日期时间"""
    try:
        timestamp = int(ms_timestamp) / 1000
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return ""


def export_portfolio_data():
    csv_file = "hyperliquid_vaults.csv"
    vault_addresses = []

    # 主输出目录
    main_output_dir = "vault_portfolios"
    if not os.path.exists(main_output_dir):
        os.makedirs(main_output_dir)

    # 读取金库地址和名称（从CSV中获取，若没有则用地址前8位）
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # 优先使用CSV中的“金库名称”，若没有则用地址
                vault_name = row.get("金库名称", row["金库地址"])
                # 清理文件夹名称中的特殊字符
                invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
                for char in invalid_chars:
                    vault_name = vault_name.replace(char, '_')
                vault_addresses.append({
                    "address": row["金库地址"],
                    "name": vault_name
                })

        print(f"成功读取并筛选出 {len(vault_addresses)} 个金库，开始处理...\n")

    except FileNotFoundError:
        print(f"错误：未找到CSV文件 '{csv_file}'")
        return
    except Exception as e:
        print(f"读取CSV文件失败：{e}")
        return

    # API请求配置
    url = "https://api-ui.hyperliquid.xyz/info"
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
        "Content-Type": "application/json"
    }

    for idx, vault in enumerate(vault_addresses, 1):
        address = vault["address"]
        name = vault["name"]
        print(f"===== 处理第 {idx}/{len(vault_addresses)} 个金库 =====")
        print(f"金库名称：{name}")
        print(f"金库地址：{address}")

        # 为当前金库创建单独的文件夹
        vault_dir = os.path.join(main_output_dir, name)
        if not os.path.exists(vault_dir):
            os.makedirs(vault_dir)

        data = {
            "type": "vaultDetails",
            "vaultAddress": address
        }

        try:
            response = requests.post(url, json=data, headers=headers, timeout=10)
            response.raise_for_status()
            vault_detail = response.json()

            if "portfolio" not in vault_detail:
                print("该金库没有portfolio数据\n")
                continue

            portfolio_data = vault_detail["portfolio"]

            # 处理并导出每种时间粒度的数据
            for item in portfolio_data:
                if len(item) != 2:
                    continue

                time_granularity, metrics = item
                filename = os.path.join(vault_dir, f"{time_granularity}_portfolio.csv")

                # 处理账户价值历史
                account_value_rows = []
                if "accountValueHistory" in metrics:
                    for entry in metrics["accountValueHistory"]:
                        if len(entry) >= 2:
                            timestamp, value = entry
                            account_value_rows.append({
                                "timestamp": timestamp,
                                "datetime": convert_timestamp(timestamp),
                                "value": value,
                                "type": "account_value"
                            })

                # 处理盈亏历史
                pnl_rows = []
                if "pnlHistory" in metrics:
                    for entry in metrics["pnlHistory"]:
                        if len(entry) >= 2:
                            timestamp, value = entry
                            pnl_rows.append({
                                "timestamp": timestamp,
                                "datetime": convert_timestamp(timestamp),
                                "value": value,
                                "type": "pnl"
                            })

                # 合并并排序数据
                all_rows = account_value_rows + pnl_rows
                all_rows.sort(key=lambda x: x["timestamp"])

                # 写入CSV
                if all_rows:
                    with open(filename, mode='w', encoding='utf-8-sig', newline='') as f:
                        fieldnames = ["timestamp", "datetime", "value", "type"]
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(all_rows)
                    print(f"已导出 {time_granularity} 数据到 {filename}")

            print(f"金库 {name} 处理完成（数据已存入 {vault_dir}）\n")
            print("=" * 50 + "\n")

        except requests.exceptions.RequestException as e:
            print(f"请求失败：{e}")
            print("\n" + "=" * 50 + "\n")
        except Exception as e:
            print(f"处理数据时出错：{e}")
            print("\n" + "=" * 50 + "\n")

    print(f"金库的portfolio数据已导出，主目录：{main_output_dir}")


def send_post_request(user_address):
    """发送POST请求获取指定金库地址的交易数据"""
    url = "https://api-ui.hyperliquid.xyz/info"
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
        "Content-Type": "application/json"
    }
    payload = {
        "aggregateByTime": True,
        "startTime": 0,
        "type": "userFillsByTime",
        "user": user_address
    }

    try:
        response = requests.post(
            url=url,
            headers=headers,
            data=json.dumps(payload),
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"请求 {user_address} 失败: {e}")
        return None


def save_to_csv(trades_data, address, output_dir="vault_trades"):
    """将交易数据保存到CSV文件（中文表头）"""
    if not trades_data:
        print(f"金库 {address} 没有交易数据可保存")
        return

    # 创建输出目录（如果不存在）
    os.makedirs(output_dir, exist_ok=True)

    # 生成唯一的文件名，使用地址的前8位和后8位
    short_address = f"{address[:8]}...{address[-8:]}"
    filename = f"{output_dir}/vault_{short_address}_trades.csv"

    # 定义CSV中文表头（与原字段一一对应）
    fieldnames = [
        "币种", "价格", "数量", "方向", "时间戳", "可读时间",
        "起始仓位", "交易方向", "平仓收益", "交易哈希", "订单ID",
        "是否交叉成交", "手续费", "交易ID", "手续费代币", "TWAP ID",
        "金库地址", "是否平仓", "关联订单ID"
    ]

    with open(filename, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for trade in trades_data:
            # 转换时间戳为可读格式
            timestamp = trade['time']
            seconds = timestamp // 1000
            milliseconds = timestamp % 1000  # 提取后3位毫秒

            # 2. 转换秒为datetime对象，再格式化时拼接毫秒
            dt = datetime.fromtimestamp(seconds)
            readable_time = dt.strftime('%Y-%m-%d %H:%M:%S') + f".{milliseconds:03d}"

            # 构建中文键对应的数据字典
            trade_data = {
                "币种": trade.get("coin"),
                "价格": trade.get("px"),
                "数量": trade.get("sz"),
                "方向": trade.get("side"),
                "时间戳": trade.get("time"),
                "可读时间": readable_time,
                "起始仓位": trade.get("startPosition"),
                "交易方向": trade.get("dir"),
                "平仓收益": trade.get("closedPnl"),
                "交易哈希": trade.get("hash"),
                "订单ID": trade.get("oid"),
                "是否交叉成交": trade.get("crossed"),
                "手续费": trade.get("fee"),
                "交易ID": trade.get("tid"),
                "手续费代币": trade.get("feeToken"),
                "TWAP ID": trade.get("twapId"),
                "金库地址": address,  # 添加金库地址信息
                "是否平仓": trade.get("liquidation", None),  # 确保字段存在
                "关联订单ID": trade.get("cloid")
            }

            writer.writerow(trade_data)

    print(f"金库 {short_address} 的交易数据已保存到 {filename}，共 {len(trades_data)} 条记录")


def read_vault_addresses(csv_file="hyperliquid_vaults.csv"):
    """从CSV文件中读取所有金库地址"""
    vault_addresses = []

    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                vault_address = row.get("金库地址")
                if vault_address:
                    vault_addresses.append(vault_address)

        print(f"成功从 {csv_file} 读取 {len(vault_addresses)} 个金库地址")
        return vault_addresses

    except FileNotFoundError:
        print(f"错误: 未找到文件 {csv_file}")
        return []
    except Exception as e:
        print(f"读取金库地址时出错: {e}")
        return []


def trade_history():
    # 读取所有金库地址
    vault_addresses = read_vault_addresses()

    if not vault_addresses:
        print("没有找到金库地址，程序退出")
        return

    # 为每个金库地址发送请求并保存结果
    for i, address in enumerate(vault_addresses, 1):
        print(f"\n===== 处理第 {i} 个金库地址: {address} =====")
        response_data = send_post_request(address)

        if response_data:
            # 保存数据到CSV
            save_to_csv(response_data, address)
        else:
            print("未获取到有效数据，跳过保存")


# 执行导出
if __name__ == "__main__":
    export_vaults_to_csv()  # 所有金库信息
    get_vault_details()  # 金库详细信息
    export_portfolio_data()  # 金库账户数据
    trade_history()  # 金库交易记录
