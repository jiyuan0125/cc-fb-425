#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""大促晚场订单流 / 支付通知流对齐程序（可续跑、可重跑）。

输入
  spool/order/shard_NNNN   每行: 订单号,下单时间,应付金额(分)；无表头
  spool/pay/shard_NNNN     每行: 一个 JSON 对象 {order,pay_time,amount,result}
  align_state.json         {"order_cursor": N, "pay_cursor": M}，已消费完的最大片号
  orders_final.csv         已消费片得到的订单中间终值
输出（全部为纯函数式重算，写入顺序在最后一步提交游标）
  orders_final.csv         每个订单一行
  grid.csv                 2026-11-10 20:00 ~ 2026-11-11 01:59 每分钟一行
  promo_summary.txt        四行统计 + 一行 notes
  align_state.json         更新到本次消费完的最大片号（最后写，保证可重跑）

幂等性
  - 只处理片号大于游标的片，升序吃进；同一订单的通知"后到为准"。
  - grid / 汇总完全由 orders_final 重算，不做增量累加。
  - 每份产物先写同目录临时文件再 os.replace，align_state.json 最后原子写入。
    任何时刻中断，重跑都得到相同结果：要么游标未推进而重放同样的片（重放幂等），
    要么游标已推进而产物已就绪。
  - done/ 目录只由上游维护，本程序不读取、不依赖（其中的残片不影响结果）。
"""

import csv
import datetime
import json
import os
import re
import sys
import tempfile

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ORDER_DIR = os.path.join(BASE_DIR, "spool", "order")
PAY_DIR = os.path.join(BASE_DIR, "spool", "pay")
STATE_PATH = os.path.join(BASE_DIR, "align_state.json")
FINAL_PATH = os.path.join(BASE_DIR, "orders_final.csv")
GRID_PATH = os.path.join(BASE_DIR, "grid.csv")
SUMMARY_PATH = os.path.join(BASE_DIR, "promo_summary.txt")

FINAL_HEADER = ["order", "placed_at", "pay_time", "amount", "result"]
GRID_HEADER = ["minute", "orders", "pays", "pay_amt"]

# grid 覆盖的分钟窗口：2026-11-10 20:00 起共 360 分钟（到 2026-11-11 01:59）
WINDOW_START = datetime.datetime(2026, 11, 10, 20, 0)
WINDOW_MINUTES = 360

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
SHARD_RE = re.compile(r"^shard_(\d+)$")
RESULTS = ("success", "fail")


class DataError(Exception):
    """输入数据不符合约定。"""


# 诊断计数（不参与产物内容，故不影响幂等）
STATS = {"pending_order": 0, "no_placed": 0}


def warn(msg):
    print("警告: " + msg, file=sys.stderr)


def check_ts(value, where):
    """时间只允许 'YYYY-MM-DD HH:MM:SS'，固定宽度下字典序即时间序。"""
    if not isinstance(value, str) or not TS_RE.match(value):
        raise DataError("%s: 时间格式不合法: %r" % (where, value))
    return value


def read_state():
    if not os.path.exists(STATE_PATH):
        return {"order_cursor": 0, "pay_cursor": 0}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as fh:
            state = json.load(fh)
    except (OSError, ValueError) as exc:
        raise DataError("align_state.json 无法解析: %s" % exc)
    if not isinstance(state, dict):
        raise DataError("align_state.json 应为对象")
    order_cursor = int(state.get("order_cursor", 0))
    pay_cursor = int(state.get("pay_cursor", 0))
    return {"order_cursor": order_cursor, "pay_cursor": pay_cursor}


def list_shards(directory, after):
    """返回片号 > after 的片，按片号升序；片号即到达先后。"""
    if not os.path.isdir(directory):
        raise DataError("目录不存在: %s" % directory)
    found = []
    for name in os.listdir(directory):
        match = SHARD_RE.match(name)
        if not match:
            continue
        number = int(match.group(1))
        if number > after:
            found.append((number, os.path.join(directory, name)))
    return sorted(found)


def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    except OSError as exc:
        raise DataError("无法读取 %s: %s" % (path, exc))


def load_orders(rows, shards):
    """订单片：订单号,下单时间,应付金额。金额按分记的整数。"""
    consumed = None
    for number, path in shards:
        for lineno, raw in enumerate(read_text(path).splitlines(), 1):
            line = raw.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) != 3:
                raise DataError("%s:%d: 应为 3 列: %r" % (path, lineno, raw))
            order = parts[0].strip()
            placed_at = check_ts(parts[1].strip(), "%s:%d" % (path, lineno))
            amount_text = parts[2].strip()
            if not order:
                raise DataError("%s:%d: 订单号为空" % (path, lineno))
            try:
                order_amount = int(amount_text)
            except ValueError:
                raise DataError("%s:%d: 应付金额不是整数: %r"
                                % (path, lineno, amount_text))
            row = rows.get(order)
            if row is None:
                # 未收到过通知：pay_time / amount 留空，result 记 unpaid
                row = {
                    "order": order,
                    "placed_at": placed_at,
                    "pay_time": "",
                    "amount": "",
                    "result": "unpaid",
                }
                rows[order] = row
            else:
                # 重复下单记录：以较晚到达的为准（同一片内后行优先）。
                # placed_at 为空说明此前只有通知占位，不是真的重复，无需告警。
                if row["placed_at"] and row["placed_at"] != placed_at:
                    warn("订单 %s 下单时间被后到记录覆盖: %s -> %s"
                         % (order, row["placed_at"], placed_at))
                row["placed_at"] = placed_at
            # 应付金额仅用于交叉校验，终值里的 amount 取通知金额
            if row["amount"] != "" and int(row["amount"]) != order_amount:
                warn("订单 %s 的通知金额 %s 与应付金额 %d 不一致"
                     % (order, row["amount"], order_amount))
        consumed = number
    return consumed


def load_pays(rows, shards):
    """支付通知片：一行一个 JSON，后到的通知覆盖先前终值。"""
    consumed = None
    for number, path in shards:
        for lineno, raw in enumerate(read_text(path).splitlines(), 1):
            line = raw.strip()
            if not line:
                continue
            where = "%s:%d" % (path, lineno)
            try:
                item = json.loads(line)
            except ValueError as exc:
                raise DataError("%s: JSON 解析失败: %s" % (where, exc))
            if not isinstance(item, dict):
                raise DataError("%s: 应为 JSON 对象" % where)
            for key in ("order", "pay_time", "amount", "result"):
                if key not in item:
                    raise DataError("%s: 缺少键 %s" % (where, key))
            order = item["order"]
            if not isinstance(order, str) or not order.strip():
                raise DataError("%s: order 非法: %r" % (where, order))
            pay_time = check_ts(item["pay_time"], where)
            amount = item["amount"]
            if isinstance(amount, bool) or not isinstance(amount, int):
                raise DataError("%s: amount 应为整数: %r" % (where, amount))
            result = item["result"]
            if result not in RESULTS:
                raise DataError("%s: result 应为 success/fail: %r"
                                % (where, result))

            row = rows.get(order)
            if row is None:
                # 通知先于订单片出现（上游两路乱序到达）：先占位，下单时间留空，
                # 等对应订单片到达时再补 placed_at。
                rows[order] = {
                    "order": order,
                    "placed_at": "",
                    "pay_time": "",
                    "amount": "",
                    "result": "unpaid",
                }
                row = rows[order]
                STATS["pending_order"] += 1
            # 后到为准：金额、结果、支付时间都随之更新
            row["pay_time"] = pay_time
            row["amount"] = amount
            row["result"] = result
        consumed = number
    return consumed


def load_final():
    """读取已有 orders_final.csv（已消费片的中间终值）。"""
    rows = {}
    if not os.path.exists(FINAL_PATH):
        return rows
    with open(FINAL_PATH, "r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header != FINAL_HEADER:
            raise DataError("orders_final.csv 表头不符: %r" % (header,))
        for lineno, parts in enumerate(reader, 2):
            if not parts:
                continue
            if len(parts) != len(FINAL_HEADER):
                raise DataError("orders_final.csv:%d: 列数不为 5" % lineno)
            order, placed_at, pay_time, amount_text, result = parts
            if placed_at:
                check_ts(placed_at, "orders_final.csv:%d" % lineno)
            if pay_time:
                check_ts(pay_time, "orders_final.csv:%d" % lineno)
            amount = ""
            if amount_text:
                try:
                    amount = int(amount_text)
                except ValueError:
                    raise DataError("orders_final.csv:%d: amount 不是整数"
                                    % lineno)
            if result == "unpaid":
                if pay_time or amount != "":
                    raise DataError("orders_final.csv:%d: unpaid 行不应有支付信息"
                                    % lineno)
            elif result not in RESULTS:
                raise DataError("orders_final.csv:%d: result 非法: %r"
                                % (lineno, result))
            rows[order] = {
                "order": order,
                "placed_at": placed_at,
                "pay_time": pay_time,
                "amount": amount,
                "result": result,
            }
    return rows


def minute_list():
    out = []
    for index in range(WINDOW_MINUTES):
        moment = WINDOW_START + datetime.timedelta(minutes=index)
        out.append(moment.strftime("%Y-%m-%d %H:%M"))
    return out


def build_grid(rows):
    """orders 按下单时间归分钟；pays/pay_amt 只计 success 通知并按 pay_time 归分钟。"""
    minutes = minute_list()
    bucket = {minute: [0, 0, 0] for minute in minutes}
    for row in rows.values():
        placed = row["placed_at"]
        if placed:
            key = placed[:16]
            if key in bucket:
                bucket[key][0] += 1
            else:
                warn("下单时间落在 grid 窗口外，未计入: %s" % placed)
        if row["result"] == "success":
            key = row["pay_time"][:16]
            if key in bucket:
                bucket[key][1] += 1
                bucket[key][2] += int(row["amount"])
            else:
                warn("支付时间落在 grid 窗口外，未计入: %s" % row["pay_time"])
    return minutes, bucket


def build_summary(rows):
    total_orders = len(rows)
    paid_orders = 0
    paid_amt = 0
    backward_pays = 0
    for row in rows.values():
        if row["result"] == "success":
            paid_orders += 1
            paid_amt += int(row["amount"])
        if (row["pay_time"] and row["placed_at"]
                and row["pay_time"] < row["placed_at"]):
            backward_pays += 1
    return total_orders, paid_orders, paid_amt, backward_pays


def render_final(rows):
    lines = [",".join(FINAL_HEADER)]
    for order in sorted(rows):
        row = rows[order]
        amount = "" if row["amount"] == "" else str(row["amount"])
        lines.append(",".join([row["order"], row["placed_at"],
                               row["pay_time"], amount, row["result"]]))
    return "\n".join(lines) + "\n"


def render_grid(minutes, bucket):
    lines = [",".join(GRID_HEADER)]
    for minute in minutes:
        orders, pays, pay_amt = bucket[minute]
        lines.append("%s,%d,%d,%d" % (minute, orders, pays, pay_amt))
    return "\n".join(lines) + "\n"


def render_summary(total_orders, paid_orders, paid_amt, backward_pays):
    notes = (
        "notes orders按placed_at归分钟；pays与pay_amt只统计result=success的通知"
        "并按pay_time归分钟；backward_pays统计pay_time早于placed_at的已通知订单"
        "（success与fail均计）"
    )
    return "\n".join([
        "total_orders %d" % total_orders,
        "paid_orders %d" % paid_orders,
        "paid_amt %d" % paid_amt,
        "backward_pays %d" % backward_pays,
        notes,
    ]) + "\n"


def render_state(order_cursor, pay_cursor):
    return json.dumps(
        {"order_cursor": order_cursor, "pay_cursor": pay_cursor}
    ) + "\n"


def write_atomic(path, text):
    directory = os.path.dirname(path) or "."
    try:
        mode = os.stat(path).st_mode & 0o777
    except OSError:
        mode = 0o644
    handle, tmp = tempfile.mkstemp(
        dir=directory, prefix="." + os.path.basename(path) + ".", suffix=".tmp"
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main():
    STATS["pending_order"] = 0
    STATS["no_placed"] = 0
    state = read_state()
    rows = load_final()

    order_shards = list_shards(ORDER_DIR, state["order_cursor"])
    pay_shards = list_shards(PAY_DIR, state["pay_cursor"])
    order_consumed = load_orders(rows, order_shards)
    pay_consumed = load_pays(rows, pay_shards)

    order_cursor = max([state["order_cursor"]] + ([order_consumed]
                                                  if order_consumed else []))
    pay_cursor = max([state["pay_cursor"]] + ([pay_consumed]
                                              if pay_consumed else []))

    minutes, bucket = build_grid(rows)
    total_orders, paid_orders, paid_amt, backward_pays = build_summary(rows)

    final_text = render_final(rows)
    grid_text = render_grid(minutes, bucket)
    summary_text = render_summary(total_orders, paid_orders, paid_amt,
                                  backward_pays)
    state_text = render_state(order_cursor, pay_cursor)

    # 先落产物，最后提交游标；每步都是原子替换。
    write_atomic(FINAL_PATH, final_text)
    write_atomic(GRID_PATH, grid_text)
    write_atomic(SUMMARY_PATH, summary_text)
    write_atomic(STATE_PATH, state_text)

    print("消费订单片 %d 个、支付片 %d 个；游标 order=%d pay=%d"
          % (len(order_shards), len(pay_shards), order_cursor, pay_cursor))
    print("total_orders=%d paid_orders=%d paid_amt=%d backward_pays=%d"
          % (total_orders, paid_orders, paid_amt, backward_pays))
    if STATS["pending_order"]:
        print("提示: 本轮有 %d 个订单先到通知、下单记录尚未到达"
              % STATS["pending_order"])
    still = sum(1 for row in rows.values() if not row["placed_at"])
    if still:
        print("提示: 仍有 %d 个订单缺下单时间（等待订单片到达）" % still)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except DataError as exc:
        print("错误: %s" % exc, file=sys.stderr)
        sys.exit(1)
