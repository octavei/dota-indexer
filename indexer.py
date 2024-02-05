import json

from dotadb.db import DotaDB
from dotacrawler.crawler import RemarkCrawler
from dot20.dot20 import Dot20
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException
from typing import Dict
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import time

load_dotenv()


def connect_substrate() -> SubstrateInterface:
    try:
        url = os.getenv("URL")
        substrate = SubstrateInterface(
            url=url,
        )
        print("连接上节点: {}".format(url))
        print(f"chain: {substrate.chain}, format: {substrate.ss58_format}, token symbol: {substrate.token_symbol}")
        if substrate.chain != os.getenv("CHAIN"):
            raise Exception(f"连接的节点不是{os.getenv('CHAIN')}")
    except Exception as e:
        print(f"连接失败 {e}，正在重试。。。")
        time.sleep(3)
        return connect_substrate()
    return substrate


# 索引器
class Indexer:

    def __init__(self, db: DotaDB, crawler: RemarkCrawler):
        self.db = db
        self.crawler = crawler
        self.dot20 = Dot20(db, self.crawler.substrate.ss58_format)
        self.supported_ticks = [ascii("dota"), ascii("dddd"), ascii("idot")]
        # 支持的操作
        self.deploy_op = "deploy"
        self.mint_op = "mint"
        self.transfer_op = "transfer"
        self.transfer_from_op = "transferFrom"
        self.approve_op = "approve"
        self.memo_op = "memo"
        self.supported_ops = [self.deploy_op, self.mint_op, self.transfer_op, self.transfer_from_op, self.approve_op,
                              self.memo_op]
        # mint 模式
        self.fair_mode = "fair"
        self.owner_mode = "owner"
        self.normal_mode = "normal"
        self.ticks_mode = {"dota": self.fair_mode}

    # 过滤在crawler中获得的p为dot20的remarks
    # 1. 过滤掉不合法的op和tick（不在支持的op和tick中）
    # 2. mint（normal和fair模式)和deploy在一个交易中只能有一个 并且不能批量 并且不能包含其他op
    # 3. memo字段必须在最后一个
    # 4. json字段必须合法
    # 5. tick字段必须用ascii表示
    def _base_filter_remarks(self, remarks: list[dict]) -> list[dict]:
        res = []
        es = []
        extrinsic_index = 0 if len(remarks) == 0 else remarks[0]["extrinsic_index"]
        for remark_id, remark in enumerate(remarks):
            if remark["memo"].get("tick") is not None and isinstance(remark["memo"].get("tick"), str):
                # ascii防特殊字符攻击
                remark["memo"]["tick"] = ascii(remark["memo"].get("tick")).lower()

            if remark["extrinsic_index"] == extrinsic_index:
                es.append(remark)

            if extrinsic_index != remark["extrinsic_index"] or remark_id == len(remarks) - 1:
                print("正在处理区块高度 {} 的第 {} 个交易".format(remark["block_num"], extrinsic_index))
                bs = []
                btach_all_index = 0 if len(es) == 0 else es[0]["batchall_index"]
                for i, r in enumerate(es):
                    if btach_all_index == r["batchall_index"]:
                        bs.append(r)

                    if btach_all_index != r["batchall_index"] or i == len(es) - 1:
                        print("正在处理 第 {} 个batchall".format(btach_all_index))
                        print("bs:", bs)
                        # bs_cp = bs.copy()
                        is_vail_mint_or_deploy = True
                        for b_i, b in enumerate(bs):
                            memo = b["memo"]
                            if self.ticks_mode.get(memo.get("tick")) is None:
                                deploy_info = self.dot20.get_deploy_info(memo.get("tick"))
                                if deploy_info is None:
                                    print(f"{memo.get('tick')} 还没有部署")
                                    # break
                                else:
                                    self.ticks_mode[memo.get("tick")] = deploy_info.get("mode")

                            # tick和op不规范 直接退出
                            if memo.get("tick") not in self.supported_ticks or memo.get(
                                    "op") not in self.supported_ops:
                                print(memo.get("tick"), memo.get(
                                    "op"))
                                print("非法op和tick， 抛弃整个batchall")
                                break

                            # 普通mint和deploy在一个交易中只能有一个 并且不能批量
                            if (memo.get("op") == self.memo_op and self.ticks_mode.get(
                                    memo.get("tick")) != self.owner_mode) or \
                                    memo.get("op") == self.deploy_op:
                                if len(es) > 1:
                                    is_vail_mint_or_deploy = False
                                    print("非法的普通mint和deploy， 抛弃整个交易")
                                    break

                            try:
                                b_cp = b.copy()
                                b_cp["memo"] = json.dumps(b["memo"])
                                self.dot20.fmt_json_data(memo.get("op"), **b_cp)
                            except Exception as e:
                                print(f"json {b} 错误 err: {e}")
                                break

                            if memo.get("op") == "memo" and len(bs) > 1:
                                if b_i != len(bs) - 1:
                                    print(b_i, len(bs) - 1)
                                    print("memo不在最后位置， 抛弃整个batchall")
                                    break
                                # 合法memo字段
                                else:
                                    # 获取op为memo的text字段
                                    memo_remark = bs[-1]["text"]
                                    # for i in range(len(bs) - 1):
                                    bs[0]["memo_remark"] = memo_remark
                                    bs = bs[:-1]
                            elif memo.get("op") == self.memo_op and len(bs) == 1:
                                print("只有一个memo字段， 抛弃整个batchall")
                                break
                            else:
                                pass

                        else:
                            print(f"batchall过滤成功 :{bs}")
                            res.extend(bs)
                        bs = []
                        btach_all_index = r["batchall_index"]
                        if is_vail_mint_or_deploy is False:
                            print("非法mint交易")
                            break
                es = []
                extrinsic_index = remark["extrinsic_index"]
        return res

    # 对remarks进行基本的分类
    # 1. 分类出合法的mint（normal、fair模式）remarks
    # 2. 分类出合法的deploy remarks
    # 3. 分类出其他remarks
    # 4. 一个区块中，一个人只能提交一笔mint(fair和normal模式) remark（不管是不是代理或者多签)
    def _classify_remarks(self, remarks: list[dict]) -> (Dict[str, list], list[dict], list[dict]):
        unique_user: Dict[str, list[str]] = {}
        mint_remarks: Dict[str, list[dict]] = {}
        extrinsic_index = 0 if len(remarks) == 0 else remarks[0]["extrinsic_index"]
        res = []
        rs = []
        deploy_remarks = []
        for remark_id, remark in enumerate(remarks):
            if extrinsic_index == remark["extrinsic_index"]:
                rs.append(remark)

            if extrinsic_index != remark["extrinsic_index"] or remark_id == len(remarks) - 1:
                # 找出合法的mint
                if len(rs) == 1:
                    memo = rs[0].get("memo")
                    # 每个用户只能发一笔
                    user = rs[0].get("origin")
                    tick = str(memo.get("tick"))
                    if memo.get("op") == self.mint_op and self.ticks_mode.get(memo.get("tick")) != self.owner_mode:
                        vail_mint_user = unique_user.get(tick) if unique_user.get(tick) is not None else []
                        if user not in vail_mint_user:
                            print(f"分类得到合法mint ： {remark}")
                            mint_remarks[tick] = [remark] if mint_remarks.get(tick) is None else \
                                mint_remarks[tick].append(remark)
                            unique_user[tick] = vail_mint_user.append(user)
                        else:
                            print(f"用户 {user} 在本区块中已经提交mint")
                        rs = []
                    if memo.get("op") == self.deploy_op:
                        deploy_remarks.append(remark)
                        rs = []
                extrinsic_index = remark["extrinsic_index"]
                res.extend(rs)
                rs = []
        print("分类后的mint交易为:", mint_remarks)
        print("分类后的deploy交易为:", deploy_remarks)
        print("分类后的其他交易为:", res)
        return mint_remarks, deploy_remarks, res,

    # 执行deploy操作
    # 1. deploy优先执行，因为同一个事务中的deploy操作会产生新表，不能跟其他操作一起
    # 2. deploy操作一个一个执行（不会批量），直到全部执行完
    # 3. 每一个tick deploy结束，会创建与tick相对应的表格
    def _do_deploy(self, deploy_remarks: list[dict]):
        print("deploy_remarks: ", deploy_remarks)
        for item in deploy_remarks:
            try:
                with self.db.session.begin():
                    memo = item["memo"]
                    if memo.get("op") != self.deploy_op:
                        raise Exception(f"{memo} 非法进入不属于自己的代码块")
                    tick = self.dot20.deploy(**item)
                    self.db.create_tables_for_new_tick(tick)
                self.db.session.commit()
            except SQLAlchemyError as e:
                print(f"deploy: {item}操作失败：{e}")
                raise e
            except Exception as e:
                print(f"deploy: {item}操作失败：{e}")

    # 执行mint（fair、normal）操作
    # 1. 如果是fair模式，会计算平均值
    # 2. mint操作有非sql失败，直接continue
    # 3. mint操作有sql失败，直接break（并且所有操作回滚)
    def _do_mint(self, remarks_dict: Dict[str, list]):
        print("mint_remarks: ", remarks_dict)
        for item, value in remarks_dict.items():
            # try:
            deploy_info = self.db.get_deploy_info(item)
            if len(deploy_info) == 0:
                raise Exception(f"{item}还没有部署")
            print("deploy_info: ", deploy_info)
            mode = deploy_info[0][11]
            av_amt = 0
            if mode == self.fair_mode:
                amt = deploy_info[0][12]
                av_amt = int(amt) / len(value)
            for v_id, v in enumerate(value):
                try:
                    with self.db.session.begin_nested():
                        memo = v["memo"]
                        if mode == self.fair_mode:
                            memo["lim"] = av_amt
                        print("mint memo:", memo)
                        v["memo"] = json.dumps(memo)
                        self.dot20.mint(**v)

                except SQLAlchemyError as e:
                    print(f"mint: {v}操作失败：{e}")
                    raise e
                except Exception as e:
                    print(f"mint: {v}操作失败：{e}")

    # 执行其他操作
    # 1. 其他操作包括：transfer, transferFrom, approve, mint（owner）
    # 2. 以batchall为单位，去执行。batchall内必须批量原子操作。batchall外失败会continue
    def _do_other_ops(self, remarks: list[dict]):
        es = []
        extrinsic_index = 0 if len(remarks) == 0 else remarks[0]["extrinsic_index"]
        for remark_id, remark in enumerate(remarks):

            if extrinsic_index == remark["extrinsic_index"]:
                print(remark_id, remark)
                es.append(remark)
            if extrinsic_index != remark["extrinsic_index"] or remark_id == len(remarks) - 1:
                batchall_index = 0 if len(es) == 0 else es[0]["batchall_index"]
                bs = []
                for b_id, b in enumerate(es):
                    if batchall_index == b["batchall_index"]:
                        bs.append(b)

                    if batchall_index != b["batchall_index"] or b_id == len(es) - 1:
                        try:
                            with self.db.session.begin_nested():
                                for b in bs:
                                    try:
                                        b_m = b["memo"]
                                        b["memo"] = json.dumps(b_m)
                                        if b_m.get("op") == self.deploy_op:
                                            raise Exception(f"部署操作非法进入不属于自己的代码块: {b}")
                                            # self.dot20.deploy(**b)
                                        elif b_m.get("op") == self.mint_op and self.ticks_mode.get(
                                                b_m.get("tick")) == self.owner_mode:
                                            self.dot20.mint(**b)
                                        elif b_m.get("op") == self.mint_op and self.ticks_mode.get(
                                                b_m.get("tick")) != self.owner_mode:
                                            raise Exception(f"普通mint操作非法进入不属于自己的代码块: {b}")
                                            # self.dot20.mint(**b)
                                        elif b_m.get("op") == self.transfer_op:
                                            self.dot20.transfer(**b)
                                        elif b_m.get("op") == self.approve_op:
                                            self.dot20.approve(**b)
                                        elif b_m.get("op") == self.transfer_from_op:
                                            self.dot20.transferFrom(**b)
                                        else:
                                            raise Exception(f"不支持的op操作: {b}")
                                            # print(f"不支持的op操作: {b}")
                                    except Exception as e:
                                        print(f"{b}操作失败：{e}")
                                        raise e
                        except SQLAlchemyError as e:
                            print(f"批量操作: {bs}, 执行失败 {e}")
                            raise e
                        except Exception as e:
                            print(f"批量操作: {bs}, 执行失败 {e}")

                        print(f"待执行的非mint交易: \n {bs}")
                        bs = []
                        batchall_index = b["batchall_index"]
                es = []
                extrinsic_index = remark["extrinsic_index"]

    # 执行整个区块的rremarks
    # 1. 先过滤remarks
    # 2. 分类remarks
    # 3. 执行deploy操作
    # 4. 执行mint操作
    # 5. 执行其他操作
    # 6. 更新indexer_status
    def _execute_remarks_by_per_batchall(self, remaks: list[dict]):
        base_filter_res = self._base_filter_remarks(remaks)
        mint_remarks, deploy_remarks, other_remarks = self._classify_remarks(base_filter_res)

        try:
            self.db.session.commit()
            self._do_deploy(deploy_remarks)
            with self.db.session.begin():
                self._do_mint(mint_remarks)
                self._do_other_ops(other_remarks)
                self.db.insert_or_update_indexer_status({"p": "dot-20", "indexer_height": self.crawler.start_block,
                                                         "crawler_height": self.crawler.start_block})
            self.db.session.commit()
        except Exception as e:
            print(f"整个区块的交易执行失败：{e}")
            raise e

    # 运行索引器
    # 1. 从start_block开始，每次爬取一个区块的extrinsics
    # 2. 过滤remarks
    # 3. 分类remarks
    # 4. 执行remarks操作
    def run(self):
        while True:
            try:
                latest_block_hash = self.crawler.substrate.get_chain_finalised_head()
                latest_block_num = self.crawler.substrate.get_block_number(latest_block_hash)
                if self.crawler.start_block + self.crawler.delay <= latest_block_num:
                    print(f"开始爬取区块高度为#{self.crawler.start_block} 的extrinsics")
                    remarks = self.crawler.get_dota_remarks_by_block_num(self.crawler.start_block)
                    self._execute_remarks_by_per_batchall(remarks)
                    self.crawler.start_block += 1
            except (ConnectionError, SubstrateRequestException, WebSocketConnectionClosedException,
                    WebSocketTimeoutException) as e:
                print("连接断开，正在连接。。。。")
                try:
                    self.crawler.substrate = connect_substrate()
                except Exception as e:
                    print(f"连接失败 {e}，正在重试。。。")
                time.sleep(3)


if __name__ == "__main__":
    user = os.getenv("MYSQLUSER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    database = os.getenv("DATABASE")
    db = DotaDB(db_url=f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
    # 删除整个表结构
    # db.drop_all_tick_table("dota")
    # 删除表中数据
    # db.delete_all_tick_table("dota")
    db.session.commit()
    status = db.get_indexer_status("dot-20")
    start_block = int(os.getenv("START_BLOCK")) if status is None else status[1] + 1
    print(f"开始的区块是: {start_block}")
    indexer = Indexer(db, RemarkCrawler(connect_substrate(), int(os.getenv("DELAY_BLOCK")), start_block))
    indexer.run()
