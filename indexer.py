import json

from dotadb.db import DotaDB
from dotacrawler.crawler import RemarkCrawler
from dot20.dot20 import Dot20
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException
from typing import Dict
from sqlalchemy.exc import SQLAlchemyError


# 索引器
class Indexer:

    def __init__(self, db: DotaDB, crawler: RemarkCrawler):
        self.db = db
        self.crawler = crawler
        self.dot20 = Dot20(db, self.crawler.substrate.ss58_format)
        self.supported_ticks = ["dota", "dddd"]
        # 支持的操作
        self.supported_ops = ["deploy", "mint", "transfer", "transferFrom", "approve", "memo"]
        self.fair_mode = "fair"
        self.owner_mode = "owner"
        self.normal_mode = "normal"
        self.ticks_mode = {"dota": self.fair_mode}

    def _base_filter_remarks(self, remarks: list[dict]) -> list[dict]:
        res = []
        es = []
        extrinsic_index = 0 if len(remarks) == 0 else remarks[0]["extrinsic_index"]
        for remark_id, remark in enumerate(remarks):
            if remark["memo"].get("tick") is not None and isinstance(remark["memo"].get("tick"), str):
                remark["memo"]["tick"] = str(remark["memo"].get("tick")).lower()

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

                            # 普通mint和deploy在一个交易中只能有一个 并且不能批量
                            if (memo.get("op") == "mint" and self.ticks_mode.get(memo.get("tick")) != self.owner_mode) or \
                                    memo.get("op") == "deploy":
                                if len(es) > 1:
                                    is_vail_mint_or_deploy = False
                                    print("非法的普通mint和deploy， 抛弃整个交易")
                                    break

                            if memo.get("op") == "memo":
                                if b_i != len(bs) - 1:
                                    print(b_i, len(bs) - 1)
                                    print("memo不在最后位置， 抛弃整个batchall")
                                    break
                            # tick和op不规范 直接退出
                            else:
                                if memo.get("tick") not in self.supported_ticks or memo.get(
                                        "op") not in self.supported_ops:
                                    print(memo.get("tick"), memo.get(
                                        "op"))
                                    print("非法op和tick， 抛弃整个batchall")
                                    break
                                try:
                                    b_cp = b.copy()
                                    b_cp["memo"] = json.dumps(b["memo"])
                                    self.dot20.fmt_json_data(memo.get("op"), **b_cp)
                                except Exception as e:
                                    print(f"json {b} 错误 err: {e}")
                                    break
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

    def _classify_remarks(self, remarks: list[dict]) -> (Dict[str, list], list):
        unique_user: Dict[str, list[str]] = {}
        mint_remarks: Dict[str, list[dict]] = {}
        extrinsic_index = 0 if len(remarks) == 0 else remarks[0]["extrinsic_index"]
        res = []
        rs = []
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
                    if memo.get("op") == "mint" and self.ticks_mode.get(memo.get("tick")) != self.owner_mode:
                        vail_mint_user = unique_user.get(tick) if unique_user.get(tick) is not None else []
                        if user not in vail_mint_user:
                            print(f"分类得到合法mint ： {remark}")
                            mint_remarks[tick] = [remark] if mint_remarks.get(tick) is None else \
                                mint_remarks[tick].append(remark)
                            unique_user[tick] = vail_mint_user.append(user)
                        else:
                            print(f"用户 {user} 在本区块中已经提交mint")
                        rs = []
                extrinsic_index = remark["extrinsic_index"]
                res.extend(rs)
                rs = []
        print("分类后的mint交易为:", mint_remarks)
        print("分类后的其他交易为:", res)
        return mint_remarks, res

    def _do_mint(self, remarks_dict: Dict[str, list]):
        print("mint_remarks: ", remarks_dict)
        for item, value in remarks_dict.items():
            # try:
                deploy_info = self.db.get_deploy_info(item)
                if deploy_info is None:
                    raise Exception(f"{item}还没有部署")
                mode = deploy_info[0][11]
                av_amt = 0
                if mode == "fair":
                    amt = deploy_info[0][12]
                    av_amt = int(amt) / len(value)
                for v_id, v in enumerate(value):
                    try:
                        with self.db.session.begin_nested():
                            memo = v["memo"]
                            if mode == "fair":
                                memo["lim"] = av_amt
                            print("mint memo:", memo)
                            v["memo"] = json.dumps(memo)
                            self.dot20.mint(**v)

                    except Exception as e:
                        print(f"mint: {v}操作失败：{e}")

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
                                        if b_m.get("op") == "deploy":
                                            self.dot20.deploy(**b)
                                        elif b_m.get("op") == "mint":
                                            self.dot20.mint(**b)
                                        elif b_m.get("op") == "transfer":
                                            self.dot20.transfer(**b)
                                        elif b_m.get("op") == "approve":
                                            self.dot20.approve(**b)
                                        elif b_m.get("op") == "transferFrom":
                                            self.dot20.transferFrom(**b)
                                        else:
                                            print("不支持的op操作")
                                    except Exception as e:
                                        print(f"{b}操作失败：{e}")
                                        raise e
                        except Exception as e:
                            print(f"批量操作: {bs}, 执行失败 {e}")
                            # raise e

                        print(f"待执行的非mint交易: \n {bs}")
                        bs = []
                        batchall_index = b["batchall_index"]
                es = []
                extrinsic_index = remark["extrinsic_index"]

    # 匹配dot20 然后选择操作执行 这个方法在batch里
    def _execute_remarks_by_per_batchall(self, remaks: list[dict]):
        base_filter_res = self._base_filter_remarks(remaks)
        mint_remarks, other_remarks = self._classify_remarks(base_filter_res)
        try:
            self.db.session.commit()
            with self.db.session.begin():
                self._do_mint(mint_remarks)
                self._do_other_ops(other_remarks)
                self.db.insert_or_update_indexer_status({"p": "dot-20", "indexer_height": self.crawler.start_block, "crawler_height": self.crawler.start_block})
            self.db.session.commit()
        except Exception as e:
            print(f"整个区块的交易执行失败：{e}")
            raise e

    def run(self):
        while True:
            latest_block_hash = self.crawler.substrate.get_chain_finalised_head()
            latest_block_num = self.crawler.substrate.get_block_number(latest_block_hash)
            if self.crawler.start_block + self.crawler.delay <= latest_block_num:
                print(f"开始爬取区块高度为#{self.crawler.start_block}的extrinsics")
                try:
                    remarks = self.crawler.get_dota_remarks_by_block_num(self.crawler.start_block)
                except (ConnectionError, SubstrateRequestException) as e:
                    print("连接断开，正在连接。。。。")
                    continue
                try:
                    self._execute_remarks_by_per_batchall(remarks)
                except Exception as e:
                    continue

                self.crawler.start_block += 1


if __name__ == "__main__":
    url = "wss://rect.me"
    substrate = SubstrateInterface(
        url=url,
    )
    db_url = 'mysql+mysqlconnector://root:116000@localhost/wjy'
    db = DotaDB(db_url=db_url)
    status = db.get_indexer_status("dot-20")
    start = 362153
    start_block = start if status is None else status[0][1]
    delay = 2
    crawler = RemarkCrawler(substrate, delay, start_block)

    # db.drop_all_tick_table("dota")
    indexer = Indexer(db, crawler)
    indexer.run()
    # crawler.crawl()
    pass




