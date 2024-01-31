from dotadb.db import DotaDB
from dotacrawler.crawler import RemarkCrawler
from dot20.dot20 import Dot20
from substrateinterface import SubstrateInterface


# 索引器
class Indexer:

    def __init__(self, db: DotaDB, crawler: RemarkCrawler):
        self.db = db
        self.crawler = crawler
        # 在这里直接创建
        # fixme 协议里面对json内容做最基本的过滤
        self.dot20 = Dot20(db)
        # self.start_block = start_block
        # 支持的ticks 未来投票上
        self.supported_ticks = ["dota", "dddd"]
        # 支持的操作
        self.supported_ops = ["deploy", "mint", "transfer", "transferFrom", "approve", "memo"]
        self.owner_ticks = set("iDOT")
        # fixme 某个tick是不是owner

    def _base_filter_remarks(self, remarks: list[dict]) -> list[dict]:
        res = []
        # 要以交易为单位 如果这笔交易中有非法的memo 直接全部干掉
        rs = []
        extrinsic_index = 0
        for remark in remarks:
            if remark["memo"].get("tick") is not None and isinstance(remark["memo"].get("tick"), str):
                remark["memo"]["tick"] = str(remark["memo"].get("tick")).lower()

            if extrinsic_index != remark["extrinsic_index"]:
                for i, r in enumerate(rs):
                    memo = r["memo"]
                    if memo.get("tick") not in self.supported_ticks or memo.get("op") not in self.supported_ops:
                        break
                    # 普通mint和deploy在一个交易中只能有一个 并且不能批量
                    # fixme 判断普通mint
                    if (memo.get("op") == "mint" and memo.get("tick") not in list(self.owner_ticks)) or \
                            memo.get("op") == "deploy":
                        if len(rs) > 1:
                            break
                    # memo只能在最后
                    if memo.get("op") == "memo":
                        if i != len(rs) - 1:
                            break
                    # todo json字段是否合法
                else:
                    res.extend(rs)
                    rs = []
                extrinsic_index = remark["extrinsic_index"]

            else:
                rs.append(remark)
        return res

    def _classify_remarks(self, remarks: list[dict]):
        unique_user = dict()
        mint_remarks = dict()
        extrinsic_index = 0
        res = []
        rs = []
        for remark in remarks:
            if extrinsic_index != remark["extrinsic_index"]:
                # 找出合法的mint
                if len(rs) == 1:
                    memo = rs[0].get("memo")
                    # 每个用户只能发一笔
                    user = rs[0].get("origin")
                    tick = str(memo.get("tick"))
                    if memo.get("op") == "mint" and memo.get("tick") not in list(self.owner_ticks):
                        vail_mint_user = unique_user.get(tick) if unique_user.get(tick) is not None else []
                        if user not in vail_mint_user:
                            mint_remarks[tick] = [remark] if mint_remarks.get(tick) is None else \
                                mint_remarks[tick].append(remark)
                            unique_user[tick] = vail_mint_user.append(user)
                        rs = []
                extrinsic_index = remark["extrinsic_index"]
                res.extend(rs)
                rs = []
            else:
                rs.append(remark)

        return mint_remarks, res

    def do_mint(self, remarks_dict: dict):
        for item, value in remarks_dict.items():
            # 查询是否是fair模式
            # 如果是fair模式 查询每个区块奖励的金额
            # 计算每个人获得的金额
            # 修改remark中的lim参数
            # 提交给dot-20处理
            pass

    def do_other_ops(self, remarks: list[dict]):
        # 根据op去调用dot-20里面的各个操作
        # 一个batchall一个batchall去执行
        pass

    # 匹配dot20 然后选择操作执行 这个方法在batch里
    def _execute_remarks_by_per_batchall(self, remaks: list[dict]):
        base_filter_res = self._base_filter_remarks(remaks)
        mint_remarks, other_remarks = self._classify_remarks(base_filter_res)
        self.do_mint(mint_remarks)
        self.do_other_ops(other_remarks)

    def run(self):
        while True:
            latest_block_hash = self.crawler.substrate.get_chain_finalised_head()
            latest_block_num = self.crawler.substrate.get_block_number(latest_block_hash)
            if self.crawler.start_block + self.crawler.delay <= latest_block_num:
                print(f"开始爬取区块高度为#{self.crawler.start_block}的extrinsics")
                remarks = self.crawler.get_dota_remarks_by_block_num(1)
                self._execute_remarks_by_per_batchall(remarks)
                self.crawler.start_block += 1


if __name__ == "__main__":
    url = "wss://rect.me"
    substrate = SubstrateInterface(
        url=url,
    )
    delay = 2
    crawler = RemarkCrawler(substrate, delay, 273115)
    url = 'mysql+mysqlconnector://root:116000@localhost/wjy'
    db = DotaDB(url)
    indexer = Indexer(db, crawler)
    indexer.run()
    # crawler.crawl()
    pass




