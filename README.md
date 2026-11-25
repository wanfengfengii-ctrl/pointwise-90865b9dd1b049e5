# 振动试验测批交班站

材料试验室白班登记测批，从共用录制器接管有物理出处的整数原读。试验台提交闭录清单后，夜班质检员按成果号核对原序明细、登记量纲、合计和首执。项目使用 Python 3.11 标准库及两个相互独立的 SQLite 库。

```sh
python3 -B check_project.py
python3 -B -m intake.station demo samples
```

第一条命令执行完整 unittest 回归。第二条从 `samples/` 读取固定测批、原包及清单，使用独立临时目录运行交班，校验实测结果后输出 `POINTWISE_SCENARIO`。样例先接纳 A 的 7，再交接混载 P 的 A 尾行 11 和 B 首行 29。暂停 B 的实收时，A 先签 18。重开时先实收 Q 中的 31，B 仍待收。P 原号续办在设备回收后发生失答，B 从站内原据补齐并离线签出 60。两批按原号可领取，B 的导出继续指向 P 的第 2 条和 Q 的第 1 条。

## Python 入口

```python
from intake.station import open_station
from recorder.capture import open_recorder

with open_station('station.sqlite3') as station:
    station.register('A', 'ax', 'm/s²')
    with open_recorder('device.sqlite3', station.catalog, capacity=4) as device:
        device.record('P', [('A', 1, 'ax', 7)])
        result = station.ingest(device, 'P')
        assert result.state == 'confirmed'
    manifest = station.close_batch('A', [1])
    report = station.seal('A', manifest.manifest_id)
    assert station.claim(report.report_id) == report
    print(station.export_report(report.report_id), end='')
```

- `register(batch, channel, unit)` 返回原登记身份。批号和通道使用字母、数字、点、下划线或连字符，首位为字母或数字，最长 80 字符。量纲为两端无空白的可打印文本。异参登记、未知测批和错误通道拒绝。
- `record(packet_id, readings)` 接收非空的 `(batch, position, channel, value)` 序列。包号为两端无空白的非空可打印文本。批内行位和包内物理位置从 1 起，原读必须是整数。录制顺序由设备固定。槽满抛出 `BufferFull`，异内容复用原号抛出 `PacketIdentityConflict`，已回收或未知的原包抛出 `PacketUnavailable`。确认与查询按指定原号区分留存、已确认及未知。设备位置是已提交确认次数。显式设备容量在首次打开时固定，省略容量可沿用原设置。
- `ingest(device, packet_id)` 返回 `confirmed`、`pending`、`awaiting_source` 或 `conflict`。确认失答保留 `pending`，再次提交原号即可核认。可用 `device=None` 查询或保留站端办理状态。冲突结果附带冲突成员的物理位置，原包仍留存。无法取得且无本地续办依据的包抛出 `PacketUnavailable`，非法原包抛出 `ValueError`。可选 `before_member` 回调发生在该成员事务开始前，已提交的成员在中断后仍有效。设备可配置 `loss_after_reclaim` 在指定原包实际回收提交后丢弃首次回包。
- `close_batch(batch, positions)` 冻结有序行位和量纲。同号同序返回原清单，重复行位或更改行位及顺序拒绝。空清单表示零行交付。
- `seal(batch, manifest_id)` 核对本批清单和原实收，缺行或错据抛出 `BatchIncomplete`，清单身份不符抛出 `ManifestMismatch`。合法清单的原读按冻结顺序精确求和，保留负数、零和原物理出处。重复签发读取固定成果及首执。数据库写入失败回滚此次发布，可用原清单补办。
- `find_report_id(batch)` 返回原成果号或 `None`。`claim(report_id)` 返回固定成果并在未知原号时抛出 `UnknownReport`。`export_report(report_id)` 记录一次成功导出并返回 CSV。首条记录包含成果、批号、量纲、合计和首执，随后的 `reading` 记录按清单顺序列出原据。读值、行位和合计在 CSV 中使用十进制整数。

同一原包中，各成员的实收与保管凭据一起提交。任何成员缺据时设备保留原包。最后一条证据落地后，站端提交确认待办才请求回收。按包的设备确认和按批的签发各自可以续办。实收按批号和行位唯一，原值或出处冲突保留原实收。清单外的合法晚到行可继续入账，已签批账按原号读取原快照。两个库分别初始化，重复打开保留已有状态，站端可在设备关闭时核账、签发及领取。

## 双包合装

`recorder` 支持合装模式，细节契约见 [COMPACTION.md](COMPACTION.md)。新建设备库时显式开启，重开可省略设置沿用原配置；普通模式的原入口和行为不变。

```python
with open_recorder('device.sqlite3', station.catalog, capacity=2, compaction=True) as device:
    ...
    outcome = station.compact(device, 'op:PQ', 'P', 'Q')  # compacted / ineligible / pending
```

- `open_recorder(path, catalog, capacity=2, compaction=True)` 开启合装模式，每槽最多两条原读。重开时省略 `capacity`、`compaction` 即沿用已固定的设置；显式传入不同值抛出 `ValueError`。合装模式下录制超过两条原读的包抛出 `ValueError`。
- `station.compact(device, operation_id, left_id, right_id)` 只依据站端已经持久化、可对回原行的实收记录构造 `(原包号, 原物理位, 成员摘要)` 凭据，再经 `device.compact_packets(...)` 送给设备。设备逐项核对原包号、物理位与 `recorder.packet.member_digest` 摘要，全部相符才把两条已交副本移出并让两条待交原读合占一槽；错证返回固定的 `ineligible`，事务失败保留原件和槽位。
- 合装失答时站端返回 `pending`。用原操作号和原有序包对重开查询即领回原终局；同号改换有序包对抛出 `ValueError`。
- 合装后的取件为只含待交成员的 v2 子集编码，`read_packet` 仍按原号调用；合装前发出的完整 v1 回复（如 `DelayedRead`）继续按原编码核验。两类回复指向同一组原成员，按原成员去重，错绑或摘要不合的成员没有实收资格。
- 按原包分别实收与确认：P 的确认不清掉 Q 的待交原读，也不改变 Q 的确认资格。闭录、签发、领取入口不变。
- `python3 -B compaction_fixture.py check-v1` 保留普通模式两槽满载对照；`python3 -B compaction_fixture.py demo` 跑通合装、失答续办、独立确认并演示停源后按成果号领取。
