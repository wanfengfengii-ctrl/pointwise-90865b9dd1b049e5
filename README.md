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
