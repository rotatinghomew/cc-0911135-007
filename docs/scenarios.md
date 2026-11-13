# 业务演练

|情景|流与历史设置|连续覆盖要求|
|---|---|---|
|小字节块|chunkSize=1|UTF-8、CRLF、多行 data 均正确处理，心跳不落事件|
|半帧断开|cutOnce=true|未完成事件不确认，重连 Last-Event-ID 为已提交值|
|重复|duplicate=true|相同 ID 同内容只提交一次|
|冲突|duplicate=true、conflict=true|通道需处理，不能后写覆盖|
|保留窗外|floor=6|回补 1 至 5 后才能安全切回实时|
|历史缺口|missing=3、floor=6|连续确认不跨过 3，状态 needs-attention|
|慢接收方|减慢持久写入，设置小 bufferLimit|峰值不越限，无静默丢弃|
|尾部推进|补采期间 tail 增长|先有明确回补上界，再接实时，不无限追赶|
|停机恢复|写完事件但未推进游标时退出|重复到达可认定，提交顺序不乱|
