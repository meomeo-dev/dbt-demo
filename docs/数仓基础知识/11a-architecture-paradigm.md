---
title: "11a · 流批一体架构范式演进与 2026 现状"
series: "数据平台技术方案 · 总-分-分"
doc_id: "11a"
role: "分支 / 详细论证"
parent: "11-solution-2026.md"
date: "2026-07"
---

# 11a · 流批一体架构范式演进与 2026 现状

> 本文是 [11 总纲](11-solution-2026.md) 的**架构范式**分支。所有结论标注置信度(🟢high / 🟡medium / 🔴refuted / ⚪open),源自 deep-research 的 3 票对抗验证。
> 相关基础:[03-medallion](03-medallion.md) · [06-data-lake](06-data-lake.md) · [07-batch-computing](07-batch-computing.md) · [08-stream-computing](08-stream-computing.md)

## 1. 范式演进脉络

```
Lambda ──→ Kappa ──→ 流批一体(Unified Batch & Streaming)──→ Streaming Lakehouse
(批+流双路)  (单一流路)      (统一 API / 统一存储)              (Kappa 直接跑在湖仓上)
```

## 2. 三分类收敛(2026 业界框架)🟡

混合流批处理在 2026 收敛为三种架构模式[1]:

| 模式 | 定义 | 理想场景 | 延迟特征 | 资源 |
|---|---|---|---|---|
| **Separate Pipelines + Unified Storage** | 批/流独立管线共享存储 | 批流需求边界清晰 | 各自独立 | 中 |
| **Lambda** | 批层 + 速度层并行,服务层合并 | 重算量大、需精确批基线 | **双峰延迟**(流秒级 / 批更长窗口) | **高**(并行双路,存储/网络/计算翻倍)[1] |
| **Kappa** | 单一流式模型统一处理 | 事件即真相 | 更一致 | 单一模型,更省 |

> ⚠️ **置信度说明**:此三分类主要出自低层级开放期刊 WJAETS[1],属**一位作者的框架而非公认标准**;
> Lambda 双路资源翻倍由 Materialize 工程博客独立印证。但「Kappa 更省资源」是**典型情形归纳**——
> PB 级下 Lambda 的批存储经济性有时反而胜出,回溯还需额外吞吐余量。故标 🟡。

## 3. Lambda vs Kappa 决策框架 🟢

**保留 Lambda 当**[2]:重算量大且罕见/表导向 · 受监管需独立留痕 · 仅需短鲜度窗口 · 批治理成熟但流运维弱 · 源非事件形态。

**选 Kappa 当**[2]:事件承载持久业务语义(订单、支付、遥测、安全信号)· 日志是自然的产品边界。

> 该框架来自 AutoMQ(2026)[2],虽为 Kafka 厂商但**在多种场景主动保留 Lambda**,非自利话术,且获多个独立工程判断源印证,故 🟢。

## 4. Streaming Lakehouse:两条技术路线

- **计算层统一**:Flink/Spark 用统一 API 表达流与批。
- **存储层统一**:Paimon/Iceberg 支持流式读写 + 增量,让「一份存储」服务流批。

### 4.1 Apache Paimon:在湖仓上落地 Kappa 🟡

Paimon 让你**直接在湖仓上实现 Kappa 架构**,因直接操作对象存储文件,大规模 TB 更新可达 **~1 分钟近实时延迟**[3]。阿里云官方 Flink 文档印证 Paimon「分钟级延迟」交付下游[3]。

> 主源为 Flink 厂商博客,但结论保守(分钟级而非亚秒),且获独立云厂商官方文档印证。

### 4.2 Apache Fluss:秒级热层 + Union Reads 🟢(能力)

Fluss 提供**秒级热流层**,其 **Tiering Service** 持续把数据下沉到标准湖仓格式(Paimon/Iceberg/Lance)[4];Fluss + Paimon 暴露**统一 catalog 和单表抽象**,使 Flink 的 **Union Reads** 通过一套 API 合并实时(Fluss)与历史(Paimon)数据,**消除传统流批栈的双 catalog / 手工切换难题**[4]。

> 该能力获**阿里云官方 Flink 文档独立印证**(lake-stream unified table / union read 自动合并),故能力本身 🟢。
> ⚠️ 但 Fluss 处 **Apache 孵化(early maturity)**,「已解决」带前瞻色彩;版本行为(Fluss 0.5–0.9)须实施时二次验证。

## 5. 开放表格式在流式优先栈中依然必需 🟢

Diskless / 对象存储版 Kafka **不替代湖仓**:SQL 分析、历史快照、治理、跨引擎访问仍要求 Iceberg 类开放表格式;推荐把 curated 流 sink 到 Apache Iceberg[5]。此结论由 Kafka 厂商 AutoMQ 亲口确认(非自利),并获多方独立印证。

## 6. 被证伪断言(本角度)🔴

| 断言 | 投票 | 说明 |
|---|---|---|
| Fluss 相比 Paimon 消除 Medallion 跨层累积延迟(3min→秒级) | 1-2 | 项目博客自述,未获印证 |
| Streamhouse 是「统一流与湖仓的新范式」 | 1-2 | 营销造词,非公认范式 |
| 2026 Lambda-vs-Kappa 已纯属存储经济学问题 | 1-2 | 过度简化 |
| 流式优先引擎不适配开放表格式 | 0-3 | 与 Paimon/Iceberg+Flink 实践相悖 |

## 7. 给总纲的关键结论

- 流批一体**不等于消灭 Lambda**;按第 3 节框架分场景保留。🟢
- 湖仓上落地 Kappa 的当红组合:**Paimon(分钟级)+ 可选 Fluss(秒级热层)**。🟡/🟢
- 开放表格式是地基,流式优先栈中仍必需。🟢
- Fluss 能力被独立印证,但成熟度是主要风险。⚠️

## 参考文献

1. WJAETS-2025-0750(流批混合模式分类,低层级开放期刊)— http://wjaets.com/sites/default/files/fulltext_pdf/WJAETS-2025-0750.pdf 🟡
2. AutoMQ:Lambda vs Kappa Architecture 2026(Diskless Kafka)— https://www.automq.com/blog/lambda-vs-kappa-architecture-2026-diskless-kafka 🟢
3. Ververica:From Kappa to Streamhouse — https://www.ververica.com/blog/from-kappa-architecture-to-streamhouse-making-lakehouse-real-time ;阿里云 Flink 官方文档 https://help.aliyun.com 🟡
4. Apache Fluss:Unified Streaming Lakehouse — https://fluss.apache.org/blog/unified-streaming-lakehouse/ ;Tiering Service https://fluss.incubator.apache.org/docs/next/streaming-lakehouse/tiering-service/ 🟢
5. AutoMQ(diskless Kafka 不替代湖仓)+ 独立湖仓博客共识 🟡
