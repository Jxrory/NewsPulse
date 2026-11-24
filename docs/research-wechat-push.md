# 历史资料：个人微信推送通道核实记录

> 已被2026-11-24用户最新决定替代：NewsPulse改用飞书通知。本文只保留此前研究依据，不属于当前实现范围；当前方案见 [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md)，通知协议见 [research-feishu-push.md](research-feishu-push.md)。

核实日期：2026-11-24（Asia/Shanghai）。仅查阅官方公开文档及官方网页加载的前端源码；未登录账户、未使用凭据、未发送测试消息。额度和付费规则会变化，实施与上线时仍应检查账户实际可用通道和配额。

## 建议结论

个人每天接收一份新闻简报，优先使用 **Server酱 Turbo 的微信服务号通道**；后台只需发起 HTTPS 请求，不必部署微信机器人、公众号回调服务器或额外消息队列。PushPlus 可作为另一种可配置实现，但第一阶段只启用一个通道，避免重复通知。

这里的接收体验是微信里的服务号通知卡片，点击查看新闻简报详情，不应承诺像联系人发聊天文本一样直接展示完整日报。Server酱通道页说明免费卡片仅显示标题，且服务号通道不支持一般通道的展开内容体验；PushPlus 的默认模板也不直接显示正文。来源：[Server酱通道说明](https://sct.ftqq.com/docs/getting-started/channels/)、[PushPlus 正文显示说明](https://www.pushplus.plus/doc/help/showmessage.html)。

## Server酱 Turbo

### 已核实的请求协议

```http
POST https://sctapi.ftqq.com/{SendKey}.send
Content-Type: application/x-www-form-urlencoded

title=...&desp=...
```

- 使用以 `SCT` 开头的 Turbo SendKey；以 `sctp` 开头的是另一产品 Server酱³，不能套用 Turbo 端点。
- `title` 必填，不能换行；`desp` 是可选的 Markdown 正文。
- Go 可直接用标准库 `http.Client`、`url.Values.Encode()`；官方也有 Go SDK。项目侧仍需自己实现超时、脱敏日志、状态落库与重试策略。
- 凭据从环境变量读取；SendKey 在 URL 路径中，日志和 HTTP 错误输出必须脱敏整个含密钥的 URL。

来源：[SendKey 入门](https://sct.ftqq.com/docs/getting-started/sendkey/)、[Go 官方教程](https://sct.ftqq.com/docs/integrations/golang/)。

### 异步受理与结果查询

**业务 `code=0` 表示进入服务端异步队列，不等于微信已经收到或用户已阅读。** 这是官方控制台 API 说明的明确语义。保存响应中的 `data.pushid` 与 `data.readkey`，随后查询：

```text
GET https://sctapi.ftqq.com/push?id={pushid}&readkey={readkey}
```

注意查询参数叫 `id`，不是 `pushid`。响应的 `wxstatus` 是通道平台接口的返回内容；为空时可能尚未执行。官方前端自身对 `wxstatus` 做 JSON 解析，说明实现时需要防御其 JSON 字符串形态，不应把“非空”直接解释为发送成功。应按已选通道的返回结构判断；无法识别时记录 `delivery_unknown` 并保留脱敏诊断数据。

**证据来源与限制：** 普通文档页未完整展开这些接口细节；本次从 [Server酱官方控制台](https://sct.ftqq.com/sendkey/) 实际加载的[公开前端 bundle](https://fox.ftqq.com/sct/static/js/main.83836226.chunk.js)中读取到“查询推送状态”说明。bundle 是本次检索时的版本，哈希地址可能变化。未使用凭据实测查询响应。因此协议契约测试和用户本人微信验收仍是实施阶段的必要步骤。

### 长度、额度与保留期

官方控制台同一 API 说明列出 `title` 最大长度 32、`desp` 最大 32KB、`short` 最大长度 64；标题和 short 的计数单位未在该处进一步定义。项目建议保守限制标题 UTF-8 不超过 30 字节且最多 20 个 Unicode 字符、正文 UTF-8 不超过 30KiB，并在联调中确认计数规则；这是项目策略，不是新增的供应商保证。不要用字节切片截断中文。

官方 FAQ 当前列明 Turbo 免费账户每天最多 5 条、新用户 7 天全功能试用；每分钟最多 50 条；Turbo 内容免费保留 1 天、会员 3 天。新闻原始数据和最终简报必须在本地长期保存，供应商详情页不应承担档案库职能。初期可将单条简报控制在约 6,000 个汉字以内并附来源链接，超长时按预算缩短摘要；有自有阅读页后再推送摘要加链接。

来源：[官方控制台及其 bundle](https://sct.ftqq.com/sendkey/)、[额度与故障 FAQ](https://sct.ftqq.com/docs/getting-started/faq/)。本次没有确认用户账户的实际配额、是否已结束试用、通道关注状态；不能用公开配额替代上线检查。

### 账户操作

用户需自行微信扫码注册、获取 SendKey，并按控制台关注和绑定接收通道。后端无需保存微信密码，不代替用户登录个人微信。官方 FAQ 要求检查服务号未取消关注或屏蔽。通道编号从控制台读取，避免从旧教程硬编码。

来源：[SendKey 获取](https://sct.ftqq.com/docs/getting-started/sendkey/)、[通道配置](https://sct.ftqq.com/docs/getting-started/channels/)、[接收排查](https://sct.ftqq.com/docs/getting-started/faq/)。

## PushPlus

### 协议与投递语义

```http
POST https://www.pushplus.plus/send
Content-Type: application/json

{
  "token": "<从环境变量读取>",
  "title": "每日新闻简报",
  "content": "<Markdown正文>",
  "template": "markdown",
  "channel": "wechat"
}
```

`topic` 和 `to` 不填写时给自己发送。官网 API 示例部分仍写 HTTP，但[官方 HTTPS 说明](https://www.pushplus.plus/doc/help/https.html)确认支持 HTTPS，项目始终使用 HTTPS。响应业务 `code=200` 只表示受理，`data` 为消息流水号；最终发送状态需查询或接收异步回调。回调状态为 `0` 未发送、`1` 发送中、`2` 发送成功、`3` 失败；成功也不等于已阅读。来源：[消息 API](https://www.pushplus.plus/doc/guide/api.html)、[一对一说明](https://www.pushplus.plus/doc/function/one.html)。

不希望增加公网回调接口时，可用开放接口查询 `GET /api/open/message/sendMessageResult?shortCode=...`，携带 `access-key` 请求头。该凭据需先通过用户 token、secretKey 获取，并配置安全 IP；有有效期，需按返回 `expiresIn` 刷新。仅用于发消息的消息 token 不支持获取该 AccessKey。以上意味着“可发送”与“可主动查询最终结果”是两组不同配置要求。来源：[开放接口文档](https://www.pushplus.plus/doc/guide/openApi.html)。

### 账户与额度

当前主站额度表：未实名无法调用发送接口（错误码 905）；实名非会员微信渠道 200 次请求/日，会员 2,000 次；非会员每分钟 5 次，标题 100 字、正文 2 万字；会员标题 200 字、正文 10 万字。错误请求也占调用次数；相同内容 1 小时限 3 条。后台应将日请求预算留给必要重试，不应逐条新闻推送。来源：[主站系统额度](https://www.pushplus.plus/doc/guide/use.html)。

主站与 dev 子站的部分功能额度不一致，例如自有公众号绑定数量；以主站、控制台与具体账户为准。此次方案不依赖自有公众号绑定能力。用户仍需完成账号注册、实名、关注接收服务号，并测试本人微信实际收到消息；本次未替用户执行。

### 接收体验与官方文档矛盾

默认公众号模板需点击链接看正文。官网两篇说明对发送“激活消息”后的有效期分别写 24 小时和 48 小时，都说明连续最多 5 条，存在不一致；方案不能依赖定期人工激活来保证每日自动接收全文。采用默认卡片加详情即可。来源：[正文显示说明](https://www.pushplus.plus/doc/help/showmessage.html)、[激活说明](https://www.pushplus.plus/doc/help/activation.html)。

## 实现约束与验收建议（项目设计）

1. 两种适配器都区分 `pending → sending → accepted → delivered/failed/delivery_unknown`，保留供应商流水号，禁止只以 HTTP 2xx 标记完成。
2. 状态查询比重新发送更优先；超时但可能已受理时标记 `send_unknown`，避免立即重发造成微信重复通知。第三方接口没有经本次确认的幂等键保证，不能声称端到端 exactly-once。
3. 使用本地唯一键约束同一“简报、接收人、通道”的自动推送任务；失败重试需次数、退避、到期和每日预算上限。
4. 查询结果只代表通道接口发送状态；真实上线验收必须由用户在微信打开一份含中文、Markdown 与原文链接的日报确认可读，并在免费试用结束后再次确认接收。
5. 本次不提供实时价格承诺，也未确认服务可用性 SLA、新闻摘要长期展示适用性、实际账户所有限制；上线前按服务方当前规则确认。简报永久档案由年度 SQLite 提供。
