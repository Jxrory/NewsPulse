# 飞书群 Webhook 核实记录

核实日期：2026-11-24。仅查官方公开文档；未使用凭据、未真实发送。用户已选择飞书，本项目实施范围是群自定义机器人。

## 已核实的最小契约

按[官方自定义机器人指南](https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot?lang=zh-CN)：

- `POST https://open.feishu.cn/open-apis/bot/v2/hook/{token}`，JSON。
- `post` 使用以下结构；段落是二维节点数组，链接用 `a`。
- 完整请求体上限 20KB；单租户单机器人 100 次/分钟、5 次/秒。
- 响应业务 `code=0` 成功；不要只检查 HTTP 或旧 `StatusCode`。
- 签名：秒级时间戳，距当前不超过 3600 秒；HMAC 的 key 是 `timestamp + "\n" + secret`，消息为空，SHA256 后标准 Base64。

```json
{
  "timestamp": "<Unix秒>",
  "sign": "<Base64签名>",
  "msg_type": "post",
  "content": {
    "post": {
      "zh_cn": {
        "title": "每日新闻",
        "content": [[
          {"tag":"text","text":"新闻摘要 "},
          {"tag":"a","text":"来源","href":"https://example.com/news"}
        ]]
      }
    }
  }
}
```

本次未发现该指南为 Webhook 提供投递查询 API、消息 ID 或幂等保证；这是检索范围内的结论，不是平台永久能力声明。

## 与应用机器人边界

群自定义机器人用于指定群；应用机器人支持群聊及单聊，属于另一接入路径。见[官方机器人类型说明](https://www.feishu.cn/hc/zh-CN/articles/360024984973-在群组中使用机器人)。

应用[发送消息 API](https://open.feishu.cn/document/server-docs/im-v1/message/create)需要应用权限及访问令牌，其 `uuid` 有请求去重能力；官方明确群自定义机器人不能调用该接口。因此本项目不引入 App ID、App Secret、应用访问令牌、收件人 Open ID，也不能把应用 API 的 `uuid` 去重语义套给群 Webhook。

## 项目侧实施建议（设计选择）

1. 仅配置 `FEISHU_WEBHOOK_URL`、`FEISHU_SIGN_SECRET`，均从环境读取；群内开启签名校验。Webhook 全路径含凭据，所有日志及错误输出脱敏。
2. 通过 Go 结构体生成 JSON；按最终 `json.Marshal` 后的 UTF-8 字节长度限制在 18,000 bytes，给供应商边界留余量。新闻文本用普通 `text` 节点；不把 Markdown 当富文本节点解释，不自动 @ 所有人。
3. 超长时先缩短背景与影响，再减少精选条数；永不按字节切开中文或直接截断 JSON。若要分消息，先冻结分片及每片唯一键，逐片记录状态。
4. 签名使用 `hmac.New(sha256.New, []byte(timestamp+"\n"+secret))`，不写入正文或密钥作为消息；`Sum(nil)` 后标准 Base64。每次尝试重新生成当前时间戳和签名；主机保持时钟同步。
5. 本地 outbox 唯一键约束同一简报、目标群和分片的自动发送。`HTTP 2xx + code=0` 记录 `sent`，含义限定为接口确认成功，不能表达用户已读。
6. 请求写出后超时、断连、进程中断，以及语义不明的服务端错误，转 `send_unknown`；默认不自动重发，由用户核对群消息后选择人工重发。明确未受理的限流失败才按有限退避重试；鉴权、签名、格式错误修复后再处理。项目无法保证端到端恰好一次。
7. 验收使用固定签名向量、中文长链接序列化大小、`200 + 非零code`、限流、响应丢失与进程重启场景；最后由用户在目标飞书群确认分区、中文、来源链接和手机阅读效果。不得以 mock 成功替代真实接收验收。
