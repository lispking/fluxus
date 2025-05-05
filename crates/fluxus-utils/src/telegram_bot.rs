use reqwest::Proxy;
use teloxide::{prelude::*, types::Recipient};

pub struct TelegramBot {
    bot: Option<Bot>,
    notice_id: String,
}

impl Default for TelegramBot {
    fn default() -> Self {
        Self {
            bot: None,
            notice_id: "".to_string(),
        }
    }
}

impl Clone for TelegramBot {
    fn clone(&self) -> Self {
        Self {
            bot: self.bot.clone(),
            notice_id: self.notice_id.clone(),
        }
    }
}
impl TelegramBot {
    pub fn new(token: String, notice_id: String, proxy: Option<String>) -> Self {
        let client_builder = reqwest::Client::builder();
        let client = if let Some(proxy_url) = proxy {
            client_builder
                .proxy(Proxy::https(&proxy_url).unwrap())
                .build()
                .unwrap()
        } else {
            client_builder.build().unwrap()
        };
        let bot = Bot::with_client(token, client);
        Self {
            bot: Some(bot),
            notice_id,
        }
    }

    pub async fn send_message(&self, message: String) -> Result<(), anyhow::Error> {
        self.send_message_to(&self.notice_id, message).await
    }

    pub async fn send_message_to(
        &self,
        recipient: &str,
        message: String,
    ) -> Result<(), anyhow::Error> {
        let recipient = if recipient.starts_with('@') {
            // 频道用户名
            Recipient::ChannelUsername(recipient.to_string())
        } else {
            // 尝试解析为聊天 ID
            match recipient.parse::<i64>() {
                Ok(id) => Recipient::Id(ChatId(id)),
                Err(_) => return Err(anyhow::anyhow!("Invalid recipient format")),
            }
        };
        self.bot
            .as_ref()
            .unwrap()
            .send_message(recipient, message)
            .await?;
        Ok(())
    }

    pub fn is_initialized(&self) -> bool {
        self.bot.is_some()
    }
}
