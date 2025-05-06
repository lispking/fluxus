use reqwest::Proxy;
use teloxide::{prelude::*, types::Recipient};

#[derive(Default, Clone)]
pub struct TelegramBot {
    bot: Option<Bot>,
    notice_id: String,
}

impl TelegramBot {
    pub fn new(token: String, notice_id: String, proxy: Option<String>) -> anyhow::Result<Self> {
        let client_builder = reqwest::Client::builder();

        let client = if let Some(proxy_url) = proxy {
            match Proxy::https(&proxy_url) {
                Ok(proxy) => client_builder.proxy(proxy).build()?,
                Err(_) => return Err(anyhow::anyhow!("Invalid proxy URL")),
            }
        } else {
            client_builder.build()?
        };

        let bot = Bot::with_client(token, client);
        Ok(Self {
            bot: Some(bot),
            notice_id,
        })
    }

    pub async fn send_message(&self, message: String) -> anyhow::Result<()> {
        self.send_message_to(&self.notice_id, message).await
    }

    pub async fn send_message_to(&self, recipient: &str, message: String) -> anyhow::Result<()> {
        let recipient = if recipient.starts_with('@') {
            // channel username
            Recipient::ChannelUsername(recipient.to_string())
        } else {
            // try to parse as chat id
            match recipient.parse::<i64>() {
                Ok(id) => Recipient::Id(ChatId(id)),
                Err(_) => return Err(anyhow::anyhow!("Invalid recipient format")),
            }
        };

        if let Some(bot) = self.bot.as_ref() {
            bot.send_message(recipient, message).await?;
        } else {
            return Err(anyhow::anyhow!("Bot not initialized"));
        }
        Ok(())
    }

    pub fn is_initialized(&self) -> bool {
        self.bot.is_some()
    }
}
