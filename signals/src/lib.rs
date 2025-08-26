use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn basic_sentiment(text: &str) -> f32 {
    const POS: [&str; 3] = ["good", "great", "up"];
    const NEG: [&str; 3] = ["bad", "down", "bear"];
    let lower = text.to_lowercase();
    let mut score = 0.0;
    for w in lower.split(|c: char| !c.is_alphanumeric()) {
        if POS.contains(&w) {
            score += 1.0;
        }
        if NEG.contains(&w) {
            score -= 1.0;
        }
    }
    score
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct NewsEvent {
    pub source: String,
    pub title: String,
    pub url: String,
    pub sentiment: f32,
    pub published_at: DateTime<Utc>,
}

pub async fn fetch_news(api_key: &str) -> Result<Vec<NewsEvent>, reqwest::Error> {
    let url = format!(
        "https://newsapi.org/v2/top-headlines?language=en&apiKey={}",
        api_key
    );
    let v: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .send()
        .await?
        .json()
        .await?;
    let mut out = Vec::new();
    if let Some(articles) = v.get("articles").and_then(|a| a.as_array()) {
        for art in articles {
            let title = art
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let source = art
                .get("source")
                .and_then(|s| s.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let url = art
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let published_at = art
                .get("publishedAt")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<DateTime<Utc>>().ok())
                .unwrap_or_else(|| Utc::now());
            let sentiment = basic_sentiment(&title);
            out.push(NewsEvent {
                source,
                title,
                url,
                sentiment,
                published_at,
            });
        }
    }
    Ok(out)
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SocialMetric {
    pub platform: String,
    pub message_rate: f32,
    pub sentiment: f32,
}

pub async fn fetch_reddit_metric(sub: &str) -> Result<SocialMetric, reqwest::Error> {
    let url = format!("https://www.reddit.com/r/{}/new.json?limit=100", sub);
    let v: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "aiarbitrage")
        .send()
        .await?
        .json()
        .await?;
    let mut rate = 0f32;
    let mut sentiment = 0f32;
    if let Some(children) = v
        .get("data")
        .and_then(|d| d.get("children"))
        .and_then(|c| c.as_array())
    {
        rate = children.len() as f32;
        for c in children {
            if let Some(title) = c
                .get("data")
                .and_then(|d| d.get("title"))
                .and_then(|t| t.as_str())
            {
                sentiment += basic_sentiment(title);
            }
        }
    }
    Ok(SocialMetric {
        platform: "reddit".into(),
        message_rate: rate,
        sentiment,
    })
}

pub async fn fetch_twitter_metric(
    bearer: &str,
    query: &str,
) -> Result<SocialMetric, reqwest::Error> {
    let url = format!(
        "https://api.twitter.com/2/tweets/search/recent?max_results=10&query={}",
        query
    );
    let v: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .bearer_auth(bearer)
        .send()
        .await?
        .json()
        .await?;
    let rate = v
        .get("meta")
        .and_then(|m| m.get("result_count"))
        .and_then(|c| c.as_u64())
        .unwrap_or(0) as f32;
    let mut sentiment = 0f32;
    if let Some(data) = v.get("data").and_then(|d| d.as_array()) {
        for t in data {
            if let Some(text) = t.get("text").and_then(|s| s.as_str()) {
                sentiment += basic_sentiment(text);
            }
        }
    }
    Ok(SocialMetric {
        platform: "twitter".into(),
        message_rate: rate,
        sentiment,
    })
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DevActivity {
    pub repo: String,
    pub stars: u32,
    pub forks: u32,
    pub open_issues: u32,
}

pub async fn fetch_github_activity(repo: &str) -> Result<DevActivity, reqwest::Error> {
    let url = format!("https://api.github.com/repos/{}", repo);
    let v: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "aiarbitrage")
        .send()
        .await?
        .json()
        .await?;
    Ok(DevActivity {
        repo: repo.to_string(),
        stars: v
            .get("stargazers_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32,
        forks: v.get("forks_count").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
        open_issues: v
            .get("open_issues_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32,
    })
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageMetric {
    pub name: String,
    pub value: f32,
}

pub async fn fetch_google_trends(keyword: &str) -> Result<UsageMetric, reqwest::Error> {
    let _ = keyword;
    Ok(UsageMetric {
        name: keyword.into(),
        value: 0.0,
    })
}

pub async fn fetch_app_usage(url: &str, name: &str) -> Result<UsageMetric, reqwest::Error> {
    let v: serde_json::Value = reqwest::Client::new().get(url).send().await?.json().await?;
    let value = v.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
    Ok(UsageMetric {
        name: name.into(),
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{mappers::*, responders::*, Expectation, Server};

    #[tokio::test]
    async fn sentiment_basic() {
        assert!(basic_sentiment("good") > 0.0);
        assert!(basic_sentiment("bad") < 0.0);
    }

    #[tokio::test]
    async fn fetch_app_usage_works() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/metric"))
                .respond_with(json_encoded(serde_json::json!({"value": 2.0}))),
        );
        let metric = fetch_app_usage(&server.url("/metric").to_string(), "app")
            .await
            .unwrap();
        assert_eq!(metric.value, 2.0);
    }
}
