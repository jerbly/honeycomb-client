use std::{
    collections::{HashMap, HashSet},
    env,
    fmt::{Display, Formatter},
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::stream::{self, FuturesOrdered, StreamExt};
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio;

#[derive(Debug, Clone)]
pub struct HoneyComb {
    pub api_key: String,
}
const URL: &str = "https://api.honeycomb.io/1/";
const HONEYCOMB_API_KEY: &str = "HONEYCOMB_API_KEY";

#[derive(Debug, Deserialize)]
pub struct Dataset {
    pub slug: String,
    pub last_written_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Column {
    pub id: String,
    pub key_name: String,
    pub r#type: String,
    pub description: String,
    pub hidden: bool,
    pub last_written: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct QueryResultLinks {
    query_url: String,
}

#[derive(Debug, Deserialize)]
struct QueryResult {
    links: QueryResultLinks,
}

#[derive(Debug, Deserialize)]
struct Query {
    id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NameAndSlug {
    pub name: String,
    pub slug: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Authorizations {
    pub api_key_access: HashMap<String, bool>,
    pub environment: NameAndSlug,
    pub team: NameAndSlug,
}

impl Authorizations {
    pub fn has_required_access(&self, access_types: &[&str]) -> bool {
        access_types
            .iter()
            .all(|access_type| *self.api_key_access.get(*access_type).unwrap_or(&false))
    }
}

impl Display for Authorizations {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut api_key_access = String::new();
        for (key, value) in &self.api_key_access {
            api_key_access.push_str(&format!("{}: {}\n", key, value));
        }
        write!(
            f,
            "api_key_access:\n{}\nenvironment: {}\nteam: {}",
            api_key_access, self.environment.name, self.team.name
        )
    }
}

impl HoneyComb {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            api_key: env::var(HONEYCOMB_API_KEY).context(format!(
                "Environment variable {} not found",
                HONEYCOMB_API_KEY
            ))?,
        })
    }

    async fn get<T>(&self, request: &str) -> anyhow::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = reqwest::Client::new()
            .get(format!("{}{}", URL, request))
            .header("X-Honeycomb-Team", &self.api_key)
            .send()
            .await?;
        let headers = response.headers().clone();
        let status = response.status();
        let text: String = response.text().await?;

        match serde_json::from_str::<T>(&text) {
            Ok(t) => Ok(t),
            Err(e) => {
                eprintln!(
                    "Invalid response: GET request = {}, \nstatus = {:?}, \nJSON-data = {}, \nheaders = {:?}",
                    request, status, text, headers
                );
                Err(anyhow::anyhow!("Failed to parse JSON data: {}", e))
            }
        }
    }

    pub async fn list_authorizations(&self) -> anyhow::Result<Authorizations> {
        self.get("auth").await
    }
    pub async fn list_all_datasets(&self) -> anyhow::Result<Vec<Dataset>> {
        self.get("datasets").await
    }
    pub async fn list_all_columns(&self, dataset_slug: &str) -> anyhow::Result<Vec<Column>> {
        self.get(&format!("columns/{}", dataset_slug)).await
    }
    pub async fn get_query_results(
        &self,
        dataset_slug: &str,
        query_result_id: &str,
    ) -> anyhow::Result<Value> {
        self.get(&format!(
            "query_results/{}/{}",
            dataset_slug, query_result_id
        ))
        .await
    }

    async fn post<T>(&self, request: &str, json: Value) -> anyhow::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut retries = 12;
        while retries > 0 {
            let response = reqwest::Client::new()
                .post(format!("{}{}", URL, request))
                .header("X-Honeycomb-Team", &self.api_key)
                .json(&json)
                .send()
                .await?;
            let status = response.status();

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                retries -= 1;
                continue;
            }
            let headers = response.headers().clone();
            let text: String = response.text().await?;

            return match serde_json::from_str::<T>(&text) {
                Ok(t) => Ok(t),
                Err(e) => {
                    eprintln!(
                        "Invalid response: POST request = {}, \nstatus = {:?}, \nJSON-data = {}, \nheaders = {:?}",
                        request, status, text, headers
                    );
                    Err(anyhow::anyhow!("Failed to parse JSON data: {}", e))
                }
            };
        }
        Err(anyhow::anyhow!("Too many retries"))
    }

    async fn get_query_url(
        &self,
        dataset_slug: &str,
        json: Value,
        disable_series: bool,
    ) -> anyhow::Result<String> {
        let query: Query = self
            .post(&format!("queries/{}", dataset_slug), json)
            .await?;

        let query_result: QueryResult = self
            .post(
                &format!("query_results/{}", dataset_slug),
                serde_json::json!({
                  "query_id": query.id,
                  "disable_series": disable_series,
                  "limit": 10000
                }),
            )
            .await?;

        Ok(query_result.links.query_url)
    }

    pub async fn get_exists_query_url(
        &self,
        dataset_slug: &str,
        column_id: &str,
        disable_series: bool,
    ) -> anyhow::Result<String> {
        self.get_query_url(
            dataset_slug,
            serde_json::json!({
                "breakdowns": [column_id],
                "calculations": [{
                    "op": "COUNT"
                }],
                "filters": [{
                    "column": column_id,
                    "op": "exists",
                }],
                "time_range": 604799
            }),
            disable_series,
        )
        .await
    }

    pub async fn get_avg_query_url(
        &self,
        dataset_slug: &str,
        column_id: &str,
    ) -> anyhow::Result<String> {
        self.get_query_url(
            dataset_slug,
            serde_json::json!({
                "calculations": [{
                    "op": "AVG",
                    "column": column_id
                }],
                "time_range": 604799
            }),
            false,
        )
        .await
    }

    pub async fn get_group_by_variants(
        &self,
        dataset_slug: &str,
        column_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let url = self
            .get_query_url(
                dataset_slug,
                serde_json::json!({
                    "breakdowns": [column_id],
                    "calculations": [{
                        "op": "COUNT"
                    }],
                    "time_range": 604799
                }),
                false,
            )
            .await?;
        let token = url.split('/').last().unwrap();
        let mut results = Vec::new();
        let mut polls = 50; // ~5 seconds
        while polls > 0 {
            let value = self.get_query_results(dataset_slug, token).await?;
            if value["complete"].as_bool().unwrap() {
                for r in value["data"]["results"].as_array().unwrap_or(&vec![]) {
                    if let Some(column) = r["data"][column_id].as_str() {
                        results.push(column.to_string());
                    }
                }
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            polls -= 1;
        }
        Ok(results)
    }

    /// Get a list of datasets that have been written to in the last `last_written` days
    pub async fn get_dataset_slugs(
        &self,
        last_written: i64,
        include_datasets: Option<HashSet<String>>,
    ) -> anyhow::Result<Vec<String>> {
        let inc_datasets = match include_datasets {
            Some(d) => d,
            None => HashSet::new(),
        };

        let now = Utc::now();
        let mut datasets = self
            .list_all_datasets()
            .await?
            .iter()
            .filter_map(|d| {
                if (now - d.last_written_at.unwrap_or(now)).num_days() < last_written {
                    if inc_datasets.is_empty() || inc_datasets.contains(&d.slug) {
                        Some(d.slug.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        datasets.sort();

        Ok(datasets)
    }

    /// Process datasets and columns in parallel and call the provided function for each dataset.
    /// The order of the datasets is preserved. Only columns that have been written to in the last
    /// `last_written` days are processed.
    pub async fn process_datasets_columns<F>(
        &self,
        last_written: i64,
        datasets: &Vec<String>,
        mut f: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(String, Vec<Column>),
    {
        let now = Utc::now();
        let mut tasks = FuturesOrdered::new();

        for dataset in datasets {
            let dataset_clone = dataset.clone();
            let hc_clone = self.clone();
            tasks.push_back(async move {
                let columns = hc_clone.list_all_columns(&dataset_clone).await;
                match columns {
                    Ok(columns) => (
                        dataset_clone,
                        columns
                            .iter()
                            .filter(|&c| (now - c.last_written).num_days() < last_written)
                            .cloned()
                            .collect(),
                    ),
                    Err(e) => {
                        eprintln!(
                            "error fetching columns for dataset {}: {}",
                            dataset_clone, e
                        );
                        (dataset_clone, vec![])
                    }
                }
            });
        }

        while let Some((dataset, columns)) = tasks.next().await {
            f(dataset, columns);
        }

        Ok(())
    }

    pub async fn get_all_group_by_variants(
        &self,
        dataset_slug: &str,
        columns_ids: &[String],
    ) -> anyhow::Result<Vec<(String, Vec<String>)>> {
        let bar = ProgressBar::new(columns_ids.len() as u64)
            .with_style(
                indicatif::ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                    .unwrap(),
            )
            .with_message("Rate-limited queries, please wait...");
        bar.inc(0);

        let mut tasks = stream::iter(columns_ids.iter().cloned())
            .map(|column_id| async {
                let variants = self.get_group_by_variants(dataset_slug, &column_id).await;
                match variants {
                    Ok(variants) => (column_id, variants),
                    Err(e) => {
                        eprintln!("error fetching variants for column {}: {}", column_id, e);
                        (column_id, vec![])
                    }
                }
            })
            .buffer_unordered(3);

        let mut results = Vec::new();
        while let Some(result) = tasks.next().await {
            bar.inc(1);
            results.push(result);
        }
        bar.finish_and_clear();

        Ok(results)
    }
}
