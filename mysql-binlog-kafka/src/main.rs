use chrono::{TimeZone, Utc};
use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::providers::mariadb::gtid::gtid_list::GtidList;
use mysql_cdc::providers::mysql::gtid::gtid_set::GtidSet;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use rskafka::client::partition::{OffsetAt, PartitionClient};
use rskafka::client::Client;
use rskafka::{
    client::{
        partition::{Compression, UnknownTopicHandling},
        ClientBuilder,
    },
    record::Record,
};
use sqlparser::ast::{Query, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::NO;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::{thread, time::Duration};

struct KafkaProducer {
    client: Client,
    topic: Option<String>,
}

impl KafkaProducer {
    async fn connect(url: String) -> Self {
        KafkaProducer {
            client: ClientBuilder::new(vec![url])
                .build()
                .await
                .expect("Couldn't connect to kafka"),
            topic: None,
        }
    }

    async fn create_topic(&mut self, topic_name: &str) {
        let topics = self.client.list_topics().await.unwrap();

        for topic in topics {
            if topic.name.eq(&topic_name.to_string()) {
                self.topic = Some(topic_name.to_string());
                println!("Topic already exist in Kafka");
                return;
            }
        }

        let controller_client = self
            .client
            .controller_client()
            .expect("Couldn't create controller client kafka");
        controller_client
            .create_topic(
                topic_name, 1,     // partitions
                1,     // replication factor
                5_000, // timeout (ms)
            )
            .await
            .unwrap();
        self.topic = Some(topic_name.to_string());
    }

    fn create_record(&self, headers: String, value: String) -> Record {
        Record {
            key: None,
            value: Some(value.into_bytes()),
            headers: BTreeMap::from([("mysql_binlog_headers".to_owned(), headers.into_bytes())]),
            timestamp: Utc.timestamp_millis(42),
        }
    }

    async fn get_partition_client(&self, partition: i32) -> Option<PartitionClient> {
        if self.topic.is_none() {
            ()
        }

        let topic = self.topic.as_ref().unwrap();
        Some(
            self.client
                .partition_client(topic, partition, UnknownTopicHandling::Retry)
                .await
                .expect("Couldn't fetch controller client"),
        )
    }
}

struct Info {
    client: BinlogClient,
    tables: HashSet<String>,
    topic_client: HashMap<String, PartitionClient>,
    topic_offset: HashMap<String, i64>,
}

impl Info {
    fn new(client: BinlogClient) -> Self {
        Self {
            client,
            tables: HashSet::new(),
            topic_client: HashMap::new(),
            topic_offset: HashMap::new(),
        }
    }

    fn extract_tables_from_event(&self, event: &BinlogEvent) -> Option<Vec<String>> {
        let dialect = GenericDialect {};
        let mut tables = Vec::new();
        match event {
            BinlogEvent::QueryEvent(e) => {
                let stmts =
                    Parser::parse_sql(&dialect, &e.sql_statement).expect("failed to parse query");
                for stmt in stmts {
                    match stmt {
                        Statement::Insert { table_name, .. } => tables.append(
                            table_name
                                .0
                                .into_iter()
                                .map(|id| id.value)
                                .collect::<Vec<String>>()
                                .as_mut(),
                        ),
                        Statement::CreateTable { name, .. } => tables.append(
                            name.0
                                .into_iter()
                                .map(|id| id.value)
                                .collect::<Vec<String>>()
                                .as_mut(),
                        ),
                        Statement::Query(q) => {
                            if let sqlparser::ast::SetExpr::Select(s) = *q.body {
                                for t in s.from {
                                    if let sqlparser::ast::TableFactor::Table { name, .. } =
                                        t.relation
                                    {
                                        tables.append(
                                            name.0
                                                .into_iter()
                                                .map(|id| id.value)
                                                .collect::<Vec<String>>()
                                                .as_mut(),
                                        );
                                    }
                                }
                            }
                        }
                        _ => {
                            println!("unimplemented");
                            return None;
                        }
                    }
                }
                Some(tables)
            }
            BinlogEvent::TableMapEvent(e) => {
                tables.push(e.table_name.clone());
                Some(tables)
            }
            BinlogEvent::RowsQueryEvent(e) => {
                let stmts = Parser::parse_sql(&dialect, &e.query).expect("failed to parse query");
                for stmt in stmts {
                    match stmt {
                        Statement::Insert { table_name, .. } => tables.append(
                            table_name
                                .0
                                .into_iter()
                                .map(|id| id.value)
                                .collect::<Vec<String>>()
                                .as_mut(),
                        ),
                        Statement::CreateTable { name, .. } => tables.append(
                            name.0
                                .into_iter()
                                .map(|id| id.value)
                                .collect::<Vec<String>>()
                                .as_mut(),
                        ),
                        Statement::Query(q) => {
                            if let sqlparser::ast::SetExpr::Select(s) = *q.body {
                                for t in s.from {
                                    if let sqlparser::ast::TableFactor::Table { name, .. } =
                                        t.relation
                                    {
                                        tables.append(
                                            name.0
                                                .into_iter()
                                                .map(|id| id.value)
                                                .collect::<Vec<String>>()
                                                .as_mut(),
                                        );
                                    }
                                }
                            }
                        }
                        _ => {
                            println!("unimplemented");
                            return None;
                        }
                    }
                }
                Some(tables)
            }
            _ => {
                println!("Unimplemented event.");
                None
            }
        }
    }

    fn add_table(&mut self, table: String) {
        self.tables.insert(table);
    }

    fn add_client(&mut self, topic: String, client: PartitionClient) {
        self.topic_client.insert(topic, client);
    }

    fn add_idx(&mut self, topic: String, idx: i64) {
        self.topic_offset.insert(topic, idx);
    }

    fn get_partition_client(&self, topic: &str) -> Option<&PartitionClient> {
        self.topic_client.get(topic)
    }

    fn get_partition_offset(&self, topic: &str) -> Option<i64> {
        self.topic_offset.get(topic).copied()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), mysql_cdc::errors::Error> {
    let sleep_time: u64 = std::env::var("SLEEP_TIME").unwrap().parse().unwrap();

    thread::sleep(Duration::from_millis(sleep_time));
    println!("Thread started");

    // // Start replication from MariaDB GTID
    // let _options = BinlogOptions::from_mariadb_gtid(GtidList::parse("0-1-270")?);
    //
    // // Start replication from MySQL GTID
    // let gtid_set =
    //     "d4c17f0c-4f11-11ea-93e3-325d3e1cd1c8:1-107, f442510a-2881-11ea-b1dd-27916133dbb2:1-7";
    // let _options = BinlogOptions::from_mysql_gtid(GtidSet::parse(gtid_set)?);
    //
    // // Start replication from the position
    // let _options = BinlogOptions::from_position(String::from("mysql-bin.000008"), 195);
    //
    // Start replication from last master position.
    // Useful when you are only interested in new changes.
    let options = BinlogOptions::from_end();

    // Start replication from first event of first available master binlog.
    // Note that binlog files by default have expiration time and deleted.
    // let options = BinlogOptions::from_start();

    let table_names: Vec<String> = match std::env::var("TABLE_NAMES") {
        Ok(names) => names
            .split_terminator(',')
            .map(|name| name.trim().to_string())
            .collect(),
        _ => vec!["payment".to_string()],
    };

    let username = std::env::var("SQL_USERNAME").unwrap();
    let password = std::env::var("SQL_PASSWORD").unwrap();
    let mysql_port = std::env::var("SQL_PORT").unwrap();
    let mysql_hostname = std::env::var("SQL_HOSTNAME").unwrap();

    let mysql_database = std::env::var("SQL_DATABASE").unwrap();
    let options = ReplicaOptions {
        username,
        password,
        port: mysql_port.parse::<u16>().unwrap(),
        hostname: mysql_hostname,
        database: Some(mysql_database.clone()),
        blocking: true,
        ssl_mode: SslMode::Disabled,
        binlog: options,
        ..Default::default()
    };

    let mut client = BinlogClient::new(options);
    println!("Connected to mysql database");

    let kafka_url = std::env::var("KAFKA_URL").unwrap();
    let mut kafka_producer = KafkaProducer::connect(kafka_url).await;
    let mut client_info = Info::new(client);
    println!("Connected to kafka server");
    //kafka_producer.create_topic("mysql_binlog_events").await;

    for (idx, table_name) in table_names.iter().enumerate() {
        let topic_name = format!("{}_{}", mysql_database.clone(), table_name.clone());
        kafka_producer.create_topic(&topic_name).await;
        println!("Created kafka topic {}", topic_name);
        let partitionClient = kafka_producer
            .get_partition_client(idx.try_into().unwrap())
            .await
            .unwrap();
        println!("Got kafka partitionClient");
        let partition_offset = partitionClient.get_offset(OffsetAt::Latest).await.unwrap();
        println!("Got kafka partition_offset");
        client_info.add_client(topic_name.clone(), partitionClient);
        client_info.add_idx(topic_name.clone(), partition_offset);
        client_info.add_table(table_name.clone());
    }

    for result in client_info.client.replicate()? {
        println!("Received MySQL event");
        let (header, event) = result?;
        let event_tables = client_info
            .extract_tables_from_event(&event)
            .unwrap()
            .into_iter()
            .filter_map(|table| {
                if table_names.contains(&table) {
                    Some(table)
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        if event_tables.is_empty() {
            continue;
        }

        let topic = format!("{}_{}", mysql_database.clone(), event_tables[0].clone());

        let json_event = serde_json::to_string(&event).expect("Couldn't convert sql event to json");
        println!("json_event: {}", json_event);
        let json_header =
            serde_json::to_string(&header).expect("Couldn't convert sql header to json");

        println!("Try to create Kafka record");
        let kafka_record = kafka_producer.create_record(json_header, json_event);
        println!("Kafka record created");
        // extract the first table in the query as partitionclient
        let partitionClient = client_info.get_partition_client(&topic).unwrap();
        partitionClient
            .produce(vec![kafka_record], Compression::default())
            .await
            .unwrap();
        println!("Kafka record produced");

        let mut partition_offset = client_info.get_partition_offset(&topic).unwrap();

        // Consumer
        let (records, high_watermark) = partitionClient
            .fetch_records(
                partition_offset, // offset
                1..100_000,       // min..max bytes
                1_000,            // max wait time
            )
            .await
            .unwrap();

        partition_offset = high_watermark;
        println!("Kafka new partition_offset");

        for record in records {
            let record_clone = record.clone();
            let timestamp = record_clone.record.timestamp;
            let value = record_clone.record.value.unwrap();
            let header = record_clone
                .record
                .headers
                .get("mysql_binlog_headers")
                .unwrap()
                .clone();

            println!("============================================== Event from Apache kafka ==========================================================================");
            println!();
            println!("Value: {}", String::from_utf8(value).unwrap());
            println!("Timestamp: {}", timestamp);
            println!("Headers: {}", String::from_utf8(header).unwrap());
            println!();
            println!();
        }

        // After you processed the event, you need to update replication position
        println!("Try to update MySQL replication");
        client_info.client.commit(&header, &event);
        println!("MySQL replication updated");
    }
    Ok(())
}
