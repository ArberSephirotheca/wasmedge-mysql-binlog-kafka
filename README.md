# Pretest Completion
This repository contains the modified code for completing the pretest. The modified code achieves the following tasks:
- Connects to a MySQL database using the provided credentials.
- Connects to an Apache Kafka server using the provided URL.
- Creates Kafka topics for each table specified in the environment variable TABLE_NAMES.
- Sets up a binlog client to replicate events from the MySQL database.
- Extracts relevant tables from the binlog events based on the provided table names.
- Converts the binlog events to JSON format.
- Produces Kafka records for each relevant event and sends them to the corresponding topic.
- Consumes Kafka records from each topic, printing the event details.

# Implementation
- Introduce new environment variable `TABLE_NAMES`:
    - An environment variable that contain a list of name of tables we are going to process.
    - Table name is seperated by comma.
```rust
    let table_names: Vec<String> = match std::env::var("TABLE_NAMES") {
        Ok(names) => names
            .split(',')
            .map(|name| name.trim().to_string())
            .collect(),
        _ => vec!["payment".to_string()],
    };
```
- Filter the records:
    - Add additional control flow statement when receive events from server.
    - only records that contain tables in `TABLE_NAMES` environment variable are processed.
    - Tables that are not in the environment variable is skipped by a `contniue` statement. 
```rust
        let (header, event) = result?;
        let event_tables = client_info
            .extract_tables_from_event(&event)
            .unwrap_or_else(||Vec::new())
            .into_iter()
            .filter_map(|table| {
                if table_names.contains(&table) {
                    Some(table)
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();
```
- Create `Info` struct: 
    - Implement new struct called `Info`.
    - Add fields to the Info struct, including topic_client to store the mapping between topics and partition clients, and topic_offset to store the current offset for each topic.
    - Implement few helper function for user to store and get information.
```rust
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
```
- Assign different tables to different topics:
    - For each table, I create a new topic in client.
    - I then assign different partition client and offset to different topics respectively.
    - The information of partition client and partition offset for different topics are stored in `Info` struct.
```rust
    for (idx, table_name) in table_names.iter().enumerate() {
        let topic_name = format!("{}_{}", mysql_database.clone(), table_name.clone());
        kafka_producer.create_topic(&topic_name).await;
        println!("Created kafka topic {}", topic_name);
        let partitionClient = kafka_producer
            .get_partition_client(0)
            .await
            .unwrap();
        println!("Got kafka partitionClient");
        let partition_offset = partitionClient.get_offset(OffsetAt::Latest).await.unwrap();
        println!("Got kafka partition_offset");
        client_info.add_client(topic_name.clone(), partitionClient);
        client_info.add_idx(topic_name.clone(), partition_offset);
        client_info.add_table(table_name.clone());
    }

```
- Implement `extract_tables from event` function:
    - Add the method `extract_tables_from_event` to the Info struct.
    - This method extracts the table names from the received binlog events.
    - It uses the `sqlparser` crate to parse the SQL statements and identify the table names involved in the events.
```rust
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
```
# Limitation
- It only handles single database as it cannot tell the difference between tables with same name but from different database when parsing.
- There are many different `BinlogEvents` remain unimplemented.

# Result
