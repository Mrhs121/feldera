use crate::{
    ensure_default_crypto_provider, InputBuffer, InputConsumer, InputEndpoint, InputReader, Parser,
    TransportInputEndpoint,
};
use actix_web::dev::{Decompress, Payload};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use awc::{Client, ClientResponse, Connector};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{InputCommandReceiver, InputReaderCommand, Resume, Watermark};
use feldera_types::{config::FtModel, program_schema::Relation, transport::s2::S2InputConfig};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{collections::VecDeque, hash::Hasher, ops::Range, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tracing::{info, info_span, Instrument};
use xxhash_rust::xxh3::Xxh3Default;

#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    seq_numbers: Range<u64>,
}

impl Metadata {
    fn from_resume_info(
        resume_info: Option<JsonValue>,
        default_start_seq_num: u64,
    ) -> Result<Self, AnyError> {
        Ok(resume_info
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or(Self {
                seq_numbers: default_start_seq_num..default_start_seq_num,
            }))
    }
}

#[derive(Debug, Deserialize)]
struct S2Cursor {
    seq_num: u64,
}

#[derive(Debug, Deserialize)]
struct S2Record {
    body: String,
    #[serde(default)]
    seq_num: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct S2ReadBatch {
    #[serde(default)]
    start: Option<S2Cursor>,
    #[serde(default)]
    records: Vec<S2Record>,
}

impl S2ReadBatch {
    fn into_records(self, default_seq_num: u64) -> Vec<(u64, String)> {
        let base_seq_num = self
            .start
            .as_ref()
            .map(|cursor| cursor.seq_num)
            .unwrap_or(default_seq_num);
        self.records
            .into_iter()
            .enumerate()
            .map(|(idx, record)| {
                let seq_num = record.seq_num.unwrap_or(base_seq_num + idx as u64);
                (seq_num, record.body)
            })
            .collect()
    }
}

struct SseParser {
    buffer: Vec<u8>,
}

impl SseParser {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    fn next_event_payload(&mut self) -> Option<Vec<u8>> {
        while let Some((event_end, delim_len)) = find_event_boundary(&self.buffer) {
            let event = self.buffer[..event_end].to_vec();
            self.buffer.drain(..event_end + delim_len);
            let payload = parse_sse_event_payload(&event);
            if !payload.is_empty() {
                return Some(payload);
            }
        }
        None
    }
}

fn find_event_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    for idx in 0..buffer.len() {
        if idx + 1 < buffer.len() && buffer[idx] == b'\n' && buffer[idx + 1] == b'\n' {
            return Some((idx, 2));
        }
        if idx + 3 < buffer.len()
            && buffer[idx] == b'\r'
            && buffer[idx + 1] == b'\n'
            && buffer[idx + 2] == b'\r'
            && buffer[idx + 3] == b'\n'
        {
            return Some((idx, 4));
        }
    }
    None
}

fn parse_sse_event_payload(event: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    for mut line in event.split(|b| *b == b'\n') {
        if line.ends_with(b"\r") {
            line = &line[..line.len() - 1];
        }
        if let Some(data) = line.strip_prefix(b"data:") {
            if !payload.is_empty() {
                payload.push(b'\n');
            }
            payload.extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
        }
    }
    payload
}

struct S2EventStream {
    config: Arc<S2InputConfig>,
    client: Client,
    parser: SseParser,
    response: Option<ClientResponse<Decompress<Payload>>>,
}

impl S2EventStream {
    fn new(config: Arc<S2InputConfig>) -> Self {
        Self {
            config,
            client: Client::builder().connector(Connector::new()).finish(),
            parser: SseParser::new(),
            response: None,
        }
    }

    fn disconnect(&mut self) {
        self.response = None;
    }

    async fn next_batch(&mut self, seq_num: u64) -> AnyResult<Option<S2ReadBatch>> {
        loop {
            if let Some(payload) = self.parser.next_event_payload() {
                let batch = serde_json::from_slice::<S2ReadBatch>(&payload).map_err(|error| {
                    anyhow!(
                        "failed to decode S2 SSE payload as JSON (payload={}): {error}",
                        String::from_utf8_lossy(&payload)
                    )
                })?;
                return Ok(Some(batch));
            }

            if self.response.is_none() {
                self.open(seq_num).await?;
            }

            let Some(response) = self.response.as_mut() else {
                continue;
            };

            let Some(item) = response.next().await else {
                self.response = None;
                return Ok(None);
            };

            let chunk = item.map_err(|error| anyhow!("error while reading S2 stream: {error}"))?;
            self.parser.push(&chunk);
        }
    }

    async fn open(&mut self, seq_num: u64) -> AnyResult<()> {
        let endpoint_url = self.config.endpoint_url.trim_end_matches('/');
        let url = format!(
            "{endpoint_url}/v1/streams/{}/records?seq_num={seq_num}",
            self.config.stream
        );

        let mut request = self
            .client
            .get(url)
            .insert_header(("Accept", "text/event-stream"))
            .insert_header(("S2-Basin", self.config.basin.as_str()));

        if let Some(auth_token) = &self.config.auth_token {
            request = request.insert_header(("Authorization", format!("Bearer {auth_token}")));
        }

        let response = request
            .timeout(Duration::from_secs(self.config.request_timeout_secs))
            .send()
            .await
            .map_err(|error| anyhow!("{error}"))?;

        if !response.status().is_success() {
            bail!(
                "S2 stream request failed with HTTP status {}",
                response.status()
            );
        }

        self.response = Some(response);
        Ok(())
    }
}

pub struct S2InputEndpoint {
    config: Arc<S2InputConfig>,
}

impl S2InputEndpoint {
    pub fn new(config: S2InputConfig) -> Result<Self, AnyError> {
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for S2InputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for S2InputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let resume_info = Metadata::from_resume_info(resume_info, self.config.start_seq_num)?;
        info!("Resume info: {:?}", resume_info);

        Ok(Box::new(S2InputReader::new(
            self.config.clone(),
            resume_info,
            consumer,
            parser,
            &schema.name.name(),
        )))
    }
}

struct S2InputReader {
    command_sender: UnboundedSender<InputReaderCommand>,
}

impl S2InputReader {
    fn new(
        config: Arc<S2InputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        table_name: &str,
    ) -> Self {
        let span = info_span!(
            "s2_input",
            table = %table_name,
            endpoint_url = %config.endpoint_url,
            basin = %config.basin,
            stream = %config.stream
        );

        let (command_sender, command_receiver) = unbounded_channel();

        let consumer_clone = consumer.clone();
        TOKIO.spawn(
            async move {
                Self::worker_task(
                    config,
                    resume_info,
                    consumer_clone,
                    parser,
                    command_receiver,
                )
                .await
                .unwrap_or_else(|e| consumer.error(true, e, Some("s2-input")));
            }
            .instrument(span),
        );

        Self { command_sender }
    }

    async fn replay_range(
        config: Arc<S2InputConfig>,
        range: Range<u64>,
        consumer: &dyn InputConsumer,
        parser: &mut Box<dyn Parser>,
    ) -> AnyResult<u64> {
        if range.is_empty() {
            consumer.replayed(BufferSize::default(), Xxh3Default::new().finish());
            return Ok(range.end);
        }

        let mut expected_seq_num = range.start;
        let mut stream = S2EventStream::new(config);
        let mut hasher = Xxh3Default::new();
        let mut total = BufferSize::default();

        while expected_seq_num < range.end {
            let batch = stream
                .next_batch(expected_seq_num)
                .await?
                .ok_or_else(|| anyhow!("unexpected end of S2 stream while replaying checkpoint"))?;
            let mut progressed = false;
            for (seq_num, body) in batch.into_records(expected_seq_num) {
                if seq_num < expected_seq_num {
                    continue;
                }
                if seq_num > expected_seq_num {
                    bail!(
                        "S2 replay gap detected: expected seq_num {}, received {}",
                        expected_seq_num,
                        seq_num
                    );
                }
                if seq_num >= range.end {
                    break;
                }
                let (buffer, errors) = parser.parse(body.as_bytes(), None);
                consumer.parse_errors(errors);
                consumer.buffered(buffer.len());
                if let Some(mut buffer) = buffer {
                    buffer.hash(&mut hasher);
                    total += buffer.len();
                    buffer.flush();
                }
                expected_seq_num += 1;
                progressed = true;
            }
            if !progressed {
                bail!("S2 replay did not progress at seq_num {}", expected_seq_num);
            }
        }

        consumer.replayed(total, hasher.finish());
        Ok(expected_seq_num)
    }

    async fn worker_task(
        config: Arc<S2InputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
    ) -> AnyResult<()> {
        ensure_default_crypto_provider();

        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);
        let mut next_seq_num = resume_info.seq_numbers.end;
        let mut stream = S2EventStream::new(config.clone());
        let mut queue = VecDeque::<(
            u64,
            Option<Box<dyn crate::InputBuffer>>,
            chrono::DateTime<Utc>,
        )>::new();
        let mut extending = false;

        while let Some((metadata, ())) = command_receiver.recv_replay().await? {
            info!("Attempt to replay: {:?}", metadata);
            next_seq_num = Self::replay_range(
                config.clone(),
                metadata.seq_numbers.clone(),
                &*consumer,
                &mut parser,
            )
            .await?;
        }

        loop {
            if !extending {
                match command_receiver.recv().await? {
                    command @ InputReaderCommand::Replay { .. } => {
                        unreachable!("{command:?} must be at the beginning of the command stream")
                    }
                    InputReaderCommand::Extend => extending = true,
                    InputReaderCommand::Pause => {}
                    InputReaderCommand::Queue { .. } => {
                        Self::flush_queue(&consumer, &mut queue, next_seq_num)?;
                    }
                    InputReaderCommand::Disconnect => return Ok(()),
                }
                continue;
            }

            select! {
                command = command_receiver.recv() => {
                    match command? {
                        command @ InputReaderCommand::Replay { .. } => {
                            unreachable!("{command:?} must be at the beginning of the command stream")
                        }
                        InputReaderCommand::Extend => {}
                        InputReaderCommand::Pause => {
                            extending = false;
                            stream.disconnect();
                        }
                        InputReaderCommand::Queue { .. } => {
                            Self::flush_queue(&consumer, &mut queue, next_seq_num)?;
                        }
                        InputReaderCommand::Disconnect => return Ok(()),
                    }
                }
                read_result = stream.next_batch(next_seq_num) => {
                    match read_result {
                        Ok(Some(batch)) => {
                            let timestamp = Utc::now();
                            for (seq_num, body) in batch.into_records(next_seq_num) {
                                if seq_num < next_seq_num {
                                    continue;
                                }
                                if seq_num > next_seq_num {
                                    bail!(
                                        "S2 input gap detected: expected seq_num {}, received {}",
                                        next_seq_num,
                                        seq_num
                                    );
                                }
                                let (buffer, errors) = parser.parse(body.as_bytes(), None);
                                consumer.parse_errors(errors);
                                consumer.buffered(buffer.len());
                                queue.push_back((seq_num, buffer, timestamp));
                                next_seq_num += 1;
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_secs(config.reconnect_timeout_secs)).await;
                        }
                        Err(error) => {
                            consumer.error(false, error, Some("s2-input"));
                            tokio::time::sleep(Duration::from_secs(config.reconnect_timeout_secs)).await;
                        }
                    }
                }
            }
        }
    }

    fn flush_queue(
        consumer: &dyn InputConsumer,
        queue: &mut VecDeque<(
            u64,
            Option<Box<dyn crate::InputBuffer>>,
            chrono::DateTime<Utc>,
        )>,
        next_seq_num: u64,
    ) -> AnyResult<()> {
        let mut total = BufferSize::default();
        let mut hasher = Xxh3Default::new();
        let mut count = 0usize;
        let limit = consumer.max_batch_size().max(1);
        let mut range: Option<Range<u64>> = None;
        let mut last_timestamp = None;

        while let Some((seq_num, buffer, timestamp)) = queue.pop_front() {
            range = Some(match range {
                Some(range) => range.start..seq_num + 1,
                None => seq_num..seq_num + 1,
            });

            if let Some(mut buffer) = buffer {
                buffer.hash(&mut hasher);
                total += buffer.len();
                buffer.flush();
            }

            count += 1;
            last_timestamp = Some(timestamp);
            if count >= limit {
                break;
            }
        }

        let seq_numbers = range.unwrap_or(next_seq_num..next_seq_num);
        let metadata_json = serde_json::to_value(&Metadata { seq_numbers })?;
        let resume = Resume::Replay {
            hash: hasher.finish(),
            seek: metadata_json.clone(),
            replay: rmpv::Value::Nil,
        };
        consumer.extended(
            total,
            Some(resume),
            vec![Watermark::new(
                last_timestamp.unwrap_or_else(Utc::now),
                Some(metadata_json),
            )],
        );
        Ok(())
    }
}

impl InputReader for S2InputReader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}

impl Drop for S2InputReader {
    fn drop(&mut self) {
        self.request(InputReaderCommand::Disconnect);
    }
}

#[cfg(test)]
mod test {
    use crate::test::{init_test_logger, mock_input_pipeline, wait, DEFAULT_TIMEOUT_MS};
    use actix::System;
    use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer};
    use anyhow::Result as AnyResult;
    use feldera_types::deserialize_without_context;
    use feldera_types::program_schema::Relation;
    use serde::{Deserialize, Serialize};
    use std::sync::mpsc::channel;
    use std::{thread, time::Duration};

    #[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
    struct S2TestRecord {
        s: String,
        b: bool,
        i: i64,
    }

    deserialize_without_context!(S2TestRecord);

    fn start_s2_mock_server() -> String {
        let (sender, receiver) = channel();
        thread::Builder::new()
            .name("s2-input-test-server".to_string())
            .spawn(move || {
                System::new().block_on(async {
                    let server = HttpServer::new(move || {
                        App::new()
                            .wrap(middleware::Logger::default())
                            .route(
                                "/v1/streams/events/records",
                                web::get().to(|req: HttpRequest| async move {
                                    let seq_num = req
                                        .query_string()
                                        .split('&')
                                        .find_map(|item| item.strip_prefix("seq_num="))
                                        .and_then(|v| v.parse::<u64>().ok())
                                        .unwrap_or(0);

                                    let body = if seq_num == 0 {
                                        "data: {\"start\":{\"seq_num\":0},\"records\":[{\"seq_num\":0,\"body\":\"{\\\"s\\\":\\\"foo\\\",\\\"b\\\":true,\\\"i\\\":10}\"},{\"seq_num\":1,\"body\":\"{\\\"s\\\":\\\"bar\\\",\\\"b\\\":false,\\\"i\\\":-10}\"}]}\n\n"
                                            .to_string()
                                    } else {
                                        String::new()
                                    };

                                    HttpResponse::Ok()
                                        .insert_header(("Content-Type", "text/event-stream"))
                                        .body(body)
                                }),
                            )
                    })
                    .workers(1)
                    .bind(("127.0.0.1", 0))
                    .unwrap();
                    sender.send(server.addrs()[0]).unwrap();
                    server.run().await.unwrap();
                });
            })
            .expect("failed to spawn S2 mock server");
        let addr = receiver.recv().unwrap();
        format!("http://{addr}")
    }

    #[test]
    fn test_s2_basic_input_consumption() -> AnyResult<()> {
        init_test_logger();
        let endpoint_url = start_s2_mock_server();

        let config_str = format!(
            r#"
stream: test_input
transport:
    name: s2_input
    config:
        endpoint_url: {endpoint_url}
        basin: test-basin
        stream: events
        auth_token: ignored
        start_seq_num: 0
format:
    name: json
    config:
        update_format: raw
"#
        );

        let (endpoint, consumer, _parser, zset) =
            mock_input_pipeline::<S2TestRecord, S2TestRecord>(
                serde_yaml::from_str(&config_str).unwrap(),
                Relation::empty(),
            )
            .unwrap();

        std::thread::sleep(Duration::from_millis(20));
        assert!(!consumer.state().eoi);

        endpoint.extend();
        wait(
            || {
                endpoint.queue(false);
                zset.state().flushed.len() == 2
            },
            DEFAULT_TIMEOUT_MS,
        )?;

        assert_eq!(zset.state().flushed[0].unwrap_insert().s, "foo");
        assert_eq!(zset.state().flushed[1].unwrap_insert().s, "bar");
        endpoint.disconnect();
        Ok(())
    }
}
