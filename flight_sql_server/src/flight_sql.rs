// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::StringArray;
use arrow::datatypes::{Field, Schema, ToByteSlice};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
    CommandGetCatalogs, CommandGetSqlInfo, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, ProstMessageExt, SqlInfo,
};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use clap::Parser;
use dashmap::DashMap;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};
use datafusion::{error::Result, execution::object_store::ObjectStoreUrl, prelude::*};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use liquid_cache_client::LiquidCacheBuilder;
use liquid_cache_common::CacheMode;
use log::info;
use once_cell::sync::Lazy;
use prost::Message;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Bytes;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use url::Url;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

#[derive(Parser, Serialize, Clone)]
pub struct FlightServerArgs {
    /// Server Flight Server on
    #[arg(long, default_value = "127.0.0.1:50052")]
    pub flight_server: SocketAddr,

    /// LiquidCache server URL
    #[arg(long, default_value = "http://localhost:15214")]
    pub cache_server: String,
}

/// FlightSqlService with Liquid cache so support adbc flight sql
/// Used with examples from: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples/flight_sql_server.rs
static INSTANCE_SQL_DATA: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    // Server information
    builder.append(
        SqlInfo::FlightSqlServerName,
        "Flight SQL Server with LiquidCache",
    );
    builder.append(SqlInfo::FlightSqlServerVersion, "1");
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1");
    builder.build().unwrap()
});

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = FlightServerArgs::parse();
    let serve_on = args.flight_server;
    let liquid_cache_server = &args.cache_server;

    let service = FlightSqlServiceImpl::new(liquid_cache_server);
    info!("Listening on {serve_on:?}, liquid cache server on {liquid_cache_server:?}");

    let svc = FlightServiceServer::new(service);
    Server::builder().add_service(svc).serve(serve_on).await?;
    Ok(())
}

pub struct FlightSqlServiceImpl {
    ctx: Arc<SessionContext>,
}

impl FlightSqlServiceImpl {
    fn new(cache_server: impl AsRef<str>) -> Self {
        let file = "file:///Users/leon.bam/data-sets/hits.parquet";
        let url = Url::parse(file).unwrap();
        let object_store_url = format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
        let ctx = LiquidCacheBuilder::new(cache_server)
            .with_object_store(
                ObjectStoreUrl::parse(object_store_url.as_str()).unwrap(),
                None,
            )
            .with_cache_mode(CacheMode::Liquid)
            .build(SessionConfig::from_env().unwrap())
            .unwrap();

        Self { ctx: Arc::new(ctx) }
    }

    async fn with_file(&self) -> datafusion::common::Result<()> {
        let file = "file:///Users/leon.bam/data-sets/hits.parquet";
        let url = Url::parse(file).unwrap();
        let table_name = Path::new(url.path())
            .file_stem()
            .unwrap_or_default()
            .to_str()
            .unwrap_or("default");
        self.ctx
            .register_parquet(table_name, url.as_ref(), Default::default())
            .await
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if !message.is::<FetchResults>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }

        let fr: FetchResults = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;

        let query = fr.handle;

        info!("getting results for {query}");

        let x = self
            .ctx
            .sql(&query)
            .await
            .map_err(|e| status!("Unable to query statement", e))?;
        let ss = x.schema().inner().clone();
        let batches = x
            .collect()
            .await
            .map_err(|e| status!("Unable to collect batch", e))?;

        let batch_stream = futures::stream::iter(batches).map(Ok);

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(ss)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let user_query = query.query.as_str();

        let ctx = self.ctx.clone();
        let plan = ctx
            .sql(user_query)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

        // self.statements.insert("plan".into(), plan.clone());
        //
        // let plan_schema = plan.schema();

        let arrow_schema = plan.schema().as_arrow();

        let fetch = FetchResults {
            handle: query.query,
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
            expiration_time: None,
            app_metadata: vec![].into(),
        };

        let info = FlightInfo::new()
            .try_with_schema(arrow_schema)
            .map_err(|e| status!("Unable to encode statement", e))?
            .with_endpoint(endpoint)
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            });
        Ok(Response::new(info))
    }
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&INSTANCE_SQL_DATA).schema().as_ref())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        self.with_file().await.unwrap();
        Ok(Response::new(flight_info))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let builder = query.into_builder(&INSTANCE_SQL_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_action_fallback(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let stream = Close {};
        Ok(Response::new(Box::pin(stream)))
    }
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

pub struct Close {}
impl Stream for Close {
    type Item = Result<arrow_flight::Result, Status>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(Ok(arrow_flight::Result::new("fdfd"))))
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/liquid_cache.com.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}
