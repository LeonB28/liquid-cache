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

use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, CommandGetSqlInfo, CommandPreparedStatementQuery,
    ProstMessageExt, SqlInfo,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, IpcMessage, SchemaAsIpc, Ticket,
};
use clap::Parser;
use dashmap::DashMap;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::{error::Result, execution::object_store::ObjectStoreUrl};
use futures::{StreamExt, TryStreamExt};
use liquid_cache_client::LiquidCacheBuilder;
use log::{debug, info};
use once_cell::sync::Lazy;
use prost::Message;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use url::Url;
use uuid::Uuid;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

#[derive(Parser, Serialize, Clone)]
pub struct FlightServerArgs {
    /// Flight Server URL
    #[arg(long, default_value = "127.0.0.1:50052")]
    pub flight_server: SocketAddr,

    /// LiquidCache Server URL
    #[arg(long, default_value = "http://localhost:15214")]
    pub cache_server: String,

    /// Object Store path
    #[arg(long)]
    pub object_store_url: String,
}

/// FlightSqlService with Liquid cache so support adbc flight sql
/// Used with examples from: <https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples/flight_sql_server.rs>
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
    let object_store_url = args.object_store_url;
    let service = FlightSqlServiceImpl::new(liquid_cache_server, object_store_url);
    info!("Listening on {serve_on:?}, liquid cache server on {liquid_cache_server:?}");

    let svc = FlightServiceServer::new(service);
    Server::builder().add_service(svc).serve(serve_on).await?;
    Ok(())
}

pub struct FlightSqlServiceImpl {
    ctx: Arc<SessionContext>,
    plans: Arc<DashMap<String, Option<LogicalPlan>>>,
    results: Arc<DashMap<String, Option<Vec<RecordBatch>>>>,
}

impl FlightSqlServiceImpl {
    fn new(cache_server: impl AsRef<str>, object_store_url: impl AsRef<str>) -> Self {
        let url = Url::parse(object_store_url.as_ref()).unwrap();
        let object_store = format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
        let ctx = LiquidCacheBuilder::new(cache_server)
            .with_object_store(ObjectStoreUrl::parse(object_store.as_str()).unwrap(), None)
            .build(SessionConfig::from_env().unwrap())
            .unwrap();

        Self {
            ctx: Arc::new(ctx),
            plans: Default::default(),
            results: Default::default(),
        }
    }

    #[allow(clippy::result_large_err)]
    fn get_plan(&self, handle: &str) -> Result<Option<LogicalPlan>, Status> {
        if let Some(plan) = self.plans.get(handle) {
            Ok(plan.clone())
        } else {
            Err(Status::internal(format!("Plan handle not found: {handle}")))?
        }
    }

    #[allow(clippy::result_large_err)]
    fn get_result(&self, handle: &str) -> Result<Option<Vec<RecordBatch>>, Status> {
        if let Some(result) = self.results.get(handle) {
            Ok(result.clone())
        } else {
            Err(Status::internal(format!(
                "Request handle not found: {handle}"
            )))?
        }
    }

    #[allow(clippy::result_large_err)]
    fn remove_plan(&self, handle: &str) -> Result<(), Status> {
        self.plans.remove(&handle.to_string());
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn remove_result(&self, handle: &str) -> Result<(), Status> {
        self.results.remove(&handle.to_string());
        Ok(())
    }

    async fn with_table(&self, file: Url, table_name: &str) -> Result<()> {
        self.ctx
            .register_parquet(table_name, file.as_ref(), ParquetReadOptions::default())
            .await
            .or_else(|e| {
                match e {
                    // allow table already exists
                    DataFusionError::Execution(message) if message.contains("already exists") => {
                        Ok(())
                    }
                    _ => Err(e),
                }
            })
    }

    fn empty_response(&self) -> (Arc<Schema>, Vec<RecordBatch>) {
        (Arc::new(Schema::empty()), vec![])
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    // this is a first call from the driver.
    // to get flight info about the server
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
        Ok(Response::new(flight_info))
    }

    // do get data about the server
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

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if message.is::<FetchResults>() {
            let fr: FetchResults = message
                .unpack()
                .map_err(|e| Status::internal(format!("{e:?}")))?
                .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;

            let handle = fr.handle;
            debug!("getting results for {handle}");
            let result = self.get_result(&handle)?;

            // if we get an empty result, create an empty schema
            let (schema, batches) = match result {
                None => self.empty_response(),
                Some(batches) => match batches.first() {
                    None => self.empty_response(),
                    Some(batch) => (batch.schema(), batches),
                },
            };

            let batch_stream = futures::stream::iter(batches).map(Ok);

            let stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batch_stream)
                .map_err(Status::from);

            Ok(Response::new(Box::pin(stream)))
        } else {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> std::result::Result<ActionCreatePreparedStatementResult, Status> {
        let user_query = query.query.as_str();
        debug!("do_action_create_prepared_statement: {user_query}");

        let plan_uuid = Uuid::new_v4().hyphenated().to_string();

        let statements = DFParser::parse_sql(user_query)
            .map_err(|e| Status::internal(format!("Failed to parse query: {e}")))?;

        let statement = statements
            .front()
            .ok_or(Status::internal("query is empty"))?;

        match statement {
            Statement::CreateExternalTable(table) => {
                let location_url =
                    Url::parse(&table.location).map_err(|e| status!("Unable parse location", e))?;

                self.with_table(location_url, &table.name.to_string())
                    .await
                    .map_err(|e| status!("Unable to register a table", e))?;

                self.plans.insert(plan_uuid.clone(), None);

                let plan_schema = self.empty_response().0;
                let message = SchemaAsIpc::new(&plan_schema, &IpcWriteOptions::default())
                    .try_into()
                    .map_err(|e| status!("Unable to serialize schema", e))?;
                let IpcMessage(schema_bytes) = message;

                let res = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: plan_uuid.into(),
                    dataset_schema: schema_bytes,
                    parameter_schema: Default::default(),
                };
                Ok(res)
            }
            _ => {
                let plan = self
                    .ctx
                    .sql(user_query)
                    .await
                    .and_then(|df| df.into_optimized_plan())
                    .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

                // store a copy of the plan, it will be used for execution
                self.plans.insert(plan_uuid.clone(), Some(plan.clone()));
                let plan_schema = plan.schema();
                let arrow_schema = (&**plan_schema).into();
                let message = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default())
                    .try_into()
                    .map_err(|e| status!("Unable to serialize schema", e))?;
                let IpcMessage(schema_bytes) = message;

                let res = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: plan_uuid.into(),
                    dataset_schema: schema_bytes,
                    parameter_schema: Default::default(),
                };
                Ok(res)
            }
        }
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_prepared_statement");
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse uuid", e))?;

        let plan = self.get_plan(handle)?;
        let plan_schema = match plan {
            Some(logical_plan) => {
                let state = self.ctx.state();
                let df = DataFrame::new(state, logical_plan);
                let result = df
                    .collect()
                    .await
                    .map_err(|e| status!("Error executing query", e))?;

                // if we get an empty result, create an empty schema
                let schema = match result.first() {
                    None => Schema::empty(),
                    Some(batch) => (*batch.schema()).clone(),
                };
                self.results.insert(handle.to_string(), Some(result));
                schema
            }
            None => {
                self.results.insert(handle.to_string(), None);
                Schema::empty()
            }
        };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let info = FlightInfo::new()
            // Encode the Arrow schema
            .try_with_schema(&plan_schema)
            .expect("encoding failed")
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            });
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn do_action_close_prepared_statement(
        &self,
        handle: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let handle = std::str::from_utf8(&handle.prepared_statement_handle);
        if let Ok(handle) = handle {
            debug!("do_action_close_prepared_statement: removing plan and results for {handle}");
            let _ = self.remove_plan(handle);
            let _ = self.remove_result(handle);
        }
        Ok(())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
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
