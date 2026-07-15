use std::io::Write as _;

use async_trait::async_trait;
use connectrpc::client::ClientConfig;
use connectrpc::client::HttpClient;
use futures::stream;
use henosis_controller_runtime::GitRepository;
use henosis_frontend_git_sync::GitSyncFrontend;
use henosis_frontend_git_sync::GraphService;
use henosis_proto::connect::henosis::v1::GraphServiceClient;
use henosis_proto::proto::henosis::v1::CreateGraphRequest;
use henosis_proto::proto::henosis::v1::CreateGraphResponse;
use henosis_proto::proto::henosis::v1::GetGraphRequest;
use henosis_proto::proto::henosis::v1::GetGraphResponse;
use henosis_proto::proto::henosis::v1::RetireGraphRequest;
use henosis_proto::proto::henosis::v1::RetireGraphResponse;
use henosis_proto::proto::henosis::v1::UpdateGraphRequest;
use henosis_proto::proto::henosis::v1::UpdateGraphResponse;
use henosis_proto::proto::henosis::v1::WatchGraphRequest;
use henosis_proto::proto::henosis::v1::WatchGraphResponse;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut arguments = std::env::args().skip(1);
    let repository = arguments.next().ok_or_else(|| {
        anyhow::anyhow!("usage: henosis-frontend-git-sync <bare-repo> <core-url>")
    })?;
    let endpoint = arguments
        .next()
        .unwrap_or_else(|| "http://127.0.0.1:4481".into());
    let service = ConnectGraphService::new(&endpoint)?;
    let mut frontend = GitSyncFrontend::new(GitRepository::new(repository), service);
    let changes = frontend.poll_git_intent().await?;
    writeln!(
        std::io::stdout().lock(),
        "git-sync applied {changes} graph intent change(s)"
    )?;
    Ok(())
}

#[derive(Clone)]
struct ConnectGraphService {
    client: GraphServiceClient<HttpClient>,
}

impl ConnectGraphService {
    fn new(endpoint: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: GraphServiceClient::new(
                HttpClient::plaintext(),
                ClientConfig::new(endpoint.parse()?),
            ),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("GraphService request failed: {0}")]
struct ServiceError(String);

#[async_trait]
impl GraphService for ConnectGraphService {
    type Error = ServiceError;
    type WatchStream = stream::Empty<Result<WatchGraphResponse, ServiceError>>;

    async fn create_graph(
        &self,
        request: CreateGraphRequest,
    ) -> Result<CreateGraphResponse, Self::Error> {
        self.client
            .create_graph(request)
            .await
            .map(|response| response.into_owned())
            .map_err(service_error)
    }

    async fn update_graph(
        &self,
        request: UpdateGraphRequest,
    ) -> Result<UpdateGraphResponse, Self::Error> {
        self.client
            .update_graph(request)
            .await
            .map(|response| response.into_owned())
            .map_err(service_error)
    }

    async fn retire_graph(
        &self,
        request: RetireGraphRequest,
    ) -> Result<RetireGraphResponse, Self::Error> {
        self.client
            .retire_graph(request)
            .await
            .map(|response| response.into_owned())
            .map_err(service_error)
    }

    async fn get_graph(&self, request: GetGraphRequest) -> Result<GetGraphResponse, Self::Error> {
        self.client
            .get_graph(request)
            .await
            .map(|response| response.into_owned())
            .map_err(service_error)
    }

    async fn watch_graph(
        &self,
        _request: WatchGraphRequest,
    ) -> Result<Self::WatchStream, Self::Error> {
        Err(ServiceError(
            "the one-shot demo frontend does not consume graph watches".into(),
        ))
    }
}

fn service_error(error: connectrpc::ConnectError) -> ServiceError {
    ServiceError(error.to_string())
}
