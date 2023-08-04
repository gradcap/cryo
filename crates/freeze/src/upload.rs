use std::path::{Path, PathBuf};

use async_channel::Receiver;
use cloud_storage::{self as cs, Client};
use futures::TryFutureExt;
use tokio::fs::File;

const BLOB_PREFIX: &str = "ethereum/";
const BLOB_MIME: &str = "application/octet-stream";

async fn cloud_uploader_loop(
    bucket: String,
    inputs_rx: Receiver<PathBuf>,
) -> Result<(), cs::Error> {
    let client = Client::default();

    while let Ok(path) = inputs_rx.recv().await {
        let meta = tokio::fs::metadata(&path).await?;

        let basename = path.file_name().unwrap().to_str().unwrap();
        let parts: Vec<_> = basename.split("__").collect();
        let blob_name = format!("{}{}", BLOB_PREFIX, parts.join("/"));

        println!("Uploading {:?} as {}, size={}", path, blob_name, meta.len());
        backoff::future::retry_notify(
            backoff::ExponentialBackoff::default(),
            || {
                async {
                    let file = File::open(&path).await?;
                    let stream = tokio_util::io::ReaderStream::new(file);
                    client
                        .object()
                        .create_streamed(&bucket, stream, meta.len(), &blob_name, BLOB_MIME)
                        .await
                }
                .err_into()
            },
            |e, dur| {
                println!("Upload for {} error {}, retry in {:?}", &blob_name, e, dur);
            },
        )
        .await?;
    }
    Ok(())
}

pub async fn upload(
    num_uploaders: usize,
    bucket: &str,
    dir: &Path,
) -> Result<(), cloud_storage::Error> {
    let mut parallel_tasks = vec![];
    {
        let (tx, rx) = async_channel::bounded(num_uploaders + 1);
        for _i in 0..num_uploaders {
            let task = cloud_uploader_loop(bucket.to_owned(), rx.clone());
            parallel_tasks.push(tokio::spawn(task));
        }

        for file_path in crate::util::list_files(dir, "parquet").await? {
            if tx.send(file_path).await.is_err() {
                // Uploader tasks closed
                break
            }
        }
    }
    crate::util::wait_all_tasks("uploaders", &mut parallel_tasks).await.unwrap()
}
