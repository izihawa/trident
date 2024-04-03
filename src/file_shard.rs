use crate::error::{Error, Result};
use crate::utils::FRAGMENT;
use async_stream::stream;
use percent_encoding::utf8_percent_encode;
use std::io;
use std::path::{Component, Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio_stream::Stream;
use tracing::info;

#[derive(Clone)]
pub struct FileShard {
    path: PathBuf,
}

pub fn normalize_path(path: impl AsRef<Path>) -> PathBuf {
    let mut components = path.as_ref().components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

impl FileShard {
    pub async fn new(path: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(path)
            .await
            .map_err(Error::io_error)?;
        Ok(FileShard {
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn get_path_for(&self, key: &str) -> io::Result<PathBuf> {
        let full_path = self.path.join(
            utf8_percent_encode(key, FRAGMENT)
                .collect::<String>()
                .to_lowercase(),
        );
        let cleaned_path = normalize_path(full_path);
        if !cleaned_path.starts_with(&self.path) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "path traversal attempt",
            ));
        }
        Ok(cleaned_path)
    }

    pub async fn open_store(&self, key: &str) -> io::Result<Option<File>> {
        let file_path = self.get_path_for(key)?;
        match File::open(file_path).await {
            Ok(file) => Ok(Some(file)),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }

    pub async fn exists(&self, key: &str) -> io::Result<Option<PathBuf>> {
        let file_path = self.get_path_for(key)?;
        if tokio::fs::try_exists(&file_path).await? {
            Ok(Some(file_path))
        } else {
            Ok(None)
        }
    }

    pub async fn insert<S: AsyncRead + Unpin>(
        &self,
        key: &str,
        mut stream: S,
    ) -> io::Result<PathBuf> {
        let file_path = self.get_path_for(key)?;
        let tmp_file_path = self.get_path_for(&format!("~{}", key))?;

        let mut tmp_file = File::create(&tmp_file_path).await?;
        tokio::io::copy(&mut stream, &mut tmp_file).await?;
        drop(tmp_file);

        tokio::fs::rename(tmp_file_path, file_path.clone()).await?;
        Ok(file_path)
    }

    pub async fn delete(&self, key: &str) -> io::Result<bool> {
        let file_path_opt = self.exists(key).await?;
        info!("delete file {:?}", file_path_opt);
        if let Some(file_path) = file_path_opt {
            tokio::fs::remove_file(file_path).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn ls(&self) -> impl Stream<Item = io::Result<String>> + Send + 'static {
        let path = self.path.clone();
        stream! {
            let mut read_dir_stream = tokio::fs::read_dir(&path).await?;
            while let Some(entry) = read_dir_stream.next_entry().await? {
                yield Ok(format!("{}\n", entry.file_name().to_string_lossy()))
            }
        }
    }
}
