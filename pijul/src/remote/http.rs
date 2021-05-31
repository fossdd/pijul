use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;

use anyhow::bail;
use libpijul::pristine::{Base32, MutTxnT, Position};
use libpijul::{Hash, RemoteRef};
use log::{debug, error};

pub struct Http {
    pub url: url::Url,
    pub channel: String,
    pub client: reqwest::Client,
    pub name: String,
}

async fn download_change(
    client: reqwest::Client,
    url: url::Url,
    mut path: PathBuf,
    c: libpijul::pristine::Hash,
) -> Result<libpijul::pristine::Hash, anyhow::Error> {
    libpijul::changestore::filesystem::push_filename(&mut path, &c);
    std::fs::create_dir_all(&path.parent().unwrap())?;
    let path_ = path.with_extension("tmp");
    let mut f = std::fs::File::create(&path_)?;
    libpijul::changestore::filesystem::pop_filename(&mut path);
    let c32 = c.to_base32();
    let url = format!("{}/{}", url, super::DOT_DIR);
    let mut delay = 1f64;
    loop {
        let mut res = if let Ok(res) = client.get(&url).query(&[("change", &c32)]).send().await {
            delay = 1f64;
            res
        } else {
            debug!("HTTP error, retrying in {} seconds", delay.round());
            tokio::time::sleep(std::time::Duration::from_secs_f64(delay)).await;
            f.set_len(0)?;
            delay *= 2.;
            continue;
        };
        debug!("response {:?}", res);
        if !res.status().is_success() {
            bail!("HTTP error {:?}", res.status())
        }
        let done = loop {
            match res.chunk().await {
                Ok(Some(chunk)) => {
                    debug!("writing {:?}", chunk.len());
                    f.write_all(&chunk)?;
                }
                Ok(None) => break true,
                Err(_) => {
                    error!("Error while downloading {:?}, retrying", url);
                    tokio::time::sleep(std::time::Duration::from_secs_f64(delay)).await;
                    delay *= 2.;
                    break false;
                }
            }
        };
        if done {
            std::fs::rename(&path_, &path_.with_extension("change"))?;
            break;
        }
    }
    Ok(c)
}

const POOL_SIZE: usize = 20;

impl Http {
    pub async fn download_changes(
        &mut self,
        pro_n: usize,
        hashes: &mut tokio::sync::mpsc::UnboundedReceiver<libpijul::pristine::Hash>,
        send: &mut tokio::sync::mpsc::Sender<libpijul::pristine::Hash>,
        path: &PathBuf,
        _full: bool,
    ) -> Result<(), anyhow::Error> {
        let mut pool = <[_; POOL_SIZE]>::default();
        let mut cur = 0;
        while let Some(c) = hashes.recv().await {
            debug!("downloading {:?}", c);
            let t = std::mem::replace(
                &mut pool[cur],
                Some(tokio::spawn(download_change(
                    self.client.clone(),
                    self.url.clone(),
                    path.clone(),
                    c,
                ))),
            );
            if let Some(t) = t {
                debug!("waiting for process {:?}", cur);
                let c = t.await??;
                debug!("sending {:?}", c);
                super::PROGRESS.borrow_mut().unwrap()[pro_n].incr();
                if send.send(c).await.is_err() {
                    debug!("err for {:?}", c);
                    break;
                }
                debug!("sent");
            }
            cur = (cur + 1) % POOL_SIZE;
        }
        for f in 0..POOL_SIZE {
            if let Some(t) = pool[(cur + f) % POOL_SIZE].take() {
                let c = t.await??;
                debug!("sending {:?}", c);
                super::PROGRESS.borrow_mut().unwrap()[pro_n].incr();
                if send.send(c).await.is_err() {
                    debug!("err for {:?}", c);
                    break;
                }
                debug!("sent");
            }
        }
        Ok(())
    }

    pub async fn upload_changes(
        &self,
        mut local: PathBuf,
        to_channel: Option<&str>,
        changes: &[libpijul::Hash],
    ) -> Result<(), anyhow::Error> {
        for c in changes {
            libpijul::changestore::filesystem::push_filename(&mut local, &c);
            let url = {
                let mut p = self.url.path().to_string();
                if !p.ends_with("/") {
                    p.push('/')
                }
                p.push_str(super::DOT_DIR);
                let mut u = self.url.clone();
                u.set_path(&p);
                u
            };
            let change = std::fs::read(&local)?;
            let mut to_channel = if let Some(ch) = to_channel {
                vec![("to_channel", ch)]
            } else {
                Vec::new()
            };
            let c = c.to_base32();
            to_channel.push(("apply", &c));
            debug!("url {:?} {:?}", url, to_channel);
            self.client
                .post(url)
                .query(&to_channel)
                .body(change)
                .send()
                .await?;
            libpijul::changestore::filesystem::pop_filename(&mut local);
        }
        Ok(())
    }

    pub async fn download_changelist<T: MutTxnT>(
        &self,
        txn: &mut T,
        remote: &mut RemoteRef<T>,
        from: u64,
        paths: &[String],
    ) -> Result<HashSet<Position<Hash>>, anyhow::Error> {
        let url = {
            let mut p = self.url.path().to_string();
            if !p.ends_with("/") {
                p.push('/')
            }
            p.push_str(super::DOT_DIR);
            let mut u = self.url.clone();
            u.set_path(&p);
            u
        };
        let from_ = from.to_string();
        let mut query = vec![("changelist", &from_), ("channel", &self.channel)];
        for p in paths.iter() {
            query.push(("path", p));
        }
        let res = self.client.get(url).query(&query).send().await?;
        if !res.status().is_success() {
            bail!("HTTP error {:?}", res.status())
        }
        let resp = res.bytes().await?;
        let mut result = HashSet::new();
        if let Ok(data) = std::str::from_utf8(&resp) {
            for l in data.lines() {
                if !l.is_empty() {
                    match super::parse_line(l)? {
                        super::ListLine::Change { n, m, h } => {
                            txn.put_remote(remote, n, (h, m))?;
                        }
                        super::ListLine::Position(pos) => {
                            result.insert(pos);
                        }
                        super::ListLine::Error(e) => {
                            let mut stderr = std::io::stderr();
                            writeln!(stderr, "{}", e)?;
                        }
                    }
                } else {
                    break;
                }
            }
        }
        Ok(result)
    }

    pub async fn get_state(
        &mut self,
        mid: Option<u64>,
    ) -> Result<Option<(u64, libpijul::Merkle)>, anyhow::Error> {
        debug!("get_state {:?}", self.url);
        let url = format!("{}/{}", self.url, super::DOT_DIR);
        let q = if let Some(mid) = mid {
            [
                ("state", format!("{}", mid)),
                ("channel", self.channel.clone()),
            ]
        } else {
            [("state", String::new()), ("channel", self.channel.clone())]
        };
        let res = self.client.get(&url).query(&q).send().await?;
        if !res.status().is_success() {
            bail!("HTTP error {:?}", res.status())
        }
        let resp = res.bytes().await?;
        let resp = std::str::from_utf8(&resp)?;
        debug!("resp = {:?}", resp);
        let mut s = resp.split(' ');
        if let (Some(n), Some(m)) = (
            s.next().and_then(|s| s.parse().ok()),
            s.next()
                .and_then(|m| libpijul::Merkle::from_base32(m.as_bytes())),
        ) {
            Ok(Some((n, m)))
        } else {
            Ok(None)
        }
    }

    pub async fn archive<W: std::io::Write + Send + 'static>(
        &mut self,
        prefix: Option<String>,
        state: Option<(libpijul::Merkle, &[Hash])>,
        mut w: W,
    ) -> Result<u64, anyhow::Error> {
        let url = {
            let mut p = self.url.path().to_string();
            if !p.ends_with("/") {
                p.push('/')
            }
            p.push_str(super::DOT_DIR);
            let mut u = self.url.clone();
            u.set_path(&p);
            u
        };
        let res = self.client.get(url).query(&[("channel", &self.channel)]);
        let res = if let Some((ref state, ref extra)) = state {
            let mut q = vec![("archive".to_string(), state.to_base32())];
            if let Some(pre) = prefix {
                q.push(("outputPrefix".to_string(), pre));
            }
            for e in extra.iter() {
                q.push(("change".to_string(), e.to_base32()))
            }
            res.query(&q)
        } else {
            res
        };
        let res = res.send().await?;
        if !res.status().is_success() {
            bail!("HTTP error {:?}", res.status())
        }
        use futures_util::StreamExt;
        let mut stream = res.bytes_stream();
        let mut conflicts = 0;
        let mut n = 0;
        while let Some(item) = stream.next().await {
            let item = item?;
            let mut off = 0;
            while n < 8 && off < item.len() {
                conflicts = (conflicts << 8) | (item[off] as u64);
                off += 1;
                n += 1
            }
            w.write_all(&item[off..])?;
        }
        Ok(conflicts as u64)
    }
}
