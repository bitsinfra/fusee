//! A simulated disk.
//!
//! Every file has a durable image and a list of writes issued since its last
//! flush. Reads see both (the page cache). A flush makes the pending writes
//! durable. A crash resolves each pending write at random: lost, landed in
//! full, or torn (some sectors landed, possibly with garbage), in no
//! particular order, which is what a real drive with a volatile cache and an
//! operating system that reorders writeback can do.

use crate::rng::Rng;
use std::collections::BTreeMap;

const SECTOR: usize = 512;

#[derive(Clone, Debug, Default)]
pub struct SimFile {
    pub durable: Vec<u8>,
    pending: Vec<(u64, Vec<u8>)>,
}

fn apply(img: &mut Vec<u8>, off: u64, bytes: &[u8]) {
    let off = off as usize;
    if img.len() < off + bytes.len() {
        img.resize(off + bytes.len(), 0);
    }
    img[off..off + bytes.len()].copy_from_slice(bytes);
}

impl SimFile {
    pub fn view(&self) -> Vec<u8> {
        let mut img = self.durable.clone();
        for (off, b) in &self.pending {
            apply(&mut img, *off, b);
        }
        img
    }

    pub fn read(&self, off: u64, len: usize) -> Option<Vec<u8>> {
        let img = self.view();
        img.get(off as usize..off as usize + len)
            .map(<[u8]>::to_vec)
    }

    pub fn write(&mut self, off: u64, bytes: &[u8]) {
        self.pending.push((off, bytes.to_vec()));
    }

    pub fn flush(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        for (off, b) in pending {
            apply(&mut self.durable, off, &b);
        }
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub fn truncate(&mut self, len: u64) {
        self.flush();
        self.durable.truncate(len as usize);
    }

    /// Resolves pending writes as a power loss would.
    pub fn crash(&mut self, rng: &mut Rng, tear: bool) {
        let mut pending = std::mem::take(&mut self.pending);
        // The device may complete writes in any order.
        rng.shuffle(&mut pending);
        for (off, b) in pending {
            match rng.below(100) {
                0..=34 => {}
                35..=69 => apply(&mut self.durable, off, &b),
                _ if !tear => apply(&mut self.durable, off, &b),
                _ => {
                    // Torn: a random subset of sectors lands; the first
                    // missing one may hold garbage.
                    let sectors = b.len().div_ceil(SECTOR);
                    for s in 0..sectors {
                        let lo = s * SECTOR;
                        let hi = ((s + 1) * SECTOR).min(b.len());
                        match rng.below(10) {
                            0..=5 => apply(&mut self.durable, off + lo as u64, &b[lo..hi]),
                            6 => {
                                let junk: Vec<u8> =
                                    (lo..hi).map(|_| rng.below(256) as u8).collect();
                                apply(&mut self.durable, off + lo as u64, &junk);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SimDisk {
    pub files: BTreeMap<String, SimFile>,
}

impl SimDisk {
    pub fn file(&mut self, name: &str) -> &mut SimFile {
        self.files.entry(name.to_owned()).or_default()
    }

    pub fn get(&self, name: &str) -> Option<&SimFile> {
        self.files.get(name)
    }

    pub fn remove(&mut self, name: &str) {
        self.files.remove(name);
    }

    pub fn names_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.files
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    pub fn crash(&mut self, rng: &mut Rng, tear: bool) {
        for f in self.files.values_mut() {
            f.crash(rng, tear);
        }
    }

    /// Flips one bit in a durable byte of some file whose name starts with
    /// `prefix` (detectable corruption).
    pub fn corrupt(&mut self, rng: &mut Rng, prefix: &str) -> Option<(String, usize)> {
        let names: Vec<String> = self
            .files
            .iter()
            .filter(|(k, f)| k.starts_with(prefix) && f.durable.len() > 4096)
            .map(|(k, _)| k.clone())
            .collect();
        if names.is_empty() {
            return None;
        }
        let name = names[rng.below(names.len() as u64) as usize].clone();
        let f = self.files.get_mut(&name)?;
        let used = f.durable.iter().rposition(|b| *b != 0)?;
        if used <= 4096 {
            return None;
        }
        let at = 4096 + rng.below((used - 4096) as u64) as usize;
        f.durable[at] ^= 1 << rng.below(8);
        Some((name, at))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flushed_data_survives_crashes() {
        let mut rng = Rng::new(7);
        let mut f = SimFile::default();
        f.write(0, &[1; 1000]);
        f.flush();
        f.write(1000, &[2; 1000]);
        f.crash(&mut rng, true);
        assert_eq!(&f.durable[..1000], &[1; 1000][..]);
    }
}
