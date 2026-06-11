use std::str::FromStr;

pub struct IncrementalSnapshotName {
    pub filename: String,
    pub from_slot: u64,
    pub to_slot: u64,
    pub hash: String,
}

impl IncrementalSnapshotName {
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let filename = url.rsplit('/').next().unwrap_or(url);

        Self::from_str(filename)
            .map_err(|e| anyhow::anyhow!("parsing incremental snapshot url: {e}"))
    }
}

impl FromStr for IncrementalSnapshotName {
    type Err = String;

    fn from_str(filename: &str) -> Result<Self, Self::Err> {
        // Ожидаем: "incremental-snapshot-{from}-{to}-{hash}.tar.zst"
        let rest = filename
            .strip_prefix("incremental-snapshot-")
            .ok_or("expected prefix 'incremental-snapshot-'")?;

        // Убираем суффикс архива, если есть
        let rest = rest
            .strip_suffix(".tar.zst")
            .or_else(|| rest.strip_suffix(".tar.lz4"))
            .unwrap_or(rest);

        let mut parts = rest.splitn(3, '-');

        let from_slot = parts
            .next()
            .ok_or("missing from_slot")?
            .parse::<u64>()
            .map_err(|e| format!("invalid from_slot: {e}"))?;

        let to_slot = parts
            .next()
            .ok_or("missing to_slot")?
            .parse::<u64>()
            .map_err(|e| format!("invalid to_slot: {e}"))?;

        let hash = parts.next().ok_or("missing hash")?.to_string();

        Ok(Self {
            filename: filename.to_string(),
            from_slot,
            to_slot,
            hash,
        })
    }
}

pub struct FullSnapshotName {
    pub filename: String,
    pub slot: u64,
    pub hash: String,
}

impl FullSnapshotName {
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let filename = url.rsplit('/').next().unwrap_or(url);

        Self::from_str(filename).map_err(|e| anyhow::anyhow!("parsing snapshot url: {e}"))
    }
}

impl FromStr for FullSnapshotName {
    type Err = String;

    fn from_str(filename: &str) -> Result<Self, Self::Err> {
        let rest = filename
            .strip_prefix("snapshot-")
            .ok_or("expected prefix 'snapshot-'")?;

        let rest = rest
            .strip_suffix(".tar.zst")
            .or_else(|| rest.strip_suffix(".tar.lz4"))
            .unwrap_or(rest);

        let (slot_str, hash) = rest.split_once('-').ok_or("missing '-' separator")?;

        let slot = slot_str
            .parse::<u64>()
            .map_err(|e| format!("invalid slot: {e}"))?;

        Ok(Self {
            filename: filename.to_string(),
            slot,
            hash: hash.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_snapshot_url() {
        let url = "http://127.0.0.1:8899/incremental-snapshot-404192616-404247502-3DsTyBkAQsLF9FmQ4V56aiTmtwbJ29ePxAjnnPKrFQpT.tar.zst";
        let parsed = IncrementalSnapshotName::from_url(url).unwrap();

        assert_eq!(
            parsed.filename,
            "incremental-snapshot-404192616-404247502-3DsTyBkAQsLF9FmQ4V56aiTmtwbJ29ePxAjnnPKrFQpT.tar.zst"
        );
        assert_eq!(parsed.from_slot, 404192616);
        assert_eq!(parsed.to_slot, 404247502);
        assert_eq!(parsed.hash, "3DsTyBkAQsLF9FmQ4V56aiTmtwbJ29ePxAjnnPKrFQpT");
    }

    #[test]
    fn snapshot_url() {
        let url = "http://127.0.0.1:8899/snapshot-404192616-3DsTyBkAQsLF9FmQ4V56aiTmtwbJ29ePxAjnnPKrFQpT.tar.zst";
        let parsed = FullSnapshotName::from_url(url).unwrap();

        assert_eq!(
            parsed.filename,
            "snapshot-404192616-3DsTyBkAQsLF9FmQ4V56aiTmtwbJ29ePxAjnnPKrFQpT.tar.zst"
        );
        assert_eq!(parsed.slot, 404192616);
        assert_eq!(parsed.hash, "3DsTyBkAQsLF9FmQ4V56aiTmtwbJ29ePxAjnnPKrFQpT");
    }
}
