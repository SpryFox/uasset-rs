use std::io::{Read, Seek, SeekFrom};

/// Maximum plausible element count for a `ComponentTags` array. Serves as a
/// sanity bound to reject garbage bytes that happen to decode as a count.
const MAX_ARRAY_COUNT: i32 = 64;

/// Upper bound on FName.number for heuristic validation. Real FName numbers
/// above this are exceedingly rare in serialized assets, and any value
/// approaching `i32::MAX` is near-certainly random bytes.
const MAX_PLAUSIBLE_FNAME_NUMBER: i32 = 1_000_000;

/// Window (in bytes) after the `ComponentTags + ArrayProperty` FName pair
/// within which the ArrayProperty's `InnerType` FName (`NameProperty`) is
/// expected. Bounded so we don't match an unrelated occurrence later in the
/// blob.
const INNER_TYPE_WINDOW: usize = 64;

/// Precomputed name-table indices for the FNames we look for. Computing these
/// once per file avoids repeated linear scans of the name table per export.
/// Returns `None` if `ComponentTags` or `ArrayProperty` are absent from the
/// name table — in which case no export in this file can possibly carry the
/// property we care about, and all extraction can be skipped.
pub struct NameIndices {
    pub component_tags: i32,
    pub array_property: i32,
    pub name_property: Option<i32>,
}

impl NameIndices {
    pub fn lookup(names: &[String]) -> Option<Self> {
        let component_tags = names.iter().position(|n| n == "ComponentTags")? as i32;
        let array_property = names.iter().position(|n| n == "ArrayProperty")? as i32;
        let name_property = names.iter().position(|n| n == "NameProperty").map(|i| i as i32);
        Some(Self { component_tags, array_property, name_property })
    }
}

/// Scan the serialized property data of an export and extract the elements
/// of its `ComponentTags` property (an `ArrayProperty` of `NameProperty`).
///
/// The UE tagged-property layout for `ArrayProperty` has varied across engine
/// versions (UE5.3+ introduced EPropertyTagFlags), so rather than attempt to
/// decode every variant of the tag header, this uses a heuristic:
///
/// 1. Find the byte pattern `FName(ComponentTags, 0) + FName(ArrayProperty, 0)`
///    within the export's serial data.
/// 2. From that point, scan forward for an `i32 count` in a plausible range
///    whose `count` following 8-byte slots all decode as valid FNames.
/// 3. Double-ended check: the 8 bytes immediately after the array must also
///    decode as a valid FName (the next property's name, or the `None`
///    terminator). This rules out random-byte coincidences where the "tags"
///    parse but the surrounding context is garbage.
/// 4. Emit those FNames as tag strings.
///
/// Returns an empty vec on any error or if the property is absent.
pub fn extract_component_tags<R: Read + Seek>(
    reader: &mut R,
    indices: &NameIndices,
    names: &[String],
    serial_offset: u64,
    serial_size: u64,
) -> Vec<String> {
    if reader.seek(SeekFrom::Start(serial_offset)).is_err() {
        return vec![];
    }
    let mut data = vec![0u8; serial_size as usize];
    if reader.read_exact(&mut data).is_err() {
        return vec![];
    }

    let needle = fname_pair_bytes(indices.component_tags, indices.array_property);
    // Outer loop retries on subsequent pattern matches if the first one fails
    // to yield a valid tag array. In practice the FName pair has only ever
    // been observed once per export, but the cost of retrying is negligible
    // and it hardens the heuristic against a spurious 16-byte collision
    // preceding a genuine match.
    let mut search_from = 0;
    while search_from < data.len() {
        let pattern_start = match find_subsequence(&data[search_from..], &needle) {
            Some(p) => search_from + p,
            None => return vec![],
        };
        if let Some(tags) = try_extract_at(&data, pattern_start, indices, names) {
            return tags;
        }
        search_from = pattern_start + 1;
    }
    vec![]
}

fn try_extract_at(
    data: &[u8],
    pattern_start: usize,
    indices: &NameIndices,
    names: &[String],
) -> Option<Vec<String>> {
    let mut scan_start = pattern_start + 16;
    if let Some(np_idx) = indices.name_property {
        let inner_needle = fname_bytes(np_idx);
        let window_end = (scan_start + INNER_TYPE_WINDOW).min(data.len());
        if scan_start < window_end {
            if let Some(pos) = find_subsequence(&data[scan_start..window_end], &inner_needle) {
                scan_start = scan_start + pos + 8;
            }
        }
    }
    let scan_end = data.len().saturating_sub(4);
    for candidate in scan_start..scan_end {
        let count = read_i32(data, candidate);
        if !(0..=MAX_ARRAY_COUNT).contains(&count) {
            continue;
        }
        let elements_start = candidate + 4;
        let elements_len = (count as usize) * 8;
        if elements_start + elements_len > data.len() {
            continue;
        }

        let mut tags = Vec::with_capacity(count as usize);
        let mut ok = true;
        for i in 0..count as usize {
            let off = elements_start + i * 8;
            let idx = read_i32(data, off);
            let num = read_i32(data, off + 4);
            if !is_valid_fname(idx, num, names.len()) {
                ok = false;
                break;
            }
            tags.push(names[idx as usize].clone());
        }

        if ok && !tags.is_empty() {
            // Double-ended canary: the 8 bytes immediately after the array
            // should be a valid FName — either the `None` terminator of this
            // property stream, or the PropertyName of the next tag. Random
            // bytes almost never decode as a valid FName, so this rules out
            // spurious matches where the tags themselves happened to parse.
            let trailer = elements_start + elements_len;
            if trailer + 8 <= data.len() {
                let t_idx = read_i32(data, trailer);
                let t_num = read_i32(data, trailer + 4);
                if !is_valid_fname(t_idx, t_num, names.len()) {
                    continue;
                }
            }
            return Some(tags);
        }
        // count == 0 could be genuine (empty array) or a spurious match on
        // zero-padding. We can't distinguish them from byte content alone,
        // so keep scanning; if we exit the loop without finding anything
        // non-empty we signal "no extraction" and let the outer loop retry
        // at the next FName-pair match (if any).
    }

    None
}

/// Cheap check: does the export's serial blob contain the FName pattern for
/// `ComponentTags`? Used by the export-scan canary to flag exports that were
/// filtered out by class-name heuristics but which nonetheless carry a
/// `ComponentTags` property (i.e. a class that breaks UE's
/// `*Component`-suffix naming convention).
pub fn serial_contains_component_tags_name<R: Read + Seek>(
    reader: &mut R,
    component_tags_idx: i32,
    serial_offset: u64,
    serial_size: u64,
) -> bool {
    if reader.seek(SeekFrom::Start(serial_offset)).is_err() {
        return false;
    }
    let mut buf = vec![0u8; serial_size as usize];
    if reader.read_exact(&mut buf).is_err() {
        return false;
    }
    let needle = fname_bytes(component_tags_idx);
    find_subsequence(&buf, &needle).is_some()
}

fn fname_bytes(idx: i32) -> [u8; 8] {
    let mut buf = [0u8; 8];
    buf[..4].copy_from_slice(&idx.to_le_bytes());
    // number (i32) = 0 — already zeroed
    buf
}

fn fname_pair_bytes(first_idx: i32, second_idx: i32) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[..4].copy_from_slice(&first_idx.to_le_bytes());
    buf[8..12].copy_from_slice(&second_idx.to_le_bytes());
    buf
}

fn read_i32(data: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

fn is_valid_fname(idx: i32, num: i32, names_len: usize) -> bool {
    idx >= 0
        && (idx as usize) < names_len
        && (0..MAX_PLAUSIBLE_FNAME_NUMBER).contains(&num)
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Canonical test name table — order matters (tests reference by index).
    // `ComponentTags` is at a non-zero index so `fname_bytes(ComponentTags)`
    // is not all-zero and doesn't accidentally match zero-padded blobs.
    const CT: i32 = 10;
    const AP: i32 = 11;
    const NP: i32 = 12;
    const COLL: i32 = 13;
    const SMO: i32 = 14;
    const NONE: i32 = 15;

    fn test_names() -> Vec<String> {
        let mut names: Vec<String> = (0..10).map(|i| format!("Filler{}", i)).collect();
        names.extend([
            "ComponentTags".into(),      // 10
            "ArrayProperty".into(),      // 11
            "NameProperty".into(),       // 12
            "CollisionComponent".into(), // 13
            "staticMeshOverride".into(), // 14
            "None".into(),               // 15
            "NextProperty".into(),       // 16
        ]);
        names
    }

    fn fname(idx: i32) -> [u8; 8] {
        fname_bytes(idx)
    }

    /// Build a blob in the shape we observe from UE5: ComponentTags+ArrayProperty,
    /// a 16-byte metadata region containing the NameProperty InnerType FName,
    /// then (after some padding) an i32 count and `count` FName entries,
    /// followed by a trailer FName (typically None).
    fn build_valid_blob(tag_indices: &[i32]) -> Vec<u8> {
        let mut blob = Vec::new();
        blob.extend_from_slice(&fname(CT));
        blob.extend_from_slice(&fname(AP));
        // 16B metadata region: 8B filler + InnerType FName(NameProperty)
        blob.extend_from_slice(&[0u8; 8]);
        blob.extend_from_slice(&fname(NP));
        // 4B size placeholder + 1B HasPropertyGuid
        blob.extend_from_slice(&[0u8; 4]);
        blob.push(0);
        // count
        blob.extend_from_slice(&(tag_indices.len() as i32).to_le_bytes());
        // tag entries
        for &i in tag_indices {
            blob.extend_from_slice(&fname(i));
        }
        // trailer: FName(None)
        blob.extend_from_slice(&fname(NONE));
        blob
    }

    fn run_extract(blob: &[u8], names: &[String]) -> Vec<String> {
        let indices = NameIndices::lookup(names).expect("names table missing markers");
        let mut reader = Cursor::new(blob.to_vec());
        extract_component_tags(&mut reader, &indices, names, 0, blob.len() as u64)
    }

    #[test]
    fn extracts_three_tags() {
        let names = test_names();
        let blob = build_valid_blob(&[COLL, SMO, COLL]);
        let tags = run_extract(&blob, &names);
        assert_eq!(tags, vec!["CollisionComponent", "staticMeshOverride", "CollisionComponent"]);
    }

    #[test]
    fn returns_empty_when_property_absent() {
        let names = test_names();
        // Blob with no ComponentTags pattern at all.
        let blob = vec![0xFFu8; 128];
        let tags = run_extract(&blob, &names);
        assert!(tags.is_empty());
    }

    #[test]
    fn rejects_trailing_garbage() {
        let names = test_names();
        let mut blob = build_valid_blob(&[COLL, SMO]);
        // Overwrite the trailer FName with a negative idx — invalid.
        let trailer_pos = blob.len() - 8;
        blob[trailer_pos..trailer_pos + 4].copy_from_slice(&(-1i32).to_le_bytes());
        let tags = run_extract(&blob, &names);
        // Should fail the double-ended check and fall through to empty.
        assert!(tags.is_empty());
    }

    #[test]
    fn inner_type_not_matched_as_array() {
        // If the heuristic skipped the InnerType NameProperty FName, it could
        // misread those 8 bytes as count=NP followed by FNames.
        // This test verifies the skip works: a blob with only the tag header
        // and InnerType — no real array — must return empty.
        let names = test_names();
        let mut blob = Vec::new();
        blob.extend_from_slice(&fname(CT));
        blob.extend_from_slice(&fname(AP));
        blob.extend_from_slice(&[0u8; 8]);
        blob.extend_from_slice(&fname(NP));
        // Followed by only 0xFF — no real count/entries, no valid FNames.
        blob.extend_from_slice(&[0xFFu8; 64]);
        let tags = run_extract(&blob, &names);
        assert!(tags.is_empty());
    }

    #[test]
    fn lookup_returns_none_if_markers_absent() {
        let names = vec!["Foo".into(), "Bar".into()];
        assert!(NameIndices::lookup(&names).is_none());
    }

    #[test]
    fn rejects_invalid_fname_mid_array() {
        let names = test_names();
        let mut blob = build_valid_blob(&[COLL, SMO, COLL]);
        // Corrupt the middle element's idx to something out-of-range.
        // Element layout starts after: 2*FName + 16B meta + 4B size + 1B guid
        // + 4B count = 41 bytes. Middle element is at 41 + 8 = 49.
        let mid = 41 + 8;
        blob[mid..mid + 4].copy_from_slice(&9999i32.to_le_bytes());
        let tags = run_extract(&blob, &names);
        assert!(tags.is_empty());
    }

    #[test]
    fn rejects_oversize_count() {
        // A count above MAX_ARRAY_COUNT should be rejected; the heuristic
        // should continue scanning and fall through to empty.
        let names = test_names();
        let mut blob = Vec::new();
        blob.extend_from_slice(&fname(CT));
        blob.extend_from_slice(&fname(AP));
        blob.extend_from_slice(&[0u8; 8]);
        blob.extend_from_slice(&fname(NP));
        blob.extend_from_slice(&[0u8; 4]);
        blob.push(0);
        // count = 10_000 — well above MAX_ARRAY_COUNT
        blob.extend_from_slice(&10_000i32.to_le_bytes());
        blob.extend_from_slice(&[0xFFu8; 64]);
        let tags = run_extract(&blob, &names);
        assert!(tags.is_empty());
    }

    #[test]
    fn rejects_count_overflowing_bounds() {
        // A count that would require reading past the end of the blob
        // must be rejected, not panic.
        let names = test_names();
        let mut blob = Vec::new();
        blob.extend_from_slice(&fname(CT));
        blob.extend_from_slice(&fname(AP));
        blob.extend_from_slice(&[0u8; 8]);
        blob.extend_from_slice(&fname(NP));
        blob.extend_from_slice(&[0u8; 4]);
        blob.push(0);
        // count=50, but only ~16 bytes remain after — would need 400B of entries
        blob.extend_from_slice(&50i32.to_le_bytes());
        blob.extend_from_slice(&[0u8; 16]);
        let tags = run_extract(&blob, &names);
        assert!(tags.is_empty());
    }

    #[test]
    fn empty_array_returns_empty() {
        // A genuinely empty ComponentTags array (count=0) with valid
        // surrounding structure is indistinguishable from zero-padding
        // under this heuristic; we return empty, same as "property absent".
        // This test pins that behavior so changes to it are intentional.
        let names = test_names();
        let mut blob = Vec::new();
        blob.extend_from_slice(&fname(CT));
        blob.extend_from_slice(&fname(AP));
        blob.extend_from_slice(&[0u8; 8]);
        blob.extend_from_slice(&fname(NP));
        blob.extend_from_slice(&[0u8; 4]);
        blob.push(0);
        blob.extend_from_slice(&0i32.to_le_bytes()); // count=0
        blob.extend_from_slice(&fname(NONE));        // trailer
        let tags = run_extract(&blob, &names);
        assert!(tags.is_empty());
    }

    #[test]
    fn retries_past_spurious_fname_pair() {
        // A spurious FName(CT)+FName(AP) pair appears earlier in the blob
        // (e.g. embedded in some other payload); the genuine tag header is
        // later. The retry loop should skip past the spurious match and
        // find the real one.
        let names = test_names();
        let mut blob = Vec::new();
        // Spurious pair — not followed by a valid tag structure.
        blob.extend_from_slice(&fname(CT));
        blob.extend_from_slice(&fname(AP));
        blob.extend_from_slice(&[0xFFu8; 64]);
        // Real tag header follows.
        let real = build_valid_blob(&[COLL, SMO]);
        blob.extend_from_slice(&real);
        let tags = run_extract(&blob, &names);
        assert_eq!(tags, vec!["CollisionComponent", "staticMeshOverride"]);
    }

    #[test]
    fn zero_size_blob_returns_empty() {
        let names = test_names();
        let tags = run_extract(&[], &names);
        assert!(tags.is_empty());
    }

    #[test]
    fn tag_with_nonzero_fname_number() {
        // Tags like `MyTag_1` serialize with FName.number != 0. The heuristic
        // must accept these as valid FNames.
        let names = test_names();
        let mut blob = Vec::new();
        blob.extend_from_slice(&fname(CT));
        blob.extend_from_slice(&fname(AP));
        blob.extend_from_slice(&[0u8; 8]);
        blob.extend_from_slice(&fname(NP));
        blob.extend_from_slice(&[0u8; 4]);
        blob.push(0);
        blob.extend_from_slice(&1i32.to_le_bytes()); // count=1
        // FName(COLL, 5) — number 5 is valid
        blob.extend_from_slice(&COLL.to_le_bytes());
        blob.extend_from_slice(&5i32.to_le_bytes());
        blob.extend_from_slice(&fname(NONE));
        let tags = run_extract(&blob, &names);
        assert_eq!(tags, vec!["CollisionComponent"]);
    }

    #[test]
    fn serial_contains_detector() {
        let names = test_names();
        let indices = NameIndices::lookup(&names).unwrap();
        let blob = build_valid_blob(&[COLL]);
        let mut reader = Cursor::new(blob.clone());
        assert!(serial_contains_component_tags_name(
            &mut reader,
            indices.component_tags,
            0,
            blob.len() as u64,
        ));

        let blob_no_ct = vec![0xFFu8; 64];
        let mut reader = Cursor::new(blob_no_ct.clone());
        assert!(!serial_contains_component_tags_name(
            &mut reader,
            indices.component_tags,
            0,
            blob_no_ct.len() as u64,
        ));
    }
}
