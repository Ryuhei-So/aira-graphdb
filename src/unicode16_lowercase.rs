//! Runtime adapter for the byte-pinned Unicode 16 lowercase table generated
//! by Aira Synapse.  The table is producer-owned; this module only executes it.

mod generated {
    include!("generated/unicode16_lowercase_lookup.rs");
}

pub use generated::V15_ENTITY_NORMALIZATION_DIGEST;

fn in_ranges(value: u32, ranges: &[(u32, u32)]) -> bool {
    ranges
        .binary_search_by(|(start, end)| {
            if value < *start {
                std::cmp::Ordering::Greater
            } else if value > *end {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

fn is_final_sigma_checked<E>(
    scalars: &[char],
    index: usize,
    checkpoint: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    let mut has_cased_before = false;
    for scalar in scalars[..index].iter().rev() {
        checkpoint()?;
        let value = *scalar as u32;
        if !in_ranges(value, generated::CASE_IGNORABLE_RANGES) {
            has_cased_before = in_ranges(value, generated::CASED_RANGES);
            break;
        }
    }
    if !has_cased_before {
        return Ok(false);
    }
    for scalar in &scalars[index + 1..] {
        checkpoint()?;
        let value = *scalar as u32;
        if !in_ranges(value, generated::CASE_IGNORABLE_RANGES) {
            return Ok(!in_ranges(value, generated::CASED_RANGES));
        }
    }
    Ok(true)
}

#[derive(Debug)]
pub enum LowercaseError<E> {
    Checkpoint(E),
    Resource,
}

pub fn lowercase_bounded<E>(
    value: &str,
    max_output_bytes: usize,
    mut checkpoint: impl FnMut() -> Result<(), E>,
) -> Result<String, LowercaseError<E>> {
    let mut scalars = Vec::new();
    scalars
        .try_reserve_exact(value.len())
        .map_err(|_| LowercaseError::Resource)?;
    for scalar in value.chars() {
        checkpoint().map_err(LowercaseError::Checkpoint)?;
        scalars.push(scalar);
    }
    let mut output = String::new();
    output
        .try_reserve(value.len().min(max_output_bytes))
        .map_err(|_| LowercaseError::Resource)?;
    for (index, scalar) in scalars.iter().copied().enumerate() {
        checkpoint().map_err(LowercaseError::Checkpoint)?;
        let codepoint = scalar as u32;
        let mut mapped = [codepoint, 0, 0];
        let mut length = 1;
        if codepoint == 0x03A3
            && is_final_sigma_checked(&scalars, index, &mut checkpoint)
                .map_err(LowercaseError::Checkpoint)?
        {
            mapped = [0x03C2, 0, 0];
        } else if let Ok(mapping_index) =
            generated::LOWERCASE_MAPPINGS.binary_search_by_key(&codepoint, |(source, _, _)| *source)
        {
            let (_, mapped_length, values) = generated::LOWERCASE_MAPPINGS[mapping_index];
            mapped = values;
            length = mapped_length;
        }
        for value in mapped.into_iter().take(length) {
            let scalar = char::from_u32(value).expect("generated lowercase scalar is valid");
            let next = output
                .len()
                .checked_add(scalar.len_utf8())
                .ok_or(LowercaseError::Resource)?;
            if next > max_output_bytes {
                return Err(LowercaseError::Resource);
            }
            output
                .try_reserve(scalar.len_utf8())
                .map_err(|_| LowercaseError::Resource)?;
            output.push(scalar);
        }
    }
    Ok(output)
}

/// Unicode 16.0.0 full lowercase for the Synapse v15 entity contract.
pub fn lowercase(value: &str) -> String {
    lowercase_bounded(value, usize::MAX, || Ok::<_, std::convert::Infallible>(()))
        .expect("lowercase allocation")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn producer_lookup_bytes_and_contextual_sigma_are_pinned() {
        let digest = Sha256::digest(include_bytes!("generated/unicode16_lowercase_lookup.rs"));
        assert_eq!(
            format!("{digest:x}"),
            "4637b1b21285887f291ab36c19edb5bb94d1660364eb948159117c0d493d8f59"
        );
        assert_eq!(
            V15_ENTITY_NORMALIZATION_DIGEST,
            "v15-entity-normalization-ecmascript-tolowercase-unicode16.0.0@1"
        );
        for (input, expected) in [
            ("Σ", "σ"),
            ("AΣ", "aς"),
            ("ΣA", "σa"),
            ("AΣA", "aσa"),
            ("AΣ\u{0301}", "aς\u{0301}"),
            ("AΣ\u{0301}A", "aσ\u{0301}a"),
        ] {
            assert_eq!(lowercase(input), expected);
        }
    }

    #[test]
    fn bounded_lowercase_rejects_growth_and_exposes_inner_work() {
        let mut checkpoints = 0_u64;
        assert_eq!(
            lowercase_bounded("AΣ\u{0301}A", 16, || {
                checkpoints += 1;
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap(),
            "aσ\u{0301}a"
        );
        assert!(checkpoints > "AΣ\u{0301}A".chars().count() as u64);
        assert!(matches!(
            lowercase_bounded("İ", 1, || Ok::<_, std::convert::Infallible>(())),
            Err(LowercaseError::Resource)
        ));
    }
}
