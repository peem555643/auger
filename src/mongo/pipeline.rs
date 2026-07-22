//! Construction of the aggregation pipeline a scan actually sends to Mongo.
//!
//! Keeping this separate from execution means the exact pipeline is a pure
//! function of the plan — it can be unit-tested without a server, and it is
//! what `EXPLAIN` prints, so what you read is what the server ran.

use bson::{Bson, Document, doc};

/// Sort direction for a pushed-down `ORDER BY` key.
///
/// The plumbing is in place and exercised by tests, but nothing constructs
/// these outside them yet: `ORDER BY` reaches the scan as a `SortExec` above
/// it, so pushing it down needs a physical optimizer rule. That is the next
/// milestone; see the roadmap in README.md.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

impl SortDir {
    fn as_i32(self) -> i32 {
        match self {
            Self::Asc => 1,
            Self::Desc => -1,
        }
    }
}

/// Everything a single collection scan pushes to the server.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MongoPlan {
    /// `$match` derived from `WHERE`.
    pub filter: Option<Document>,
    /// Extra `$match` restricting this partition to an `_id` range. Kept apart
    /// from `filter` so `EXPLAIN` can show which part is parallelism and which
    /// part is the user's predicate.
    pub partition: Option<Document>,
    /// Dotted paths to fetch. Empty means "the whole document".
    pub projection: Vec<String>,
    /// `$sort` derived from `ORDER BY`.
    pub sort: Vec<(String, SortDir)>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
}

impl MongoPlan {
    /// The `$match` document, combining the user predicate with the partition
    /// bound. Returns `None` when the scan is unfiltered.
    pub fn match_stage(&self) -> Option<Document> {
        match (&self.filter, &self.partition) {
            (None, None) => None,
            (Some(f), None) => Some(f.clone()),
            (None, Some(p)) => Some(p.clone()),
            (Some(f), Some(p)) => Some(doc! {
                "$and": [Bson::Document(f.clone()), Bson::Document(p.clone())]
            }),
        }
    }

    /// Render the plan as an aggregation pipeline.
    ///
    /// Stage order matters for more than tidiness: `$match` first is what lets
    /// the server use an index, and `$sort` immediately after it is what lets a
    /// `$sort` + `$limit` pair collapse into a bounded top-k rather than an
    /// in-memory sort of the whole collection.
    pub fn to_pipeline(&self) -> Vec<Document> {
        let mut stages = Vec::with_capacity(5);

        if let Some(m) = self.match_stage() {
            stages.push(doc! { "$match": m });
        }
        if !self.sort.is_empty() {
            let mut sort = Document::new();
            for (path, dir) in &self.sort {
                sort.insert(path.clone(), dir.as_i32());
            }
            stages.push(doc! { "$sort": sort });
        }
        if let Some(skip) = self.skip.filter(|s| *s > 0) {
            stages.push(doc! { "$skip": skip });
        }
        if let Some(limit) = self.limit.filter(|l| *l >= 0) {
            stages.push(doc! { "$limit": limit });
        }
        if !self.projection.is_empty() {
            stages.push(doc! { "$project": projection_doc(&self.projection) });
        }

        stages
    }
}

/// Build a `$project` document from dotted paths.
///
/// Mongo returns `_id` unless it is explicitly suppressed, so a projection that
/// does not ask for `_id` has to say `_id: 0` — otherwise the server sends a
/// column nobody asked for on every document of every scan.
pub fn projection_doc(paths: &[String]) -> Document {
    let mut projection = Document::new();
    let mut wants_id = false;

    // A projection of both `a` and `a.b` is an error on older servers and
    // redundant on newer ones; keeping only the shortest prefix avoids it.
    let mut kept: Vec<&String> = Vec::with_capacity(paths.len());
    for path in paths {
        if paths.iter().any(|other| is_strict_prefix(other, path)) {
            continue;
        }
        kept.push(path);
    }

    for path in kept {
        if path == "_id" || path.starts_with("_id.") {
            wants_id = true;
        }
        projection.insert(path.clone(), 1);
    }
    if !wants_id {
        projection.insert("_id", 0);
    }
    projection
}

fn is_strict_prefix(prefix: &str, path: &str) -> bool {
    path.len() > prefix.len()
        && path.starts_with(prefix)
        && path.as_bytes().get(prefix.len()) == Some(&b'.')
}

/// `$match` restricting a scan to documents whose `_id` falls in `[lo, hi)`.
///
/// `_id` always carries a unique index, so range-splitting on it gives every
/// partition an index-driven scan. An open end is represented by `None`, which
/// is how the first and last partitions cover the tails.
pub fn id_range(lo: Option<&Bson>, hi: Option<&Bson>) -> Option<Document> {
    let mut bounds = Document::new();
    if let Some(lo) = lo {
        bounds.insert("$gte", lo.clone());
    }
    if let Some(hi) = hi {
        bounds.insert("$lt", hi.clone());
    }
    (!bounds.is_empty()).then(|| doc! { "_id": bounds })
}

/// Split sampled `_id` values into `n` contiguous, non-overlapping ranges that
/// together cover every document — including ones outside the sampled span.
pub fn partition_bounds(sorted_ids: &[Bson], n: usize) -> Vec<Option<Document>> {
    if n <= 1 || sorted_ids.len() < 2 {
        return vec![None];
    }
    let n = n.min(sorted_ids.len());
    let mut cuts: Vec<&Bson> = Vec::with_capacity(n - 1);
    for i in 1..n {
        let idx = i * sorted_ids.len() / n;
        let candidate = &sorted_ids[idx];
        // Duplicate cut points would produce empty partitions.
        if cuts.last().is_none_or(|last| *last != candidate) {
            cuts.push(candidate);
        }
    }

    let mut out = Vec::with_capacity(cuts.len() + 1);
    let mut prev: Option<&Bson> = None;
    for cut in &cuts {
        out.push(id_range(prev, Some(cut)));
        prev = Some(cut);
    }
    // The final partition is open-ended so documents inserted after sampling
    // are still scanned exactly once.
    out.push(id_range(prev, None));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trivial_plan_produces_an_empty_pipeline() {
        assert!(MongoPlan::default().to_pipeline().is_empty());
    }

    #[test]
    fn stages_are_ordered_match_sort_skip_limit_project() {
        let plan = MongoPlan {
            filter: Some(doc! { "age": { "$gt": 18 } }),
            projection: vec!["name".into()],
            sort: vec![("age".into(), SortDir::Desc)],
            skip: Some(10),
            limit: Some(5),
            ..Default::default()
        };
        let keys: Vec<String> =
            plan.to_pipeline().iter().map(|s| s.keys().next().unwrap().clone()).collect();
        assert_eq!(keys, ["$match", "$sort", "$skip", "$limit", "$project"]);
    }

    #[test]
    fn partition_bound_is_conjoined_with_the_user_filter() {
        let plan = MongoPlan {
            filter: Some(doc! { "age": { "$gt": 18 } }),
            partition: Some(doc! { "_id": { "$lt": 100 } }),
            ..Default::default()
        };
        assert_eq!(
            plan.match_stage().unwrap(),
            doc! { "$and": [ { "age": { "$gt": 18 } }, { "_id": { "$lt": 100 } } ] }
        );
    }

    #[test]
    fn projection_suppresses_id_when_it_was_not_requested() {
        assert_eq!(projection_doc(&["name".into()]), doc! { "name": 1, "_id": 0 });
        assert_eq!(projection_doc(&["_id".into(), "name".into()]), doc! { "_id": 1, "name": 1 });
    }

    #[test]
    fn projection_drops_paths_covered_by_a_shorter_prefix() {
        // Asking for both `a` and `a.b` is a server error on older versions.
        let p = projection_doc(&["a".into(), "a.b".into(), "c.d".into()]);
        assert_eq!(p, doc! { "a": 1, "c.d": 1, "_id": 0 });
    }

    #[test]
    fn single_partition_has_no_bound() {
        let ids: Vec<Bson> = (0..10).map(Bson::Int32).collect();
        assert_eq!(partition_bounds(&ids, 1), vec![None]);
    }

    #[test]
    fn partitions_are_contiguous_and_cover_the_tails() {
        let ids: Vec<Bson> = (0..100).map(Bson::Int32).collect();
        let parts = partition_bounds(&ids, 4);
        assert_eq!(parts.len(), 4);

        // First partition is open on the low side, last on the high side, so
        // documents outside the sampled range are still scanned.
        let first = parts[0].as_ref().unwrap().get_document("_id").unwrap();
        assert!(!first.contains_key("$gte"));
        let last = parts[3].as_ref().unwrap().get_document("_id").unwrap();
        assert!(!last.contains_key("$lt"));

        // Every upper bound is the next partition's lower bound: no gaps, no overlap.
        for pair in parts.windows(2) {
            let hi = pair[0].as_ref().unwrap().get_document("_id").unwrap().get("$lt").unwrap();
            let lo = pair[1].as_ref().unwrap().get_document("_id").unwrap().get("$gte").unwrap();
            assert_eq!(hi, lo);
        }
    }

    #[test]
    fn duplicate_cut_points_do_not_create_empty_partitions() {
        let ids: Vec<Bson> = std::iter::repeat_n(Bson::Int32(1), 8).collect();
        let parts = partition_bounds(&ids, 4);
        assert_eq!(parts.len(), 2, "all-equal ids can only be split once");
    }

    #[test]
    fn zero_limit_is_preserved_but_negative_skip_is_dropped() {
        let plan = MongoPlan { limit: Some(0), skip: Some(0), ..Default::default() };
        let keys: Vec<String> =
            plan.to_pipeline().iter().map(|s| s.keys().next().unwrap().clone()).collect();
        assert_eq!(keys, ["$limit"], "LIMIT 0 must still be pushed; SKIP 0 is a no-op");
    }
}
