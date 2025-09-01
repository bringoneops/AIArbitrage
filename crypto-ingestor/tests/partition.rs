use ingestor::config::partition_specs;

#[test]
fn partition_subsets_are_disjoint() {
    let specs = vec![
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
        "d".to_string(),
    ];

    let p0 = partition_specs(specs.clone(), 2, 0);
    let p1 = partition_specs(specs.clone(), 2, 1);

    // subsets should not overlap
    for s in &p0 {
        assert!(!p1.contains(s));
    }
    assert_eq!(p0.len() + p1.len(), specs.len());
}
