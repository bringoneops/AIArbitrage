use ethers::types::Address;
use ingestor::labels::load_labels;
use std::fs;

#[tokio::test]
async fn loads_labels_from_csv() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("labels.csv");
    fs::write(&file_path, "0x0000000000000000000000000000000000000001,One").unwrap();
    let map = load_labels(file_path.to_str().unwrap()).unwrap();
    let addr: Address = "0x0000000000000000000000000000000000000001".parse().unwrap();
    assert_eq!(map.get(&addr).unwrap(), "One");
}
