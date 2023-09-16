// base are the initial setup features we will use through the program

use std::path;

// TODO: add an arg parser
pub const API_URL: &str = "http://localhost:8080";

pub fn get_current_dir() -> path::PathBuf {
    let current_dir = std::env::current_dir().unwrap();
    current_dir
}

pub fn get_local_interchain_dir() -> path::PathBuf {
    let parent_dir = get_current_dir().parent().unwrap().to_path_buf();
    parent_dir
}

pub fn get_contract_path() -> path::PathBuf {
    let contract_path = get_local_interchain_dir().join("contracts");
    contract_path
}

pub fn create_contract_path() {
    let contract_path = get_contract_path();
    if !contract_path.exists() {
        std::fs::create_dir(contract_path).unwrap();
    }
}

pub fn get_contract_json_path() -> path::PathBuf {
    let contract_json_path = get_local_interchain_dir().join("configs").join("contract.json");
    contract_json_path
}

