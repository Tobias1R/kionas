// Core domain module for Kionas: catalog, table, warehouse
// This file provides a minimal trait and module exports for domain types.

pub trait DomainResource {
    /// Short kind name (e.g. "catalog", "table", "warehouse")
    fn kind(&self) -> &'static str;

    /// Basic validation for the resource; return Err(String) with message on failure.
    fn validate(&self) -> Result<(), String>;
}

pub mod catalog;
pub mod table;
pub mod warehouse;
pub mod database;
pub mod domain_service;

pub use catalog::KionasCatalog;
pub use table::KionasTable;
pub use warehouse::KionasWarehouse;
pub use database::KionasDatabase;
pub use domain_service::DomainService;
