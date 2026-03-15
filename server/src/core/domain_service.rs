use crate::core::{KionasCatalog, KionasTable, DomainResource};
use kionas::parser::datafusion_sql::sqlparser::ast::{SchemaName, ColumnDef, Ident, CreateTable};

pub struct DomainService {}

impl DomainService {
    pub fn from_create_schema(name: &SchemaName, owner: &str) -> Result<KionasCatalog, String> {
        // Use the ObjectName's displayable string form
        let n = name.to_string();
        let c = KionasCatalog::new(n, owner.to_string());
        c.validate()?;
        Ok(c)
    }

    pub fn from_create_table(ct: &CreateTable, default_schema: &str) -> Result<KionasTable, String> {
        let table_name = ct.name.to_string();
        // Build simple column list (name, type) using debug formatting for type
        let mut cols: Vec<(String,String)> = Vec::new();
        for col in ct.columns.iter() {
            let cname = col.name.to_string();
            let ctype = format!("{:?}", col.data_type);
            cols.push((cname, ctype));
        }
        let t = KionasTable::new((*ct).clone());
        DomainResource::validate(&t)?;
        Ok(t)
    }
}
