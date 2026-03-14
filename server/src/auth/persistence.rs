use super::{User, Token};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};

use parquet::record::RowAccessor;
use std::fs::File;
use std::sync::Arc;

use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{StringArray, UInt64Array, ArrayRef};

pub struct Persistence{
    data_path: String,
}

impl Persistence {
    pub fn new(data_path: String) -> Self {
        Self {
            data_path,
        }
    }

    
    pub fn save_users(users: Vec<User>, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("username", DataType::Utf8, false),
            Field::new("password", DataType::Utf8, false),
        ]));
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(WriterProperties::builder().build()))?;

        let usernames: Vec<&str> = users.iter().map(|u| u.username.as_str()).collect();
        let passwords: Vec<&str> = users.iter().map(|u| u.password.as_str()).collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(usernames)) as ArrayRef,
                Arc::new(StringArray::from(passwords)) as ArrayRef,
            ],
        )?;

        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    pub fn load_users(path: &str) -> Result<Vec<User>, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let users_projected_schema = Arc::new(
            Schema::new(
                vec![
                    Field::new("username", DataType::Utf8, false),
                    Field::new("password", DataType::Utf8, false),
                    ]));
        let schema = reader.metadata().file_metadata().schema();
        let schema = schema.clone();
        let schema = Arc::new(schema.clone());

        let mut arrow_reader = reader.get_row_iter(Some(Arc::try_unwrap(schema).unwrap_or_else(|s| (*s).clone())))?;

        let mut users = Vec::new();
        while let Some(record) = arrow_reader.next() {
            let record = record?;
            let username = record.get_string(0).expect("Failed to get username");
            let password = record.get_string(1).expect("Failed to get password");
    
            users.push(User {
                username: username.to_string(),
                password: password.to_string(),
            });
        }
        Ok(users)
    }

    pub fn save_tokens(tokens: Vec<Token>, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("token", DataType::Utf8, false),
            Field::new("username", DataType::Utf8, false),
            Field::new("exp", DataType::UInt64, false),
        ]));
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(WriterProperties::builder().build()))?;

        let tokens_str: Vec<&str> = tokens.iter().map(|t| t.token.as_str()).collect();
        let usernames: Vec<&str> = tokens.iter().map(|t| t.username.as_str()).collect();
        let exps: Vec<u64> = tokens.iter().map(|t| t.exp as u64).collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tokens_str)) as ArrayRef,
                Arc::new(StringArray::from(usernames)) as ArrayRef,
                Arc::new(UInt64Array::from(exps)) as ArrayRef,
            ],
        )?;

        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    pub fn load_tokens(path: &str) -> Result<Vec<Token>, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let tokens_projected_schema = Arc::new(
            Schema::new(
                vec![
                    Field::new("token", DataType::Utf8, false),
                    Field::new("username", DataType::Utf8, false),
                    Field::new("exp", DataType::UInt64, false),
                    ]));

        let schema = reader.metadata().file_metadata().schema();
        let schema = schema.clone();
        let schema = Arc::new(schema.clone());

        let mut arrow_reader = reader.get_row_iter(Some(Arc::try_unwrap(schema).unwrap_or_else(|s| (*s).clone())))?;

        let mut tokens = Vec::new();
        while let Some(record) = arrow_reader.next() {
            let record = record?;
            let token = record.get_string(0).expect("Failed to get token");
            let username = record.get_string(1).expect("Failed to get username");
            let exp = record.get_ulong(2).expect("Failed to get exp") as usize;
    
            tokens.push(Token {
                token: token.to_string(),
                username: username.to_string(),
                exp,
            });
        }
        Ok(tokens)
    }
}