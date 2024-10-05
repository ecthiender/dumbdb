use dumbdb::{Database, GetItemCommand, PutItemCommand};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_json::json;

fn main() -> anyhow::Result<()> {
    println!("Hello, world! Executing commands in dumbdb ----> ");
    let authors_table = json!({
        "name": "authors",
        "columns": [
            {
                "name": "id",
                "type": "Integer",
            },
            {
                "name": "name",
                "type": "Text",
            }
        ],
        "primary_key": "id"
    });

    let mut db = Database::new("./data/dumbdb")?;
    db.create_table(serde_json::from_value(authors_table)?)?;

    for i in 0..10000 {
        let author_item = create_put_item(i)?;
        db.put_item(author_item)?;
    }

    for i in 5672..8764 {
        let cmd = create_get_item(i)?;
        let record = db.get_item(cmd)?;
        println!("Get Item of {}: Result: {:?}", i, record);
    }

    Ok(())
}

fn create_get_item(id: u64) -> anyhow::Result<GetItemCommand> {
    Ok(serde_json::from_value(json!({
        "table_name": "authors",
        "key": id.to_string(),
    }))?)
}

fn create_put_item(id: u64) -> anyhow::Result<PutItemCommand> {
    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();

    Ok(serde_json::from_value(json!({
        "table_name": "authors",
        "item": {
            "id": id,
            "name": rand_string,
        }
    }))?)
}
